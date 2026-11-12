namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module private DemandReleaseSupport =
    let wait (pending: Task<'T>) = pending.WaitAsync(TimeSpan.FromSeconds 30.)
    let joined (pending: Task) = pending.WaitAsync(TimeSpan.FromSeconds 30.)
    let run workflow = Async.StartAsTask(workflow, cancellationToken = CancellationToken.None)
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)
    let same expected actual = check (expected = actual) (sprintf "Expected %A, got %A" expected actual)
    let epoch = EpochId 1UL
    let scope = ScopeId 1UL
    let work = WorkId 1UL
    let otherWork = WorkId 2UL

    let define id stamp =
        ScopeEntry.Define { Work = id; Stamp = DefinitionStamp stamp; Reads = [] }

    let expectCanceled (pending: Task<'T>) = task {
        let mutable canceled = false
        try
            let! _ = wait pending
            ()
        with :? OperationCanceledException ->
            canceled <- true
        check canceled "Expected outward task cancellation."
    }

    let withHealthyClose handle release body = task {
        let mutable failure: exn option = None
        try
            do! body ()
        with error ->
            failure <- Some error
        release ()
        let! closed = AsyncMailbox.close handle |> run |> wait
        failure |> Option.iter raise
        same (Ok ()) closed
    }

    type BurstRecorder(callerCount: int, iterationsPerCaller: int) =
        let gate = obj ()
        let totalDemands = callerCount * iterationsPerCaller
        let demandSeq = Array.zeroCreate<int> (totalDemands + 4)
        let releaseSeq = Array.zeroCreate<int> (totalDemands + 4)
        let releaseCount = Array.zeroCreate<int> (totalDemands + 4)
        let activeForCaller = Array.zeroCreate<uint64> callerCount
        let startedAttempts = HashSet<AttemptId>()
        let drainedAttempts = HashSet<AttemptId>()
        let protocolErrors = ResizeArray<ProtocolError>()
        let mutable sequence = 0
        let mutable activeTotal = 0
        let mutable peakActive = 0
        let mutable fifoViolations = 0
        let mutable firstViolation: string option = None
        let extraCommandSeq = Dictionary<Action, int>()

        let callerIndex (rawId: uint64) =
            if rawId >= 1UL && rawId <= uint64 totalDemands then
                int ((rawId - 1UL) / uint64 iterationsPerCaller)
            else
                -1

        member _.Step (command: Command) (state: Core.State) =
            lock gate (fun () ->
                sequence <- sequence + 1
                let currentSeq = sequence
                match command.Action with
                | Action.Demand(DemandId rawId, _) ->
                    let idx = int rawId
                    if idx >= 1 && idx < demandSeq.Length && demandSeq[idx] = 0 then
                        demandSeq[idx] <- currentSeq
                    let caller = callerIndex rawId
                    if caller >= 0 then
                        let previous = activeForCaller[caller]
                        if previous <> 0UL then
                            fifoViolations <- fifoViolations + 1
                            if firstViolation.IsNone then
                                firstViolation <-
                                    Some(
                                        sprintf
                                            "Caller %d enqueued Demand(%d) at step %d before Release(%d) arrived."
                                            caller
                                            rawId
                                            currentSeq
                                            previous)
                        activeForCaller[caller] <- rawId
                    activeTotal <- activeTotal + 1
                    if activeTotal > peakActive then
                        peakActive <- activeTotal
                | Action.Release(DemandId rawId) ->
                    let idx = int rawId
                    if idx >= 1 && idx < releaseSeq.Length then
                        if releaseSeq[idx] = 0 then
                            releaseSeq[idx] <- currentSeq
                        releaseCount[idx] <- releaseCount[idx] + 1
                    let caller = callerIndex rawId
                    if caller >= 0 && activeForCaller[caller] = rawId then
                        activeForCaller[caller] <- 0UL
                    if activeTotal > 0 then
                        activeTotal <- activeTotal - 1
                | other ->
                    extraCommandSeq[other] <- currentSeq

                match Core.step command state with
                | Error err ->
                    protocolErrors.Add err
                    Error err
                | Ok(next, effects) ->
                    for effect in effects do
                        match effect.Action with
                        | EffectAction.Start req -> startedAttempts.Add req.Attempt |> ignore
                        | EffectAction.Drain attemptId -> drainedAttempts.Add attemptId |> ignore
                        | _ -> ()
                    Ok(next, effects))

        member _.DemandSeq(rawId: int) = lock gate (fun () -> demandSeq[rawId])
        member _.ReleaseSeq(rawId: int) = lock gate (fun () -> releaseSeq[rawId])
        member _.ReleaseCount(rawId: int) = lock gate (fun () -> releaseCount[rawId])
        member _.CommandSeq(action: Action) = lock gate (fun () -> extraCommandSeq[action])
        member _.ActiveTotal = lock gate (fun () -> activeTotal)
        member _.PeakActive = lock gate (fun () -> peakActive)
        member _.FifoViolations = lock gate (fun () -> fifoViolations)
        member _.FirstViolation = lock gate (fun () -> firstViolation)
        member _.ProtocolErrors = lock gate (fun () -> protocolErrors |> Seq.toList)
        member _.UnmatchedStarts =
            lock gate (fun () ->
                startedAttempts
                |> Seq.filter (fun id -> not (drainedAttempts.Contains id))
                |> Seq.toList)

open DemandReleaseSupport

[<TestFixture>]
type DemandReleaseTests() =
    [<Test>]
    member _.``100 x 1000 rapid cancellation burst enqueues Release synchronously before next caller Demand``() = task {
        let callerCount = 100
        let iterationsPerCaller = 1000
        let totalBurstDemands = callerCount * iterationsPerCaller
        let recorder = BurstRecorder(callerCount, iterationsPerCaller)
        let mutable holdEvaluation = 1

        let evaluator _ cancellation = async {
            if Volatile.Read(&holdEvaluation) = 1 then
                do! WorkCancellation.wait cancellation
                return StepOutcome.Complete Completion.Cancelled
            else
                return StepOutcome.Complete(Completion.Succeeded(ValueToken 777UL))
        }

        let settings: AsyncMailbox.Settings = {
            Epoch = epoch
            MaxConcurrency = 4
            CommandCapacity = 262_144
        }

        let handle =
            match AsyncMailbox.createWithStep settings evaluator recorder.Step with
            | Ok value -> value
            | Error err -> failwithf "Unexpected mailbox configuration error: %A" err

        match AsyncMailbox.start handle with
        | Ok () -> ()
        | Error err -> failwithf "Unexpected start error: %A" err

        let post action = task {
            match AsyncMailbox.admit action handle with
            | Error err -> return failwithf "Unexpected admit refusal: %A" err
            | Ok op ->
                let! receipt = AsyncMailbox.observe op |> run |> wait
                match receipt with
                | Ok value -> return value
                | Error err -> return failwithf "Unexpected command refusal: %A" err
        }

        do! withHealthyClose handle (fun () -> Volatile.Write(&holdEvaluation, 0)) (fun () -> task {
            let! _ = post (Action.ReserveScope(scope, RevisionId 1UL))
            let! _ = post (Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL]))

            let workers =
                [| for caller in 0 .. callerCount - 1 ->
                    Task.Run(Func<Task>(fun () -> task {
                        let baseId = caller * iterationsPerCaller
                        for iter in 0 .. iterationsPerCaller - 1 do
                            let rawId = uint64 (baseId + iter + 1)
                            use cts = new CancellationTokenSource()
                            let pending = AsyncMailbox.demandTask (DemandId rawId) work cts.Token handle
                            if (iter &&& 63) = 63 then
                                do! Task.Yield()
                            cts.Cancel()
                            try
                                let! _ = pending
                                failwith "Cancelled burst demand unexpectedly completed without cancellation."
                            with :? OperationCanceledException -> ()
                    })) |]

            do! joined (Task.WhenAll workers)
            let! idleOutcome = AsyncMailbox.waitForIdle handle |> run |> wait
            same (Ok ()) idleOutcome

            match recorder.FirstViolation with
            | Some detail -> Assert.Fail(detail)
            | None -> ()
            same 0 recorder.FifoViolations
            same [] recorder.ProtocolErrors
            same [] (AsyncMailbox.drainDiagnostics handle)
            same [] recorder.UnmatchedStarts
            same 0 recorder.ActiveTotal
            check (recorder.PeakActive <= callerCount) (sprintf "Peak active demands %d exceeded caller concurrency %d." recorder.PeakActive callerCount)

            for caller in 0 .. callerCount - 1 do
                let baseId = caller * iterationsPerCaller
                for iter in 0 .. iterationsPerCaller - 1 do
                    let currentId = baseId + iter + 1
                    let dSeq = recorder.DemandSeq currentId
                    let rSeq = recorder.ReleaseSeq currentId
                    check (dSeq > 0) (sprintf "Missing Demand sequence for %d." currentId)
                    check (rSeq > dSeq) (sprintf "Release(%d) step %d did not follow Demand(%d) step %d." currentId rSeq currentId dSeq)
                    same 1 (recorder.ReleaseCount currentId)
                    if iter + 1 < iterationsPerCaller then
                        let nextId = currentId + 1
                        let nextDemandSeq = recorder.DemandSeq nextId
                        check
                            (rSeq < nextDemandSeq)
                            (sprintf
                                "FIFO ordering violated for caller %d: Release(%d) at step %d arrived after Demand(%d) at step %d."
                                caller
                                currentId
                                rSeq
                                nextId
                                nextDemandSeq)

            let postBurstSnapshot = AsyncMailbox.snapshot handle
            same 0 postBurstSnapshot.Graph.PendingAttempts
            same 0 postBurstSnapshot.QueuedCommands
            for view in postBurstSnapshot.Graph.Works do
                same false view.Demanded

            // Verify terminal completion also enqueues Release synchronously prior to waking outward continuation.
            Volatile.Write(&holdEvaluation, 0)
            let terminalId = totalBurstDemands + 1
            let! terminalOutcome = wait (AsyncMailbox.demandTask (DemandId(uint64 terminalId)) work CancellationToken.None handle)
            let markerAction = Action.SetInputs [{ Input = InputId 99UL; Stamp = InputStamp 1UL; Value = ValueToken 1UL }]
            let! _ = post markerAction
            let! idleAfterTerminal = AsyncMailbox.waitForIdle handle |> run |> wait
            same (Ok ()) idleAfterTerminal

            match terminalOutcome with
            | Ok resultHandle -> same (ValueToken 777UL) resultHandle.Value
            | Error err -> failwithf "Expected eligible result after burst, got %A" err

            let terminalReleaseSeq = recorder.ReleaseSeq terminalId
            let markerSeq = recorder.CommandSeq markerAction
            check (terminalReleaseSeq > recorder.DemandSeq terminalId) "Terminal demand must enqueue Release."
            check
                (terminalReleaseSeq < markerSeq)
                (sprintf
                    "Terminal Release(%d) at step %d must precede post-continuation command at step %d."
                    terminalId
                    terminalReleaseSeq
                    markerSeq)
            same 1 (recorder.ReleaseCount terminalId)
        })
    }

    [<Test>]
    member _.``Host DemandAsync burst cancellation commits Release synchronously before next caller Demand``() = task {
        let callerCount = 25
        let iterationsPerCaller = 200
        let totalBurstDemands = callerCount * iterationsPerCaller
        let recorder = BurstRecorder(callerCount, iterationsPerCaller)
        let mutable holdEvaluation = 1

        let evaluator _ (token: CancellationToken) : Task<ValueToken> = task {
            if Volatile.Read(&holdEvaluation) = 1 then
                let tcs = TaskCompletionSource<ValueToken>(TaskCreationOptions.RunContinuationsAsynchronously)
                use _ = token.Register(Action(fun () -> tcs.TrySetCanceled(token) |> ignore))
                return! tcs.Task
            else
                return ValueToken 901UL
        }

        use host = new Host(epoch, 4, evaluator, recorder.Step)
        same (Ok []) (host.Send(Action.ReserveScope(scope, RevisionId 1UL)))
        same (Ok []) (host.Send(Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL])))

        let workers =
            [| for caller in 0 .. callerCount - 1 ->
                Task.Run(Func<Task>(fun () -> task {
                    let baseId = caller * iterationsPerCaller
                    for iter in 0 .. iterationsPerCaller - 1 do
                        let rawId = uint64 (baseId + iter + 1)
                        use cts = new CancellationTokenSource()
                        let pending = host.DemandAsync(DemandId rawId, work, cts.Token)
                        if (iter &&& 31) = 31 then
                            do! Task.Yield()
                        cts.Cancel()
                        try
                            let! _ = pending
                            failwith "Cancelled Host demand unexpectedly succeeded."
                        with :? OperationCanceledException -> ()
                })) |]

        do! joined (Task.WhenAll workers)
        do! joined (host.WaitForIdleAsync CancellationToken.None)

        match recorder.FirstViolation with
        | Some detail -> Assert.Fail(detail)
        | None -> ()
        same 0 recorder.FifoViolations
        same [] recorder.ProtocolErrors
        same [] (host.DrainDiagnostics())
        same [] recorder.UnmatchedStarts
        same 0 recorder.ActiveTotal

        for caller in 0 .. callerCount - 1 do
            let baseId = caller * iterationsPerCaller
            for iter in 0 .. iterationsPerCaller - 2 do
                let currentId = baseId + iter + 1
                let nextId = currentId + 1
                check
                    (recorder.ReleaseSeq currentId < recorder.DemandSeq nextId)
                    "Host Release must commit synchronously before the next caller Demand."

        Volatile.Write(&holdEvaluation, 0)
        let terminalId = totalBurstDemands + 1
        let! terminalOutcome = wait (host.DemandAsync(DemandId(uint64 terminalId), work))
        let markerAction = Action.SetInputs [{ Input = InputId 55UL; Stamp = InputStamp 1UL; Value = ValueToken 1UL }]
        match host.Send markerAction with
        | Ok _ -> ()
        | Error err -> failwithf "Unexpected Host marker error: %A" err

        match terminalOutcome with
        | Ok handle -> same (ValueToken 901UL) handle.Value
        | Error err -> failwithf "Unexpected Host terminal demand error: %A" err

        check (recorder.ReleaseSeq terminalId < recorder.CommandSeq markerAction) "Host terminal Release must precede caller continuation command."
        for view in host.Snapshot.Works do
            same false view.Demanded
    }

    [<Test>]
    member _.``cancelling one observer releases only its DemandId while shared producer completes for another observer``() = task {
        // 1. MailboxHost / AsyncMailbox surface
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowFinish = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable cancelRequestedOnSharedAttempt = false
        let mutable mailboxEvaluations = 0

        use mailboxHost = new MailboxHost(epoch, 2, 32, fun _ token -> task {
            Interlocked.Increment(&mailboxEvaluations) |> ignore
            started.TrySetResult() |> ignore
            do! allowFinish.Task
            cancelRequestedOnSharedAttempt <- token.IsCancellationRequested
            return StepOutcome.Complete(Completion.Succeeded(ValueToken 42UL))
        })

        let! _ = wait (mailboxHost.PostAsync(Action.ReserveScope(scope, RevisionId 1UL)))
        let! _ = wait (mailboxHost.PostAsync(Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL])))

        use ctsA = new CancellationTokenSource()
        use ctsB = new CancellationTokenSource()
        let observerA = mailboxHost.DemandAsync(DemandId 1UL, work, ctsA.Token)
        let observerB = mailboxHost.DemandAsync(DemandId 2UL, work, ctsB.Token)

        do! joined started.Task
        ctsA.Cancel()
        do! expectCanceled observerA

        // Synchronous Release(1UL) was already enqueued before observerA woke; send a barrier command to flush it.
        let! _ = wait (mailboxHost.PostAsync(Action.SetInputs [{ Input = InputId 1UL; Stamp = InputStamp 1UL; Value = ValueToken 1UL }]))
        let midView = mailboxHost.Snapshot.Graph.Works |> List.exactlyOne
        same true midView.Demanded
        check (not observerB.IsCompleted) "Observer B must remain active while shared producer runs."

        allowFinish.TrySetResult() |> ignore
        let! outcomeB = wait observerB
        match outcomeB with
        | Ok handle -> same (ValueToken 42UL) handle.Value
        | Error err -> failwithf "Observer B failed unexpectedly: %A" err

        do! joined (mailboxHost.WaitForIdleAsync CancellationToken.None)
        same false cancelRequestedOnSharedAttempt
        same 1 mailboxEvaluations
        let endView = mailboxHost.Snapshot.Graph.Works |> List.exactlyOne
        same false endView.Demanded

        // 2. Host surface
        let hostStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let hostAllowFinish = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable hostCancelObserved = false
        let mutable hostEvaluations = 0

        use host = new Host(epoch, 2, fun _ token -> task {
            Interlocked.Increment(&hostEvaluations) |> ignore
            hostStarted.TrySetResult() |> ignore
            do! hostAllowFinish.Task
            hostCancelObserved <- token.IsCancellationRequested
            return ValueToken 84UL
        })

        same (Ok []) (host.Send(Action.ReserveScope(scope, RevisionId 1UL)))
        same (Ok []) (host.Send(Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL])))

        use hostCtsA = new CancellationTokenSource()
        let hostObserverA = host.DemandAsync(DemandId 10UL, work, hostCtsA.Token)
        let hostObserverB = host.DemandAsync(DemandId 20UL, work)

        do! joined hostStarted.Task
        hostCtsA.Cancel()
        do! expectCanceled hostObserverA

        let hostMidView = host.Snapshot.Works |> List.exactlyOne
        same true hostMidView.Demanded

        hostAllowFinish.TrySetResult() |> ignore
        let! hostOutcomeB = wait hostObserverB
        match hostOutcomeB with
        | Ok handle -> same (ValueToken 84UL) handle.Value
        | Error err -> failwithf "Host observer B failed unexpectedly: %A" err

        do! joined (host.WaitForIdleAsync CancellationToken.None)
        same false hostCancelObserved
        same 1 hostEvaluations
        let hostEndView = host.Snapshot.Works |> List.exactlyOne
        same false hostEndView.Demanded
    }

    [<Test>]
    member _.``simultaneous completion and cancellation race releases once and enforces DemandIdReused tombstones``() = task {
        let raceCount = 200
        let recorder = BurstRecorder(1, raceCount + 10)
        let gates = Array.init (raceCount + 1) (fun _ -> TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously))
        let mutable activeGate = 0

        let evaluator _ cancellation = async {
            let idx = Volatile.Read(&activeGate)
            let gateTask = gates[idx].Task
            let! completed = Async.AwaitTask(Task.WhenAny(gateTask, ClrInterop.toTask CancellationToken.None (WorkCancellation.wait cancellation)))
            if obj.ReferenceEquals(completed, gateTask) && not (WorkCancellation.isRequested cancellation) then
                return StepOutcome.Complete(Completion.Succeeded(ValueToken(uint64 idx)))
            else
                return StepOutcome.Complete Completion.Cancelled
        }

        let settings: AsyncMailbox.Settings = {
            Epoch = epoch
            MaxConcurrency = 2
            CommandCapacity = 1024
        }

        let handle =
            match AsyncMailbox.createWithStep settings evaluator recorder.Step with
            | Ok value -> value
            | Error err -> failwithf "Unexpected settings error: %A" err

        AsyncMailbox.start handle |> ignore

        let post action = task {
            match AsyncMailbox.admit action handle with
            | Error err -> return failwithf "Admit failed: %A" err
            | Ok op ->
                let! r = AsyncMailbox.observe op |> run |> wait
                return r
        }

        do! withHealthyClose handle (fun () -> for g in gates do g.TrySetResult() |> ignore) (fun () -> task {
            let! _ = post (Action.ReserveScope(scope, RevisionId 1UL))
            let! _ = post (Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL; define otherWork 1UL]))

            for i in 1 .. raceCount do
                Volatile.Write(&activeGate, i)
                // Bump input/scope only when previous result cached so each iteration exercises live race or cached hit cleanly.
                let rev = uint64 (i + 1)
                let! _ = post (Action.ReserveScope(scope, RevisionId rev))
                let! _ = post (Action.ReplaceScope(scope, RevisionId rev, [define work rev; define otherWork rev]))

                use cts = new CancellationTokenSource()
                let pending = AsyncMailbox.demandTask (DemandId(uint64 i)) work cts.Token handle
                let racer = Task.Run(fun () -> gates[i].TrySetResult() |> ignore)
                cts.Cancel()
                do! joined racer
                try
                    let! _ = wait pending
                    ()
                with :? OperationCanceledException -> ()
                let! _ = AsyncMailbox.waitForIdle handle |> run |> wait
                same 1 (recorder.ReleaseCount i)

            // Duplicate explicit Release on an already-released DemandId remains idempotent.
            let! duplicateRelease = post (Action.Release(DemandId 1UL))
            match duplicateRelease with
            | Ok _ -> ()
            | Error err -> failwithf "Expected idempotent duplicate Release, got %A" err

            // Reusing a tombstoned DemandId fails with exact ProtocolError.DemandIdReused.
            let! reusedTombstone = wait (AsyncMailbox.demandTask (DemandId 1UL) work CancellationToken.None handle)
            same
                (Error(MailboxDemandError.Mailbox(MailboxError.InvalidCommand(ProtocolError.DemandIdReused(DemandId 1UL)))))
                reusedTombstone

            // Conflicting reuse of an active DemandId on a different work item fails with DemandIdReused
            // without releasing the original holder's active DemandId.
            Volatile.Write(&activeGate, 0)
            use activeHolderCts = new CancellationTokenSource()
            let activeId = DemandId 9000UL
            let holderTask = AsyncMailbox.demandTask activeId work activeHolderCts.Token handle
            // Flush admission so activeId is registered in Core.State.Demands.
            let! _ = post (Action.SetInputs [{ Input = InputId 77UL; Stamp = InputStamp 1UL; Value = ValueToken 1UL }])
            let! conflictingReuse = wait (AsyncMailbox.demandTask activeId otherWork CancellationToken.None handle)
            same
                (Error(MailboxDemandError.Mailbox(MailboxError.InvalidCommand(ProtocolError.DemandIdReused activeId))))
                conflictingReuse
            let workView = (AsyncMailbox.snapshot handle).Graph.Works |> List.find (fun v -> v.Work = work)
            same true workView.Demanded

            gates[0].TrySetResult() |> ignore
            let! holderOutcome = wait holderTask
            match holderOutcome with
            | Ok _ -> ()
            | Error err -> failwithf "Original holder failed after conflicting reuse refusal: %A" err

            // UnknownWork and pre-cancelled token checks.
            let! unknownOutcome = wait (AsyncMailbox.demandTask (DemandId 9001UL) (WorkId 999UL) CancellationToken.None handle)
            same
                (Error(MailboxDemandError.Mailbox(MailboxError.InvalidCommand(ProtocolError.UnknownWork(WorkId 999UL)))))
                unknownOutcome

            use preCancelled = new CancellationTokenSource()
            preCancelled.Cancel()
            let preCancelledId = raceCount + 5
            do! expectCanceled (AsyncMailbox.demandTask (DemandId(uint64 preCancelledId)) work preCancelled.Token handle)
            same 0 (recorder.DemandSeq preCancelledId)
            same 0 (recorder.ReleaseCount preCancelledId)

            same [] (AsyncMailbox.drainDiagnostics handle)
        })
    }

    [<Test>]
    member _.``tight CommandCapacity saturation never drops latched Release and observer faults still enqueue Release before TrySetException``() = task {
        let holdEvaluator = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let recorder = BurstRecorder(1, 10)

        let evaluator _ cancellation = async {
            let! _ = Async.AwaitTask(Task.WhenAny(holdEvaluator.Task, ClrInterop.toTask CancellationToken.None (WorkCancellation.wait cancellation)))
            if WorkCancellation.isRequested cancellation then
                return StepOutcome.Complete Completion.Cancelled
            else
                return StepOutcome.Complete(Completion.Succeeded(ValueToken 55UL))
        }

        // CommandCapacity = 1: cancelling immediately after admission must still enqueue Release without QueueFull.
        let settings: AsyncMailbox.Settings = {
            Epoch = epoch
            MaxConcurrency = 1
            CommandCapacity = 1
        }

        let handle =
            match AsyncMailbox.createWithStep settings evaluator recorder.Step with
            | Ok value -> value
            | Error err -> failwithf "Unexpected error: %A" err

        AsyncMailbox.start handle |> ignore

        let sendOne action = task {
            let mutable sent = false
            while not sent do
                match AsyncMailbox.admit action handle with
                | Ok op ->
                    let! r = AsyncMailbox.observe op |> run |> wait
                    match r with
                    | Ok _ -> sent <- true
                    | Error err -> failwithf "Unexpected command error: %A" err
                | Error MailboxError.QueueFull -> do! Task.Delay 1
                | Error err -> failwithf "Unexpected admit error: %A" err
        }

        do! withHealthyClose handle (fun () -> holdEvaluator.TrySetResult() |> ignore) (fun () -> task {
            do! sendOne (Action.ReserveScope(scope, RevisionId 1UL))
            do! sendOne (Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL]))

            use cts = new CancellationTokenSource()
            let scoped = AsyncMailbox.demandTask (DemandId 1UL) work cts.Token handle
            cts.Cancel()
            do! expectCanceled scoped
            let! idle1 = AsyncMailbox.waitForIdle handle |> run |> wait
            same (Ok ()) idle1
            same 1 (recorder.ReleaseCount 1)
            for view in (AsyncMailbox.snapshot handle).Graph.Works do
                same false view.Demanded

            // Observer exception in withDemandTask must synchronously enqueue Release prior to TrySetException.
            let faultyObserver =
                AsyncMailbox.withDemandTask
                    (DemandId 2UL)
                    work
                    (fun _ -> async { return invalidOp "boom-observer" })
                    CancellationToken.None
                    handle

            let mutable caughtObserverFault = false
            try
                let! _ = wait faultyObserver
                ()
            with
            | :? InvalidOperationException as ex when ex.Message = "boom-observer" ->
                caughtObserverFault <- true
            check caughtObserverFault "Expected observer exception to propagate through TrySetException."

            let marker = Action.SetInputs [{ Input = InputId 10UL; Stamp = InputStamp 1UL; Value = ValueToken 1UL }]
            do! sendOne marker
            let! idle2 = AsyncMailbox.waitForIdle handle |> run |> wait
            same (Ok ()) idle2
            check (recorder.ReleaseSeq 2 < recorder.CommandSeq marker) "Release on observer fault must precede post-exception caller command."

            // Functional Async surface (AsyncMailbox.demand) and ClrInterop.observeWithRelease check.
            holdEvaluator.TrySetResult() |> ignore
            let! asyncOutcome = AsyncMailbox.demand (DemandId 3UL) work handle |> run |> wait
            match asyncOutcome with
            | Ok resultHandle -> same (ValueToken 55UL) resultHandle.Value
            | Error err -> failwithf "Expected AsyncMailbox.demand success, got %A" err

            let orderTrace = ResizeArray<string>()
            use observeCts = new CancellationTokenSource()
            let neverWorkflow = Async.FromContinuations<int>(fun _ -> ())
            let observedTask =
                ClrInterop.observeWithRelease
                    (fun () -> lock orderTrace (fun () -> orderTrace.Add "released"))
                    observeCts.Token
                    neverWorkflow
            observeCts.Cancel()
            do! expectCanceled observedTask
            lock orderTrace (fun () -> orderTrace.Add "after-await")
            same ["released"; "after-await"] (lock orderTrace (fun () -> orderTrace |> Seq.toList))
        })
    }

    [<Test>]
    member _.``cancelling or faulting an admitted scoped demand after coordinator fault surfaces MailboxReleaseException``() = task {
        let holdEvaluator = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let holdCancelObserver = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
        let triggerObserverEx = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let cancelObserverEntered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let faultObserverEntered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let poisonInput = InputId 404UL

        let faultyStep (command: Command) (state: Core.State) =
            match command.Action with
            | Action.SetInputs [{ Input = id }] when id = poisonInput ->
                invalidOp "injected-coordinator-fault"
            | _ ->
                Core.step command state

        let evaluator _ cancellation = async {
            let! _ = Async.AwaitTask(Task.WhenAny(holdEvaluator.Task, ClrInterop.toTask CancellationToken.None (WorkCancellation.wait cancellation)))
            return StepOutcome.Complete Completion.Cancelled
        }

        let settings: AsyncMailbox.Settings = {
            Epoch = epoch
            MaxConcurrency = 1
            CommandCapacity = 16
        }

        let handle =
            match AsyncMailbox.createWithStep settings evaluator faultyStep with
            | Ok value -> value
            | Error err -> failwithf "Unexpected settings error: %A" err

        AsyncMailbox.start handle |> ignore

        let post action = task {
            match AsyncMailbox.admit action handle with
            | Error err -> return failwithf "Unexpected admit refusal: %A" err
            | Ok op -> return! AsyncMailbox.observe op |> run |> wait
        }

        try
            let! _ = post (Action.ReserveScope(scope, RevisionId 1UL))
            let! _ = post (Action.ReplaceScope(scope, RevisionId 1UL, [define work 1UL]))

            use ctsCancel = new CancellationTokenSource()
            let cancelDemandId = DemandId 101UL
            let faultDemandId = DemandId 102UL

            let pendingCancel =
                AsyncMailbox.withDemandTask
                    cancelDemandId
                    work
                    (fun _ -> async {
                        cancelObserverEntered.TrySetResult() |> ignore
                        return! Async.AwaitTask holdCancelObserver.Task
                    })
                    ctsCancel.Token
                    handle

            let pendingObserverFault =
                AsyncMailbox.withDemandTask
                    faultDemandId
                    work
                    (fun _ -> async {
                        faultObserverEntered.TrySetResult() |> ignore
                        do! Async.AwaitTask triggerObserverEx.Task
                        return invalidOp "observer-boom"
                    })
                    CancellationToken.None
                    handle

            do! joined cancelObserverEntered.Task
            do! joined faultObserverEntered.Task

            // Fault the coordinator via the injected step exception.
            let expectedFault: Failure = { Code = "host-protocol"; Message = "injected-coordinator-fault" }
            let! poisoned = post (Action.SetInputs [{ Input = poisonInput; Stamp = InputStamp 1UL; Value = ValueToken 1UL }])
            same (Error(MailboxError.Faulted expectedFault)) poisoned

            // 1. Cancelling an admitted scoped demand when enqueueRelease returns Faulted
            // must complete the outward Task with MailboxReleaseException carrying the MailboxError.
            ctsCancel.Cancel()
            let mutable cancelReleaseError: (DemandId * MailboxError) option = None
            try
                let! _ = wait pendingCancel
                ()
            with
            | :? MailboxReleaseException as ex ->
                cancelReleaseError <- Some(ex.DemandId, ex.MailboxError)
            same (Some(cancelDemandId, MailboxError.Faulted expectedFault)) cancelReleaseError

            // 2. Observer failure when enqueueRelease also returns Faulted must aggregate
            // both the observer failure and MailboxReleaseException.
            triggerObserverEx.TrySetResult() |> ignore
            let mutable aggregatedCauses: exn list = []
            try
                let! _ = wait pendingObserverFault
                ()
            with
            | :? AggregateException as agg ->
                aggregatedCauses <- agg.InnerExceptions |> Seq.toList
            same 2 aggregatedCauses.Length
            match aggregatedCauses with
            | [ :? InvalidOperationException as obsEx; :? MailboxReleaseException as relEx ] ->
                same "observer-boom" obsEx.Message
                same faultDemandId relEx.DemandId
                same (MailboxError.Faulted expectedFault) relEx.MailboxError
            | other ->
                failwithf "Unexpected aggregate causes: %A" other
        finally
            holdEvaluator.TrySetResult() |> ignore
            holdCancelObserver.TrySetResult 0 |> ignore
            triggerObserverEx.TrySetResult() |> ignore

        let! closed = AsyncMailbox.close handle |> run |> wait
        same (Error { Code = "host-protocol"; Message = "injected-coordinator-fault" }) closed
    }
