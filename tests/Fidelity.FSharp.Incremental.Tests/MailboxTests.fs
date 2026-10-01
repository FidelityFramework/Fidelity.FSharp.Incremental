namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module private MailboxTestSupport =
    let completion<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
    let wait (value: Task<'T>) = value.WaitAsync(TimeSpan.FromSeconds 5.)
    let joined (value: Task) = value.WaitAsync(TimeSpan.FromSeconds 5.)
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)
    let same expected actual = check (expected = actual) (sprintf "Expected %A, got %A" expected actual)
    let epoch = EpochId 1UL
    let scope = ScopeId 1UL
    let work = WorkId 1UL
    let other = WorkId 2UL
    let definition id stamp reads =
        ScopeEntry.Define { Work = id; Stamp = DefinitionStamp stamp; Reads = reads }
    let succeeded number = StepOutcome.Complete(Completion.Succeeded(ValueToken number))
    let post (host: MailboxHost) action = task {
        let! answer = wait (host.PostAsync action)
        match answer with
        | Ok receipt -> return receipt
        | Error error -> return failwithf "Unexpected mailbox refusal for %A: %A" action error
    }
    let send (host: MailboxHost) action = task {
        let! _ = post host action
        return ()
    }
    let register (host: MailboxHost) owner entries = task {
        do! send host (Action.ReserveScope(owner, RevisionId 1UL))
        do! send host (Action.ReplaceScope(owner, RevisionId 1UL, entries))
    }
    let idle (host: MailboxHost) = joined (host.WaitForIdleAsync CancellationToken.None)
    // Poll only for an observable state transition. Time never authorizes an
    // ordering assertion: those use evaluator/cleanup barriers or command acks.
    let snapshotWhere (host: MailboxHost) predicate = task {
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 5.)
        let mutable snapshot = host.Snapshot
        while not (predicate snapshot) do
            do! Task.Delay(1, timeout.Token)
            snapshot <- host.Snapshot
        return snapshot
    }
    let suspension (host: MailboxHost) = task {
        let! snapshot = snapshotWhere host (fun value -> value.Suspensions.Length = 1)
        return List.exactlyOne snapshot.Suspensions
    }
    let noResult (host: MailboxHost) id = same (None: ResultHandle option) (host.TryResult id)
    let result (host: MailboxHost) id =
        host.TryResult id |> Option.defaultWith (fun () -> failwithf "Missing result for %A" id)
    let offered (host: MailboxHost) =
        host.DrainEvents()
        |> List.choose (fun effect -> match effect.Action with EffectAction.Offer value -> Some value | _ -> None)

open MailboxTestSupport

[<TestFixture>]
type MailboxTests() =
    [<Test>]
    member _.``reservation acknowledgement withdraws authority before the caller writes``() = task {
        let mutable source = 10UL
        let mutable calls = 0
        use host = new MailboxHost(epoch, 1, 8, fun _ _ ->
            Interlocked.Increment(&calls) |> ignore
            Task.FromResult(succeeded source))
        let! reserved = post host (Action.ReserveScope(scope, RevisionId 1UL))
        let! defined = post host (Action.ReplaceScope(scope, RevisionId 1UL, [definition work 1UL []]))
        same 0 calls
        check (reserved.Order < defined.Order) "Acknowledgements must preserve accepted order."
        do! send host (Action.Demand(DemandId 1UL, work))
        do! idle host
        let original = result host work
        let! edit = post host (Action.ReserveScope(scope, RevisionId 2UL))
        check (defined.Order < edit.Order) "The later reservation must have a later order."
        check (not (host.IsEligible original)) "An acknowledged edit must already withdraw the old result."
        noResult host work
        check (edit.Effects |> List.exists (fun item -> item.Action = EffectAction.Withdraw original)) "Receipt must include the actual withdrawal."
        // Simulate the external source write only after successful reservation.
        source <- 20UL
        do! send host (Action.ReplaceScope(scope, RevisionId 2UL, [definition work 2UL []]))
        do! idle host
        same (ValueToken 20UL) (result host work).Value
        same 2 calls
    }

    [<Test>]
    member _.``bounded burst preserves FIFO and only accepted input updates take effect``() = task {
        let input = InputId 1UL
        let read = { Slot = ReadSlotId 1UL; Source = ReadSource.Input input }
        use host = new MailboxHost(epoch, 1, 1, fun invocation _ ->
            match invocation with
            | StepInvocation.Start request ->
                match request.Reads with
                | [{ Value = ReadValue.Input(_, _, value) }] -> Task.FromResult(StepOutcome.Complete(Completion.Succeeded value))
                | values -> failwithf "Unexpected reads: %A" values
            | StepInvocation.Resume _ -> failwith "No suspension in this fixture.")
        do! register host scope [definition work 1UL [read]]
        do! send host (Action.SetInputs [{ Input = input; Stamp = InputStamp 0UL; Value = ValueToken 0UL }])
        let pending =
            [| for index in 1UL .. 256UL do
                let posted = host.PostAsync(Action.SetInputs [{ Input = input; Stamp = InputStamp index; Value = ValueToken index }])
                check (host.Snapshot.QueuedCommands <= 1) "External admission exceeded its configured capacity."
                yield index, posted |]
        let! answers = pending |> Array.map snd |> Task.WhenAll |> wait
        let accepted =
            Array.zip (pending |> Array.map fst) answers
            |> Array.choose (fun (index, answer) ->
                match answer with
                | Ok receipt -> Some(index, receipt.Order)
                | Error MailboxError.QueueFull -> None
                | Error error -> failwithf "Unexpected burst refusal: %A" error)
        check (accepted.Length > 0) "At least the initially empty queue must accept a command."
        check (accepted |> Array.pairwise |> Array.forall (fun ((_, left), (_, right)) -> left < right)) "Accepted receipts reordered the submitted FIFO."
        same 0 host.Snapshot.QueuedCommands
        do! send host (Action.Demand(DemandId 1UL, work))
        do! idle host
        same (ValueToken(fst (Array.last accepted))) (result host work).Value
    }

    [<Test>]
    member _.``pre-cancelled posts are absent but cancelling an accepted acknowledgement does not roll back``() = task {
        use host = new MailboxHost(epoch, 1, 8, fun _ _ -> Task.FromResult(succeeded 1UL))
        use alreadyCancelled = new CancellationTokenSource()
        alreadyCancelled.Cancel()
        let mutable rejected = false
        try
            let! _ = host.PostAsync(Action.ReserveScope(scope, RevisionId 1UL), cancellationToken = alreadyCancelled.Token)
            ()
        with :? OperationCanceledException -> rejected <- true
        check rejected "Pre-cancelled admission must cancel without accepting a command."
        same [] host.Snapshot.Graph.Scopes
        use observer = new CancellationTokenSource()
        let accepted = host.PostAsync(Action.ReserveScope(scope, RevisionId 1UL), cancellationToken = observer.Token)
        // PostAsync has returned after immediate admission. Observer cancellation
        // can race its acknowledgement, but must never revoke that admission.
        observer.Cancel()
        try
            let! answer = wait accepted
            match answer with
            | Ok _ -> ()
            | Error error -> failwithf "Reservation was not accepted: %A" error
        with :? OperationCanceledException -> ()
        do! send host (Action.ReplaceScope(scope, RevisionId 1UL, [definition work 1UL []]))
        do! send host (Action.Demand(DemandId 1UL, work))
        do! idle host
        same (ValueToken 1UL) (result host work).Value
    }

    [<Test>]
    member _.``mailbox stays responsive and one detached demand does not cancel shared work``() = task {
        let started = completion<CancellationToken>()
        let release = completion<StepOutcome>()
        let mutable calls = 0
        use host = new MailboxHost(epoch, 1, 8, fun _ token ->
            Interlocked.Increment(&calls) |> ignore
            started.TrySetResult token |> ignore
            release.Task)
        try
            do! register host scope [definition work 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            let! token = wait started.Task
            do! send host (Action.Demand(DemandId 2UL, work))
            do! send host (Action.Release(DemandId 1UL))
            do! send host (Action.SetInputs [{ Input = InputId 9UL; Stamp = InputStamp 1UL; Value = ValueToken 9UL }])
            check (not release.Task.IsCompleted) "The evaluator barrier must still be held while commands are acknowledged."
            check (not token.IsCancellationRequested) "A surviving consumer still owns this shared attempt."
            same 1 host.Snapshot.RunningSteps
            release.TrySetResult(succeeded 42UL) |> ignore
            do! idle host
            same 1 calls
            same (ValueToken 42UL) (result host work).Value
        finally release.TrySetResult(succeeded 42UL) |> ignore
    }

    [<Test>]
    member _.``releasing queued work prevents its evaluator from starting``() = task {
        let started = completion<unit>()
        let release = completion<StepOutcome>()
        let mutable otherCalls = 0
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ ->
            match invocation with
            | StepInvocation.Start request when request.Definition.Work = work ->
                started.TrySetResult() |> ignore
                release.Task
            | _ ->
                Interlocked.Increment(&otherCalls) |> ignore
                Task.FromResult(succeeded 2UL))
        try
            do! register host scope [definition work 1UL []; definition other 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait started.Task
            do! send host (Action.Demand(DemandId 2UL, other))
            same 1 host.Snapshot.QueuedSteps
            do! send host (Action.Release(DemandId 2UL))
            release.TrySetResult(succeeded 1UL) |> ignore
            do! idle host
            same 0 otherCalls
            noResult host other
        finally release.TrySetResult(succeeded 1UL) |> ignore
    }

    [<Test>]
    member _.``physical concurrency counts running steps rather than demanded work``() = task {
        let entered = Array.init 3 (fun _ -> completion<unit>())
        let release = Array.init 3 (fun _ -> completion<StepOutcome>())
        let countGate = obj ()
        let mutable active = 0
        let mutable peak = 0
        use host = new MailboxHost(epoch, 2, 8, fun invocation _ -> task {
            let index =
                match invocation with
                | StepInvocation.Start request -> let (WorkId number) = request.Definition.Work in int number - 1
                | StepInvocation.Resume _ -> failwith "No suspension in this fixture."
            lock countGate (fun () -> active <- active + 1; peak <- max peak active)
            entered[index].TrySetResult() |> ignore
            try return! release[index].Task
            finally lock countGate (fun () -> active <- active - 1)
        })
        try
            do! register host scope [for number in 1UL .. 3UL -> definition (WorkId number) 1UL []]
            for number in 1UL .. 3UL do
                do! send host (Action.Demand(DemandId number, WorkId number))
            do! wait entered[0].Task
            do! wait entered[1].Task
            check (not entered[2].Task.IsCompleted) "A third step must remain queued while both slots are held."
            same 2 host.Snapshot.RunningSteps
            same 1 host.Snapshot.QueuedSteps
            release[0].TrySetResult(succeeded 1UL) |> ignore
            do! wait entered[2].Task
            release[1].TrySetResult(succeeded 2UL) |> ignore
            release[2].TrySetResult(succeeded 3UL) |> ignore
            do! idle host
            same 2 peak
            same 0 active
        finally
            for pending in release do pending.TrySetResult(succeeded 0UL) |> ignore
    }

    [<Test>]
    member _.``suspension joins cleanup then frees capacity without releasing logical ownership``() = task {
        let cleanupStarted = completion<unit>()
        let cleanupRelease = completion<unit>()
        let otherStarted = completion<unit>()
        let resumed = completion<ResumeRequest>()
        let mutable firstAttempt = None
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ -> task {
            match invocation with
            | StepInvocation.Start request when request.Definition.Work = work ->
                firstAttempt <- Some request.Attempt
                use owned =
                    { new IAsyncDisposable with
                        member _.DisposeAsync() =
                            cleanupStarted.TrySetResult() |> ignore
                            ValueTask(cleanupRelease.Task) }
                return StepOutcome.Suspend(StepId 7UL, ValueToken 70UL)
            | StepInvocation.Start _ ->
                otherStarted.TrySetResult() |> ignore
                return succeeded 2UL
            | StepInvocation.Resume request ->
                resumed.TrySetResult request |> ignore
                return succeeded 1UL
        })
        try
            do! register host scope [definition work 1UL []; definition other 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait cleanupStarted.Task
            do! send host (Action.Demand(DemandId 2UL, other))
            same [] host.Snapshot.Suspensions
            same 1 host.Snapshot.RunningSteps
            check (not otherStarted.Task.IsCompleted) "A pending Suspend cannot release capacity before its cleanup."
            cleanupRelease.TrySetResult() |> ignore
            do! wait otherStarted.Task
            let! parked = suspension host
            let! _ = snapshotWhere host (fun value -> value.Graph.PendingAttempts = 1 && value.RunningSteps = 0)
            same firstAttempt (Some parked.Attempt)
            same (ValueToken 70UL) parked.Environment
            noResult host work
            check (not (host.WaitForIdleAsync(CancellationToken.None).IsCompleted)) "A parked logical attempt is not idle."
            do! send host (Action.Resume(parked, ValueToken 700UL))
            let! request = wait resumed.Task
            same parked request.Suspension
            same (ValueToken 700UL) request.Response
            same parked.Attempt request.Start.Attempt
            do! idle host
            same (ValueToken 1UL) (result host work).Value
            same (ValueToken 2UL) (result host other).Value
        finally cleanupRelease.TrySetResult() |> ignore
    }

    [<Test>]
    member _.``altered or consumed resume handles never invoke the evaluator``() = task {
        let mutable resumes = 0
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ ->
            match invocation with
            | StepInvocation.Start _ -> Task.FromResult(StepOutcome.Suspend(StepId 4UL, ValueToken 40UL))
            | StepInvocation.Resume _ ->
                Interlocked.Increment(&resumes) |> ignore
                Task.FromResult(succeeded 4UL))
        do! register host scope [definition work 1UL []]
        do! send host (Action.Demand(DemandId 1UL, work))
        let! parked = suspension host
        for altered in [{ parked with Step = StepId 5UL }; { parked with Environment = ValueToken 41UL }] do
            let! answer = wait (host.PostAsync(Action.Resume(altered, ValueToken 0UL)))
            same (Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension parked.Id))) answer
            same [parked] host.Snapshot.Suspensions
            same 0 resumes
        do! send host (Action.Resume(parked, ValueToken 0UL))
        do! idle host
        let! replay = wait (host.PostAsync(Action.Resume(parked, ValueToken 0UL)))
        same (Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension parked.Id))) replay
        same 1 resumes
        same (ValueToken 4UL) (result host work).Value
    }

    [<Test>]
    member _.``observer cancellation while parked leaves the exact continuation resumable``() = task {
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ ->
            match invocation with
            | StepInvocation.Start _ -> Task.FromResult(StepOutcome.Suspend(StepId 1UL, ValueToken 10UL))
            | StepInvocation.Resume _ -> Task.FromResult(succeeded 10UL))
        do! register host scope [definition work 1UL []]
        do! send host (Action.Demand(DemandId 1UL, work))
        let! parked = suspension host
        use observer = new CancellationTokenSource()
        let waiting = host.WaitForIdleAsync observer.Token
        observer.Cancel()
        let mutable cancelled = false
        try do! joined waiting with :? OperationCanceledException -> cancelled <- true
        check cancelled "Only this wait observer should cancel."
        same 1 host.Snapshot.Graph.PendingAttempts
        same [parked] host.Snapshot.Suspensions
        do! send host (Action.Resume(parked, ValueToken 0UL))
        do! idle host
        same (ValueToken 10UL) (result host work).Value
    }

    [<Test>]
    member _.``close retires a parked attempt without invoking a resume and is idempotent``() = task {
        let mutable resumes = 0
        use host = new MailboxHost(epoch, 1, 1, fun invocation _ ->
            match invocation with
            | StepInvocation.Start _ -> Task.FromResult(StepOutcome.Suspend(StepId 1UL, ValueToken 1UL))
            | StepInvocation.Resume _ ->
                Interlocked.Increment(&resumes) |> ignore
                Task.FromResult(succeeded 1UL))
        do! register host scope [definition work 1UL []]
        do! send host (Action.Demand(DemandId 1UL, work))
        let! parked = suspension host
        let first = host.CloseAsync()
        let second = host.CloseAsync()
        do! joined first
        do! joined second
        same 0 resumes
        same 0 host.Snapshot.Graph.PendingAttempts
        same [] host.Snapshot.Suspensions
        noResult host work
        let! answer = wait (host.PostAsync(Action.Resume(parked, ValueToken 0UL)))
        same (Error MailboxError.Closed) answer
        same [] (offered host)
    }

    [<Test>]
    member _.``late suspension after reservation drains before the replacement starts``() = task {
        let started = completion<unit>()
        let release = completion<StepOutcome>()
        let replacementStarted = completion<unit>()
        let mutable resumes = 0
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ ->
            match invocation with
            | StepInvocation.Start request when request.Definition.Stamp = DefinitionStamp 1UL ->
                started.TrySetResult() |> ignore
                release.Task
            | StepInvocation.Start _ ->
                replacementStarted.TrySetResult() |> ignore
                Task.FromResult(succeeded 2UL)
            | StepInvocation.Resume _ ->
                Interlocked.Increment(&resumes) |> ignore
                Task.FromResult(succeeded 99UL))
        try
            do! register host scope [definition work 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait started.Task
            do! send host (Action.ReserveScope(scope, RevisionId 2UL))
            do! send host (Action.ReplaceScope(scope, RevisionId 2UL, [definition work 2UL []]))
            check (not replacementStarted.Task.IsCompleted) "The old physical step still owns its cleanup."
            same 1 host.Snapshot.Graph.PendingAttempts
            release.TrySetResult(StepOutcome.Suspend(StepId 1UL, ValueToken 1UL)) |> ignore
            do! wait replacementStarted.Task
            do! idle host
            same 0 resumes
            same [] host.Snapshot.Suspensions
            same [] (host.DrainDiagnostics())
            same [ValueToken 2UL] (offered host |> List.map (fun item -> item.Value))
        finally release.TrySetResult(StepOutcome.Suspend(StepId 1UL, ValueToken 1UL)) |> ignore
    }

    [<Test>]
    member _.``a queued resume invalidated before its slot is free never runs``() = task {
        let otherScope = ScopeId 2UL
        let otherStarted = completion<unit>()
        let otherRelease = completion<StepOutcome>()
        let mutable resumes = 0
        use host = new MailboxHost(epoch, 1, 8, fun invocation _ ->
            match invocation with
            | StepInvocation.Start request when request.Definition.Work = work ->
                Task.FromResult(StepOutcome.Suspend(StepId 1UL, ValueToken 10UL))
            | StepInvocation.Start _ ->
                otherStarted.TrySetResult() |> ignore
                otherRelease.Task
            | StepInvocation.Resume _ ->
                Interlocked.Increment(&resumes) |> ignore
                Task.FromResult(succeeded 10UL))
        try
            do! register host scope [definition work 1UL []]
            do! register host otherScope [definition other 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            let! parked = suspension host
            do! send host (Action.Demand(DemandId 2UL, other))
            do! wait otherStarted.Task
            do! send host (Action.Resume(parked, ValueToken 0UL))
            same 1 host.Snapshot.QueuedSteps
            do! send host (Action.ReserveScope(scope, RevisionId 2UL))
            otherRelease.TrySetResult(succeeded 2UL) |> ignore
            do! idle host
            same 0 resumes
            noResult host work
            same [] host.Snapshot.Suspensions
            let! repeated = wait (host.PostAsync(Action.Resume(parked, ValueToken 0UL)))
            same (Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension parked.Id))) repeated
        finally otherRelease.TrySetResult(succeeded 2UL) |> ignore
    }

    [<Test>]
    member _.``close waits for a token ignoring step and discards its late suspension``() = task {
        let started = completion<unit>()
        let release = completion<StepOutcome>()
        use host = new MailboxHost(epoch, 1, 8, fun _ _ ->
            started.TrySetResult() |> ignore
            release.Task)
        try
            do! register host scope [definition work 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait started.Task
            let closing = host.CloseAsync()
            check (not closing.IsCompleted) "Close must join the actual step rather than fabricate drain."
            release.TrySetResult(StepOutcome.Suspend(StepId 9UL, ValueToken 9UL)) |> ignore
            do! joined closing
            same 0 host.Snapshot.Graph.PendingAttempts
            same [] host.Snapshot.Suspensions
            same [] (host.DrainDiagnostics())
            same [] (offered host)
        finally release.TrySetResult(StepOutcome.Suspend(StepId 9UL, ValueToken 9UL)) |> ignore
    }

    [<Test>]
    member _.``throwing cleanup prevents a checkpoint and exposes evaluator failure``() = task {
        use host = new MailboxHost(epoch, 1, 8, fun _ _ -> task {
            use owned = { new IDisposable with member _.Dispose() = raise (InvalidOperationException "owned cleanup failed") }
            return StepOutcome.Suspend(StepId 1UL, ValueToken 1UL)
        })
        do! register host scope [definition work 1UL []]
        do! send host (Action.Demand(DemandId 1UL, work))
        do! idle host
        same [] host.Snapshot.Suspensions
        noResult host work
        match (host.Snapshot.Graph.Works |> List.exactlyOne).Status with
        | WorkStatus.Failed error ->
            same "evaluation" error.Code
            same "owned cleanup failed" error.Message
        | status -> failwithf "Cleanup failure was lost: %A" status
        same [] (offered host)
    }

    [<Test>]
    member _.``cancellation callbacks retain ownership and their failure is surfaced``() = task {
        let started = completion<unit>()
        let callbackStarted = completion<unit>()
        let callbackRelease = completion<unit>()
        let evaluatorRelease = completion<StepOutcome>()
        use host = new MailboxHost(epoch, 1, 8, fun _ token ->
            token.Register(fun () ->
                callbackStarted.TrySetResult() |> ignore
                callbackRelease.Task.GetAwaiter().GetResult()
                raise (InvalidOperationException "callback cleanup failed")) |> ignore
            started.TrySetResult() |> ignore
            evaluatorRelease.Task)
        try
            do! register host scope [definition work 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait started.Task
            let closing = host.CloseAsync()
            do! wait callbackStarted.Task
            evaluatorRelease.TrySetResult(succeeded 1UL) |> ignore
            check (not closing.IsCompleted) "An evaluator result cannot acknowledge pending cancellation callback cleanup."
            same 1 host.Snapshot.Graph.PendingAttempts
            callbackRelease.TrySetResult() |> ignore
            do! joined closing
            same 0 host.Snapshot.Graph.PendingAttempts
            let failures = host.DrainDiagnostics() |> List.map (fun item -> item.Failure)
            same ["cancellation-callback"] (failures |> List.map (fun item -> item.Code))
            check ((List.exactlyOne failures).Message.Contains "callback cleanup failed") "Cancellation callback exception was lost."
            same [] (offered host)
        finally
            evaluatorRelease.TrySetResult(succeeded 1UL) |> ignore
            callbackRelease.TrySetResult() |> ignore
    }

    [<Test>]
    member _.``callers cannot forge host owned lifecycle transitions``() = task {
        use host = new MailboxHost(epoch, 1, 8, fun _ _ -> Task.FromResult(succeeded 1UL))
        for action in [
            Action.Finished(AttemptId 1UL, Completion.Succeeded(ValueToken 1UL))
            Action.Drained(AttemptId 1UL)
            Action.Suspend(AttemptId 1UL, StepId 1UL, ValueToken 1UL)
        ] do
            let! answer = wait (host.PostAsync action)
            same (Error MailboxError.LifecycleOwnedByHost) answer
        same [] host.Snapshot.Graph.Scopes
        same 0 host.Snapshot.Graph.PendingAttempts
    }

    [<Test>]
    member _.``close resolves accepted FIFO commands and rejects later admission``() = task {
        use host = new MailboxHost(epoch, 1, 1, fun _ _ -> Task.FromResult(succeeded 1UL))
        let pending =
            [| for number in 1UL .. 64UL ->
                host.PostAsync(Action.ReserveScope(ScopeId number, RevisionId 1UL)) |]
        let closing = host.CloseAsync()
        let! answers = Task.WhenAll pending |> wait
        let accepted =
            answers |> Array.choose (function
                | Ok receipt -> Some receipt.Order
                | Error MailboxError.QueueFull -> None
                | Error error -> failwithf "Previously submitted command was not resolved: %A" error)
        check (accepted.Length > 0) "The empty mailbox must accept its first command."
        check (accepted |> Array.pairwise |> Array.forall (fun (left, right) -> left < right)) "Close reordered accepted commands."
        do! joined closing
        let! answer = wait (host.PostAsync(Action.ReserveScope(ScopeId 999UL, RevisionId 1UL)))
        same (Error MailboxError.Closed) answer
        same 0 host.Snapshot.QueuedCommands
        same 0 host.Snapshot.Graph.PendingAttempts
        check (host.Snapshot.Graph.Scopes |> List.forall (fun value -> value.Phase = ScopePhase.Closed)) "Accepted scopes must retire before close completes."
    }

    [<Test>]
    member _.``a completed step holds its physical slot until cancellation callbacks finish``() = task {
        let started = completion<unit>()
        let callbackStarted = completion<unit>()
        let callbackRelease = completion<unit>()
        let evaluatorRelease = completion<StepOutcome>()
        let otherStarted = completion<unit>()
        let returned: Failure = { Code = "step-returned"; Message = "Evaluator task has completed." }
        let mutable observed = None
        use host = new MailboxHost(epoch, 1, 8, fun invocation token ->
            match invocation with
            | StepInvocation.Start request when request.Definition.Work = work ->
                token.Register(fun () ->
                    callbackStarted.TrySetResult() |> ignore
                    callbackRelease.Task.GetAwaiter().GetResult()) |> ignore
                started.TrySetResult() |> ignore
                evaluatorRelease.Task
            | StepInvocation.Start _ ->
                otherStarted.TrySetResult() |> ignore
                Task.FromResult(succeeded 2UL)
            | StepInvocation.Resume _ -> failwith "No suspension in this fixture.")
        try
            do! register host scope [definition work 1UL []]
            do! register host (ScopeId 2UL) [definition other 1UL []]
            do! send host (Action.Demand(DemandId 1UL, work))
            do! wait started.Task
            do! send host (Action.Demand(DemandId 2UL, other))
            same 1 host.Snapshot.QueuedSteps
            do! send host (Action.ReserveScope(scope, RevisionId 2UL))
            do! wait callbackStarted.Task
            evaluatorRelease.TrySetResult(StepOutcome.Complete(Completion.Failed returned)) |> ignore
            let! _ = snapshotWhere host (fun _ ->
                host.DrainDiagnostics()
                |> List.iter (fun item -> if item.Failure.Code = returned.Code then observed <- Some item.Failure)
                observed.IsSome)
            same (Some returned) observed
            // The diagnostic establishes StepCompleted was processed; this FIFO
            // acknowledgement establishes that transition has fully published.
            do! send host (Action.SetInputs [])
            same 1 host.Snapshot.RunningSteps
            same 1 host.Snapshot.QueuedSteps
            check (not otherStarted.Task.IsCompleted) "The next evaluator cannot borrow a slot still held by cancellation cleanup."
            callbackRelease.TrySetResult() |> ignore
            do! wait otherStarted.Task
            do! idle host
            same 0 host.Snapshot.RunningSteps
            noResult host work
            same (ValueToken 2UL) (result host other).Value
        finally
            evaluatorRelease.TrySetResult(StepOutcome.Complete(Completion.Failed returned)) |> ignore
            callbackRelease.TrySetResult() |> ignore
    }
