namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module private HostFaultTestSupport =
    let timeout = TimeSpan.FromSeconds 5.
    let completion<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
    let wait (work: Task<'T>) = work.WaitAsync timeout
    let joined (work: Task) = work.WaitAsync timeout
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)
    let evaluatorFailure = "held evaluator cleanup failed"
    let callbackFailure = "held cancellation callback failed"

    let observe (work: Task) = task {
        try
            do! joined work
            return None
        with error -> return Some error
    }

    let send (host: Host) action =
        match host.Send action with
        | Ok _ -> ()
        | Error error -> failwithf "Unexpected command refusal: %A" error

    let define id = ScopeEntry.Define { Work = WorkId id; Stamp = DefinitionStamp 1UL; Reads = [] }

    // Poll for an observed host transition, never to authorize release of an
    // ownership barrier. Retain every diagnostic for the final failure checks.
    let diagnosticWhere (host: Host) (seen: ResizeArray<HostDiagnostic>) predicate = task {
        use deadline = new CancellationTokenSource(timeout)
        seen.AddRange(host.DrainDiagnostics())
        while not (seen |> Seq.exists predicate) do
            do! Task.Delay(1, deadline.Token)
            seen.AddRange(host.DrainDiagnostics())
    }

    // Registration deliberately outlives the evaluator task. A use-binding in
    // the evaluator would join the callback there and collapse the two barriers.
    type Gates(failing: bool) =
        let firstEntered = completion<StartRequest>()
        let secondEntered = completion<StartRequest>()
        let firstRelease = completion<ValueToken>()
        let secondRelease = completion<unit>()
        let secondTask = completion<Task<ValueToken>>()
        let callbackEntered = completion<unit>()
        let callbackRelease = completion<unit>()
        let callbackExited = completion<unit>()
        let mutable registration = Unchecked.defaultof<CancellationTokenRegistration>
        let mutable queuedCalls = 0

        member _.FirstEntered = firstEntered.Task
        member _.SecondEntered = secondEntered.Task
        member _.SecondTask = secondTask.Task
        member _.CallbackEntered = callbackEntered.Task
        member _.CallbackExited = callbackExited.Task
        member _.QueuedCalls = Volatile.Read(&queuedCalls)
        member _.ReleaseFirst() = firstRelease.TrySetResult(ValueToken 101UL) |> ignore
        member _.ReleaseSecond() = secondRelease.TrySetResult() |> ignore
        member _.ReleaseCallback() = callbackRelease.TrySetResult() |> ignore
        member _.DisposeRegistration() = registration.Dispose()

        member _.Evaluate(request: StartRequest, token: CancellationToken) : Task<ValueToken> =
            match request.Definition.Work with
            | WorkId 99UL -> Task.FromResult(ValueToken 999UL)
            | WorkId 1UL ->
                firstEntered.TrySetResult request |> ignore
                firstRelease.Task
            | WorkId 2UL ->
                registration <- token.Register(fun () ->
                    callbackEntered.TrySetResult() |> ignore
                    try
                        callbackRelease.Task.GetAwaiter().GetResult()
                        if failing then raise (InvalidOperationException callbackFailure)
                    finally callbackExited.TrySetResult() |> ignore)
                let work = task {
                    do! secondRelease.Task
                    if failing then return raise (InvalidOperationException evaluatorFailure)
                    else return ValueToken 202UL
                }
                secondTask.TrySetResult work |> ignore
                secondEntered.TrySetResult request |> ignore
                work
            | WorkId 3UL ->
                Interlocked.Increment(&queuedCalls) |> ignore
                Task.FromResult(ValueToken 303UL)
            | work -> Task.FromException<ValueToken>(InvalidOperationException $"Unexpected work {work}")

    let prepare (host: Host) (gates: Gates) includeQueued = task {
        send host (Action.ReserveScope(ScopeId 1UL, RevisionId 1UL))
        send host (Action.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [define 99UL; define 1UL; define 2UL; define 3UL]))
        send host (Action.Demand(DemandId 99UL, WorkId 99UL))
        do! joined (host.WaitForIdleAsync CancellationToken.None)
        let seed = host.TryResult(WorkId 99UL) |> Option.defaultWith (fun () -> failwith "Missing seed result")
        check (host.IsEligible seed) "The protected handle must actually be eligible before the fault."
        host.DrainEvents() |> ignore
        send host (Action.Demand(DemandId 1UL, WorkId 1UL))
        let! first = wait gates.FirstEntered
        send host (Action.Demand(DemandId 2UL, WorkId 2UL))
        let! _ = wait gates.SecondEntered
        let! secondTask = wait gates.SecondTask
        if includeQueued then send host (Action.Demand(DemandId 3UL, WorkId 3UL))
        return seed, first.Attempt, secondTask
    }

    let protectedQueries (host: Host) seed =
        Assert.That(host.TryResult(WorkId 99UL), Is.EqualTo(None: ResultHandle option))
        check (not (host.IsEligible seed)) "The exact previously eligible seed handle must stay revoked."
        let error = Assert.Throws<InvalidOperationException>(fun () -> send host (Action.SetInputs []))
        Assert.That(error.Message, Is.EqualTo "Host protocol is faulted.")

    let cleanup (host: Host) (gates: Gates) = task {
        gates.ReleaseFirst()
        gates.ReleaseSecond()
        gates.ReleaseCallback()
        let! _ = observe (host.CloseAsync())
        // An assertion may fail before close becomes a valid join boundary.
        // Release and dispose the test-owned registration independently as well.
        gates.DisposeRegistration()
    }

open HostFaultTestSupport

[<TestFixture>]
type HostFaultTests() =
    [<TestCase("Finished", false)>]
    [<TestCase("Finished", true)>]
    [<TestCase("Drained", false)>]
    [<TestCase("Drained", true)>]
    member _.``protocol fault joins evaluator and cancellation callback before reporting aggregate failure``(acknowledgement: string, callbackFirst: bool) = task {
        let gates = Gates(true)
        let mutable faultAttempt = None
        let mutable injected = false
        let protocolStep (command: Command) state =
            let command =
                match command.Action with
                | Action.Finished(attempt, outcome) when acknowledgement = "Finished" && faultAttempt = Some attempt && not injected ->
                    injected <- true
                    { command with Action = Action.Finished(AttemptId UInt64.MaxValue, outcome) }
                | Action.Drained attempt when acknowledgement = "Drained" && faultAttempt = Some attempt && not injected ->
                    injected <- true
                    { command with Action = Action.Drained(AttemptId UInt64.MaxValue) }
                | _ -> command
            Core.step command state
        let host = new Host(EpochId 1UL, 2, (fun request token -> gates.Evaluate(request, token)), protocolStep)
        let diagnostics = ResizeArray<HostDiagnostic>()
        let mutable testFailure = None
        try
            let! seed, firstAttempt, secondTask = prepare host gates false
            faultAttempt <- Some firstAttempt
            gates.ReleaseFirst()
            do! diagnosticWhere host diagnostics (fun item ->
                item.Attempt = firstAttempt && item.Failure.Code = "host-protocol" && item.Failure.Message.Contains "UnknownAttempt")
            check injected "The fault must come from the actual Core.step rejection."
            protectedQueries host seed

            let closing = host.CloseAsync()
            do! wait gates.CallbackEntered
            let idle = host.WaitForIdleAsync CancellationToken.None
            check (obj.ReferenceEquals(closing, host.CloseAsync())) "Repeated close must share its ownership join."
            check (not secondTask.IsCompleted) "The evaluator barrier must still be held."
            check (not gates.CallbackExited.IsCompleted) "The callback barrier must still be held."
            check (not closing.IsCompleted) "Close reported the fault while both ownership barriers were held."
            check (not idle.IsCompleted) "Idle reported the fault before physical cleanup joined."

            if callbackFirst then
                gates.ReleaseCallback()
                do! wait gates.CallbackExited
                do! diagnosticWhere host diagnostics (fun item -> item.Failure.Code = "cancellation-callback")
                check (not secondTask.IsCompleted) "Callback completion must not release the evaluator barrier."
                check (not closing.IsCompleted) "Close completed while the evaluator was still owned."
                check (not idle.IsCompleted) "Idle completed while the evaluator was still owned."
                gates.ReleaseSecond()
            else
                gates.ReleaseSecond()
                let! evaluatorOutcome = observe secondTask
                check (evaluatorOutcome |> Option.exists (fun error -> error.Message = evaluatorFailure)) "Expected the held evaluator's exact failure."
                do! diagnosticWhere host diagnostics (fun item -> item.Failure.Code = "evaluation" && item.Failure.Message = evaluatorFailure)
                check (not gates.CallbackExited.IsCompleted) "Evaluator completion must not release the callback barrier."
                check (not closing.IsCompleted) "Close completed while the cancellation callback was still owned."
                check (not idle.IsCompleted) "Idle completed while the cancellation callback was still owned."
                gates.ReleaseCallback()

            do! wait gates.CallbackExited
            let! closeOutcome = observe closing
            let! idleOutcome = observe idle
            check secondTask.IsCompleted "Close must include evaluator completion."
            check closing.IsFaulted "The failed protocol must never be reported as a successful close."
            check idle.IsFaulted "The failed protocol must never be reported as successful idle."
            for outcome in [closeOutcome; idleOutcome] do
                let error = outcome |> Option.defaultWith (fun () -> failwith "Missing protocol failure")
                check (error :? AggregateException) "Join must report the aggregate failure after all cleanup."
                let details = error.ToString()
                check (details.Contains "UnknownAttempt" && details.Contains(string UInt64.MaxValue)) "The original invalid acknowledgement must remain visible."
                check (details.Contains evaluatorFailure) "Join must preserve the evaluator cleanup failure."
                check (details.Contains callbackFailure) "Join must preserve the cancellation callback failure."
            check (obj.ReferenceEquals(closing, host.CloseAsync())) "Completed repeated close must return the same task."
            protectedQueries host seed
            Assert.That(host.TryResult(WorkId 1UL), Is.EqualTo(None: ResultHandle option))
            Assert.That(host.TryResult(WorkId 2UL), Is.EqualTo(None: ResultHandle option))
            Assert.That(host.TryResult(WorkId 3UL), Is.EqualTo(None: ResultHandle option))
            check (host.Snapshot.PendingAttempts > 0) "Physical cleanup must not fabricate core Drained acknowledgements after disagreement."
            check (host.DrainEvents() |> List.forall (fun item ->
                match item.Action with
                | EffectAction.Offer _ | EffectAction.EpochDrained -> false
                | _ -> true)) "A faulted host must not fabricate success or epoch-drained receipts."
        with error -> testFailure <- Some error
        do! cleanup host gates
        testFailure |> Option.iter raise
    }

    [<Test>]
    member _.``rejected retirement returns one close task that joins evaluator and callback before faulting``() = task {
        let gates = Gates(false)
        let mutable injected = false
        let protocolStep (command: Command) state =
            let command =
                match command.Action with
                | Action.Retire ->
                    injected <- true
                    { command with Epoch = EpochId 2UL }
                | _ -> command
            Core.step command state
        let host = new Host(EpochId 1UL, 2, (fun request token -> gates.Evaluate(request, token)), protocolStep)
        let mutable testFailure = None
        try
            let! seed, _, secondTask = prepare host gates false
            let mutable closing = Task.CompletedTask
            Assert.DoesNotThrow(fun () -> closing <- host.CloseAsync())
            check injected "Retirement must be refused by the real Core.step ForeignEpoch check."
            check (obj.ReferenceEquals(closing, host.CloseAsync())) "Rejected retirement must retain a stable close operation."
            protectedQueries host seed
            do! wait gates.CallbackEntered
            check (not secondTask.IsCompleted) "The evaluator barrier must still be held."
            check (not gates.CallbackExited.IsCompleted) "The callback barrier must still be held."
            check (not closing.IsCompleted) "Rejected retirement must join both ownership barriers."

            gates.ReleaseFirst()
            gates.ReleaseSecond()
            let! _ = wait secondTask
            check (not gates.CallbackExited.IsCompleted) "Evaluator completion must leave the callback independently held."
            check (not closing.IsCompleted) "Rejected retirement must still join the cancellation callback."
            gates.ReleaseCallback()
            do! wait gates.CallbackExited
            let! closeOutcome = observe closing
            check closing.IsFaulted "A rejected retirement must not become a successful close."
            let error = closeOutcome |> Option.defaultWith (fun () -> failwith "Missing retirement failure")
            check (error :? AggregateException) "Retirement failure must surface after cleanup as an aggregate."
            check (error.ToString().Contains "ForeignEpoch") "The real retirement refusal must remain visible."
            check (obj.ReferenceEquals(closing, host.CloseAsync())) "Repeated close after failure must retain the same task."
            protectedQueries host seed
            check (host.Snapshot.PendingAttempts > 0) "Failed retirement must not fabricate core Drained acknowledgements."
            check (host.DrainEvents() |> List.forall (fun item ->
                match item.Action with
                | EffectAction.Offer _ | EffectAction.EpochDrained -> false
                | _ -> true)) "Rejected retirement must not fabricate successful results or epoch-drained receipts."
        with error -> testFailure <- Some error
        do! cleanup host gates
        testFailure |> Option.iter raise
    }

    [<Test>]
    member _.``protocol fault cancels owned work and stops queued admission before close``() = task {
        let gates = Gates(false)
        let mutable faultAttempt = None
        let protocolStep (command: Command) state =
            let command =
                match command.Action with
                | Action.Finished(attempt, outcome) when faultAttempt = Some attempt ->
                    { command with Action = Action.Finished(AttemptId UInt64.MaxValue, outcome) }
                | _ -> command
            Core.step command state
        let host = new Host(EpochId 1UL, 2, (fun request token -> gates.Evaluate(request, token)), protocolStep)
        let diagnostics = ResizeArray<HostDiagnostic>()
        let mutable testFailure = None
        try
            let! seed, firstAttempt, secondTask = prepare host gates true
            faultAttempt <- Some firstAttempt
            gates.ReleaseFirst()
            do! diagnosticWhere host diagnostics (fun item -> item.Failure.Code = "host-protocol")
            protectedQueries host seed
            // No CloseAsync has been requested: the fault itself must cancel
            // remaining ownership and stop the already queued evaluator.
            do! wait gates.CallbackEntered
            check (not secondTask.IsCompleted) "Cancellation is a request; the evaluator remains owned."
            Assert.That(gates.QueuedCalls, Is.Zero)
        with error -> testFailure <- Some error
        do! cleanup host gates
        Assert.That(gates.QueuedCalls, Is.Zero)
        testFailure |> Option.iter raise
    }

    [<Test>]
    member _.``uninjected close joins separately held evaluator and cancellation callback``() = task {
        let gates = Gates(false)
        let host = new Host(EpochId 1UL, 2, fun request token -> gates.Evaluate(request, token))
        let mutable testFailure = None
        try
            let! seed, _, secondTask = prepare host gates false
            let closing = host.CloseAsync()
            do! wait gates.CallbackEntered
            check (not closing.IsCompleted) "Close must wait while evaluators and callback are held."
            gates.ReleaseFirst()
            gates.ReleaseSecond()
            let! _ = wait secondTask
            check (not gates.CallbackExited.IsCompleted) "The callback barrier must remain independent."
            check (not closing.IsCompleted) "Close must keep joining after evaluator completion."
            gates.ReleaseCallback()
            do! wait gates.CallbackExited
            do! joined closing
            check (obj.ReferenceEquals(closing, host.CloseAsync())) "Successful repeated close must return the same task."
            Assert.That(host.Snapshot.PendingAttempts, Is.Zero)
            Assert.That(host.TryResult(WorkId 99UL), Is.EqualTo(None: ResultHandle option))
            check (not (host.IsEligible seed)) "Retirement must revoke the exact seed handle."
            check (host.DrainEvents() |> List.exists (fun item -> item.Action = EffectAction.EpochDrained)) "The valid protocol should publish its real epoch-drained receipt."
        with error -> testFailure <- Some error
        do! cleanup host gates
        testFailure |> Option.iter raise
    }
