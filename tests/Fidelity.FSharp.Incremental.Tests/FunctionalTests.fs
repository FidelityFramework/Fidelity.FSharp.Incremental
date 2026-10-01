namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module private FunctionalChecks =
    let completion<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
    let wait (pending: Task<'T>) = pending.WaitAsync(TimeSpan.FromSeconds 5.)
    let run workflow = Async.StartAsTask(workflow, cancellationToken = CancellationToken.None)
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)
    let same expected actual = check (expected = actual) (sprintf "Expected %A, got %A" expected actual)
    let epoch = EpochId 1UL
    let scope = ScopeId 1UL
    let work = WorkId 1UL
    let settings: AsyncMailbox.Settings = { Epoch = epoch; MaxConcurrency = 1; CommandCapacity = 8 }
    let define id = ScopeEntry.Define { Work = id; Stamp = DefinitionStamp 1UL; Reads = [] }
    let succeeded value = StepOutcome.Complete(Completion.Succeeded(ValueToken value))
    let get = function Ok value -> value | Error error -> failwithf "Unexpected refusal: %A" error
    let create evaluator = AsyncMailbox.create settings evaluator |> get
    let start handle = AsyncMailbox.start handle |> get
    let admit handle action = AsyncMailbox.admit action handle |> get
    let post handle action = task {
        let operation = admit handle action
        let! answer = AsyncMailbox.observe operation |> run |> wait
        let receipt = get answer
        same (AsyncMailbox.operationOrder operation) receipt.Order
        return receipt
    }
    let send handle action = task {
        let! _ = post handle action
        return ()
    }
    let register handle entries = task {
        do! send handle (Action.ReserveScope(scope, RevisionId 1UL))
        do! send handle (Action.ReplaceScope(scope, RevisionId 1UL, entries))
    }
    let idle handle = task {
        let! outcome = AsyncMailbox.waitForIdle handle |> run |> wait
        same (Ok ()) outcome
    }
    let result handle = AsyncMailbox.tryResult work handle |> Option.defaultWith (fun () -> failwith "Expected an eligible result.")
    // Timeouts bound a failed test. Observable barriers/acknowledgements, never
    // elapsed time, establish the ordering asserted by the tests below.
    let until predicate = task {
        use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 5.)
        while not (predicate ()) do
            do! Task.Delay(1, deadline.Token)
    }
    let cancelled (pending: Task<'T>) = task {
        let mutable observed = false
        try
            let! _ = wait pending
            ()
        with :? OperationCanceledException -> observed <- true
        check observed "The observer must receive cancellation."
    }
    // Release every test-owned barrier even when an assertion fails, then join
    // physical ownership before propagating that failure to NUnit.
    let protect handle release body = task {
        let mutable failure = None
        try do! body ()
        with error -> failure <- Some error
        release ()
        let! closed = AsyncMailbox.close handle |> run |> wait
        failure |> Option.iter raise
        return closed
    }
    let healthy handle release body = task {
        let! closed = protect handle release body
        same (Ok ()) closed
    }
    let noRelease () = ()

open FunctionalChecks

[<TestFixture>]
type FunctionalTests() =
    [<Test>]
    member _.``creation is cold and explicit start does not demand work``() = task {
        let mutable calls = 0
        let evaluator _ _ = async {
            Interlocked.Increment(&calls) |> ignore
            return succeeded 42UL
        }
        for invalid, expected in
            [ { settings with MaxConcurrency = 0 }, AsyncMailbox.ConfigurationError.InvalidConcurrency
              { settings with CommandCapacity = 0 }, AsyncMailbox.ConfigurationError.InvalidCommandCapacity ] do
            match AsyncMailbox.create invalid evaluator with
            | Error error -> same expected error
            | Ok _ -> failwith "Invalid settings were accepted."
        let handle = create evaluator
        do! healthy handle noRelease (fun () -> task {
            match AsyncMailbox.admit (Action.ReserveScope(scope, RevisionId 1UL)) handle with
            | Error error -> same MailboxError.NotStarted error
            | Ok _ -> failwith "A cold handle admitted a command."
            same [] (AsyncMailbox.snapshot handle).Graph.Scopes
            same 0 calls
            start handle
            start handle
            do! register handle [define work]
            same 0 calls
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! idle handle
            same 1 calls
            same (ValueToken 42UL) (result handle).Value
        })
    }

    [<Test>]
    member _.``closing a cold handle retains one join and never starts its evaluator``() = task {
        let mutable calls = 0
        let handle = create (fun _ _ -> async { Interlocked.Increment(&calls) |> ignore; return succeeded 1UL })
        let coldClose = AsyncMailbox.close handle
        check (not (AsyncMailbox.snapshot handle).IsClosing) "Constructing close must not start it."
        let operation = AsyncMailbox.beginClose handle
        check (obj.ReferenceEquals(operation, AsyncMailbox.beginClose handle)) "Close must retain one operation."
        let! first = AsyncMailbox.awaitClose operation |> run |> wait
        let! second = coldClose |> run |> wait
        same (Ok ()) first
        same first second
        same (Some first) (AsyncMailbox.tryClose operation)
        same (Error MailboxError.Closed) (AsyncMailbox.start handle)
        same 0 calls
        same 0 (AsyncMailbox.snapshot handle).Graph.PendingAttempts
    }

    [<Test>]
    member _.``one admitted operation survives cancelled observation and can be observed repeatedly``() = task {
        let entered = completion<unit> ()
        let release = completion<unit> ()
        let mutable transitions = 0
        let step (command: Command) state =
            match command.Action with
            | Action.ReserveScope _ ->
                Interlocked.Increment(&transitions) |> ignore
                entered.TrySetResult() |> ignore
                release.Task.GetAwaiter().GetResult()
            | _ -> ()
            Core.step command state
        let handle = AsyncMailbox.createWithStep settings (fun _ _ -> async { return succeeded 1UL }) step |> get
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            let operation = admit handle (Action.ReserveScope(scope, RevisionId 1UL))
            do! wait entered.Task
            same None (AsyncMailbox.tryObserve operation)
            use observer = new CancellationTokenSource()
            let detached = Async.StartAsTask(AsyncMailbox.observe operation, cancellationToken = observer.Token)
            observer.Cancel()
            do! cancelled detached
            same None (AsyncMailbox.tryObserve operation)
            release.TrySetResult() |> ignore
            let! first = AsyncMailbox.observe operation |> run |> wait
            let! second = AsyncMailbox.observe operation |> run |> wait
            same first second
            same (AsyncMailbox.operationOrder operation) (get first).Order
            same (Some first) (AsyncMailbox.tryObserve operation)
            same 1 transitions
            same [{ Scope = scope; Phase = ScopePhase.Reserved(RevisionId 1UL); PendingAttempts = 0 }]
                (AsyncMailbox.snapshot handle).Graph.Scopes
        })
    }

    [<Test>]
    member _.``a blocked synchronous evaluator prefix leaves the coordinator responsive``() = task {
        let entered = completion<unit> ()
        let release = completion<unit> ()
        let handle = create (fun _ _ ->
            // Deliberately before the Async value is returned by the evaluator.
            entered.TrySetResult() |> ignore
            release.Task.GetAwaiter().GetResult()
            async { return succeeded 7UL })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! wait entered.Task
            do! send handle (Action.ReserveScope(ScopeId 2UL, RevisionId 1UL))
            check (not release.Task.IsCompleted) "The coordinator acknowledged while the prefix was still blocked."
            same 1 (AsyncMailbox.snapshot handle).RunningSteps
            release.TrySetResult() |> ignore
            do! idle handle
            same (ValueToken 7UL) (result handle).Value
        })
    }

    [<Test>]
    member _.``detaching one shared demand and an idle observer does not cancel owned work``() = task {
        let entered = completion<WorkCancellation> ()
        let release = completion<unit> ()
        let mutable calls = 0
        let handle = create (fun _ cancellation -> async {
            Interlocked.Increment(&calls) |> ignore
            entered.TrySetResult cancellation |> ignore
            do! Async.AwaitTask release.Task
            return succeeded 42UL
        })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            let! cancellation = wait entered.Task
            do! send handle (Action.Demand(DemandId 2UL, work))
            do! send handle (Action.Release(DemandId 1UL))
            use observer = new CancellationTokenSource()
            let detached = Async.StartAsTask(AsyncMailbox.waitForIdle handle, cancellationToken = observer.Token)
            observer.Cancel()
            do! cancelled detached
            check (not (WorkCancellation.isRequested cancellation)) "The surviving demand owns this same attempt."
            same 1 (AsyncMailbox.snapshot handle).Graph.PendingAttempts
            release.TrySetResult() |> ignore
            do! idle handle
            same 1 calls
            same (ValueToken 42UL) (result handle).Value
        })
    }

    [<Test>]
    member _.``cancelling a close observer does not detach the physical evaluator join``() = task {
        let entered = completion<WorkCancellation> ()
        let release = completion<unit> ()
        let handle = create (fun _ cancellation -> async {
            entered.TrySetResult cancellation |> ignore
            do! Async.AwaitTask release.Task
            return succeeded 9UL
        })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            let! cancellation = wait entered.Task
            let closing = AsyncMailbox.beginClose handle
            do! WorkCancellation.wait cancellation |> run |> wait
            use observer = new CancellationTokenSource()
            let detached = Async.StartAsTask(AsyncMailbox.awaitClose closing, cancellationToken = observer.Token)
            observer.Cancel()
            do! cancelled detached
            same None (AsyncMailbox.tryClose closing)
            check (obj.ReferenceEquals(closing, AsyncMailbox.beginClose handle)) "Observers must share the exact join."
            check (not release.Task.IsCompleted) "Cancellation must not manufacture evaluator completion."
            release.TrySetResult() |> ignore
            let! outcome = AsyncMailbox.awaitClose closing |> run |> wait
            same (Ok ()) outcome
            same 0 (AsyncMailbox.snapshot handle).Graph.PendingAttempts
            same None (AsyncMailbox.tryResult work handle)
        })
    }

    [<Test>]
    member _.``the CLR factory boundary joins a task returned during a cancellation request``() = task {
        let factoryEntered = completion<WorkCancellation> ()
        let releaseFactory = completion<unit> ()
        let factoryReturned = completion<unit> ()
        let releaseTask = completion<uint64> ()
        let mutable calls = 0
        let handle = create (fun _ cancellation -> async {
            let! value =
                ClrInterop.fromTask (fun _ ->
                    Interlocked.Increment(&calls) |> ignore
                    factoryEntered.TrySetResult cancellation |> ignore
                    releaseFactory.Task.GetAwaiter().GetResult()
                    factoryReturned.TrySetResult() |> ignore
                    releaseTask.Task) cancellation
            return succeeded value
        })
        let release () =
            releaseFactory.TrySetResult() |> ignore
            releaseTask.TrySetResult 42UL |> ignore
        start handle
        do! healthy handle release (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            let! cancellation = wait factoryEntered.Task
            let closing = AsyncMailbox.beginClose handle
            do! WorkCancellation.wait cancellation |> run |> wait
            check (not releaseFactory.Task.IsCompleted) "Request must arrive before the factory returns its held task."
            releaseFactory.TrySetResult() |> ignore
            do! wait factoryReturned.Task
            same None (AsyncMailbox.tryClose closing)
            check (not releaseTask.Task.IsCompleted) "The bridge must still own the newly returned task."
            releaseTask.TrySetResult 42UL |> ignore
            let! closed = AsyncMailbox.awaitClose closing |> run |> wait
            same (Ok ()) closed
            same 1 calls
            same None (AsyncMailbox.tryResult work handle)
            check (AsyncMailbox.drainEvents handle |> List.forall (fun effect ->
                match effect.Action with EffectAction.Offer _ -> false | _ -> true)) "Cancelled work cannot offer a result."
        })
    }

    [<Test>]
    member _.``explicit work cancellation preserves an async finally failure``() = task {
        let entered = completion<unit> ()
        let cleanup = completion<unit> ()
        let message = "functional finally failure after explicit request"
        let handle = create (fun _ cancellation -> async {
            try
                entered.TrySetResult() |> ignore
                do! WorkCancellation.wait cancellation
                return StepOutcome.Complete Completion.Cancelled
            finally
                cleanup.TrySetResult() |> ignore
                raise (InvalidOperationException message)
        })
        start handle
        do! healthy handle noRelease (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! wait entered.Task
            do! send handle (Action.Release(DemandId 1UL))
            do! wait cleanup.Task
            do! idle handle
            let diagnostics = AsyncMailbox.drainDiagnostics handle
            check (diagnostics |> List.exists (fun item -> item.Failure.Code = "evaluation" && item.Failure.Message = message))
                "Ambient Async cancellation must not replace the cleanup exception."
            same None (AsyncMailbox.tryResult work handle)
        })
    }

    [<Test>]
    member _.``suspension resumes exactly once while retaining the same logical attempt``() = task {
        let resumeEntered = completion<ResumeRequest> ()
        let release = completion<unit> ()
        let mutable starts = 0
        let mutable resumes = 0
        let handle = create (fun invocation _ -> async {
            match invocation with
            | StepInvocation.Start _ ->
                Interlocked.Increment(&starts) |> ignore
                return StepOutcome.Suspend(StepId 3UL, ValueToken 10UL)
            | StepInvocation.Resume request ->
                Interlocked.Increment(&resumes) |> ignore
                resumeEntered.TrySetResult request |> ignore
                do! Async.AwaitTask release.Task
                return StepOutcome.Complete(Completion.Succeeded request.Response)
        })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! until (fun () -> (AsyncMailbox.snapshot handle).Suspensions.Length = 1)
            let suspension = (AsyncMailbox.snapshot handle).Suspensions |> List.exactlyOne
            same 0 (AsyncMailbox.snapshot handle).RunningSteps
            same 1 (AsyncMailbox.snapshot handle).Graph.PendingAttempts
            do! send handle (Action.Resume(suspension, ValueToken 42UL))
            let! request = wait resumeEntered.Task
            same suspension request.Suspension
            same suspension.Attempt request.Start.Attempt
            let repeated = admit handle (Action.Resume(suspension, ValueToken 42UL))
            let! refused = AsyncMailbox.observe repeated |> run |> wait
            same (Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension suspension.Id))) refused
            release.TrySetResult() |> ignore
            do! idle handle
            same 1 starts
            same 1 resumes
            same suspension.Attempt (result handle).Attempt
            same (ValueToken 42UL) (result handle).Value
        })
    }

    [<Test>]
    member _.``acknowledged reservation rejects a held resume before external source mutation``() = task {
        let mutable resumes = 0
        let handle = create (fun invocation _ -> async {
            match invocation with
            | StepInvocation.Start _ -> return StepOutcome.Suspend(StepId 1UL, ValueToken 9UL)
            | StepInvocation.Resume _ ->
                Interlocked.Increment(&resumes) |> ignore
                return succeeded 9UL
        })
        start handle
        do! healthy handle noRelease (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! until (fun () -> (AsyncMailbox.snapshot handle).Suspensions.Length = 1)
            let suspension = (AsyncMailbox.snapshot handle).Suspensions |> List.exactlyOne
            do! send handle (Action.ReserveScope(scope, RevisionId 2UL))
            // No source write or replacement has happened before this refusal.
            let operation = admit handle (Action.Resume(suspension, ValueToken 99UL))
            let! answer = AsyncMailbox.observe operation |> run |> wait
            same (Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension suspension.Id))) answer
            do! idle handle
            same 0 resumes
            same None (AsyncMailbox.tryResult work handle)
        })
    }

    [<Test>]
    member _.``functional coordinator emits the exact deterministic core command trace``() = task {
        let entered = completion<StartRequest> ()
        let release = completion<unit> ()
        let handle = create (fun invocation _ -> async {
            match invocation with
            | StepInvocation.Start request ->
                entered.TrySetResult request |> ignore
                do! Async.AwaitTask release.Task
                return succeeded 42UL
            | StepInvocation.Resume _ -> return failwith "No suspension in this trace."
        })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            let actions =
                [ Action.ReserveScope(scope, RevisionId 1UL)
                  Action.ReplaceScope(scope, RevisionId 1UL, [define work])
                  Action.Demand(DemandId 1UL, work) ]
            let mutable expectedState = Core.init epoch
            let expectedEvents = ResizeArray<Effect>()
            let apply action =
                let next, effects = Core.step { Epoch = epoch; Action = action } expectedState |> get
                expectedState <- next
                expectedEvents.AddRange effects
                effects
            for action in actions do
                let effects = apply action
                let! receipt = post handle action
                same effects receipt.Effects
            let! request = wait entered.Task
            release.TrySetResult() |> ignore
            do! idle handle
            apply (Action.Finished(request.Attempt, Completion.Succeeded(ValueToken 42UL))) |> ignore
            apply (Action.Drained request.Attempt) |> ignore
            same (expectedEvents |> Seq.toList) (AsyncMailbox.drainEvents handle)
            same (Core.snapshot expectedState) (AsyncMailbox.snapshot handle).Graph
            same (Core.tryResult work expectedState) (AsyncMailbox.tryResult work handle)
            let actual = result handle
            check (AsyncMailbox.isEligible actual handle) "The real current result must be eligible."
        })
    }

    [<Test>]
    member _.``a genuine core retirement refusal joins independent evaluator and callback ownership``() = task {
        let entered = completion<unit> ()
        let releaseEvaluator = completion<unit> ()
        let evaluatorReturned = completion<unit> ()
        let callbackEntered = completion<unit> ()
        let releaseCallback = completion<unit> ()
        let callbackExited = completion<unit> ()
        let mutable registration = Unchecked.defaultof<CancellationTokenRegistration>
        use registrationOwner = { new IDisposable with member _.Dispose() = registration.Dispose() }
        let evaluatorFailure = "held functional evaluator finished with failure"
        let diagnostics = ResizeArray<HostDiagnostic>()
        let mutable injected = false
        let step (command: Command) state =
            let changed =
                match command.Action with
                | Action.Retire ->
                    injected <- true
                    { command with Epoch = EpochId 999UL }
                | _ -> command
            Core.step changed state
        let evaluator _ cancellation = async {
            registration <- (ClrInterop.cancellationToken cancellation).Register(fun () ->
                callbackEntered.TrySetResult() |> ignore
                try releaseCallback.Task.GetAwaiter().GetResult()
                finally callbackExited.TrySetResult() |> ignore)
            entered.TrySetResult() |> ignore
            do! Async.AwaitTask releaseEvaluator.Task
            evaluatorReturned.TrySetResult() |> ignore
            return raise (InvalidOperationException evaluatorFailure)
        }
        let handle = AsyncMailbox.createWithStep settings evaluator step |> get
        let release () =
            releaseEvaluator.TrySetResult() |> ignore
            releaseCallback.TrySetResult() |> ignore
        start handle
        let! closed = protect handle release (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! wait entered.Task
            AsyncMailbox.drainEvents handle |> ignore
            let closing = AsyncMailbox.beginClose handle
            do! wait callbackEntered.Task
            check injected "Failure must come from an actual Core.step epoch rejection."
            same None (AsyncMailbox.tryClose closing)
            releaseEvaluator.TrySetResult() |> ignore
            do! wait evaluatorReturned.Task
            do! until (fun () ->
                diagnostics.AddRange(AsyncMailbox.drainDiagnostics handle)
                diagnostics |> Seq.exists (fun item -> item.Failure.Code = "evaluation" && item.Failure.Message = evaluatorFailure))
            same 1 (AsyncMailbox.snapshot handle).RunningSteps
            check (not callbackExited.Task.IsCompleted) "The independent callback is still physically owned."
            same None (AsyncMailbox.tryClose closing)
            let observingIdle = Async.StartImmediateAsTask(AsyncMailbox.waitForIdle handle, cancellationToken = CancellationToken.None)
            check (not observingIdle.IsCompleted) "A fault cannot bypass the callback join."
            releaseCallback.TrySetResult() |> ignore
            do! wait callbackExited.Task
            let! closeResult = AsyncMailbox.awaitClose closing |> run |> wait
            let! idleResult = observingIdle |> wait
            same closeResult idleResult
            match closeResult with
            | Error error ->
                same "host-protocol" error.Code
                check (error.Message.Contains "ForeignEpoch") "Retain the exact Core refusal."
            | Ok () -> failwith "The rejected retirement was reported successful."
            check ((AsyncMailbox.snapshot handle).Graph.PendingAttempts > 0) "Physical join cannot fabricate Core.Drained."
            same None (AsyncMailbox.tryResult work handle)
            check (AsyncMailbox.drainEvents handle |> List.forall (fun effect ->
                match effect.Action with EffectAction.Offer _ | EffectAction.EpochDrained -> false | _ -> true))
                "Fault cleanup must not manufacture authority."
        })
        match closed with Error error -> same "host-protocol" error.Code | Ok () -> failwith "Missing physical-close failure."
    }

    [<Test>]
    member _.``async cell removes detached waiters and retains exactly one settled value``() = task {
        let cell: AsyncCell.Cell<int> = AsyncCell.create ()
        use observer = new CancellationTokenSource()
        let detached = Async.StartAsTask(AsyncCell.observe cell, cancellationToken = observer.Token)
        let survivor = AsyncCell.observe cell |> run
        try
            do! until (fun () -> AsyncCell.waiterCount cell = 2)
            observer.Cancel()
            do! cancelled detached
            same 1 (AsyncCell.waiterCount cell)
            same None (AsyncCell.tryRead cell)
            check (AsyncCell.publish 42 cell) "First settlement must win."
            same 0 (AsyncCell.waiterCount cell)
            let! actual = wait survivor
            same 42 actual
            check (not (AsyncCell.publish 99 cell)) "The settled value is immutable."
            for _ in 1 .. 20 do
                let! repeated = AsyncCell.observe cell |> run |> wait
                same 42 repeated
            same 0 (AsyncCell.waiterCount cell)
            same (Some 42) (AsyncCell.tryRead cell)
        finally
            observer.Cancel()
            AsyncCell.publish 42 cell |> ignore
    }

    [<Test>]
    member _.``external lifecycle commands cannot counterfeit owned workflow completion``() = task {
        let entered = completion<StartRequest> ()
        let release = completion<unit> ()
        let handle = create (fun invocation _ -> async {
            match invocation with
            | StepInvocation.Start request ->
                entered.TrySetResult request |> ignore
                do! Async.AwaitTask release.Task
                return succeeded 42UL
            | StepInvocation.Resume _ -> return failwith "No resume expected."
        })
        start handle
        do! healthy handle (fun () -> release.TrySetResult() |> ignore) (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            let! request = wait entered.Task
            for action in
                [ Action.Finished(request.Attempt, Completion.Succeeded(ValueToken 99UL))
                  Action.Drained request.Attempt
                  Action.Suspend(request.Attempt, StepId 1UL, ValueToken 99UL) ] do
                match AsyncMailbox.admit action handle with
                | Error error -> same MailboxError.LifecycleOwnedByHost error
                | Ok _ -> failwith "An external caller acquired lifecycle authority."
            same None (AsyncMailbox.tryResult work handle)
            same 1 (AsyncMailbox.snapshot handle).Graph.PendingAttempts
            release.TrySetResult() |> ignore
            do! idle handle
            same (ValueToken 42UL) (result handle).Value
        })
    }

    [<Test>]
    member _.``a failed workflow requires explicit retry and does not poison the coordinator``() = task {
        let mutable calls = 0
        let expected = { Code = "fixture-failure"; Message = "retry only by an owner command" }
        let handle = create (fun _ _ -> async {
            let call = Interlocked.Increment(&calls)
            if call = 1 then return StepOutcome.Complete(Completion.Failed expected)
            else return succeeded 42UL
        })
        start handle
        do! healthy handle noRelease (fun () -> task {
            do! register handle [define work]
            do! send handle (Action.Demand(DemandId 1UL, work))
            do! idle handle
            same None (AsyncMailbox.tryResult work handle)
            same (WorkStatus.Failed expected) ((AsyncMailbox.snapshot handle).Graph.Works |> List.exactlyOne).Status
            do! send handle (Action.SetInputs [])
            same 1 calls
            do! send handle (Action.Retry work)
            do! idle handle
            same 2 calls
            same (ValueToken 42UL) (result handle).Value
            check (AsyncMailbox.drainDiagnostics handle |> List.exists (fun item -> item.Failure = expected))
                "The actual workflow failure must remain observable after retry."
        })
    }
