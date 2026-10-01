namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module private HostTestSupport =
    let completion<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
    let wait (value: Task<'T>) = value.WaitAsync(TimeSpan.FromSeconds 5.)

open HostTestSupport

[<TestFixture>]
type HostTests() =
    let epoch = EpochId 1UL
    let scope = ScopeId 1UL
    let work = WorkId 1UL
    let send (host: Host) command =
        match host.Send command with
        | Ok effects -> effects
        | Error error -> failwithf "Unexpected protocol error: %A" error
    let register (host: Host) scopeId entries =
        send host (Action.ReserveScope(scopeId, RevisionId 1UL)) |> ignore
        send host (Action.ReplaceScope(scopeId, RevisionId 1UL, entries)) |> ignore
    let definition id stamp reads = ScopeEntry.Define { Work = id; Stamp = DefinitionStamp stamp; Reads = reads }
    let idle (host: Host) = host.WaitForIdleAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds 5.)

    [<Test>]
    member _.``construction and registration are cold and each retry invokes a fresh evaluator``() = task {
        let mutable calls = 0
        use host = new Host(epoch, 1, fun _ _ ->
            calls <- calls + 1
            if calls = 1 then Task.FromException<ValueToken>(InvalidOperationException "first")
            else Task.FromResult(ValueToken(uint64 calls)))
        register host scope [definition work 1UL []]
        Assert.That(calls, Is.Zero)
        send host (Action.Demand(DemandId 1UL, work)) |> ignore
        do! idle host
        Assert.That(host.TryResult work, Is.EqualTo(None: ResultHandle option))
        send host (Action.Retry work) |> ignore
        do! idle host
        Assert.That(calls, Is.EqualTo 2)
        Assert.That((host.TryResult work).Value.Value, Is.EqualTo(ValueToken 2UL))
    }

    [<Test>]
    member _.``releasing one of two demands preserves their shared execution``() = task {
        let started = completion<unit>()
        let release = completion<ValueToken>()
        let mutable calls = 0
        use host = new Host(epoch, 1, fun _ _ ->
            Interlocked.Increment(&calls) |> ignore
            started.TrySetResult() |> ignore
            release.Task)
        try
            register host scope [definition work 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            send host (Action.Demand(DemandId 2UL, work)) |> ignore
            do! wait started.Task
            send host (Action.Release(DemandId 1UL)) |> ignore
            release.TrySetResult(ValueToken 42UL) |> ignore
            do! idle host
            Assert.That(calls, Is.EqualTo 1)
            Assert.That((host.TryResult work).Value.Value, Is.EqualTo(ValueToken 42UL))
        finally release.TrySetResult(ValueToken 42UL) |> ignore
    }

    [<Test>]
    member _.``cancelling a queued demand never invokes its evaluator``() = task {
        let started = completion<unit>()
        let release = completion<ValueToken>()
        let other = WorkId 2UL
        let mutable queuedCalls = 0
        use host = new Host(epoch, 1, fun request _ ->
            if request.Definition.Work = work then
                started.TrySetResult() |> ignore
                release.Task
            else
                Interlocked.Increment(&queuedCalls) |> ignore
                Task.FromResult(ValueToken 2UL))
        try
            register host scope [definition work 1UL []; definition other 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            do! wait started.Task
            send host (Action.Demand(DemandId 2UL, other)) |> ignore
            send host (Action.Release(DemandId 2UL)) |> ignore
            release.TrySetResult(ValueToken 1UL) |> ignore
            do! idle host
            Assert.That(queuedCalls, Is.Zero)
            Assert.That(host.TryResult other, Is.EqualTo(None: ResultHandle option))
        finally release.TrySetResult(ValueToken 1UL) |> ignore
    }

    [<Test>]
    member _.``reservation rejects a late successful result and keeps capacity until completion``() = task {
        let started = completion<unit>()
        let obsolete = completion<ValueToken>()
        let replacementStarted = completion<unit>()
        use host = new Host(epoch, 1, fun request _ ->
            if request.Definition.Stamp = DefinitionStamp 1UL then
                started.TrySetResult() |> ignore
                obsolete.Task
            else
                replacementStarted.TrySetResult() |> ignore
                Task.FromResult(ValueToken 20UL))
        try
            register host scope [definition work 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            do! wait started.Task
            send host (Action.ReserveScope(scope, RevisionId 2UL)) |> ignore
            Assert.That(host.TryResult work, Is.EqualTo(None: ResultHandle option))
            send host (Action.ReplaceScope(scope, RevisionId 2UL, [definition work 2UL []])) |> ignore
            Assert.That(replacementStarted.Task.IsCompleted, Is.False)
            Assert.That(host.Snapshot.PendingAttempts, Is.EqualTo 1)
            obsolete.TrySetResult(ValueToken 10UL) |> ignore
            do! wait replacementStarted.Task
            do! idle host
            let offered = host.DrainEvents() |> List.choose (fun effect -> match effect.Action with EffectAction.Offer value -> Some value.Value | _ -> None)
            Assert.That((offered = [ValueToken 20UL]), Is.True)
        finally obsolete.TrySetResult(ValueToken 10UL) |> ignore
    }

    [<Test>]
    member _.``observer cancellation detaches without releasing work ownership``() = task {
        let started = completion<unit>()
        let release = completion<ValueToken>()
        use host = new Host(epoch, 1, fun _ _ -> started.TrySetResult() |> ignore; release.Task)
        use observer = new CancellationTokenSource()
        try
            register host scope [definition work 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            do! wait started.Task
            let waiting = host.WaitForIdleAsync observer.Token
            observer.Cancel()
            let mutable cancelled = false
            try do! waiting with :? OperationCanceledException -> cancelled <- true
            Assert.That(cancelled, Is.True)
            Assert.That(host.Snapshot.PendingAttempts, Is.EqualTo 1)
            release.TrySetResult(ValueToken 7UL) |> ignore
            do! idle host
            Assert.That((host.TryResult work).Value.Value, Is.EqualTo(ValueToken 7UL))
        finally release.TrySetResult(ValueToken 7UL) |> ignore
    }

    [<Test>]
    member _.``close joins token ignoring work and makes its late result unavailable``() = task {
        let started = completion<unit>()
        let release = completion<ValueToken>()
        use host = new Host(epoch, 1, fun _ _ -> started.TrySetResult() |> ignore; release.Task)
        try
            register host scope [definition work 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            do! wait started.Task
            let closing = host.CloseAsync()
            Assert.That(closing.IsCompleted, Is.False)
            release.TrySetResult(ValueToken 8UL) |> ignore
            do! closing.WaitAsync(TimeSpan.FromSeconds 5.)
            Assert.That(host.Snapshot.PendingAttempts, Is.Zero)
            Assert.That(host.TryResult work, Is.EqualTo(None: ResultHandle option))
            Assert.That(host.DrainEvents() |> List.exists (fun effect -> match effect.Action with EffectAction.Offer _ -> true | _ -> false), Is.False)
        finally release.TrySetResult(ValueToken 8UL) |> ignore
    }

    [<Test>]
    member _.``throwing cancellation callback is observed and joined without an offer``() = task {
        let started = completion<unit>()
        let release = completion<ValueToken>()
        let callback = completion<unit>()
        use host = new Host(epoch, 1, fun _ token -> task {
            use registration = token.Register(fun () -> callback.TrySetResult() |> ignore; raise (InvalidOperationException "callback"))
            started.TrySetResult() |> ignore
            return! release.Task
        })
        try
            register host scope [definition work 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            do! wait started.Task
            send host (Action.Release(DemandId 1UL)) |> ignore
            do! wait callback.Task
            release.TrySetResult(ValueToken 9UL) |> ignore
            do! idle host
            Assert.That((host.DrainDiagnostics() |> List.map (fun item -> item.Failure.Code)) = ["cancellation-callback"], Is.True)
            Assert.That(host.Snapshot.PendingAttempts, Is.Zero)
            Assert.That(host.TryResult work, Is.EqualTo(None: ResultHandle option))
        finally release.TrySetResult(ValueToken 9UL) |> ignore
    }

    [<Test>]
    member _.``unchanged retained definition is offered with new eligibility without evaluation``() = task {
        let mutable calls = 0
        use host = new Host(epoch, 1, fun _ _ -> Interlocked.Increment(&calls) |> ignore; Task.FromResult(ValueToken 3UL))
        register host scope [definition work 1UL []]
        send host (Action.Demand(DemandId 1UL, work)) |> ignore
        do! idle host
        let old = (host.TryResult work).Value
        send host (Action.ReserveScope(scope, RevisionId 2UL)) |> ignore
        Assert.That(host.IsEligible old, Is.False)
        send host (Action.ReplaceScope(scope, RevisionId 2UL, [ScopeEntry.Retain work])) |> ignore
        do! idle host
        let current = (host.TryResult work).Value
        Assert.That(current.Value, Is.EqualTo old.Value)
        Assert.That(current.Attempt, Is.EqualTo old.Attempt)
        Assert.That(current.Eligibility, Is.Not.EqualTo old.Eligibility)
        Assert.That(calls, Is.EqualTo 1)
        Assert.That(host.IsEligible old, Is.False)
        Assert.That(host.IsEligible current, Is.True)
    }

    [<Test>]
    member _.``independent ready branches can run concurrently within the bound``() = task {
        let bothStarted = completion<unit>()
        let release = completion<ValueToken>()
        let mutable started = 0
        use host = new Host(epoch, 2, fun _ _ ->
            if Interlocked.Increment(&started) = 2 then bothStarted.TrySetResult() |> ignore
            release.Task)
        try
            register host scope [definition work 1UL []; definition (WorkId 2UL) 1UL []]
            send host (Action.Demand(DemandId 1UL, work)) |> ignore
            send host (Action.Demand(DemandId 2UL, WorkId 2UL)) |> ignore
            do! wait bothStarted.Task
            Assert.That(started, Is.EqualTo 2)
            release.TrySetResult(ValueToken 4UL) |> ignore
            do! idle host
        finally release.TrySetResult(ValueToken 4UL) |> ignore
    }

    [<Test>]
    member _.``caller cannot forge lifecycle events or step the non-step host``() = task {
        use host = new Host(epoch, 1, fun _ _ -> Task.FromResult(ValueToken 1UL))
        Assert.Throws<ArgumentException>(fun () -> host.Send(Action.Finished(AttemptId 1UL, Completion.Succeeded(ValueToken 1UL))) |> ignore) |> ignore
        Assert.Throws<ArgumentException>(fun () -> host.Send(Action.Drained(AttemptId 1UL)) |> ignore) |> ignore
        Assert.Throws<ArgumentException>(fun () -> host.Send(Action.Suspend(AttemptId 1UL, StepId 1UL, ValueToken 1UL)) |> ignore) |> ignore
        let handle = { Epoch = epoch; Attempt = AttemptId 1UL; Id = SuspensionId 1UL; Step = StepId 1UL; Environment = ValueToken 1UL }
        Assert.Throws<ArgumentException>(fun () -> host.Send(Action.Resume(handle, ValueToken 1UL)) |> ignore) |> ignore
    }
