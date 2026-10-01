namespace Fidelity.FSharp.Incremental.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental.Hosting

module private InteropChecks =
    let completion<'T> () = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
    let wait (work: Task<'T>) = work.WaitAsync(TimeSpan.FromSeconds 5.)
    let run work = ClrInterop.toTask CancellationToken.None work
    let failure (work: Task<'T>) = task {
        try
            let! _ = wait work
            return failwith "Expected an owned CLR failure."
        with error -> return error
    }
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)

open InteropChecks

[<TestFixture>]
type ClrInteropTests() =
    [<Test>]
    member _.``uncancelled CLR factory is cold and each execution owns its exact task``() = task {
        let entered = completion<unit> ()
        let release = completion<int> ()
        let mutable calls = 0
        let workflow = ClrInterop.fromUncancelledTask (fun () ->
            Interlocked.Increment(&calls) |> ignore
            entered.TrySetResult () |> ignore
            release.Task)
        Assert.That(calls, Is.Zero)
        let owned = run workflow
        do! wait entered.Task
        use cancellation = new CancellationTokenSource()
        let observer = owned.WaitAsync cancellation.Token
        cancellation.Cancel()
        let! detached = failure observer
        check (detached :? OperationCanceledException) "Only the observer should receive cancellation."
        check (not owned.IsCompleted) "Observer cancellation cannot settle the held owned task."
        release.TrySetResult 42 |> ignore
        let! first = wait owned
        let! repeated = workflow |> run |> wait
        Assert.That(first, Is.EqualTo 42)
        Assert.That(repeated, Is.EqualTo 42)
        Assert.That(calls, Is.EqualTo 2)
    }

    [<Test>]
    member _.``CLR bridge retains exact asynchronous fault including aggregate identity``() = task {
        for original in [ InvalidOperationException("original task failure") :> exn
                          AggregateException("original aggregate", InvalidOperationException("inner")) :> exn ] do
            let entered = completion<unit> ()
            let release = completion<int> ()
            let owned = ClrInterop.fromUncancelledTask (fun () ->
                entered.TrySetResult () |> ignore
                release.Task) |> run
            do! wait entered.Task
            check (not owned.IsCompleted) "The original task is still owned before failure."
            release.TrySetException original |> ignore
            let! observed = failure owned
            check (obj.ReferenceEquals(original, observed)) "The bridge must preserve the original fault without wrapping or flattening."
    }

    [<Test>]
    member _.``CLR bridge retains a synchronous factory exception``() = task {
        let original = InvalidOperationException("factory failed before returning a task")
        let workflow: Async<int> = ClrInterop.fromUncancelledTask (fun () -> raise original)
        let! observed = workflow |> run |> failure
        check (obj.ReferenceEquals(original, observed)) "Synchronous factory failure identity must survive the boundary."
    }

    [<Test>]
    member _.``CLR bridge refuses a null task as an owned failure``() = task {
        let! observed = ClrInterop.fromUncancelledTask (fun () -> Unchecked.defaultof<Task<int>>) |> run |> failure
        check (observed :? InvalidOperationException) "A null task must be an explicit factory-contract failure."
        Assert.That(observed.Message, Is.EqualTo "The Task factory returned null.")
    }

    [<Test>]
    member _.``canceled CLR task takes the owned error path with its original token``() = task {
        use source = new CancellationTokenSource()
        source.Cancel()
        let outcome = completion<string * exn option> ()
        Async.StartWithContinuations(
            ClrInterop.fromUncancelledTask (fun () -> Task.FromCanceled<int>(source.Token)),
            (fun _ -> outcome.TrySetResult(("success", None)) |> ignore),
            (fun error -> outcome.TrySetResult(("error", Some error)) |> ignore),
            (fun error -> outcome.TrySetResult(("ambient-cancel", Some error)) |> ignore),
            cancellationToken = CancellationToken.None)
        let! route, observed = wait outcome.Task
        Assert.That(route, Is.EqualTo "error", "Task cancellation must not skip the owner's error/cleanup path.")
        match observed with
        | Some (:? OperationCanceledException as error) -> Assert.That(error.CancellationToken, Is.EqualTo source.Token)
        | _ -> Assert.Fail "Expected the exact Task cancellation outcome."
    }

    [<Test>]
    member _.``attempt-aware CLR factory shares the same exact completion bridge``() = task {
        let cancellation = ExecutionBoundary.createCancellation ()
        let original = InvalidOperationException("attempt task failed")
        try
            let workflow = ClrInterop.fromTask (fun token ->
                Assert.That(token, Is.EqualTo(ClrInterop.cancellationToken cancellation))
                Task.FromException<int>(original)) cancellation
            let! observed = workflow |> run |> failure
            check (obj.ReferenceEquals(original, observed)) "Existing attempt-aware callers must keep exact fault identity."
        finally ExecutionBoundary.dispose cancellation
    }
