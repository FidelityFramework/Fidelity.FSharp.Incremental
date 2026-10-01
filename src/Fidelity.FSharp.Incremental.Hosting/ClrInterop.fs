namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Threading
open System.Threading.Tasks

/// CLR interoperation is explicit. These functions do not define core semantics.
[<RequireQualifiedAccess>]
module ClrInterop =
    /// For .NET I/O or callback registration inside an owned workflow. The
    /// request belongs to the attempt, never to a reply/close observer.
    let cancellationToken cancellation = ExecutionBoundary.token cancellation

    let private invokeAndJoin (factory: unit -> Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (success, error, _) ->
            let pending =
                try Ok(factory ())
                with failure -> Error failure
            match pending with
            | Error failure -> error failure
            | Ok work when isNull work -> error (InvalidOperationException("The Task factory returned null."))
            | Ok work ->
                work.ContinueWith(
                    Action<Task<'T>>(fun completed ->
                        let outcome =
                            try Ok(completed.GetAwaiter().GetResult())
                            with failure -> Error failure
                        match outcome with
                        | Ok value -> success value
                        | Error failure -> error failure),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default) |> ignore)

    /// Invoke a cold Task factory and attach its completion in one continuation
    /// boundary. Once invoked, the wait cannot detach before that Task finishes.
    /// The factory's Task must itself include its owned children and cleanup.
    let fromTask (factory: CancellationToken -> Task<'T>) cancellation : Async<'T> =
        invokeAndJoin (fun () -> factory (cancellationToken cancellation))

    /// For a CLR owner outside a mailbox attempt. Run this cold workflow with
    /// ambient CancellationToken.None; cancel separate observers, not its owner.
    /// Once invoked, it joins the exact returned Task, which must include all
    /// owned children and cleanup. Faults retain their original exception;
    /// canceled Tasks report OperationCanceledException through the error path
    /// so an owned workflow can finish cleanup without Async cancellation.
    let fromUncancelledTask (factory: unit -> Task<'T>) : Async<'T> =
        invokeAndJoin factory

    /// Starts an observer workflow for a CLR consumer. The supplied token
    /// cancels that observer; it is not the owned attempt's cancellation request.
    let toTask (observer: CancellationToken) (workflow: Async<'T>) : Task<'T> =
        Async.StartAsTask(workflow, cancellationToken = observer,
                          taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously)
