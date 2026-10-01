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

    /// Invoke a cold Task factory and attach its completion in one continuation
    /// boundary. Once invoked, the wait cannot detach before that Task finishes.
    /// The factory's Task must itself include its owned children and cleanup.
    let fromTask (factory: CancellationToken -> Task<'T>) cancellation : Async<'T> =
        Async.FromContinuations(fun (success, error, _) ->
            let pending =
                try Ok(factory (cancellationToken cancellation))
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

    /// Starts an observer workflow for a CLR consumer. The supplied token
    /// cancels that observer; it is not the owned attempt's cancellation request.
    let toTask (observer: CancellationToken) (workflow: Async<'T>) : Task<'T> =
        Async.StartAsTask(workflow, cancellationToken = observer,
                          taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously)
