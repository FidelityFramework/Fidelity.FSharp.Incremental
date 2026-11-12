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

    /// Observes a workflow as an outward Task while executing releaseSynchronously
    /// atomically once via Interlocked.Exchange inside both terminal observation
    /// and CancellationToken.Register callbacks prior to invoking TrySetResult,
    /// TrySetException, or TrySetCanceled on the returned Task.
    let observeWithRelease
        (releaseSynchronously: unit -> unit)
        (observer: CancellationToken)
        (workflow: Async<'T>) : Task<'T> =
        let tcs = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable releaseLatch = 0
        let mutable registration = Unchecked.defaultof<CancellationTokenRegistration>
        let mutable hasRegistration = 0
        let unregister () =
            if Interlocked.Exchange(&hasRegistration, 0) <> 0 then
                registration.Unregister() |> ignore
        let cancelWithRelease () =
            if Interlocked.Exchange(&releaseLatch, 1) = 0 then
                try
                    releaseSynchronously ()
                    unregister ()
                    tcs.TrySetCanceled(observer) |> ignore
                with releaseFailure ->
                    unregister ()
                    tcs.TrySetException(releaseFailure) |> ignore
        if observer.IsCancellationRequested then
            cancelWithRelease ()
            tcs.Task
        else
            if observer.CanBeCanceled then
                let reg = observer.Register(Action cancelWithRelease)
                registration <- reg
                Volatile.Write(&hasRegistration, 1)
                if Volatile.Read(&releaseLatch) <> 0 then
                    unregister ()
            if Volatile.Read(&releaseLatch) = 0 then
                Async.StartWithContinuations(
                    async {
                        do! Async.SwitchToThreadPool()
                        return! workflow
                    },
                    (fun value ->
                        if Interlocked.Exchange(&releaseLatch, 1) = 0 then
                            try
                                releaseSynchronously ()
                                unregister ()
                                tcs.TrySetResult(value) |> ignore
                            with releaseFailure ->
                                unregister ()
                                tcs.TrySetException(releaseFailure) |> ignore),
                    (fun failure ->
                        match failure with
                        | :? OperationCanceledException when observer.IsCancellationRequested ->
                            cancelWithRelease ()
                        | _ ->
                            if Interlocked.Exchange(&releaseLatch, 1) = 0 then
                                try
                                    releaseSynchronously ()
                                    unregister ()
                                    tcs.TrySetException(failure) |> ignore
                                with releaseFailure ->
                                    unregister ()
                                    tcs.TrySetException(AggregateException(failure, releaseFailure)) |> ignore),
                    (fun _ -> cancelWithRelease ()),
                    cancellationToken = observer)
            tcs.Task
