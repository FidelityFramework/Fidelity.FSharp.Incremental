namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Threading

/// An owned cancellation request. Observing it does not cancel an evaluator,
/// dispose its resources, or certify that its cleanup has completed.
[<NoEquality; NoComparison>]
type WorkCancellation = private {
    Requested: AsyncCell.Cell<unit>
    Source: CancellationTokenSource
}

[<RequireQualifiedAccess>]
module WorkCancellation =
    let isRequested cancellation =
        AsyncCell.tryRead cancellation.Requested |> Option.isSome

    /// Wait for the request. Cancelling this observation detaches only its waiter.
    let wait cancellation = AsyncCell.observe cancellation.Requested

/// CLR execution and cancellation interoperation stay at this named boundary.
module internal ExecutionBoundary =
    let createCancellation () = {
        Requested = AsyncCell.create ()
        Source = new CancellationTokenSource()
    }

    let markRequested cancellation =
        AsyncCell.publish () cancellation.Requested |> ignore

    /// The runtime owns and joins this synchronous callback invocation separately
    /// from evaluator completion; callers must run it outside the coordinator.
    let cancelCallbacks cancellation = cancellation.Source.Cancel()

    let dispose cancellation = cancellation.Source.Dispose()

    let token cancellation = cancellation.Source.Token

    let run
        (work: Async<'T>)
        (success: 'T -> unit)
        (error: exn -> unit)
        (cancelled: OperationCanceledException -> unit) =
        // An explicit request must not skip a join or let ambient F# cancellation
        // replace evaluator/cleanup errors. Ownership outlives observer cancellation.
        Async.StartWithContinuations(
            async {
                do! Async.SwitchToThreadPool()
                return! work
            },
            success,
            error,
            cancelled,
            cancellationToken = CancellationToken.None)
