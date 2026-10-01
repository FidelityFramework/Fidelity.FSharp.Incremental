namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental

/// A fresh invocation for each attempt. Completion must include all owned cleanup.
type Evaluator = StartRequest -> CancellationToken -> Task<ValueToken>

type HostDiagnostic = { Attempt: AttemptId; Failure: Failure }

type private Running(request: StartRequest) =
    member _.Request = request
    member val Source = new CancellationTokenSource()
    member val Done = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    member val CancelRequested = false with get, set
    member val Cancellation: Task option = None with get, set

/// Serializes the portable protocol and bounds actual evaluator ownership.
/// Observer cancellation only detaches the observer; Release removes demand.
type Host(epoch: EpochId, maxConcurrency: int, evaluator: Evaluator) =
    do if maxConcurrency < 1 then invalidArg (nameof maxConcurrency) "Concurrency must be positive."

    let gate = obj ()
    let slots = new SemaphoreSlim(maxConcurrency, maxConcurrency)
    let running = Dictionary<AttemptId, Running>()
    let events = Queue<Effect>()
    let diagnostics = Queue<HostDiagnostic>()
    let mutable state = Core.init epoch
    let mutable closing: Task option = None
    let mutable protocolFault: exn option = None

    let failure code (error: exn) = { Code = code; Message = error.Message }

    // Called under gate. External evaluators and token callbacks never run here.
    let transition action =
        match Core.step { Epoch = epoch; Action = action } state with
        | Error error -> Error error
        | Ok (next, effects) ->
            state <- next
            let starts = ResizeArray<Running>()
            for effect in effects do
                events.Enqueue effect
                match effect.Action with
                | EffectAction.Start request ->
                    let entry = Running(request)
                    running.Add(request.Attempt, entry)
                    starts.Add entry
                | EffectAction.Cancel attempt ->
                    match running.TryGetValue attempt with
                    | true, entry when not entry.CancelRequested ->
                        entry.CancelRequested <- true
                        // Store the owned callback task before leaving the coordinator.
                        // A throwing callback is reported, and cannot prevent joining it.
                        entry.Cancellation <- Some(Task.Run(fun () ->
                            try entry.Source.Cancel()
                            with error ->
                                lock gate (fun () ->
                                    diagnostics.Enqueue { Attempt = attempt; Failure = failure "cancellation-callback" error })))
                    | _ -> ()
                | _ -> ()
            Ok (effects, starts |> Seq.toList)

    let requireTransition action =
        match transition action with
        | Ok (_, starts) -> starts
        | Error error -> invalidOp $"Host/core protocol disagreement: {error}"

    let rec launch (entries: Running list) =
        for entry in entries do
            // Always enter through the pool, even when the evaluator is synchronous.
            Task.Run(Func<Task>(fun () ->
                let owned: Task = task {
                    try do! execute entry
                    with error ->
                        lock gate (fun () ->
                            protocolFault <- Some error
                            diagnostics.Enqueue { Attempt = entry.Request.Attempt; Failure = failure "host-protocol" error }
                            entry.Done.TrySetException error |> ignore)
                }
                owned)) |> ignore

    and execute (entry: Running) = task {
        let mutable acquired = false
        let mutable outcome = Completion.Cancelled
        try
            try
                do! slots.WaitAsync(entry.Source.Token)
                acquired <- true
                // This check is the admission point. A later reservation may race
                // the call below, but it cannot make that attempt's result eligible.
                let admitted = lock gate (fun () -> not entry.CancelRequested)
                if admitted then
                    entry.Source.Token.ThrowIfCancellationRequested()
                    let! value = evaluator entry.Request entry.Source.Token
                    outcome <- Completion.Succeeded value
            with
            | :? OperationCanceledException when entry.Source.IsCancellationRequested || entry.CancelRequested ->
                outcome <- Completion.Cancelled
            | error -> outcome <- Completion.Failed (failure "evaluation" error)

            lock gate (fun () -> requireTransition (Action.Finished(entry.Request.Attempt, outcome))) |> launch

            // Close the cancel/drain race under the same coordinator lock. Cancellation
            // callbacks themselves may re-enter the host, so join them outside that lock.
            let mutable drained = false
            while not drained do
                let waitFor, starts =
                    lock gate (fun () ->
                        match entry.Cancellation with
                        | Some work when not work.IsCompleted -> Some work, []
                        | _ ->
                            entry.Source.Dispose()
                            if acquired then
                                slots.Release() |> ignore
                                acquired <- false
                            let starts = requireTransition (Action.Drained entry.Request.Attempt)
                            running.Remove entry.Request.Attempt |> ignore
                            entry.Done.TrySetResult() |> ignore
                            drained <- true
                            None, starts)
                launch starts
                match waitFor with
                | Some work -> do! work
                | None -> ()
        finally
            if acquired then slots.Release() |> ignore
    }

    /// Applies a command atomically. Reservation withdraws eligibility before returning.
    /// Finished and Drained belong to this host and cannot be supplied by its caller.
    member _.Send(action: Action) =
        match action with
        | Action.Finished _ | Action.Drained _ -> invalidArg (nameof action) "The host owns completion and drain acknowledgements."
        | _ -> ()
        let result = lock gate (fun () ->
            match protocolFault with
            | Some error -> raise (InvalidOperationException("Host protocol is faulted.", error))
            | None -> transition action)
        match result with
        | Error error -> Error error
        | Ok (effects, starts) -> launch starts; Ok effects

    member _.Snapshot = lock gate (fun () -> Core.snapshot state)
    member _.TryResult(work: WorkId) = lock gate (fun () -> if protocolFault.IsSome then None else Core.tryResult work state)
    member _.IsEligible(handle: ResultHandle) = lock gate (fun () -> protocolFault.IsNone && Core.isEligible handle state)

    /// Observations are data, not callbacks inside a state transition. Drain regularly.
    member _.DrainEvents() = lock gate (fun () ->
        let result = events |> Seq.toList
        events.Clear()
        result)

    member _.DrainDiagnostics() = lock gate (fun () ->
        let result = diagnostics |> Seq.toList
        diagnostics.Clear()
        result)

    /// Joins all presently owned work, including work scheduled by completions.
    /// The token cancels this wait only. New demands may be sent by other callers.
    member _.WaitForIdleAsync(cancellationToken: CancellationToken) : Task = task {
        let mutable idle = false
        while not idle do
            let pending = lock gate (fun () ->
                match protocolFault with
                | Some error -> raise (InvalidOperationException("Host protocol is faulted.", error))
                | None -> running.Values |> Seq.map (fun item -> item.Done.Task :> Task) |> Seq.toArray)
            if pending.Length = 0 then idle <- true
            else do! Task.WhenAll(pending).WaitAsync(cancellationToken)
        cancellationToken.ThrowIfCancellationRequested()
    }

    /// Retires the epoch and joins actual work. It does not forcefully preempt an
    /// evaluator that ignores its token. The owner must implement its cleanup contract.
    member this.CloseAsync() : Task =
        lock gate (fun () ->
            match closing with
            | Some work -> work
            | None ->
                let starts = requireTransition Action.Retire
                launch starts
                let work: Task = task {
                    do! this.WaitForIdleAsync(CancellationToken.None)
                    slots.Dispose()
                }
                closing <- Some work
                work)

    interface IAsyncDisposable with
        member this.DisposeAsync() = ValueTask(this.CloseAsync())
