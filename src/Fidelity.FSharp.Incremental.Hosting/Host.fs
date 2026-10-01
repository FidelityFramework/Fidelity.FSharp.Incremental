namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental

/// A fresh invocation for each attempt. Completion must include all owned cleanup.
type Evaluator = StartRequest -> CancellationToken -> Task<ValueToken>

type private Running(request: StartRequest) =
    member _.Request = request
    member val Source = new CancellationTokenSource()
    // Physical ownership only: protocol validity is recorded separately.
    member val Done = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    member val Failures = ResizeArray<exn>()
    member val CancelRequested = false with get, set
    member val Cancellation: Task option = None with get, set

/// Serializes the portable protocol and bounds actual evaluator ownership.
/// Observer cancellation only detaches the observer; Release removes demand.
type Host internal (epoch: EpochId, maxConcurrency: int, evaluator: Evaluator,
                    protocolStep: Command -> Core.State -> Result<Core.State * Effect list, ProtocolError>) =
    do if maxConcurrency < 1 then invalidArg (nameof maxConcurrency) "Concurrency must be positive."

    let gate = obj ()
    let slots = new SemaphoreSlim(maxConcurrency, maxConcurrency)
    let running = Dictionary<AttemptId, Running>()
    let events = Queue<Effect>()
    let diagnostics = Queue<HostDiagnostic>()
    let mutable state = Core.init epoch
    let mutable closing: Task option = None
    let mutable protocolFault: exn option = None
    let shutdownErrors = ResizeArray<exn>()

    let failure code (error: exn) = { Code = code; Message = error.Message }

    // All bookkeeping below is under gate. User callbacks run outside it.
    let recordFailure (entry: Running) code error =
        entry.Failures.Add error
        if protocolFault.IsSome then shutdownErrors.Add error
        diagnostics.Enqueue { Attempt = entry.Request.Attempt; Failure = failure code error }

    let cancel (entry: Running) =
        if not entry.CancelRequested then
            entry.CancelRequested <- true
            // Publish the owned callback task before cleanup can check it.
            entry.Cancellation <- Some(Task.Run(fun () ->
                try entry.Source.Cancel()
                with error -> lock gate (fun () -> recordFailure entry "cancellation-callback" error)))

    let failProtocol (owner: Running option) error =
        if protocolFault.IsNone then
            // Keep failures only from ownership still present at the fault,
            // including an entry just physically removed before Drained failed.
            for entry in running.Values do shutdownErrors.AddRange entry.Failures
            match owner with
            | Some entry when not (running.ContainsKey entry.Request.Attempt) -> shutdownErrors.AddRange entry.Failures
            | _ -> ()
            protocolFault <- Some error
        shutdownErrors.Add error
        owner |> Option.iter (fun entry ->
            diagnostics.Enqueue { Attempt = entry.Request.Attempt; Failure = failure "host-protocol" error })
        for entry in running.Values do cancel entry

    // Called under gate. External evaluators and token callbacks never run here.
    let transition action =
        match protocolStep { Epoch = epoch; Action = action } state with
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
                    | true, entry -> cancel entry
                    | _ -> ()
                | _ -> ()
            Ok (effects, starts |> Seq.toList)

    let requireTransition action =
        match transition action with
        | Ok (_, starts) -> starts
        | Error error -> invalidOp $"Host/core protocol disagreement: {error}"

    let lifecycle owner action =
        if protocolFault.IsSome then []
        else
            try requireTransition action
            with error -> failProtocol owner error; []

    let rec launch (entries: Running list) =
        for entry in entries do
            // Always enter through the pool, even when the evaluator is synchronous.
            Task.Run(Func<Task>(fun () -> execute entry)) |> ignore

    and execute (entry: Running) : Task = task {
        let mutable acquired = false
        let mutable outcome = Completion.Cancelled
        try
            do! slots.WaitAsync(entry.Source.Token)
            acquired <- true
            // Scheduling admission is not authority for a later external effect.
            let admitted = lock gate (fun () -> not entry.CancelRequested && protocolFault.IsNone)
            if admitted then
                entry.Source.Token.ThrowIfCancellationRequested()
                let! value = evaluator entry.Request entry.Source.Token
                outcome <- Completion.Succeeded value
        with
        | :? OperationCanceledException when lock gate (fun () -> entry.CancelRequested || entry.Source.IsCancellationRequested) ->
            outcome <- Completion.Cancelled
        | error ->
            outcome <- Completion.Failed (failure "evaluation" error)
            lock gate (fun () -> recordFailure entry "evaluation" error)

        lock gate (fun () -> lifecycle (Some entry) (Action.Finished(entry.Request.Attempt, outcome))) |> launch

        // Even a rejected acknowledgement leaves physical ownership intact.
        // Cancellation callbacks can reenter the host, so await outside gate.
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
                        // A failing Drained acknowledgement must not schedule a
                        // cancellation against this already-disposed source.
                        running.Remove entry.Request.Attempt |> ignore
                        let starts = lifecycle (Some entry) (Action.Drained entry.Request.Attempt)
                        entry.Done.TrySetResult() |> ignore
                        drained <- true
                        None, starts)
            launch starts
            match waitFor with
            | Some work -> do! work
            | None -> ()
    }

    let joinPhysical (cancellationToken: CancellationToken) : Task = task {
        let mutable idle = false
        while not idle do
            let pending = lock gate (fun () ->
                running.Values |> Seq.map (fun item -> item.Done.Task :> Task) |> Seq.toArray)
            if pending.Length = 0 then idle <- true
            else do! Task.WhenAll(pending).WaitAsync(cancellationToken)
        cancellationToken.ThrowIfCancellationRequested()
    }

    let reportFault () = lock gate (fun () ->
        if protocolFault.IsSome then
            raise (AggregateException("Host protocol failed; owned execution and cancellation cleanup have joined.", shutdownErrors.ToArray())))

    new(epoch: EpochId, maxConcurrency: int, evaluator: Evaluator) =
        new Host(epoch, maxConcurrency, evaluator, Core.step)

    /// Applies a command atomically. Reservation withdraws eligibility before returning.
    /// Step suspension/resumption requires MailboxHost. Lifecycle acknowledgements
    /// belong to the host and cannot be supplied by its caller.
    member _.Send(action: Action) =
        match action with
        | Action.Finished _ | Action.Drained _ | Action.Suspend _ | Action.Resume _ ->
            invalidArg (nameof action) "The host owns lifecycle acknowledgements; use MailboxHost for explicit steps."
        | _ -> ()
        let result = lock gate (fun () ->
            match protocolFault with
            | Some error -> raise (InvalidOperationException("Host protocol is faulted.", error))
            | None when closing.IsSome -> invalidOp "Host is closing."
            | None ->
                try transition action
                with error ->
                    failProtocol None error
                    raise (InvalidOperationException("Host protocol is faulted.", error)))
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
    /// Protocol failure is reported after physical cleanup, never in place of it.
    member _.WaitForIdleAsync(cancellationToken: CancellationToken) : Task = task {
        do! joinPhysical cancellationToken
        reportFault ()
    }

    /// Retires the epoch and joins actual work. It does not forcefully preempt an
    /// evaluator that ignores its token. The owner must implement its cleanup contract.
    member _.CloseAsync() : Task =
        let shared, completion, starts = lock gate (fun () ->
            match closing with
            | Some work -> work, None, []
            | None ->
                let completion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let shared = completion.Task :> Task
                // Install before Retire: a rejected retirement still has one
                // stable owner-visible operation joining the remaining work.
                closing <- Some shared
                let starts = lifecycle None Action.Retire
                shared, Some completion, starts)
        launch starts
        completion |> Option.iter (fun completion ->
            let finish: Task = task {
                try
                    do! joinPhysical CancellationToken.None
                    slots.Dispose()
                    reportFault ()
                    completion.TrySetResult() |> ignore
                with error -> completion.TrySetException error |> ignore
            }
            // The task owns its exception path and never replaces the shared task.
            finish |> ignore)
        shared

    interface IAsyncDisposable with
        member this.DisposeAsync() = ValueTask(this.CloseAsync())
