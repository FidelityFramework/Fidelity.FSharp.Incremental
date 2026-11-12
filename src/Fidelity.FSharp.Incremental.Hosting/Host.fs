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

[<NoEquality; NoComparison>]
type private DemandWaiter = {
    Work: WorkId
    Resolve: Choice<Result<ResultHandle, HostDemandError>, exn> -> unit
    mutable Node: LinkedListNode<DemandWaiter> option
}

[<RequireQualifiedAccess>]
type private DemandCheck =
    | Pending
    | Ready of Result<ResultHandle, HostDemandError>
    | Faulted of exn

/// Serializes the portable protocol and bounds actual evaluator ownership.
/// Observer cancellation only detaches the observer; Release removes demand.
type Host internal (epoch: EpochId, maxConcurrency: int, evaluator: Evaluator,
                    protocolStep: Command -> Core.State -> Result<Core.State * Effect list, ProtocolError>) =
    do if maxConcurrency < 1 then invalidArg (nameof maxConcurrency) "Concurrency must be positive."

    let gate = obj ()
    let slots = new SemaphoreSlim(maxConcurrency, maxConcurrency)
    let running = Dictionary<AttemptId, Running>()
    let demandWaiters = LinkedList<DemandWaiter>()
    let events = Queue<Effect>()
    let diagnostics = Queue<HostDiagnostic>()
    let mutable state = Core.init epoch
    let mutable closing: Task option = None
    let mutable protocolFault: exn option = None
    let shutdownErrors = ResizeArray<exn>()

    let failure code (error: exn) = { Code = code; Message = error.Message }

    let inspectDemand (work: WorkId) =
        match protocolFault with
        | Some error -> DemandCheck.Faulted(InvalidOperationException("Host protocol is faulted.", error))
        | None ->
            match Core.tryResult work state with
            | Some handle when Core.isEligible handle state -> DemandCheck.Ready(Ok handle)
            | _ when closing.IsSome -> DemandCheck.Ready(Error HostDemandError.Closed)
            | _ ->
                match (Core.snapshot state).Works |> List.tryFind (fun item -> item.Work = work) with
                | None -> DemandCheck.Ready(Error HostDemandError.WorkUnavailable)
                | Some { Status = WorkStatus.Eligible handle } when Core.isEligible handle state ->
                    DemandCheck.Ready(Ok handle)
                | Some { Status = WorkStatus.Failed detail } ->
                    DemandCheck.Ready(Error(HostDemandError.WorkFailed detail))
                | Some { Status = WorkStatus.Cancelled } ->
                    DemandCheck.Ready(Error HostDemandError.WorkCancelled)
                | Some _ -> DemandCheck.Pending

    let collectReadyWaiters () =
        if demandWaiters.Count = 0 then
            Array.empty
        else
            let ready = ResizeArray<DemandWaiter * Choice<Result<ResultHandle, HostDemandError>, exn>>()
            let mutable node = demandWaiters.First
            while not (isNull node) do
                let next = node.Next
                let waiter = node.Value
                match inspectDemand waiter.Work with
                | DemandCheck.Pending -> ()
                | DemandCheck.Ready outcome ->
                    demandWaiters.Remove node
                    waiter.Node <- None
                    ready.Add(waiter, Choice1Of2 outcome)
                | DemandCheck.Faulted error ->
                    demandWaiters.Remove node
                    waiter.Node <- None
                    ready.Add(waiter, Choice2Of2 error)
                node <- next
            ready.ToArray()

    let notifyWaiters (ready: (DemandWaiter * Choice<Result<ResultHandle, HostDemandError>, exn>)[]) =
        for waiter, outcome in ready do
            waiter.Resolve outcome

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

        let finishedStarts, finishedReady =
            lock gate (fun () ->
                let starts = lifecycle (Some entry) (Action.Finished(entry.Request.Attempt, outcome))
                starts, collectReadyWaiters ())
        launch finishedStarts
        notifyWaiters finishedReady

        // Even a rejected acknowledgement leaves physical ownership intact.
        // Cancellation callbacks can reenter the host, so await outside gate.
        let mutable drained = false
        while not drained do
            let waitFor, starts, ready =
                lock gate (fun () ->
                    match entry.Cancellation with
                    | Some work when not work.IsCompleted -> Some work, [], Array.empty
                    | _ ->
                        entry.Source.Dispose()
                        if acquired then
                            slots.Release() |> ignore
                            acquired <- false
                        // A failing Drained acknowledgement must not schedule a
                        // cancellation against this already-disposed source.
                        running.Remove entry.Request.Attempt |> ignore
                        let starts = lifecycle (Some entry) (Action.Drained entry.Request.Attempt)
                        let ready = collectReadyWaiters ()
                        entry.Done.TrySetResult() |> ignore
                        drained <- true
                        None, starts, ready)
            launch starts
            notifyWaiters ready
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
        let result, ready, faultToRaise = lock gate (fun () ->
            match protocolFault with
            | Some error -> Error ProtocolError.ForeignEpoch, Array.empty, Some(InvalidOperationException("Host protocol is faulted.", error))
            | None when closing.IsSome -> Error ProtocolError.RetiredEpoch, Array.empty, Some(InvalidOperationException("Host is closing."))
            | None ->
                try
                    let stepped = transition action
                    stepped, collectReadyWaiters (), None
                with error ->
                    failProtocol None error
                    Error ProtocolError.ForeignEpoch, collectReadyWaiters (), Some(InvalidOperationException("Host protocol is faulted.", error)))
        notifyWaiters ready
        match faultToRaise with
        | Some fault -> raise fault
        | None ->
            match result with
            | Error error -> Error error
            | Ok (effects, starts) -> launch starts; Ok effects

    /// Demands work for a single request and commits Action.Release synchronously
    /// prior to invoking TrySetResult, TrySetException, or TrySetCanceled on the
    /// outward Task. Cancelling one observer releases only its DemandId; each
    /// concurrent request must supply a distinct DemandId.
    member _.DemandAsync(demandId: DemandId, work: WorkId, ?cancellationToken: CancellationToken) : Task<Result<ResultHandle, HostDemandError>> =
        let token = defaultArg cancellationToken CancellationToken.None
        if token.IsCancellationRequested then
            Task.FromCanceled<Result<ResultHandle, HostDemandError>>(token)
        else
            let tcs = TaskCompletionSource<Result<ResultHandle, HostDemandError>>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable releaseLatch = 0
            let mutable registration = Unchecked.defaultof<CancellationTokenRegistration>
            let mutable hasRegistration = 0
            let unregister () =
                if Interlocked.Exchange(&hasRegistration, 0) <> 0 then
                    registration.Unregister() |> ignore
            let mutable waiterRef: DemandWaiter option = None
            let releaseSynchronously () =
                let starts, ready, releaseFault =
                    lock gate (fun () ->
                        waiterRef |> Option.iter (fun waiter ->
                            match waiter.Node with
                            | Some node ->
                                demandWaiters.Remove node
                                waiter.Node <- None
                            | None -> ())
                        if protocolFault.IsSome then
                            [], Array.empty, None
                        else
                            try
                                match transition (Action.Release demandId) with
                                | Ok (_, starts) -> starts, collectReadyWaiters (), None
                                | Error error ->
                                    let fault = InvalidOperationException($"Host/core protocol disagreement: {error}")
                                    failProtocol None fault
                                    [], collectReadyWaiters (), Some(InvalidOperationException("Host protocol is faulted.", fault))
                            with error ->
                                failProtocol None error
                                [], collectReadyWaiters (), Some(InvalidOperationException("Host protocol is faulted.", error)))
                launch starts
                notifyWaiters ready
                releaseFault
            let resolveWaiter (outcome: Choice<Result<ResultHandle, HostDemandError>, exn>) =
                if Interlocked.Exchange(&releaseLatch, 1) = 0 then
                    let releaseFault = releaseSynchronously ()
                    unregister ()
                    match releaseFault, outcome with
                    | Some fault, _ -> tcs.TrySetException(fault) |> ignore
                    | None, Choice1Of2 result -> tcs.TrySetResult(result) |> ignore
                    | None, Choice2Of2 fault -> tcs.TrySetException(fault) |> ignore
            let waiter = { Work = work; Resolve = resolveWaiter; Node = None }
            waiterRef <- Some waiter
            let immediate, starts, ready =
                lock gate (fun () ->
                    match protocolFault with
                    | Some error ->
                        Some(Choice2Of2(InvalidOperationException("Host protocol is faulted.", error) :> exn)), [], Array.empty
                    | None when closing.IsSome ->
                        Some(Choice1Of2(Error HostDemandError.Closed)), [], Array.empty
                    | None ->
                        try
                            match transition (Action.Demand(demandId, work)) with
                            | Error error ->
                                Some(Choice1Of2(Error(HostDemandError.InvalidCommand error))), [], Array.empty
                            | Ok (_, demandStarts) ->
                                match inspectDemand work with
                                | DemandCheck.Pending ->
                                    waiter.Node <- Some(demandWaiters.AddLast waiter)
                                    None, demandStarts, collectReadyWaiters ()
                                | DemandCheck.Ready outcome ->
                                    None, demandStarts, [| waiter, Choice1Of2 outcome |]
                                | DemandCheck.Faulted fault ->
                                    None, demandStarts, [| waiter, Choice2Of2 fault |]
                        with error ->
                            failProtocol None error
                            Some(Choice2Of2(InvalidOperationException("Host protocol is faulted.", error) :> exn)), [], collectReadyWaiters ())
            launch starts
            match immediate with
            | Some(Choice1Of2 outcome) ->
                notifyWaiters ready
                Interlocked.Exchange(&releaseLatch, 1) |> ignore
                tcs.TrySetResult(outcome) |> ignore
            | Some(Choice2Of2 fault) ->
                notifyWaiters ready
                Interlocked.Exchange(&releaseLatch, 1) |> ignore
                tcs.TrySetException(fault) |> ignore
            | None ->
                if ready.Length > 0 then
                    notifyWaiters ready
                if token.CanBeCanceled && Volatile.Read(&releaseLatch) = 0 then
                    let cancelWithRelease () =
                        if Interlocked.Exchange(&releaseLatch, 1) = 0 then
                            match releaseSynchronously () with
                            | Some fault -> tcs.TrySetException(fault) |> ignore
                            | None -> tcs.TrySetCanceled(token) |> ignore
                    let reg = token.Register(Action cancelWithRelease)
                    registration <- reg
                    Volatile.Write(&hasRegistration, 1)
                    if Volatile.Read(&releaseLatch) <> 0 then
                        unregister ()
            tcs.Task

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
        let shared, completion, starts, ready = lock gate (fun () ->
            match closing with
            | Some work -> work, None, [], Array.empty
            | None ->
                let completion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let shared = completion.Task :> Task
                // Install before Retire: a rejected retirement still has one
                // stable owner-visible operation joining the remaining work.
                closing <- Some shared
                let starts = lifecycle None Action.Retire
                shared, Some completion, starts, collectReadyWaiters ())
        launch starts
        notifyWaiters ready
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
