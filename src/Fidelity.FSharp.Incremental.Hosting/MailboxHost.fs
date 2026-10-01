namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Fidelity.FSharp.Incremental

[<RequireQualifiedAccess>]
type StepInvocation =
    | Start of StartRequest
    | Resume of ResumeRequest

/// Every return joins the step's children and cleanup. A suspension retains only
/// immutable data identified by Environment, never a live stack or resource handle.
[<RequireQualifiedAccess>]
type StepOutcome =
    | Complete of Completion
    | Suspend of StepId * ValueToken

type StepEvaluator = StepInvocation -> CancellationToken -> Task<StepOutcome>

[<RequireQualifiedAccess>]
type MailboxError =
    | QueueFull
    | Closed
    | LifecycleOwnedByHost
    | InvalidCommand of ProtocolError
    | Faulted of Failure

type MailboxReceipt = { Order: uint64; Effects: Effect list }

type MailboxSnapshot = {
    Graph: Snapshot
    QueuedCommands: int
    QueuedSteps: int
    RunningSteps: int
    Suspensions: SuspensionHandle list
    IsClosing: bool
}

type private MailboxEntry(request: StartRequest) =
    member _.Request = request
    member val Source = new CancellationTokenSource()
    member val Active = false with get, set
    member val Queued = false with get, set
    member val SlotHeld = false with get, set
    member val CancelRequested = false with get, set
    member val CancellationDone = true with get, set
    member val Terminal: Completion option = None with get, set

type private Envelope =
    | Command of uint64 * Action * TaskCompletionSource<Result<MailboxReceipt, MailboxError>>
    | StepCompleted of AttemptId * StepOutcome
    | CancellationCompleted of AttemptId * Failure option
    | Close

/// FIFO coordinator with bounded external admission and bounded physical steps.
/// Internal completion traffic cannot be denied by a full external command queue.
type MailboxHost(epoch: EpochId, maxConcurrency: int, commandCapacity: int, evaluator: StepEvaluator) =
    do
        if maxConcurrency < 1 then invalidArg (nameof maxConcurrency) "Concurrency must be positive."
        if commandCapacity < 1 then invalidArg (nameof commandCapacity) "Command capacity must be positive."

    let gate = obj ()
    let inbox = Channel.CreateUnbounded<Envelope>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = false))
    let entries = Dictionary<AttemptId, MailboxEntry>()
    let ready = Queue<AttemptId * StepInvocation>()
    let events = Queue<Effect>()
    let pendingEvents = ResizeArray<Effect>()
    let diagnostics = Queue<HostDiagnostic>()
    let closeDone = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let signal () = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable changed = signal ()
    let mutable state = Core.init epoch
    let mutable published = state
    let mutable queuedCommands = 0
    let mutable queuedSteps = 0
    let mutable publishedRunning = 0
    let mutable running = 0
    let mutable order = 0UL
    let mutable accepting = true
    let mutable closing = false
    let mutable retired = false
    let mutable fault: Failure option = None

    let failure code (error: exn) : Failure = { Code = code; Message = error.Message }
    let pulse () =
        let old = changed
        changed <- signal ()
        old.TrySetResult() |> ignore

    let publish consumedCommand = lock gate (fun () ->
        published <- state
        if consumedCommand then queuedCommands <- queuedCommands - 1
        queuedSteps <- entries.Values |> Seq.filter (fun item -> item.Queued && not item.CancelRequested) |> Seq.length
        publishedRunning <- running
        for effect in pendingEvents do events.Enqueue effect
        pendingEvents.Clear()
        pulse ())

    let diagnose attempt error = lock gate (fun () -> diagnostics.Enqueue { Attempt = attempt; Failure = error })
    let sendInternal message =
        if not (inbox.Writer.TryWrite message) then invalidOp "Owned work outlived its mailbox."

    // The coordinator never invokes a user evaluator or a cancellation callback.
    let cancel (entry: MailboxEntry) =
        if not entry.CancelRequested then
            entry.CancelRequested <- true
            entry.CancellationDone <- false
            if not entry.Active then entry.Terminal <- Some Completion.Cancelled
            Task.Run(fun () ->
                let error =
                    try entry.Source.Cancel(); None
                    with error -> Some(failure "cancellation-callback" error)
                sendInternal (CancellationCompleted(entry.Request.Attempt, error))) |> ignore

    let interpret effects =
        pendingEvents.AddRange effects
        for effect in effects do
            match effect.Action with
            | EffectAction.Start request ->
                let entry = MailboxEntry(request)
                entry.Queued <- true
                entries.Add(request.Attempt, entry)
                ready.Enqueue(request.Attempt, StepInvocation.Start request)
            | EffectAction.Continue request ->
                let entry = entries[request.Start.Attempt]
                entry.Queued <- true
                ready.Enqueue(request.Start.Attempt, StepInvocation.Resume request)
            | EffectAction.Cancel attempt ->
                match entries.TryGetValue attempt with
                | true, entry -> cancel entry
                | _ -> invalidOp "Core cancelled an attempt not owned by this host."
            | _ -> ()

    let transition action =
        match Core.step { Epoch = epoch; Action = action } state with
        | Error error -> Error error
        | Ok(next, effects) ->
            state <- next
            interpret effects
            Ok effects

    let require action =
        match transition action with
        | Ok effects -> effects
        | Error error -> invalidOp $"Mailbox/core protocol disagreement: {error}"

    let releaseSlot (entry: MailboxEntry) =
        if entry.SlotHeld then
            entry.SlotHeld <- false
            running <- running - 1

    // Called only after the evaluator task AND any owned Cancel callbacks have
    // returned. Completed cancellation alone never certifies evaluator cleanup.
    let finalize () =
        let completed =
            entries.Values
            |> Seq.filter (fun item -> not item.Active && item.Terminal.IsSome && item.CancellationDone)
            |> Seq.toArray
        for entry in completed do
            let attempt = entry.Request.Attempt
            entry.Source.Dispose()
            releaseSlot entry
            entries.Remove attempt |> ignore
            if fault.IsNone then
                require (Action.Finished(attempt, entry.Terminal.Value)) |> ignore
                require (Action.Drained attempt) |> ignore

    let pump () =
        while running < maxConcurrency && ready.Count > 0 do
            let attempt, invocation = ready.Dequeue()
            match entries.TryGetValue attempt with
            | true, entry when not entry.CancelRequested && fault.IsNone && (Core.tryActiveRequest attempt state).IsSome ->
                entry.Queued <- false
                entry.Active <- true
                entry.SlotHeld <- true
                running <- running + 1
                Task.Run(Func<Task>(fun () -> task {
                    let! outcome = task {
                        try
                            entry.Source.Token.ThrowIfCancellationRequested()
                            return! evaluator invocation entry.Source.Token
                        with
                        | :? OperationCanceledException when entry.Source.IsCancellationRequested ->
                            return StepOutcome.Complete Completion.Cancelled
                        | error -> return StepOutcome.Complete(Completion.Failed(failure "evaluation" error))
                    }
                    sendInternal (StepCompleted(attempt, outcome))
                })) |> ignore
            | true, entry -> entry.Queued <- false
            | _ -> ()

    let processStep attempt outcome =
        let entry = entries[attempt]
        entry.Active <- false
        match outcome with
        | StepOutcome.Complete completion ->
            match completion with
            | Completion.Failed error -> diagnose attempt error
            | _ -> ()
            entry.Terminal <- Some(
                match completion with
                | Completion.Succeeded _ when entry.CancelRequested -> Completion.Cancelled
                | other -> other)
        | StepOutcome.Suspend(step, environment) ->
            if not entry.CancelRequested && fault.IsNone && (Core.tryActiveRequest attempt state).IsSome then
                require (Action.Suspend(attempt, step, environment)) |> ignore
                releaseSlot entry
            else
                // A valid old step can finish after reservation/close. Its state
                // cannot become a new continuation, but it still acknowledges cleanup.
                entry.Terminal <- Some Completion.Cancelled

    let failCoordinator (error: exn) =
        let detail = failure "host-protocol" error
        lock gate (fun () ->
            fault <- Some detail
            accepting <- false
            closing <- true
            pulse ())
        retired <- true
        // Fail closed, but keep the inbox alive to join all real owned work.
        // Do not invent Core.Drained certificates after a protocol disagreement.
        for entry in entries.Values do
            diagnose entry.Request.Attempt detail
            cancel entry

    let coordinator = task {
        let mutable stopped = false
        while not stopped do
            let! message = inbox.Reader.ReadAsync().AsTask()
            let mutable reply: (TaskCompletionSource<Result<MailboxReceipt, MailboxError>> * Result<MailboxReceipt, MailboxError>) option = None
            try
                match message with
                | Command(sequence, action, completion) ->
                    let result =
                        match fault with
                        | Some error -> Error(MailboxError.Faulted error)
                        | None ->
                            match transition action with
                            | Ok effects -> Ok { Order = sequence; Effects = effects }
                            | Error error -> Error(MailboxError.InvalidCommand error)
                    reply <- Some(completion, result)
                | StepCompleted(attempt, outcome) -> processStep attempt outcome
                | CancellationCompleted(attempt, error) ->
                    let entry = entries[attempt]
                    entry.CancellationDone <- true
                    error |> Option.iter (diagnose attempt)
                | Close ->
                    if fault.IsNone then require Action.Retire |> ignore
                    retired <- true
                finalize ()
                pump ()
            with error ->
                failCoordinator error
                match message with
                | Command(_, _, completion) -> reply <- Some(completion, Error(MailboxError.Faulted fault.Value))
                | _ -> ()
                finalize ()
            publish (match message with Command _ -> true | _ -> false)
            // State (including withdrawals) is visible before acknowledgement.
            reply |> Option.iter (fun (completion, result) -> completion.TrySetResult result |> ignore)
            let canStop = lock gate (fun () -> retired && entries.Count = 0 && queuedCommands = 0)
            if canStop then
                stopped <- true
                ready.Clear()
                inbox.Writer.TryComplete() |> ignore
                match fault with
                | Some error -> closeDone.TrySetException(InvalidOperationException(error.Message)) |> ignore
                | None -> closeDone.TrySetResult() |> ignore
    }

    do
        // Keep a reference and surface an unexpected coordinator crash to closers.
        coordinator.ContinueWith((fun (finished: Task) ->
            if finished.IsFaulted then closeDone.TrySetException(finished.Exception.InnerExceptions) |> ignore), TaskScheduler.Default) |> ignore

    /// Immediate bounded admission. QueueFull means nothing was enqueued. Once
    /// enqueued, cancellation detaches only this acknowledgement observer. A caller
    /// must receive ReserveScope success before treating its edit as reserved.
    member _.PostAsync(action: Action, ?cancellationToken: CancellationToken) : Task<Result<MailboxReceipt, MailboxError>> =
        let token = defaultArg cancellationToken CancellationToken.None
        if token.IsCancellationRequested then Task.FromCanceled<Result<MailboxReceipt, MailboxError>>(token)
        else
            let pending = lock gate (fun () ->
                match fault, action with
                | _ when token.IsCancellationRequested -> Task.FromCanceled<Result<MailboxReceipt, MailboxError>>(token)
                | Some error, _ -> Task.FromResult(Error(MailboxError.Faulted error))
                | _, (Action.Finished _ | Action.Drained _ | Action.Suspend _) -> Task.FromResult(Error MailboxError.LifecycleOwnedByHost)
                | _ when not accepting -> Task.FromResult(Error MailboxError.Closed)
                | _ when queuedCommands >= commandCapacity -> Task.FromResult(Error MailboxError.QueueFull)
                | _ when order = UInt64.MaxValue -> Task.FromResult(Error(MailboxError.InvalidCommand ProtocolError.IdentifierExhausted))
                | _ ->
                    let completion = TaskCompletionSource<Result<MailboxReceipt, MailboxError>>(TaskCreationOptions.RunContinuationsAsynchronously)
                    order <- order + 1UL
                    queuedCommands <- queuedCommands + 1
                    if not (inbox.Writer.TryWrite(Command(order, action, completion))) then
                        queuedCommands <- queuedCommands - 1
                        completion.TrySetResult(Error MailboxError.Closed) |> ignore
                    pulse ()
                    completion.Task)
            if token.CanBeCanceled then pending.WaitAsync(token) else pending

    member _.Snapshot = lock gate (fun () ->
        let graph = Core.snapshot published
        {
            Graph = graph
            QueuedCommands = queuedCommands
            QueuedSteps = queuedSteps
            RunningSteps = publishedRunning
            Suspensions = graph.Works |> List.choose (fun work ->
                match work.Status with WorkStatus.AwaitingResume handle -> Some handle | _ -> None)
            IsClosing = closing
        })

    member _.TryResult(work: WorkId) = lock gate (fun () -> if fault.IsSome then None else Core.tryResult work published)
    member _.IsEligible(handle: ResultHandle) = lock gate (fun () -> fault.IsNone && Core.isEligible handle published)
    member _.DrainEvents() = lock gate (fun () ->
        let result = events |> Seq.toList
        events.Clear()
        result)
    member _.DrainDiagnostics() = lock gate (fun () ->
        let result = diagnostics |> Seq.toList
        diagnostics.Clear()
        result)

    /// Suspended logical attempts are not idle. Cancellation detaches the observer.
    /// As with any idle observation, another client may submit new work afterwards.
    member _.WaitForIdleAsync(cancellationToken: CancellationToken) : Task = task {
        let mutable idle = false
        while not idle do
            let next = lock gate (fun () ->
                match fault with
                | Some error -> raise (InvalidOperationException(error.Message))
                | None when Core.pendingAttempts published = 0 && queuedCommands = 0 -> None
                | None -> Some changed.Task)
            match next with
            | None -> idle <- true
            | Some signal -> do! signal.WaitAsync(cancellationToken)
        cancellationToken.ThrowIfCancellationRequested()
    }

    /// Closes admission, processes already accepted commands, retires the epoch,
    /// and joins real evaluator/cancellation cleanup. A noncooperative step can
    /// delay close indefinitely; no synthetic cleanup acknowledgement is issued.
    member _.CloseAsync() : Task = lock gate (fun () ->
        if accepting then
            accepting <- false
            closing <- true
            sendInternal Close
            pulse ()
        closeDone.Task)

    interface IAsyncDisposable with
        member this.DisposeAsync() = ValueTask(this.CloseAsync())
