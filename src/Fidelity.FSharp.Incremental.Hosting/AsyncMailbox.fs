namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading
open Fidelity.FSharp.Incremental

/// Each return includes every owned child and cleanup operation. Cancellation is
/// an explicit request; the workflow keeps ownership until it actually returns.
type WorkflowEvaluator = StepInvocation -> WorkCancellation -> Async<StepOutcome>

/// Functional access to one owned coordinator. Creating a handle is cold;
/// starting, admitting a command and beginning close are explicit operations.
[<RequireQualifiedAccess>]
module AsyncMailbox =
    type Settings = {
        Epoch: EpochId
        MaxConcurrency: int
        CommandCapacity: int
    }

    [<RequireQualifiedAccess>]
    type ConfigurationError =
        | InvalidConcurrency
        | InvalidCommandCapacity

    [<NoEquality; NoComparison>]
    type Operation = private {
        Order: uint64
        Outcome: AsyncCell.Cell<Result<MailboxReceipt, MailboxError>>
    }

    [<NoEquality; NoComparison>]
    type CloseOperation = private {
        Outcome: AsyncCell.Cell<Result<unit, Failure>>
    }

    type private Entry = {
        Request: StartRequest
        Cancellation: WorkCancellation
        mutable Active: bool
        mutable Queued: bool
        mutable SlotHeld: bool
        mutable CancelRequested: bool
        mutable CancellationDone: bool
        mutable Terminal: Completion option
    }

    type private Envelope =
        | Command of Action * Operation
        | StepCompleted of AttemptId * StepOutcome
        | CancellationCompleted of AttemptId * Failure option
        | Close

    type private Runtime = {
        Settings: Settings
        Evaluator: WorkflowEvaluator
        Step: Command -> Core.State -> Result<Core.State * Effect list, ProtocolError>
        Gate: obj
        Entries: Dictionary<AttemptId, Entry>
        Ready: Queue<AttemptId * StepInvocation>
        Events: Queue<Effect>
        PendingEvents: ResizeArray<Effect>
        Diagnostics: Queue<HostDiagnostic>
        Close: CloseOperation
        mutable Changed: AsyncCell.Cell<unit>
        mutable Inbox: MailboxProcessor<Envelope> option
        mutable State: Core.State
        mutable Published: Core.State
        mutable QueuedCommands: int
        mutable QueuedSteps: int
        mutable PublishedRunning: int
        mutable Running: int
        mutable Order: uint64
        mutable Started: bool
        mutable Accepting: bool
        mutable Closing: bool
        mutable Retired: bool
        mutable Fault: Failure option
    }

    [<NoEquality; NoComparison>]
    type Handle = private { Runtime: Runtime }

    let private failure code (error: exn) : Failure =
        { Code = code; Message = error.Message }

    // AsyncCell queues its observers; publication never runs their code under
    // the admission lock or on the coordinator's state transition stack.
    let private pulse runtime =
        let old = runtime.Changed
        runtime.Changed <- AsyncCell.create ()
        AsyncCell.publish () old |> ignore

    let private publish consumedCommand runtime = lock runtime.Gate (fun () ->
        runtime.Published <- runtime.State
        if consumedCommand then runtime.QueuedCommands <- runtime.QueuedCommands - 1
        runtime.QueuedSteps <-
            runtime.Entries.Values
            |> Seq.filter (fun entry -> entry.Queued && not entry.CancelRequested)
            |> Seq.length
        runtime.PublishedRunning <- runtime.Running
        for effect in runtime.PendingEvents do runtime.Events.Enqueue effect
        runtime.PendingEvents.Clear()
        pulse runtime)

    let private diagnose attempt detail runtime = lock runtime.Gate (fun () ->
        runtime.Diagnostics.Enqueue { Attempt = attempt; Failure = detail })

    let private sendInternal message runtime =
        match runtime.Inbox with
        | Some inbox -> inbox.Post message
        | None -> invalidOp "Owned work has no started coordinator."

    // Record the explicit request synchronously. CLR cancellation callbacks run
    // outside the coordinator and remain a separate part of physical ownership.
    let private cancel (entry: Entry) runtime =
        if not entry.CancelRequested then
            entry.CancelRequested <- true
            entry.CancellationDone <- false
            ExecutionBoundary.markRequested entry.Cancellation
            if not entry.Active then entry.Terminal <- Some Completion.Cancelled
            let finished detail =
                sendInternal (CancellationCompleted(entry.Request.Attempt, detail)) runtime
            ExecutionBoundary.run
                (async { ExecutionBoundary.cancelCallbacks entry.Cancellation })
                (fun () -> finished None)
                (fun error -> finished (Some(failure "cancellation-callback" error)))
                (fun error -> finished (Some(failure "cancellation-callback" error)))

    let private interpret effects runtime =
        runtime.PendingEvents.AddRange effects
        for effect in effects do
            match effect.Action with
            | EffectAction.Start request ->
                let entry = {
                    Request = request
                    Cancellation = ExecutionBoundary.createCancellation ()
                    Active = false
                    Queued = true
                    SlotHeld = false
                    CancelRequested = false
                    CancellationDone = true
                    Terminal = None
                }
                runtime.Entries.Add(request.Attempt, entry)
                runtime.Ready.Enqueue(request.Attempt, StepInvocation.Start request)
            | EffectAction.Continue request ->
                let entry = runtime.Entries[request.Start.Attempt]
                entry.Queued <- true
                runtime.Ready.Enqueue(request.Start.Attempt, StepInvocation.Resume request)
            | EffectAction.Cancel attempt ->
                match runtime.Entries.TryGetValue attempt with
                | true, entry -> cancel entry runtime
                | _ -> invalidOp "Core cancelled an attempt not owned by this host."
            | _ -> ()

    let private transition action runtime =
        match runtime.Step { Epoch = runtime.Settings.Epoch; Action = action } runtime.State with
        | Error error -> Error error
        | Ok(next, effects) ->
            runtime.State <- next
            interpret effects runtime
            Ok effects

    let private require action runtime =
        match transition action runtime with
        | Ok effects -> effects
        | Error error -> invalidOp $"Mailbox/core protocol disagreement: {error}"

    let private releaseSlot (entry: Entry) runtime =
        if entry.SlotHeld then
            entry.SlotHeld <- false
            runtime.Running <- runtime.Running - 1

    // A rejected lifecycle acknowledgement cannot undo physical completion or
    // justify abandoning another evaluator/callback that is still owned.
    let private finalize runtime =
        let completed =
            runtime.Entries.Values
            |> Seq.filter (fun entry -> not entry.Active && entry.Terminal.IsSome && entry.CancellationDone)
            |> Seq.toArray
        for entry in completed do
            let attempt = entry.Request.Attempt
            ExecutionBoundary.dispose entry.Cancellation
            releaseSlot entry runtime
            runtime.Entries.Remove attempt |> ignore
            if runtime.Fault.IsNone then
                require (Action.Finished(attempt, entry.Terminal.Value)) runtime |> ignore
                require (Action.Drained attempt) runtime |> ignore

    let private pump runtime =
        while runtime.Running < runtime.Settings.MaxConcurrency && runtime.Ready.Count > 0 do
            let attempt, invocation = runtime.Ready.Dequeue()
            match runtime.Entries.TryGetValue attempt with
            | true, entry when not entry.CancelRequested && runtime.Fault.IsNone && (Core.tryActiveRequest attempt runtime.State).IsSome ->
                entry.Queued <- false
                entry.Active <- true
                entry.SlotHeld <- true
                runtime.Running <- runtime.Running + 1
                let completed outcome = sendInternal (StepCompleted(attempt, outcome)) runtime
                let failed (error: exn) =
                    let completion =
                        match error with
                        | :? OperationCanceledException when WorkCancellation.isRequested entry.Cancellation -> Completion.Cancelled
                        | _ -> Completion.Failed(failure "evaluation" error)
                    completed (StepOutcome.Complete completion)
                ExecutionBoundary.run
                    (async {
                        // Dispatch is not effect authority. This check prevents
                        // already withdrawn queued dispatch from invoking user code.
                        if WorkCancellation.isRequested entry.Cancellation then
                            return StepOutcome.Complete Completion.Cancelled
                        else
                            return! runtime.Evaluator invocation entry.Cancellation
                    })
                    completed
                    failed
                    (fun error -> failed error)
            | true, entry -> entry.Queued <- false
            | _ -> ()

    let private processStep attempt outcome runtime =
        let entry = runtime.Entries[attempt]
        entry.Active <- false
        match outcome with
        | StepOutcome.Complete completion ->
            match completion with
            | Completion.Failed error -> diagnose attempt error runtime
            | _ -> ()
            entry.Terminal <- Some(
                match completion with
                | Completion.Succeeded _ when entry.CancelRequested -> Completion.Cancelled
                | other -> other)
        | StepOutcome.Suspend(step, environment) ->
            if not entry.CancelRequested && runtime.Fault.IsNone && (Core.tryActiveRequest attempt runtime.State).IsSome then
                require (Action.Suspend(attempt, step, environment)) runtime |> ignore
                releaseSlot entry runtime
            else
                entry.Terminal <- Some Completion.Cancelled

    let private failCoordinator (error: exn) runtime =
        let detail = failure "host-protocol" error
        lock runtime.Gate (fun () ->
            if runtime.Fault.IsNone then runtime.Fault <- Some detail
            runtime.Accepting <- false
            runtime.Closing <- true
            pulse runtime)
        runtime.Retired <- true
        // Keep receiving until all physical ownership joins. Never fabricate
        // Finished/Drained acknowledgements to conceal protocol disagreement.
        for entry in runtime.Entries.Values do
            diagnose entry.Request.Attempt detail runtime
            cancel entry runtime

    let private completeClose runtime =
        let outcome =
            match runtime.Fault with
            | Some error -> Error error
            | None -> Ok ()
        AsyncCell.publish outcome runtime.Close.Outcome |> ignore

    let private coordinator runtime (inbox: MailboxProcessor<Envelope>) = async {
        let mutable stopped = false
        while not stopped do
            let! message = inbox.Receive()
            let mutable reply: (Operation * Result<MailboxReceipt, MailboxError>) option = None
            try
                match message with
                | Command(action, operation) ->
                    let result =
                        match runtime.Fault with
                        | Some error -> Error(MailboxError.Faulted error)
                        | None ->
                            match transition action runtime with
                            | Ok effects -> Ok { Order = operation.Order; Effects = effects }
                            | Error error -> Error(MailboxError.InvalidCommand error)
                    reply <- Some(operation, result)
                | StepCompleted(attempt, outcome) -> processStep attempt outcome runtime
                | CancellationCompleted(attempt, error) ->
                    let entry = runtime.Entries[attempt]
                    entry.CancellationDone <- true
                    error |> Option.iter (fun detail -> diagnose attempt detail runtime)
                | Close ->
                    if runtime.Fault.IsNone then require Action.Retire runtime |> ignore
                    runtime.Retired <- true
                finalize runtime
                pump runtime
            with error ->
                failCoordinator error runtime
                match message with
                | Command(_, operation) -> reply <- Some(operation, Error(MailboxError.Faulted runtime.Fault.Value))
                | _ -> ()
                finalize runtime
            publish (match message with Command _ -> true | _ -> false) runtime
            // Withdrawals and state publication precede exact acknowledgement.
            reply |> Option.iter (fun (operation, result) -> AsyncCell.publish result operation.Outcome |> ignore)
            let canStop = lock runtime.Gate (fun () ->
                runtime.Retired && runtime.Entries.Count = 0 && runtime.QueuedCommands = 0)
            if canStop then
                stopped <- true
                runtime.Ready.Clear()
                inbox.Dispose()
                completeClose runtime
    }

    let internal createWithStep settings evaluator protocolStep =
        if settings.MaxConcurrency < 1 then Error ConfigurationError.InvalidConcurrency
        elif settings.CommandCapacity < 1 then Error ConfigurationError.InvalidCommandCapacity
        else
            let state = Core.init settings.Epoch
            Ok { Runtime = {
                Settings = settings
                Evaluator = evaluator
                Step = protocolStep
                Gate = obj ()
                Entries = Dictionary<AttemptId, Entry>()
                Ready = Queue<AttemptId * StepInvocation>()
                Events = Queue<Effect>()
                PendingEvents = ResizeArray<Effect>()
                Diagnostics = Queue<HostDiagnostic>()
                Close = { Outcome = AsyncCell.create () }
                Changed = AsyncCell.create ()
                Inbox = None
                State = state
                Published = state
                QueuedCommands = 0
                QueuedSteps = 0
                PublishedRunning = 0
                Running = 0
                Order = 0UL
                Started = false
                Accepting = true
                Closing = false
                Retired = false
                Fault = None
            } }

    /// Validate a cold description. No coordinator or evaluator starts here.
    let create settings evaluator = createWithStep settings evaluator Core.step

    /// Start the owned coordinator once. A closed handle cannot be restarted.
    let start handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            match runtime.Fault with
            | Some error -> Error(MailboxError.Faulted error)
            | None when not runtime.Accepting -> Error MailboxError.Closed
            | None when runtime.Started -> Ok ()
            | None ->
                let inbox = new MailboxProcessor<Envelope>(coordinator runtime, cancellationToken = CancellationToken.None)
                runtime.Inbox <- Some inbox
                runtime.Started <- true
                inbox.Start()
                Ok ())

    /// Immediate bounded admission. The returned handle retains the exact
    /// command's result independently of any current or future observer.
    let admit action handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            match runtime.Fault, action with
            | Some error, _ -> Error(MailboxError.Faulted error)
            | _ when not runtime.Accepting -> Error MailboxError.Closed
            | _ when not runtime.Started -> Error MailboxError.NotStarted
            | _, (Action.Finished _ | Action.Drained _ | Action.Suspend _) -> Error MailboxError.LifecycleOwnedByHost
            | _ when runtime.QueuedCommands >= runtime.Settings.CommandCapacity -> Error MailboxError.QueueFull
            | _ when runtime.Order = UInt64.MaxValue -> Error(MailboxError.InvalidCommand ProtocolError.IdentifierExhausted)
            | _ ->
                runtime.Order <- runtime.Order + 1UL
                let operation = { Order = runtime.Order; Outcome = AsyncCell.create () }
                runtime.QueuedCommands <- runtime.QueuedCommands + 1
                sendInternal (Command(action, operation)) runtime
                pulse runtime
                Ok operation)

    /// Cancelling this cold, repeatable observation does not retract admission.
    let observe (operation: Operation) = AsyncCell.observe operation.Outcome

    let tryObserve (operation: Operation) = AsyncCell.tryRead operation.Outcome

    let operationOrder (operation: Operation) = operation.Order

    /// Seal admission immediately and retain one physical join operation.
    /// Closing a cold handle retires its empty graph without starting a mailbox.
    let beginClose handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            if runtime.Accepting then
                runtime.Accepting <- false
                runtime.Closing <- true
                if runtime.Started then sendInternal Close runtime
                else
                    try
                        require Action.Retire runtime |> ignore
                        runtime.Retired <- true
                    with error -> failCoordinator error runtime
                    publish false runtime
                    completeClose runtime
                pulse runtime
            runtime.Close)

    /// Cancellation detaches this observer; owned cleanup continues to its end.
    let awaitClose (operation: CloseOperation) = AsyncCell.observe operation.Outcome

    let tryClose (operation: CloseOperation) = AsyncCell.tryRead operation.Outcome

    /// Cold convenience: admission is sealed only when this workflow executes.
    let close handle = async {
        let operation = beginClose handle
        return! awaitClose operation
    }

    type private IdleObservation =
        | Idle
        | Changed of AsyncCell.Cell<unit>
        | FaultJoin of CloseOperation

    /// Suspensions remain outstanding. Protocol failure is reported only after
    /// all actual owned work joins, even if the graph cannot certify drain.
    let rec waitForIdle handle = async {
        let runtime = handle.Runtime
        let observation = lock runtime.Gate (fun () ->
            match runtime.Fault with
            | Some _ -> FaultJoin runtime.Close
            | None when Core.pendingAttempts runtime.Published = 0 && runtime.QueuedCommands = 0 -> Idle
            | None -> Changed runtime.Changed)
        match observation with
        | Idle -> return Ok ()
        | FaultJoin operation -> return! awaitClose operation
        | Changed signal ->
            do! AsyncCell.observe signal
            return! waitForIdle handle
    }

    let private readSnapshot runtime =
        let graph = Core.snapshot runtime.Published
        {
            Graph = graph
            QueuedCommands = runtime.QueuedCommands
            QueuedSteps = runtime.QueuedSteps
            RunningSteps = runtime.PublishedRunning
            Suspensions = graph.Works |> List.choose (fun work ->
                match work.Status with WorkStatus.AwaitingResume suspension -> Some suspension | _ -> None)
            IsClosing = runtime.Closing
        }

    let snapshot handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () -> readSnapshot runtime)

    /// Capture the published state and its next change notification together.
    /// A change before observation starts is retained, avoiding a lost wakeup.
    /// The cold notification is repeatable; cancelling it detaches only that
    /// observer. It neither admits work nor consumes events. Recheck the owned
    /// condition after every wakeup; a wakeup is not result or effect authority.
    /// When IsClosing is true, observe beginClose/awaitClose instead: there may
    /// be no further state change after the physical close has completed.
    let watch handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () -> readSnapshot runtime, AsyncCell.observe runtime.Changed)

    let tryResult work handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            if runtime.Fault.IsSome then None else Core.tryResult work runtime.Published)

    let isEligible result handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            runtime.Fault.IsNone && Core.isEligible result runtime.Published)

    /// One owner drains observations and fans them out to any other clients.
    let drainEvents handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            let events = runtime.Events |> Seq.toList
            runtime.Events.Clear()
            events)

    let drainDiagnostics handle =
        let runtime = handle.Runtime
        lock runtime.Gate (fun () ->
            let diagnostics = runtime.Diagnostics |> Seq.toList
            runtime.Diagnostics.Clear()
            diagnostics)
