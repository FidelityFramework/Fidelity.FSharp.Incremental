namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental

/// Compatibility boundary for callers whose owned work is expressed as a Task.
type StepEvaluator = StepInvocation -> CancellationToken -> Task<StepOutcome>

/// CLR adapter over AsyncMailbox. Construction starts the coordinator for
/// compatibility; functional callers use explicit create/start/admit operations.
type MailboxHost private (epoch: EpochId, maxConcurrency: int, commandCapacity: int, evaluator: StepEvaluator, journalOpt: IInputJournal option) =
    let handle =
        let settings: AsyncMailbox.Settings = {
            Epoch = epoch; MaxConcurrency = maxConcurrency; CommandCapacity = commandCapacity
        }
        let workflow invocation cancellation = ClrInterop.fromTask (evaluator invocation) cancellation
        let created =
            match journalOpt with
            | None -> AsyncMailbox.create settings workflow
            | Some journal -> AsyncMailbox.createWithJournal settings journal workflow
        match created with
        | Ok value -> value
        | Error AsyncMailbox.ConfigurationError.InvalidConcurrency ->
            invalidArg (nameof maxConcurrency) "Concurrency must be positive."
        | Error AsyncMailbox.ConfigurationError.InvalidCommandCapacity ->
            invalidArg (nameof commandCapacity) "Command capacity must be positive."

    let closeGate = obj ()
    let mutable closeTask: Task option = None
    let asLegacyCompletion workflow = async {
        let! result = workflow
        match result with
        | Ok () -> return ()
        | Error (failure: Failure) -> return raise (InvalidOperationException(failure.Message))
    }

    do
        journalOpt |> Option.iter (fun journal -> journal.BeginEpoch epoch)
        match AsyncMailbox.start handle with
        | Ok () -> ()
        | Error error -> invalidOp $"Could not start mailbox: {error}"

    new(epoch: EpochId, maxConcurrency: int, commandCapacity: int, evaluator: StepEvaluator) =
        new MailboxHost(epoch, maxConcurrency, commandCapacity, evaluator, None)

    new(epoch: EpochId, maxConcurrency: int, commandCapacity: int, evaluator: StepEvaluator, journal: IInputJournal) =
        if isNull (box journal) then nullArg (nameof journal)
        new MailboxHost(epoch, maxConcurrency, commandCapacity, evaluator, Some journal)

    /// Admission occurs at invocation, before any reply observation begins.
    member _.PostAsync(action: Action, ?cancellationToken: CancellationToken) =
        let token = defaultArg cancellationToken CancellationToken.None
        if token.IsCancellationRequested then
            Task.FromCanceled<Result<MailboxReceipt, MailboxError>>(token)
        else
            match AsyncMailbox.admit action handle with
            | Error error -> Task.FromResult(Error error)
            | Ok operation -> ClrInterop.toTask token (AsyncMailbox.observe operation)

    /// Demands work for a single request and enqueues Action.Release synchronously
    /// on the coordinator FIFO inbox before completing the returned Task.
    /// Each concurrent request must supply a distinct DemandId.
    member _.DemandAsync(demandId: DemandId, work: WorkId, ?cancellationToken: CancellationToken) =
        let token = defaultArg cancellationToken CancellationToken.None
        AsyncMailbox.demandTask demandId work token handle

    /// Runs observe under a request-scoped demand and enqueues Action.Release
    /// synchronously on the coordinator FIFO inbox before completing the returned Task.
    /// Each concurrent request must supply a distinct DemandId.
    member _.WithDemandAsync<'T>(demandId: DemandId, work: WorkId, observe: CancellationToken -> Task<'T>, ?cancellationToken: CancellationToken) =
        let token = defaultArg cancellationToken CancellationToken.None
        AsyncMailbox.withDemandTask
            demandId
            work
            (fun _ -> ClrInterop.fromUncancelledTask (fun () -> observe token))
            token
            handle

    member _.Snapshot = AsyncMailbox.snapshot handle
    member _.TryResult(work: WorkId) = AsyncMailbox.tryResult work handle
    member _.IsEligible(result: ResultHandle) = AsyncMailbox.isEligible result handle
    member _.DrainEvents() = AsyncMailbox.drainEvents handle
    member _.DrainDiagnostics() = AsyncMailbox.drainDiagnostics handle

    member _.WaitForIdleAsync(cancellationToken: CancellationToken) : Task =
        AsyncMailbox.waitForIdle handle
        |> asLegacyCompletion
        |> ClrInterop.toTask cancellationToken
        :> Task

    /// Seals admission immediately. The shared Task joins owned work even when
    /// individual callers stop observing it.
    member _.CloseAsync() : Task = lock closeGate (fun () ->
        match closeTask with
        | Some pending -> pending
        | None ->
            let operation = AsyncMailbox.beginClose handle
            let pending =
                AsyncMailbox.awaitClose operation
                |> asLegacyCompletion
                |> ClrInterop.toTask CancellationToken.None
                :> Task
            closeTask <- Some pending
            pending)

    interface IAsyncDisposable with
        member this.DisposeAsync() = ValueTask(this.CloseAsync())
