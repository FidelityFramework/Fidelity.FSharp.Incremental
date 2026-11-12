namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental

/// Per-epoch outcome emitted by Diagnostic or Reenact journal replay.
type EpochReplayReport = {
    Epoch: EpochId
    Effects: Effect list
    Causes: CausalEdge list
    FinalSnapshot: Snapshot
}

/// Multi-epoch outcome emitted by Diagnostic or Reenact journal replay.
type ReplayReport = {
    Version: JournalVersion
    Epochs: EpochReplayReport list
    Causes: CausalEdge list
}

/// Live Host configuration used during Reenact replay.
type ReenactHostConfig = {
    MaxConcurrency: int
    ResolveInput: EpochId -> InputId -> InputStamp -> ValueToken
    Evaluator: EpochId -> Evaluator
}

/// Live AsyncMailbox configuration used during Reenact replay.
type ReenactMailboxConfig = {
    MaxConcurrency: int
    CommandCapacity: int
    ResolveInput: EpochId -> InputId -> InputStamp -> ValueToken
    ResolveResume: EpochId -> SuspensionFrame -> ValueToken
    Evaluator: EpochId -> WorkflowEvaluator
}

/// Pure Diagnostic and live Reenact replay runners for V1 journals. Reenact
/// checks observations at barriers and segment ends; their ordering relative to
/// an external command between barriers cannot be established atomically.
[<RequireQualifiedAccess>]
module Replay =
    let private extractEntryEpoch = function
        | JournalEntry.BeginEpoch epoch
        | JournalEntry.ExternalCommand(epoch, _)
        | JournalEntry.HostObservation(epoch, _)
        | JournalEntry.LabelRegistered(epoch, _) -> epoch
        | JournalEntry.Causal edge -> edge.Epoch

    let private segmentEpochs (entries: JournalEntry list) : Result<(EpochId * JournalEntry list) list, JournalError> =
        match entries with
        | [] -> Ok []
        | _ :: _ ->
            let mutable seen: Set<EpochId> = Set.empty
            let mutable maxEpoch: uint64 option = None
            let mutable currentEpoch: EpochId option = None
            let mutable currentItems = ResizeArray<JournalEntry>()
            let segments = ResizeArray<EpochId * JournalEntry list>()

            let flushCurrent () =
                match currentEpoch with
                | None -> ()
                | Some epoch ->
                    segments.Add(epoch, currentItems |> Seq.toList)
                    currentItems <- ResizeArray<JournalEntry>()

            let openEpoch (epoch: EpochId) =
                let (EpochId raw) = epoch
                if Set.contains epoch seen then
                    Error(JournalError.Protocol ProtocolError.RetiredEpoch)
                else
                    match maxEpoch with
                    | Some previous when raw <= previous ->
                        Error(JournalError.Protocol ProtocolError.RetiredEpoch)
                    | _ ->
                        flushCurrent ()
                        seen <- Set.add epoch seen
                        maxEpoch <- Some raw
                        currentEpoch <- Some epoch
                        Ok()

            let rec loop remaining =
                match remaining with
                | [] ->
                    flushCurrent ()
                    Ok(segments |> Seq.toList)
                | entry :: rest ->
                    match entry with
                    | JournalEntry.BeginEpoch epoch ->
                        match openEpoch epoch with
                        | Error err -> Error err
                        | Ok () -> loop rest
                    | other ->
                        let ensured =
                            match currentEpoch with
                            | Some _ -> Ok()
                            | None -> Error JournalError.MissingBeginEpoch
                        match ensured with
                        | Error err -> Error err
                        | Ok () ->
                            currentItems.Add other
                            loop rest

            loop entries

    /// Pure replay over Core.stepWithCauses without executing any host evaluators.
    let diagnostic (entries: JournalEntry list) : Result<ReplayReport, JournalError> =
        match segmentEpochs entries with
        | Error err -> Error err
        | Ok segments ->
            let rec runSegments (labels: Map<WorkId * ReadSlotId, string>) acc allCauses = function
                | [] ->
                    Ok {
                        Version = JournalVersion.V1
                        Epochs = List.rev acc
                        Causes = List.rev allCauses
                    }
                | (epoch, segmentEntries) :: restSegments ->
                    let rec stepEntries state currentLabels effects causes = function
                        | [] ->
                            let report = {
                                Epoch = epoch
                                Effects = List.rev effects
                                Causes = List.rev causes
                                FinalSnapshot = Journal.canonicalizeSnapshot (Core.snapshot state)
                            }
                            Ok(report, currentLabels, causes @ allCauses)
                        | item :: tail ->
                            match item with
                            | JournalEntry.BeginEpoch _ ->
                                stepEntries state currentLabels effects causes tail
                            | JournalEntry.LabelRegistered(lblEpoch, label) ->
                                if lblEpoch <> epoch then
                                    Error(JournalError.Protocol ProtocolError.ForeignEpoch)
                                else
                                    let updated = Map.add (label.Work, label.Slot) label.Label currentLabels
                                    stepEntries state updated effects causes tail
                            | JournalEntry.Causal edge ->
                                if edge.Epoch <> epoch then
                                    Error(JournalError.Protocol ProtocolError.ForeignEpoch)
                                else
                                    stepEntries state currentLabels effects causes tail
                            | JournalEntry.ExternalCommand(cmdEpoch, actionFrame) ->
                                let command = Journal.toDiagnosticCommand cmdEpoch (Choice1Of2 actionFrame)
                                match Core.stepWithCauses currentLabels command state with
                                | Error protocolErr -> Error(JournalError.Protocol protocolErr)
                                | Ok(nextState, emitted, stepCauses) ->
                                    let canonicalEmitted = emitted |> List.map Journal.canonicalizeEffect
                                    stepEntries
                                        nextState
                                        currentLabels
                                        (List.rev canonicalEmitted @ effects)
                                        (List.rev stepCauses @ causes)
                                        tail
                            | JournalEntry.HostObservation(obsEpoch, obsFrame) ->
                                let command = Journal.toDiagnosticCommand obsEpoch (Choice2Of2 obsFrame)
                                match Core.stepWithCauses currentLabels command state with
                                | Error protocolErr -> Error(JournalError.Protocol protocolErr)
                                | Ok(nextState, emitted, stepCauses) ->
                                    let canonicalEmitted = emitted |> List.map Journal.canonicalizeEffect
                                    stepEntries
                                        nextState
                                        currentLabels
                                        (List.rev canonicalEmitted @ effects)
                                        (List.rev stepCauses @ causes)
                                        tail

                    match stepEntries (Core.init epoch) labels [] [] segmentEntries with
                    | Error err -> Error err
                    | Ok(report, updatedLabels, updatedAllCauses) ->
                        runSegments updatedLabels (report :: acc) updatedAllCauses restSegments

            runSegments Map.empty [] [] segments

    /// Decodes a V1 byte array and runs pure Diagnostic replay over Core.stepWithCauses.
    let diagnosticBytes (bytes: byte[]) : Result<ReplayReport, JournalError> =
        match JournalCodec.decodeV1 bytes with
        | Error err -> Error err
        | Ok(headerEpoch, entries) ->
            match segmentEpochs entries with
            | Error err -> Error err
            | Ok [] ->
                Ok {
                    Version = JournalVersion.V1
                    Epochs = []
                    Causes = []
                }
            | Ok _ ->
                match entries with
                | JournalEntry.BeginEpoch firstEpoch :: _ when firstEpoch <> headerEpoch ->
                    Error(JournalError.Protocol ProtocolError.ForeignEpoch)
                | _ -> diagnostic entries

    type private ReenactObserver(initialLabels: Map<WorkId * ReadSlotId, string>) =
        let gate = obj ()
        let signal = new SemaphoreSlim(0)
        let causes = ResizeArray<CausalEdge>()
        let mutable labels = initialLabels
        let observations = ResizeArray<ObservationFrame>()

        member _.LabelsSnapshot = lock gate (fun () -> labels)
        member _.CausesSnapshot = lock gate (fun () -> causes |> Seq.toList)

        member _.WaitForObservationsAsync(expected: ResizeArray<ObservationFrame>, idle: Task, cancellationToken: CancellationToken) = task {
            let mutable result = None
            while result.IsNone do
                let actual = lock gate (fun () -> observations.ToArray())
                let mismatch =
                    actual
                    |> Array.mapi (fun i value -> i, value)
                    |> Array.tryFind (fun (i, value) -> i >= expected.Count || expected[i] <> value)
                match mismatch with
                | Some(index, value) ->
                    result <- Some(Error(JournalError.ReplayDivergence(sprintf "Unexpected observation at %d: %A" index value)))
                | None when actual.Length = expected.Count -> result <- Some(Ok())
                | None when idle.IsCompleted ->
                    do! idle
                    let latest = lock gate (fun () -> observations.ToArray())
                    if latest = actual then
                        result <- Some(Error(JournalError.ReplayDivergence(sprintf "Idle before observation %A" expected[actual.Length])))
                | None ->
                    let wait = signal.WaitAsync(cancellationToken)
                    let! _ = Task.WhenAny(wait, idle)
                    ()
            return result.Value
        }

        interface IDisposable with
            member _.Dispose() = signal.Dispose()

        interface IInputJournal with
            member _.Version = JournalVersion.V1
            member _.BeginEpoch _ = ()
            member _.Flush() = ()
            member _.Labels() = lock gate (fun () -> labels)
            member _.Synchronize action = lock gate action

            member _.RecordLabel(label: TraceLabel) =
                lock gate (fun () ->
                    labels <- Map.add (label.Work, label.Slot) label.Label labels)

            member _.Record(entry: JournalEntry) =
                let mutable notify = false
                lock gate (fun () ->
                    match entry with
                    | JournalEntry.HostObservation(_, observation) ->
                        observations.Add observation
                        notify <- true
                    | JournalEntry.Causal edge ->
                        causes.Add edge
                    | JournalEntry.LabelRegistered(_, label) ->
                        labels <- Map.add (label.Work, label.Slot) label.Label labels
                    | JournalEntry.ExternalCommand _
                    | JournalEntry.BeginEpoch _ -> ())
                if notify then
                    signal.Release() |> ignore

    let private toLiveAction
        (epoch: EpochId)
        (resolveInput: EpochId -> InputId -> InputStamp -> ValueToken)
        (resolveSuspension: SuspensionFrame -> SuspensionHandle * ValueToken)
        (frame: ExternalActionFrame) : Action =
        match frame with
        | ExternalActionFrame.ReserveScope(scope, revision) ->
            Action.ReserveScope(scope, revision)
        | ExternalActionFrame.ReplaceScope(scope, revision, entries) ->
            Action.ReplaceScope(scope, revision, entries)
        | ExternalActionFrame.SetInputs frames ->
            let inputs =
                frames
                |> List.map (fun item -> {
                    Input = item.Input
                    Stamp = item.Stamp
                    Value = resolveInput epoch item.Input item.Stamp
                })
            Action.SetInputs inputs
        | ExternalActionFrame.RemoveInputs ids ->
            Action.RemoveInputs ids
        | ExternalActionFrame.Demand(demandId, work) ->
            Action.Demand(demandId, work)
        | ExternalActionFrame.Release demandId ->
            Action.Release demandId
        | ExternalActionFrame.Retry work ->
            Action.Retry work
        | ExternalActionFrame.Resume suspension ->
            let handle, response = resolveSuspension suspension
            Action.Resume(handle, response)
        | ExternalActionFrame.CloseScope scope ->
            Action.CloseScope scope
        | ExternalActionFrame.Retire ->
            Action.Retire

    /// Reenacts a journal stream against live Host instances across epochs.
    let reenactWithHost
        (config: ReenactHostConfig)
        (entries: JournalEntry list)
        (cancellationToken: CancellationToken) : Task<Result<ReplayReport, JournalError>> = task {
        match segmentEpochs entries with
        | Error err -> return Error err
        | Ok segments ->
            let reports = ResizeArray<EpochReplayReport>()
            let allCauses = ResizeArray<CausalEdge>()
            let mutable carriedLabels: Map<WorkId * ReadSlotId, string> = Map.empty
            let mutable failure: JournalError option = None
            let mutable segIdx = 0

            while segIdx < segments.Length && failure.IsNone do
                let epoch, segEntries = segments[segIdx]
                let items = segEntries |> List.toArray
                use observer = new ReenactObserver(carriedLabels)
                let host = new Host(epoch, config.MaxConcurrency, config.Evaluator epoch, observer)
                let mutable closedByRetire = false
                let expected = ResizeArray<ObservationFrame>()
                let mutable cursor = 0

                try
                    while cursor < items.Length && failure.IsNone do
                        match items[cursor] with
                        | JournalEntry.BeginEpoch _ ->
                            cursor <- cursor + 1
                        | JournalEntry.LabelRegistered(lblEpoch, label) ->
                            if lblEpoch <> epoch then
                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                            else
                                (observer :> IInputJournal).RecordLabel label
                                cursor <- cursor + 1
                        | JournalEntry.Causal edge ->
                            if edge.Epoch <> epoch then
                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                            else
                                cursor <- cursor + 1
                        | JournalEntry.ExternalCommand(cmdEpoch, extFrame) ->
                            if cmdEpoch <> epoch then
                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                            else
                                match extFrame with
                                | ExternalActionFrame.Resume _ ->
                                    failure <- Some(JournalError.ReplayUnsupported "Action.Resume requires reenactWithMailbox.")
                                | ExternalActionFrame.Retire ->
                                    do! host.CloseAsync().WaitAsync(cancellationToken)
                                    closedByRetire <- true
                                    cursor <- cursor + 1
                                | other ->
                                    let action =
                                        toLiveAction
                                            epoch
                                            config.ResolveInput
                                            (fun s ->
                                                { Epoch = s.Epoch; Attempt = s.Attempt; Id = s.Id; Step = s.Step; Environment = Journal.canonicalToken },
                                                Journal.canonicalToken)
                                            other
                                    match host.Send action with
                                    | Error protocolErr ->
                                        failure <- Some(JournalError.Protocol protocolErr)
                                    | Ok _ ->
                                        cursor <- cursor + 1
                        | JournalEntry.HostObservation _ ->
                            let mutable scanning = true
                            while cursor < items.Length && scanning && failure.IsNone do
                                match items[cursor] with
                                | JournalEntry.HostObservation(obsEpoch, obs) ->
                                    if obsEpoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        expected.Add obs
                                        cursor <- cursor + 1
                                | JournalEntry.LabelRegistered(lblEpoch, label) ->
                                    if lblEpoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        (observer :> IInputJournal).RecordLabel label
                                        cursor <- cursor + 1
                                | JournalEntry.Causal edge ->
                                    if edge.Epoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        cursor <- cursor + 1
                                | JournalEntry.ExternalCommand _
                                | JournalEntry.BeginEpoch _ ->
                                    scanning <- false
                            if failure.IsNone then
                                let idle = host.WaitForIdleAsync(cancellationToken)
                                let! outcome = observer.WaitForObservationsAsync(expected, idle, cancellationToken)
                                match outcome with Error err -> failure <- Some err | Ok () -> ()

                    if failure.IsNone then
                        let idle = host.WaitForIdleAsync(cancellationToken)
                        do! idle
                        let! outcome = observer.WaitForObservationsAsync(expected, idle, cancellationToken)
                        match outcome with Error err -> failure <- Some err | Ok () -> ()

                    if failure.IsNone then
                        let effects = host.DrainEvents() |> List.map Journal.canonicalizeEffect
                        let finalSnapshot = host.Snapshot |> Journal.canonicalizeSnapshot
                        let epochCauses = observer.CausesSnapshot
                        carriedLabels <- observer.LabelsSnapshot
                        reports.Add {
                            Epoch = epoch
                            Effects = effects
                            Causes = epochCauses
                            FinalSnapshot = finalSnapshot
                        }
                        allCauses.AddRange epochCauses
                finally
                    if not closedByRetire then
                        host.CloseAsync().GetAwaiter().GetResult()

                segIdx <- segIdx + 1

            match failure with
            | Some err -> return Error err
            | None ->
                return Ok {
                    Version = JournalVersion.V1
                    Epochs = reports |> Seq.toList
                    Causes = allCauses |> Seq.toList
                }
    }

    /// Decodes a V1 byte array and reenacts it against live Host instances.
    let reenactBytesWithHost
        (config: ReenactHostConfig)
        (bytes: byte[])
        (cancellationToken: CancellationToken) : Task<Result<ReplayReport, JournalError>> = task {
        match JournalCodec.decodeV1 bytes with
        | Error err -> return Error err
        | Ok(headerEpoch, entries) ->
            match entries with
            | [] ->
                return Ok {
                    Version = JournalVersion.V1
                    Epochs = []
                    Causes = []
                }
            | first :: _ when extractEntryEpoch first <> headerEpoch ->
                return Error(JournalError.Protocol ProtocolError.ForeignEpoch)
            | _ ->
                return! reenactWithHost config entries cancellationToken
    }

    /// Reenacts a journal stream against live AsyncMailbox coordinators across epochs.
    let reenactWithMailbox
        (config: ReenactMailboxConfig)
        (entries: JournalEntry list)
        (cancellationToken: CancellationToken) : Task<Result<ReplayReport, JournalError>> = task {
        match segmentEpochs entries with
        | Error err -> return Error err
        | Ok segments ->
            let reports = ResizeArray<EpochReplayReport>()
            let allCauses = ResizeArray<CausalEdge>()
            let mutable carriedLabels: Map<WorkId * ReadSlotId, string> = Map.empty
            let mutable failure: JournalError option = None
            let mutable segIdx = 0

            while segIdx < segments.Length && failure.IsNone do
                let epoch, segEntries = segments[segIdx]
                let items = segEntries |> List.toArray
                use observer = new ReenactObserver(carriedLabels)
                let settings: AsyncMailbox.Settings = {
                    Epoch = epoch
                    MaxConcurrency = config.MaxConcurrency
                    CommandCapacity = config.CommandCapacity
                }
                match AsyncMailbox.createWithJournal settings observer (config.Evaluator epoch) with
                | Error AsyncMailbox.ConfigurationError.InvalidConcurrency ->
                    failure <- Some(JournalError.ReplayHostRejected "Invalid mailbox MaxConcurrency.")
                | Error AsyncMailbox.ConfigurationError.InvalidCommandCapacity ->
                    failure <- Some(JournalError.ReplayHostRejected "Invalid mailbox CommandCapacity.")
                | Ok handle ->
                    match AsyncMailbox.start handle with
                    | Error(MailboxError.InvalidCommand protocolErr) ->
                        failure <- Some(JournalError.Protocol protocolErr)
                    | Error(MailboxError.Faulted detail) ->
                        failure <- Some(JournalError.HostFailure detail)
                    | Error mbErr ->
                        failure <- Some(JournalError.ReplayHostRejected(sprintf "Could not start mailbox: %A" mbErr))
                    | Ok () ->
                        let mutable closedByRetire = false
                        let expected = ResizeArray<ObservationFrame>()
                        let mutable cursor = 0
                        let waitForQuiescence () = task {
                            let mutable doneWaiting = false
                            while not doneWaiting do
                                let quiescent, changed = AsyncMailbox.watchQuiescence handle
                                if quiescent then doneWaiting <- true
                                else do! ClrInterop.toTask cancellationToken changed
                        }
                        try
                            while cursor < items.Length && failure.IsNone do
                                match items[cursor] with
                                | JournalEntry.BeginEpoch _ ->
                                    cursor <- cursor + 1
                                | JournalEntry.LabelRegistered(lblEpoch, label) ->
                                    if lblEpoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        (observer :> IInputJournal).RecordLabel label
                                        cursor <- cursor + 1
                                | JournalEntry.Causal edge ->
                                    if edge.Epoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        cursor <- cursor + 1
                                | JournalEntry.ExternalCommand(cmdEpoch, extFrame) ->
                                    if cmdEpoch <> epoch then
                                        failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                    else
                                        match extFrame with
                                        | ExternalActionFrame.Retire ->
                                            let! closed = ClrInterop.toTask cancellationToken (AsyncMailbox.close handle)
                                            closedByRetire <- true
                                            match closed with
                                            | Ok () -> cursor <- cursor + 1
                                            | Error detail -> failure <- Some(JournalError.HostFailure detail)
                                        | other ->
                                            let action =
                                                match other with
                                                | ExternalActionFrame.Resume s ->
                                                    (AsyncMailbox.snapshot handle).Suspensions
                                                    |> List.tryFind (fun item ->
                                                        item.Epoch = s.Epoch && item.Attempt = s.Attempt && item.Id = s.Id && item.Step = s.Step)
                                                    |> Option.map (fun live -> Action.Resume(live, config.ResolveResume epoch s))
                                                | _ ->
                                                    Some(toLiveAction epoch config.ResolveInput (fun _ -> invalidOp "Unexpected resume") other)
                                            match action with
                                            | None ->
                                                failure <- Some(JournalError.ReplayDivergence(sprintf "Missing live suspension: %A" other))
                                            | Some action ->
                                             match AsyncMailbox.admit action handle with
                                             | Error(MailboxError.InvalidCommand protocolErr) ->
                                                failure <- Some(JournalError.Protocol protocolErr)
                                             | Error(MailboxError.Faulted detail) ->
                                                failure <- Some(JournalError.HostFailure detail)
                                             | Error mbErr ->
                                                failure <- Some(JournalError.ReplayHostRejected(sprintf "Mailbox rejected admission: %A" mbErr))
                                             | Ok op ->
                                                let! receipt = ClrInterop.toTask cancellationToken (AsyncMailbox.observe op)
                                                match receipt with
                                                | Ok _ -> cursor <- cursor + 1
                                                | Error(MailboxError.InvalidCommand protocolErr) ->
                                                    failure <- Some(JournalError.Protocol protocolErr)
                                                | Error(MailboxError.Faulted detail) ->
                                                    failure <- Some(JournalError.HostFailure detail)
                                                | Error mbErr ->
                                                    failure <- Some(JournalError.ReplayHostRejected(sprintf "Mailbox command error: %A" mbErr))
                                | JournalEntry.HostObservation _ ->
                                    let mutable scanning = true
                                    while cursor < items.Length && scanning && failure.IsNone do
                                        match items[cursor] with
                                        | JournalEntry.HostObservation(obsEpoch, obs) ->
                                            if obsEpoch <> epoch then
                                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                            else
                                                expected.Add obs
                                                cursor <- cursor + 1
                                        | JournalEntry.LabelRegistered(lblEpoch, label) ->
                                            if lblEpoch <> epoch then
                                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                            else
                                                (observer :> IInputJournal).RecordLabel label
                                                cursor <- cursor + 1
                                        | JournalEntry.Causal edge ->
                                            if edge.Epoch <> epoch then
                                                failure <- Some(JournalError.Protocol ProtocolError.ForeignEpoch)
                                            else
                                                cursor <- cursor + 1
                                        | JournalEntry.ExternalCommand _
                                        | JournalEntry.BeginEpoch _ ->
                                            scanning <- false
                                    if failure.IsNone then
                                        let idle = waitForQuiescence ()
                                        let! outcome = observer.WaitForObservationsAsync(expected, idle :> Task, cancellationToken)
                                        match outcome with Error err -> failure <- Some err | Ok () -> ()

                            if failure.IsNone then
                                let idle = waitForQuiescence ()
                                do! idle
                                let! outcome = observer.WaitForObservationsAsync(expected, idle :> Task, cancellationToken)
                                match outcome with Error err -> failure <- Some err | Ok () -> ()

                            if failure.IsNone then
                                let effects = AsyncMailbox.drainEvents handle |> List.map Journal.canonicalizeEffect
                                let finalSnapshot = (AsyncMailbox.snapshot handle).Graph |> Journal.canonicalizeSnapshot
                                let epochCauses = observer.CausesSnapshot
                                carriedLabels <- observer.LabelsSnapshot
                                reports.Add {
                                    Epoch = epoch
                                    Effects = effects
                                    Causes = epochCauses
                                    FinalSnapshot = finalSnapshot
                                }
                                allCauses.AddRange epochCauses
                        finally
                            if not closedByRetire then
                                (ClrInterop.toTask CancellationToken.None (AsyncMailbox.close handle)).GetAwaiter().GetResult() |> ignore

                segIdx <- segIdx + 1

            match failure with
            | Some err -> return Error err
            | None ->
                return Ok {
                    Version = JournalVersion.V1
                    Epochs = reports |> Seq.toList
                    Causes = allCauses |> Seq.toList
                }
    }

    /// Decodes a V1 byte array and reenacts it against live AsyncMailbox coordinators.
    let reenactBytesWithMailbox
        (config: ReenactMailboxConfig)
        (bytes: byte[])
        (cancellationToken: CancellationToken) : Task<Result<ReplayReport, JournalError>> = task {
        match JournalCodec.decodeV1 bytes with
        | Error err -> return Error err
        | Ok(headerEpoch, entries) ->
            match entries with
            | [] ->
                return Ok {
                    Version = JournalVersion.V1
                    Epochs = []
                    Causes = []
                }
            | first :: _ when extractEntryEpoch first <> headerEpoch ->
                return Error(JournalError.Protocol ProtocolError.ForeignEpoch)
            | _ ->
                return! reenactWithMailbox config entries cancellationToken
    }
