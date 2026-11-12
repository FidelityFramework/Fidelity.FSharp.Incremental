namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Buffers.Binary
open System.Collections.Generic
open System.IO
open System.Text
open Fidelity.FSharp.Incremental

/// Wire version for serialized command boundary journals.
[<RequireQualifiedAccess>]
type JournalVersion =
    | V1

/// Serialized input stamp entry with zero ValueToken retention at rest.
type InputValueFrame = {
    Input: InputId
    Stamp: InputStamp
}

/// Serialized suspension checkpoint identity with zero ValueToken retention at rest.
type SuspensionFrame = {
    Epoch: EpochId
    Attempt: AttemptId
    Id: SuspensionId
    Step: StepId
}

/// Serialized attempt completion with zero ValueToken retention at rest.
[<RequireQualifiedAccess>]
type CompletionFrame =
    | Succeeded
    | Failed of Failure
    | Cancelled

/// External coordinator commands admitted across the host boundary.
[<RequireQualifiedAccess>]
type ExternalActionFrame =
    | ReserveScope of ScopeId * RevisionId
    | ReplaceScope of ScopeId * RevisionId * ScopeEntry list
    | SetInputs of InputValueFrame list
    | RemoveInputs of InputId list
    | Demand of DemandId * WorkId
    | Release of DemandId
    | Retry of WorkId
    | Resume of SuspensionFrame
    | CloseScope of ScopeId
    | Retire

/// Host-owned lifecycle observations recorded for deterministic replay.
[<RequireQualifiedAccess>]
type ObservationFrame =
    | Suspended of AttemptId * StepId
    | Finished of AttemptId * CompletionFrame
    | Drained of AttemptId

/// Versioned journal stream entry free of host payload references and ValueTokens.
[<RequireQualifiedAccess>]
type JournalEntry =
    | BeginEpoch of EpochId
    | ExternalCommand of EpochId * ExternalActionFrame
    | HostObservation of EpochId * ObservationFrame
    | LabelRegistered of EpochId * TraceLabel
    | Causal of CausalEdge

/// Deterministic decode and protocol validation errors for journal streams.
[<RequireQualifiedAccess>]
type JournalError =
    | InvalidHeader of string
    | UnsupportedVersion of uint16
    | TruncatedFrame of index: int
    | CorruptFrame of index: int * string
    | HostFailure of Failure
    | Protocol of ProtocolError
    | MissingBeginEpoch
    | ReplayDivergence of string
    | ReplayUnsupported of string
    | ReplayHostRejected of string

/// Boundary recorder capturing protocol commands, lifecycle observations, and causal labels.
type IInputJournal =
    abstract Version: JournalVersion
    abstract BeginEpoch: EpochId -> unit
    abstract Record: JournalEntry -> unit
    abstract RecordLabel: TraceLabel -> unit
    abstract Labels: unit -> Map<WorkId * ReadSlotId, string>
    abstract Synchronize<'T>: (unit -> 'T) -> 'T
    abstract Flush: unit -> unit

/// Conversion, canonicalization, and Core.step wrapping helpers for boundary journals.
[<RequireQualifiedAccess>]
module Journal =
    [<Literal>]
    let private canonicalRawToken = 0UL

    let canonicalToken = ValueToken canonicalRawToken

    /// Strips host-specific ValueTokens from a Command into a journal entry.
    let fromCommand (command: Command) : JournalEntry =
        let epoch = command.Epoch
        match command.Action with
        | Action.ReserveScope(scope, revision) ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReserveScope(scope, revision))
        | Action.ReplaceScope(scope, revision, entries) ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReplaceScope(scope, revision, entries))
        | Action.SetInputs values ->
            let frames =
                values
                |> List.map (fun item -> { Input = item.Input; Stamp = item.Stamp })
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.SetInputs frames)
        | Action.RemoveInputs ids ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.RemoveInputs ids)
        | Action.Demand(demandId, work) ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Demand(demandId, work))
        | Action.Release demandId ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Release demandId)
        | Action.Retry work ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Retry work)
        | Action.Suspend(attempt, step, _) ->
            JournalEntry.HostObservation(epoch, ObservationFrame.Suspended(attempt, step))
        | Action.Resume(handle, _) ->
            let frame = {
                Epoch = handle.Epoch
                Attempt = handle.Attempt
                Id = handle.Id
                Step = handle.Step
            }
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Resume frame)
        | Action.Finished(attempt, completion) ->
            let frame =
                match completion with
                | Completion.Succeeded _ -> CompletionFrame.Succeeded
                | Completion.Failed detail -> CompletionFrame.Failed detail
                | Completion.Cancelled -> CompletionFrame.Cancelled
            JournalEntry.HostObservation(epoch, ObservationFrame.Finished(attempt, frame))
        | Action.Drained attempt ->
            JournalEntry.HostObservation(epoch, ObservationFrame.Drained attempt)
        | Action.CloseScope scope ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.CloseScope scope)
        | Action.Retire ->
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Retire)

    /// Reconstructs a pure Command using canonical placeholder tokens for diagnostic replay.
    let toDiagnosticCommand (epoch: EpochId) (choice: Choice<ExternalActionFrame, ObservationFrame>) : Command =
        let action =
            match choice with
            | Choice1Of2 externalAction ->
                match externalAction with
                | ExternalActionFrame.ReserveScope(scope, revision) ->
                    Action.ReserveScope(scope, revision)
                | ExternalActionFrame.ReplaceScope(scope, revision, entries) ->
                    Action.ReplaceScope(scope, revision, entries)
                | ExternalActionFrame.SetInputs frames ->
                    let values =
                        frames
                        |> List.map (fun item -> {
                            Input = item.Input
                            Stamp = item.Stamp
                            Value = canonicalToken
                        })
                    Action.SetInputs values
                | ExternalActionFrame.RemoveInputs ids ->
                    Action.RemoveInputs ids
                | ExternalActionFrame.Demand(demandId, work) ->
                    Action.Demand(demandId, work)
                | ExternalActionFrame.Release demandId ->
                    Action.Release demandId
                | ExternalActionFrame.Retry work ->
                    Action.Retry work
                | ExternalActionFrame.Resume frame ->
                    let handle = {
                        Epoch = frame.Epoch
                        Attempt = frame.Attempt
                        Id = frame.Id
                        Step = frame.Step
                        Environment = canonicalToken
                    }
                    Action.Resume(handle, canonicalToken)
                | ExternalActionFrame.CloseScope scope ->
                    Action.CloseScope scope
                | ExternalActionFrame.Retire ->
                    Action.Retire
            | Choice2Of2 observation ->
                match observation with
                | ObservationFrame.Suspended(attempt, step) ->
                    Action.Suspend(attempt, step, canonicalToken)
                | ObservationFrame.Finished(attempt, completion) ->
                    let mapped =
                        match completion with
                        | CompletionFrame.Succeeded -> Completion.Succeeded canonicalToken
                        | CompletionFrame.Failed detail -> Completion.Failed detail
                        | CompletionFrame.Cancelled -> Completion.Cancelled
                    Action.Finished(attempt, mapped)
                | ObservationFrame.Drained attempt ->
                    Action.Drained attempt
        { Epoch = epoch; Action = action }

    let private canonicalizeRead (resolved: ResolvedRead) : ResolvedRead =
        let value =
            match resolved.Value with
            | ReadValue.Input(id, stamp, _) -> ReadValue.Input(id, stamp, canonicalToken)
            | ReadValue.Work(id, stamp, attempt, _) -> ReadValue.Work(id, stamp, attempt, canonicalToken)
        { Slot = resolved.Slot; Value = value }

    let private canonicalizeStart (request: StartRequest) : StartRequest =
        { request with Reads = request.Reads |> List.map canonicalizeRead }

    let private canonicalizeSuspension (handle: SuspensionHandle) : SuspensionHandle =
        { handle with Environment = canonicalToken }

    let private canonicalizeResult (handle: ResultHandle) : ResultHandle =
        { handle with Value = canonicalToken }

    /// Scrubs host-specific ValueTokens from an Effect for deterministic parity checks.
    let canonicalizeEffect (effect: Effect) : Effect =
        let action =
            match effect.Action with
            | EffectAction.Start request ->
                EffectAction.Start(canonicalizeStart request)
            | EffectAction.Suspended handle ->
                EffectAction.Suspended(canonicalizeSuspension handle)
            | EffectAction.Continue request ->
                EffectAction.Continue {
                    Start = canonicalizeStart request.Start
                    Suspension = canonicalizeSuspension request.Suspension
                    Response = canonicalToken
                }
            | EffectAction.Cancel attempt ->
                EffectAction.Cancel attempt
            | EffectAction.Drain attempt ->
                EffectAction.Drain attempt
            | EffectAction.Withdraw handle ->
                EffectAction.Withdraw(canonicalizeResult handle)
            | EffectAction.Offer handle ->
                EffectAction.Offer(canonicalizeResult handle)
            | EffectAction.ScopeClosed scope ->
                EffectAction.ScopeClosed scope
            | EffectAction.EpochDrained ->
                EffectAction.EpochDrained
        { Epoch = effect.Epoch; Action = action }

    /// Scrubs host-specific ValueTokens from a Snapshot for deterministic parity checks.
    let canonicalizeSnapshot (snapshot: Snapshot) : Snapshot =
        let works =
            snapshot.Works
            |> List.map (fun item ->
                let status =
                    match item.Status with
                    | WorkStatus.Eligible handle -> WorkStatus.Eligible(canonicalizeResult handle)
                    | WorkStatus.AwaitingResume handle -> WorkStatus.AwaitingResume(canonicalizeSuspension handle)
                    | other -> other
                { item with Status = status })
        { snapshot with Works = works }

    /// Wraps Core.stepWithCauses so accepted transitions and causal edges record into journal.
    let wrapStep
        (journal: IInputJournal)
        (stepWithCauses: Map<WorkId * ReadSlotId, string> -> Command -> Core.State -> Result<Core.State * Effect list * CausalEdge list, ProtocolError>)
        : Command -> Core.State -> Result<Core.State * Effect list, ProtocolError> =
        fun command state ->
            journal.Synchronize(fun () ->
                let labels = journal.Labels()
                match stepWithCauses labels command state with
                | Error error -> Error error
                | Ok(next, effects, causes) ->
                    journal.BeginEpoch command.Epoch
                    journal.Record(fromCommand command)
                    for edge in causes do
                        journal.Record(JournalEntry.Causal edge)
                    Ok(next, effects))

    /// Pure Core.step adapter backed by journal recording.
    let step (journal: IInputJournal) : Command -> Core.State -> Result<Core.State * Effect list, ProtocolError> =
        wrapStep journal Core.stepWithCauses

/// Binary V1 encoder and decoder for JournalEntry streams.
[<RequireQualifiedAccess>]
module JournalCodec =
    let private utf8 = UTF8Encoding(false, true)
    let private magic0 = byte 'F'
    let private magic1 = byte 'F'
    let private magic2 = byte 'I'
    let private magic3 = byte 'J'
    let private versionV1 = 1us

    let private writeString (writer: BinaryWriter) (value: string) =
        if isNull value then nullArg (nameof value)
        let bytes = utf8.GetBytes value
        writer.Write(uint32 bytes.Length)
        writer.Write bytes

    let private writeOption (writer: BinaryWriter) (writeValue: BinaryWriter -> 'T -> unit) (value: 'T option) =
        match value with
        | None -> writer.Write 0uy
        | Some inner ->
            writer.Write 1uy
            writeValue writer inner

    let private encodeEntryPayload (entry: JournalEntry) : byte[] =
        use buffer = new MemoryStream()
        use writer = new BinaryWriter(buffer, Encoding.UTF8, true)
        match entry with
        | JournalEntry.BeginEpoch(EpochId epoch) ->
            writer.Write 1uy
            writer.Write epoch
        | JournalEntry.ExternalCommand(EpochId epoch, action) ->
            writer.Write 2uy
            writer.Write epoch
            match action with
            | ExternalActionFrame.ReserveScope(ScopeId scope, RevisionId revision) ->
                writer.Write 1uy
                writer.Write scope
                writer.Write revision
            | ExternalActionFrame.ReplaceScope(ScopeId scope, RevisionId revision, entries) ->
                writer.Write 2uy
                writer.Write scope
                writer.Write revision
                writer.Write(uint32 entries.Length)
                for scopeEntry in entries do
                    match scopeEntry with
                    | ScopeEntry.Retain(WorkId work) ->
                        writer.Write 1uy
                        writer.Write work
                    | ScopeEntry.Define definition ->
                        let (WorkId work) = definition.Work
                        let (DefinitionStamp stamp) = definition.Stamp
                        writer.Write 2uy
                        writer.Write work
                        writer.Write stamp
                        writer.Write(uint32 definition.Reads.Length)
                        for read in definition.Reads do
                            let (ReadSlotId slot) = read.Slot
                            writer.Write slot
                            match read.Source with
                            | ReadSource.Input(InputId input) ->
                                writer.Write 1uy
                                writer.Write input
                            | ReadSource.Work(WorkId upstream) ->
                                writer.Write 2uy
                                writer.Write upstream
            | ExternalActionFrame.SetInputs inputs ->
                writer.Write 3uy
                writer.Write(uint32 inputs.Length)
                for item in inputs do
                    let (InputId input) = item.Input
                    let (InputStamp stamp) = item.Stamp
                    writer.Write input
                    writer.Write stamp
            | ExternalActionFrame.RemoveInputs ids ->
                writer.Write 4uy
                writer.Write(uint32 ids.Length)
                for InputId id in ids do
                    writer.Write id
            | ExternalActionFrame.Demand(DemandId demandId, WorkId work) ->
                writer.Write 5uy
                writer.Write demandId
                writer.Write work
            | ExternalActionFrame.Release(DemandId demandId) ->
                writer.Write 6uy
                writer.Write demandId
            | ExternalActionFrame.Retry(WorkId work) ->
                writer.Write 7uy
                writer.Write work
            | ExternalActionFrame.Resume suspension ->
                let (EpochId sEpoch) = suspension.Epoch
                let (AttemptId attempt) = suspension.Attempt
                let (SuspensionId suspId) = suspension.Id
                let (StepId step) = suspension.Step
                writer.Write 8uy
                writer.Write sEpoch
                writer.Write attempt
                writer.Write suspId
                writer.Write step
            | ExternalActionFrame.CloseScope(ScopeId scope) ->
                writer.Write 9uy
                writer.Write scope
            | ExternalActionFrame.Retire ->
                writer.Write 10uy
        | JournalEntry.HostObservation(EpochId epoch, observation) ->
            writer.Write 3uy
            writer.Write epoch
            match observation with
            | ObservationFrame.Suspended(AttemptId attempt, StepId step) ->
                writer.Write 1uy
                writer.Write attempt
                writer.Write step
            | ObservationFrame.Finished(AttemptId attempt, completion) ->
                writer.Write 2uy
                writer.Write attempt
                match completion with
                | CompletionFrame.Succeeded ->
                    writer.Write 1uy
                | CompletionFrame.Failed detail ->
                    writer.Write 2uy
                    writeString writer detail.Code
                    writeString writer detail.Message
                | CompletionFrame.Cancelled ->
                    writer.Write 3uy
            | ObservationFrame.Drained(AttemptId attempt) ->
                writer.Write 3uy
                writer.Write attempt
        | JournalEntry.LabelRegistered(EpochId epoch, label) ->
            let (WorkId work) = label.Work
            let (ReadSlotId slot) = label.Slot
            writer.Write 4uy
            writer.Write epoch
            writer.Write work
            writer.Write slot
            writeString writer label.Label
        | JournalEntry.Causal edge ->
            let (EpochId epoch) = edge.Epoch
            let (WorkId affected) = edge.AffectedWork
            writer.Write 5uy
            writer.Write epoch
            match edge.Trigger with
            | CausalTrigger.InputChanged(InputId input, InputStamp stamp) ->
                writer.Write 1uy
                writer.Write input
                writer.Write stamp
            | CausalTrigger.InputRemoved(InputId input) ->
                writer.Write 2uy
                writer.Write input
            | CausalTrigger.ScopeReserved(ScopeId scope, RevisionId revision) ->
                writer.Write 3uy
                writer.Write scope
                writer.Write revision
            | CausalTrigger.ScopeClosed(ScopeId scope) ->
                writer.Write 4uy
                writer.Write scope
            writeOption writer (fun w (ReadSlotId slot) -> w.Write slot) edge.InvalidatedSlot
            writeOption writer (fun w (WorkId source) -> w.Write source) edge.SourceWork
            writer.Write affected
            writeOption writer (fun w (AttemptId attempt) -> w.Write attempt) edge.RestartedAttempt
            writeOption writer writeString edge.Label
        writer.Flush()
        buffer.ToArray()

    /// Encodes a sequence of JournalEntry frames using the versioned V1 wire format.
    let encodeV1 (entries: JournalEntry list) : byte[] =
        let initialEpoch =
            entries
            |> List.tryPick (function
                | JournalEntry.BeginEpoch(EpochId e)
                | JournalEntry.ExternalCommand(EpochId e, _)
                | JournalEntry.HostObservation(EpochId e, _)
                | JournalEntry.LabelRegistered(EpochId e, _) -> Some e
                | JournalEntry.Causal edge ->
                    let (EpochId e) = edge.Epoch
                    Some e)
            |> Option.defaultValue 0UL
        use stream = new MemoryStream()
        use writer = new BinaryWriter(stream, Encoding.UTF8, true)
        writer.Write magic0
        writer.Write magic1
        writer.Write magic2
        writer.Write magic3
        writer.Write versionV1
        writer.Write 0us
        writer.Write initialEpoch
        writer.Write(uint32 entries.Length)
        for entry in entries do
            let payload = encodeEntryPayload entry
            writer.Write(uint32 payload.Length)
            writer.Write payload
        writer.Flush()
        stream.ToArray()

    type private SliceReader(buffer: byte[], startOffset: int, length: int) =
        let mutable cursor = startOffset
        let limit = startOffset + length

        member _.Remaining = limit - cursor

        member this.ReadByte(index: int) =
            if this.Remaining < 1 then Error(JournalError.TruncatedFrame index)
            else
                let value = buffer[cursor]
                cursor <- cursor + 1
                Ok value

        member this.ReadUInt32(index: int) =
            if this.Remaining < 4 then Error(JournalError.TruncatedFrame index)
            else
                let value = BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(cursor, 4))
                cursor <- cursor + 4
                Ok value

        member this.ReadUInt64(index: int) =
            if this.Remaining < 8 then Error(JournalError.TruncatedFrame index)
            else
                let value = BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(cursor, 8))
                cursor <- cursor + 8
                Ok value

        member this.ReadString(index: int) =
            match this.ReadUInt32 index with
            | Error err -> Error err
            | Ok byteLen ->
                if uint64 byteLen > uint64 this.Remaining then
                    Error(JournalError.TruncatedFrame index)
                else
                    let count = int byteLen
                    try
                        let text = utf8.GetString(buffer, cursor, count)
                        cursor <- cursor + count
                        Ok text
                    with :? DecoderFallbackException ->
                        Error(JournalError.CorruptFrame(index, "Invalid UTF-8 string."))

    let private readOption (index: int) (reader: SliceReader) (readInner: int -> SliceReader -> Result<'T, JournalError>) =
        match reader.ReadByte index with
        | Error err -> Error err
        | Ok 0uy -> Ok None
        | Ok 1uy -> readInner index reader |> Result.map Some
        | Ok tag -> Error(JournalError.CorruptFrame(index, sprintf "Invalid option tag 0x%02X." tag))

    let private readCountedList (index: int) (minItemBytes: uint64) (reader: SliceReader) (readItem: int -> SliceReader -> Result<'T, JournalError>) =
        match reader.ReadUInt32 index with
        | Error err -> Error err
        | Ok count ->
            if uint64 count * minItemBytes > uint64 reader.Remaining then
                Error(JournalError.TruncatedFrame index)
            else
                let total = int count
                let acc = ResizeArray<'T>(total)
                let rec loop i =
                    if i = total then Ok(acc |> Seq.toList)
                    else
                        match readItem index reader with
                        | Error err -> Error err
                        | Ok item ->
                            acc.Add item
                            loop (i + 1)
                loop 0

    let private decodeRead (index: int) (reader: SliceReader) : Result<Read, JournalError> =
        match reader.ReadUInt64 index, reader.ReadByte index with
        | Error err, _ | _, Error err -> Error err
        | Ok slot, Ok 1uy ->
            reader.ReadUInt64 index
            |> Result.map (fun input -> { Slot = ReadSlotId slot; Source = ReadSource.Input(InputId input) })
        | Ok slot, Ok 2uy ->
            reader.ReadUInt64 index
            |> Result.map (fun upstream -> { Slot = ReadSlotId slot; Source = ReadSource.Work(WorkId upstream) })
        | _, Ok sourceTag ->
            Error(JournalError.CorruptFrame(index, sprintf "Unknown read source tag 0x%02X." sourceTag))

    let private decodeScopeEntry (index: int) (reader: SliceReader) : Result<ScopeEntry, JournalError> =
        match reader.ReadByte index with
        | Error err -> Error err
        | Ok 1uy ->
            reader.ReadUInt64 index |> Result.map (WorkId >> ScopeEntry.Retain)
        | Ok 2uy ->
            match reader.ReadUInt64 index with
            | Error err -> Error err
            | Ok work ->
                match reader.ReadUInt64 index with
                | Error err -> Error err
                | Ok stamp ->
                    readCountedList index 17UL reader decodeRead
                    |> Result.map (fun reads ->
                        ScopeEntry.Define {
                            Work = WorkId work
                            Stamp = DefinitionStamp stamp
                            Reads = reads
                        })
        | Ok entryTag ->
            Error(JournalError.CorruptFrame(index, sprintf "Unknown scope entry tag 0x%02X." entryTag))

    let private decodeFrame (index: int) (buffer: byte[]) (offset: int) (length: int) : Result<JournalEntry, JournalError> =
        let reader = SliceReader(buffer, offset, length)
        let decoded =
            match reader.ReadByte index with
            | Error err -> Error err
            | Ok 1uy ->
                reader.ReadUInt64 index |> Result.map (EpochId >> JournalEntry.BeginEpoch)
            | Ok 2uy ->
                match reader.ReadUInt64 index, reader.ReadByte index with
                | Error err, _ | _, Error err -> Error err
                | Ok epoch, Ok 1uy ->
                    match reader.ReadUInt64 index, reader.ReadUInt64 index with
                    | Error err, _ | _, Error err -> Error err
                    | Ok scope, Ok revision ->
                        Ok(JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.ReserveScope(ScopeId scope, RevisionId revision)))
                | Ok epoch, Ok 2uy ->
                    match reader.ReadUInt64 index with
                    | Error err -> Error err
                    | Ok scope ->
                        match reader.ReadUInt64 index with
                        | Error err -> Error err
                        | Ok revision ->
                            readCountedList index 9UL reader decodeScopeEntry
                            |> Result.map (fun entries ->
                                JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.ReplaceScope(ScopeId scope, RevisionId revision, entries)))
                | Ok epoch, Ok 3uy ->
                    readCountedList index 16UL reader (fun i r ->
                        match r.ReadUInt64 i with
                        | Error err -> Error err
                        | Ok input ->
                            r.ReadUInt64 i
                            |> Result.map (fun stamp -> { Input = InputId input; Stamp = InputStamp stamp }))
                    |> Result.map (fun inputs -> JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.SetInputs inputs))
                | Ok epoch, Ok 4uy ->
                    readCountedList index 8UL reader (fun i r -> r.ReadUInt64 i |> Result.map InputId)
                    |> Result.map (fun ids -> JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.RemoveInputs ids))
                | Ok epoch, Ok 5uy ->
                    match reader.ReadUInt64 index, reader.ReadUInt64 index with
                    | Error err, _ | _, Error err -> Error err
                    | Ok demandId, Ok work ->
                        Ok(JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.Demand(DemandId demandId, WorkId work)))
                | Ok epoch, Ok 6uy ->
                    reader.ReadUInt64 index
                    |> Result.map (fun demandId -> JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.Release(DemandId demandId)))
                | Ok epoch, Ok 7uy ->
                    reader.ReadUInt64 index
                    |> Result.map (fun work -> JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.Retry(WorkId work)))
                | Ok epoch, Ok 8uy ->
                    match reader.ReadUInt64 index with
                    | Error err -> Error err
                    | Ok sEpoch ->
                        match reader.ReadUInt64 index with
                        | Error err -> Error err
                        | Ok attempt ->
                            match reader.ReadUInt64 index with
                            | Error err -> Error err
                            | Ok suspId ->
                                reader.ReadUInt64 index
                                |> Result.map (fun step ->
                                    let frame = {
                                        Epoch = EpochId sEpoch
                                        Attempt = AttemptId attempt
                                        Id = SuspensionId suspId
                                        Step = StepId step
                                    }
                                    JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.Resume frame))
                | Ok epoch, Ok 9uy ->
                    reader.ReadUInt64 index
                    |> Result.map (fun scope -> JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.CloseScope(ScopeId scope)))
                | Ok epoch, Ok 10uy ->
                    Ok(JournalEntry.ExternalCommand(EpochId epoch, ExternalActionFrame.Retire))
                | _, Ok actionTag ->
                    Error(JournalError.CorruptFrame(index, sprintf "Unknown external action tag 0x%02X." actionTag))
            | Ok 3uy ->
                match reader.ReadUInt64 index, reader.ReadByte index with
                | Error err, _ | _, Error err -> Error err
                | Ok epoch, Ok 1uy ->
                    match reader.ReadUInt64 index, reader.ReadUInt64 index with
                    | Error err, _ | _, Error err -> Error err
                    | Ok attempt, Ok step ->
                        Ok(JournalEntry.HostObservation(EpochId epoch, ObservationFrame.Suspended(AttemptId attempt, StepId step)))
                | Ok epoch, Ok 2uy ->
                    match reader.ReadUInt64 index, reader.ReadByte index with
                    | Error err, _ | _, Error err -> Error err
                    | Ok attempt, Ok 1uy ->
                        Ok(JournalEntry.HostObservation(EpochId epoch, ObservationFrame.Finished(AttemptId attempt, CompletionFrame.Succeeded)))
                    | Ok epochAttempt, Ok 2uy ->
                        match reader.ReadString index with
                        | Error err -> Error err
                        | Ok code ->
                            reader.ReadString index
                            |> Result.map (fun message ->
                                JournalEntry.HostObservation(
                                    EpochId epoch,
                                    ObservationFrame.Finished(AttemptId epochAttempt, CompletionFrame.Failed { Code = code; Message = message })))
                    | Ok attempt, Ok 3uy ->
                        Ok(JournalEntry.HostObservation(EpochId epoch, ObservationFrame.Finished(AttemptId attempt, CompletionFrame.Cancelled)))
                    | _, Ok compTag ->
                        Error(JournalError.CorruptFrame(index, sprintf "Unknown completion tag 0x%02X." compTag))
                | Ok epoch, Ok 3uy ->
                    reader.ReadUInt64 index
                    |> Result.map (fun attempt -> JournalEntry.HostObservation(EpochId epoch, ObservationFrame.Drained(AttemptId attempt)))
                | _, Ok obsTag ->
                    Error(JournalError.CorruptFrame(index, sprintf "Unknown observation tag 0x%02X." obsTag))
            | Ok 4uy ->
                match reader.ReadUInt64 index with
                | Error err -> Error err
                | Ok epoch ->
                    match reader.ReadUInt64 index with
                    | Error err -> Error err
                    | Ok work ->
                        match reader.ReadUInt64 index with
                        | Error err -> Error err
                        | Ok slot ->
                            reader.ReadString index
                            |> Result.map (fun text ->
                                JournalEntry.LabelRegistered(
                                    EpochId epoch,
                                    { Work = WorkId work; Slot = ReadSlotId slot; Label = text }))
            | Ok 5uy ->
                match reader.ReadUInt64 index, reader.ReadByte index with
                | Error err, _ | _, Error err -> Error err
                | Ok epoch, Ok triggerTag ->
                    let triggerResult =
                        match triggerTag with
                        | 1uy ->
                            match reader.ReadUInt64 index, reader.ReadUInt64 index with
                            | Error err, _ | _, Error err -> Error err
                            | Ok input, Ok stamp -> Ok(CausalTrigger.InputChanged(InputId input, InputStamp stamp))
                        | 2uy ->
                            reader.ReadUInt64 index |> Result.map (InputId >> CausalTrigger.InputRemoved)
                        | 3uy ->
                            match reader.ReadUInt64 index, reader.ReadUInt64 index with
                            | Error err, _ | _, Error err -> Error err
                            | Ok scope, Ok revision -> Ok(CausalTrigger.ScopeReserved(ScopeId scope, RevisionId revision))
                        | 4uy ->
                            reader.ReadUInt64 index |> Result.map (ScopeId >> CausalTrigger.ScopeClosed)
                        | other ->
                            Error(JournalError.CorruptFrame(index, sprintf "Unknown causal trigger tag 0x%02X." other))
                    match triggerResult with
                    | Error err -> Error err
                    | Ok trigger ->
                        match readOption index reader (fun i r -> r.ReadUInt64 i |> Result.map ReadSlotId) with
                        | Error err -> Error err
                        | Ok slotOpt ->
                            match readOption index reader (fun i r -> r.ReadUInt64 i |> Result.map WorkId) with
                            | Error err -> Error err
                            | Ok sourceOpt ->
                                match reader.ReadUInt64 index with
                                | Error err -> Error err
                                | Ok affected ->
                                    match readOption index reader (fun i r -> r.ReadUInt64 i |> Result.map AttemptId) with
                                    | Error err -> Error err
                                    | Ok attemptOpt ->
                                        readOption index reader (fun i r -> r.ReadString i)
                                        |> Result.map (fun labelOpt ->
                                            JournalEntry.Causal {
                                                Epoch = EpochId epoch
                                                Trigger = trigger
                                                InvalidatedSlot = slotOpt
                                                SourceWork = sourceOpt
                                                AffectedWork = WorkId affected
                                                RestartedAttempt = attemptOpt
                                                Label = labelOpt
                                            })
            | Ok kindTag ->
                Error(JournalError.CorruptFrame(index, sprintf "Unknown journal entry kind 0x%02X." kindTag))
        match decoded with
        | Error err -> Error err
        | Ok entry ->
            if reader.Remaining <> 0 then
                Error(JournalError.CorruptFrame(index, sprintf "Trailing %d byte(s) in frame." reader.Remaining))
            else
                Ok entry

    /// Decodes a V1 binary journal stream with strict header, framing, and bounds checks.
    let decodeV1 (bytes: byte[]) : Result<EpochId * JournalEntry list, JournalError> =
        if isNull bytes || bytes.Length < 4 then
            Error(JournalError.InvalidHeader "Journal payload is shorter than the 4-byte magic header.")
        elif bytes[0] <> magic0 || bytes[1] <> magic1 || bytes[2] <> magic2 || bytes[3] <> magic3 then
            Error(JournalError.InvalidHeader "Journal magic bytes do not match 'FFIJ'.")
        elif bytes.Length < 6 then
            Error(JournalError.InvalidHeader "Truncated journal version header.")
        else
            let version = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2))
            if version <> versionV1 then
                Error(JournalError.UnsupportedVersion version)
            elif bytes.Length < 20 then
                Error(JournalError.InvalidHeader "Truncated V1 journal header.")
            else
                let flags = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6, 2))
                if flags <> 0us then
                    Error(JournalError.InvalidHeader(sprintf "Unsupported V1 header flags 0x%04X." flags))
                else
                    let initialEpoch = EpochId(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8)))
                    let entryCount = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(16, 4))
                    let mutable offset = 20
                    let remainingHeader = bytes.Length - offset
                    if uint64 entryCount * 5UL > uint64 remainingHeader then
                        let failIndex =
                            let mutable idx = 0
                            let mutable probe = offset
                            let mutable searching = true
                            while searching && idx < int entryCount do
                                if bytes.Length - probe < 4 then
                                    searching <- false
                                else
                                    let len = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(probe, 4))
                                    if uint64 len > uint64 (bytes.Length - probe - 4) then
                                        searching <- false
                                    else
                                        probe <- probe + 4 + int len
                                        idx <- idx + 1
                            idx
                        Error(JournalError.TruncatedFrame failIndex)
                    else
                        let total = int entryCount
                        let entries = ResizeArray<JournalEntry>(total)
                        let rec loop i =
                            if i = total then
                                if offset <> bytes.Length then
                                    Error(JournalError.CorruptFrame(total, sprintf "Unexpected %d trailing byte(s) after journal stream." (bytes.Length - offset)))
                                else
                                    Ok(initialEpoch, entries |> Seq.toList)
                            elif bytes.Length - offset < 4 then
                                Error(JournalError.TruncatedFrame i)
                            else
                                let frameLen = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset, 4))
                                offset <- offset + 4
                                if uint64 frameLen > uint64 (bytes.Length - offset) then
                                    Error(JournalError.TruncatedFrame i)
                                else
                                    let len = int frameLen
                                    match decodeFrame i bytes offset len with
                                    | Error err -> Error err
                                    | Ok entry ->
                                        offset <- offset + len
                                        entries.Add entry
                                        loop (i + 1)
                        loop 0

/// Thread-safe in-memory boundary journal storing protocol frames without ValueTokens.
type InMemoryInputJournal(?initialEpoch: EpochId) =
    let gate = obj ()
    let entries = ResizeArray<JournalEntry>()
    let pendingLabels = ResizeArray<TraceLabel>()
    let mutable labels: Map<WorkId * ReadSlotId, string> = Map.empty
    let mutable currentEpoch: EpochId option = None

    let beginEpochLocked epoch =
        if currentEpoch <> Some epoch then
            currentEpoch <- Some epoch
            entries.Add(JournalEntry.BeginEpoch epoch)
            for pending in pendingLabels do
                entries.Add(JournalEntry.LabelRegistered(epoch, pending))
            pendingLabels.Clear()

    do initialEpoch |> Option.iter beginEpochLocked

    /// Active TraceOn handle forwarding lazy Trace.named labels into this journal.
    member this.Trace: TraceOn = Trace.On(fun label -> (this :> IInputJournal).RecordLabel label)

    /// Snapshot of recorded journal entries in commit order.
    member _.Entries: JournalEntry list =
        lock gate (fun () -> entries |> Seq.toList)

    /// Snapshot of recorded causal restart edges in commit order.
    member _.CausalEdges: CausalEdge list =
        lock gate (fun () ->
            entries
            |> Seq.choose (function JournalEntry.Causal edge -> Some edge | _ -> None)
            |> Seq.toList)

    /// Encodes all recorded journal entries into a V1 byte array.
    member this.ToBytes() : byte[] =
        JournalCodec.encodeV1 this.Entries

    interface IInputJournal with
        member _.Version = JournalVersion.V1

        member _.BeginEpoch(epoch: EpochId) =
            lock gate (fun () -> beginEpochLocked epoch)

        member _.Record(entry: JournalEntry) =
            lock gate (fun () -> entries.Add entry)

        member _.RecordLabel(label: TraceLabel) =
            lock gate (fun () ->
                labels <- Map.add (label.Work, label.Slot) label.Label labels
                match currentEpoch with
                | Some epoch -> entries.Add(JournalEntry.LabelRegistered(epoch, label))
                | None -> pendingLabels.Add label)

        member _.Labels() =
            lock gate (fun () -> labels)

        member _.Synchronize action = lock gate action

        member _.Flush() = ()
