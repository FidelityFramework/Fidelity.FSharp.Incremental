namespace Fidelity.FSharp.Incremental

// Token payloads have no semantic interpretation in this library.
[<Struct>] type EpochId = EpochId of uint64
[<Struct>] type ScopeId = ScopeId of uint64
[<Struct>] type RevisionId = RevisionId of uint64
[<Struct>] type WorkId = WorkId of uint64
[<Struct>] type AttemptId = AttemptId of uint64
[<Struct>] type DemandId = DemandId of uint64
[<Struct>] type InputId = InputId of uint64
[<Struct>] type ReadSlotId = ReadSlotId of uint64
[<Struct>] type DefinitionStamp = DefinitionStamp of uint64
[<Struct>] type InputStamp = InputStamp of uint64
[<Struct>] type ValueToken = ValueToken of uint64
[<Struct>] type EligibilityId = EligibilityId of uint64

[<RequireQualifiedAccess>]
type ReadSource =
    | Input of InputId
    | Work of WorkId

type Read = { Slot: ReadSlotId; Source: ReadSource }

type Definition = {
    Work: WorkId
    Stamp: DefinitionStamp
    Reads: Read list
}

type InputValue = { Input: InputId; Stamp: InputStamp; Value: ValueToken }

[<RequireQualifiedAccess>]
type ScopeEntry =
    | Retain of WorkId
    | Define of Definition

[<RequireQualifiedAccess>]
type ReadValue =
    | Input of InputId * InputStamp * ValueToken
    | Work of WorkId * DefinitionStamp * AttemptId * ValueToken

type ResolvedRead = { Slot: ReadSlotId; Value: ReadValue }

type StartRequest = {
    Epoch: EpochId
    Scope: ScopeId
    Revision: RevisionId
    Attempt: AttemptId
    Definition: Definition
    Reads: ResolvedRead list
}

type ResultHandle = {
    Epoch: EpochId
    Scope: ScopeId
    Revision: RevisionId
    Work: WorkId
    Definition: DefinitionStamp
    Attempt: AttemptId
    Eligibility: EligibilityId
    Value: ValueToken
}

type Failure = { Code: string; Message: string }

[<RequireQualifiedAccess>]
type Completion =
    | Succeeded of ValueToken
    | Failed of Failure
    | Cancelled

[<RequireQualifiedAccess>]
type Action =
    | ReserveScope of ScopeId * RevisionId
    | ReplaceScope of ScopeId * RevisionId * ScopeEntry list
    | SetInputs of InputValue list
    | RemoveInputs of InputId list
    | Demand of DemandId * WorkId
    | Release of DemandId
    | Retry of WorkId
    | Finished of AttemptId * Completion
    | Drained of AttemptId
    | CloseScope of ScopeId
    | Retire

type Command = { Epoch: EpochId; Action: Action }

[<RequireQualifiedAccess>]
type EffectAction =
    | Start of StartRequest
    | Cancel of AttemptId
    | Drain of AttemptId
    | Withdraw of ResultHandle
    | Offer of ResultHandle
    | ScopeClosed of ScopeId
    | EpochDrained

type Effect = { Epoch: EpochId; Action: EffectAction }

[<RequireQualifiedAccess>]
type ProtocolError =
    | ForeignEpoch
    | RetiredEpoch
    | UnknownScope of ScopeId
    | ClosedScope of ScopeId
    | RevisionNotIncreasing of ScopeId
    | ReservationMismatch of ScopeId
    | DuplicateWork of WorkId
    | ForeignWorkOwner of WorkId
    | UnknownRetainedWork of WorkId
    | DefinitionStampReused of WorkId
    | DuplicateReadSlot of WorkId * ReadSlotId
    | DependencyCycle of WorkId list
    | DuplicateInput of InputId
    | InputStampReused of InputId
    | DemandIdReused of DemandId
    | UnknownWork of WorkId
    | UnknownAttempt of AttemptId
    | ConflictingCompletion of AttemptId
    | DrainBeforeCompletion of AttemptId
    | RetryNotFailed of WorkId
    | IdentifierExhausted

[<RequireQualifiedAccess>]
type ScopePhase =
    | Reserved of RevisionId
    | Open of RevisionId
    | Closing
    | Closed

[<RequireQualifiedAccess>]
type WorkStatus =
    | Suspended
    | Idle
    | Waiting
    | Running of AttemptId
    | Draining of AttemptId
    | Eligible of ResultHandle
    | Failed of Failure
    | Cancelled

type ScopeView = { Scope: ScopeId; Phase: ScopePhase; PendingAttempts: int }
type WorkView = { Work: WorkId; Scope: ScopeId; Demanded: bool; Status: WorkStatus }
type Snapshot = {
    Epoch: EpochId
    Retiring: bool
    PendingAttempts: int
    Scopes: ScopeView list
    Works: WorkView list
}

// Core.fs will expose an opaque Core.State and these functions:
// Core.init : EpochId -> Core.State
// Core.step : Command -> Core.State -> Result<Core.State * Effect list, ProtocolError>
// Core.snapshot : Core.State -> Snapshot
// Core.tryResult : WorkId -> Core.State -> ResultHandle option
// Core.isEligible : ResultHandle -> Core.State -> bool
// Core.pendingAttempts : Core.State -> int
// Core.scopeClosed : ScopeId -> Core.State -> bool
// Core.isDrained : Core.State -> bool
