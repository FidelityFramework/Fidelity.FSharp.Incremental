namespace Fidelity.FSharp.Incremental.Hosting

open Fidelity.FSharp.Incremental

type HostDiagnostic = { Attempt: AttemptId; Failure: Failure }

[<RequireQualifiedAccess>]
type StepInvocation =
    | Start of StartRequest
    | Resume of ResumeRequest

/// Every return includes owned children and cleanup. Suspensions retain only
/// immutable owner data; their semantic inputs must be declared dependencies.
[<RequireQualifiedAccess>]
type StepOutcome =
    | Complete of Completion
    | Suspend of StepId * ValueToken

[<RequireQualifiedAccess>]
type MailboxError =
    | QueueFull
    | Closed
    | NotStarted
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
