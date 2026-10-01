# Dependency and execution ownership

The first implementation separates a deterministic command processor from execution. `Fidelity.FSharp.Incremental` contains the public token model and `Core`; `Fidelity.FSharp.Incremental.Hosting` interprets effects using .NET facilities. This document specifies that initial contract. It is not evidence that every invariant has passed testing, or that Composer already uses the library.

The core has no tasks, cancellation tokens, filesystem operations, clocks, ambient transactions, user callbacks, or generic payload equality. The host stores immutable payloads behind `ValueToken`. Commands and effects carry an `EpochId`. A caller serializes commands, commits the returned state, then interprets the ordered effects. Rejected commands return neither a replacement state nor effects. This leaves a small protocol to port to a future host without importing CLR task semantics into Clef.

The authored [steps and mailbox extension](Steps_and_Mailboxes.md) retains logical attempts across resource-free suspension and adds a FIFO coordinator with acknowledged reservation. Its [auditor checkpoint](Mailbox_Auditor_Checkpoint.md) remains a separate validation gate. Environment/response tokens carry immutable owner data; they do not discover dependencies or replace complete declared reads and stamps.

## Identity and complete dependencies

`ScopeId` identifies an independently closable owner. A `RevisionId` identifies its reserved or open definition set. `WorkId` is stable lookup identity; `DefinitionStamp` distinguishes definitions of that work. Input identity, input stamp, and value token are separate. The library does not interpret these tokens or prove that their owner issued them correctly.

Every definition supplies an ordered list of reads. Each `ReadSlotId` identifies one occurrence, whose source is an input or another work's output. Two slots may read the same source. Removing one occurrence must preserve the other; treating reads as a set would lose that distinction. Work IDs cannot be owned by multiple scopes, duplicate slots are rejected, and cycles across the registered graph are rejected atomically. Missing dependencies block readiness.

Dependency completeness belongs to the owner. A positive read list cannot express “there are no other uses” unless it includes a versioned census input or a whole-revision input representing that fact. Configuration, platform, effect assumptions and external state also require explicit inputs. The first implementation does not discover arbitrary dynamic dependencies while callbacks execute. Stable identity or equal output bytes cannot excuse an omitted dependency.

## Reserve, replace and invalidate

`ReserveScope` must precede source mutation. It withdraws current results of the scope and their reverse-dependency closure, and cancels affected live attempts. `ReplaceScope` must match that reservation and supplies the complete new membership: `Retain` names an existing definition, while `Define` supplies a new definition stamp. Omission removes work. Closed scopes cannot reopen; removed definitions and released consumer IDs cannot silently revive through identity reuse.

An already successful, drained result may survive reservation as an unavailable cache candidate. It becomes eligible again only through explicit `Retain` with identical ordered resolved reads. Revalidation issues a fresh `EligibilityId` and current revision handle; the old handle stays invalid. Late pre-reservation completion cannot populate this cache. Changed definitions do not inherit a prior result.

Input updates and removals invalidate the affected transitive closure before replacement starts. Stamps retain tombstones, so an old stamp cannot be reused for changed data. Hosts must process withdrawals before allowing edits or treating any retained value as current. `Core.isEligible` checks the complete current `ResultHandle`, not just a work ID or value token.

## Demand and execution

`DemandId` represents one subscription. Demand propagates through work dependencies, sharing producers while preserving each consumer. Releasing one subscription cannot cancel work still needed by another direct or transitive consumer. Releasing an unknown ID establishes a tombstone; conflicting reuse is rejected.

Demanded work starts only after its inputs resolve and its producer work has eligible, drained results. A replacement attempt for the same work ID also waits until all prior attempts for that work drain; independent work can progress. Independent ready work is ordered deterministically by work ID. Each `StartRequest` freezes the definition, revision and ordered resolved reads. Failed or cancelled current work requires explicit `Retry`, or changed premises, rather than an unbounded automatic retry loop. Independent branches can continue while another branch fails.

Execution ownership begins when the core emits `Start`, even if the host has not invoked the operation yet. A cancelled-before-start operation therefore still requires a terminal acknowledgement. Cancellation is cooperative: `Cancel` requests that the host stop owned work; it cannot prove that synchronous code or a child process has stopped.

`Finished` records success, failure or cancellation and requests `Drain`. It cannot offer a successful result on its own. `Drained` acknowledges that owned execution and resources are finished. Only a still-current, demanded success with unchanged revision and reads can then produce `Offer`. Duplicate identical acknowledgements are idempotent; conflicting completion or draining before completion is rejected. Obsolete successes are discarded.

The host must convert synchronous start failures and asynchronous exceptions into terminal outcomes, perform cleanup, and report draining without stranding ownership. Cancellation of an individual wait must remain separate from cancellation of shared work. Callbacks and cleanup run outside core state transitions; exceptions cannot partially commit the state machine. An `Offer` is bookkeeping eligibility, not an external publication instruction.

## Functional workflow surface

`AsyncMailbox` is the preferred hosted surface: typed module operations and an
evaluator `StepInvocation -> WorkCancellation -> Async<StepOutcome>`. Creation is
cold, coordinator start is explicit, and bounded command admission returns a
retained operation handle. Observing that handle is cold and repeatable; an
observer's cancellation cannot retract admission or cancel shared work. Await a
successful reservation receipt before mutation. `beginClose` seals admission
immediately; the convenience `close` workflow does so only when executed.

The implementation is an F# async coordinator and typed continuation cells, not
an Async wrapper around a Task engine. Owned workflows receive an explicit stop
request and run without ambient attempt cancellation, preserving joins and cleanup
errors. CLR callbacks run outside the coordinator and join separately. Physical
ownership and faulted graph state remain distinct. The functional close outcome is
`Result<unit, Failure>`; additional evaluator/callback failures remain diagnostics.

`ClrInterop` supplies Task factories, observer conversion and cancellation tokens
for necessary .NET I/O. `MailboxHost` is a compatibility adapter over the same
engine, retaining eager construction and immediate method-invocation admission.
The older `Host` below remains a separate legacy API. See the
[functional checkpoint](Functional_Async_Checkpoint_2026-10-01.md) for timing,
reconciliation, lifetime controls and the current native-port boundary.

## Legacy .NET host surface

`Host(epoch, maxConcurrency, evaluator)` coordinates the core and bounds admitted evaluator lifetimes with a semaphore. The evaluator has type `StartRequest -> CancellationToken -> Task<ValueToken>` and must create fresh work for each invocation. Its returned task must include every owned child operation and resource cleanup. The host cannot discover a task the evaluator abandons, and cannot forcibly interrupt synchronous code or a child process that ignores cancellation.

`Send(action)` returns protocol errors for rejected commands and effects for accepted ones. `Finished` and `Drained` are reserved for the host; callers cannot inject them through `Send`. Returned effects and `DrainEvents()` are observations: the host already executes their lifecycle actions. Consumers must not start a second evaluator from an observed `Start`. `Snapshot`, `TryResult(work)` and `IsEligible(handle)` expose current bookkeeping. `DrainDiagnostics()` retrieves recorded host failures; event and diagnostic queues should be drained regularly.

`WaitForIdleAsync(token)` joins currently owned work, including successors scheduled by completion. Cancelling that token cancels only the wait. It does not release a demand or cancel shared evaluation. The idle observation is instantaneous: another caller can submit demand afterward. `CloseAsync()` retires the epoch and joins owned work; repeated calls share its closing task. `IAsyncDisposable.DisposeAsync()` uses this close operation.

The gate-protected cancellation check after capacity acquisition is the invocation admission point. If reservation wins that check, the queued evaluator is not admitted. A later reservation can race the physical callback invocation; it still prevents that attempt from becoming eligible. Evaluators that mutate external state therefore require an authority check at their own commit point. Do not interpret a cancellation request as proof that all instructions have stopped.

For Bozzetto adoption, preserve the existing ordering of artifact launch against
edit reservation. A check performed when constructing or scheduling a deferred
operation is too early. The actual launch must enter Composer's current-artifact
authority boundary, with validation and launch ordered atomically against
reservation. A separate read of `IsEligible` followed by an unprotected process
start would introduce a race. Required integration control: hold execution after
scheduling, reserve an edit, then release the deferred operation; the old artifact
must never launch. Also test the opposite ordering, where launch legitimately
wins reservation. These are consumer integration gates, not established by this
library's task-lifecycle tests.

Token cancellation callbacks run outside the coordinator and are joined before
draining and capacity reuse. Ordinary evaluator exceptions become failed
completions and callback failures remain diagnostics; neither alone faults the
host protocol. The original `Host` now also records evaluator failures as
diagnostics.

An internal host/core disagreement stops new admission and cancels remaining
owned work. Physical completion remains separate from graph acknowledgements.
`Send` fails immediately and result queries fail closed; `WaitForIdleAsync` and
`CloseAsync` join evaluators and cancellation callbacks before reporting an
`AggregateException` containing the protocol error and failures from work owned
at or after that fault. An observer may still cancel its own idle wait. Close
shares one stable task, even if the core refuses `Retire`, and rejects further
commands once shutdown starts. No new `Finished` or `Drained` certificates are
manufactured after disagreement, so a faulted graph's pending count can remain
nonzero after physical cleanup. See the [audit response](Mailbox_Audit_Response_2026-10-01.md)
for the fault-injection controls and remaining consumer gates.

## Retirement and integration boundaries

`CloseScope` withdraws and removes that scope's definitions, invalidates dependent work, and cancels its attempts. Unrelated scopes remain open. `ScopeClosed` appears only after the scope's attempts drain. `Retire` closes the epoch; it permits necessary terminal acknowledgements but no new work. `EpochDrained`, snapshots and pending-attempt counts expose lifecycle state without reflection into host tasks.

Composer remains a separate future integration. Baker owns source semantics and complete proof premises; PSG publication copies established rows; Alex consumes those rows passively. Source solver discharge, MLIR checks and native artifact correspondence remain distinct. A library offer cannot bypass any of them. Compiler epoch changes, reservation before editing, fresh disk receipts, the existing serialized CCS gate and final generation acceptance remain necessary. The current Composer model was independently retrieved and byte-matched at commit `1bb54c6` in retrieval generation 30; that documentary agreement is not an executed integration test.

Required validation includes chain and diamond readiness, duplicate read occurrences, shared consumer release, reservation and fresh retention handles, changes outside positive participants through census inputs, late completion after replacement, cleanup failures, close/drain races, exact replay, cycle rejection, and owner rejection of a foreign proof receipt despite library eligibility. Collections, custom equality policies, bounded payload eviction and general fixed points remain outside this first contract. See [Audit](Audit.md) for the concrete upstream experiments motivating these boundaries.
