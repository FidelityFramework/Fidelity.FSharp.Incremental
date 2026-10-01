# Functional async checkpoint — 2026-10-01

This tranche responds to the [auditor follow-up](Mailbox_Auditor_Followup_2026-10-01.md): make the workflow and public API functional while preserving ownership, reservation and freshness. The independently closed F1 repair remains intact. This is a library checkpoint; Bozzetto and Composer adoption remain separate gates.

## Surface and implementation

`AsyncMailbox` uses small module functions and opaque typed handles. Its evaluator is `StepInvocation -> WorkCancellation -> Async<StepOutcome>`. The coordinator is an F# async mailbox; execution, replies and close use async continuations and typed one-settlement cells. It does not await an underlying Task host. The deterministic API/Core command and effect implementation is unchanged.

| Operation | Timing and authority |
| --- | --- |
| `create settings evaluator` | Validates configuration and returns a cold handle. No coordinator or evaluator starts. |
| `start handle` | Explicitly starts one coordinator; repeated start while open is harmless. |
| `admit action handle` | Immediately attempts bounded admission. Success returns an exact operation handle, not a committed receipt. |
| `observe operation` | Cold, repeatable observation of that one admission. Cancelling an observer removes its waiter; it neither retracts nor repeats the command. |
| `beginClose handle` | Immediately seals admission and returns the stable close operation. Accepted commands and physical ownership still join. |
| `close handle` | Cold convenience workflow: admission closes when the workflow executes. |
| `awaitClose operation` | Observes actual close. Cancelling this observation cannot certify or interrupt owned cleanup. |

Retain the operation handle before awaiting its result. In particular, an accepted `ReserveScope` operation is not permission to mutate: its successful **receipt** must first confirm publication and withdrawal. A lost observer can re-observe that same operation without admitting a second reservation. This is in-memory reconciliation, not persistent recovery or deduplication across process restart.

`MailboxHost` is now the explicit CLR compatibility adapter over that same engine. Its constructor starts the coordinator and `PostAsync` admits at method invocation, preserving existing timing. Task conversion, token interoperation and async disposal are named boundaries. The earlier semaphore-based `Host` remains a legacy Task API with its independently validated F1 repair; it has not acquired a second functional façade.

## Why cancellation is explicit

F# async syntax does not by itself establish lifetime ownership. Eight isolated controls against FSharp.Core 10.1.401 confirmed:

- `StartWithContinuations` executes the synchronous prefix inline unless dispatch is explicitly moved; the owned runner switches to the pool before invoking the evaluator.
- An already-subscribed `AwaitTask` remains joined after ambient cancellation. Cancellation before subscription can instead bypass an already-created Task.
- Ambient cancellation can bypass an exception handler or discard an exception raised by `finally` during cancellation unwind.
- `FromContinuations` does not automatically detach an observer when its ambient token is cancelled after entry.

Owned workflows therefore run without ambient attempt cancellation. `WorkCancellation.isRequested` and `WorkCancellation.wait` expose the explicit stop request; the evaluator remains responsible for completing its children and cleanup. This preserves ordinary exception handling, including cleanup failures. Observer workflows retain independent ambient cancellation, with explicit waiter removal in their cells.

CLR cancellation callbacks are invoked outside the coordinator and joined separately from evaluator completion. The named `ClrInterop.fromTask` adapter invokes a cold Task factory and subscribes inside one continuation boundary, retaining ownership even when the request arrives during factory execution. It cannot discover children abandoned by that factory. A noncooperative workflow can delay close indefinitely.

Physical ownership remains separate from graph validity. On protocol disagreement, admission and result queries fail closed; the host requests cancellation and joins evaluators and callbacks before reporting the typed close failure. It does not invent successful graph acknowledgements. Additional evaluator/callback failures remain available through diagnostics. Normal computation failures do not themselves corrupt the host protocol.

## Typed data and port boundary

`ValueToken` remains identity only. The `AsyncDocuments` sample gives it a typed immutable payload map and explicit declared inputs, including the resume response. No object-backed payload store or captured resource-bearing continuation is introduced. Retained graph suspensions still describe the next step and immutable captures; they are not saved CLR stacks.

The current executor still uses CLR locks, the thread pool, cancellation registrations and FSharp.Core's mailbox implementation. Those are Hosting mechanisms, not the native specification. The portable contract is the command/effect trace plus ownership laws. A future Clef host must preserve both; this checkpoint is not evidence of native compilation or specialized-target equivalence.

## Validation and audit request

The final restored Release suite passes **99/99**, zero failures or skips: all
84 preceding cases and 15 functional API cases. The solution build passes with
zero warnings/errors on SDK 10.0.401. All three samples pass, including the typed
document example and both unchanged compatibility samples. Raw logs remain
outside Git under `/home/hhh/.codex/work/incremental-functional-2026-10-01/`.

| Control | Executed outcome |
| --- | --- |
| Cold creation/start and cold close | Creation runs no evaluator; pre-start admission is refused; close-before-start cannot reopen. |
| Exact operation and observer cancellation | One reservation transition remains observable through the same handle after cancelled observation; cell waiters are removed. |
| Synchronous prefix and shared demand | A held evaluator prefix leaves coordination responsive; one released demand and cancelled idle observer leave shared work alive. |
| Physical joins and explicit cancellation | Close survives observer cancellation; a held Task returned during cancellation remains joined; an Async `finally` failure remains a diagnostic. |
| Suspension and authority | Current resume is one-shot; acknowledged reservation rejects the old resume before source mutation; external lifecycle acknowledgements are refused. |
| Portable trace | A controlled one-work execution matches exact Core command effects, final snapshot and eligibility handle. This is a bounded trace, not whole-system equivalence. |
| Internal fault | A real Core epoch refusal during retirement retains independent evaluator/callback joins and reports failure without inventing drain or offer evidence. |
| Negative oracle control | An intentional mutation returned faulted idle immediately. The selected regression failed at “A fault cannot bypass the callback join.” Source was byte-restored, rebuilt, then all 99 tests passed. |

The first test compilation stopped at two test-helper type annotations (NUnit
overload resolution and explicit generic argument syntax); no semantic tests ran.
The first mutant selection had an NUnit filter parse error and also ran no tests.
The corrected selection executed one intended failure against the already-built
mutant. Neither tooling failure is counted as semantic evidence. No mutation is
present in the checkpoint.

The final `validation/release-final.trx` SHA256 is
`6f16a90cd9d06d5a2b6c0043f234c455fe6cd8a8cd24fd0824f9b567001cd05b`.
External manifests identify reviewed source and tested assemblies separately from
any later packaging build. The follow-up assessment remains byte-identical:
`bdd3678fa10bf95a83f2553f4901d4a3ae3fd997acaa9b63d81a4ee6e895cdde`.

A bounded LAN design review supplied suggestions, not acceptance evidence; its
disposition is retained at
`/home/hhh/.codex/work/incremental-async-2026-10-01/lan-api/DISPOSITION.txt`.
Suggestions that conflated reply observers with owned execution or weakened
faulted-close joins were rejected. Local source review of package-pinned
FSharp.Core also identified coordinator wait-handle disposal, now performed before
close publication. The eight isolated Async controls are recorded separately in
`async-lifetime/probe.log`; they are not included in the 99 repository cases.

## Committed distribution receipt

Implementation pin: `d3239c26cf4de5e06babd541f576d9466e7a1986`.
Both local `0.1.0-preview.4` packages were built from that committed source and
carry its exact repository pin. A separate console project with only the Hosting
package reference restored, built and ran the complete `AsyncDocuments` sample.
Its dependency closure contains only core, Hosting and FSharp.Core 10.1.401; it
has no project reference. This checks distribution shape and the exercised API,
not package-feed publication or integration into a compiler workspace.

| Archive | SHA256 |
| --- | --- |
| Core | `25054798c3d46f6df0babafcb468403432a9e41970e9b04d3d079b1af537c6fb` |
| Hosting | `8f963aa925a4e54645797c2eb7139dffd6821913c8f53f6bc39093ac0ce29c86` |

Package archives, manifests and the isolated consumer remain in the external
validation directory. The shared Bozzetto build lease was released. No compiler
distribution, service, model configuration or consumer dependency was changed.

Review the distinction between admission and acknowledgement, exact-operation re-observation, observer versus owner cancellation, physical joins after faults, typed payloads, and the CLR boundary. The independent original assessment and follow-up are preserved unchanged.

The original consumer conditions remain: one mutation owner, control admission under saturation, authority checked atomically at actual external launch/commit, total logical-work and payload budgets, one event-drain owner with fan-out, and real-process cleanup. This tranche does not establish compiler proof eligibility or native artifact authority, nor provide actor supervision, remote transport or general delimited continuations.
