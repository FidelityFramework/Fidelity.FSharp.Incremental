# Consumer integration checkpoint — October 1, 2026

This document records implementation and validation progress. It is not a
deployment receipt or a release decision.

The recorded preview.5 integration input is implementation
`87c77d91ab24c8e4d065e4e726f8c06abc3180c4`. The independent
[consumer observation assessment](Consumer_Observation_Auditor_Assessment_2026-10-01.md),
committed at `deabc97`, accepts it for integration: 102/102 tests, a package-only
consumer and three additional observation controls passed. Package hashes and
source provenance are in the [observation checkpoint](Consumer_Observation_Checkpoint_2026-10-01.md).

| Consumer | Earlier preview.5 integration evidence |
| --- | --- |
| Clef / CCS | [Workspace integration at `7927b3f`](https://forge.spkez.dev/FidelityFramework/clef/src/commit/7927b3f8dcc8528f8a76571ae1dc75c5e66e0cd0/docs/Incremental_Project_Workspace_2026-10-01.md): cold shared checking, immutable input capture, process ownership and joined cleanup. Focused 22/22 passed. Full suite: 2,125 passed, 99 ongoing compiler-backlog failures; all fifteen additions pass, no missing tests or new failures. |
| Composer | First full run: 377/378 passed, retaining all 369 prior identities and passing nine additions. The existing callable-`Result` initial native build still fails; its later edit/receipt assertions are unexercised. Subsequent reservation and solver-ownership repairs need fresh integration gates. |
| Bozzetto | Provider integration shares demand and joins producer cleanup. [Initial provider checkpoint at `a64010a8`](https://forge.spkez.dev/FidelityFramework/Bozzetto/src/commit/a64010a8/docs/Incremental_Provider_Checkpoint_2026-10-01.md). Release test-project build passed with zero warnings/errors; default and native provider validation are pending. |

The installed Bozzetto daemon still uses its previous compiler closure. Each
reserved compiler generation receives fresh source checking and existing
proof/artifact checks. Library eligibility does not replace compiler, proof,
artifact or actual-launch authority. The compiler backlog is separate from the
library's recorded audit scope.

## Integration-driven Task bridge extension

Lattice's retired-proof ownership test exposed an exception-identity mismatch:
`Async.AwaitTask` wrapped a Task fault, so the owning join did not retain the
original failure. The existing `ClrInterop.fromTask` bridge already preserves
that completion, but requires a mailbox attempt's `WorkCancellation`. A retired
physical job has no such attempt; inventing one would misstate ownership.

Preview.6 adds cold `ClrInterop.fromUncancelledTask`, sharing the
same continuation bridge. It invokes the factory on execution, joins the exact
returned Task and reports its original fault. A cancelled Task follows the
owned error path, preserving its cancellation token. The owner runs with ambient
`CancellationToken.None`; cancellable observers remain separate. The Task itself
must include its children and cleanup. This changes the .NET Hosting boundary,
not portable core semantics or compiler/proof admission.

Six new controls cover cold execution and detached observers, exact asynchronous
and synchronous faults, null tasks, Task cancellation routing, and compatibility
with the attempt-aware bridge. **All 108 library tests passed.** Implementation
`3b86e2dac96ad55cb965341bfc04395061d09c46` is pushed. Clef, Composer and Bozzetto
now carry identical preview.6 archives: core SHA-256
`cc017440e0fadd177b3ba8cc2ab0f809aa754728b52f45310d998271af9562f9`, Hosting
`1afaa709250d578b14bea8fa15a59ed8a227f86f003e3653017106db775522e5`.

Bozzetto checkpoint `fd10e283` records 9,788 passing default tests, four ignores,
and 40/40 passing Composer integration cases, including actual native artifacts
and the live MCP/browser workflow. Composer's solver verdict and four physical
lifetime controls pass on preview.6. Subsequent compiler/editor semantic repairs
are still being validated; these receipts identify the compiler closure they
actually exercised and are not a promotion of the installed daemon.

The earlier audits remain evidence for their exact revisions; they do not freeze
the library against changes required by consumer integration. The current
[cross-project checkpoint](https://forge.spkez.dev/FidelityFramework/Bozzetto/src/commit/fd10e283/docs/Incremental_Provider_Checkpoint_2026-10-01.md)
is the audit entry for the aligned consumers and remaining compiler work.
