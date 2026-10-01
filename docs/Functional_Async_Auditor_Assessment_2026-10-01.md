# Functional async auditor assessment — 2026-10-01

**Accept this bounded functional API tranche for consumer integration work.** The preferred `AsyncMailbox` surface meets the requested interim F# async direction: typed module operations, explicit startup/admission, repeatable exact-operation observation and genuinely async-owned execution. Independent review found no concrete new correctness defect. The previous F1 cleanup repair remains closed, including through the replacement mailbox engine. This is library acceptance, not Bozzetto/Composer or native-host acceptance.

Reviewed checkpoint: `613e2600c1f1696eb0a363b18d8ecdd7c9b2764b`.
Implementation and preview.4 package pin: `d3239c26cf4de5e06babd541f576d9466e7a1986`.
The intervening commit only adds distribution receipts to the [checkpoint](Functional_Async_Checkpoint_2026-10-01.md). The checkout was clean at audit entry. Implementation and repository tests were not edited by this audit.

Concurrent work advanced the checkout to `87c77d91ab24c8e4d065e4e726f8c06abc3180c4` during final documentation. That later observation-API change is outside this assessment. The complete reviewed source manifest and independent probe's original-source manifest were verified against an archived `613e260` checkout in the evidence directory. The independent build and 99-test receipt precede the newer commit; final mutable checkout/build outputs must not be substituted for those receipts.

Both earlier independent assessments remain unchanged:

| Assessment | SHA256 |
| --- | --- |
| [Original mailbox assessment](Mailbox_Auditor_Assessment_2026-10-01.md) | `ee20df2a721cc31a8c1827445386da7cedf4428fbfa0f4c6b281deb9240d02e3` |
| [F1 follow-up and functional API direction](Mailbox_Auditor_Followup_2026-10-01.md) | `bdd3678fa10bf95a83f2553f4901d4a3ae3fd997acaa9b63d81a4ee6e895cdde` |

## Functional shape and general applicability

[AsyncMailbox.fs](../src/Fidelity.FSharp.Incremental.Hosting/AsyncMailbox.fs) owns the coordinator, execution entries and acknowledgements directly. Its evaluator is `StepInvocation -> WorkCancellation -> Async<StepOutcome>`. The receive loop is F# async; it does not await an older Task host. [MailboxHost.fs](../src/Fidelity.FSharp.Incremental.Hosting/MailboxHost.fs) now adapts that engine for CLR callers while retaining its existing eager-construction and immediate-admission behavior. The separate legacy `Host` retains its repaired lifetime implementation.

The new surface makes the meaningful boundaries explicit:

| Operation | Audited meaning |
| --- | --- |
| `create` / `start` | Cold handle creation followed by explicit coordinator startup; neither alone demands evaluation. A closed handle cannot restart. |
| `admit` | Immediate bounded admission returning a typed operation handle. Admission is not committed permission. |
| `observe` / `tryObserve` | Observe that exact admission's retained result without resubmitting its action. Each async observation is cold; cancellation detaches its observer only. |
| `beginClose` / `awaitClose` | Immediately seal admission, then observe the retained physical join operation. Cancelling an observation leaves that operation owned. |
| `close` | Cold convenience workflow; merely constructing it does not seal admission. |

This addresses the earlier API-shape gap for the preferred interim host. No object-backed semantic payload store, `box`/`unbox`, or `:> obj` workaround was introduced in source or samples. Private lock objects, runtime collections, cancellation registrations and FSharp.Core's mailbox are still CLR implementation mechanisms. The actual Task bridge and cancellation-token access are named in [ClrInterop.fs](../src/Fidelity.FSharp.Incremental.Hosting/ClrInterop.fs). Opaque operation handles and observer continuations are in-memory host state, not serialized graph payloads or a claim of native portability.

The core API/state transition implementation is unchanged. `AsyncDocuments` provides a noncompiler example with a typed immutable payload union/map, declared document inputs, source mutation after a reconciled reservation receipt, rejected stale/duplicate resumes, an updated word count and unchanged banner reuse. Its response refers to an already-declared snapshot; it does not introduce an undeclared dependency. The protocol remains generally applicable to owner-declared incremental work. Compiler/proof meaning, mutation permission and external-effect authority still belong to their respective owners.

## Observation, cancellation and cleanup

[AsyncCell.fs](../src/Fidelity.FSharp.Incremental.Hosting/AsyncCell.fs), lines 44–111, uses one settlement and a lock-protected waiter list. Publication or cancellation removes each waiter once; registering after removal unregisters the otherwise orphaned registration. Observers resume outside the cell/coordinator lock. The cell test establishes attached waiters before cancelling one, checks removal and repeat observation, and rejects a second settlement.

The exact-operation test additionally demonstrates that a reservation remains one transition after cancelled observation and returns the same receipt on later observations. Its cancellation may happen before subscription; the separate cell test supplies the attached-waiter evidence. These tests and source review support the two distinct guarantees without assuming that every scheduling race was executed.

Successful command receipts follow state publication and withdrawal. Operation handles retain their outcomes independently of observers. This closes the library's **in-process lost-observer** gap. An adapter must retain the exact handle before observing it; discarding it and re-admitting the same action is not reconciliation. `operationOrder` is local ordering, not a globally unique transport identity. Process restart, lost handles, remote request correlation and durable duplicate detection remain outside this guarantee. Receipt retention also belongs in the consumer's memory budget.

[Execution.fs](../src/Fidelity.FSharp.Incremental.Hosting/Execution.fs), lines 40–55, dispatches before evaluator invocation and runs owned work without ambient attempt cancellation. Work receives an explicit `WorkCancellation` request. That choice preserves joins and ordinary cleanup error handling; an observer's cancellation cannot unwind shared owned execution. `WorkCancellation.wait` observes a request, not completion of cleanup.

`ClrInterop.fromTask` invokes a cold factory and subscribes to its returned task within one continuation boundary. Factory failure/null returns become failures; an explicit cancellation request during the factory cannot detach the eventual join. The factory must still return a task that includes its children and cleanup. The engine cannot discover work the evaluator abandons, nor force a noncooperative workflow to terminate.

In AsyncMailbox, evaluator completion and cancellation-callback completion independently gate entry disposal/capacity release (lines 178–190). Faulted idle observes the same close operation (lines 414–433); it cannot bypass that join. Accepted commands are settled before the coordinator disposes its mailbox and publishes close (lines 263–304). The typed faulted-close result identifies the protocol failure; additional evaluator/callback failures remain diagnostics, requiring an observation owner. This differs intentionally from legacy Host's aggregate exception and must be preserved in adapters.

## Independent validation

Evidence directory: `/home/hhh/.cache/bozzetto/audits/incremental-functional-2026-10-01/`. Preserve it when clearing caches. The audit used SDK **10.0.401**, acquired and released the shared Bozzetto build/test leases, and preserved the running daemon.

| Check | Result and scope |
| --- | --- |
| Release solution build | Exit 0, zero warnings/errors; `release-build.log`. |
| Unfiltered Release suite | **99 executed/passed**, zero failed/skipped. Exact inventory: 36 core, 13 suspension, 10 legacy Host, seven Host faults, 18 compatibility mailbox, 15 functional API cases. `independent-release.trx`, log and `test-inventory.log`. All preceding 84 names and their test sources remain present. |
| Three repository samples | All passed. `async-documents.log`, `mailbox-sample.log`, `selective-sample.log`. Selective visits remain 3→2; elapsed times are smoke observations. |
| Package-only consumer | Existing preview.4 consumer rerun passed against hash-verified package assemblies; `package-consumer-rerun.log`. This audit did not repeat package restore/build. Source equals the complete AsyncDocuments sample; assets contain only core, Hosting and FSharp.Core 10.1.401, with no project references. |
| Original independent fault probe on the new engine | Isolated build passed with zero warnings/errors; all four scenarios passed: normal Host and mailbox controls, then injected Host and mailbox protocol faults. Both physical barriers join before faulted close. `fault-build.log`, `fault-probe.log`, source/diffs/manifests under `fault-probe/`. |
| Eight lifetime controls | All eight passed in both the SDK FSI replay and an isolated executable pinned to package FSharp.Core 10.1.401. These are the same eight controls, separate from the 99 tests; they are not sixteen different cases. `lifetime-probe.log`, `lifetime-package-probe.log` and `lifetime-package/`. |
| Author's negative control | Inspected the corrected mutant receipt: one executed failure, “A fault cannot bypass the callback join.” The first invalid-selector run executed zero tests. The initial helper-annotation compile failure also supplied no semantic evidence. Source matches the saved restored implementation. Mutation was not repeated in this audit. |

The independent fault harness is unchanged from the preceding repair audit. Only its isolated library copy is refreshed; the mailbox fault hook moves from the former Task host into `AsyncMailbox.require`. It corrupts one `Finished` acknowledgement to produce a real core refusal. Separate evaluator/callback barriers, observed evaluation diagnostics, a previously eligible seed, refused commands and retained close identity distinguish cleanup from logical invalidation. The instrumented assemblies were never installed or substituted into the production-source 99-test run.

The portable trace case covers one controlled work item, not whole-system equivalence. Finite tests and these selected internal refusals do not establish all interleavings, arbitrary coordinator-crash recovery, scheduling fairness, bounded lifetime memory or forced preemption.

The lifetime controls expose why async syntax alone cannot prove ownership: synchronous-prefix invocation, cancellation before versus after task subscription, exception/finally behavior during ambient cancellation, and the absence of automatic observer detachment in `FromContinuations`. The original receipt printed only assembly version `10.1.0.0`, insufficient by itself to establish a particular package build. This audit records loaded paths and hashes, and repeats the controls in a project explicitly referencing **FSharp.Core 10.1.401**. Its loaded DLL SHA256 is `9abd1f889fc8e2d2a74d6ac83a31e6c3012c075a17946a7d687c0cbe33ed15be`, identical to the 99-test output's FSharp.Core. The SDK FSI DLL has a different hash and is retained as a separate receipt, not substituted for package evidence.

Independent TRX SHA256: `4e1f8b4df3ee067952182ead517d273a62e34df5146ba9b29553cc6a044099f2`.

| Fresh checkpoint assembly | SHA256 |
| --- | --- |
| Core | `0b24623f0af064a9bd6b5f3a93311212d4be473ce9045342b55ff98fc6b3800c` |
| Hosting | `81928c6a7d942af6a1c590aecd5390e49542aab0f217356cf07b6ce8f34d3ef6` |
| Tests | `22bc5693fca162b691289bd70ba6d63f373554a647679840b3f99c8be10e828b` |

Both preview.4 archive hashes and nuspec source pins match the checkpoint. Packaged core/Hosting DLL hashes are respectively `202e3dac92af0d4f75708afb797db3a06f2e37bc4f13f8bc151897f6ff65ab2b` and `f9236078f1f5b066326f838dedc312f56c20e53837391e97473dc04edfc4ce6b`, matching the consumer. `reviewed-source.sha256`, `tested-binaries.sha256`, `earlier-assessments.sha256`, `audit-evidence.sha256` and the probe manifests preserve the distinct receipts. Fresh checkpoint outputs do not establish historical pre-commit binary hashes.

The four tested core/Hosting/test/FSharp.Core DLLs were also preserved under `tested-assemblies/`; `preserved-tested-binaries.sha256` matches the hashes recorded above despite subsequent changes to the live build directories.

## Bozzetto/Composer integration gates

During this audit the user relayed the Clef agent's findings that Composer/editor paths use separate locks for the same compiler state, and that a Bozzetto cancellation path can retire a result before cleanup has joined. That agent is addressing shared whole-project checking and joined work lifetimes. These are **agent-reported integration blockers**, not independently reproduced library defects or a claim that those repairs have landed.

The resulting acceptance checks must establish:

1. Composer and editor checking enter one compiler-owned serialization boundary for the same state. Two independently locked callers do not provide that guarantee. Check overlapping whole-project requests, reservation and state replacement through the actual shared owner while preserving current proof/artifact gates.
2. Cancellation can withdraw result eligibility immediately while retaining physical ownership. Hold real work and cleanup independently, revoke the result, and show that replacement/disposal/capacity reuse and any cleanup-complete claim obey the required join. Distinguish logical retirement from a physical drain certificate; the former happening first is not itself an error.
3. Bozzetto retains exact library operations behind correlated workspace/client requests, re-observes unknown outcomes without duplicate admission, and grants edits only after all required compiler reservation fences commit. Library handles alone do not establish permission after a service restart.
4. External control remains serviceable under ordinary-work saturation. Actual launch/commit is ordered atomically against reservation. One mutation owner, complete dependency declarations, logical/payload/receipt budgets and one event-drain owner with fan-out remain required.

Run that journey against the exact selected compiler/library/host distribution, including real child-process cleanup and both reserve/physical-launch orderings. Passing standalone async tests cannot establish the backend's ownership promises. The library's generic contract should support these checks without absorbing compiler semantics or becoming an application-wide actor/transport framework.

Native compilation, capture/lifetime settlement, resource-bearing continuations, distributed recovery and specialized-target numeric/proof equivalence remain separate work. The accepted interim F# async surface preserves a usable path toward those goals; it is not their acceptance receipt.
