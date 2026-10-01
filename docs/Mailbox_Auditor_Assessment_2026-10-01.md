# Mailbox auditor assessment — 2026-10-01

Independent consumer audit requested at the [mailbox checkpoint](Mailbox_Auditor_Checkpoint.md), covering Bozzetto's needs and applicability beyond the Fidelity compiler pipeline.

Reviewed checkpoint: `8ad9b2640bbf20d9b59dd10ff6ad69bdac4abffe`.
Implementation/package source: `5f87a548f28360bd91fe59c4c10dd014b544d9ad`.
The intervening diff contains only checkpoint and validation documentation. The library checkout was clean before this assessment; the audit does not modify implementation or test sources.

**Disposition: accept the bounded core/mailbox tranche as a reusable foundation for integration work.** The independent Release build, all 77 tests, a noncompiler consumer probe and the mailbox's injected-fault shutdown check pass. This is not full Hosting-package fault-path acceptance: the older `Host` has one confirmed shutdown defect described below. Bozzetto/Composer adoption remains conditional on the owner-level integration gates in this assessment.

## Finding: older Host loses its cleanup-join boundary after a protocol fault

**F1 — medium severity, confirmed by fault injection; applies to the original `Host`, not the new `MailboxHost`.** [Host.fs](../src/Fidelity.FSharp.Incremental.Hosting/Host.fs), lines 165–188, makes `WaitForIdleAsync` throw immediately when `protocolFault` exists, then implements `CloseAsync` by awaiting that method. Consequently, close can terminate exceptionally before actual owned evaluators and cancellation callbacks terminate. Repeated close returns the same faulted task; the caller has no subsequent host operation that joins the remaining ownership. The internal failure catch at lines 74–80 faults an entry's `Done` without a separate physical-cleanup completion path.

This is not a false successful close. The error is observable and queries fail closed. The defect is the loss of the documented close/join lifetime boundary, which matters to any consumer disposing resources or replacing a worker after awaited shutdown. No ordinary public-command sequence was found that creates this internal disagreement; the reproduced trigger deliberately injects an implementation fault, exactly the fault-path gap requested by the checkpoint.

The isolated probe completes an eligible seed result, then starts two held evaluators. It corrupts one `Finished` acknowledgement to use `AttemptId UInt64.MaxValue`, producing a real `Core.step` `UnknownAttempt` refusal through the existing host error path. A separate barrier holds the second evaluator, and another holds its cancellation callback. The copied core, close logic, cancellation and query implementations are unchanged.

Executed observations:

- Both hosts with injection disabled keep close pending through evaluator completion and then through the separately held callback; close succeeds after both join.
- The injected original Host rejects further commands and revokes access to the seed result, but close faults while **both** ownership barriers remain held. The probe subsequently releases them independently; that harness cleanup does not repair the host or establish a host-owned join.
- The injected MailboxHost rejects commands and hides the old result, keeps close pending through both barriers, then reports the protocol fault with zero running steps. A diagnostic proves it consumed the evaluator completion while the callback still held its slot. It does not manufacture core `Drained` receipts after disagreement, so a faulted graph's pending count must not be interpreted as a physical-process count.

Evidence: `fault-probe.log`, `fault-probe/Program.fs`, `fault-probe/injection.patch`, source hashes and invocation instructions in `fault-probe/README.md`, under the evidence directory below. The harness exits zero when it observes both mailbox success **and the expected original-Host defect**; that exit is not a claim that both hosts satisfy faulted shutdown.

Requested repair: keep physical completion/join accounting independent of protocol validity; stop further admission, cancel remaining owned work, and retain a close operation that joins evaluators and callbacks before surfacing the aggregate failure. Do not fabricate successful core lifecycle acknowledgements to force an idle state. Add this two-barrier fault case as a regression test. Repair is required before adopting the original Host for ownership-sensitive supervision, or explicitly exclude that API from the adopted surface. It does not block a bounded MailboxHost integration experiment.

## General applicability

The architecture is reusable as an owner-declared incremental dependency graph and owned-execution protocol. The public API contains identities, ordered reads, revisions, demands, attempts, completion and suspension; it has no compiler, proof, editor, filesystem or device types. Shared demand, invalidation, fresh eligibility and cleanup ownership solve problems in document analysis, asset transformation and processing immutable data snapshots as well as compilation.

The useful boundary is a deterministic immutable core plus replaceable hosting mechanisms. The .NET mailbox uses Channels and tasks; neither belongs in the portable command/effect contract. A future native host must reproduce both core transitions and real execution-lifetime invariants. This is a concrete route toward self-hosting, not evidence of an implemented native host.

Generality has conditions: declared acyclic dependencies, owner-controlled immutable payloads, resource-free suspension and cooperative cleanup. It does not establish arbitrary actor behavior, dynamic dependency discovery, general fixed points, serialized CLR stacks or distributed recovery. These are scope boundaries, not reasons to make the core compiler-specific.

The response/environment distinction is especially important for every consumer:

- A response computed from an already-declared immutable snapshot may resume the same attempt.
- A newly observed fact that changes the computation's premises must update declared inputs/stamps and invalidate the old attempt. A new response token cannot silently add a read to that attempt.

An independent executed document-analysis probe demonstrates these rules with a typed immutable payload map: two clients share one word-count step; releasing one preserves the checkpoint; per-step disposal finishes before the sole active slot runs an independent banner transformation; a changed document input revokes the old continuation; the current response resumes once; duplicate delivery refuses; the unchanged banner result retains its attempt; close drains and revokes both results. This is one concrete noncompiler workload, not evidence for arbitrary effectful or distributed applications. The owner checks that a response belongs to its declared document snapshot; the library cannot infer that relationship from tokens.

Keep these laws in the generic library. Domain authority belongs at consumer boundaries: source/proof meaning in Baker/PSG, artifact acceptance and execution in Composer, and client routing, leases and worker ownership in Bozzetto. The same separation applies to database commits, file publication and device commands in other applications.

## Consumer integration conditions

These are explicit adoption requirements, not additional confirmed core defects.

| Boundary | Library behavior | Required owner/adapter behavior |
| --- | --- | --- |
| Acknowledgement and unknown outcome | `PostAsync` acknowledges after publication; cancellation after admission only detaches the observer. `MailboxReceipt` carries `Order` and `Effects`. | Correlate caller requests and retain their underlying completion independently of transport timeouts. Reconcile the exact operation before granting permission. A snapshot or empty effect list is not an exact-request receipt. Define duplicate delivery and restart behavior. |
| Reservation and mutation | Reservation withdraws eligibility and versions a scope; it is not an exclusive editing lease. | Serialize competing mutation owners. Two successful reservations at successive revisions do not prevent the earlier caller subsequently writing an external file. Bozzetto must retain file claims and await both library and compiler reservation fences. |
| Overload and control | Internal completion/cancellation and `CloseAsync` bypass external command quota. External reserve, release, close-scope and retire commands all share that quota. | Preserve a fair route for external control under ordinary-work saturation. Define admission/coalescing policy and measure control latency with a held evaluator and callback. `QueueFull` is an explicit refusal, not lost work, but callers must handle it. |
| External effects | Currentness is checked before scheduling; evaluation happens later on another thread. Cancellation is asynchronous. | Serialize the actual effect/commit with revocation. Checking eligibility and then performing I/O is insufficient. Keep Composer's validation/launch boundary; test reservation winning after scheduling but before launch, and the opposite ordering. |
| Payload and memory | Command capacity bounds external queued commands; concurrency bounds active steps. Suspended attempts, graph size, ready work, payload backing, observations and history have separate lifetimes. | Budget logical work and payloads, drain observations, coalesce updates and recycle epochs. Measure retained memory and large-transition latency. Neither configured number is a total-memory or fairness guarantee. |
| Multiple observers | `DrainEvents` and `DrainDiagnostics` consume shared queues. | One owner drains and projects observations to MCP/browser/editor clients or other subscribers. Independent clients must not consume one another's evidence. Status queries remain non-demanding. |
| Cleanup | Evaluator completion is trusted to include owned children and cleanup; callback cancellation is separately joined. | Join actual processes/resources. Preserve physical retirement and cleanup failures after logical authority is withdrawn. A wrapper task cannot certify that an abandoned child stopped. |

These policies can be expressed through generic adapters and conformance cases; they do not require importing Bozzetto identities into the core. A durable request ledger, distributed transport, resource-bearing continuation or specialized-target executor should be justified by an actual consumer before it becomes a mandatory library feature.

## Source review

- [Core.fs](../src/Fidelity.FSharp.Incremental/Core.fs), lines 403–453: suspension and resumption check the current attempt and complete handle, preserve original resolved reads, and consume a checkpoint before emitting `Continue`.
- Core lines 141–185: invalidation clears held authority while preserving obsolete ownership for cancellation and drain. Lines 244–246 exclude a replacement while an earlier attempt for that work remains pending.
- Core lines 455–505: success cannot bypass a held checkpoint; draining rechecks freshness before eligibility.
- [MailboxHost.fs](../src/Fidelity.FSharp.Incremental.Hosting/MailboxHost.fs), lines 94–101 and 263–265: external command accounting remains charged until state publication; acknowledgement follows publication.
- MailboxHost lines 107–117 and 172–192: callbacks and evaluators run outside the receive loop. Lines 158–170 retain capacity through evaluator and cancellation cleanup.
- MailboxHost lines 284–305: pre-cancelled requests are absent; cancellation after admission detaches the waiter. Lifecycle events owned by the host cannot be supplied externally.

No concrete regression was found in the reviewed core suspension/resumption path. Suspended cross-scope/census variants use the same invalidation machinery as existing tests, but lack separate new named cases; that observation is source evidence, not additional executed coverage. The existing responsiveness test holds an asynchronous task, not an evaluator blocked synchronously before returning, and establishes no thread-pool starvation bound.

## Independent execution and provenance

Evidence directory: `/home/hhh/.cache/bozzetto/audits/incremental-mailbox-2026-10-01/`.
The audit used .NET SDK **10.0.401** and the existing shared Bozzetto lease service. No compiler promotion, package publication or service replacement occurred.

| Check | Result and scope |
| --- | --- |
| Release solution build | Passed, zero warnings/errors; `release-build.log`. |
| Unfiltered Release test suite | **77 executed, 77 passed, zero failed/skipped**: 36 baseline core, 13 suspension, 10 original Host and 18 mailbox cases. `independent-release.trx`, log and `test-inventory.log`. The 200-edit model is one test, not 200 tests. |
| MailboxSteps sample | Passed: reservation acknowledged at command 6, stale resume refused, current value 11, exactly one resume, stable attempt retained, close with zero pending; `mailbox-sample.log`. |
| SelectiveReuse sample | Passed: value 30→35, visits 3→2, stable attempt retained, old handle revoked; `selective-sample.log`. Timings are not performance acceptance. |
| Author's mutation evidence | Independently inspected the corrected executed mutation receipt: one failing capacity case, `Expected 1, got 0`. The earlier invalid-filter zero-test attempt is excluded. Mutation was not repeated in this audit. |
| Preview package provenance | Both archive hashes, nuspec implementation pins, packaged DLL hashes and package-only dependency closure match [Validation](Validation.md). The consumer references Hosting only; its closure contains Hosting, core and FSharp.Core 10.1.401, with no project references. |
| Package-only consumer rerun | Existing consumer binary executed successfully against the hash-checked packaged DLLs; `package-consumer-rerun.log`. This audit did not repeat package restore/build or publish a feed package. |
| Noncompiler consumer probe | Passed the document-analysis assertions described above; `generic-probe.fsx` and `generic-probe.log`. Separate from the repository's 77 cases. |
| Isolated protocol-fault probe | Release build passed with zero warnings/errors. Two controls and the mailbox fault case passed; original Host early-close defect detected. `fault-build.log`, `fault-probe.log`, source/diff/harness under `fault-probe/`. Four scenarios, not additions to the repository test count. |

Independent TRX SHA256: `77d792c90cee235ddcdb2ef60dce64c12d0957a2bd5e37f1736ee544209f5df8`.
`reviewed-source.sha256`, `tested-binaries.sha256`, `receipts.sha256` and `probe-evidence.sha256` preserve the audit's source, binary and artifact vector. Preserve this evidence directory when clearing caches. The fresh checkpoint build has different assembly hashes from the implementation-pin packages; do not substitute one for the other. Fault-injected assemblies are isolated copies and were never used for the original 77-test acceptance run or installed into either repository/service.

Freshly tested checkpoint assemblies:

| Assembly | SHA256 |
| --- | --- |
| Core | `95091c276951fb98b71ca9bd1da212b6bf5f18cd3026869fc308535d4f1f318c` |
| Hosting | `5e41f35339e0ddbca8fb18a2862143be5e9c2ee254b7cb71e4517620ab9b81c9` |
| Tests | `8b3f9145aee3db8add4cf5868c75ce3474dc2ae270ab022ccc7f93008f6b5f91` |

The first independent script invocation failed to compile because its `use` binding required `IDisposable` in the selected FSI environment while the host implements `IAsyncDisposable`. The script now explicitly awaits close. `generic-probe-compile-only-failure.log` preserves that unsuccessful harness attempt; it executed no assertions and is not library failure evidence.

## Remaining H1 acceptance

Bozzetto is still an intended consumer, without a library project/package reference. Before adoption is declared complete, run an actual compiler-owned workspace journey through the library-backed adapter: two clients sharing demand; one leaving; reservation before mutation; cold build and selective reuse; current disk and proof/artifact receipts; delayed completion and stale response across revision/epoch replacement; exact-request timeout reconciliation; both reserve/physical-launch orderings; saturated control admission; and failure/close with a real child process still alive.

Record compiler distribution identity, library package identity, workspace/compiler epochs and physical cleanup evidence together. Library eligibility does not authorize proof reuse or native execution. Native host conformance and cross-target/distributed orchestration remain separate acceptance horizons; this audit does not validate CPU substitution or target numeric/proof equivalence.
