# Validation checkpoint — 2026-10-01

## Functional async host — preview.4

The final restored Release suite passes **99/99**, zero failures/skips. The
preceding 84 cases remain; 15 new cases cover explicit startup/admission,
exact-operation reconciliation, observer cancellation, physical joins, explicit
stop requests, cleanup exceptions, suspension authority and a bounded exact Core
trace. `MailboxHost` now delegates to this Async engine, so its existing 18 cases
exercise the new implementation through the compatibility boundary. The older
`Host` repair remains unchanged apart from relocating a shared diagnostic type.

The full solution builds without warnings/errors. All three samples pass.
Eight isolated FSharp.Core lifetime controls pass separately. A deliberately
early-returning faulted-idle mutant fails its intended join assertion; after
restoration and rebuild, all 99 repository tests pass. The initial test-helper
compilation and mutant-filter errors executed no tests and are recorded separately.

The [functional checkpoint](Functional_Async_Checkpoint_2026-10-01.md) records the
API timing, evidence, SHA256 receipts and remaining adoption conditions. Final TRX:
`6f16a90cd9d06d5a2b6c0043f234c455fe6cd8a8cd24fd0824f9b567001cd05b`.
Evidence is external at
`/home/hhh/.codex/work/incremental-functional-2026-10-01/`.
Neither Bozzetto/Composer integration nor native-host equivalence is established.

## Faulted host cleanup — preview.3

The [independent assessment](Mailbox_Auditor_Assessment_2026-10-01.md) found that
the original `Host.CloseAsync` could report an internal protocol fault before
owned evaluation and cancellation callbacks joined. The repair separates physical
completion from graph acknowledgement, cancels all owned work on disagreement,
and reports the aggregate error only after joining. The core and `MailboxHost`
source are unchanged.

Seven focused cases now cover `Finished` and `Drained` rejection with both cleanup
orders, rejected retirement, automatic cancellation before close, and uninjected
shutdown. The initial four-case red run executed three failures and one passing
control against the old implementation. The final full Release suite passes
**84/84**, zero failures or skips. Receipt `release-final.trx` has SHA256
`c085da659334e176d8593127668307c8ed10ecab5608e92010169689d65f7286`.
Logs and source/binary hashes are external at
`/home/hhh/.codex/work/incremental-host-fault-2026-10-01/`.
See the [repair response](Mailbox_Audit_Response_2026-10-01.md) for the precise
changes, reproduction and consumer adoption conditions.

## Mailbox and explicit steps — preview.2

The restored Release suite passes **77/77**, zero failures or skips: 49 portable
core cases, 10 original host cases and 18 mailbox cases. The 46 prior cases remain
present; 13 suspension and 18 mailbox cases are new. The whole-solution Release
build passes with zero warnings and errors. SDK 10.0.401 remains the build host.
This is library validation; neither Composer nor Bozzetto has adopted this API.

The new cases exercise exact one-use resumes, changed declared response inputs,
reservation acknowledgements, shared demand, bounded command admission, queued
resume invalidation, logical ownership while suspended, and actual cleanup before
capacity reuse. A response token does not discover dependencies: the owner still
supplies complete reads and immutable payloads. Queue burst checks compare accepted
receipt order and the last accepted input; they do not assert a timing-dependent
number of queue refusals or establish a fairness guarantee.

One discriminating control deliberately released the physical slot as soon as an
evaluator returned, before its held cancellation callback finished. The focused
test **failed as intended** (`Expected 1, got 0`). It first observed the completed
step's diagnostic and then a FIFO command acknowledgement, so the assertion did
not depend on an arbitrary delay. Correct source was restored and rebuilt; the
full 77-case suite passed again. This checks a concrete faulty implementation, not
every possible scheduler interleaving.

The first full test attempt stopped at two F# indentation errors in test bindings;
no semantic tests executed. A first mutation selection attempt had an NUnit filter
syntax error and likewise executed no tests. Both are preserved separately from
the subsequent executed results. The intentional mutant is absent from this
checkpoint.

The runnable `MailboxSteps` sample confirms that reservation is acknowledged
before input mutation, an obsolete suspension cannot invoke its evaluator, the
fresh response produces value 11, an independent unchanged attempt is retained,
old eligibility is revoked, and close leaves zero pending attempts. The original
`SelectiveReuse` sample still reports 30→35, visits 3→2 and retained unchanged work.
Neither is a compiler integration or throughput measurement.

Raw receipts are external at
`/home/hhh/.codex/work/incremental-mailbox-2026-10-01/validation/`.
The final restored test receipt is `release-final.trx`, SHA256
`45ac8a566ac5e04b0d0e3b3a89e38a1478b4ec71384e266ff069595d2a5bca13`.
The tested core assembly SHA256 is
`30c8b9d36fe0451bc1717b46f41053cb0d50c0ab5e94ec3b5aa3a42240a4cbc0`;
the tested Hosting assembly is
`0233c70df5dc4db41bd7b416a5e0099b9be44e4d7f5ec2b4ad82d03121c10452`.
These identify the tested pre-commit build; any subsequent package build has its
own distribution receipt. See [the auditor checkpoint](Mailbox_Auditor_Checkpoint.md)
for review scope and consumer acceptance work.

The implementation was then committed as
`5f87a548f28360bd91fe59c4c10dd014b544d9ad`. Both preview.2 packages were built
from that clean commit and their NuGet repository metadata carries that exact pin.
The core depends on FSharp.Core 10.1.401; Hosting adds the core package. An isolated
console project with only a Hosting `PackageReference` restored, built and ran
the complete mailbox sample successfully, with no project references. Its assets
contain exactly those three packages. This validates local distribution shape and
the exercised public API, not consumer adoption or package-feed publication.

| Local preview.2 package | SHA256 |
| --- | --- |
| Core | `ae864026be098e0c6f705fa15dcdc2daabc4baa1fb8bf2d66b71a83b6655e928` |
| Hosting | `3abf1880af74235290aa2a594648aa8aa264d73e916e2a676b2ea338bb0f6474` |

Packaging regenerated assembly metadata for the committed source. The packaged
core assembly SHA256 is
`af6877134ea2d4990e2dea3fec4601d9a177868eb0d9668e4d05af2d46af9c75`;
the packaged Hosting assembly is
`590a178f77a12f354d9fb0725dc93bf1b2cacd61d57d9de86669bc42fc67e23e`.
The external validation directory also contains `source.sha256`,
`tested-binaries.sha256`, package archives and `package-consumer.log`.

## Original foundation — preview.1

Both the first Debug suite and the final Release suite passed **46/46 tests, with zero failures or skips** on .NET 10. Each contains 36 core tests and 10 host tests. Release testing includes drained-attempt compaction and two strengthened post-drain assertions. The six preceding attempts stopped at source or test-helper compilation errors; they are retained as build evidence and do not count as executed semantic tests.

| Gate | Recorded outcome |
| --- | --- |
| First complete Debug suite | 46 passed / 46 executed; exit 0 |
| Whole-solution Release build after drained-attempt compaction | Exit 0; zero warnings and errors |
| Final Release suite after compaction | 46 passed / 46 executed; exit 0 |
| SelectiveReuse sample execution | Exit 0; value 30→35, visits 3→2, stable attempt retained, old handle revoked |
| Package creation and dependency inspection | Two `0.1.0-preview.1` packages created; exit 0; no FDA/IcedTasks dependency |
| Isolated package consumer | Restored Hosting and its dependencies from the local package directory; built and ran the sample successfully without project references |
| Composer or Bozzetto adoption | Not implemented; requires a separate integration gate |

Core tests cover demand sharing, duplicate dependency occurrences, ordered read receipts, transitive withdrawal, retained results with fresh eligibility handles, stale or foreign identities, scope retirement, malformed-command atomicity, exact lifecycle acknowledgements, and deterministic replay. One test applies 200 deterministic edits to an eight-node DAG spanning two scopes. An independent recursive evaluator computes expected values without calling the core or using its caches. The test compares every current value and the exact visited work set, including unchanged-input and explicit-retention steps. This is one bounded reference-model test, not 200 independent tests or a performance benchmark.

Host tests exercise cold registration and retry, shared execution, queued cancellation, rejection of late success after reservation, retained capacity until completion, cancellation of an individual waiter, token-ignoring work during close, throwing cancellation callbacks, unchanged-result retention, bounded independent concurrency, and rejection of forged completion/drain events. These executed examples do not establish every interleaving, forced preemption, or cleanup of child tasks that an evaluator abandons.

The final Release receipt validates the private compaction of drained attempts. It removes full request/read records from completed attempts while retaining minimal completion outcomes for exact acknowledgement behavior; it does not promise bounded total memory for an indefinitely live epoch. Stamp, demand and completion tombstones, current cached read receipts, and owner-managed payload backing still need an epoch lifecycle and eventual disposal.

The sample measured an affected-work ratio of 2/3 (0.667). Its elapsed observations were 19.349 ms initially and 0.374 ms after the edit. This single smoke run includes cold-start effects and is not a speedup claim, compiler benchmark or solver measurement. Package creation checks distribution shape; it does not constitute package-feed publication or consumer integration.

An isolated F# console project outside the repository referenced only the Hosting
preview package. It restored, compiled and reproduced the same values, 2/3 visit
ratio and stale-handle refusal. The core package depends only on FSharp.Core
10.1.401; Hosting adds the core package. NUnit and the test SDK are confined to
the test project. SDK 10.0.401 was used for all library gates.

Library eligibility is bookkeeping, not compiler admission. The tests do not establish Baker dependency completeness, proof reuse, native artifact acceptance, persistence across restart, collection operators, or production-scale throughput. Composer's proof and generation gates remain unchanged. See [Architecture](Architecture.md) for the ownership contract and [Audit](Audit.md) for the separately measured upstream experiments.

Original evidence is outside Git at `/home/hhh/.codex/work/hosted-incremental-2026-10-01/library-validation/`: `seventh.trx`, `release.trx`, and the build, test, sample and pack logs with recorded exits. The Release TRX SHA256 is `f63f2aab216fe977e761c5347273eec4af081e2a34f3085b4e94b5cf63c34292`. These results establish the local checkpoint; publishing a committed revision and adopting it in another application are separate actions.
