# Consumer observation auditor assessment — 2026-10-01

**Accept `87c77d9`'s observation addition for consumer integration.** No blocking
correctness issue was found. It preserves the quiet, typed F# async API and
supports observing an individual result without requiring unrelated work to
finish. The accepted `613e260` baseline and all three earlier assessments remain
unchanged. Consumer integration and compiler distribution promotion remain
separate gates.

Reviewed implementation: `87c77d91ab24c8e4d065e4e726f8c06abc3180c4`, preview.5.
Validation used an isolated archive of that commit. The live checkout was
`0c38d00`, whose later changes contain documentation only. No library or consumer
implementation was changed by this audit.

## Contract review

`AsyncMailbox.watch` captures the published snapshot and its actual change cell
under the same lock. The retained cell therefore covers a change that happens
before the returned async workflow starts. Publication, admission, close and
fault paths signal under that lock. Observation is repeatable, does not consume
owner events, and cancellation detaches only that observer.

A notification means to inspect state again. It can reflect unrelated work or
admission before a command commits; it is neither a reservation acknowledgement
nor result authority. Consumers must retain exact command handles, check their
own work/scope and eligibility, and recapture `watch` after each notification.
Once `IsClosing` is true, they must observe the retained close operation because
another change signal is not guaranteed. Close still joins physical cleanup.

These operations are useful for general incremental workloads: the independent
probe uses two ordinary work items, with no compiler-specific interpretation of
their results. Compiler proof, publication and actual-launch authority remain
with their existing owners. This review does not establish native lowering.

## Independent evidence

Evidence is retained at
`/home/hhh/.cache/bozzetto/audits/incremental-observation-2026-10-01/`.
SDK 10.0.401 was used with Bozzetto build/test leases, released afterward.

| Check | Result |
| --- | --- |
| Isolated Release solution build | Passed, zero warnings/errors. |
| Unfiltered Release suite | **102 executed and passed**, zero failures/skips; all previous 99 cases retained. |
| Independent package-only consumer | Built and passed against the recorded preview.5 packages and FSharp.Core 10.1.401. No project references. |
| Unrelated notification | A retained signal can be observed twice without making held foreground work eligible or consuming owner events. |
| Individual completion | Foreground observation finishes while background work remains running with one pending attempt and global idle incomplete. |
| Close handoff | Active and late observers use retained close; it remains incomplete until deliberately held evaluator cleanup is released. |

The last three rows are three controls in one independent scenario, additional
to the 102 repository tests. Barriers establish ordering; deadlines only bound
failed observations. Earlier cancellation/fault tests remain in the full suite;
the prior audit's separate injected-fault and eight lifetime probes were not
repeated for this small addition.

`release-build.log`, `release-tests.log`, `independent-release.trx`,
`test-inventory.log`, `observation-probe.log`, source/binary manifests and the
package-only probe project preserve the receipts. Two initial probe build errors
(duplicate FSharp.Core reference and an incorrect request field in the harness)
were corrected; they supplied no behavioral evidence. The author's recorded
102-test receipt is a separate Debug run.

Package archive hashes match the checkpoint and the Bozzetto, Composer and Clef
vendored copies. Core records the full source commit in its nuspec; Hosting
records the unambiguous abbreviation `87c77d9`. The checkpoint wording is
corrected to reflect that minor metadata distinction; no repackaging is needed.

The remaining acceptance work is in the consumers: prove shared compiler-state
serialization across editor/Composer paths, retain owned work through cleanup,
and preserve reservation ordering at actual publication/launch. The reviewed
consumer observation loops are consistent with this API, but their real compiler,
process and deployed-worker journeys require their own receipts.
