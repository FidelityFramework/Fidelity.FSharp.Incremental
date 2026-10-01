# Mailbox auditor follow-up — 2026-10-01

**F1 is closed for the reviewed repair.** Independent source review, an unfiltered **84/84** Release test run, and the original independent fault probe adapted to the repaired source all confirm that `Host` joins owned evaluation and cancellation callbacks before reporting protocol failure. No new correctness defect was found in this bounded review. Consumer integration conditions remain open.

Reviewed checkpoint: `33d5ff272c81bd0df98a1c920ded7ce267debb27`.
Implementation and preview.3 package pin: `c7fbcd17daf9bbc54b49559975a34617818f44bf`.
The later commit changes only the [repair response](Mailbox_Audit_Response_2026-10-01.md). The checkout was clean at audit entry. This follow-up changes documentation only.

The [original assessment](Mailbox_Auditor_Assessment_2026-10-01.md) remains unchanged, with SHA256 `ee20df2a721cc31a8c1827445386da7cedf4428fbfa0f4c6b281deb9240d02e3`. Its F1 description is the historical finding; this document records the repair disposition.

## Why the repair closes F1

In [Host.fs](../src/Fidelity.FSharp.Incremental.Hosting/Host.fs):

- Lines 14–21 give each owned attempt a physical completion signal distinct from protocol validity. Lines 125–150 continue cleanup after a rejected lifecycle acknowledgement. The final callback check, token disposal, capacity release and ownership removal occur under the same lock; callback execution occurs outside it.
- Lines 55–67 stop further admission through the fault state and request cancellation of all remaining ownership. Already queued evaluation also checks that state before invocation. Scheduling remains distinct from authority to perform a later external effect.
- Lines 140–144 remove a physically completed entry before its `Drained` acknowledgement. If that acknowledgement fails, the fault path retains its failures without cancelling its disposed token source.
- Lines 153–165 and 209–211 join physical work before reporting the aggregate failure. Cancelling an idle observer can still end that observer's wait; it does not certify cleanup or release shared ownership.
- Lines 216–240 install a stable close task before attempting retirement, seal admission, join actual work, dispose capacity and then report failure. A rejected retirement no longer throws synchronously in place of an owned close operation.

Normal evaluation/callback failures remain computation failures or diagnostics; they do not themselves imply protocol corruption. The aggregate after protocol failure retains failures from ownership present at the fault and subsequently observed failures. Source inspection supports the exclusion of already-drained historical failures; there is no separate historical-failure regression in the seven new cases.

No successful `Finished`, `Drained`, `Offer` or `EpochDrained` result is invented to make a corrupted graph appear idle. A faulted graph may retain pending attempts after physical ownership has joined. Consumers must distinguish those two observations.

## Independent execution and provenance

Evidence directory: `/home/hhh/.cache/bozzetto/audits/incremental-host-repair-2026-10-01/`. Preserve it when clearing caches. Runs used SDK **10.0.401** and the existing shared Bozzetto lease service; acquired leases were released. No service replacement, dependency adoption, compiler promotion or package publication occurred.

| Check | Independent result |
| --- | --- |
| Release solution build | Exit 0, zero warnings/errors; `release-build.log`. |
| Full unfiltered Release suite | Exit 0, **84 executed/passed**, zero failed/skipped. Inventory: 36 core, 13 suspension, 10 original-host, 18 mailbox, seven host-fault cases. `independent-release.trx`, log and `test-inventory.log`. |
| Original independent fault probe on repaired source | Isolated Release build passed with zero warnings/errors. Both ordinary controls and both injected host cases passed; `fault-build.log`, `fault-probe.log`. These four scenarios are separate from the 84 repository tests. |
| Both repository samples | Passed. Selective reuse retained visits 3→2 and the stable attempt; mailbox sample retained acknowledged reservation, stale-resume refusal, one current resume and drained close. Timings remain smoke observations. |
| Preview.3 package-only consumer | Existing consumer binary rerun successfully against hash-verified packaged assemblies. It reproduced selective reuse and verified the public three-argument constructor. `package-consumer-rerun.log`. Package restore/build was not repeated by this audit. |
| Author's repair controls | Inspected recorded RED: four executed, three failed and one control passed. Two failures caught early close; the third caught queued evaluation after fault. Initial NUnit overload error was compile-only. Recorded focused GREEN was four passes; final receipt was 84 passes. All preceding 77 test names remain present. These historical runs were inspected, not re-executed on old source. |

The seven new regressions inject real `Core.step` refusals for `Finished`, `Drained` and `Retire`; the four acknowledgement cases cover both evaluator-first and callback-first cleanup. Separate barriers and observed diagnostics make the checks discriminate physical cleanup from a cancellation request. They also check stable close identity, retained aggregate causes, refused commands, revoked seed eligibility and absence of fabricated success receipts.

The independent probe retains the original F1 experiment's separate evaluator/callback barriers and seed result. An isolated copy corrupts one terminal attempt ID through the same internal helper as the first audit. Both hosts must now keep close pending after evaluation returns while its callback remains held, then report the injected failure after the callback joins. The repaired Host must additionally retain the evaluator failure in its aggregate. `fault-probe/injection.patch` records the source instrumentation; `harness.patch` records the changed expectation from defect detection to repaired behavior. Production source has no environment-controlled injection. API/Core and the production MailboxHost are unchanged from the original reviewed checkpoint; copied API/Core bytes were also checked.

The author's preview.3 archive hashes and implementation metadata match the repair response. The package-only consumer has no project references; its closure is core, Hosting and FSharp.Core 10.1.401. The internal transition constructor is exposed only to the named test assembly. Public-constructor preservation is compatibility evidence for this repair, not a requirement to preserve an object-oriented API indefinitely.

Independent TRX SHA256: `a77e3e7911f168c02b1d08cd9b5d2cfcf9d3ee5719f16f9bdf254f45389c5033`.

| Fresh checkpoint assembly | SHA256 |
| --- | --- |
| Core | `152ccbbfc206b6a6541a19112ed6d710a35fd16616141fff0b6115d439011378` |
| Hosting | `88644e5f426c274f454c6c12381c3e89fd1fba6a25c49ca76cd366db6b8b52e8` |
| Tests | `e722a31f0340e7622ca7a2873ab683031de1415465b0d5d9e16f765dddefe227` |

`reviewed-source.sha256`, `tested-binaries.sha256`, `original-assessment.sha256`, `audit-evidence.sha256` and the isolated probe manifests preserve the corresponding source/binary vectors. Fresh checkpoint builds, historical pre-commit test assemblies and implementation-pin package assemblies are distinct receipts. No injected assembly was installed or substituted into the 84-test run.

## Quiet API and the path to Clef

During this follow-up, the user made the design preference explicit: keep a quiet, typed functional API amenable to Clef from the outset; prefer pure F# `Async` for interim .NET workflows; avoid boxed payloads, casts or OO mechanisms substituting for explicit semantics. The clarification permits necessary .NET interoperability. It does not require pretending that .NET's execution machinery is absent.

**The current core is aligned; the Hosting surface still needs design work.** API/Core use typed records/unions, explicit identities, immutable state and commands returning results/effects. Source inspection found no explicit `obj` casts, boxing/unboxing, reflection dispatch or object hierarchy carrying their semantics. This is a source-shape assessment, not a successful Clef compilation receipt.

Hosting publicly exposes `Host`/`MailboxHost` objects, `Task`-returning operations and evaluators, cancellation tokens and async disposal. Its private mutable entry classes and CLR synchronization machinery also remain. The two `obj()` values are lock gates; the explicit Host upcasts are `Task<unit> :> Task`, not payload casts. No `:> obj` or object-backed semantic payload store was found in `src` or `samples`. That distinction explains the current code; it does not settle the desired API shape.

The next API tranche should meet these criteria:

1. Prefer small module operations, explicit typed state/handles and composable F# async workflows. Keep CLR task conversion, cancellation-token plumbing and disposal interoperability at named execution boundaries. The workflow implementation should follow that direction too; a thin `Async.AwaitTask` facade alone does not establish it.
2. Preserve a cold description of work until explicit owned admission/start. Decide coordinator startup and command admission timing deliberately: today the mailbox constructor starts its coordinator, and `PostAsync` admits synchronously before returning a task. Moving these into cold async changes observable behavior unless that boundary is specified.
3. Separate cancellation of an observer from cancellation of an owned attempt. Detaching a reply waiter must neither retract an admitted command nor cancel another client's shared work. Cleanup must continue after authority is withdrawn and after observer cancellation. Awaited shutdown must join all owned evaluator/child/callback work before reporting its outcome.
4. Retain typed owner payloads and explicit captured data. `ValueToken` is an identity, not a license to hide `obj`, an arbitrary CLR closure or a resource-bearing continuation in an untyped store. Declare response/environment dependencies and preserve exact one-shot, epoch/attempt-bound resumption.
5. Keep scheduling, source semantics and external-effect authority separate. Async composition must preserve declared read occurrences and the existing reservation, freshness and drain laws. It must not infer dependencies from convenient syntax or grant launch authority from queue order.
6. Compare portable command/effect traces and real host ownership tests across the revised .NET host and eventual native host. F# async is the preferred authoring model; its current CLR implementation is not the native specification. A native continuation can represent the next step and typed captures without requiring a CLR `Task` object. Compiler settlement of capture/lifetime semantics remains compiler work, not a library cast or serialization shortcut.

This is an open API/adoption direction, separate from the repaired F1 correctness defect. It applies to noncompiler consumers as well as Bozzetto. The earlier independent document-analysis probe remains useful evidence for generic dependency/demand semantics; it was not rerun as evidence of an async API that does not yet exist.

The user supplied [Dotnet to Fidelity Concurrency](https://clef-lang.com/blog/dotnet-to-fidelity-concurrency/) and explicitly identified it as an early directional post. It supplies historical context for separating async authoring from runtime representation. Its illustrative APIs, component assignments and performance aspirations are not current requirements or implementation evidence. This assessment takes its current API direction from the user's clarification and checks implemented behavior against the reviewed source.

## Consumer adoption remains open

The original assessment's integration conditions still apply: exact-request reconciliation after lost replies; exclusive mutation ownership; external control admission under saturation; authority at the actual effect/launch boundary; logical-work and payload budgets; one observation owner with client fan-out; and real-process cleanup. Bozzetto/Composer still need the actual workspace journey through a selected library/host/compiler distribution.

Closing F1 removes the original Host's identified cleanup blocker. It neither completes those integrations nor endorses the current CLR-shaped public API as the final Clef-facing contract. Native compilation, specialized-target numeric/proof equivalence, resource-bearing continuations and distributed recovery remain separately evidenced work.
