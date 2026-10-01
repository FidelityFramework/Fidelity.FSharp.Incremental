# Response to the mailbox auditor: F1 cleanup ownership

The [independent assessment](Mailbox_Auditor_Assessment_2026-10-01.md) identified
a valid defect in the older `Host`: a protocol error could complete close before
owned evaluators and cancellation callbacks terminated. The repair is implemented
and the complete Release suite passes **84/84**. This response is author evidence
for follow-up review; it does not replace the auditor's assessment or declare
consumer integration complete.

## Repair and retained boundaries

`Running.Done` now records physical completion independently of protocol validity.
A rejected lifecycle acknowledgement stops new admission, cancels all remaining
owned work, and hides eligible results. Each worker continues joining its evaluator
and cancellation callbacks before disposing its token source and releasing capacity.
The final callback check, resource disposal and ownership removal remain under the
same lock; callbacks themselves execute outside that lock.

`CloseAsync` installs its shared completion task before attempting retirement.
It joins physical ownership and disposes the semaphore before reporting an
`AggregateException`. `WaitForIdleAsync` likewise joins before reporting a
protocol failure, while its observer token may still cancel just that wait.
The aggregate preserves the original protocol error and evaluator/callback
failures belonging to work owned when the fault occurred or observed afterward.
Already-drained historical failures are not accumulated into a later shutdown.

Normal evaluator failure remains a failed computation. Normal cancellation-callback
failure remains an observable diagnostic; these alone do not poison the host
protocol. Evaluator failures are now also diagnostic entries. Close rejects further
commands once it starts. After protocol disagreement, physical cleanup does not
manufacture `Finished`, `Drained`, `Offer` or `EpochDrained` certificates. Graph
pending counts may therefore remain nonzero after physical shutdown.

The public three-argument constructor still supplies the real `Core.step`.
An internal constructor, visible to the test assembly, accepts that transition
function so tests can inject a real core refusal. There is no environment-variable
fault switch or public injection parameter. `Core.fs`, its public token protocol,
and `MailboxHost.fs` are unchanged by this repair.

## Executed regression and controls

The first executable four-case regression ran against the unrepaired host with
only the internal test seam added. Both cleanup-order cases detected early close;
the automatic-cancellation case also detected queued evaluation starting after
the fault. The uninjected control passed: **three failed, one passed**. An earlier
attempt stopped at an NUnit overload inference error in a test helper; it executed
no assertions and is preserved separately.

The repaired implementation passed those four cases. Three additional cases
exercise distinct shutdown branches, for seven new cases in the final suite:

- Corrupted `Finished` and `Drained` acknowledgements use an unknown attempt ID
  through the real core, with both evaluator-first and callback-first cleanup.
- Rejected `Retire` uses a foreign epoch through the real core. Close returns one
  stable task without throwing synchronously, joins both barriers, then faults.
- Automatic fault cancellation stops queued evaluation before a caller closes.
- Uninjected shutdown joins the two independent barriers and emits its genuine
  epoch-drained receipt.

The evaluator and callback have separate completion barriers. Marker diagnostics
establish that the host consumed a failure while the other barrier remains held;
elapsed time never grants cleanup permission. Fault cases check the exact prior
seed handle, refused admission, shared close identity, preserved aggregate causes,
and absence of fabricated success receipts. The final suite contains the original
77 cases plus these seven, with zero failures or skips.

Raw evidence is external at
`/home/hhh/.codex/work/incremental-host-fault-2026-10-01/`: `red.log` is compile-only,
`red-executed.trx` records the three failures and control, `green-focused.trx`
records the initial repaired cases, and `release-final.trx` records all 84.
Source and tested-binary hashes accompany the logs. The restored test receipt
SHA256 is `c085da659334e176d8593127668307c8ed10ecab5608e92010169689d65f7286`.
The Release solution build also passed with zero warnings or errors. Both samples
passed: the original host retained the 3→2 selective-work ratio, and the mailbox
sample preserved stale-resume refusal and unchanged-attempt reuse. Their timings
remain smoke observations, not performance acceptance.

An independent local source review checked the final lifetime ordering. LAN TWO's
bounded review of the old source confirmed existing concerns but also contained
incorrect causal claims; those were rejected in its external disposition. That
advisory review is separate from the executed regression and the consumer auditor.

## Consumer gates and follow-up

The audit's general-purpose applicability finding is retained. Its adoption
conditions remain required: exact-request reconciliation, mutation ownership,
control admission under load, external-effect authority at the actual commit or
launch, logical/payload memory budgets, one observation owner, and real-process
cleanup. This repair does not establish those owner-level policies. It changes
no compiler distribution, workspace adapter or live service.

The auditor can rerun the seven cases with:

```sh
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj -c Release --filter 'FullyQualifiedName~HostFaultTests'
```

Use the shared build lease and SDK 10.0.401. Follow-up findings can be returned in
`docs/Mailbox_Auditor_Followup_2026-10-01.md`, identifying the reviewed source pin
and distinguishing library cleanup evidence from actual consumer adoption.
