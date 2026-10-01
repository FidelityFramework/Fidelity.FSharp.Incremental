# Mailbox and continuation auditor checkpoint

Status: ready for independent audit of the mailbox/step tranche, with a restored Release suite passing 77/77 tests. The preceding commit `affb92d206242a21fc2032d51016cd0f29e7e54f` is the historical 46-test baseline. The Bozzetto peer is the intended auditor; no deployment or consumer adoption follows merely from this document.

Implementation and package source pin:
[`5f87a548f28360bd91fe59c4c10dd014b544d9ad`](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental/src/commit/5f87a548f28360bd91fe59c4c10dd014b544d9ad).
The following documentation checkpoint adds distribution receipts without changing
that implementation. Both local `0.1.0-preview.2` packages identify this source pin;
an isolated package-only consumer built and ran the mailbox sample successfully.
No package feed publication, compiler promotion or service replacement occurred.

Return the assessment in `docs/Mailbox_Auditor_Assessment_2026-10-01.md`, identifying the reviewed commit, evidence checked, concrete findings and remaining adoption conditions. That assessment has not been authored here on the auditor's behalf.

From this repository, using .NET SDK 10.0.401 and the existing shared build lease:

```sh
dotnet build Fidelity.FSharp.Incremental.slnx -c Release
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj -c Release
dotnet run --project samples/MailboxSteps/MailboxSteps.fsproj -c Release
```

The bounded claim is that immutable dependency bookkeeping can retain an owned logical attempt across a resource-free suspension, resume one exact checkpoint once, and serialize commands through a responsive .NET coordinator. [Steps and Mailboxes](Steps_and_Mailboxes.md) defines the distinction between a logical attempt, active step, queue admission and acknowledged reservation.

## Review the actual contract

The portable API now includes `StepId`, `SuspensionId`, `SuspensionHandle`, `ResumeRequest`, `Action.Suspend`, `Action.Resume`, `EffectAction.Suspended`, `EffectAction.Continue`, and `WorkStatus.AwaitingResume`. Validate these against current `API.fs` and `Core.fs`, not this name list alone. The environment token is immutable owner data; suspension must not silently retain live resource ownership. A step evaluator's task must include all child work and cleanup before it reports suspension.

The authored `MailboxHost` accepts a `StepEvaluator`, with `StepInvocation.Start/Resume` and `StepOutcome.Complete/Suspend`. Its `PostAsync` distinguishes bounded admission failures from invalid commands and returns a `MailboxReceipt` only after state publication. A short receive transition may register and schedule work but must not await that work. External command capacity and active-step capacity are distinct. Caller-supplied lifecycle events must not bypass host ownership; the original non-step `Host` must reject operations it cannot interpret. Query/event observation must not execute a second copy of a step.

`Action.Retire` retires core authority only. Host shutdown still requires `CloseAsync()`; `IsClosing` and the graph's `Retiring` state deliberately describe different boundaries. Queue accounting must remain outstanding until the processed command's state is published, and observable effects must agree with that published snapshot.

Environment and response tokens do not add dependency edges. The auditor must verify that any consumer's semantic response inputs are already represented by declared reads/stamps or cause owner-issued invalidation. A one-shot handle prevents replay; it does not establish completeness of the caller's dependency declaration. Public handles are protocol identities, not security capabilities: authentication and isolation of untrusted callers require an external owner boundary.

## Discriminating acceptance cases

| Concern | Required observable distinction |
| --- | --- |
| One-shot continuation | Correct handle resumes once; duplicate, modified environment/step, foreign epoch and superseded handle refuse without a second evaluator invocation |
| Freshness while waiting | Reserve, changed dependency/census, released last demand, scope close and retirement revoke held resume authority; late replies cannot restore eligibility |
| Resource-free checkpoint | Suspended step releases active capacity only after its task and cleanup finish; logical ownership persists; success cannot bypass a held checkpoint |
| Coordinator responsiveness | A held or blocking evaluator does not prevent a reservation acknowledgement or cleanup notification; independent admitted work can progress |
| Queue saturation | Refusal/backpressure is observable and does not lose commands; completion and retirement cannot depend on a data-queue slot that only they can release |
| Edit acknowledgement | A queued reservation is insufficient; only successful committed acknowledgement permits the caller's edit; cancelled waits cannot masquerade as successful acknowledgements |
| Shared ownership | Cancelling one waiter does not cancel another consumer's work; invalidated attempts cannot overlap a replacement for the same work ID before draining |
| Closing and faults | Awaited close joins actual owned work; synchronous evaluator and callback failures remain observable; internal protocol faults cannot be mistaken for cleanup success |
| Regression | Prior dependency, retention, exact read occurrence, 200-edit reference-model and host cases remain present and pass |

The executed suite covers these distinctions within its concrete fixtures, with one fault-path gap: ordinary evaluator, cleanup and cancellation-callback errors were tested, but internal host/core protocol disagreement was only source-inspected. Injecting such an implementation fault and checking shutdown behavior remains an adoption audit task. A finite suite does not prove all interleavings, scheduling fairness, bounded lifetime memory, resource-bearing continuations, native preemption or distributed delivery.

## Executed checkpoint evidence

The first valid Release run passed 77/77 tests with no skips: 49 core cases (36 retained plus 13 suspension cases), 10 retained host cases and 18 mailbox cases. An earlier compile-only attempt did not execute tests and is excluded from semantic results.

The capacity test holds a cancellation callback after its evaluator has returned, waits for the completion transition and an acknowledgement, then checks that the physical slot is still occupied. Deliberately releasing that slot early produced one executed failure, `Expected 1, got 0`, at `MailboxTests.fs:588`. The first mutation invocation used an invalid adapter filter and executed no tests; the corrected filter ran the same mutant binary. After restoring the original source, the complete Release suite again passed 77/77 with zero skips. The Release solution build reported zero warnings/errors, and both samples passed. [Validation](Validation.md) records the exact source/binary receipts and distribution checks.

LAN TWO returned a bounded static review of the current core suspension operations. Local inspection confirmed its current-attempt, complete-handle, one-use and invalidation observations, but rejected its implication that public handles prevent forgery as security capabilities. LAN ONE's mailbox host review was cancelled without output and supplies no review evidence. Both are distinct from the requested Bozzetto assessment and the executed tests. Raw packets and dispositions remain outside Git under `/home/hhh/.codex/work/hosted-incremental-2026-10-01/suspension-review/` and `/home/hhh/.codex/work/incremental-mailbox-2026-10-01/lan-review/`.

## Evidence required before the checkpoint is accepted

Record the committed or exact dirty source vector, actual test inventory, counts/exits, and the hashes of tested assemblies. Preserve build-only failures separately from executed test outcomes. Retain one deterministic mailbox sample showing acknowledged reservation, suspension, exact resume, stale-reply refusal, and an independent step progressing while another attempt waits. State whether each claim comes from source inspection, a model trace or real task execution. Raw logs and model packets stay outside this repository's narrative docs.

The final handoff must identify the tested revision and remaining limitations. Composer/Bozzetto integration then needs a separate H1 audit: native launch must remain inside Composer's authority decision, with reservation before mutation, current disk receipts, compiler epoch identity, fresh proof/artifact gates, and no enqueue-as-acceptance shortcut. Library eligibility is never proof admission.
