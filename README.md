# Fidelity.FSharp.Incremental

An explicit dependency and execution-lifecycle protocol for hosted incremental work in F#. The core processes immutable commands and emits effects; a functional F# async host owns running work, cancellation and cleanup. CLR interoperability stays at named boundaries.

**Preview.5 is independently accepted for consumer integration, which is now underway in Clef, Composer and Bozzetto.** The reviewed implementation is [`87c77d9`](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental/commit/87c77d91ab24c8e4d065e4e726f8c06abc3180c4). Its isolated Release build and **102/102 tests** passed, together with a package-only consumer and three additional observation controls. The [independent assessment recorded at `deabc97`](docs/Consumer_Observation_Auditor_Assessment_2026-10-01.md) accepts this library revision; consumer results are recorded separately.

Consumer status on **October 1, 2026**:

| Consumer | Integration and validation |
| --- | --- |
| [Clef / CCS](https://forge.spkez.dev/FidelityFramework/clef) | [`7927b3f` workspace integration](https://forge.spkez.dev/FidelityFramework/clef/src/commit/7927b3f8dcc8528f8a76571ae1dc75c5e66e0cd0/docs/Incremental_Project_Workspace_2026-10-01.md): cold, shared whole-project checking with immutable captured inputs and joined cleanup. **22/22 focused tests passed**, including fifteen new workspace cases. The full compiler suite passed **2,125/2,224**; the same **99 pre-existing compiler failures** remain, with no missing tests or new failures. |
| [Composer](https://forge.spkez.dev/FidelityFramework/Composer) | Project-session and editor source integration is implemented. Build passed; consumer test validation is ongoing. Each new generation still receives fresh source checking and its existing proof/artifact checks. |
| [Bozzetto](https://forge.spkez.dev/FidelityFramework/Bozzetto) | Provider source integration at `827db496` / [`a64010a8`](https://forge.spkez.dev/FidelityFramework/Bozzetto/src/commit/a64010a8/docs/Incremental_Provider_Checkpoint_2026-10-01.md) shares demand and joins producer cleanup. The Release test-project build passed with zero warnings/errors. Default and native provider validation remain pending; the installed daemon still uses the previous closure. |

These source changes do not identify a deployed consumer update. The compiler's existing 99 failures do not reverse the library's independent acceptance. Library eligibility does not replace compiler, proof, artifact or actual-launch authority.

The first scope is deliberately small:

- Complete, owner-declared dependency reads and deterministic scheduling of acyclic work.
- Shared demand with independent consumer cancellation.
- Reservation before edits, transitive withdrawal, and explicit retention of unchanged results.
- Attempt completion followed by resource draining before a result becomes eligible.
- Scope and epoch retirement that cannot revive superseded work.

It has its own API and implementation, with no FSharp.Data.Adaptive or IcedTasks runtime dependency or wrapper. It uses FSharp.Core and the .NET host. Adaptive collections, automatic dependency discovery, compiler proof caching, and live native-state replacement are outside the initial scope.

Cold construction and explicit demand follow Clef's lazy-default direction: creating a host does not evaluate its work. The deterministic core and explicit ownership contracts keep the boundary for a future self-hosting port visible. The current execution host is .NET; native Clef realization remains future work.

Read [Architecture](docs/Architecture.md) for identities, ownership and the command protocol. [Audit](docs/Audit.md) records the source pins, executed experiments, and limits that informed the design. An eligible library result means its declared bookkeeping conditions hold; its owner must still validate what that result means.

The [steps and mailbox extension](docs/Steps_and_Mailboxes.md) provides explicit one-shot suspension handles and acknowledged command coordination. The [functional async assessment](docs/Functional_Async_Auditor_Assessment_2026-10-01.md) records the preceding API and cleanup review; the [consumer observation assessment](docs/Consumer_Observation_Auditor_Assessment_2026-10-01.md) covers preview.5's addition.

Use `AsyncMailbox.create` and `start` for explicit coordinator lifetime, `admit`
to retain an exact command operation, and `observe` to await its receipt. Observers
may cancel and later re-observe the same operation. Owned evaluators receive a
typed `WorkCancellation` request and return `Async<StepOutcome>`; cancellation
does not excuse joining children or cleanup. `beginClose` seals admission now;
the cold `close` workflow seals it when executed. The `AsyncDocuments` sample
demonstrates typed payloads and unchanged-result reuse. `MailboxHost` and the older
`Host` remain CLR compatibility APIs.

`AsyncMailbox.watch` captures published state and its next change notification
together, so an individual result can be observed while unrelated work continues.
After notification, recheck that work's eligibility; a notification is not a
command acknowledgement. Once closing begins, observe the retained close operation.

Build and test with .NET 10:

```sh
dotnet build Fidelity.FSharp.Incremental.slnx
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj
dotnet run -c Release --project samples/SelectiveReuse/SelectiveReuse.fsproj
dotnet run -c Release --project samples/MailboxSteps/MailboxSteps.fsproj
dotnet run -c Release --project samples/AsyncDocuments/AsyncDocuments.fsproj
```

The current version is **`0.1.0-preview.5`**. Consumers restore pinned archives from explicit vendored NuGet feeds; exact package hashes and source provenance are recorded in the [consumer observation checkpoint](docs/Consumer_Observation_Checkpoint_2026-10-01.md). [Validation](docs/Validation.md) retains earlier evidence and its scope.

The project is MIT licensed. FSharp.Data.Adaptive and Jimmy Byrd's IcedTasks are acknowledged inspirations; the audit distinguishes useful mechanisms, measured counterexamples and untested risks. The primary repository is [Fidelity.FSharp.Incremental on Forgejo](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental).
