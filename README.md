# Fidelity.FSharp.Incremental

An explicit dependency and execution-lifecycle protocol for hosted incremental work in F#. The core processes immutable commands and emits effects; a functional F# async host owns running work, cancellation and cleanup. CLR interoperability stays at named boundaries.

This is an initial, **pre-integration** implementation. Composer and Bozzetto have not adopted it. The Release suite passes 99/99 tests, including all 84 preceding cases and 15 functional API cases. See [Validation](docs/Validation.md) for evidence and limits, and the [functional async checkpoint](docs/Functional_Async_Checkpoint_2026-10-01.md) for the current audit request. Library eligibility does not replace generation, compiler, proof or artifact gates.

The first scope is deliberately small:

- Complete, owner-declared dependency reads and deterministic scheduling of acyclic work.
- Shared demand with independent consumer cancellation.
- Reservation before edits, transitive withdrawal, and explicit retention of unchanged results.
- Attempt completion followed by resource draining before a result becomes eligible.
- Scope and epoch retirement that cannot revive superseded work.

It has its own API and implementation, with no FSharp.Data.Adaptive or IcedTasks runtime dependency or wrapper. It uses FSharp.Core and the .NET host. Adaptive collections, automatic dependency discovery, compiler proof caching, and live native-state replacement are outside the initial scope.

Read [Architecture](docs/Architecture.md) for identities, ownership and the command protocol. [Audit](docs/Audit.md) records the source pins, executed experiments, and limits that informed the design. An eligible library result means its declared bookkeeping conditions hold; its owner must still validate what that result means.

The [steps and mailbox extension](docs/Steps_and_Mailboxes.md) adds explicit one-shot suspension handles and acknowledged command coordination. A deliberate premature-capacity-release mutation failed its targeted test; the restored implementation then passed the full suite. The [mailbox auditor checkpoint](docs/Mailbox_Auditor_Checkpoint.md) requests an independent assessment before consumer adoption.

Use `AsyncMailbox.create` and `start` for explicit coordinator lifetime, `admit`
to retain an exact command operation, and `observe` to await its receipt. Observers
may cancel and later re-observe the same operation. Owned evaluators receive a
typed `WorkCancellation` request and return `Async<StepOutcome>`; cancellation
does not excuse joining children or cleanup. `beginClose` seals admission now;
the cold `close` workflow seals it when executed. The `AsyncDocuments` sample
demonstrates typed payloads and unchanged-result reuse. `MailboxHost` and the older
`Host` remain CLR compatibility APIs.

Build and test with .NET 10:

```sh
dotnet build Fidelity.FSharp.Incremental.slnx
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj
dotnet run -c Release --project samples/SelectiveReuse/SelectiveReuse.fsproj
dotnet run -c Release --project samples/MailboxSteps/MailboxSteps.fsproj
dotnet run -c Release --project samples/AsyncDocuments/AsyncDocuments.fsproj
```

The project is MIT licensed. FSharp.Data.Adaptive and Jimmy Byrd's IcedTasks are acknowledged inspirations; the audit distinguishes useful mechanisms, measured counterexamples and untested risks. The primary repository is [Fidelity.FSharp.Incremental on Forgejo](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental); the owner will arrange a GitHub mirror. The source version is `0.1.0-preview.4`; local distribution receipts are recorded in the checkpoint documents. Packages are not published to a feed. Consumers should pin a Git commit until distribution is established.
