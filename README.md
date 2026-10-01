# Fidelity.FSharp.Incremental

An explicit dependency and execution-lifecycle protocol for hosted incremental work in F#. The library separates immutable bookkeeping from .NET task execution: the core processes commands and emits effects; the host owns running work, cancellation and cleanup.

This is an initial, **pre-integration** implementation. Composer and Bozzetto have not adopted it. The current Release suite passed 77/77 tests, including all 46 tests from the preceding checkpoint and 31 new suspension/mailbox cases. Both samples passed, and the Release solution build reported no warnings or errors. See [Validation](docs/Validation.md) for the exact evidence and limits. Library eligibility does not replace existing generation, compiler, proof or artifact gates.

The first scope is deliberately small:

- Complete, owner-declared dependency reads and deterministic scheduling of acyclic work.
- Shared demand with independent consumer cancellation.
- Reservation before edits, transitive withdrawal, and explicit retention of unchanged results.
- Attempt completion followed by resource draining before a result becomes eligible.
- Scope and epoch retirement that cannot revive superseded work.

It has its own API and implementation, with no FSharp.Data.Adaptive or IcedTasks runtime dependency or wrapper. It uses FSharp.Core and the .NET host. Adaptive collections, automatic dependency discovery, compiler proof caching, and live native-state replacement are outside the initial scope.

Read [Architecture](docs/Architecture.md) for identities, ownership and the command protocol. [Audit](docs/Audit.md) records the source pins, executed experiments, and limits that informed the design. An eligible library result means its declared bookkeeping conditions hold; its owner must still validate what that result means.

The [steps and mailbox extension](docs/Steps_and_Mailboxes.md) adds explicit one-shot suspension handles and acknowledged command coordination. A deliberate premature-capacity-release mutation failed its targeted test; the restored implementation then passed the full suite. The [mailbox auditor checkpoint](docs/Mailbox_Auditor_Checkpoint.md) requests an independent assessment before consumer adoption.

Build and test with .NET 10:

```sh
dotnet build Fidelity.FSharp.Incremental.slnx
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj
dotnet run -c Release --project samples/SelectiveReuse/SelectiveReuse.fsproj
dotnet run -c Release --project samples/MailboxSteps/MailboxSteps.fsproj
```

The project is MIT licensed. FSharp.Data.Adaptive and Jimmy Byrd's IcedTasks are acknowledged inspirations; the audit distinguishes useful mechanisms, measured counterexamples and untested risks. The primary repository is [Fidelity.FSharp.Incremental on Forgejo](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental); the owner will arrange a GitHub mirror. The package version is `0.1.0-preview.2`; packages are currently built locally, not published to a package feed. Consumers should pin a Git commit until distribution is established.
