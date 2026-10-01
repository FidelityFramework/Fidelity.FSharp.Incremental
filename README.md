# Fidelity.FSharp.Incremental

An explicit dependency and execution-lifecycle protocol for hosted incremental work in F#. The library separates immutable bookkeeping from .NET task execution: the core processes commands and emits effects; the host owns running work, cancellation and cleanup.

This is an initial, **pre-integration** implementation. Composer and Bozzetto have not adopted it. Debug and Release suites each passed 46/46 tests; the selective-reuse sample and local preview packaging also passed. See [Validation](docs/Validation.md) for the exact evidence and limits. Library eligibility does not replace existing generation, compiler, proof or artifact gates.

The first scope is deliberately small:

- Complete, owner-declared dependency reads and deterministic scheduling of acyclic work.
- Shared demand with independent consumer cancellation.
- Reservation before edits, transitive withdrawal, and explicit retention of unchanged results.
- Attempt completion followed by resource draining before a result becomes eligible.
- Scope and epoch retirement that cannot revive superseded work.

It has its own API and implementation, with no FSharp.Data.Adaptive or IcedTasks runtime dependency or wrapper. It uses FSharp.Core and the .NET host. Adaptive collections, automatic dependency discovery, compiler proof caching, and live native-state replacement are outside the initial scope.

Read [Architecture](docs/Architecture.md) for identities, ownership and the command protocol. [Audit](docs/Audit.md) records the source pins, executed experiments, and limits that informed the design. An eligible library result means its declared bookkeeping conditions hold; its owner must still validate what that result means.

Build and test with .NET 10:

```sh
dotnet build Fidelity.FSharp.Incremental.slnx
dotnet test tests/Fidelity.FSharp.Incremental.Tests/Fidelity.FSharp.Incremental.Tests.fsproj
dotnet run -c Release --project samples/SelectiveReuse/SelectiveReuse.fsproj
```

The project is MIT licensed. FSharp.Data.Adaptive and Jimmy Byrd's IcedTasks are acknowledged inspirations; the audit distinguishes useful mechanisms, measured counterexamples and untested risks. The primary repository is [Fidelity.FSharp.Incremental on Forgejo](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental); the owner will arrange a GitHub mirror. The package version is `0.1.0-preview.1`; packages are currently built locally, not published to a package feed. Consumers should pin a Git commit until distribution is established.
