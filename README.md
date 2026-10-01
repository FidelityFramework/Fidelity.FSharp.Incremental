# Fidelity.FSharp.Incremental

An explicit dependency and execution-lifecycle protocol for hosted incremental work in F#. The core processes immutable commands and emits effects; a functional F# async host owns running work, cancellation and cleanup. CLR interoperability stays at named boundaries.

The library provides a shared foundation for incremental services, including
Clef/CCS, Composer and Bozzetto. It tracks dependencies and work lifetime while
each consumer retains authority over its results: a compiler still decides
whether its proofs and executable artifacts are valid.

It supports:

- Complete, owner-declared dependency reads and deterministic scheduling of acyclic work.
- Shared demand with independent consumer cancellation.
- Reservation before edits, transitive withdrawal, and explicit retention of unchanged results.
- Attempt completion followed by resource draining before a result becomes eligible.
- Scope and epoch retirement that cannot revive superseded work.

It has its own API and implementation, with no FSharp.Data.Adaptive or IcedTasks runtime dependency or wrapper. It uses FSharp.Core and the .NET host. Adaptive collections, automatic dependency discovery, compiler proof caching, and live native-state replacement are outside the initial scope.

Cold construction and explicit demand follow Clef's lazy-default direction: creating a host does not evaluate its work. The deterministic core and explicit ownership contracts keep the boundary for a future self-hosting port visible. The current execution host is .NET; native Clef realization remains future work.

Read [Architecture](docs/Architecture.md) for identities, ownership and the command
protocol, and [Steps and mailboxes](docs/Steps_and_Mailboxes.md) for suspension,
resumption and acknowledged coordination.

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

Development evidence, package provenance and consumer progress belong in the
[integration checkpoint](docs/Consumer_Integration_Checkpoint_2026-10-01.md) and
[validation documentation](docs/Validation.md).

The project is MIT licensed. FSharp.Data.Adaptive and Jimmy Byrd's IcedTasks are
acknowledged inspirations. The primary repository is
[Fidelity.FSharp.Incremental on Forgejo](https://forge.spkez.dev/FidelityFramework/Fidelity.FSharp.Incremental).
