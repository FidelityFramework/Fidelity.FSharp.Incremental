# Consumer observation checkpoint — 2026-10-01

The first Clef/Composer/Bozzetto integration needs to await an individual work
item without waiting for every other item in its host. Preview.5 adds
`AsyncMailbox.watch : Handle -> MailboxSnapshot * Async<unit>` for that purpose.
The snapshot and its next change signal are captured under the same lock.
A change before the caller starts observing is retained. Observation is cold,
repeatable, cancellable independently and does not consume the owner's events.

A notification means only “inspect state again.” Consumers must check their
specific work/scope and result eligibility; compiler proof and launch authority
remain outside this library. If `IsClosing` is set, callers observe the retained
close operation instead of waiting for another change that might never occur.

Validation: **102/102 tests passed**, including three new controls for a change
before observation, independent cancellation of two observers, and cold-host
close. The existing 99 tests remain present. Evidence is outside the repository
at `/home/hhh/.codex/work/incremental-adoption-2026-10-01/validation/`.
The initial command used the absent `.sln` filename and executed no tests;
the recorded `.slnx` run supplies the result above.

The [independent preview.4 assessment](Functional_Async_Auditor_Assessment_2026-10-01.md)
and its original checkpoint are unchanged. Its acceptance covers preview.4;
this additive observer and the actual consumer integrations need their own
review. This is not compiler/native-host acceptance.

The consumer packages were packed from implementation commit
`87c77d91ab24c8e4d065e4e726f8c06abc3180c4`; Core's nuspec records the full pin,
while Hosting's records its abbreviation `87c77d9`.
Exact preview.5 archive SHA256 values:

- Core: `7f27688e98c2cee965b6ebf5f20073fb9d17b3f9d89e12511daf5e5a4eca3a88`.
- Hosting: `fb63ca9b638dde967958c032cce13e685dd78baf6b1e67766a764b00afa8e5e3`.

Identical copies are retained in the consumers' explicit vendored NuGet feeds.
This records dependency provenance; consumer builds, native behavior and
distribution promotion have separate acceptance receipts.
