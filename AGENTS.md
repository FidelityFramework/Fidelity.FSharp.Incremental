# Architectural boundaries

Use the primary checkout. Keep local gate logs and scratch fixtures in `.scratch/`.

- Keep the core deterministic: explicit immutable commands, state and effects.
  No tasks, clocks, ambient transactions, callbacks, I/O or generic payload equality.
- The portable core must not reference System namespaces or BCL APIs. Use F#
  language primitives and dependency-free Fidelity foundations where needed.
  Keep platform interop in explicit replaceable Hosting adapters; their temporary
  CLR implementation must not determine the core protocol or its data types.
- Keep .NET execution and resource ownership in the Hosting project. Evaluator
  completion must include its owned children and cleanup; cancellation is a request.
- Preserve complete dependency occurrences, reservation before mutation, shared
  demand, freshness and drain gates. Never treat value equality as proof of validity.
- Baker owns source semantics and proof premises; PSG is its publication contract.
  This library schedules work. It cannot authorize a proof receipt or native artifact.
- Do not add FDA or IcedTasks wrappers/dependencies. Preserve attribution for any
  future adapted source and keep the self-hosting port boundary explicit.
- Use .NET tooling, not Python. Validate changes with the repository tests and
  distinguish library evidence from compiler integration and language acceptance.
