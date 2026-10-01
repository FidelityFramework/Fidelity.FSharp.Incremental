# Architectural boundaries

- Keep the core deterministic: explicit immutable commands, state and effects.
  No tasks, clocks, ambient transactions, callbacks, I/O or generic payload equality.
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
