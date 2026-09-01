# Repository instructions

## Context

- For domain language, read `CONTEXT.md`.
- For storage semantics, durable formats, recovery, GC, or pipeline policy, read
  the relevant accepted record in `docs/adr/` before changing code.

## Workspace-local execution

- Put every build, test, benchmark, corpus, profile, and temporary artifact under
  `/source/fastdup/.artifacts/`.
- Set `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
  `TMPDIR=/source/fastdup/.artifacts/tmp` for every Cargo build or test. Create
  the workspace-local directories before invoking tools that require them.
- Keep generated and downloaded corpora out of source control.

## Durable code

- Serialize on-disk structures field by field; Rust memory layout is not a file
  format.
- Pair each new durable invariant at writer, reader/recovery, and offline-scrub
  boundaries, including a fault-injection case where applicable.

## Unsafe code

- `unsafe` is permitted when an A/B benchmark demonstrates a measurable
  advantage over the safe implementation. Keep it behind the smallest
  auditable interface, document every safety invariant, and retain the
  benchmark evidence in the repository.
