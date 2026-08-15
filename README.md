# fastdup

fastdup is an integrity-first prototype for a POSIX deduplicating storage
appliance. The repository is deliberately early: the implemented slice is the
versioned Stage-1 RAW container format, its durable XFS publication path, and a
deterministic crash model. A separate in-memory reference pipeline now exercises
FastCDC, Exact Dedup, Zstd/Dictionary, bounded Similarity/Depth-1 Delta, FILL,
and bounded Reorder, but those records do not yet have a durable on-disk format.
A low-level FUSE checkpoint is mountable and exercises volatile live POSIX
semantics. Versioned Manifest/Namespace/Commit-WAL formats now recover through a
type-erased appliance adapter into the same POSIX seam, including lazy verified
DATA/FILL/HOLE reads. That recovered seam is deliberately read-only until the
ten-second checkpoint scheduler and durable Inode reservation publisher are
connected, so this is not yet a usable backup target.

Start with [CONTEXT.md](CONTEXT.md), the accepted decisions in
[`docs/adr/`](docs/adr/), and the byte-exact
[`container-v1` specification](docs/specs/container-v1.md). The current measured
baseline is recorded in
[`docs/benchmarks/stage0-raw-container.md`](docs/benchmarks/stage0-raw-container.md).
The sustained ten-ISO ingest and byte-exact restore is recorded in
[`docs/benchmarks/stage1-iso-raw-ingest.md`](docs/benchmarks/stage1-iso-raw-ingest.md),
and the future filesystem is gated by the explicit
[`POSIX conformance plan`](docs/testing/posix-conformance.md).
Its durable namespace and recovery rules are specified in
[`metadata-generation-v1`](docs/specs/metadata-generation-v1.md).
The current reduction policy, real 10-ISO results, worker scaling, integrity
gates, and explicit limitations are recorded in
[`data-reduction-reference-v1`](docs/benchmarks/data-reduction-reference-v1.md).

## Workspace

- `fastdup-format`: explicit bytes to validated durable objects; no implicit
  Rust layout.
- `fastdup-appliance`: the narrow orchestration seam from verified generations
  and Manifest-backed content into the POSIX namespace.
- `fastdup-posix`: byte-exact volatile namespace semantics, deterministic model
  tests, committed-base/dirty-epoch layering, a thin low-level FUSE adapter, and
  a real-mount smoke harness.
- `fastdup-store`: ordered BUILDING-to-PUBLISHED container lifecycle behind an
  injectable storage boundary, plus the non-durable reduction reference engine.
- `fastdup-testkit`: separate live/durable state, crash simulation, and
  deterministic I/O failures.

All generated artifacts stay under `/source/fastdup/.artifacts/`. With the
workspace-local toolchain installed, run:

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p fastdup-posix --example fuse_mount_smoke -- \
  /source/fastdup/.artifacts/tier-meta/fuse-mount
cargo run --release -p fastdup-store --example raw_store_bench -- \
  /source/fastdup/.artifacts/bench 32
cargo run --release -p fastdup-testkit --example generate_structured_corpus -- \
  /source/fastdup/.artifacts/corpus/structured-v1
cargo run --release -p fastdup-testkit --example ingest_iso_variants
cargo run --release -p fastdup-testkit --example audit_container_store
cargo run --release -p fastdup-store --example reduction_matrix -- \
  --preset all --workers 8 --inflight-mib 128 \
  /source/fastdup/.artifacts/corpus/structured-v1/*
```

The next correctness work is the single-writer checkpoint scheduler: reserve a
fresh Inode range, cut immutable per-Inode dirty epochs, publish data and
metadata outside Inode locks, commit the Namespace Root, then retire exactly the
installed prefix while preserving later writes. WAL segmentation and an indexed
verified Chunk-location path remain production blockers; the current 64-MiB WAL
and full-container DATA verification are intentionally bounded checkpoint
implementations. The reference reduction implementation remains benchmark
evidence and a format-design oracle, not permission to bypass POSIX conformance
gates.
