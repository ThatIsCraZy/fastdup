# fastdup

fastdup is an integrity-first prototype for a POSIX deduplicating storage
appliance. The repository is deliberately early: the implemented slice is the
versioned RAW/Zstd container format, durable metadata generations, immutable
Exact-Index runs, their XFS publication paths, and a deterministic crash model.
A separate in-memory reference pipeline exercises
FastCDC, Exact Dedup, Zstd/Dictionary, bounded Similarity/Depth-1 Delta, FILL,
and bounded Reorder. Dependency-free Zstd Compression Regions now have a durable
on-disk format; Dictionary, Similarity, Delta, and Reorder records do not yet.
A low-level FUSE checkpoint is mountable with writable live POSIX semantics,
five-second durable checkpoints, a ten-second mutation-admission gate, and an
additional 512-MiB active-Dirty-DATA pressure checkpoint.
Versioned Manifest/Namespace/Commit-WAL formats recover through a type-erased
adapter into the same seam, including lazy verified DATA/FILL/HOLE reads. A
valid activated Exact-Index Run Set is pinned at mount and bounds normal DATA
reads. New checkpoints stream FastCDC-v1 boundaries, publish verified RAW/Zstd
Locations as level-zero Runs, and reuse Exact Hits across later checkpoints;
four same-level Runs are merged RoW before the 64-Run reader bound is reached.
Missing or corrupt index state falls back to verified Container scans. Adaptive
Compression Regions are encoded by a bounded cache-local worker pool and merged
in deterministic input order.
This remains an experimental checkpoint rather than a production backup target.

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
The first sustained kernel-FUSE write/checkpoint/read/delete run, including
per-stage CPU, memory, Exact/Compression efficiency, and device-I/O evidence,
is recorded in
[`io-intensive-fuse-600s`](docs/benchmarks/io-intensive-fuse-600s.md).

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

Verified Locations now provide commit-time and recovery DATA proof without a
Container-directory scan while the active index is healthy. Namespace commits
rotate through two bounded, overlapping Commit-Log slots, and large recipes are
published as content-addressed Manifest trees whose unchanged leaves are reused.
The next scalability work is tree-native lazy reads/path updates, metadata GC,
a durable Container-generation high-water, and per-DATA-region Chunking Profile
identities. The current compactor is explicitly bounded to 262,144 input
entries; streaming partitioned compaction remains necessary above that scale.
External index rebuild/scrub, a format-epoch fence, and process-kill deadline
evidence remain production blockers.
The reference reduction implementation remains benchmark evidence and a
format-design oracle, not permission to bypass POSIX conformance gates.
