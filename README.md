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
in deterministic input order. Exact-Index activation now rotates through two
overlapping 64-record slots and migrates the former 16,384-record WAL without a
lifetime write stop.
The POSIX/FUSE seam also supports metadata-only range clones across arbitrary
FastCDC boundaries through Manifest v2 Chunk slices, plus atomic replacement
rename. Clone checkpoints allocate no frontend payload, rechunk no bytes, and
perform no DATA-container I/O. This is the filesystem primitive needed by
Veeam synthetic full. The experimental GPL `vfs_fastdup` Samba adapter now
advertises block refcounting only when explicitly enabled, maps Duplicate
Extents to FUSE `copy_file_range`, and exposes one fixed Integrity Information
state. It compiles against Samba 4.23.5, but Veeam Fast Clone is not advertised
as supported until a real SMB/Veeam trace and protocol matrix are green.
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
The persistent CPU-pool scaling result is recorded in
[`reduction-worker-pool`](docs/benchmarks/reduction-worker-pool.md).
The first sustained kernel-FUSE write/checkpoint/read/delete run, including
per-stage CPU, memory, Exact/Compression efficiency, and device-I/O evidence,
is recorded in
[`io-intensive-fuse-600s`](docs/benchmarks/io-intensive-fuse-600s.md).
The range-clone design, crash matrix, and real FUSE evidence are recorded in
[`veeam-fast-clone`](docs/testing/veeam-fast-clone.md).
Offline integrity verification and RoW Exact-Index reconstruction are described
in [`scrub and Exact-Index rebuild`](docs/operations/scrub-and-exact-index-rebuild.md).

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
- `samba/vfs_fastdup`: GPL Samba VFS adapter and a dependency-free executable
  contract test for Duplicate Extents, Integrity Information, and CLOSE
  ordering.

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
cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline scrub METADATA_ROOT CONTAINER_ROOT
```

Verified Locations now provide commit-time and recovery DATA proof without a
Container-directory scan while the active index is healthy. Namespace commits
rotate through two bounded, overlapping Commit-Log slots, and large recipes are
published as content-addressed Manifest trees. Lazy reads, equal-length updates,
append, truncate, and arbitrary length-changing middle splice/concat reuse
unchanged subtrees without flattening the file recipe. Exact-Run compaction and
offline rebuild stream inputs with one verified 4-KiB page per source family
instead of retaining a complete entry set. Offline scrub verifies every retained
generation, published Container, active index object, and ACTIVE Location; the
RoW rebuild activates only a fully audited replacement Run Set.

Offline scrub-bound GC removes fully unreachable Containers and compacts
profitable sets of partially live Containers only after verified replacements
and a filtered RoW Exact Index are active and the current/previous generation
proof is revalidated. Online GC still requires RETIRING transitions and pin
drain. The next recovery work is Metadata GC, a durable
Container-generation high-water, per-DATA-region Chunking Profile identities,
and an Appliance Lease/format-epoch fence. A bounded real-process
[`SIGKILL`/remount/deadline harness](docs/testing/sigkill-remount-deadline.md)
is green; broad load, randomized kill, and block-device power-cut evidence
remain open. Partitioned Run
families remain necessary only when one canonical output exceeds the Run-v1
one-GiB object bound. Large-store rebuild/scrub performance remains a production
gate even though the bounded integrity path and deterministic fault matrix are
implemented.
The reference reduction implementation remains benchmark evidence and a
format-design oracle, not permission to bypass POSIX conformance gates.
