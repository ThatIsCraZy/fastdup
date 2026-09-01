---
status: proposed
---

# Build the Exact Index from immutable sorted runs

Represent the persistent Exact Index as immutable, page-checksummed sorted runs
selected by a separate append-only, hash-chained activation log. A run contains
versioned Location transitions ordered by `(Chunk ID, logical length, physical
Location)`. New container publication creates small level-zero runs; bounded
background compaction creates replacement runs and publishes a new active run
set without overwriting old evidence.

The active run set is acceleration, never liveness authority. A positive lookup
returns an untrusted Location candidate that must still be paired with the
requested Chunk ID and length and verified against the immutable Container. A
negative lookup may cause a duplicate Location or a bounded rebuild/slow-path
search; it may not establish that durable DATA is missing. Corrupt or missing
index runs refuse that index generation but do not roll back the Namespace
Commit WAL.

## Why this remains proposed

The immutable-run direction follows the accepted RoW and rebuild decisions, but
the production fanout is workload-sensitive. Benchmarks on the pinned Rocky ISO
variants and structured corpus must select page-cache policy, level count,
level-zero family cap, compaction ratio, and Bloom/Binary-Fuse placement. The
need for partitioned Run families is accepted by
[ADR 0045](0045-partition-exact-index-compaction-into-run-families.md); its
262,144-entry target remains pinned policy rather than a Run-format constant.

The proposal is accepted only after the prototype demonstrates all of:

- no complete Chunk-to-Location map retained in RAM;
- bounded lookup work for a configured active run-set generation;
- one index failure never makes a valid committed Manifest unreadable;
- rebuild from Container Recovery Indexes produces byte-identical canonical
  runs independent of discovery or worker order;
- compaction fail-before/fail-after recovery selects either the old or complete
  new run set; and
- measured ingest, restore, and index write-amplification results beat the
  current verified container-scan baseline.

The current prototype has demonstrated bounded active-Run lookup without a
complete in-memory map, old-or-complete-new activation under deterministic
fail-before/fail-after injection, verified Container-scan fallback after an
active Run page is corrupted, a Run-Set pin behind ordinary POSIX/FUSE Manifest
reads, and automatic level-zero publication of fully verified RAW/Zstd
Locations. A healthy next checkpoint reuses these Locations only after bounded
Container verification; index publication failure is explicitly
nonauthoritative and does not block Namespace durability. The same pinned
Location path now proves complete commit and recovery DATA graphs without a
Container-directory scan while healthy; any unusable candidate falls back to
one complete verified scan.

Four oldest same-level Runs are now compacted RoW into a deterministic
higher-level Run. Every input page and complete Run hash is audited before the
merge, repeated physical Locations retain the transition from the newest source
generation, and the output is writer-reread before activation. Seventy separate
checkpoint generations remain below the 64-active-Run bound and retain a first-
generation Exact Hit across remount. Fail-before/fail-after injection across the
complete compaction publication exposes only absence or a complete canonical
Run; the activation matrix independently selects only the old or complete new
Run Set. Activation uses two overlapping 64-record slots from repository
creation; writer, recovery, offline audit, and fault injection enforce the same
bridge identity. The former 262,144-entry transient cap is now an output
partition target rather than a failure limit. Compaction performs two complete
verified K-way merge passes, retains one 4-KiB page per source family plus one
output page, and streams a complete key-disjoint Run family. Lookup selects at
most one partition per family. The offline external rebuild now scans one
verified Container at a time, publishes hidden level-zero/compacted families,
performs a bounded global cross-family invariant pass, and atomically activates
the replacement. Deterministic fail-before/fail-after injection proves only the
old/absent or complete new index is selected; orphan retry uses a monotonic
name high-water. The exact procedure and limitations are recorded in
[scrub and Exact-Index rebuild](../operations/scrub-and-exact-index-rebuild.md).

Each active physical Run may now carry a rebuildable RAM membership hint. It
reuses the existing `BlockedBloomHint` from the reduction pipeline: the key is
exactly `(Chunk ID, logical length)`, one probe touches one aligned 64-byte
block, and seven distinct bits target ten bits per key. The hint is constructed
during the already mandatory complete Run audit, so activation adds no second
Run read. `DefinitelyAbsent` skips that Run's page lookup; every positive is
still untrusted and follows the unchanged page-checksummed lookup and Container
verification path. Allocation failure or disabled admission simply leaves the
Run unfiltered. Writer insertion is paired with an immediate no-false-negative
assertion and offline scrub probes every authenticated Run entry again.

All filters in one newly activated Run Set share one budget: one 32nd of the
effective memory limit, clamped to 1 MiB through 8 GiB and further bounded by
live available memory above the shared cache/I/O reserve. The ordinary
repository resamples current host/cgroup headroom at every Run-Set activation;
any observed Swap sets that new active set's budget to zero. Filters are
immutable with their Runs, contain no Locations, and are rebuilt after restart;
they are
neither serialized nor content authority. Telemetry separates current filter
count/bytes from process-lifetime absent/maybe probe counters.

Rocky/structured-corpus throughput, discovery/worker-order canonicality at
scale, and index write-amplification gates remain open, so this ADR remains
proposed.

## Consequences

Index runs, run-set manifests, and activation records are independently
versioned. Compaction and rebuild are RoW. Old runs remain until no reader pins
their run-set generation. Location transitions carry complete physical
identity, including Container ID and record coordinates; they never alter the
logical Chunk ID. A Chunk ID observed with two logical lengths is Corruption at
writer, lookup, rebuild, and scrub paths.

The page geometry and entry encoding are fixed by
[Exact Index Run v1](../specs/exact-index-run-v1.md). Run Set and activation-log
layouts are assigned separately by
[Exact Index Run Set v1](../specs/exact-index-run-set-v1.md) and
[Exact Index Activation Log v1](../specs/exact-index-activation-v1.md); their
implementation does not give any index object content or liveness authority.
