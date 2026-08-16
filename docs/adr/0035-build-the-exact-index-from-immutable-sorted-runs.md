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
level-zero run cap, compaction ratio, Bloom/Binary-Fuse placement, and whether a
single sorted run or a partitioned run family gives the best NVMe read and write
amplification. These values are policy, not durable-format constants.

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
Run Set. The current merge deliberately caps its transient input at 262,144
entries. Canonical external rebuild, streaming/partitioned compaction above that
bound, and the Rocky/structured-corpus performance gates remain open, so this
ADR remains proposed.

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
[Exact Index Activation WAL v1](../specs/exact-index-activation-v1.md); their
implementation does not give any index object content or liveness authority.
