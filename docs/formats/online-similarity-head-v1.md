# Online Similarity Head v1

Implementation: `fastdup-store/src/online_similarity.rs`; policy: ADR 0089.
All integers are unsigned little-endian, serialized field by field. A head
is exactly 4096 bytes; no Rust layout is part of the format.

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 8 | ASCII `FDRED001` |
| 8 | 8 | Nonzero activation sequence |
| 16 | 32 | Nonzero Exact Run Set identity observed at publication |
| 48 | 8 | Family count, 0 through 24 |
| 56 | 8 | Zero |
| 64 | count × 64 | Chronological family references |
| after references | through offset 4063 | Zero |
| 4064 | 32 | BLAKE3 of bytes 0 through 4063 |

Each family reference has this layout:

| Relative offset | Bytes | Value |
| --- | --- | --- |
| 0 | 8 | Nonzero physical family generation |
| 8 | 8 | First incorporated publication sequence |
| 16 | 8 | Last incorporated publication sequence |
| 24 | 1 | Compaction level, 0 through 15 |
| 25 | 7 | Zero |
| 32 | 32 | Nonzero BLAKE3 of the complete existing family manifest |

Physical family generations must be unique within a head. Each interval has
`first <= last <= head.sequence`; intervals are strictly increasing and
nonoverlapping. An imported offline snapshot uses interval `[0,0]`, level 15.
New online families use `[sequence,sequence]`, level 0. A fan-in-four merge
preserves the first/last chronology of its inputs and increments their level.
Compaction physical generations are not Bucket-State freshness timestamps.

The writer validates these invariants before writing. Recovery and scrub
decode, validate, and compare a canonical re-encoding to the original bytes,
including every reserved byte. They verify each selected manifest hash and
audit the referenced physical Similarity partitions using their existing
profile, page, descriptor, ordering and bucket invariants. Family manifests
and partitions reuse the existing v1 profiles and partition format.

## Activation, recovery and lifetime

The selected filename is `reduction-head.{sequence % 2}.fds`. Publish the
immutable partitions and family manifests durably first, then write the head,
read back and decode it, sync the head and directory, and finally swap the
process-local Arc. Recovery selects the highest complete checksummed head.
A torn/invalid slot can be ignored when the other slot is valid; contradictory
valid heads with the same sequence are corruption. If neither existing slot
is valid, or a selected dependency is missing/corrupt, the optional index
fails closed and DATA/Exact remain independently recoverable.

The two slots are independently checksummed, not a hash chain. There is no
claim of recovery from malicious replay or simultaneous destruction of both
slots. A head's Exact ID is provenance, not a requirement to retain historical
Exact files. Queries hold an immutable Similarity view, then a current Exact
pin, and resolve/verify every chosen independent base through normal reads.
Compaction holds no long-lived Exact pin and never reads DATA.

Dependent-write planning additionally holds a process-local Container
publication admission through target Exact activation. GC cannot authorize a
retirement during this interval; new planning during an existing retirement
uses independent encoding. A failed Exact publication retains one admission
until writable-owner teardown and disables new advanced planning. This guard
is not a head field, not a candidate source, and never follows asynchronous
Similarity compaction. The normal recovered-GC finalizer still runs before
new writable admission after restart.

Retirement keeps all families referenced by either activation slot or an
in-flight reader. Filesystem immutable leases also prevent deletion while an
external mapped reader is alive. Retirement resumes for the previous slot's
families after reopening. Interrupted unselected publications may leave
orphan index artifacts; they are not guessed away during normal recovery.
Only one online publisher may own a repository storage root at a time.

## Work bounds and failure semantics

The queue contains at most two batches, each capped at 4096 independent
entries; one worker processes another batch. Queries never inspect this RAM
queue. They probe at most 24 families for each of four keys, inspect at most
256 representatives in total, rank at most 16, and perform at most four base
reads/trial encodings. A complete replacement value contains at most the
smallest 64 full Chunk IDs. At publication, at most 16384 changed-key groups
are assembled and only one old BucketState64 is loaded at a time. Compaction
streams four inputs; an output partition buffers at most 32768 references.
This bounds builder memory, not the total I/O duration of a higher-level merge.

Similarity admission is best effort. Queue saturation, admission limits,
publication faults and a lost crash tail do not change DATA durability or
Exact semantics. They can reduce the future compression benefit. Telemetry
reports batches, compactions, skipped entries, errors and active families.
Stale retained representatives are currently misses, not automatically
backfilled. There is no independent hint journal or queryable online overlay.

Tests cover every injected before/after I/O failure across L0 publication and
fan-in-four compaction, torn head fallback, selected partition corruption,
canonical reserved fields, chronology/identity rejection, filesystem leases,
restart usefulness and a blocked publisher with a saturated queue.
