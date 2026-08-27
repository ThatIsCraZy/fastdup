---
status: accepted
---

# Discover GC candidates incrementally and prove victims locally

Online GC separates cheap candidate discovery from destructive authority. An
immutable Container summary and a rebuildable `GcCandidateCatalog` may identify
likely zero-live or profitable partially-live Containers without a preceding
complete End-to-End Scrub. Only a bounded, generation-bound
`GcCandidateProof` may authorize `RETIRING`, Location-Set replacement, pin
drain, and unlink.

This supersedes only ADR 0048's requirement that every DATA GC plan originate
from one complete successful End-to-End Scrub. Its pressure thresholds,
replacement-before-deletion ordering, generation revalidation, exact canonical
identity check, and directory sync remain required. Full scrub becomes a
periodic audit and catalog-repair source.

This advances the Container envelope from format version 1 to version 2.

## Trust levels

The Container Header and Footer mirror one 96-byte, 64-byte-aligned intrinsic
summary field by field. It contains encoded and decoded bytes by codec, record
and Chunk geometry, and outgoing dependency shape. Envelope decoding validates
the duplicate and its layout equations; recovery and scrub derive it again from
the authenticated records. It never contains mutable live-byte counts,
reference counts, `RETIRING`, pin state, or a serialized victim score.

The `GcCandidateCatalog` is immutable-run acceleration built incrementally from
Container publication, Metadata-liveness changes, and Location-generation
changes. A stale or missing catalog can cause extra verification or suppress a
cycle, but never data loss. Bloom/Xor filters, samples, and sketches are allowed
only at this hint level and are not part of the first Container summary.
The implemented v1 catalog uses Container-ID-sorted 96-byte rows, paired 4-KiB
envelopes, generation freshness bindings, and a whole-row-stream BLAKE3 digest.
Successor publication merge-joins a bounded update set with the previous
immutable generation. Empty generations are durable tombstones, so recovery
cannot fall back to candidates from an older nonempty pool view.

If no catalog exists, adaptive Online GC bootstraps one by counting canonical
published names and then streaming Container-ID-ordered rows from paired
Header/Footer intrinsic summaries. Bootstrap reads no record payload and keeps
no pool-sized row map. These envelope facts remain hints; local proof fully
verifies every shortlisted victim. A stale liveness base or a completed
relocation may publish a fresh bootstrap generation before incremental deltas
continue.

Metadata liveness advances as a set delta between protected two-generation
windows: the window ending at the catalog's incorporated Commit generation and
the window ending at the current Commit generation. Exact lookups attribute
changed logical targets to likely Containers, but incomplete or negative Exact
results remain hints. Unknown-count underflow clears the estimate instead of
creating a false zero-live row.

A `GcCandidateProof` binds the current and immediately previous Commit Records,
the protected Active/Frozen roots and open-orphan DATA dependencies, the
selected Location and Exact/Similarity generations, the exact victim identities
and Recovery Indexes, and the complete target/Base dependency closure. It
verifies replacement coverage for every reachable victim Chunk. Any changed
binding invalidates the proof before the retirement barrier.

The implementation derives Active/Frozen and open-orphan DATA from the same
process-local Metadata Root Pin registry that protects their immutable Manifest
graphs. Online liveness scans the durable Commit pair plus every pinned Manifest
root. Final revalidation holds the Metadata publication barrier and Commit lock
until the selection barrier is committed and the RETIRING Exact generation is
active, so neither a new unpublished root nor a Namespace commit can enter
between proof validation and retirement authority.

The local proof builds and caches a process-local Reverse Dependency Generation
for the exact protected Commit pair and Exact activation. It looks up every
protected target, requires a complete effective ACTIVE Location prefix, and
records Base-to-dependent edges from authenticated Exact Location fields. An
incomplete lookup or a live target without an ACTIVE Location fails closed. The
bounded victim read then replaces only protected target Chunks and Base Chunks
named by that generation. Catalog fanout estimates and Exact negatives remain
non-authoritative. The projection is discarded after either binding changes
and rebuilt after process start; it introduces no frontend write.

Paired Similarity families authenticate the selected Exact Run Set under ADR
0062, and paired recovery refuses a family bound to any other Run Set. The
proof's exact activation therefore transitively binds the only Similarity
generation eligible for online selection; every paired rebuild activates a new
Exact Run Set before publishing its Similarity family.

A victim set is rejected unless the independent-RAW replacement upper bound
still proves positive physical gain and remains under the bounded replacement
budget. When the authoritative protected proof contains no DATA Chunk at all,
the Reverse Dependency Generation is empty and the proof retires verified
victims without replacements.

## Consequences

Likely victims may be examined and verified replacements may be published
speculatively; these are harmless additional Locations. New Exact reuse and
Similarity Base selection exclude a Container only after durable `RETIRING`.
Physical deletion follows replacement activation and drain of reader, writer,
and reduction-snapshot pins.

Urgent GC prefers proved zero-live Containers and then the least relocation per
net reclaimed byte. Background GC may incorporate Container age and codec or
dependency cost. Merge sets use bounded similar-live-size packing and must beat
a conservative independent-RAW replacement bound. No approximate value is a
deletion invariant.

Normal scans use an audited read-only mapping held under the immutable-file
lease. Adapters without that lease use bounded positional reads. The mapping's
unsafe operation is confined to that ownership boundary; neither path casts
file bytes to Rust structs. Publication batches row writes and shortlist
selection retains at most 4,096 rows in an `O(container_count * log(limit))`
heap.

Online execution publishes ACTIVE replacement Locations and RETIRING victim
Locations in one atomically activated Exact L0 generation. The process closes
new scan-fallback selection before activation, closes new work admission on the
displaced Exact generation at activation, and waits reader, writer, and
reduction-operation pins from every still-live predecessor generation before
unlink. The scan barrier is transactional until Exact activation commits. A
final L0 generation records REMOVED tombstones after DATA directory sync.
Recovery derives the scan-selection barrier from effective RETIRING
transitions; Scrub verifies only effective ACTIVE dependencies while
authenticating all transition bytes.

Long-lived and cached Manifest readers retain an uncounted Exact-generation
snapshot, not a generation pin. Each bounded DATA read attempts one operation
pin; if retirement has closed admission, that read uses verified Container
discovery instead. This keeps Exact acceleration coherent for the operation
without allowing an idle file object or an already closed frontend read to
stall RETIRING drain indefinitely.

Before admitting frontend I/O, the writable appliance runs the Online GC
recovery finalizer. A restarted process has no surviving predecessor-generation
pins, so the active generation's effective RETIRING entries are sufficient
authority. Each present victim must fully verify and reproduce the complete
RETIRING Location set; a victim already absent may represent a directory-synced
unlink interrupted before REMOVED publication. The finalizer syncs DATA before
activating REMOVED and is idempotent across every interruption. A finalization
error prevents writable mount admission rather than weakening the barrier.

This authorizes bounded same-process online execution through the shared
Container and Exact repositories, including restart completion. ADR 0065 adds
automatic candidate scheduling and ADR 0069 requires one cross-process
Appliance Lease before recovery or mutation.
