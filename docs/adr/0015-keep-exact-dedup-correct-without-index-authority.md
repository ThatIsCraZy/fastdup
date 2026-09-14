---
status: accepted
---

# Keep exact dedup correct without index authority

The full NVMe Exact Index maps BLAKE3-256 Chunk IDs plus logical lengths to
Location Sets, but remains rebuildable from verified containers. A matching hash
and length is sufficient for the trusted-client ingest hot path; scrub always
rehashes decoded bytes and AUDIT samples may compare during ingest. Bloom filters
and locality caches only avoid lookups and may never establish an Exact Hit.

## Consequences

A length mismatch for one Chunk ID is Corruption. Concurrent workers may store
the same new chunk twice and merge both durable Locations rather than serialize
on a global hash lock. False negatives waste space but do not lose data; Bloom
positives require exact lookup. Writers pin the observed Location-Set generation
through commit, and GC cannot delete a retiring container until no reader or
writer pins it and a newer durable generation covers every live chunk.

## Online reference selection (2026-09-13)

Exact-only ingest implements this contract without reading old DATA. It hashes
the incoming bytes, looks up the complete Chunk ID and length in the activated,
validated Exact Run Set, and merges newest transitions by physical Location.
An ACTIVE result supplies the reference for the new Manifest. Neither its
Container envelope nor its encoding Record is read, decoded or rehashed.
Process-owned, durably published Locations awaiting asynchronous Exact activation
remain eligible overlays unless a newer transition rejects them. Historical
cache entries are preferences that must pass current index selection again.

Writer protection is coalesced into at most one DATA-reference admission per
Active and Frozen Commit epoch, using the existing dependent-publication/GC
barrier. GC rechecks that admission atomically before RETIRING activation and
postpones retirement while a writer introduces references. A selection retains
its admission through transfer into the epoch, including a concurrent freeze.
Failed commits retain Frozen admission for retry; a cancelled freeze returns
it to Active; successful Commit releases it. Cache pressure and the 65,536
per-Chunk proof bound cannot drop it. During an existing retirement, a new
transaction conservatively encodes independently instead of selecting victims.
An idle checkpoint demotes late proofs from an already completed cut, releasing
their admission instead of retaining an unnecessary permanent GC barrier.

When per-Chunk evidence cannot be retained, online successor checking resolves
the dependency through the activated Exact index after the publication fence.
A current index pin is acquired inside the Namespace Commit lock, which also
serializes GC retirement activation; an earlier planning snapshot is insufficient.
A valid ACTIVE match still causes no DATA read. Unavailable or invalid mappings
retain the existing independent fallback. Full recovery and scrub do not use
this commit-only reference checker.

An Exact reference is not physically verified Location evidence and never
populates the payload or physical-proof cache. Demand reads, DATA recovery,
index rebuild and scrub still check stored CRCs, decoded hashes and dependencies.
Latent damage may be discovered at those independent boundaries, as with
writer-carried publication evidence in ADR 0059. Formats and DATA/Metadata/WAL
sync ordering do not change.
