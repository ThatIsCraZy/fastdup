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
