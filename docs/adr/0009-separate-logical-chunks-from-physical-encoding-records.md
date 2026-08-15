---
status: accepted
---

# Separate logical chunks from physical encoding records

Manifests reference logical Chunk IDs while a physical Encoding Record may hold
a bounded compression region containing multiple complete chunks. Its chunk
table partitions decoded bytes without gaps or overlap, and no logical chunk may
span records. This preserves stable logical identity while allowing compression
grouping and physical relocation to evolve independently.

## Consequences

Every container carries a complete Chunk-ID-sorted Recovery Index mapping chunks
to record and decoded slice. Writer, reader, recovery, and scrub pairwise verify
the index against each record's chunk table. The committed Location Set, rather
than one intrinsic canonical copy, authorizes active, retiring, and quarantined
encodings. Manifest liveness is extended by the bases and versioned dictionaries
required by the selected physical encodings; refcounts remain a checked cache.
