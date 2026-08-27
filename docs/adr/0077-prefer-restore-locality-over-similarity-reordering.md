---
status: accepted
---

# Prefer restore locality over similarity reordering

The DATA Tier is HDD-backed in production, so independent Records are kept in
logical placement order. Similarity-based reordering after encoding is not a
production placement policy: it saved no additional bytes in the reference
corpus while turning adjacent restore demand into avoidable seeks. The
Compression Region and 64-MiB Placement-Window bounds from ADR 0016 remain;
this record supersedes only its permission to physically cluster already
encoded Regions by similarity.

Demand restores may build a bounded Verified Read Plan for the DATA extents in
one frontend read. The plan may select between at most two effective ACTIVE
Locations per Chunk, prefer the preceding Container and forward offset, sort
physical work by Container and Record offset, and decode one shared Encoding
Record once. It must preserve Manifest order in its result, verify every
selected Location and decoded Chunk, and fall back to the ordinary bounded
candidate and verified Container-scan path. A single DATA extent keeps the
scalar path and allocates no plan.

The plan is acceleration, never content, liveness, or Location authority. It
adds no durable field, format version, migration, global lock, speculative I/O,
or work to the write hot loop. A later read-ahead policy requires HDD evidence,
an explicit memory/queue budget, cancellation on direction or locality change,
and lower priority than demand reads. GC may improve placement only while it is
already relocating live data and only after a separate placement-order proof;
proactive reordering that creates write amplification remains excluded.

