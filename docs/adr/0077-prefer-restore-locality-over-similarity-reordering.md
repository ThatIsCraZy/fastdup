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
Record once. Directly adjacent Records in the same Container may share one
physical read, capped at the global 1-MiB storage-range bound. Each Record is
then sliced and verified independently before any of its Chunks resolve. The
plan must preserve Manifest order in its result, verify every selected Location
and decoded Chunk, and fall back to the ordinary bounded candidate and verified
Container-scan path. A single DATA extent keeps the scalar path and allocates no
plan.

The plan is acceleration, never content, liveness, or Location authority. It
adds no durable field, format version, migration, global lock, speculative I/O,
or work to the write hot loop. A later read-ahead policy requires HDD evidence,
an explicit memory/queue budget, cancellation on direction or locality change,
and lower priority than demand reads. GC may improve placement only while it is
already relocating live data and only after a separate placement-order proof;
proactive reordering that creates write amplification remains excluded.

## Evidence status

The corrected 2026-08-27 A/B uses the production `IoUringStorageIo` adapter and
separately measures fastdup calls, ring submissions, and guest block I/O. For
64-KiB Chunks, planned reduces submissions from 128 to 16 and is 25.7% faster;
both paths produce the same ten sequential block reads. For 256-KiB Chunks it
reduces submissions from 128 to 64, changes block reads only from 34 to 33, and
is 12.0% slower. With device readahead disabled both 64-KiB paths produce 2,054
page-level block reads, so API coalescing alone is not device-I/O coalescing.

Although the guest reports `ROTA=1`, no HDD latency was emulated. Bounded
coalescing remains selected because the 8x ring reduction wins and physical
sorting can matter on the intended HDD tier; the sequential fixture cannot
prove seek savings. Physical-HDD fragmented-file evidence is required before
adding a 2x activation threshold, speculative read-ahead, or parallel I/O. See
[`verified-restore-coalescing-2026-08-27.md`](../benchmarks/verified-restore-coalescing-2026-08-27.md).
