---
status: proposed
---

# Repair FUSE fragmentation through a bounded per-inode write pipeline

The raw FUSE session assigns a nonzero per-inode receive ticket
before spawning the asynchronous request task. The ticket records the FUSE
offset and length. Within the first eight received writes, the adapter selects
the next contiguous fragment while never moving a request ahead of an
overlapping predecessor. A missing contiguous fragment falls back to receive
order after two milliseconds or when the window fills. Only the selected write
crosses the Namespace seam; other tickets wait asynchronously without occupying
a blocking-executor worker or stopping dispatch for reads and other inodes.
This can repair interleaving between already delivered one-MiB FUSE fragments
while preserving overlapping-write semantics.

Writable regular-file handles do not advertise `FOPEN_PARALLEL_DIRECT_WRITES`.
The kernel may withhold a pwrite's later FUSE fragments until its earlier
fragment receives a reply, so userspace cannot always see the missing
contiguous fragment inside any bounded window. Release 5 (strict receive order)
and release 6 (eight-request offset reorder) both remained heavily Target-bound
and repeatedly closed mutation admission. The pipeline therefore remains a
tested seam, but activation is fail-safe closed until ingest accepts ordered
whole-pwrite units or the kernel-facing boundary can bound complete writes.

Blocking the single FUSE receive loop was rejected because mutation pressure
must not stop read-only operations. Strict receive-order execution was also
rejected after release 5 showed that parallel kernel fragmentation can
interleave offsets even when userspace task order is preserved: Veeam remained
Target-bound at 23.6--25.2 MB/s per task and repository mutation admission
closed for 40.9 seconds in the first minutes. Unbounded same-inode task entry
was rejected because it previously changed chunk boundaries, drove admission
closure, and reduced Veeam throughput.

Acceptance requires reverse-scheduling and overlapping-write tests, a bounded
interleaved-fragment test, the existing POSIX and write-through
deduplication/recovery suites, a local same-inode A/B with no throughput
regression, and a clean Veeam end-to-end run. Release 7 passed two Active Full
runs after installation: the cold run completed at 680.7 MB/s and the warm run
at 798.7 MB/s, with all eight tasks successful and no service errors. The warm
run added no mutation-admission closure, but Veeam still classified the job as
Target overall (one task Source, seven Target). The serialized gate is therefore
qualified without a material regression, while parallel-write activation and
this decision remain proposed until the missing whole-write boundary exists.
