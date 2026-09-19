---
status: accepted
---

# Announce the commit cut before requesting the admission fence

A mutation holds admission while its write-through observer runs, but a commit
needs the exclusive admission fence. If that observer waits for ingest capacity
that only the commit can reclaim, waiting for the fence is circular.

`begin_commit` therefore announces the cut before requesting the fence and
clears the announcement after the fence is taken. Both advisory waits—the
per-inode fragment ring and the multi-stream unbatched budget—wake and observe
the announcement. A writer that would block seals its batch and returns; its
Dirty Extent Map remains authoritative for checkpoint planning and durability.

The announcement exists only during fence acquisition, so ordinary
backpressure resumes immediately. The five-second supervisor remains a fallback
for unrelated slow checkpoints, not the normal release path. Tests park writers
on each capacity wait and require the cut to complete with admission open and
without watchdog intervention.
