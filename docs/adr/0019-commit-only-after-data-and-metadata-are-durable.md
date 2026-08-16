---
status: accepted
---

# Commit only after data and metadata are durable

Every periodic generation first seals and synchronizes all data containers,
then writes and synchronizes immutable metadata objects, then appends and
synchronizes one Commit Record to the Commit WAL. Only the final WAL sync makes
the Namespace Root recoverable. The five-second target and ten-second guarantee
take priority over container fill, so deadlines may seal small containers for
later RoW compaction.

## Consequences

WAL records explicitly encode length, type, version, generation, previous-record
hash, CRC32C, and a BLAKE3-identified payload. Recovery accepts only a contiguous
valid prefix and stops at the first torn or invalid record. The append-only WAL
is authoritative; redundant superblock slots may accelerate segment discovery
but an in-place root pointer is never required for correctness. Size and memory
pressure may commit early, while the oldest admitted mutation controls the hard
deadline. The v1 daemon starts an immediate pressure checkpoint at 512 MiB of
active checkpointable Dirty DATA, exactly eight format-v1 maximum Container
sizes. It closes mutation admission while that pressure checkpoint catches up,
and also closes admission if the next active epoch reaches the same threshold
while a time-triggered checkpoint is still running. Sparse holes, overwrites of
already dirty ranges, frozen epochs, and encoder buffers do not inflate this
trigger; it is a bounded commit-batch rule rather than an RSS measurement.
