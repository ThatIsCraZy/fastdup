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
deadline.
