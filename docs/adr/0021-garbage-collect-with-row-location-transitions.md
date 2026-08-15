---
status: accepted
---

# Garbage collect with RoW location transitions

GC first commits a container as RETIRING, preventing new selection while pinned
generations may continue reading it. It then writes and verifies replacement
encodings, revalidates liveness against the current generation, atomically
switches Location Sets, waits for old pins, and only then unlinks and directory-
syncs the old container. Every interruption leaves either the old coverage or
additional verified copies.

## Consequences

Partially live multi-chunk records may be decoded, BLAKE3-verified, and regrouped;
logical Chunk IDs do not change. A failure quarantines the bad Location and
prevents deletion. Logical BaseChunkId references let a high-fanout base relocate
once without child rewrites; re-anchoring is separate maintenance. Candidate
selection scores reclaimable bytes against write amplification, codec CPU,
dependency retention, and restore locality under explicit space watermarks.
