---
status: superseded by ADR 0095
---

# Shard Namespace state behind one committed root

This ADR introduced the graph contract that remains current: one Commit Record
selects one Namespace Root descriptor, which binds an ordered set of bounded,
content-addressed Namespace Shards. The writer makes every child durable before
the descriptor and publishes the Commit Record last. Recovery, Metadata GC,
offline scrub, and Recovery Checkpoints traverse the same graph and fail closed
on a missing, corrupt, reordered, duplicated, or substituted child.

ADR 0095 replaces this ADR's physical layout. Epoch 3 no longer cuts one
canonical Namespace byte stream with FastCDC; it stores independently decodable
Inode and Entry record-range shards. The single-root visibility and complete
graph-validation rules above are unchanged.
