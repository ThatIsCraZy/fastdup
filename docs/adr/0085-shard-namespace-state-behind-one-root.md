---
status: accepted
---

# Shard namespace state behind one committed root

The Commit Record continues to select exactly one Namespace Root Object ID, but
that object is now a compact descriptor for an ordered graph of bounded
Namespace Shards. The logical Namespace Root remains one canonical, completely
validated namespace state. Its canonical byte stream is divided by one fixed
FastCDC profile (256-KiB minimum, 512-KiB average, 1-MiB maximum); each slice is
stored as a Metadata Object no larger than the existing 16-MiB object bound.
The root descriptor binds the exact ordered shard IDs, byte offsets, total
length, counters copied into the Commit Record, and the BLAKE3 hash of the
complete canonical stream.

The writer publishes and synchronizes all unique shards before the descriptor,
then appends the Commit Record last. Recovery, normal reads, Metadata GC,
offline scrub, and DATA-tier Recovery Checkpoint traversal all resolve the same
descriptor, require an exact contiguous shard partition, authenticate every
object and the reconstructed full-stream hash, and only then decode and validate
the global namespace. Missing, reordered, duplicated, substituted, or truncated
shards invalidate the whole generation. Metadata-GC addition classification
includes every newly published shard and descriptor.

This keeps the commit and POSIX interfaces independent of physical sharding,
removes the 16-MiB namespace-capacity failure, and gives local namespace edits
bounded resynchronization instead of forcing fixed-offset suffix rewrites. It
does not make checkpoint construction incremental: the current checkpoint path
still snapshots and canonicalizes the complete in-memory namespace outside the
request hot loop. A persistent namespace tree may later replace that internal
construction without changing the single-root or graph-validation contract.

The repository is pre-production. Writers emit only this graph form and readers
accept only this graph form; no flat-root decoder, migration, or format-version
fallback is retained.
