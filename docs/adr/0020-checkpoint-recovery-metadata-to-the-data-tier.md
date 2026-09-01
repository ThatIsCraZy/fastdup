---
status: accepted
---

# Checkpoint recovery metadata to the data tier

Every 90 seconds, and once during orderly shutdown, fastdup attempts to publish
a self-contained immutable Recovery Checkpoint to the redundantly protected Data
Tier. It embeds one Commit Record and the complete transitive Metadata graph
needed by its Namespace Root. This deliberately slower disaster-recovery RPO
keeps HDD checkpoint work out of the normal five-second Commit hot path.

## Consequences

Two fixed, paired selector heads name only the current and previous complete
checkpoints. Publication verifies the whole graph and every referenced DATA
Chunk, stabilizes the immutable file and directory entry, then commits the
inactive selector head. It does not scan the Container namespace for discovery.

After complete Metadata-Tier loss, recovery selects the highest wholly valid
checkpoint, verifies every embedded object and reachable DATA dependency before
mutating the replacement Metadata Tier, installs immutable objects, and writes
the original Commit as the last recovery anchor. The daemon then rebuilds Exact
and, when enabled, Similarity as fresh generations before opening the namespace.
Missing, torn, transitively incomplete, or DATA-incomplete checkpoints are
ignored as whole generations rather than partially merged. Later independently
verifiable objects may be offered only through a separately designed
`lost+found` path; they are never merged automatically.

Metadata and DATA GC retain both selected checkpoint graphs. A short-lived
process-local root pin protects a candidate while its graph is copied, but the
scan, verification, and HDD publication hold neither the Commit lock nor the
Metadata-GC publication barrier. The exact v1 byte layout and crash boundaries
are specified in
[`recovery-checkpoint-v1.md`](../specs/recovery-checkpoint-v1.md).
