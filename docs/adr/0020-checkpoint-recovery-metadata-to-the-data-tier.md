---
status: accepted
---

# Checkpoint recovery metadata to the data tier

Current policy (2026-09-07): [ADR 0090](0090-mount-after-structural-validation-and-scrub-in-background.md)
implements structural normal startup followed by background content scrub.
It supersedes mandatory startup rehashing and the runtime's repeated DATA proof
when copying an already committed graph to a Recovery Checkpoint. Full demand
reads, disaster recovery and offline scrub retain independent content checks.
The original rationale and remaining contracts follow below.

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

During shutdown the periodic scheduler is stopped immediately, alongside GC and
Scrub, so it cannot repeatedly copy the same graph while another worker drains.
An already running copy is awaited. The mandatory final publication runs after
final Namespace catch-up; stopping periodic scheduling early does not move that
last disaster-recovery point before the final durable Commit.

## Unchanged committed graph (2026-09-13)

The destination owner shares one serialized successful-publication receipt
across its clones: the full Commit Record and resulting checkpoint summary.
After selecting and pinning the source candidate through the normal Commit
validation boundary, an ordinary `publish_committed` call may reuse that
receipt when the complete record matches. It skips graph enumeration and all
destination checkpoint I/O. This bounded owner state is not a content cache.

Only successful completion of checkpoint publication, including its selector
durability barriers, issues the receipt. Fallible attempts take it first, so an
error revokes reuse. A new owner starts without a receipt. Explicit verified
publication and Independent intent do not reuse it; disaster recovery and
offline scrub revoke it before validating physical bytes. A newer Commit follows the full
publication protocol. Shutdown reuses a receipt only if its final Commit is
already covered; this does not change the disaster-recovery RPO or file format.

## Writer-carried checkpoint validation (2026-09-13)

New checkpoint publication no longer rereads its freshly written image. The
publisher first validates the exact transitive source graph under its root pin,
including Manifest and Chunk lengths. Each object copied into the file must
hash to its selected source identity. Entry checksums, the body hash and lengths
are computed from the emitted bytes using bounded writer memory. The resulting
summary carries the required-Chunk count from that source traversal. Explicit
full publication verifies required DATA once against the source graph.

File sync, no-replace publication, directory sync and selector durability retain
their existing ordering. A readback is not a substitute for those barriers.
Existing published files encountered on retry or collision are still audited;
a racing file must also match the complete writer descriptor. Selector reads
remain bounded control I/O. Disaster recovery and scrub independently verify
stored images and their complete graphs as before. No whole-image RAM cache or
new disk format is introduced. This supersedes immediate image readback in the
original publication protocol and ADR 0090.
