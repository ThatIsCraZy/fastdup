---
status: accepted
---

# Mount after structural validation and scrub in the background

Current normal-start policy: [ADR 0091](0091-start-from-the-committed-metadata-graph.md)
defers Container structure and DATA availability checks to the initial scrub.
The selected committed Metadata graph is still checked before mounting.

Normal startup uses the structural-start policy described in ADR 0023 instead
of ADR 0036's mandatory complete DATA proof before mounting. The Commit WAL's
causal durability ordering remains authoritative: Container bytes and directory
publication, then immutable Metadata, then the Commit record are synchronized.
Repeating a full payload read does not establish additional crash atomicity;
it detects latent corruption, which belongs to demand verification and scrub.

Startup validates the WAL prefix and compatibility, Namespace and complete
Manifest graphs, file lengths and allocation totals, and Container structure.
A structural Container check pairs Header/Footer with actual length and filename,
validates every Record header and Chunk Table, cross-checks the Recovery Index,
recomputes structural BLAKE3, and checks inter-section padding. The required
Chunk identities and lengths must have structural Locations; each selected
Depth-1 dependent Chunk must have an independently encoded Base of matching
identity and length. Exact and Similarity hints are not authority. Only required
identities and their candidate Bases are retained across streaming Container
passes; no complete pool Chunk map is built. Structure reads suppress speculative
payload readahead through separate random-advice file descriptors.

The normal structural start fails closed on a rejected newest graph; it does
not silently roll back to an older generation. The explicit full recovery and
offline verification APIs retain their complete proof and fallback behavior.
Metadata-Tier loss still uses full Recovery-Checkpoint verification and rebuild.

## Deferred content verification

Structural evidence has a separate format type and never implements the
`RequiredChunkVerifier` content-proof interface. Demand readers verify complete
stored Record CRCs, decoded Chunk identities and Base dependencies before any
bytes escape. New or reused DATA introduced by a writer still needs the existing
complete content/publication proof. Unchanged committed Manifest dependencies
may be inherited after structural startup because their original Commit already
established durability; this carries no claim that startup rehashed their bytes.
Physical formats and writer commit barriers do not change.

After mount, one read-only worker fully verifies a snapshot of published
Containers, including payloads and dependent Bases. It retains at most one
bounded Container image and verification working buffers, runs at idle I/O and
reduced CPU priority, and issues payload reads in at most 256-KiB portions. Saved-round envelope
reconciliation follows ADR 0092’s separate bounded parallel resume path. Payload pauses adapt
to measured read time and frontend storage activity: approximately 10% read duty
under activity and at most 50% while idle. These are operating targets, not a
block-scheduler latency guarantee. Cancellation is checked between portions and
in short sleep slices. ADR 0092 supersedes the original process-local progress policy: incomplete
rounds durably retain full checks and resume after current-envelope and DATA
coverage reconciliation. This remains historical work, never a current payload
proof or deletion capability.

Automatic and manually requested Online GC wait for successful completion of
this initial pass. No Container deletion can race its snapshot. Completing the
pass only opens that scheduling gate; it grants no GC deletion authority. The
usual generation-bound candidate proofs, barriers and deletion checks remain.
A scrub error latches mutation admission closed for the lifetime of the mount,
wakes blocked mutation waiters with `EIO`, keeps the GC gate closed, and reports
the failure in the journal and WebUI. A checkpoint resume cannot clear this
latch. Demand reads continue to validate their own data. No automatic rollback,
repair or object deletion occurs on scrub failure.

## Recovery Checkpoint publication

The runtime's periodic DATA-tier Metadata copy uses an exact, pinned committed
graph and independently verifies the copied objects, graph, image and selector
publication, without rereading all DATA twice every 90 seconds. The source
Commit's DATA durability is inherited as above. The separate full-publication
API remains available. Restoring a lost Metadata Tier and offline scrub still
require complete DATA verification. Both publication paths retain the same
before/after-I/O crash fault matrix and on-disk format from ADR 0020.

This supersedes the startup and periodic-copy verification timing in ADRs 0020
and 0036, while preserving their durability ordering and content verification
on actual reads, writes, disaster recovery and offline scrub.
