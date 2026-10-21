---
status: accepted
---

# Chain incremental Recovery Checkpoints

ADR 0020 publishes a self-contained copy of the complete committed Metadata
graph whenever the Commit changes. On the test appliance on 20 and 21 October
2026 that graph held 13,706 objects and 1.83 GB. During a Veeam job a new copy
followed every 80–160 seconds: roughly 19 MiB/s of random Metadata-tier reads
plus the same sequential DATA-tier writes for as long as ingest ran. Every
restart repeated one full copy, and each new checkpoint made Online GC reread
the whole image twice to rebuild its protection set. The cost grew with the
stored Metadata, not with the change. This record supersedes the full-copy
publication of ADR 0020 and its v1 format. Its 90-second schedule, shutdown
publication, head selection, recovery ordering and DATA-tier placement remain.

## Decision

A Recovery Checkpoint becomes a chain of immutable checkpoint files: one Base
followed by at most 255 Deltas. Each file still embeds one Commit Record. A
Base embeds the complete transitive Metadata graph of its Commit. A Delta embeds
exactly the reachable objects that are absent from its parent chain prefix and
names that parent by generation, file length and body hash. The chain prefix
ending at any file is closed under Metadata references (prefix closure). Its
own Commit therefore recovers from that prefix alone.

Each file carries an authenticated object table with the payload span,
checksum and structural facts of every embedded object: kind, level, logical
length and allocated bytes. Writers validate a new parent's reference to an
already checkpointed child against these facts instead of rereading the child.
Readers and scrub recompute the facts from every decoded object.

The publisher traverses the pinned source graph and stops at every object the
chain already contains. Namespace graph objects are still read in full on every
publication; Manifest subtrees cost only their new nodes. The publisher keeps
the chain's object facts as active writer state behind the existing
publication lock. A new owner adopts the selected chain by reading descriptors
and authenticated tables only. A head-selected file was synced and renamed
before its head became durable. Undetected media corruption after publication
is the responsibility of recovery and scrub, as in ADR 0020.

A new Base is written when no selected chain can be adopted, when the chain
already has 256 files, when the chain's Delta payload reaches its Base payload,
or when explicit verified publication is requested. A Base names the tip of
the chain it replaces. That previous chain stays retained and recoverable until
the next Base, so a damaged Base never removes every recovery point.

Selection authority remains the two fixed heads. Recovery tries the newest
head, then the older head, then the newest head's previous-chain link. It
accepts a candidate only after every file of its chain verifies: descriptor,
links, table, body hash, payload identities, facts and prefix closure. Then the
Commit graph and every reachable DATA Chunk must verify. Corruption anywhere in
a chain rejects that whole candidate. Only reachable objects are installed.

Retention keeps every file of both selected chains and of the linked previous
chain. After the new head is durable, the publisher removes other names with the
checkpoint prefix and suffix. Removal is cleanup, never selection authority.

DATA GC protects exactly the Chunks reachable from every retained recovery
point. Online GC caches each immutable file's references, keyed by its identity,
and traverses the chain in memory. A new Delta costs one read of that Delta.
Offline scrub and GC recompute protection from fully verified chains.

## Consequences

Publication I/O is proportional to new Metadata. At the measured change rate,
a Delta is tens of megabytes instead of 1.83 GB. A Base still costs one full
copy, amortized over its chain. Restart costs descriptor and table reads.

Recovery reads up to 256 files and chain garbage, bounded by the Base payload.
Objects superseded within a chain stay in the DATA tier until the next Base but
are neither protected nor installed. Previous-chain retention keeps chunks
alive for at most one chain lifetime longer than ADR 0020 did.

The disk format is incompatible with v1. Pre-production appliances discard v1
files. No decoder or migration exists. The byte layout and crash boundaries are
specified in [`recovery-checkpoint-v2.md`](../specs/recovery-checkpoint-v2.md).
