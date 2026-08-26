---
status: accepted
---

# Persist Metadata marks but reprove after process start

Metadata GC publishes each exact reachability result as an immutable Metadata
Mark Catalog generation. Its field-by-field v1 format has mirrored 4-KiB
envelopes, strictly Object-ID-sorted 32-byte rows, a Commit-segment binding,
checked layout, zero padding, and domain-separated BLAKE3 hashes for both the
row stream and envelopes. The writer audits the temporary generation before
sync, publishes it without replacement, removes older catalog generations and
Metadata garbage, then makes the combined namespace transition durable with
one Metadata-directory sync.

The catalog remains acceleration rather than deletion authority. A shared
process-local liveness epoch is advanced after new Metadata publication, every
Commit append, and final Metadata Root Pin release. An unchanged epoch may
answer later Online-GC quanta without another graph traversal or directory
inventory. After process start the first collection always performs the exact
Commit/Pin mark and streaming `.fdm` inventory before establishing a reusable
clean state; this prevents a catalog written before an uncommitted crash orphan
from hiding that orphan. Offline scrub independently audits every published
catalog, while collection can discard a damaged old hint and rebuild it from
the authoritative graph.

Directory enumeration has a streaming storage seam so the filesystem adapter
does not allocate a pool-sized vector of names. The exact mark set and the
garbage candidate batch remain bounded by live and reclaimable Metadata object
counts respectively. ADR 0068 allows classified additions from proof-bearing,
nonrotating commits to advance this catalog with immutable delta runs. Every
potential root removal still triggers a complete exact mark and cannot weaken
the exact deletion proof.
