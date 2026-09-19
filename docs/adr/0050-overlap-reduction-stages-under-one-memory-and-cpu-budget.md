---
status: accepted
---

# Overlap reduction stages under one memory and CPU budget

SeqCDC, reduction, Container preparation, DATA publication, Exact maintenance,
and commit Metadata are bounded stages. A full Container becomes immutable
detached work, releasing its Ingest Lane so chunking can overlap compression and
storage I/O. Per-inode retirement remains ordered; Sync, Release, and Commit
wait through their sampled ingest and publication prefixes.

## Memory and admission

Write-through has one 400 MiB ownership budget, including the 32 MiB admission
queue, eight registered Lanes plus one overflow Lane, incomplete SeqCDC suffixes,
and at most two detached 32 MiB Container payloads. ADR 0098 defines the
pending-region sub-ledger. Every transfer changes owners exactly once; shared
immutable payload backing is not double-charged.

One writable inode uses a segmented ring of eight 4 MiB batches. Batches seal on
size, age, discontinuity, or a sequence fence. With multiple writable inodes,
admission uses bounded one-MiB fragments and per-inode round-robin ordering.
Both paths apply frontend backpressure only at their explicit queue limits.

## CPU ownership

All CPU-heavy reduction and maintenance work uses one permanent Rayon pool sized
from the effective CPU quota. Counted permits bound runnable work; stage-specific
byte limits remain authoritative. Jobs request no more permits than their actual
parallel work, return permits at worker completion, and never wait recursively
for the same pool. io_uring verification remains a separate bounded I/O-ordering
boundary.

Parallel hashing, fingerprinting, materialization, codec trials, and verification
must preserve serial Chunk identities, candidate/trial bounds, Record order, and
byte-identical Container images. Worker-count heuristics are performance policy,
not durable format.

## Evidence and publication

Stable Chunk identities and encoder-produced Locations travel with immutable
writer bytes. Publication may trust that writer evidence under ADR 0059;
demand read, recovery, and scrub verify stored bytes independently. Bloom,
recent overlays, and Exact results remain hints.

Metadata objects publish before one shared directory barrier; Namespace Root
and Commit WAL follow, with WAL sync last. Maintenance uses bounded sequential
or read-ahead I/O, the same CPU admission, deterministic reduction order, and
ADR 0048 priority rules.

Tests cover detached-payload and total-budget accounting, SingleStream and
MultiStream backpressure, cross-inode progress, blocked durability, sequence
fences, serial/parallel byte equality, permit unwind, independent verification,
and old-or-complete-new crash recovery.
