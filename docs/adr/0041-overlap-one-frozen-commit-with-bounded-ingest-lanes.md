---
status: accepted
---

# Overlap one frozen commit with bounded Ingest Lanes

Current-state note (2026-09-05): ADR 0054 replaces the historical FastCDC names
below with SeqCDC-v1. The Active/Frozen ownership and admission rules remain.

fastdup maintains exactly one Active Dirty Epoch and at most one Frozen Commit
Cut. Freezing a cut does not close mutation admission: later accepted mutations
receive their normal per-inode sequence and remain immediately readable in the
new Active Dirty Epoch. A failed durable publication retains the identical
frozen token and bytes for retry. No second frozen cut may be created until the
first is installed or abandoned by process loss.

Checkpoint publication remains single-writer. Its order is immutable DATA and
Container directory durability, immutable Manifest and Namespace objects, and
the Commit WAL file sync last. Only that final sync makes the frozen cut
recoverable. A crash discards the Active Dirty Epoch and selects a wholly valid
WAL generation; it never merges active mutations into the recovered cut.

The write-through reduction path has a permanent globally bounded worker pool
and one ordered job queue per inode. The POSIX mutation becomes immediately
visible after one owned immutable Mutation Payload is installed into the Dirty
Extent Map. The Observer receives shared views of that same allocation and
splits them into at most one-MiB jobs without another payload copy. Only queue
admission, not FastCDC, encoding, or DATA I/O, is part of the write request. A
full 16-MiB queue applies ordinary backpressure to the admitting request after
releasing the inode-data lock. A separate per-inode Observer-order lock spans
live mutation and complete queue admission, so assigned mutation sequences
cannot overtake each other while readers and workers remain unblocked. Queue
assertions reject decreasing sequences and more than one active job for an
inode. Before admitting an unbatched fragment, the queue seals any older
partial batch for that inode, including before a backpressure wait releases
the queue lock. Writable-handle changes can switch admission mode while an
earlier admission is waiting; the mode switch cannot let a newer fragment
pass the open batch. Sealing moves existing payload views into the ordered
queue and adds neither a payload copy nor a storage operation.

Each lane retains a segmented userspace Tail built from those immutable views.
It compacts only the bounded prefix required by FastCDC, never shifts an entire
Container-sized Tail, and keeps Pending Chunks as zero-copy slices of the
compacted prefix through compression preparation. The per-inode `VecDeque` is
the internal ring buffer; mutex and condition-variable blocking occurs only for
real empty/full contention, not between ordinary reduction steps. Lane state is
cache-line aligned so unrelated inode lanes do not share their hot sequencing
state. A partial overwrite or truncate copies only surviving split fragments
into exact-sized backings; retaining a tiny slice of a large obsolete request
allocation is forbidden because it would hide process RSS from Dirty-DATA
pressure accounting.

Different inode queues may FastCDC, encode, and publish concurrently; one inode
always advances in mutation order. `Sync`, `Release`, and a Frozen Commit Cut
wait through the newest actually enqueued DATA job at or below the sampled
Inode Version before consuming reduction evidence. Metadata-only version gaps
have no Ingest work and therefore cannot become an unfulfillable queue fence.
This fence is not a new durability promise: Namespace/WAL visibility still
follows the bounded commit policy. Unlink, truncate, metadata clone, and
rename-overwrite enqueue zero-payload sequence barriers because they invalidate
or reorder DATA lane state. The POSIX Dirty Extent Map remains authoritative
until independently verified immutable extents replace its resident ranges.

Format-v1 retains no more than eight registered Ingest Lanes. An idle
least-recently-used lane may be discarded because its bytes remain authoritative
in the Dirty Extent Map. An in-flight lane is never evicted. If all registered
lanes are active, new inodes use one serialized overflow lane; it resets on
every inode change and therefore trades reduction continuity for bounded memory
without mixing file bytes.

Truncate and other invalidation barriers reset a registered lane in place.
A checkpoint may already hold a snapshot of that lane; removing it and creating
a replacement would permit two independently drained streams for the same inode.
Only registry-exclusive idle ownership permits eviction. The registry lock is
released before an invalidation waits for the lane lock. A staging failure uses
the same in-place reset. A failed detached publication marks degradation without
resetting later lane contents or waiting for their lock: those contents remain
valid, and their producer may be waiting for that publication's queue space.

The Container-generation allocator lock covers only checked reservation of one
number. It is never held during chunking, compression, Container I/O, Manifest
planning, or metadata I/O. Exact-Index publication has one separate publisher
lock covering the complete read-predecessor, publish/compact, activate, and
install sequence. Individual immutable-object locks are insufficient because
two publishers derived from the same active Run Set could otherwise race and
drop one rebuildable acceleration update. Exact-Index failure degrades
acceleration only and cannot authorize content or roll back Namespace state.

All write-through encoders share one counted worker-permit budget. A lane may
request a smaller fair share based on the number of active writers, but it can
start encoding only after acquiring real permits. Permit acquisition and
retirement assert that the sum of outstanding permits never exceeds the
write-through worker cap; the permit lease ends after deterministic Container
preparation and before Container file/directory durability. A slow data device
therefore cannot monopolize every compression permit and prevent another file
from preparing and publishing concurrently. A transient activity-count
snapshot is not treated as a resource guarantee. Compression, Similarity,
Delta, Reorder, checkpoint Container encoding, and
maintenance verification now execute on one permanent quota-sized Rayon pool
with worker-local codec state. The io_uring verifier remains a separate bounded
pool because every queued input holds an io_uring publication-memory lease and
crosses a kernel-I/O ordering boundary.

Normal backpressure is hierarchical:

- a write waits only when admitting it would exceed the 16-MiB Ingest Queue;
- one inode has one active job while different inode queues use the permanent
  worker pool concurrently;
- excess simultaneously active inodes reduce through the overflow lane;
- a full Container triggers an immediate durable DATA publication;
- published Containers are coalesced only within ADR 0040's bounded window; and
- global mutation admission closes only at the five-second safety guard, the
  512-MiB resident Dirty-DATA guard, or a durable-progress failure.

The write-through registry lock protects only lane selection, bounded counters,
and the sealed-Container queue. No data-tier or metadata-tier I/O occurs while
it is held. A checkpoint captures the number of already sealed Containers
before freezing the Namespace cut. A Container sealed concurrently after that
capture remains conservatively charged to the next cut; a successful commit
can therefore overcount temporarily but can never retire future evidence.

## Paired invariants and evidence

- Writer: one checkpoint lock permits one Frozen Commit Cut; POSIX completion
  installs exactly that token. Reader/recovery: the hash-chained WAL selects one
  complete generation. Tests freeze generation N, write N+1, recover N, then
  commit and crash-recover N+1 byte-exactly.
- Writer: registered lane count is asserted at or below eight and eviction is
  restricted to registry-only `Arc` ownership. Runtime tests hold all eight
  lanes and verify that a ninth receives the overflow lane until one is
  idle.
- Writer/recovery: a deterministic checkpoint test pins a lane snapshot, then
  truncates and publishes a later full Container before allowing the checkpoint
  to drain. Publication stays monotonic, live reads return the replacement, the
  first recovered cut returns the old file, and the next cut recovers the exact
  replacement. The previous lane-removal implementation fails the publication
  sequence assertion in this test.
- Writer: every planner boundary recomputes the exact Pending Chunk byte sum,
  verifies ordered non-overlapping ranges, enforces the 256-KiB Chunk maximum,
  and asserts that one lane retains at most one 32-MiB Container target plus a
  256-KiB FastCDC suffix. Registry aggregation asserts the 384-MiB
  write-through memory budget. Deliberately corrupted accounting tests must
  panic at this boundary; storage exhaustion and I/O failures remain ordinary
  errors with resident fallback.
- Writer: counted encode-worker permits assert on over-acquire, underflow,
  overflow, or retirement above the configured cap. A concurrency test holds
  disjoint leases and proves that their sum exactly exhausts, then restores,
  the cap.
- Writer: a lane records its inode and last observed mutation sequence and
  resets CDC continuity on inode, placement, offset, or sequence discontinuity.
  Before replacing a populated lane, it drains complete Chunks from the old
  range and detaches Pending Chunks under the old inode, placement, and carried
  mutation sequences. Only the bounded incomplete CDC suffix is discarded.
  POSIX externalization separately verifies inode, mutation sequence, range,
  and complete content identity before releasing resident bytes.
- Writer: the Observer-order lock makes live mutation plus queue admission one
  ordered per-inode handoff; queue admission asserts monotonic sequences and
  the scheduler asserts one active job per inode. Public tests block Container
  sync, prove write completion and live reads before durability, prove
  cross-inode progress, force the 16-MiB queue to apply backpressure, and prove
  `Release` cannot skip queued write or unlink barriers.
- Writer: the POSIX Observer owns its Mutation Payload after the borrowed FUSE
  request returns; Dirty and Queue views share that backing. Differential tests
  split deterministic content at hostile segment boundaries and require the
  segmented Tail to produce the exact FastCDC-v1 boundaries of one contiguous
  input. Planner assertions pair every cached Tail and Pending-Chunk byte count.
- Writer: the Exact-Index publisher lock covers predecessor selection through
  active-reader replacement. Concurrent public ingest tests require both L0
  histories to remain active and every file to read byte-exactly.

## Consequences

Slow compression or DATA/metadata persistence no longer executes synchronously
inside ordinary writes. The frozen cut consumes separate immutable views while
the active epoch and unrelated Ingest Lanes advance. Admission may still block
at the explicit 16-MiB queue boundary. Queue bytes remain a conservative work
budget but no longer imply an additive 16 MiB payload allocation while the
corresponding Dirty view survives. Memory is bounded by the POSIX resident
Dirty-DATA guard, the logical Ingest Queue, at most nine write-through lane
targets and their FastCDC suffixes, two detached Container payloads,
worker-local encoding buffers, and bounded
metadata/index caches. The overflow lane may reduce dedup/compression quality
for workloads with more than eight simultaneously hot files; increasing that
bound is a benchmark and RSS decision rather than a correctness or format
change.


## Partial publication and cut fences (2026-09-06)

Detached work carries both its final admission sequence and the minimum of its
Chunks' carried sequences. A Sync/Release/Commit fence snapshots the greatest
already-enqueued publication ordinal containing a complete Chunk at or before
the requested sequence. It waits through ordered retirement of that fixed
prefix; later arrivals cannot extend the fence. A Container ending after a cut
may still contain required pre-cut recipes.

A partial commit drain detaches through the same 64-MiB publication queue as a
full Container. It captures a finite per-inode retirement target while holding
the Lane lock, then releases that lock before waiting for encode, DATA durability
and Namespace externalization. A bounded completion reply propagates a failed
partial publication to the checkpoint; resident DATA and the Frozen token remain
available for retry. Queue pressure may still block a producer with its Lane
held; no uncharged detached payload is introduced.

A failed partial-publication test requires the same Frozen token and byte-exact
crash recovery after retry. Queue tests cover crossing sequences and fixed
retirement targets. A blocked
partial-publication integration test requires same-inode forward progress and
a second concurrent publication, then crash-recovers only the original Frozen
prefix byte-exactly.

Inline Exact/FILL reuse must attach its Prepared Extent Recipes to Namespace
before releasing the Lane lock. A post-cut Ingest job can consume pre-cut Tail
bytes without belonging to the cut's Ingest fence. Unlike detached Container
work, its inline recipes have no publication ordinal: returning them to the
worker after unlocking would leave a window in which the commit drain sees
neither the Tail nor its recipes. Staging therefore completes this memory-only
handoff itself. It does not wait for DATA durability or later Ingest retirement.
Mutation observers release inode state before queue admission; externalization
does not acquire Observer-order locks. The per-instance test-only pause after
Lane release exercises both Exact and FILL handoffs through queued writes and
checkpoint/recovery, including exclusion of the later Active suffix.

## Preserve stable work across discontinuous writes (2026-09-06)

Two adjacent writes may arrive in reversed offset order. Clearing an entire
partially filled lane in that case discarded almost a Container of reusable
work, even when most of the preceding range was unchanged. The checkpoint then
had to process that resident range again. The public write/checkpoint regression
reproduces a 30.30-MiB rechunk rest after swapping two one-MiB writes following
a 28-MiB sequential prefix; draining before reset reduces it to 1.23 MiB.

This drain uses the existing worker permits and 64-MiB detached publication
budget. It executes in the Ingest worker and does not wait for storage durability
while resetting the lane, apart from ordinary bounded queue backpressure. It
preserves old sequences: a subsequent overwrite cannot authorize stale recipes
in Active, while an earlier Frozen cut can still reuse its original bytes.
Background publication failure retains resident fallback and degrades
acceleration through the existing path. Truncate/barrier invalidation and idle
lane eviction retain their existing resident-fallback policy.

The drain can create partial Containers at a discontinuity. This trades some
Container overhead for preserving the preceding stable range; it adds neither
an unbounded staging buffer nor a new durable format. Existing reader, recovery,
and scrub checks remain authoritative. Regression cases recover the complete
file byte-exactly, isolate the Frozen prefix from a later overwrite, and inject
a failed publication before retrying the checkpoint.

## Complete a durable cut without fallible allocation reads (2026-09-06)

Before installing any inode, Namespace completion checks every installed
reader's verified logical size, allocated-byte summary, and mutation sequence
against the Frozen cut. Replacing that exact prefix leaves the Active overlay
and its already maintained live allocation count unchanged. Installation must
not repeat an allocation-range walk through the new Manifest after retiring
the Frozen epoch: Metadata I/O can fail even for a previously verified object,
and a failure midway through installation would poison the Namespace locks
after the WAL Commit was already durable.

A public completion regression supplies a verified summary with unavailable
allocation pages and preserves a later overlapping write and exact `st_blocks`.
A storage-fault matrix combines sparse truncate/extend, replacement rename,
Hardlinks, an open replaced inode, and post-cut truncate/unlink. Failures before
and after Metadata/DATA operations must recover the preceding or complete
Frozen generation, never the later Active image. Independent read, recovery,
and scrub verification remain required.
