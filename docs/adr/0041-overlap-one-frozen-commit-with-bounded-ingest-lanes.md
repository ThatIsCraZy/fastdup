---
status: accepted
---

# Overlap one frozen commit with bounded Ingest Lanes

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

The write-through reduction path uses one mutex-protected Ingest Lane per hot
inode. Different lanes may FastCDC, encode, and publish concurrently. Observer
callbacks for the same inode queue behind that inode's lane without closing
unrelated lanes. Because concurrent POSIX requests can reach the observer in a
different order than their already-assigned mutation sequences, every lane
validates monotonic sequence and offset continuity and resets on an inversion;
the POSIX Dirty Extent Map remains authoritative. Format-v1 retains no more than
ten registered lanes. An idle least-recently-used lane may be discarded
because its bytes remain authoritative in that map. An in-flight lane is never
evicted. If all registered lanes are active, new inodes use one serialized
overflow lane; it resets on every inode change and therefore trades reduction
continuity for bounded memory without mixing file bytes.

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
write-through worker cap; a transient activity-count snapshot is not treated
as a resource guarantee. The checkpoint planner has a separate bounded worker
pool; a future shared CPU scheduler may coordinate both pools if measurements
justify that added coupling.

Normal backpressure is hierarchical:

- the same inode waits only for its Ingest Lane;
- excess simultaneously active inodes wait on the overflow lane;
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
- Writer: registered lane count is asserted at or below ten and eviction is
  restricted to registry-only `Arc` ownership. Runtime tests hold all ten
  lanes and verify that an eleventh receives the overflow lane until one is
  idle.
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
  resets all CDC/pending state on inode, offset, or sequence discontinuity.
  POSIX externalization separately verifies inode, mutation sequence, range,
  and complete content identity before releasing resident bytes.
- Writer: the Exact-Index publisher lock covers predecessor selection through
  active-reader replacement. Concurrent public ingest tests require both L0
  histories to remain active and every file to read byte-exactly.

## Consequences

Slow compression or metadata persistence no longer imposes a global stop on
ordinary writes. The frozen cut consumes separate immutable views while the
active epoch and unrelated Ingest Lanes advance. Memory is bounded by the POSIX
resident Dirty-DATA guard plus at most eleven write-through lane targets and their
FastCDC suffixes, worker-local encoding buffers, and bounded metadata/index
caches. The overflow lane may reduce dedup/compression quality for workloads
with more than ten simultaneously hot files; increasing that bound is a
benchmark and RSS decision rather than a correctness or format change.
