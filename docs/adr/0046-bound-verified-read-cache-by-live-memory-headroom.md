---
status: accepted
---

# Bound the verified read cache by live memory headroom

fastdup shares decoded Chunk bytes only after the complete stored encoding,
logical length, and BLAKE3 Chunk identity have been verified. The cache is
attached to installed Manifest readers, not recovery or scrub, so it can never
become DATA authority or hide a missing durable dependency.

The same live-headroom policy also governs a separate repository-wide cache of
independently verified immutable Exact Index pages. This cache retains only a
hot subset of the persistent index, never the complete Chunk map. A hit removes
Metadata-Tier page I/O but still yields only unverified Location candidates;
the selected immutable Container record and Chunk identity must be verified
before reuse. Exact pages use direct-mapped cache-line-separated slots rather
than a global LRU.

The cache is four-way set associative and sharded on cache-line-separated
locks. There is no process-global pointer-heavy LRU. A hit touches one shard
and at most four entries. A miss performs ordinary verified Container I/O
without holding a cache lock; only the final immutable admission and exact byte
accounting are serialized. Concurrent misses may therefore repeat work, but
they cannot duplicate one resident identity or exceed the current target.

The default hard cache limit is the smaller of one eighth of effective RAM and
8 GiB. At least the greater of one quarter of effective RAM and 4 GiB remains
outside the cache for Dirty DATA, Ingest Lanes, codec workers, metadata/index
caches, XFS clean pages, writeback, and device queues. The effective limit and
available headroom are the conservative minima of `/proc/meminfo` and finite
cgroup-v2 `memory.high`/`memory.max` limits. One process-wide
`MemoryBudgetGovernor` owns Linux sampling and publishes one fail-closed
snapshot to every rebuildable cache. Sampling occurs at most every 250 ms on
cache access, not in FastCDC, Bloom probes, or encoding loops.

The Exact Index page cache has an independent smaller hard budget: one 128th of
effective RAM, clamped between 1 MiB and 256 MiB. Its resident target grows up
to that preallocated geometry only while `MemAvailable` exceeds the shared
reserve. Falling below the reserve purges resident pages; Swap charged to the
fastdup process sets its target to zero. Fixed slot metadata is cache-line aligned, and page
admission never allocates an unbounded lookup table.

Active immutable Exact Runs additionally use rebuildable membership filters.
They reuse the same cache-line-aligned blocked Bloom implementation as the
reduction hot path, but have a separate active-set budget of one 32nd of
effective RAM, clamped to 1 MiB through 8 GiB and limited by available bytes
above the same shared reserve. Dense filters of at least 2 MiB use a dedicated
anonymous mapping with `MADV_HUGEPAGE`; smaller filters stay on the heap. The
advice never covers allocator pages containing unrelated Rust objects, and
lookup remains one ordinary slice access with no branch on backing type.
Headroom is resampled for every Run-Set activation; if the fastdup process has
charged Swap, no filters are admitted to the replacement.
A missing filter changes only performance:
lookup falls through to verified Exact pages. The immutable filters are rebuilt
during Run activation audit, never persisted, and never authorize a Location.

The fixed set metadata plus resident payload can never exceed the hard limit.
When available memory cannot cover the reserve, the payload target shrinks. If
resident payload exceeds the new target, all payload entries are discarded
rather than gradually chasing pressure. Nonzero Process Swap sets the payload
target to zero, purges all entries, and refuses new admission until that charge
returns to zero. Host and current-cgroup Swap remain separate telemetry because
either may belong to another workload when the process is not yet running in
its dedicated production cgroup. Durable reads continue through the verified
XFS path.

This process policy prevents the fastdup caches from intentionally consuming
the I/O reserve, but an application budget cannot overrule kernel reclaim. A
production service that promises no fastdup Swap must run in a cgroup with
`memory.swap.max=0` (systemd `MemorySwapMax=0`) and set
`FASTDUP_REQUIRE_CGROUP_NO_SWAP=1`; daemon startup then validates the kernel
boundary before creating or opening either repository root. `MemoryHigh` and
`MemoryMax` remain explicit deployment inputs. `mlock` is rejected: pinning
cache pages would make kernel and device-queue pressure worse and commonly
requires unsafe privilege/limit assumptions.

DATA-cache hits, misses, admissions, evictions, pressure/oversize rejections, entry count,
payload resident/target bytes, fixed metadata bytes, hard limit, and reserve
plus the last effective-limit/available/Swap sample are exposed in the
appliance and FUSE telemetry. Cache-hit rate is derived from
hits divided by hits plus misses; a zero denominator means no DATA lookup was
attempted.

Exact-page hits, misses, hit-rate basis points, resident/target/capacity pages,
evictions, pressure rejections, reserve, and the last memory/Swap snapshot are
reported separately. This keeps Chunk payload locality and Exact Index lookup
locality measurable without conflating their memory costs.

Bounded Container reads retain a third cache: decoded and verified
Header/Footer descriptors keyed by immutable `ContainerId`. It never retains
the 4-KiB envelope pages or Chunk payloads. Its hard capacity is 16,777,216
descriptors. That addresses 512 TiB even at the current minimum 32-MiB physical
Container size. The cache uses 256 cache-line-aligned shards and allocates
HashMap storage only for descriptors actually admitted; constructing it does
not reserve the multi-gigabyte hard capacity. Lookup takes one shard lock and
there is no process-global LRU lock.

The resident target is the smaller of hard capacity, two percent of effective
RAM using a conservative 160 bytes per entry, and available bytes above the
shared cache reserve. A healthy 128-GiB appliance can therefore reach the full
512-TiB addressable set. Lower-memory or pressured systems retain less. Any
Swap charged to the fastdup process sets the target to zero, releases all
allocated shard maps, and rejects admission until pressure clears. Allocation failure
rejects only the cache entry and never the verified read.

A hit removes `object_len` plus Header/Footer reads, but the selected Record is
still range-read and must pass coordinate, CRC, decoded-length, and Chunk-ID
verification. Hits, misses, admissions, evictions, pressure/allocation
rejections, hard and current entry/coverage limits, resident/fixed bytes, and
the pressure sample are reported separately.

Run-membership filter count, allocated and huge-page-advised bytes, probes,
definite absences, and required Exact lookups are reported independently. A
useful filter should remove substantially more Exact-page misses than its
immutable RAM footprint; this remains a benchmark gate rather than a
correctness assumption.

## XFS publication and io_uring

ADR 0058 supersedes this section's worker-loop, verifier-pool, setup-fallback,
and synchronous-policy details. The durability order and memory bounds below
remain in force.

The same memory reserve applies to a future XFS `io_uring` publisher. At high
load, fastdup may create hundreds or thousands of immutable Containers per
minute. At 1,000 Containers/minute, per-Container file and directory syncs alone
mean roughly 33 sync operations/second. The useful unit is therefore not one
Container's small syscall count, but many independent publication state
machines in flight:

`create/write/set-length -> reread -> CPU VERIFY -> file fsync -> rename-noreplace -> root-directory fsync`

Operations inside one chain must remain ordered and error-canceling; chains for
independent Containers may execute concurrently. Linux linked SQEs provide
kernel ordering only inside one submission phase, while unlinked chains may run
in parallel. The CPU VERIFY boundary requires a completion and resubmission; it
must not be hidden inside one blind linked chain. A plain queued write followed
by fsync is also insufficient because unordered SQEs can execute in parallel;
the kernel documentation requires links, a drain, or a completion barrier for
that dependency. Fsync and renameat operations are available in the current
io_uring interface.

Directory durability should be coalesced independently of Compression Regions
and Namespace Commit Groups. A publisher cohort may rename several individually
synced and verified Containers, issue one root-directory fsync, and complete
every member only after that shared barrier. Renames racing after the cohort
cut belong to the next barrier. This Sync Group is likely more valuable than
merely replacing blocking `pwrite` calls and must keep the existing
absent-or-complete crash oracle.

The initial cache implementation kept the proven synchronous `StorageIo` path
because the test host reported `kernel.io_uring_disabled=2`. The host was later
configured with `kernel.io_uring_disabled=0`, enabling the evidence-gated
publisher slice. The data tier can now use `IoUringStorageIo`: one shared
bounded ring and one worker serve all clones while preserving the blocking
`StorageIo` interface. This lets independent Container publishers overlap
without exposing ring ordering, pointer lifetime, or CQE handling to the
Container Repository.
Metadata, Commit WAL, Exact-Index publication, and CPU-only reduction stages
remain on their existing paths.

The Container Repository transfers one owned prepared image through the deep
`publish_owned_container` seam. The ring worker retains the complete Building
-> Body -> Sealed Header -> writer reread/VERIFY -> file fsync -> rename ->
root-directory-fsync state machine; the caller waits once for the final result
rather than once per phase. Positional writes, complete rereads, file syncs,
no-replace renames, and directory syncs use `io_uring`. Creation and length
changes remain synchronous worker-side control operations. Short reads and
writes are resubmitted until complete; a zero-length short write and premature
read EOF fail the complete immutable publication.

The default ring has 256 entries and a separate 256-MiB publication-buffer
budget. Owned publication charges one image, releases that image after its
final write, and only then allocates the same-sized reread buffer under the
unchanged lease. The intended image's complete Container BLAKE3 from the
writer envelope is retained and paired with the fully decoded reread,
Container ID, and generation before file sync. Thus a nominal 64-MiB workload
can keep four images admitted instead of one under two-image accounting,
without an unbounded writer-reread copy. Borrowed `write_at` remains available
for the generic compatibility seam, but its copied bytes are explicit
telemetry and normal Container publication must report zero. Worker- and
caller-written telemetry occupy separate cache-line-aligned records.

Complete Container verification at or above 1 MiB runs on one permanent,
bounded CPU verifier pool. The default worker count uses all effective CPUs
reported by `available_parallelism`; a configured nonzero override exists for
tests and machine profiles. Its job queue is capped by the ring-entry count and
every job continues to hold its publication-buffer lease. Smaller Containers
verify inline because measured channel and wakeup costs exceed their decode
work.

Verifier workers receive only owned reread bytes plus expected Container hash,
ID, and generation. File descriptors, phase state, replies, and durability
operations remain with the ring worker. A successful result is therefore a
capability to enter `FileSync`, not permission to acknowledge or publish.
`fsync`, rename, directory sync, and caller completion remain strictly after
the result. Failed VERIFY returns the existing integrity error and drops the
publication without issuing a file sync. Per-pool telemetry uses a separate
cache-line-aligned counter record; job dequeue locking occurs only between
whole-Container verification jobs, never inside decode loops.

Root sync callers form bounded cohorts. Once a root-sync request reaches the
worker it admits already queued callers for a short bounded interval, submits
one directory fsync, and releases exactly that captured cohort after its CQE.
Renames arriving after the cut cannot be acknowledged by that barrier and must
join a later cohort. This ADR originally allowed fallback to `FsStorageIo` when
ring setup was unavailable; ADR 0058 removes that fallback.

The implementation therefore satisfies these previously recorded gates:

- retain one shared bounded ring and fixed publisher worker set, not one ring
  or thread per Container;
- cap submitted-but-incomplete image bytes independently of the read cache and
  Dirty-DATA budget, including writer-reread buffers and CQ state;
- preserve the existing writer reread/VERIFY before file sync and the root
  directory sync as the publication commit point;
- cut explicit root-sync cohorts so one directory fsync can safely release many
  completed Containers without acknowledging a later racing rename;
- surface the first real CQE error and treat dependent cancellations as one
  failed, retryable immutable publication;
- report setup or required-opcode failure without weakening durability; and
- pass the existing fail-before/fail-after crash matrix with the same
  absent-or-complete recovery oracle before becoming the default.

The faultable `StorageIo` protocol is unchanged, so the existing exhaustive
fail-before/fail-after publication matrix remains the crash oracle. Actual-ring
tests additionally reopen and verify published Containers, exercise many
parallel publishers through one bounded ring, validate root-sync coalescing,
and validate setup failure. A power-cut-capable XFS harness remains a
separate hardware validation gate; process restart cannot emulate loss of an
unsynced directory entry.

The first shallow same-host benchmark found the Ring adapter 41% slower on root
XFS and 52% slower on the separate data XFS. The owned state machine removes
the extra payload copy and per-phase caller handoffs, but a repeated
1,000-by-128-KiB data-XFS run remains about 40% slower. A 50-by-63-MiB run is
also about 40% slower, while reducing measured peak RSS from 2.34 GiB to 1.51
GiB. Parallel verification removes most of that large-Container deficit. On a
fresh 50-by-63-MiB comparison, the pool reduced Ring wall time from the prior
8.006 s to 5.919 s; the simultaneous synchronous run took 5.530 s. Ring is now
about 7% slower rather than 40% slower, retains the 1.51-GiB measured RSS peak,
and exercised four verifier workers concurrently. Host Swap and `pswpout`
remained unchanged at zero. The 128-KiB workload stays inline; the latest
interleaved run still leaves Ring materially slower.

The prototype daemon initially defaulted to Ring with setup fallback so the
active publisher received end-to-end workload coverage. ADR 0058 later made
Ring mandatory. The benchmark and exact measurements are recorded in
[the Container publisher benchmark](../benchmarks/io-uring-container-publisher.md).

No io_uring operation is inserted between CPU-only reduction stages, and the
FUSE request path remains independent. The ring is justified only at XFS
syscall boundaries and only after a many-Container benchmark demonstrates
higher throughput or lower CPU per durable Container without violating commit
latency or memory reserve.

## Evidence

- Linux documents that `io_uring_disabled=2` rejects all new rings with
  `EPERM`: <https://docs.kernel.org/next/admin-guide/sysctl/kernel.html#io-uring-disabled>.
- Linked requests serialize only one dependency chain while independent chains
  remain concurrent: <https://man7.org/linux/man-pages/man7/io_uring_linked_requests.7.html>.
- Unlinked write and fsync requests are not ordered merely by submission:
  <https://man7.org/linux/man-pages/man2/io_uring_enter.2.html>.
- Fsync and renameat preparation interfaces:
  <https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html> and
  <https://man7.org/linux/man-pages/man3/io_uring_prep_renameat.3.html>.

## Consequences

The hot decoded cache can improve repeated and shared-base reads without
competing unboundedly with ingest. On a dedicated appliance with Swap disabled
at the cgroup, cache pressure becomes eviction or a verified XFS read, never a
Swap storm. The conservative all-entry purge sacrifices hit rate under pressure
to protect durability latency and can later become a NUMA-local proportional
shrink only if RSS and tail-latency measurements justify the extra state.

`io_uring` is an XFS publisher optimization, not a new crash protocol. High
Container fan-out is its intended workload. The original owned adapter met its
memory and correctness gates but did not beat the synchronous baseline. ADR
0058 replaces its batch worker and makes ring capability a startup requirement.
