---
status: accepted
---

# Bound the verified read cache by live memory headroom

fastdup shares decoded Chunk bytes only after the complete stored encoding,
logical length, and BLAKE3 Chunk identity have been verified. The cache is
attached to installed Manifest readers, not recovery or scrub, so it can never
become DATA authority or hide a missing durable dependency.

A read response may retain one verified allocation across several adjacent
DATA/DATA_SLICE extents. Its `VerifiedReadView` has only an owner and checked
byte range, never an invented Chunk identity. Joining requires the same live
owner and exactly adjoining source ranges. Sparse extents, RAW header/padding
gaps, reordered ranges and different owners use bounded response assembly.
Admission grouping uses an ephemeral backing identity while payload owners
remain retained; large batches may index it after enough distinct owners have
been observed. This index neither persists pointers nor changes cache charges.

The same live-headroom policy also governs a separate repository-wide cache of
independently verified immutable Exact Index pages. This cache retains only a
hot subset of the persistent index, never the complete Chunk map. A hit removes
Metadata-Tier page I/O but still yields only unverified Location candidates;
the selected immutable Container record and Chunk identity must be verified
before reuse. Exact pages use lazy cache-line-separated shards with local FIFO
replacement rather than a global LRU.

The verified DATA cache is four-way set associative and sharded on cache-line-separated
locks. There is no process-global pointer-heavy LRU. A hit touches one shard
and at most four entries. A miss performs ordinary verified Container I/O
without holding a cache lock; only the final immutable admission and exact byte
accounting are serialized. Concurrent misses may therefore repeat work, but
they cannot duplicate one resident identity or exceed the current target.

A full payload byte budget must not permanently freeze admission merely because
an incoming key maps to an empty way or a victim that shares its backing with
other entries. Admission may reclaim entries across sets using one persistent
round-robin cursor under the existing admission lock. Each decoded allocation
group probes at most 256 slots; a later admission continues where the previous
one stopped. Only one shard lock is held at a time, and the incoming group's
already admitted views are protected. The allocation charge is released only
when its last cache view disappears; outstanding verified reader views remain
valid through their own immutable ownership.

This is bounded replacement, not a global LRU: hits keep their existing single
shard lookup and acquire no new global lock or replacement metadata. Large,
sparse geometries and widely shared backings can require multiple admission
attempts before enough bytes are reclaimed. Exhausting the probe budget skips
admission and uses the ordinary verified read result, without additional I/O,
weakened verification, or exceeding the live byte target. Memory-pressure and
process-Swap purges still take precedence. Workload-transition regression tests
must cover a full byte budget with free set ways, multi-view allocation
accounting, cursor progress, and concurrent reads/admissions/pressure updates.

## Shared adaptive budget (2026-09-07)

System-configured caches share one broker instead of independent fixed RAM
fractions. The operating ceiling is 92% of effective host/cgroup memory: 8%
remains available for concurrent working-memory and kernel demand. The sampled
cache budget is current conservatively charged cache residency plus available
memory minus that reserve, capped at 92% of the effective limit. Already
resident cache bytes are included so a full useful cache does not mistakenly
shrink itself just because its allocation lowered `MemAvailable`. Dirty DATA,
Ingest Lanes, codec workers, pinned Generation Proof Sets and active immutable
Run views consume working memory outside these reclaimable leases and reduce
available headroom. This is an operating target sampled every 250 ms, not a
kernel guarantee against an external allocation between samples.

One broker accounts nonoverlapping byte leases for verified DATA payloads,
historical DATA proofs, Container descriptors, Exact pages and Similarity
pages. A cache reports cumulative hits, avoided logical read bytes, misses,
evictions and resident bytes on its existing cold refresh path. Recent avoided
read bytes per resident byte determine benefit; an exponentially decayed window
adapts when a workload changes. DATA-fallback benefit receives a 16x weight
relative to Metadata-fallback benefit. This is an explicit priority multiplier,
not a per-cache byte reservation. Payload/proof hits count actual logical
length; descriptor hits represent the two envelope pages and index hits one
4-KiB page. These values estimate avoided fallback work and do not claim measured
physical I/O on every miss.

A bounded exploration share (1/16 of the current distributable budget across
pools) lets a cold or previously displaced cache demonstrate reuse. Misses and
replacement demand permit gradual growth; a pool already serving its working
set cannot reserve unlimited unused space. Weighted allocation redistributes
unclaimed capacity after each pool's current demand is satisfied. Activity
ages out, so an idle pool loses priority. Counter snapshots and broker locks
stay off the normal cache-hit path; hits retain their existing local locks.

Shrinking sets a desired target first. The old lease remains charged until
the donor has stopped admission and completed local eviction under its own
admission lock. Partial shrink retains useful entries; historical proof arenas
compact survivors so vacant slots do not retain a returned lease. Only then can
another pool borrow those bytes. Outstanding
immutable reader views remain valid and any memory they still own remains part
of sampled process working memory. Failed pressure samples or Process Swap
revoke payload admission; fixed lookup metadata stays conservatively charged.
A pool's addressable geometry limits indexing, not its desired RAM allocation.
Explicit externally governed/test constructors retain deterministic policies;
production constructors all register with the shared broker.

Exact and Similarity pages use lazy cache-line-separated sharded maps and FIFO
replacement. Their former 256-MiB and 512-page limits do not constrain system
allocations. Maps grow only on actual admission, are conservatively charged
alongside decoded pages, and release excess capacity when a target shrinks.
The page budget includes Similarity bucket-fence entries. A hit remains a
verified immutable page lookup and never authorizes DATA reuse by itself.

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

Fixed lookup metadata and payload together remain inside a granted cache
lease. Verified DATA reclamation preserves the bounded admission scan and
uses a complete cold-path scan when a donor must reach a lower target.
Process Swap revokes payload leases and clears entries; host/cgroup Swap from
other workloads remains separate telemetry. Reads continue through normal
verification after every miss. Independently held verified reader allocations
are not cache entries and are accounted through available process memory.

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

The resident descriptor target follows its DATA-priority shared lease, up to
the existing addressable Container-count limit. Shards allocate on demand;
allocation failure rejects only cache admission. Its fixed shard directory and
conservative per-entry charges participate in the same broker as payloads and
index pages.

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

## Verified Manifest nodes (9 September 2026)

A repository-local decoded Manifest-node cache joins the shared broker as
`manifestNodes`, with Metadata fallback priority. It has no fixed preferred RAM
allocation. Its lazy sharded lookup storage, decoded nodes and replacement
metadata are charged against the same leases and 92% operating ceiling. Normal
hits touch one local shard; admission and pressure changes serialize separately.
Concurrent misses perform verification without holding cache locks and converge
to one charged entry. Eviction releases ownership and map/FIFO capacity before
returning budget. Outstanding immutable Arc views remain working memory.

Only installed Manifest readers use this cache. Each Generation Repository owns
one distinct instance; clones share it and another repository/open gets a new
instance. Admission verifies the expected Metadata Object ID, envelope CRC and
complete node structure. Cache hits reuse that immutable decoded value while the
reader retains its existing Metadata Root Pin. Every traversal still checks the
parent's level, length and allocated-byte summaries. A cached node never pins a
root, proves reachability, or authorizes DATA deletion or publication by itself.
Recovery, writer/successor validation, GC and offline scrub retain independent
storage verification and never consult this cache. Newly serialized target
metadata still receives its own CRC and content identity.

Clone preparation obtains its extents and tests for HOLEs in one verified range
walk; a preliminary allocation walk would only repeat the same work. DATA and
DATA_SLICE retain the original full Chunk identity without rereading or hashing
payload merely to create a reference. The full-range partition, bounds and
existing dependency proof requirements remain unchanged.

Tests cover a tree with several leaves, repeated/adjacent range use, allocation
queries, expected-root mismatch on a hit, invalid cold bytes, concurrent misses,
isolation between cache instances, and owned views surviving a complete purge.
The release-mode A/B runs the same tree traversal with admission disabled/enabled;
see the [qualification record](../testing/clone-optimization-2026-09-09.md).

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


## Compact resident keys (2026-09-06)

A resident entry compares its verified payload's full Chunk ID and logical length
directly, rather than storing the same key a second time. Backing ownership,
shared allocation charging, sharding and replacement remain unchanged. Fixed
geometry continues to derive from the actual CacheSet size. On the measured
x86-64 build a set shrinks from 904 to 744 bytes; this is a metadata saving,
not an equivalent reduction in total process RSS.

Independent Record provenance now stores only the physical Record coordinates
used by candidate matching. The dependency ID has already been proven zero;
per-Chunk coordinates remain on the payload rather than being duplicated in
the first Location. A positive Record length supplies the optional proof's
in-memory niche. This reduces the measured x86-64 payload from 176 to 128 bytes
and its four-way CacheSet from 744 to 552 bytes without another allocation or
pointer lookup. It changes no durable format or independent-read verification.
Cache geometry continues to charge its actual type sizes.

Decoded-group admission consumes verified payloads directly. It derives full
Chunk identity and logical length from each payload instead of expanding them
into a temporary keyed vector and comparing the copied values back again.
Shared backing identity/size checks, admission serialization, shard locks,
victim order, pressure behavior and allocation charging remain unchanged.

## Independently compressed Verified Read entries (9 September 2026)

Verified Read now keeps decoded and independently LZ4-compressed representations
inside the same sharded cache and the same DATA-priority broker lease. There is
no fixed RAM split. Historical Proofs are unchanged. The compressed RAM format
is process-local and never written into a Container or used by recovery/scrub.

New verified admissions trial fast LZ4 on complete logical Chunks, including
fully reconstructed dependent Chunks. A compact entry has no external Base,
Dictionary, disk address to follow, or backend callback. Its original verified
independent-Location provenance is retained separately from its RAM encoding.
Compression is admitted only if compact bytes plus owner/allocation overhead
are smaller than the represented payload and the complete admission costs less
than the original shared allocation. A mixed group stays decoded if one sibling
cannot profitably compress: a remaining raw sibling must not pin a complete
Record while additional compressed copies consume RAM. Codec workspace
exhaustion also retains the original decoded group without delaying admission.

Compressed entries keep only weak references to live decoded reader buffers.
These preserve existing zero-copy slices while a caller already owns the
original Record or a recent decode, without keeping that allocation resident in
the cache. A compressed hit with no live view reconstructs exactly one bounded
Chunk, checks its full logical length and BLAKE3 identity, and only then creates
a verified payload. Readers of the same entry share the decode while its owned
result remains alive. A damaged cache copy is removed and becomes an ordinary
verified storage miss; it cannot repeatedly poison replacement admission or
create verified bytes. Recovery, full verification and scrub remain independent.

Hot promotion is optional and nonblocking. Reuse evidence ages after roughly one
resident-entry count of new admission groups. At least two recent hits plus
measured decode-and-verification nanoseconds per additional resident byte must
justify decoded retention relative to a smoothed portfolio of observed decodes.
This is a bounded heuristic, not a calibrated probability or a fixed size quota.
Promotion uses only free capacity in the current lease: saving CPU must not
evict another entry that prevents DATA reads. Under admission pressure one old,
sole-owned decoded victim near a separate bounded demotion cursor may be recompressed
outside cache locks. Shared decoded allocations retain their existing ownership
rules; normal bounded replacement handles entries that cannot be demoted.

Compression/decompression never runs with a shard or admission lock held.
Compression is optional and does not wait for workspace. A RAM hit may wait for
bounded codec workspace, never turn into disk I/O merely because codecs are
busy. The conservative workspace bound derives from effective memory reserve
and available CPUs, with room for at least one maximum-Chunk operation. It
charges prepared compact output, bounded source/output buffers and codec scratch;
returned immutable views are ordinary reader working memory, as on a cold read.
Outstanding views remain valid after eviction, lease shrink, or process-Swap
admission closure. The existing sampled 92% ceiling remains an operating target,
not a kernel guarantee against unrelated concurrent allocations.

Resident bytes charge each surviving decoded backing once or each independent
compact owner once. Promotion, demotion, invalidation and eviction update both
representations under the admission lock before returning a broker lease.
Telemetry reports decoded RAM, compact RAM including owner overhead, logical
bytes represented by compact entries, and workspace current/peak/limit gauges.
Separate lifetime counters expose compact hits, actual decodes, compression and
decode/identity-check time, promotions, demotions, bypasses and invalid copies.
The UI keeps these sample-time gauges separate from its five-minute hit-rate
window and labels codec cost counters as lifetime values. Missing fields in old
runtime/history samples remain unavailable.

Validation and performance limits are recorded in
[compressed read cache qualification](../testing/compressed-read-cache-2026-09-09.md).


## Allocator retention is separate from cache residency (12 September 2026)

Freed cache/working buffers can remain in glibc arenas after application owners
release them. They do not count as resident cache entries, but their resident
pages still reduce OS headroom. Increasing cache leases based on virtual free
allocator counters would overpromise memory and is not permitted.

One daemon-owned background worker instead returns sufficiently large resident
free-arena slack to the OS. It samples anonymous RSS and glibc counters outside
hot paths, and trims only when both free blocks and resident slack exceed the
shared 8% operating reserve. It runs no more than once per 30 seconds and backs
off according to measured housekeeping cost. It cannot invalidate live cache
views or touch durable data. The broker keeps using actual OS availability.
Allocator gauges remain separate from cache gauges because free blocks may
already have been discarded from RSS. GNU/Linux provides the narrow allocator
hooks; unsupported allocator targets report no measurement.

The [A/B qualification](../benchmarks/allocator-reclaim-2026-09-12.md) records
retention reduction, added work and why neither per-request trim nor a universal
low mmap threshold is suitable for this workload.
