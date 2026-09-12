# One application-owned read cache — 2026-09-12

Research for [ADR 0046](../adr/0046-bound-verified-read-cache-by-live-memory-headroom.md).
Sources below were checked online on 12 September 2026. External facts are
separated from the proposed FastDup design; the explicitly attributed truncate
observation below is local qualification evidence, not a completed-implementation
claim. The earlier [Direct I/O research](linux-direct-io-guidance-2026-09-01.md)
describes a selective buffered/direct policy. Its buffered exceptions do not
satisfy the newly requested application-owned architecture.

## What the requirement can mean on Linux

Linux defines Direct I/O as file I/O that bypasses the page cache. Its `iomap`
implementation still flushes dirty cached ranges before a direct read and can
request buffered fallback in some situations. A successful `open(O_DIRECT)`
alone therefore does not qualify every filesystem, file type and operation.
[Linux iomap operations](https://docs.kernel.org/filesystems/iomap/operations.html#direct-i-o)

The XFS/FUSE target is **no Linux file-content cache tier for FastDup repository
I/O or its exposed FUSE files**; arbitrary-length writes still face the truncate
limitation below. VFS uses dentries and
in-memory inodes to resolve names and maintain open files; opening a file holds
these structures in use. Removing all kernel metadata caches would require a
different storage boundary and would not follow from `O_DIRECT`. Repeated
application `open`, `stat`, directory and extent-discovery work can still cause
filesystem metadata I/O even when file-content caching is disabled.
[Linux VFS overview](https://docs.kernel.org/filesystems/vfs.html#directory-entry-cache-dcache)

Device caches also remain below the application. Linux uses flush and Force
Unit Access mechanisms to obtain persistence from devices with volatile write
caches. An independent direct read is independent of FastDup RAM caching, not a
promise that every read physically reaches magnetic media or NAND.
[Linux volatile write-cache control](https://docs.kernel.org/block/writeback_cache_control.html)

Neither advisory eviction nor drop-behind fulfills this contract:

| Mechanism | Why it is insufficient |
| --- | --- |
| `POSIX_FADV_DONTNEED` / `NOREUSE` | Advisory page-cache behavior; partial-page discard requests are ignored and dirty pages may remain. [Linux `posix_fadvise(2)`](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html) |
| `RWF_DONTCACHE` | Since Linux 6.14, prunes newly instantiated page-cache content after I/O; pre-existing cached ranges remain. It still uses buffered I/O. [Linux `preadv2(2)`](https://man7.org/linux/man-pages/man2/preadv2.2.html) |
| `/proc/sys/vm/drop_caches` | Drops clean cached objects after the fact; the kernel explicitly says it is not a mechanism to control cache growth. [Linux VM sysctls](https://docs.kernel.org/admin-guide/sysctl/vm.html#drop-caches) |
| File-backed `mmap` | Conflicts with the no-page-cache target; Linux advises against mixing mappings or buffered I/O with Direct I/O on the same files. [Linux `open(2)`](https://man7.org/linux/man-pages/man2/open.2.html) |

## One cache engine, with typed values

RocksDB demonstrates sharing the same cache instance across database instances
and storing index/filter blocks beside DATA blocks. It also documents the
memory surprises caused by independently allocated filters, indexes and pinned
iterator blocks. Sharing only nominal limits does not cover these allocations.
[RocksDB memory usage](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB)

**Proposed FastDup structure:** create one appliance-owned cache engine shared
by the Metadata, DATA and Small-file Tiers, foreground readers, reduction and
maintenance. It owns one key directory, allocation accounting, admission,
replacement, invalidation and miss coordination. Shards are an implementation
detail of this engine. Object classes remain labels for typing, priority and
telemetry; they do not own independent caches, capacity shares or eviction
controllers. A budget broker over the present independent implementations is a
migration state, not this end state.

| Read-saving value | Required identity and interpretation |
| --- | --- |
| Verified logical Chunk, including Base/Dictionary-dependent reconstruction | Repository/open epoch, Chunk ID, logical length and representation version; retain physical proof provenance separately |
| Container envelope/descriptor and bounded Encoding Record bytes | Immutable Container identity, seal/generation binding, physical length and checked range |
| Exact/Similarity pages, bucket fences and rebuildable membership filters | Immutable Run identity/hash, format, page/filter coordinates; candidates remain acceleration |
| Namespace, inode and Manifest Metadata Objects; decoded node views | Metadata Object content identity and type; live root/generation bindings stay caller-owned |
| Dictionary objects and other decoding dependencies | Full immutable object identity, expected length and verified type |
| Historical DATA proofs | Exact physical provenance and verification class; never a new durable certificate |
| Cached lengths, open-object handles or namespace lookup results | Immutable object/lifetime or explicit mutation epoch; charge retained handles and invalidate with mutation |

Use type-specific lookup/verification adapters over the engine rather than an
untyped byte API that can accidentally return unverified bytes as a verified
Chunk. Encoded and decoded forms may coexist only as explicitly charged
representations of the same managed object. Shared views charge backing
capacity once; independently allocated decoded projections still cost memory.
Do not introduce a raw-block cache underneath the existing independent caches
and call their sum unified.

All potential storage work needs a declared route through one read boundary:
range reads, whole-file reads, structure/envelope reads, object-length probes,
dictionary/Base fetches, index activation, compaction, metadata traversal,
recovery checkpoints, temporary sort/spool readers and physical inventory.
An operation can explicitly bypass admission or require fresh storage; it must
not silently open its own buffered descriptor or create a mapping. Directory
enumeration and mutable control state are managed operations, not immutable
content-cache entries. This coverage list is a design checklist, not a claim
that these paths already share one implementation.

## Replacement, scans and concurrent misses

S3-FIFO uses a small probation queue, a main queue and a ghost queue, with small
reuse counters instead of moving every hit to the front of an LRU list. Its
authors' trace evaluation supports early rejection of one-hit objects and
reduced hit-path contention; it is not evidence of a particular FastDup speedup.
[S3-FIFO authors](https://s3fifo.com/blog/2023/08/01/fifo-queues-are-all-you-need-for-cache-eviction/)

**Recommendation:** use one bounded, byte-weighted FIFO/CLOCK-derived engine
with probation and decayed reuse evidence. Charge ghost/directory capacity too.
Preserve FastDup's DATA-versus-Metadata cost preference as a policy weight,
then qualify its effect on metadata-heavy workloads. It must not create fixed
per-class partitions or allow a sequential audit to evict every useful DATA
entry. Shard-local hit synchronization is appropriate, but bytes must remain
available across object classes and shards rather than stranded by fixed
quotas. RocksDB explicitly documents both the contention motivation for
sharding and the capacity imbalance risk when shards cannot share capacity.
[RocksDB block cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)

Separate these read intents in the common engine:

| Intent | Cache lookup | Retain new result | Storage requirement |
| --- | --- | --- | --- |
| Demand/reusable maintenance, including online GC graph reads and relocation | Yes | According to common admission policy | Direct I/O on miss |
| One-pass scan/compaction | May reuse admissible existing bytes | Bounded probation or explicit no-admission | Direct I/O; bounded coalescing/readahead |
| Independent durability verification | No shared-cache hit or shared in-flight result | No shared read acceleration within the proof | Fresh direct storage reads |

These are FastDup policy choices. Application readahead becomes necessary to
replace useful kernel readahead for sequential work; RocksDB calls out this
need for direct compaction reads. Readahead needs explicit byte, concurrency
and cancellation limits, with demand I/O taking priority.
[RocksDB I/O guide](https://github.com/facebook/rocksdb/wiki/IO)

Use bounded per-key single-flight loading for compatible misses. Rust's
`quick_cache` documents a guard pattern where callers for the same key wait
until the loader inserts a value or releases its guard.
[quick_cache API](https://docs.rs/quick_cache/latest/quick_cache/sync/struct.Cache.html#method.get_value_or_guard_async)
For FastDup, the guard must also handle failed verification, cancellation,
pressure rejection and invalidation. Do not hold cache locks over I/O or
codec work. Bound waiters and outstanding loads, prevent recursive dependency
self-waits, and never join an independent proof to a normal cached load.

## Direct I/O boundary and small files

Alignment is filesystem/kernel/file dependent. Misaligned `O_DIRECT` requests
may fail or silently fall back to buffered I/O. `O_DIRECT` does not itself
provide the synchronized-write guarantees of `O_SYNC`.
[Linux `open(2)`](https://man7.org/linux/man-pages/man2/open.2.html)
Prefer `statx(STATX_DIOALIGN)` and inspect the returned mask and memory/offset
alignment fields. XFS also exposes read-specific alignment through
`STATX_DIO_READ_ALIGN` on supporting kernels; missing fields are not evidence
of unrestricted I/O. Qualify the supported older-XFS fallback explicitly.
[Linux `statx(2)`](https://man7.org/linux/man-pages/man2/statx.2.html)

**Recommended reader:** align the start downward and end upward, read into
bounded initialized aligned storage, then return only the requested verified
range. Keep the allocation and descriptor alive until I/O completion. Small
immutable files do not inherently need on-disk padding **to be read directly**:
Linux iomap clamps read completion to the known file size. Accept an EOF-short
result only when
it covers every requested logical byte; any earlier short read/truncation must
fail verification. This conclusion follows from iomap's EOF handling and must
be exercised on FastDup's actual XFS/kernel combination.
[Linux iomap direct-read implementation](https://github.com/torvalds/linux/blob/master/fs/iomap/direct-io.c)

The policy must cover writes too. Buffered publications, WAL, selector and
temporary-file writes still populate a kernel content cache even if later
reads use Direct I/O. RocksDB's Direct I/O guide explicitly exempts WAL and
MANIFEST; copying those exceptions would violate FastDup's stricter target.
[RocksDB Direct I/O](https://github.com/facebook/rocksdb/wiki/Direct-IO#notes)

For immutable publication, aligned writes plus restoring the canonical logical
length do not by themselves satisfy the no-page-cache requirement. Mutable small
files need a crash-safe aligned update strategy; do not silently change their
format or overwrite a committed prefix merely to satisfy alignment. These
writer changes require their own writer/recovery/scrub fault cases. Maintain
file synchronization and directory synchronization: a synced file alone does
not make its containing directory entry durable.
[Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)

### XFS partial-block truncation is a remaining writer boundary

The implementation task reported this local observation on 12 September 2026:
an aligned 4,096-byte `O_DIRECT` write, followed by `ftruncate(73)` and `fsync`,
left 4,096 bytes resident according to `fincore` on XFS. The workspace kernel
reports `6.12.0-211.50.1.el10_2.x86_64`. This research subtask did not rerun that
measurement. It contradicts any claim that aligned-write-then-truncate alone
guarantees zero file-content page-cache residency.

The upstream Linux 6.12 path explains the result. `xfs_setattr_size` calls
`xfs_truncate_page` on downward truncation before updating the size. It has no
`O_DIRECT` branch to suppress this step. Subsequent synchronization ensures
zeroing reaches storage; it does not promise eviction of the surviving EOF
page. [XFS size changes](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/xfs/xfs_iops.c)

For an ordinary non-DAX inode, `xfs_truncate_page` selects
`iomap_truncate_page` with buffered write operations; its alternative is DAX.
[XFS truncate dispatch](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/xfs/xfs_iomap.c)
Iomap skips work at a filesystem-block boundary. Otherwise it zeroes from
the new EOF to the block end. A written extent takes the folio write-begin,
zero and write-end path, which can allocate/read/dirty a page-cache folio.
Already clean holes/unwritten extents can skip zeroing, but that does not cover
an arbitrary nonzero last block. Pre-zeroing the tail by a direct write does
not turn its extent back into an unwritten mapping.
[Iomap truncate and zero implementation](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/iomap/buffered-io.c)
Current upstream iomap still implements partial-block truncation through
page-cache zeroing. [Iomap truncation documentation](https://docs.kernel.org/filesystems/iomap/operations.html#truncation)

**Narrow conclusion:** no general supported userspace XFS operation was found
that preserves arbitrary existing file lengths and arbitrary last-block data
while guaranteeing that partial-block truncation never creates content-cache
pages. `FALLOC_FL_KEEP_SIZE` concerns allocation, not a write that preserves
EOF; hole punching and zero ranges must handle partial blocks, and collapse
range cannot reach EOF.
[Linux `fallocate(2)`](https://man7.org/linux/man-pages/man2/fallocate.2.html)
Reflink is not a general unaligned-tail escape: disk filesystems impose block
alignment on clone offsets and lengths.
[Linux `FICLONERANGE`](https://man7.org/linux/man-pages/man2/ioctl_ficlonerange.2.html)
XFS range exchange also cannot transplant a prefix of an aligned donor into an
arbitrary smaller EOF: ordinary ranges must fit both files, partial EOF blocks
cannot move into the middle of the other file, and exchange-to-EOF carries
the donor's size.
[XFS exchange validation](https://raw.githubusercontent.com/torvalds/linux/master/fs/xfs/xfs_exchrange.c)
DAX is a supported different path but requires memory-like, CPU-byte-accessible
storage. Merely setting an XFS flag on ordinary NVMe/HDD storage cannot enable
it. [Linux DAX](https://docs.kernel.org/filesystems/dax.html)

For the strict target, a promising separately qualified design is an aligned
physical backend envelope/container holding a verified logical length, so no
partial-block truncate is needed. That changes durable storage representation
and requires an explicit migration with writer, reader/recovery and scrub
coverage. Until such a design or another supported boundary is qualified,
retain the truncate issue as an open migration gap. `fadvise`, cache dropping,
or calling the EOF folio merely metadata would not close it.

Retaining already verified publication output can prevent immediate rereads,
but only after FastDup's publication protocol permits installation and under
the common admission policy. RocksDB documents this advantage and the cache
pollution risk of indiscriminately warming compaction output.
[RocksDB publication cache options](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table.h)

## FUSE and SMB are separate boundaries

FUSE `FOPEN_DIRECT_IO` bypasses the FUSE file-content page cache for both reads
and writes and disables kernel readahead. Shared mmap is disabled by default;
do not enable `FUSE_DIRECT_IO_ALLOW_MMAP` or writeback caching as a hidden
replacement tier. Backend `O_DIRECT` does not select this frontend mode.
[Linux FUSE I/O modes](https://docs.kernel.org/filesystems/fuse/fuse-io.html)

For an application-controlled FUSE metadata policy, return zero entry,
attribute and negative lookup TTLs, and keep `cache_readdir` disabled. These
are separate controls from file-data Direct I/O; they do not remove the VFS
objects needed to maintain the mount and open handles. Qualify the extra
lookup traffic against FastDup's common metadata cache.
[libfuse configuration](https://libfuse.github.io/doxygen/structfuse__config.html),
[libfuse file-info fields](https://libfuse.github.io/doxygen/structfuse__file__info.html)

SMB leases/oplocks permit remote clients to cache data and handle operations;
SMB3 directory leases additionally permit directory caching. They are outside
the daemon's local Linux page-cache boundary. The deployment contract must
name this boundary. If application-controlled caching includes remote
clients, qualify a separate Samba/client policy; changing FastDup's backend
flags cannot guarantee it.
[Samba configuration reference](https://www.samba.org/samba/docs/current/man-html/smb.conf.5.html#SMB2LEASES)

## Memory ownership and pressure

cgroup v2 accounts anonymous memory, file cache and major kernel allocations.
`memory.current` includes descendants; all controller limits are hierarchical.
`memory.high` causes throttling/reclaim rather than imposing a hard ceiling;
`memory.max` can cause OOM, and `memory.swap.max=0` prohibits anonymous memory
being swapped out in that cgroup. Host free memory alone is therefore not the
available budget for the daemon.
[Linux cgroup v2 memory controller](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory)

**Recommendation:** retain ADR 0046's conservative live-headroom target, but
make a common allocation owner account reclaimable residency, reader-retained
owners, in-flight aligned buffers, decode/compression work, speculative reads,
lookup/ghost metadata and idle buffer capacity. Reserve bytes before starting
new work. Cache eviction removes reuse eligibility; it does not free the bytes
of a reader-retained allocation. Transfer that charge to the live-reader class
without a gap or double charge, and release it only with the final owner. Cache
shrink cannot promise immediate RSS release or reclaim pinned generation state.

Dirty DATA and pinned Generation Proof Sets remain non-evictable working state
with bounded admission, not ordinary read-cache entries. A shared memory
controller may account them without changing their durability role. RocksDB's
write-buffer manager supplies a precedent for charging active write memory
against the same overall allowance as read-cache memory.
[RocksDB write-buffer manager](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager)

Use effective ancestor headroom and the existing reserve, revoke speculative
admission early, and respond to failed pressure samples conservatively. Add
memory/IO PSI and cgroup event deltas to diagnose pressure; PSI measures time
lost to contention and supports threshold-triggered notification. Hysteresis
and growth limits need FastDup qualification rather than copied constants.
[Linux PSI](https://docs.kernel.org/accounting/psi.html)

## Integrity and completion evidence

The common engine does not unify verification authority. A verified DATA
value, an Exact candidate, a Metadata Object and a historical physical proof
retain their distinct meanings from [CONTEXT.md](../../CONTEXT.md). Warm
normal reads may reuse immutable verified bytes, while independent recovery,
scrub and explicit stored-byte publication validation must read the current
durable object. Under the clarified task policy, online GC also uses the unified
cache, including graph reads and relocation; entering online GC is not a reason
to bypass every cache. Its generation, liveness, dependency and pin checks still
apply. A step specifically establishing fresh physical integrity must read that
physical object, because a process-local key cannot prove that it still exists
or remains uncorrupted. Verification meanings follow from FastDup's own
[fresh recovery proof](../adr/0037-separate-structural-recovery-from-current-data-proof.md)
and [ADR 0046](../adr/0046-bound-verified-read-cache-by-live-memory-headroom.md).

Required qualification before claiming the target is implemented:

- Every declared read source has hit, miss, explicit-bypass and object-class
  attribution; immutable content identity and mutation/retirement tests cover
  reuse across namespace, Manifest, index and DATA consumers.
- Warm-cache corruption, missing dependency, failed publication and independent
  recovery/scrub cases still fail from fresh storage. Invalidation races cannot
  republish a retired entry or lend an old proof to a new object.
- Alignment tests cover empty and sub-sector files, nonaligned EOF/ranges,
  exact-page boundaries, short reads, cancellation and unsupported Direct I/O.
  Publication/WAL/control migration keeps existing crash fault boundaries.
- Pressure tests keep reader-owned buffers and concurrent misses alive while
  shrinking, verify allocation charges and load bounds, and exercise class and
  shard imbalance plus scan/demand contention.
- Production-XFS tracing checks actual backend descriptor modes, file-content
  page-cache insertions and mappings, FUSE open flags/TTLs, and block-level
  reads. Logical cache misses are reported separately from physical I/O;
  unavoidable filesystem metadata work remains visible.
- Before/after workloads cover cold reads, hot restore, metadata-heavy traversal,
  reduction-base reuse, compaction/scrub plus ingest, and memory pressure. Report
  throughput, p99/max latency, system/user CPU, RSS/cgroup usage, cache-owned and
  reader-retained bytes, aligned-read amplification and physical disk traffic.

The architecture should be judged by these coverage and ownership properties,
not by whether existing cache structs have acquired a common name.
