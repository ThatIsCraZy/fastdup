# ZFS as a physical backend for fastdup Containers

Status: research note, not an accepted architecture decision  
Last reviewed: 2026-09-01  
OpenZFS source revision: `58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7`

## Decision summary

fastdup **can** place its immutable Container files on an ordinary ZFS
filesystem without changing their durable format. That is the only one of the
three evaluated forms that keeps fastdup behind a supported, stable userspace
boundary and preserves its current Rust/FUSE architecture.

A raw ZVOL is also a supported boundary, but it removes the file namespace,
atomic no-replace publication, and directory durability that `StorageIo`
currently consumes. fastdup would have to add a raw allocator, object map,
free-space recovery, discard policy, and block-addressed publication protocol.
ZFS would still see only opaque fixed-size blocks. This is more work and does
not expose DMU transactions to fastdup, so it has no clear architectural
advantage over ordinary ZFS files.

Direct DMU/ZAP/objset integration is technically possible in three
version-coupled forms: embedding the unstable userspace `libzpool` SPA/DMU
implementation and giving the process exclusive pool ownership; consuming
OpenZFS' exported internal kernel interfaces, as Lustre does; or forking
OpenZFS and adding a new UAPI. It is **not** an available stable userspace API
and **not** a stable Linux/OpenZFS kernel ABI. Each form replaces fastdup's
supported syscall boundary with matching OpenZFS builds, custom crash and sync
handling, and matching operational tools. It should not be a product direction
unless the interface is designed and maintained upstream with OpenZFS.

Therefore:

1. keep XFS as the accepted production profile;
2. if ZFS is desired for vdev redundancy and self-healing, prototype **normal
   ZFS datasets containing ordinary fastdup files**;
3. do not build a ZVOL backend without an independent reason to own a raw
   allocator; and
4. reject direct DMU integration as a private-ABI dependency for the current
   product.

## What is actually supported

OpenZFS explicitly says that administrative programs use `libzfs` and
`libzfs_core`, for which its build maintains ABI across point releases. The
`libzfs_core` implementation still classifies its interface as “Evolving (not
Committed)”, so this must not be generalized into an unlimited major-version
compatibility promise. The same build source describes `libzpool` as a
userspace build of the DMU and SPA layers for `zdb` and `ztest`, and says its
interfaces may change at any time.
[`lib/Makefile.am`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/lib/Makefile.am)
[`lib/libzfs_core/libzfs_core.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/lib/libzfs_core/libzfs_core.c)

The stable `libzfs_core` API can create only filesystem and ZVOL datasets and
offers administrative operations such as snapshot, send/receive, sync, wait,
and scrub. It exposes no API for allocating arbitrary DMU objects or issuing
`dmu_read`, `dmu_write`, or ZAP updates.
[`include/libzfs_core.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/include/libzfs_core.h)

The internal objset enum contains `DMU_OST_OTHER`, but labels it “For testing
only”; `ztest` is its production-tree user. Standard `libzfs` rejects such a
dataset handle, and standard receive rejects a stream whose objset is neither
ZFS nor ZVOL. DMU does provide generic data/metadata/ZAP *object* types, but
that does not supply a supported product objset or management contract. This is
evidence of implementation feasibility, not a supported application object
store.
[`include/sys/fs/zfs.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/include/sys/fs/zfs.h),
[`include/sys/dmu.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/include/sys/dmu.h),
[`lib/libzfs/libzfs_dataset.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/lib/libzfs/libzfs_dataset.c),
[`lib/libzfs/libzfs_sendrecv.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/lib/libzfs/libzfs_sendrecv.c),
[`cmd/ztest.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/cmd/ztest.c)

OpenZFS exports many DMU, ZAP, objset, ZIL, and transaction symbols from its
kernel module. Some comments explicitly preserve hooks for the Lustre ZFS OSD.
That makes an external kernel consumer possible, but OpenZFS does not promise a
stable kernel ABI for those symbols. Linux itself states that it has neither a
stable in-kernel source interface nor a stable binary kernel interface; the
stable boundary is the syscall/UAPI boundary.
[`module/zfs/dmu.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/module/zfs/dmu.c),
[`module/zfs/dmu_tx.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/module/zfs/dmu_tx.c),
[`module/zfs/zap.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/module/zfs/zap.c),
[Linux in-kernel API policy](https://docs.kernel.org/process/stable-api-nonsense.html)

The OpenZFS tree is predominantly CDDL-licensed. Any linked or derived kernel
design needs a specific distribution and licensing review; this note makes no
legal compatibility conclusion. OpenZFS and Linux both direct legal questions
to the applicable license terms or counsel.
[`README.md`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/README.md),
[Linux licensing rules](https://docs.kernel.org/process/license-rules.html)

## Three implementation forms

| Concern | Ordinary ZFS filesystem | ZVOL / raw block | Direct DMU/ZAP/objset |
| --- | --- | --- | --- |
| Product boundary | Stable POSIX syscall interface; optional point-release-ABI-managed `libzfs_core` administration | Stable OS block-device interface at `/dev/zvol/...` | Unstable `libzpool`, exported but unstable OpenZFS kernel interfaces, or a new custom ioctl/UAPI |
| fastdup change | Replace/qualify `FsStorageIo` root; Container format and recovery remain | Replace named files with allocator, object table, free map, raw WAL, discard and recovery | Embed an entire private userspace SPA with exclusive device ownership, or move storage operations into a kernel module/fork and add a bridge; ordinary Rust userspace cannot call the live kernel DMU directly |
| Publication | Existing write/reread/`fsync`/rename-no-replace/directory-`fsync` protocol | Block writes plus FLUSH/FUA; fastdup must recreate atomic publication and name selection | One assigned DMU transaction can update held objects/ZAP entries, but durable completion and replay are the integration's responsibility |
| ZFS data protection | Pool checksums, vdev redundancy and scrub | Same | Same only if the custom objects use correct DMU write policies and remain reachable to scan/tooling |
| Caching | ARC plus fastdup/FUSE caches unless Direct I/O or metadata-only ARC is used | ARC remains; raw `O_DIRECT` avoids a filesystem page cache, while ZVOL Direct-I/O property support is explicitly absent | Ordinary DMU calls use ARC/dbufs; internal direct-DMU calls exist but add more private-ABI coupling |
| Geometry | `recordsize` is a per-file block-size policy; a 64-MiB Container spans several records | Fixed `volblocksize`; immutable after first write | Object block size and object types selected manually within DMU limits |
| Compression/dedup | Dataset properties | Dataset properties | Must select/inherit per-object policy correctly |
| `io_uring` | Official OpenZFS test covers buffered/direct read/write through Linux `io_uring`; fastdup's exact opcode chain still needs qualification | The OpenZFS ZVOL stress suite demonstrates BIO/libaio paths, not fastdup's `io_uring` protocol | No userspace `io_uring` path; a new copy/pinning/completion UAPI would be needed |
| Upgrade/repair tooling | Standard `zfs`, `zpool`, scrub, snapshots and send/receive; Container semantics remain self-describing | Standard pool/ZVOL tools, but fastdup must diagnose raw addresses itself | Matching OpenZFS/kernel/module required; stock tools do not understand fastdup's logical object graph |
| Portability | Same fastdup code can retain POSIX backends; OpenZFS supports its own platform ports | Device paths and block behavior are OS-specific | Separate Linux/FreeBSD kernel integrations and continuous source adaptation |

OpenZFS documents a ZVOL as a block device exported at `/dev/zvol/path`.
FLUSH and FUA are interpreted by the ZVOL request path, but the caller receives
block ordering, not an object transaction API.
[`zfs-create(8)`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man8/zfs-create.8),
[`module/os/linux/zfs/zvol_os.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/module/os/linux/zfs/zvol_os.c)

## Transaction and durability semantics

For normal files, `sync=standard` gives POSIX synchronous requests stable
storage and device flush semantics. `sync=always` forces every transaction and
has a documented large performance penalty. `sync=disabled` ignores
synchronous requests and is incompatible with fastdup's publication protocol.
[`zfsprops(7): sync`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man7/zfsprops.7)

ZFS normally limits an open TXG to five seconds, but fastdup must not treat that
timer as its Commit point: load, suspension, and errors still require explicit
admission and durable-publication handling. Keep `fsync` ordering for Container,
Metadata, and Commit WAL exactly as today.
[`zfs(4): zfs_txg_timeout`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man4/zfs.4)

At the DMU layer, callers create a transaction, declare every object/range they
may modify, assign it to a transaction group, mutate only held objects, and
then call `dmu_tx_commit`. Stable completion is a separate event: commit
callbacks run after the TXG is safely written, while `txg_wait_synced` waits for
a TXG. ZIL integration adds another private protocol if an operation must be
replayable before the TXG commits. A custom DMU backend therefore does not get
fastdup's durability contract merely by calling `dmu_tx_commit`.
[`include/sys/dmu.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/include/sys/dmu.h),
[`include/sys/txg.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd07fc0b53d12d1b7339d2fe7/include/sys/txg.h),
[`include/sys/zil.h`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/include/sys/zil.h)

fastdup's Metadata and DATA tiers are currently separate physical capacity and
fault domains. Two ZFS pools cannot share one atomic DMU transaction. Collapsing
them into one pool would be a product-level change to isolation and failure
policy, not a storage-adapter optimization.

## Integrity, scrub, and application checksums

A normal ZFS scrub examines all pool data, verifies each block checksum, and
repairs damage when mirror/RAIDZ/dRAID redundancy is available. A thorough
scrub additionally decompresses/decrypts ZFS blocks.
[`zpool-scrub(8)`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man8/zpool-scrub.8)

Those guarantees apply below all three forms, but they do not replace fastdup's
CRC32C, Container structural hash, decoded Chunk ID, Manifest reachability, or
Commit-chain proof. ZFS sees a fastdup-encoded Container as opaque bytes and
cannot validate its inner Zstd/Prefix decoding or detect a logically wrong but
otherwise valid Container object. fastdup scrub and recovery therefore remain
authoritative for application structure.

Custom `DMU_OST_OTHER` data would also lose the ordinary file-path attribution
that ZFS can provide for ZPL files. A private integration would have to maintain
its own object-to-Container diagnostic mapping and ensure custom objects are
reachable by ordinary scrub and send/receive tooling.

## Cache, I/O, and encoding policy

OpenZFS allows `primarycache` and `secondarycache` to retain all data,
metadata only, or nothing. For normal ZFS files, `direct=standard` bypasses ARC
when requests meet its constraints: writes must be `recordsize`-aligned, all
buffers must be page-aligned, and sizes must be page multiples. Unaligned write
portions silently use ARC. Mapping the file forces overlapping Direct I/O back
to buffered I/O. Direct writes are incompatible with ZFS dedup, and the Direct
I/O property is currently unsupported for ZVOLs.
[`zfsprops(7): cache and Direct I/O`](https://github.com/openzfs/zfs/blob/58d73c90dcdd07fc0b53d12d1b7339d2fe7/man/man7/zfsprops.7)

`io_uring` is not synonymous with Direct I/O. The current
[`IoUringStorageIo`](../../crates/fastdup-io-uring/src/lib.rs) opens ordinary
file descriptors without `O_DIRECT`, so its XFS publication path is buffered
today. If a future ZFS adapter requests Direct I/O, a 4-KiB Container write is
still buffered when the dataset `recordsize` is larger. Padding every
publication to `recordsize` would alter physical accounting and durable format
behavior. A prototype must distinguish buffered `io_uring` from `O_DIRECT` and
measure actual CQEs, copies, and ARC traffic.

For a ZFS-file prototype, start with:

- `checksum=on` (or an explicitly qualified algorithm);
- `compression=off`, because fastdup already selects RAW/Zstd/Prefix encodings;
- `dedup=off`, because ZFS deduplicates fixed ZFS records while fastdup's
  logical Chunk identity is independent of Container record boundaries, and
  OpenZFS itself advises against enabling dedup unless necessary;
- `primarycache=metadata`, `secondarycache=metadata`, and a measured prefetch
  policy to avoid competing with fastdup's verified caches; and
- `sync=standard`.

These are benchmark starting points, not universal optimal settings. ZFS
compression can still help headers/zeroes, and ARC can help repeated restores;
only end-to-end memory, CPU, ingest, restore, and scrub measurements can decide.

`recordsize` is a suggested filesystem block size from 512 B through 128 KiB,
or up to 16 MiB with `large_blocks`; a 64-MiB fastdup Container is naturally
several ZFS records. `volblocksize` has the same range and cannot be changed
after a volume has been written. Choosing either incorrectly can create
read-modify-write and space amplification.
[`zfsprops(7): recordsize/volblocksize/compression/dedup`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man7/zfsprops.7)

The official OpenZFS functional test verifies Linux `io_uring` buffered and
direct sequential/random I/O on a ZFS filesystem. It proves functional support,
not parity with XFS for fastdup's linked write/reread/fsync/rename/directory-
fsync state machine. ZVOL stress tests currently use `libaio`, so ZVOL
`io_uring` performance and completion behavior require separate evidence.
[`tests/functional/io/io_uring.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/io/io_uring.ksh),
[`zvol_stress.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/zvol/zvol_stress/zvol_stress.ksh)

## Operational and upgrade consequences

An ordinary ZFS-files backend preserves standard import/export, scrub,
snapshots, send/receive, quotas, and recovery tools. It still requires a new
fastdup deployment profile: the current accepted ADRs require separate XFS
filesystems and a mandatory XFS-qualified `io_uring` publisher. ZFS support
would need a superseding ADR plus the same fail-before/fail-after, SIGKILL,
power-loss, full-pool, pool-isolation, and latency evidence.

Those standard snapshot tools cannot be enabled as an uncoordinated bonus.
Snapshots retain Containers after fastdup GC unlinks their live names, so pool
space does not return even though fastdup has completed logical reclamation.
The physical capacity sampler remains conservatively safe, but GC can no
longer explain or recover the retained bytes and may repeatedly compact under
space pressure. Snapshots of separate Metadata and DATA pools are also not one
atomic cross-pool point, and rollback can violate fastdup's monotonic
generation/reservation assumptions. A supported ZFS profile must initially
forbid unmanaged snapshots, clones, and rollback, or integrate them explicitly
at the committed-generation and capacity-accounting seams.

A ZVOL additionally requires raw-format versioning and offline allocator/scrub
tools. Putting XFS inside a ZVOL avoids that rewrite but stacks XFS allocation,
journaling, caching, and write amplification on ZFS COW/TXGs; it is not the
“direct ZFS object” design under consideration.

A DMU backend must be rebuilt and retested for the exact Linux and OpenZFS
source versions. It must ship recovery tooling that can operate when the
fastdup module fails to load, version a new userspace/kernel protocol, preserve
pool import and upgrade behavior, and coordinate upstream changes. The Linux
kernel's published no-stable-kernel-ABI policy makes this an ongoing product
obligation, not a one-time integration cost.

## Qualification gate for a normal ZFS-files prototype

If ZFS remains desirable after this architecture decision, the smallest honest
experiment is an ordinary-files backend, not DMU:

1. create separate Metadata and DATA ZFS pools so current capacity/fault-domain
   semantics remain explicit;
2. run the existing Container publication and Commit fault matrices unchanged;
3. verify every required `io_uring` opcode and directory durability boundary;
4. compare ARC `all` versus `metadata` and aligned Direct I/O while measuring
   fastdup RSS, ARC bytes, CPU, physical writes, ingest/restore p99, scrub and
   GC interference;
5. inject checksum faults and confirm both ZFS scrub and fastdup scrub report
   their respective layers without either treating the other as authority; and
6. repeat real SIGKILL and power-cut tests with `sync=standard`.

No DMU prototype is justified unless ordinary ZFS files first show a material,
repeatable limitation that the private interface can plausibly remove.
