# POSIX conformance plan

Status: test plan with volatile and bounded durable RAW/Zstd checkpoints.

This document defines how the POSIX surface will be tested. It is not a claim
that the listed operations are implemented, and it does not redefine their
semantics. The authoritative decisions are
[ADR 0027](../adr/0027-define-posix-edge-semantics-explicitly.md) and
[ADR 0032](../adr/0032-deliver-a-posix-exact-dedup-mvp-before-advanced-reduction.md),
with the shared volatile seam fixed by
[ADR 0033](../adr/0033-share-one-posix-seam-between-model-and-fuse.md) and the
write and recovery rules in ADRs
[0003](../adr/0003-fsync-does-not-strengthen-durability.md),
[0005](../adr/0005-recover-committed-prefixes-of-interrupted-ingests.md),
[0006](../adr/0006-commit-content-and-namespace-in-atomic-generations.md),
[0007](../adr/0007-keep-writes-visible-to-the-daemon.md), and
[0019](../adr/0019-commit-only-after-data-and-metadata-are-durable.md).

## Current boundary

The repository implements the Stage-1 immutable Container Store, versioned
Manifest leaves, flat Namespace Roots, Commit Records and WAL, the shared
namespace/FUSE seam, and the bounded adaptive RAW/Zstd orchestration recorded in
[Durable POSIX/FUSE checkpoint](durable-posix-checkpoint.md). A five-second
runtime loop, event-driven 512-MiB active-Dirty-DATA pressure trigger, and
mutation-admission gate are connected to the real mount.
FastCDC-v1, automatic level-zero Exact-Index publication, cross-checkpoint Exact
Hits, bounded parallel RAW/Zstd region encoding, hierarchical tree-native
Manifest updates, four-way Exact-Index compaction, and atomic replacement rename
are connected. Offline maintenance now scrubs every retained generation,
published Container, active Exact object, and ACTIVE Location, and can rebuild
the Exact Index through hidden RoW Runs plus final atomic activation. Nested
`mkdir`, `rmdir`, lookup, `readdir`, cross-parent rename, durable recovery, and
offline scrub are connected through Namespace Root v2. Volatile advisory POSIX
byte-range locks implement `F_GETLK`, `F_SETLK`, and `F_SETLKW`, including
owner cleanup on close. Metadata-only allocation, hole punch, zero range,
DATA/HOLE seek, and the shared-seam collapse/insert operations are connected.
The stock Linux FUSE kernel forwards allocate, punch, and zero, but rejects
collapse/insert flags before userspace. It does not yet implement per-region
serialized Chunking Profile IDs, fake-clock stalled-I/O deadline proof, or BSD
`flock`. Hardlinks, symlinks, ownership, timestamps, xattrs, POSIX ACLs, and
volatile POSIX record locks are connected through the durable namespace and
FUSE adapter. A bounded real-process
[`SIGKILL`/remount/deadline matrix](sigkill-remount-deadline.md) now covers
acknowledged sequential writes. When a valid Run Set already exists,
normal POSIX reads use bounded verified Locations and transparently fall back to
Container scans on index loss or corruption.

`fastdup_store::StorageIo` is an internal adapter for publishing canonical
container files. Its `create_new`, `write_at`, `set_len`, `read`, sync, and
rename-like operations are not POSIX filesystem operations. In particular, its
names are Rust strings because internal Container Store names are canonical
ASCII. It must not be reused as the user namespace: user names are byte-exact
and are not required to be UTF-8 under ADR 0027.

The existing no-replace rename and directory sync test only immutable container
publication. They do not establish user-visible rename atomicity. Likewise, the
existing deterministic fault model establishes that a container is absent or
fully verified after a publication fault; it does not exercise a POSIX mutation
or Namespace Root recovery.

## Execution levels

Each conformance row names the lowest useful execution level. A row does not
pass overall until it passes at every applicable later level.

1. **M -- deterministic model.** Exercise a public namespace/inode seam with a
   fake monotonic clock and injectable storage. Enumerate operation failures and
   crash points without involving the kernel page cache.
2. **F -- real FUSE mount.** Run through system calls against a low-level FUSE
   mount on the workspace's XFS-backed test tiers. Verify negotiated cache
   behavior as well as returned bytes, metadata, and errno values.
3. **K -- SIGKILL and remount.** Kill the daemon at controlled or randomized
   commit phases, restart it, and compare the recovered namespace with complete
   generation oracles. The first bounded public K-level tracer is implemented
   in the `SIGKILL`/remount/deadline harness. Block-device power-cut and
   torn-write injection extend this level when available.
4. **S -- Samba consumer.** After the FUSE MVP, run Samba and SMB conformance
   tests against the same mount. ADR 0032 deliberately places production Samba
   hardening after the POSIX Exact-Dedup MVP; S results cannot substitute for M,
   F, or K results.

Passing tests directly against the underlying XFS mounts only tests XFS. Such
results are useful for harness validation, but are not fastdup conformance
results.

## Status and priority

- **P0** blocks a claim that fastdup is a usable POSIX Exact-Dedup MVP.
- **P1** blocks a robustness claim and must pass before exposing the MVP to
  general backup workloads.
- Rows are still MVP gates, not satisfied by a narrower checkpoint. The `Level`
  column is the first level at which its complete oracle applies. The checkpoint
  provides partial M/F evidence for mount, inode identity within one process,
  create/lookup, acknowledged writes, mutation ordering, open orphans, truncate
  bytes, byte-exact names, append, and expected errors. The dedicated
  real-process harness provides bounded K evidence for acknowledged sequential
  writes and their ten-second deadline, but not yet for the remaining rows.

## P0 conformance matrix

| Surface | Required observation | Concrete test oracle | Level |
| --- | --- | --- | --- |
| Low-level FUSE mount | The daemon mounts without a writeback cache and handles independent inodes concurrently. | Inspect negotiated FUSE capabilities and mount state; issue blocked operations on one inode while another inode continues to make progress. | F |
| Inode identity | Inode IDs are stable, monotonic, 64-bit, and never reused. | Record IDs across create, unlink, commit, crash, and recreate cycles; every newly allocated ID is greater than all prior IDs and surviving names keep their ID. | M |
| Create and lookup | A successful create has one live inode and one byte-exact directory entry. | Immediately look up and open the new name through an independent handle; after each injected fault recover either the complete generation before create or the complete generation after it. | M |
| Acknowledged writes | Every successfully acknowledged write is visible to subsequent reads while the daemon remains alive. | After each write reply, read the affected and neighboring ranges through the same and independent handles; bytes equal the ordered mutation overlay, including overlaps. | M |
| Mutation ordering | Accepted content and metadata mutations form contiguous per-inode sequence prefixes; a later overlapping write wins. | Record assigned sequence numbers in a test observer, permute worker completion, and compare the live and committed bytes with a serial mutation oracle. | M |
| Ten-second durability | Every acknowledged mutation becomes part of a recoverable commit within the accepted deadline. | With a fake clock, advance to the deadline and crash at every commit operation; with a real mount, kill after the deadline plus tolerance. Recovery must include the mutation in one wholly valid generation. | M |
| Admission backpressure | The daemon does not acknowledge new mutations when it cannot preserve the deadline or active Dirty DATA reaches 512 MiB. | Stall durable progress before the configured warning point, advance the fake clock, and prove later calls fail or remain unacknowledged while already admitted mutations retain priority. Separately cross the exact byte-pressure edge, require an immediate checkpoint/gate without timer polling, and prove sparse holes and repeated overwrites do not trigger early. | M |
| Filesystem capacity | `statfs` reports post-reduction physical data capacity after the ten-percent operating reserve. Exhausting the metadata reserve reports zero available blocks. Explicit fake total and available values affect reporting only. | Compare the reply with `statvfs` on both tiers, cross each reserve boundary in the pure capacity model, mount with exact fake values, and prove malformed override pairs fail startup. | F |
| `fsync`, `fdatasync`, `O_SYNC`, `O_DSYNC` | Sync calls do not promise a stronger crash boundary than the system window, while acknowledged bytes remain live-readable. | Sync immediately after a write, then verify exact live reads. A crash inside the permitted window may recover the complete old or new generation; a crash after the deadline must recover the write. Never accept a mixed generation. | M |
| Interrupted ingest | A long open ingest recovers its newest wholly committed prefix. | Append uniquely numbered records for longer than one commit interval and kill at every commit phase. Recovered bytes end exactly at a committed record boundary and equal a prefix of acknowledged bytes. | K |
| Atomic user rename | Rename replacement is entirely before or entirely after in both namespace and inode state. | Rename a source over an existing target while both have open handles and hardlinks. After each fault, compare paths, inode IDs, link counts, and contents with exactly the pre- or post-rename oracle. | M |
| Unlink and open orphan | An unlinked inode remains usable through live handles, but has no name and need not reappear after daemon crash. | Open through two handles, unlink, read and write through both, close them independently, then kill and remount. Namespace lookup always fails after unlink and no orphan name is synthesized. | F |
| Truncate | Shrink removes the exact suffix; grow creates a logical hole; the size transition is atomic. | Generate boundary sizes around chunks and manifest leaves. Compare reads and sparse layout to a byte-plus-extent reference model before and after faults. | M |
| Sparse seek-write | Writes beyond EOF preserve holes rather than materializing zero chunks. | Seek beyond EOF, write a marker, remount, and compare DATA/HOLE extents and bytes with the reference extent map. | F |
| Hole punch | Punching creates a HOLE over the exact requested range under the supported `fallocate` flags. | Seed nonzero data, punch aligned and unaligned ranges, and verify bytes, extent kinds, size, and unaffected boundaries before and after crash. | F |
| Zero range | Zeroing creates allocated `FILL(0)` DATA and not a HOLE. | Apply zero range with and without `KEEP_SIZE`; reads return zero while the internal manifest oracle reports FILL rather than HOLE. Validate the distinction again after remount. | F |
| Thin allocation | Successful `fallocate` preserves existing bytes and represents previously sparse bytes as `FILL(0)` metadata. It makes no physical-capacity promise, and `KEEP_SIZE` beyond EOF has no retained effect. | Mix allocation with DATA and holes, compare bytes, `st_blocks`, DATA/HOLE seeks, recovery, and scrub, and prove a 1-TiB allocation retains no dirty payload. | F |
| Structural range splice | Collapse deletes one middle range and shifts the suffix left; insert adds a HOLE and shifts the suffix right. Both reuse immutable recipes without DATA ingest. | Differentially compare byte and allocation maps, require zero checkpoint rechunk bytes, inject every metadata publication fault, and document the stock Linux FUSE `EOPNOTSUPP` gate. | M |
| Hardlinks | All names for an inode observe the same content and metadata version and correct link count. | Link a file, mutate through alternating names, rename and unlink each name, and compare `st_ino`, `st_nlink`, bytes, and recovery with one shared-inode model. | M |
| Symlinks | Symlink targets are reproduced byte-for-byte and remain separate from their referents. | Create relative, absolute, dangling, maximum supported, and non-UTF-8 targets; compare `readlink` bytes before and after rename and remount. | F |
| Corruption read boundary | Corrupt durable data is never returned as valid user content. | Flip each selected container, index, manifest, and Namespace Root field. Reads return `EIO` or recovery selects a previous whole generation; no corrupt payload bytes reach the caller. | M |

The sparse-layout model above uses the DATA, HOLE, and FILL rules from
[ADR 0011](../adr/0011-use-hierarchical-immutable-manifests.md). Random-update
cases must also exercise the bounded resynchronization rules from
[ADR 0013](../adr/0013-bound-random-write-rechunking.md), without making those
format decisions part of the POSIX test API.

## P1 conformance matrix

| Surface | Required observation | Concrete test oracle | Level |
| --- | --- | --- | --- |
| Byte-exact names | Names are case-sensitive byte sequences without Unicode normalization. | Use raw system calls to create non-UTF-8 names, normalization-equivalent Unicode byte sequences, case variants, and maximum-length components; enumerate and reopen the exact original bytes. | F |
| Directories | `mkdir`, `rmdir`, lookup, and `readdir` remain coherent during concurrent mutation. | Compare results with a serialized namespace model; validate `ENOTEMPTY`, cycle prevention, `.`/`..`, and resumable directory cookies without missing stable entries or inventing entries. | F |
| Atomic append | Each acknowledged `O_APPEND` write occupies one non-overlapping contiguous range. | Run many processes writing framed records of varied sizes and verify every complete acknowledged record occurs exactly once without overlap. | F |
| Permissions and ownership | Mode, uid, gid, permission checks, and ownership transitions are coherent and atomic. | Exercise access as distinct credentials; inject faults around `chmod` and `chown`; recovered state equals one complete generation and reported errno is stable. | F |
| Timestamps | Relatime, mtime, ctime, and explicitly set times follow ADR 0027 as ordinary atomic metadata mutations. | Use a controlled clock, read/write/chmod/rename/`utimensat`, and compare exact timestamp transitions before and after remount. | M |
| Extended attributes | Binary xattr names/values and create, replace, list, get, and remove semantics are atomic. | Test empty and bounded-large binary values, `XATTR_CREATE`/`XATTR_REPLACE`, missing attributes, concurrent replacements, and every commit fault. | M |
| POSIX ACLs | Access/default ACLs, inheritance, and mode-mask interactions survive commits byte-exactly. | Use `setfacl`/`getfacl`, create children under default ACLs, mutate mode bits, test access under multiple credentials, and remount. | F |
| Advisory locks | POSIX byte-range and whole-file lock conflicts are owner-aware, independent of mutation admission, and transient. Ordinary reads and writes remain advisory rather than mandatory-lock operations. | Use independent processes for overlapping/non-overlapping lock ranges, partial unlock and conversion, close and duplicate descriptors, unlink/rename locked files, kill a lock owner, and verify locks do not survive daemon restart. | F |
| Kernel read cache | Writes invalidate every affected cached range; stale acknowledged data is never read. | Warm page-cache ranges through multiple descriptors, perform overlapping writes, and immediately reread boundaries under the normal and benchmark-only `direct_io` configurations. | F |
| Shared writable mapping | Writable shared mappings are consistently rejected in v1; read-only mappings remain coherent. | Attempt `MAP_SHARED|PROT_WRITE` and require the documented failure; exercise read-only faults around concurrent writes and truncation without stale or unchecked bytes. | F |
| Error mapping | Expected errors such as `ENOENT`, `EEXIST`, `ENOTEMPTY`, `ENOSPC`, `EFBIG`, `EOPNOTSUPP`, and `EIO` are not assertion crashes. | Table-drive invalid and resource-limited operations through the model and FUSE mount; compare operation, errno, and unchanged state with the oracle. | M |
| Handle and request races | Lookup/forget, open/release, cancellation, and concurrent independent requests do not leak liveness or reuse identity. | Randomly schedule request completion, interruption, handle close, unlink, and daemon shutdown; all retained objects have an explicit handle, namespace, or generation pin. | M |

## Harness gates

The first namespace implementation should expose the same semantic operation
API to the deterministic model and FUSE adapter. This keeps model tests from
becoming a second implementation of the rules. Every durable invariant added by
that slice needs:

1. a writer-side check before publication;
2. a reader/recovery check before exposure;
3. an offline scrub check when a scrub surface exists; and
4. a deterministic fault case proving an invalid intermediate state is not
   selected.

After the row-specific tests pass, run broad tools through the mounted
filesystem: a selected `pjdfstest` set, `fsx`, `fsstress`, and the relevant
`xfstests` generic cases. Record exclusions with the exact unsupported contract
and ADR rather than silently filtering failures. Later Samba runs should add
SMB rename, lock, ACL, sparse-file, and flush interoperability, while retaining
the POSIX mount as the source of storage semantics.

## Current truthful result

The public namespace seam, real FUSE mount, atomic commit cut, immutable
generation writer, recovery adapter, Inode reservation, scheduler gate, sparse
RAW/Zstd checkpoint, pinned Exact-Index demand reads, and deterministic
storage-operation fault matrix are green. The bounded real-process
`SIGKILL`/remount matrix additionally proves complete-prefix recovery inside
the accepted window and recovery of every acknowledged record after its
ten-second deadline.
This provides direct M/F evidence and durable-operation fault evidence for the
implemented subset. The public maintenance seam adds deterministic corruption,
global-index-invariant, and fail-before/fail-after evidence, but does not replace
real block-device power-cut campaigns. The complete P0 matrix has not yet been
rerun at every applicable M/F/K level: fake-clock stalled-I/O coverage, broad
randomized process-kill coverage, and the remaining POSIX/Samba matrices are
still absent. Stock Linux FUSE still prevents mounted collapse/insert despite
shared-seam coverage. Container Store,
deterministic namespace, durable orchestration, offline scrub/rebuild, and
real-mount results remain separately reported evidence.
