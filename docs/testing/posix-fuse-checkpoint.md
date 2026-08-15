# Volatile POSIX/FUSE checkpoint

Status: implemented and measured; deliberately non-durable. The later
[durable POSIX/FUSE checkpoint](durable-posix-checkpoint.md) reuses this exact
semantic seam and kernel adapter.

This checkpoint is the first shared namespace seam and real low-level FUSE
mount. Its purpose is to establish live POSIX behavior before connecting the
namespace to immutable manifests and commit generations. It is not the POSIX
Exact-Dedup MVP from [ADR 0032](../adr/0032-deliver-a-posix-exact-dedup-mvp-before-advanced-reduction.md).

## Implemented surface

- fixed root inode `1`, monotonic process-local inode and handle allocation;
- byte-exact, case-sensitive component names without UTF-8 conversion;
- regular-file `lookup`, `create`, `open`, `getattr`, `read`, `write`, `flush`,
  `fsync`, `release`, `unlink`, and size-only `setattr`;
- root `opendir`, `readdir`, `readdirplus`, `releasedir`, and `fsyncdir`;
- immediately coherent acknowledged writes through independent handles;
- per-inode mutation sequences and later-write-wins overlap ordering;
- atomic `O_APPEND` placement under the per-inode write lock;
- open-orphan reads after unlink and reclamation after the last handle;
- seek-write and truncate over an in-memory sparse extent map; logical holes do
  not allocate their zero-filled range, but are not yet durable HOLE/FILL nodes;
- bounded 16-MiB coalescing for adjacent writes, targeted B-tree range walks
  for reads and overwrites, and constant-time cached allocated-byte accounting;
- `readdirplus` lookup-pin ownership that retains only entries accepted into
  the kernel reply and rolls back a pending or unpolled entry on stream drop;
- directory snapshots bounded to 256 entries per namespace call, avoiding an
  all-name copy and unbounded speculative lookup-pin acquisition;
- expected errno mapping without assertions for client errors;
- FUSE writeback disabled, all regular handles `DIRECT_IO`, and zero TTLs.

The namespace catalog protects names, inode identity, and handle liveness. File
bytes and mutation sequences use per-inode locks, so independent inodes do not
share a data lock. Each inode sequencer begins at a 64-byte-aligned allocation
to prevent adjacent inode locks from sharing a cache line. The FUSE session and
Tokio runtime can execute independent
requests concurrently; CPU-heavy reduction work is not yet connected to this
path.

The 256-entry page bound limits per-reply allocation and speculative pins, but
the current index cookie still rescans the static directory prefix on later
pages. Therefore this checkpoint makes no large-directory scaling claim; a
seekable cookie/key cursor is deferred with concurrent-cookie stabilization.

## Validation on 2026-08-15

All artifacts remained under `/source/fastdup/.artifacts/`. The deterministic
suite covers live overlap, independent handles, open orphans, FUSE lookup/forget
pins, monotonic inode IDs, a one-byte write at a 1-TiB sparse offset, truncate,
access modes, zero-length writes, file-size bounds, raw names, static resumable
directory offsets, 8 concurrent append writers, 16 contending creates with
exactly one winner, and 256 lookup/unlink race schedules without assertion.
An additional 4,096-step deterministic differential test compares sparse
overwrites and truncates with a dense byte oracle after every mutation.
Focused tests also force a partially consumed `readdirplus` stream to prove its
unemitted lookup pin is released, and verify that 20 sequential MiB coalesce
into two bounded extents without changing byte reconstruction or block counts.
A 600-entry static-directory oracle resumes across bounded 256-entry namespace
pages without omissions or duplicates.

The final release-mode mount harness used `/dev/fuse` and the `/dev/sdb`
XFS-backed `/source/fastdup/.artifacts/tier-meta/fuse-mount`. It passed raw non-UTF-8 create/reopen,
cross-handle live reads, `fsync`, overlapping writes, `ftruncate`, seek-write,
including a one-byte write at a 1-TiB offset with only the written extents
reported as blocks, unlink while open, inode non-reuse, `readdirplus`, and 8
kernel-level append writers producing 512 unique, non-overlapping 16-byte
frames. The mount then wrote and reconstructed a 20-MiB sequential file across
the coalescing boundary and enumerated 512 long names with forced
`readdirplus`, requiring multiple bounded kernel replies. It then unmounted
cleanly.

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

cargo test -p fastdup-posix --all-targets
cargo clippy -p fastdup-posix --all-targets -- -D warnings
cargo run -p fastdup-posix --example fuse_mount_smoke -- \
  /source/fastdup/.artifacts/tier-meta/fuse-mount
```

For manual inspection, `cargo run -p fastdup-posix --bin fastdup-fuse --
MOUNT_PATH` mounts until `Ctrl-C`. The daemon prints an explicit volatile-data
warning.

## Assertion boundary

Expected inputs such as a missing entry, duplicate create, invalid byte
component, overlong name, wrong access mode, stale handle, oversized offset, or
capacity failure return `PosixError` and a stable errno. Assertions remain active
for impossible internal relationships: a name without an inode, a named inode
with zero links while the catalog is locked, handle-to-inode disagreement after
validation, allocator ID reuse, overlapping sparse DATA extents, reply-type
disagreement between the namespace and FUSE, and orphan reclamation before its
last handle or lookup reference.

Every fallible size, offset, sequence, inode, and handle transition is checked.
A write validates its final length and reserves memory before publishing bytes
or advancing the mutation sequence. There is not yet a durable writer,
reader/recovery, or scrub triple for namespace objects; claiming those paired
assertions is blocked on the manifest/Commit-WAL slice.

## Explicitly absent

- crash recovery, persistent inode-ID reservation, the five-/ten-second commit
  scheduler, admission backpressure, and any durability from `fsync`;
- durable manifests, Namespace Roots, Commit Records, or data-reduction records;
- atomic rename, hardlinks, symlinks, nested directories, xattrs, ACLs, locks,
  `fallocate`, hole punch, zero range, and DATA/HOLE seeking;
- durable sparse physical representation, `SEEK_DATA`/`SEEK_HOLE`, stable
  directory cookies during concurrent mutation, read-only mmap, normal kernel
  read caching, cache invalidation, timestamps, ownership changes, and
  production `statfs`;
- Samba and crash/remount conformance.

The versioned immutable manifest and namespace generation path is now connected
as the bounded RAW slice documented in
[Durable POSIX/FUSE checkpoint](durable-posix-checkpoint.md). Its explicit
limitations supersede this checkpoint's former next-step note.
