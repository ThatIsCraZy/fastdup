# Durable POSIX/FUSE checkpoint

Status: implemented and fault-tested; pre-MVP RAW checkpoint.

This checkpoint connects the shared POSIX namespace seam to immutable RAW
containers, Manifest leaves, Namespace Roots, and the Commit WAL. It advances
the earlier [volatile FUSE checkpoint](posix-fuse-checkpoint.md) without yet
claiming the Exact-Dedup MVP from
[ADR 0032](../adr/0032-deliver-a-posix-exact-dedup-mvp-before-advanced-reduction.md).

## Implemented durability path

`fastdup_appliance::DurableNamespace` owns the writable namespace and the only
checkpoint serialization lock. Opening a new repository publishes an initial
Inode ID reservation before mutation admission. Reopening an existing
repository first skips the complete old reservation and durably publishes a
fresh range; no ID acknowledged inside a lost durability window is reused.

A checkpoint performs this order:

1. freeze one retryable namespace and per-Inode mutation prefix;
2. seal, verify, file-sync, publish, and directory-sync immutable RAW
   containers;
3. publish immutable Manifest leaves;
4. construct and verify the complete Namespace Root graph;
5. append, reread, verify, and sync the Commit Record in `commit.wal`;
6. install verified Manifest-backed readers below the live dirty overlay.

Opening the writable appliance fully verifies published containers once and
initializes a scalar next-Container-generation high-water. Checkpoints consume
that generation under the serialization lock before attempting publication;
gaps are permitted after failed attempts, but an in-process fail-after can
never reuse a generation. This removes one directory/full-container scan from
every checkpoint without introducing a RAM-resident full Chunk index or
weakening the graph verifier.

Writes and namespace mutations accepted after the cut remain immediately
visible to reads and belong to the next epoch. Retrying before installation
returns the same token and frozen bytes. A successful install cannot retire a
later overlapping write. `fsync` retains the accepted system-wide durability
window and does not force a private transaction.

Sparse layout is retained. The checkpoint planner uses allocation metadata to
split the frozen range into exact DATA, HOLE, and FILL extents without scanning
the bytes of large holes. DATA reads are bounded to the format-v1 maximum
logical chunk length. Uniform allocated ranges become FILL, including
`FILL(0)`; unallocated zero ranges remain HOLE.

For a size-stable file, the frozen commit cut also exports sorted, coalesced
DATA/HOLE change ranges. The durable planner expands those ranges to bounded
256-KiB cells and to complete pre-existing DATA extents, replans only those
ranges, and splices verified unchanged DATA/FILL/HOLE extents into the new
Manifest. It never clips a DATA extent. A file-size change or a cache mismatch
falls back to the complete planner. Thus the optimization can reduce data I/O
and container publication without changing the recovery format or turning a
dirty-range hint into an integrity authority.

## Scheduler and admission

The `fastdup-durable-fuse` binary mounts this namespace through the same
low-level FUSE adapter as the model. It targets one checkpoint every five
seconds. Checkpoint work runs on a blocking worker rather than a Tokio reactor
thread. If one checkpoint has not completed within another five seconds, or if
durable progress returns an error, mutation admission closes while reads and
already admitted dirty data remain available. New mutation calls receive
`EAGAIN`, not an assertion or a false durability acknowledgement. After a
timed-out checkpoint catches up completely, admission reopens; after an error it
remains closed until a later retry catches up.

Closing the gate takes an exclusive admission lock. Every mutating dispatch
holds a shared admission guard through application of the mutation, so the
close operation cannot race past a write that passed admission but has not yet
updated the live view.

This is a bounded-progress mechanism, not a claim that arbitrarily stalled
hardware can meet a wall-clock deadline. Arbitrarily stalled total I/O remains
outside the supported failure envelope in ADR 0007. A fake-clock deadline test,
process `SIGKILL` matrix, and persisted appliance-health state remain open.

## Validation on 2026-08-16

All build, test, mount, and repository artifacts remained under
`/source/fastdup/.artifacts/`.

The public POSIX commit-cut tests prove:

- a later overlapping write is live-readable during an in-flight commit;
- retry returns the same frozen token and bytes;
- installing the committed prefix preserves later dirty bytes;
- create and unlink after a cut appear atomically in the next cut;
- paused admission rejects only mutations while reads and sync calls remain
  available; and
- closing admission waits for an already admitted, deliberately blocked write
  to finish applying before the gate reports closed.

The durable appliance integration test checkpoints a byte-exact raw name and a
sparse seek-write, reopens through production recovery, verifies size,
allocation, bytes, mode, mutation sequence, and Inode identity, then confirms a
fresh writable recovery skips the old Inode reservation.

Bounded-update integration tests commit four distinct 256-KiB DATA chunks,
change one byte in the second chunk, and observe exactly one additional
published chunk (`4 -> 5`) before a byte-exact remount. A following
namespace-only generation remains at five chunks. A separate sparse-file case
changes the leading DATA extent while retaining a distant tail and its hole;
the verified physical count is `2 -> 3`, and recovery preserves both changed
bytes and the sparse gap.

The deterministic storage backend also observes Container operations for a
namespace-only generation. Container-generation allocation performs no scan;
the one remaining `ListNames` call belongs to reachable-DATA graph verification.
That proof now carries opaque inode-associated Manifest readers into commit and
read-only recovery installation, so installation does not immediately repeat
the same scan. Removing the final graph scan requires the persistent verified
Location/Exact-Index path, not an unbounded authoritative RAM map.
An injected fail-after on Container directory sync leaves generation 1 durable
but returns an ambiguous error; retrying the identical frozen cut publishes
generation 2. Verification observes `[1, 2]`, proving that the live allocator
does not reuse an ambiguously published generation.

The deterministic storage fault matrix injects both fail-before and fail-after
at every storage operation observed in the complete checkpoint path, separately
for the container and metadata repositories. After a modeled crash, recovery
observes only the initial reservation generation or the complete DATA-bearing
generation. Only fail-after on the final effective Commit WAL sync may expose
the new generation despite returning an error.

A real kernel mount on XFS created a file containing the workspace
`Cargo.toml`, grew it sparsely to 1 MiB, and wrote `tail` beyond that range. The
live and recovered SHA-256 was
`22875c74ebcb2f44ecc061ad9706e608580abaff9ed8917fcfd4f770fda9e6fa`.
Before and after remount it reported size `1,048,580`, two 512-byte blocks, Inode
`2`, and the exact `tail` suffix. After restart, the next create received Inode
`4098`, the beginning of the newly durable reservation rather than an unused ID
from the old range.

After bounded rewriting was connected, a second real kernel mount copied the
first 1 MiB of the pinned Rocky Linux ISO into `rocky-prefix.bin`. Its initial
SHA-256 was
`4443bb8347f702e254a60373a1d160b38493e750f5357b3782a12b52a04bd19f`.
Byte 262,181 (value 95) was changed to zero; the live and post-remount SHA-256
was `204027504762a45b263cec6c2094e4556c50cd2fadd194b6dc7a02f1b846f61d`.
Size (1,048,576), allocated blocks (2,048 512-byte units), Inode (2), and the
changed byte all survived a clean unmount/remount. A full production-format
audit reported two containers and five chunks: four initial cells plus exactly
one rewritten cell.

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

cargo test -p fastdup-posix -p fastdup-appliance --all-targets
cargo clippy -p fastdup-posix -p fastdup-appliance --all-targets -- -D warnings
cargo run -p fastdup-appliance --bin fastdup-durable-fuse -- \
  MOUNT_PATH METADATA_ROOT CONTAINER_ROOT
```

## Explicitly incomplete

- The durable writer currently uses bounded fixed RAW chunks. FastCDC, the
  persistent Exact Index, Bloom/hot lookup, and adaptive Zstd are not yet wired
  into this durable path.
- It still serializes one complete flat Manifest leaf for every linked file in
  the cut. Size-stable updates reuse unchanged extents and avoid rereading or
  republishing their DATA, but metadata work remains O(number of file extents).
  Hierarchical Manifest paths are still required for the target 100-TB files.
- Manifest planning and RAW container assembly are currently single-worker.
  The scheduler keeps this work off Tokio reactor threads, but this checkpoint
  makes no ingest-scaling claim; bounded per-worker planning and deterministic
  merge remain to be connected.
- Reachable-DATA graph verification and demand reads still scan published
  containers. Reader installation consumes the graph verifier's opaque proof,
  and Container-generation discovery is mount-time only. The remaining
  rebuild-safe scans must be replaced in normal operation by verified
  persistent location lookup.
- The flat v1 Namespace Root and Manifest leaf retain their documented metadata
  size limits.
- Atomic rename, links, nested directories, xattrs/ACLs, locks, allocation
  operations, normal read caching, automatic GC, scrub, and Samba conformance
  remain open.
- WAL segmentation/rotation, durable Appliance Lease, fake-clock deadline
  proofs, `SIGKILL`/remount sweeps, and multi-process writer exclusion remain
  open.

The next performance-safe slice is not wider POSIX surface area. It is bounded
Manifest path rewriting plus durable Exact-Index/location lookup, retaining the
same commit-cut and recovery interfaces.
