# Durable POSIX/FUSE checkpoint

Status: implemented and fault-tested; pre-MVP FastCDC/Exact/adaptive RAW/Zstd checkpoint.

This checkpoint connects the shared POSIX namespace seam to immutable adaptive
RAW/Zstd containers, Manifest leaves, Namespace Roots, and the Commit WAL. It advances
the earlier [volatile FUSE checkpoint](posix-fuse-checkpoint.md) without yet
claiming the Exact-Dedup MVP from
[ADR 0032](../adr/0032-deliver-a-posix-exact-dedup-mvp-before-advanced-reduction.md).

## Implemented durability path

`fastdup_appliance::DurableNamespace` owns the writable namespace and the only
checkpoint serialization lock. Opening a new repository publishes an initial
Inode ID reservation before mutation admission. Reopening an existing
repository first skips the complete old reservation and durably publishes a
fresh range; no ID acknowledged inside a lost durability window is reused.
The daemon derives its nonzero Policy Set ID from canonical
`checkpoint-policy-v1` bytes that pin FastCDC, Compression Region, Zstd
selection, level-zero publication, and four-way compaction decisions; these rules cannot change
silently under the earlier experimental constant ID.

A checkpoint performs this order:

1. freeze one retryable namespace and per-Inode mutation prefix;
2. stream FastCDC-v1 over allocated DATA with 16/64/256-KiB min/target/max,
   Normalization Level 1, and seed 0; holes and forced rewrite boundaries reset
   the chunker;
3. group complete DATA chunks into bounded 512-KiB Compression Regions, encode
   independent regions on at most `min(available CPUs, 64, region count)`
   private-worker lanes, merge by input ordinal, select RAW or Zstd from complete
   record cost, then seal, verify, file-sync, publish, and directory-sync;
4. derive ACTIVE Exact-Index entries only from the complete Container writer
   reread, publish one immutable level-zero Run, recursively merge the four
   oldest same-level Runs into one verified higher-level Run, publish the
   successor Run Set, then sync its activation WAL. Index failure marks the
   accelerator degraded but does not block Namespace durability; an activated
   orphan Location is not liveness;
5. publish immutable Manifest leaves;
6. construct and verify the complete Namespace Root graph;
7. append, reread, verify, and sync the Commit Record in `commit.wal`;
8. install verified Manifest-backed readers below the live dirty overlay.

Opening the writable appliance initializes a scalar next-Container-generation
high-water from each published object's paired 4-KiB Header/Footer envelope and
physical length. It does not read Container payloads for allocator recovery.
Checkpoints consume that generation under the serialization lock before
attempting publication; gaps are permitted after failed attempts, but an
in-process fail-after can never reuse a generation. This removes a
directory/full-container scan from every checkpoint without introducing a
RAM-resident full Chunk index or weakening the separate graph verifier.

At mount, the daemon recovers the newest valid immutable Exact-Index Run Set
from the metadata tier. Each newly installed Manifest reader pins the current
immutable Run Set. A checkpoint probes that same generation before retaining a
new physical copy; a hit is accepted only after bounded Container-envelope,
Record, coordinate, decode, length, and Chunk-ID verification. Newly published
RAW and dependency-free Zstd Locations form a successor level-zero Run and are
available to the next checkpoint. Missing or corrupt acceleration stores a
duplicate Location or uses the complete verified read scan; it never rolls the
Namespace back or prevents a Namespace commit.

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

The frozen commit cut exports sorted, coalesced DATA/HOLE change ranges. For a
size-stable file, the durable planner expands those ranges to bounded 256-KiB
cells and complete pre-existing DATA extents, replans only those ranges, and
splices verified unchanged DATA/FILL/HOLE extents into the new Manifest. It
never clips a DATA extent.

For an append, the planner preserves the old Manifest prefix and replays at
most its final DATA Chunk as the FastCDC state anchor before streaming the new
suffix. A final FILL or HOLE already forces a boundary and needs no old DATA
replay. Consequently old-prefix CDC work is bounded by the FastCDC-v1 256-KiB
maximum instead of the current file length. Mixed in-prefix mutations and an
append retain both bounded rewrite regions. Truncation and a Manifest cache
mismatch still take the complete planner. The tail-anchor rule is pinned in the
canonical Policy Set ID; it changes no recovery format and never treats a
dirty-range hint as integrity authority.

Normal successor commits also compose their DATA proof incrementally. The
planner compares each proposed Manifest with the immediately preceding verified
and installed Manifest. Equal prefix/suffix extents retain their prior complete
proof; every DATA dependency in the changed middle goes through the ordinary
full Exact-candidate/Container verifier before WAL commit. The verifier repeats
the dependency subset check against the independently reread proposed Manifest.
It retains no full in-RAM Chunk set. Recovery and offline verification never use
this shortcut and prove the complete selected graph from durable objects. This
is the [Successor Graph Proof decision](../adr/0036-compose-successor-data-proofs-from-the-installed-generation.md).

## Scheduler and admission

The `fastdup-durable-fuse` binary mounts this namespace through the same
low-level FUSE adapter as the model. It targets one checkpoint every five
seconds. Independently, 512 MiB of unique active checkpointable Dirty DATA
(eight 64-MiB format-v1 maximum Container sizes) wakes the scheduler without
polling. A pressure-triggered cycle closes mutation admission before cutting the
generation; a time-triggered cycle closes it if its next active epoch reaches
the same pressure threshold while the older cut is still running. This avoids a
multi-gigabyte overshoot during a slow checkpoint. The counter excludes sparse
holes, repeated overwrites of already dirty bytes, frozen epochs, and encoder
work buffers, so it is not reported as total process RSS.

Checkpoint work runs on a blocking worker rather than a Tokio reactor thread.
If one checkpoint has not completed within another five seconds, or if durable
progress returns an error, mutation admission closes while reads and already
admitted dirty data remain available. The synchronous model reports that state
as `Again`; the kernel-FUSE adapter waits for the admission notification and
retries the not-yet-applied mutation instead of leaking `EAGAIN` to ordinary
POSIX writers. After a timed-out or pressure checkpoint catches up completely,
admission reopens; after an error it remains closed until a later retry catches
up. A future persisted health state must distinguish terminal capacity or
device faults from retryable catch-up so they can return a stable errno instead
of waiting indefinitely.

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
  to finish applying before the gate reports closed;
- checkpoint pressure counts unique active DATA rather than write traffic,
  wakes an already waiting scheduler at the configured edge, and resets when a
  commit cut freezes the epoch; and
- unlink removes an open orphan from checkpoint pressure while preserving
  byte-exact writes through its existing handle and contiguous Inode mutation
  sequencing.

A real release-build kernel-FUSE run then wrote exactly 536,870,912 zero bytes
in 0.31 seconds. The daemon reported the pressure edge at exactly 536,870,912
active dirty bytes, closed admission, and durably committed generation 2 in
0.558 seconds before reopening admission. The payload reduced to 2,048 FILL
chunks and no physical DATA Container, demonstrating that the trigger measures
pre-reduction RAM pressure rather than post-reduction output. After a clean
unmount and fresh recovery mount, size and allocation remained 536,870,912
bytes and SHA-256 matched the independently generated all-zero oracle:
`9acca8e8c22201155389f65abbf6bc9723edc7384ead80503839f49dcc56d767`.
Artifacts remain under
`/source/fastdup/.artifacts/pressure-fuse/run.vKiH7M`.

The durable reduction integration tests additionally prove that FastCDC-v1
produces bounded content-defined Manifest extents, one-worker and four-worker
adaptive encoding are byte-identical, verified multi-Chunk Zstd Locations are
usable through bounded Exact-Index reads, a later duplicate checkpoint publishes
no second Container, and an injected index-publication failure still recovers
the complete Namespace without index authority.

The compaction integration path commits 70 unique payloads in 70 separate
generations, remains below the 64-Run reader bound without degradation, reuses a
Location from the first generation in a later duplicate-only checkpoint, and
recovers both names byte-exactly after remount. Repository tests also require
canonical output independent of source discovery order, collapse repeated
physical Locations to their newest transition, reject a checksummed-page source
corruption without publishing output, and complete an offline full-run audit.
The deterministic backend injects fail-before and fail-after at every source
read and output-publication operation; after crash the compacted Run is either
absent or completely published, and only an effective final directory sync may
make an error-returning publication durable. Existing replacement-activation
fault tests then select only the old or complete new Run Set.

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

The indexed recovery/commit tracer measures the Container backend around the
complete public operation rather than only later demand reads. Healthy read-only
recovery performs no `ListNames` or whole-object `Read`. Writable recovery uses
one directory listing for the still-separate Container-generation high-water,
then reads only object length plus Header/Footer for each Container; recovery
graph verification, reservation commit, and reader installation add no
whole-object scan. A later healthy indexed checkpoint performs neither
operation for graph proof. Corrupting the pinned Run page switches the same
verifier to one complete Container scan and still returns byte-exact data.

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

After FastCDC, automatic level-zero publication, and parallel region encoding
were connected, a real kernel mount copied the 591,329-byte structured JSON
fixture twice under different names in separate checkpoint intervals. The host
exposed ten checkpoint workers. Before and after the duplicate commit there was
exactly one Container and one Exact-Index Run; after complete unmount/remount
both files compared byte-for-byte with the source. Full Container audit reported
seven FastCDC chunks, two Zstd records, zero RAW records, and `PASS`. Artifacts
are retained under
`/source/fastdup/.artifacts/tier-meta/fuse-fastcdc-exact.ZkiJ3T` and
`/source/fastdup/.artifacts/tier-data/fuse-fastcdc-exact.TdRLTs`.

After bounded compaction and indexed graph proof were connected, the same
591,329-byte fixture was re-run through the current Policy Set on a fresh kernel
mount. Two names committed in separate checkpoint intervals retained Container
and active-Run counts `1 -> 1`; both compared byte-exactly before shutdown and
after remount. Startup recovered one Run with ten checkpoint workers and
`exact-index-degraded=false`. Full Container audit again reported seven chunks,
two Zstd records, zero RAW records, and `PASS`. Current artifacts are retained
under `/source/fastdup/.artifacts/fuse-validation/indexed-graph.B5CTvk`.

After paired Commit-Log rotation and hierarchical Manifest publication were
connected, a release kernel mount copied the complete pinned Rocky Linux 10.2
minimal ISO (2,072,444,928 bytes). Three 512-MiB pressure cuts closed mutation
admission while durable work caught up. The first run exposed that returning
the model's `Again` as kernel `EAGAIN` makes ordinary `cp` abort at the first
cut; the FUSE adapter now waits and retries the not-yet-applied mutation. The
same copy then completed in 13.47 seconds. Its live and post-unmount/remount
SHA-256 both matched the official source:
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
The largest published `.fdm` object was 61,440 bytes, demonstrating bounded
Manifest leaves/inner nodes rather than one flat file recipe. A later unlink
was durable as generation 10. Workspace-local artifacts remain under
`/source/fastdup/.artifacts/manifest-tree-iso.mount.JImxOc`,
`/source/fastdup/.artifacts/tier-meta/manifest-tree-iso.meta.FbR465`, and
`/source/fastdup/.artifacts/tier-data/manifest-tree-iso.data.2wZnXu`.

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

cargo test -p fastdup-posix -p fastdup-appliance --all-targets
cargo clippy -p fastdup-posix -p fastdup-appliance --all-targets -- -D warnings
MALLOC_MMAP_THRESHOLD_=131072 \
  cargo run -p fastdup-appliance --bin fastdup-durable-fuse -- \
  MOUNT_PATH METADATA_ROOT CONTAINER_ROOT
```

Production must additionally place the daemon in a no-swap cgroup as described
in [memory and swap containment](../operations/memory-and-swap.md).

## Explicitly incomplete

- The writer has FastCDC-v1, automatic level-zero Exact-Index publication, and
  deterministic four-way tiered compaction before the 64-active-Run reader
  bound. The current cold-path merge materializes at most 262,144 verified
  entries and fails the nonauthoritative index closed above that bound;
  streaming partitioned compaction remains required for the full capacity
  target. Per-DATA-region Chunking Profile IDs are not yet serialized in the
  Manifest tree, and Bloom/hot negative-lookup acceleration is not connected.
- Durable recipes now use content-addressed Manifest leaves and bounded inner
  nodes; unchanged leaves are reused and a change publishes only its new leaf
  and ancestor path. Planning and installed readers still flatten the complete
  extent recipe in memory, so CPU/RAM work remains O(number of file extents).
  Tree-native lazy reads and path-local mutation are required for 100-TB files.
- Sparse planning, FastCDC, and BLAKE3 identity construction are currently
  serial. Independent adaptive Compression Regions use bounded private worker
  outputs and a deterministic ordinal merge; pipeline-overlapped planning and
  per-worker reusable codec contexts remain to be measured and connected.
- Reachable-DATA graph verification uses verified persistent Locations during
  healthy indexed read-only recovery, writable recovery/reservation, and every
  checkpoint. Any missing, corrupt, stale, or unusable candidate invokes one
  complete verified scan because the index remains nonauthoritative. The
  verifier still walks every unique Chunk in the flattened graph, so hierarchical
  incremental proof reuse is required for 100-TB files. Container-generation
  discovery separately retains one O(number of Containers) mount-time envelope
  scan until it receives its own durable high-water record.
- The flat v1 Namespace Root and compatibility Manifest planner retain their
  documented metadata size limits.
- Atomic rename, links, nested directories, xattrs/ACLs, locks, allocation
  operations, normal read caching, automatic GC, scrub, and Samba conformance
  remain open.
- Commit-Log rotation is implemented through paired bounded slots. A durable
  Appliance Lease/format-epoch fence, offline slot scrub, fake-clock deadline
  proofs, `SIGKILL`/remount sweeps, and multi-process writer exclusion remain
  open.

The next recovery-safe scaling slice is tree-native lazy Manifest traversal and
path-local updates, followed by Metadata GC and a durable Container-generation
high-water.
