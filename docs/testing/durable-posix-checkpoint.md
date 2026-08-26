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
selection, level-zero publication, and four-way compaction decisions; these
rules cannot change silently under the earlier experimental constant ID.

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

The durable daemon supplies production `statfs` values from the data and
metadata backing filesystems. The client-visible total is physical data-tier
capacity after a ten-percent operating reserve. Available blocks also subtract
that reserve and fall to zero if the metadata tier reaches its own reserve.
The daemon never applies the observed reduction ratio to these values. It
refreshes the physical observation every five seconds outside the FUSE request
path. Each `statfs` request reads only the cached snapshot, because Samba may
ask for free space during an active write.

Two paired environment variables provide a reporting-only override:
`FASTDUP_STATFS_FAKE_CAPACITY_BYTES` and
`FASTDUP_STATFS_FAKE_AVAILABLE_BYTES`. Both contain unsigned decimal bytes and
must be set together with available not greater than capacity. The override
does not weaken physical write admission, so a later write may still return
`ENOSPC` before the presented capacity is consumed.

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

Writer verification now survives as a bounded opaque online dependency proof.
One public checkpoint test publishes 2 MiB of DATA and requires zero
`ReadExactAt` operations between the mandatory writer reread and Commit-WAL
visibility; it observed 32 redundant Record reads before the change. A second
public test writes an identical file in a later generation and likewise
requires zero DATA reads. Recovery and the existing fault matrices reopen with
an empty online-proof cache and continue to verify durable DATA independently.

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

Write-through reduction is independently asynchronous. A write first updates
the authoritative POSIX Dirty Extent Map with one owned immutable Mutation
Payload, then admits zero-copy shared views of at most one MiB to a global
16-MiB Ingest Queue, and returns without waiting for FastCDC, compression, or
Container durability. One permanent bounded worker pool executes at most one
job per inode while allowing different inodes to advance concurrently. Live
reads therefore observe accepted bytes even while the data tier is blocked. A
full Ingest Queue blocks only the admitting request after the inode-data lock
has been released; it does not close global mutation admission.

The lane Tail is segmented rather than one shifting `Vec`: appending a Queue
view is zero-copy, FastCDC compacts at most its 256-KiB maximum lookahead, and a
Pending Chunk remains a slice of that compacted buffer. The deterministic
differential test compares hostile input segmentation with the pinned
contiguous FastCDC-v1 boundary sequence. Container publication performs three
ordered writes (Building Header, complete Body/Footer, Sealed Header) rather
than one syscall per 4-KiB format page; the same fail-before/fail-after matrix
still requires absent-or-fully-verified recovery.

On the 2026-08-21 test host, `strace` observes exactly three `pwrite64`, two
`fsync`, and one `renameat2` calls for one complete Container publication. The
kernel was built with `CONFIG_IO_URING=y`, but the runtime policy reports
`kernel.io_uring_disabled=2`; Linux defines this value as disabling
`io_uring_setup()` for every process with `EPERM` ([kernel sysctl
documentation](https://www.kernel.org/doc/html/v6.15/admin-guide/sysctl/kernel.html#io-uring-disabled)).
An io_uring Storage adapter is therefore not enabled on this host. Even on an
enabled host, v1 first requires evidence that batching multiple independent
Container writes or syncs beats the existing bounded publisher threads;
wrapping one large write and its strictly ordered verify/sync/rename chain is
not a sufficient gate. FUSE-over-io_uring is separately still described as an
in-development interface with incomplete request coverage ([kernel FUSE
io_uring documentation](https://cdn.kernel.org/doc/html/latest/filesystems/fuse/fuse-io-uring.html#limitations)).

`Sync`, `Release`, and checkpoint planning fence the relevant per-inode
mutation sequence. The fence waits for reduction processing, but `Sync` does
not create a private durable generation or strengthen the accepted system-wide
commit window. Zero-payload barriers represent truncate, unlink, metadata
clone, and rename-overwrite in the same sequence space. Compression permits
cover only CPU preparation and are returned before immutable Container I/O, so
one stalled data-file sync cannot retain the complete encode budget.

This is a bounded-progress mechanism, not a claim that arbitrarily stalled
hardware can meet a wall-clock deadline. Arbitrarily stalled total I/O remains
outside the supported failure envelope in ADR 0007. The public
[`SIGKILL`, remount, and deadline harness](sigkill-remount-deadline.md) now
proves complete-prefix recovery inside the window and full recovery after ten
seconds on the real mount. A fake-clock stalled-I/O deadline test and persisted
appliance-health state remain open.

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

The asynchronous write-through integration tests pause Container file sync and
prove through the public Namespace seam that writes still return and remain
live-readable, `Release` waits for its final queued sequence, two files reach
data-tier durability concurrently, and admission blocks only after the explicit
16-MiB queue fills. A separate unlink-during-reduction tracer proves that its
zero-payload barrier cannot be skipped and cannot leave `Release` waiting on an
unobserved sequence.

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

The Exact Index Activation Log no longer stops at the former 16,384-record
limit. A filesystem-backed migration tracer fills the complete legacy 64-MiB
file, activates generation 16,385 through the second slot, reopens it, and
recovers that exact Run Set. A separate 130-activation tracer crosses two
ordinary rotation boundaries while keeping both slot files at or below 256 KiB.
The deterministic backend fails before and after every first-rotation operation;
only an effective final target-slot sync may expose generation 65. Writer,
recovery, and offline audit all reject an authenticated-chain failure in the
inactive peer.

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

After prepared extent recipes were connected to the Frozen Commit Cut, another
fresh release mount copied the same complete ISO in 22.84 seconds through FUSE
(90.74 MB/s logical). Stable full-Container cuts commonly adopted about
66.5 MB directly and passed only 0.35--0.61 MB through checkpoint FastCDC. A
34-MiB public integration test additionally overwrites one byte inside an
externalized Chunk, bounds all checkpoint rechunking below 4 MiB, crashes both
stores, and verifies the complete recovered file byte-for-byte.

The complete ISO SHA-256 before restart and after a new daemon recovered the
mount was the official
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
The initial verified read took 32.00 seconds and the recovered read 33.49
seconds. That run exposed two distinct races: a Container completed after its
mutation had entered the Frozen Cut, and a checkpoint cut a stable but
not-yet-full Ingest Lane. The first was fixed by byte-verified late recipe
attachment; the second by fencing already admitted observers at cut formation
and draining complete FastCDC chunks immediately after the cut.

The complete pinned ISO was then copied again through a fresh release mount.
The copy completed in 47.88 seconds and produced 49 generation checkpoints.
Across them, 2,054,849,812 bytes were adopted directly as prepared recipes and
20,209,047 bytes passed through checkpoint FastCDC. The maximum per-generation
rechunk was 582,760 bytes, down from the observed 28--34 MiB spikes and bounded
near the incomplete CDC suffix plus a boundary chunk. Live and post-restart
SHA-256 both matched the official
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
Artifacts are retained under `recipe-cut-fenced.meta.YUq65F` on the metadata
tier, `recipe-cut-fenced.data.sRajqp` on the DATA tier, and
`/source/fastdup/.artifacts/recipe-cut-fenced.log`.

The next append-native Manifest successor slice retained the accepted 500-ms
full-Container coalescing policy and removed the growing complete-tree commit
scan from sequential appends. A fresh release copy of the same pinned ISO
completed in 35.02 seconds with 43 checkpoints. Total checkpoint wall/CPU time
was 18.79/27.46 seconds and Metadata wall/CPU time was 8.76/17.81 seconds,
compared with 28.36/41.08 and 10.98/24.05 seconds in the preceding 47.88-second
run. The checkpoints reused 2,056,954,238 recipe bytes, rechunked 15,490,690
bytes, and never buffered more than 570,039 reduction bytes. Live and
post-restart SHA-256 both matched the official source. Artifacts are retained
under `append-proof-500ms.meta.LpYKYs`, `append-proof-500ms.data.fyiszJ`, and
`/source/fastdup/.artifacts/append-proof-500ms.log`.

A separate 2-second coalescing experiment reduced commit count but regressed
the complete copy to 51.84 seconds as larger generations increased Metadata
work. That policy change was rejected; 500 ms remains the accepted value.

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

The online dependency cache now separates correctness pins from historical
acceleration. Active and Frozen Generation Proof Sets together retain at most
65,536 verified Locations and never enter eviction. A successful commit moves
only its Frozen set into the sharded Historical S3-FIFO cache. A failed commit
keeps the same Frozen set for retry. Historical admission uses a dynamic
two-percent-of-effective-RAM target, a shared headroom reserve, 224 accounted
bytes per proof, and an immediate purge when Swap use is observed. Cache lookup
matches the full Chunk ID and logical length. The daemon reports hits, misses,
Ghost hits, admission/allocation rejections, evictions, target and resident
bytes, maximum eviction steps, available RAM, and Swap use after checkpoints.

## Explicitly incomplete

- The writer has FastCDC-v1, automatic level-zero Exact-Index publication, and
  deterministic four-way tiered compaction before the 64-active-Run reader
  bound. The cold-path merge now makes two complete source-audit passes and
  retains only one verified 4-KiB page per source, one heap entry per source,
  and one output page. The public 262,145-entry tracer completes in 1.95 s with
  37,468 KiB maximum RSS and zero swap on the development host. Partitioned Run
  families remain required above the Run-v1 one-GiB output-object bound.
  Per-DATA-region Chunking Profile IDs are not yet serialized in the Manifest
  tree. Active Runs now build the existing cache-line-aligned blocked Bloom
  hint during their mandatory full audit. A definite absence bypasses that
  Run's Exact pages; a positive remains untrusted and follows the complete
  verified lookup. Filter memory and absent/maybe probes are reported
  independently from the pressure-bounded Exact Index page cache.
- Durable recipes use content-addressed Manifest leaves and bounded inner
  nodes. Installed reads are tree-native; equal-length changes publish only
  replacement leaves and ancestor paths, and sequential appends additionally
  commit from an opaque right-spine successor proof. Equal-length replacements
  now extend that proof from their touched paths. Truncate and arbitrary
  length-changing middle splice/concat are also tree-native and preserve
  complete shifted suffix-subtree identities.
- FastCDC for one stream and its Manifest edit planning remain ordered. The
  write-through path overlaps the next Container's CDC with durability of the
  prior detached Container. BLAKE3 shards, independent Compression Regions,
  Similarity fingerprints, Delta trials, Reorder keys, and maintenance
  verification share one permanent quota-sized work-stealing pool. Results
  merge by deterministic ordinal, and each pool worker retains its private
  Zstd codec context. Commit-time planning of independent inode tails remains
  serial because the common Exact set and 32-MiB Container packing cross inode
  boundaries; most long-stream bytes have already left that path through
  write-through reduction.
- Reachable-DATA graph verification uses verified persistent Locations during
  healthy indexed read-only recovery, writable recovery/reservation, and every
  checkpoint. Any missing, corrupt, stale, or unusable candidate invokes one
  complete verified scan because the index remains nonauthoritative. The
  legacy fallback verifier still walks every unique Chunk in the graph. New
  files, sequential appends, and equal-length replacement successors verify
  only introduced DATA. Every such proof is fenced to the exact installed
  Commit Record and stale proofs are rejected before dependency verification.
  Container-generation discovery separately retains one O(number of Containers)
  mount-time envelope scan until it receives its own durable high-water record.
- The bounded v2 Namespace Root stores regular and directory inode versions
  plus nested byte-exact entries. `mkdir`, empty-only `rmdir`, `..`, link counts,
  cycle rejection, cross-parent rename, recovery, and scrub share the same
  namespace rules. The compatibility Manifest planner retains its documented
  metadata size limits.
- Hardlinks, symlinks, ownership, timestamps, and xattrs/ACLs are durable and
  connected through FUSE. BSD `flock` and broad Samba/Veeam conformance remain
  open. Metadata-only allocate, punch, zero, DATA/HOLE seek, collapse, and
  insert share the durable POSIX seam. The first four reach a real FUSE mount;
  Linux FUSE rejects collapse/insert flags before dispatching them to
  userspace. Volatile POSIX record
  locks are connected through FUSE but intentionally do not enter the durable
  namespace. Atomic replacement rename, bounded
  verified read caching, offline end-to-end scrub, and RoW Exact-Index rebuild
  are implemented; the maintenance path is documented in
  [scrub and Exact-Index rebuild](../operations/scrub-and-exact-index-rebuild.md).
- Commit-Log rotation is implemented through paired bounded slots. A bounded
  real-process `SIGKILL`/remount/deadline matrix is green. The exclusive
  kernel-backed Appliance Lease now prevents a second daemon or offline
  maintenance process from opening the repository. A stable format-epoch
  fence, fake-clock stalled-I/O proofs, and broad randomized
  process-kill/power-cut campaigns remain open.

The next recovery-hardening slice is the fake-clock stalled-I/O proof and
broader process-kill/power-cut campaigns, followed by a durable
Container-generation high-water and stable format-epoch fence.
