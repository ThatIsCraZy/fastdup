# OpenZFS filesystem-fault tests applicable to fastdup

Status: research note, not an accepted architecture decision  
Last reviewed: 2026-09-01  
OpenZFS source revision: `58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7`

## Question

Which tests and test patterns in the official OpenZFS repository exercise
failure invariants that fastdup should adopt, and which are specific to ZFS
device redundancy or features outside fastdup's contract?

## Conclusion

The most useful OpenZFS contribution is its **oracle shape**, not a direct port
of its shell tests. fastdup should retain its deterministic operation-by-
operation `StorageIo` matrices, then add a smaller public-boundary layer for
random process death, full-pool behavior, decode failures, and scrub error
lifecycle.

The highest-priority transferable cases are:

1. repeatedly kill a child at random times and reopen the same durable state;
2. corrupt data and metadata independently and require online read, recovery,
   and offline scrub to agree;
3. fail compressed-data decoding without returning unchecked or partial bytes;
4. fill the physical backing tiers, continue returning stable `ENOSPC`, keep
   deletion and diagnosis usable, and recover a complete committed prefix;
5. stall or fail durable I/O and require admission to close without false
   success or a mixed generation; and
6. keep corruption visible until a later complete verification proves that the
   affected live graph is healthy.

## Implemented from this review

`crates/fastdup-appliance/tests/openzfs_fault_oracles.rs` now pins the two
deterministic P0 boundary oracles that were not previously expressed together:

- an authenticated corrupt Zstd frame reaches the decoder, makes the public
  POSIX read fail wholly with `EIO`, admits no Verified Read Cache entry, and is
  rejected by offline scrub; and
- corrupt, truncated, and missing newest Namespace Root objects recover only
  the immediately previous complete generation, while exhaustive scrub still
  reports the damaged retained generation; and
- a deliberately suspended DATA sync preserves live acknowledged bytes, then
  resumes to exactly one Commit and recovers the byte-exact file after a modeled
  crash and remount;
- a newest-Container durable torn write rejects the damaged generation,
  recovers a Container-independent previous generation, and remains visible to
  exhaustive scrub;
- a fixed-seed real-process SIGKILL soak exercises mixed namespace and sparse
  file mutations and accepts only acknowledged public-view prefixes; and
- a privileged real-XFS/FUSE harness fills DATA and the independent Small-File
  project quota, proves stable `ENOSPC`, cleanup, scrub, and byte-exact remount.

Scheduled longevity execution, real block-device power cut, the ambiguous-sync
cross-product, and persistent scrub-attribution lifecycle remain qualification
work.

Mirror repair, RAIDZ/dRAID tolerance, spares, resilvering, ZED policy,
encryption, and arbitrary historical pool rewind are not fastdup MVP test
oracles. Device redundancy is delegated to the block layer and fastdup exposes
only the current and immediately previous Commit generations as recovery
candidates.

## Primary-source basis

OpenZFS' `ztest` explains the core stress pattern directly: simple functional
operations run concurrently; faults are injected; a child kills itself with
`SIGKILL` at random times; and the parent reuses the existing pool to verify
on-disk consistency. It also embeds transaction-generation numbers in data to
detect future-state leakage into older data. The driver accepts a configurable
kill percentage. [`cmd/ztest.c`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/cmd/ztest.c),
[`ztest(1)`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man1/ztest.1)

OpenZFS' `zloop.sh` runs `ztest` repeatedly with randomized arguments and
preserves logs, vdev files, and cores when a crash is encountered. This is a
longevity wrapper, distinct from deterministic fault-position tests.
[`scripts/zloop.sh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/scripts/zloop.sh)

`zinject` can simulate checksum corruption, post-read bit corruption,
decompression/decryption failures, `EIO`/`ENXIO`, probe failures, I/O latency,
and a device that does not honor cache flushes. That catalog is useful when
defining fastdup's fault vocabulary, but it does not imply that every ZFS
device fault belongs inside fastdup. [`zinject(8)`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/man/man8/zinject.8)

## Applicability matrix

| Priority | Official OpenZFS case | OpenZFS invariant | fastdup adaptation and public seam | Current coverage / gap |
| --- | --- | --- | --- | --- |
| P0 | `cmd/ztest.c`, `scripts/zloop.sh` | Random `SIGKILL` followed by reopening the same pool never loses on-disk consistency; tagged generations do not leak future state into an older view. | Run a fixed-seed, bounded long-ingest workload through `fastdup-durable-fuse`; randomly kill the daemon, remount the same Metadata/Data roots, and accept only a byte-exact committed prefix. Include create/write/truncate/rename/unlink/sparse operations and record the seed and kill offset. | The deterministic matrices and `SigkillRemountConfig` already establish the oracle, but `crates/fastdup-testkit/tests/sigkill_remount_deadline.rs` has seven fixed offsets and is ignored by default. The gap is a randomized, repeated real-XFS/FUSE soak with retained evidence. |
| P0 | `checksum/filetest_001_pos.ksh` | Corrupting level-0 data is detected by scrub; clean data does not produce false checksum errors. [`filetest_001_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/checksum/filetest_001_pos.ksh) | Corrupt one Container record/payload byte after publication. `ContainerRepository::read`, verified Manifest reads, recovery proof, and offline scrub must never return unchecked logical bytes. | Container and maintenance tests cover selected corruption cases. Add one table-driven cross-boundary matrix so the same injected object is checked through read, recovery, and scrub. |
| P0 | `checksum/filetest_002_pos.ksh` | Corrupting level-1 metadata is detected after export/import rather than being mistaken for valid data. [`filetest_002_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/checksum/filetest_002_pos.ksh) | Independently corrupt/truncate/remove/substitute bytes in Commit slots, Namespace shards, Manifest nodes, Recovery Checkpoints, Exact activation state, and index dependencies. Exercise `recover_mount`, repository readers, and offline audit; only a complete permitted fallback or a closed mount is valid. | Many repository-specific tests exist in `crates/fastdup-testkit/tests/*_faults.rs`. The gap is a shared durable-object matrix proving consistent classification at writer, recovery, and scrub boundaries. |
| P0 | `fault/decompress_fault.ksh` | Injected decompression failures make the read fail and are observable; the read is not reported as successful. [`decompress_fault.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/fault/decompress_fault.ksh) | Inject decoder failure and decoded-length/checksum mismatch for Zstd and depth-1 Prefix records. Read through the FUSE/public Namespace seam and require `EIO`, no prefix bytes, no cache insertion, and a scrub finding for the same physical object. | Codec unit tests reject malformed inputs, and verified reads exist. Add an end-to-end FUSE error-mapping case and verify that a failed read cannot populate `VerifiedReadCache`. |
| P0 | `no_space/enospc_001_pos.ksh` | A filesystem that reached `ENOSPC` returns `ENOSPC` again for a later write rather than hanging or changing error class. [`enospc_001_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/no_space/enospc_001_pos.ksh) | Fill Data and Metadata capacity separately through the mounted filesystem. Repeated mutations must return `ENOSPC`; rejected mutations must not enter the live Namespace or consume IDs/generations; remount must expose the last complete Commit. | `CommitCapacityGovernor` tests cover reservation accounting at the shared POSIX seam. The gap is a real quota/full-filesystem FUSE test against physical tier boundaries. |
| P0 | `no_space/enospc_rm.ksh` | Files can still be removed after the filesystem is full. [`enospc_rm.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/no_space/enospc_rm.ksh) | When new growth is rejected, unlink, truncate/shrink, and GC needed to recover capacity must remain admissible under the reserved Commit budget; crash at every cleanup publication point and reopen. | Unit tests cover sparse shrink and active create/unlink claim release. Add the public full-tier test because physical directory/Commit publication can still fail after logical admission. |
| P1 | `no_space/enospc_002_pos.ksh`, `no_space/enospc_df.ksh` | Selected administrative operations and scrub remain usable on a full pool, and `df` reports a meaningful nonzero filesystem. [`enospc_002_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/no_space/enospc_002_pos.ksh), [`enospc_df.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/no_space/enospc_df.ksh) | At zero client-visible availability, `statfs`, offline scrub, and read-only recovery remain usable. `StatFsSource` must report bounded nonzero capacity/used values and zero available space without arithmetic collapse. ZFS snapshot/clone/property commands are not part of the adaptation. | `TieredStatFsSource` has arithmetic tests and a reporting reserve. Add one mounted full-tier integration case and invoke `fastdup-maintenance scrub`. |
| P0 | `failmode/failmode.kshlib`, `syncfs/syncfs_suspend.ksh` | When backing I/O fails and the pool suspends, synchronous operations either block until recovery or return an error; `syncfs()` must not return success while suspended. [`failmode.kshlib`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/failmode/failmode.kshlib), [`syncfs_suspend.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/syncfs/syncfs_suspend.ksh) | fastdup deliberately does not give `fsync` a stronger durability boundary. The applicable oracle is instead: a stalled DATA or Metadata sync closes new mutation admission by the guard deadline; already accepted writes remain live; recovery exposes no mixed generation; an actual I/O failure is surfaced as `EIO`/closed admission rather than false commit success. Exercise `DurableNamespace`, `checkpoint_action`, and FUSE error mapping. | `stalled_io_admission.rs` covers fake-clock DATA and Metadata sync stalls. Add failure-after-effect and resume cases through the daemon boundary, including an ambiguous final sync. |
| P1 | `fault/suspend_resume_single.ksh` | After device return and resume, previously buffered data completes and remains byte-exact after export/import. [`suspend_resume_single.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/fault/suspend_resume_single.ksh) | Pause one `StorageIo::sync_file`/`sync_root`, close new admission, resume, then require the frozen Commit to complete exactly once. Kill before and after resume to cover ambiguous completion. | `PausedStorageIo` and stalled-admission tests supply the seam. The cross-product of resume, fail-after-effect, and crash is not one named end-to-end matrix. |
| P1 | `cli_root/zpool_scrub/zpool_error_scrub_003_pos.ksh` | A targeted error scrub retains errors while corruption persists and clears them only after the fault is removed and a later scrub verifies health. [`zpool_error_scrub_003_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/cli_root/zpool_scrub/zpool_error_scrub_003_pos.ksh) | A fastdup scrub finding remains actionable until a complete later audit proves the selected live graph healthy. Repair must publish a new immutable verified Location/generation; it must not overwrite damaged evidence. A failed/partial scrub cannot authorize GC or clear the recovery latch. | Scrub rejects corruption and GC consumes bound proof, but persistent finding/clear lifecycle is not represented as a single public maintenance test. |
| P1 | `cli_root/zpool_status/zpool_status_003_pos.ksh` | One corrupt physical block is attributed to every affected filesystem/snapshot/clone name. [`zpool_status_003_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/cli_root/zpool_status/zpool_status_003_pos.ksh) | One corrupt shared Location may affect several Exact-deduplicated files, range clones, hardlinks, and both live recovery candidates. Test that every read fails closed and scrub impact reporting does not undercount logical consumers. Do not import ZFS snapshot semantics. | Byte-integrity coverage exists; complete logical impact attribution for shared physical data appears to be a reporting gap. |

## Existing fastdup foundations to reuse

The proposed tests should use existing public/deep seams rather than creating
test-only access to format internals:

- `fastdup_store::StorageIo` is the durable I/O boundary. The testkit's
  `MemoryStorageIo` models live versus durable state, fail-before,
  fail-after-effect, wrong-object read substitution, and crash; `PausedStorageIo`
  models selected stalls. See `crates/fastdup-testkit/src/lib.rs` and
  [ADR 0025](../adr/0025-make-fault-injection-and-benchmarks-the-first-milestone.md).
- Repository fault matrices already cover Container publication, Commit and
  Manifest generations, Exact publication/activation, GC catalogs, and
  Recovery Checkpoints under `crates/fastdup-testkit/tests/`.
- `recover_mount` and `DurableNamespace` are the normal recovery and visibility
  seams. The real-process oracle is `SigkillRemountConfig`, documented in
  [the SIGKILL/remount test note](../testing/sigkill-remount-deadline.md).
- `FuseFilesystem` maps `PosixError::NoSpace` to `ENOSPC` and `PosixError::Io`
  to `EIO`; `StatFsSource` is the mounted capacity-reporting seam.
- Offline maintenance already supplies scrub/rebuild and GC proof boundaries;
  tests belong at that API/CLI boundary, not inside checksum helpers.

Every new durable corruption case must be checked at writer/reread,
reader/recovery, and offline scrub boundaries, consistent with
[ADR 0022](../adr/0022-separate-assert-verify-and-audit.md). Crash publication
tests must continue to accept only the previous or complete new generation,
consistent with [ADR 0019](../adr/0019-commit-only-after-data-and-metadata-are-durable.md)
and [ADR 0037](../adr/0037-separate-structural-recovery-from-current-data-proof.md).

## Explicitly non-applicable OpenZFS groups

| OpenZFS group/case | Why it is not a fastdup test oracle |
| --- | --- |
| `functional/redundancy`, `functional/scrub_mirror`, `fault/auto_spare_*`, `fault/auto_replace_*`, `fault/auto_online_*` | These verify mirror/RAIDZ/dRAID redundancy, ZED policy, device replacement, and self-healing. fastdup delegates device redundancy and degraded-array behavior to the block layer. The transferable sub-oracle is only detection/no unchecked bytes. Examples: [`scrub_mirror_001_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/scrub_mirror/scrub_mirror_001_pos.ksh), [`auto_spare_001_pos.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/fault/auto_spare_001_pos.ksh). |
| `fault/decrypt_fault.ksh`, encrypted scrub cases | fastdup has no durable encryption/key-unload contract. Do not disguise codec-integrity tests as encryption coverage. [`decrypt_fault.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/fault/decrypt_fault.ksh) |
| `zpool_scrub_offline_device.ksh`, DTL/resilver tests | Dirty-time logs and resilver correctness are ZFS vdev-replica mechanisms. Missing fastdup physical dependencies instead cause one atomic fallback or a closed mount. [`zpool_scrub_offline_device.ksh`](https://github.com/openzfs/zfs/blob/58d73c90dcdd77cdd07fc0b53d12d1b7339d2fe7/tests/zfs-tests/tests/functional/cli_root/zpool_scrub/zpool_scrub_offline_device.ksh) |
| ZFS snapshots, clones, rollback, send/receive, and arbitrary import rewind | fastdup does not expose row history as snapshots and normal recovery may consider only current and immediately previous generations. Range clone and hardlinks should be tested as shared-data fanout, not as ZFS clone semantics. |
| `failmode` expectations for `fsync`, `O_SYNC`, `msync`, and `sync=always` as stronger persistence points | fastdup's accepted contract deliberately makes `fsync`, `fdatasync`, SMB `FLUSH`, `O_SYNC`, and `O_DSYNC` no stronger than its ten-second system window. Only the no-false-success/error-propagation and admission-stall ideas transfer. See [ADR 0003](../adr/0003-fsync-does-not-strengthen-durability.md). |
| `zinject -I` ignored-cache-flush hardware model as a correctness promise | fastdup requires an honest stable-storage stack with working Flush/FUA or power-loss protection. Such a test is a deployment rejection/qualification test, not a promise to recover from unsupported lying hardware. See [ADR 0028](../adr/0028-require-an-honest-stable-storage-stack.md). |

## Recommended implementation order

1. Add the table-driven **data-versus-metadata corruption matrix** and the
   end-to-end compressed decode `EIO` case. These are deterministic and protect
   the most important invariant: no unchecked logical bytes.
2. Add one **real full-tier FUSE test** covering repeated `ENOSPC`, shrink and
   unlink, `statfs`, scrub, crash, and remount. Run it against workspace-local
   XFS/project-quota fixtures once quota isolation is available.
3. Extend the existing ignored SIGKILL harness into a fixed-seed **randomized
   zloop-style soak**, retaining the seed, operation log, kill offset, daemon
   logs, and repository image on failure.
4. Add **stalled/resumed/ambiguous-sync** cases by composing
   `PausedStorageIo`, fail-after-effect, crash, and fake time.
5. Add the **scrub finding lifecycle and shared-Location attribution** test when
   the maintenance reporting format has a stable public seam.

The deterministic suite remains the merge gate. Real FUSE/XFS, quota, and
long-running randomized cases should be separately labeled qualification or
longevity gates so a missing privileged environment is not mistaken for a
passing fault test.
