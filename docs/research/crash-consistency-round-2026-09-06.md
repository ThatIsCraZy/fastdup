# Crash consistency round — 2026-09-06

This round found and fixed two live-state failure-handling bugs. Neither the
successful recovery oracles nor the offline scrubs exposed mixed generations or
corrupt returned content. This is bounded test evidence, not a guarantee against
all device failure modes.

## Final result

| Check on the final source/binary | Result |
| --- | --- |
| Workspace tests | 781 passed, 0 failed, 16 normally ignored manual gates |
| Clippy, workspace/all targets, warnings denied | Passed |
| New exhaustive Frozen-replace fault matrix | 284 positions passed |
| Real randomized SIGKILL cases | 64 passed; 13,109 acknowledged operations |
| Real deadline matrix | 14 cases passed |
| Additional mixed/concurrent FUSE scenarios | 8 passed; 394 successful concurrent reads |
| Additional interrupted restarts | 6; final remounts passed |
| Final offline scrubs | 8 passed |

The two normally ignored real-SIGKILL tests were explicitly enabled for the
separate runs. In the mixed cases, immediate kills selected an allowed earlier
prefix; both 5.2-second and 11-second cases recovered the complete final image.
Both concurrent cases retained the exact 4-MiB committed prefix. Test-owned
repositories were archived, then removed from the two test disks; all owned
FUSE and bind mounts were detached.

## Fixes

### Allocation-page I/O after a durable Commit

`VersionedFile::install_commit_view` installed the new committed reader and
retired its Frozen epoch, then recomputed live allocation by walking Manifest
ranges. The walk can perform Metadata I/O. An injected failure on the first
Metadata operation after the WAL sync panicked with:

```
ASSERT: installed verified allocation metadata must remain readable: Io
```

The reproduction's Metadata operation 127 was the successful WAL sync; operation
128 (`ObjectLen`) failed. The stack ran through `publish_generation`,
`Namespace::complete_commit`, and `install_commit_view`. The already durable
Commit remains recoverable, but the live process could be left partially
installed with poisoned Namespace/inode locks.

Completion now retains the exact live allocation counter. Before any inode
installation, `complete_commit` already compares every verified reader's size,
allocation summary, and mutation sequence against the Frozen cut. Replacing
that exact prefix preserves the Active overlay. No fallible range read belongs
after that replacement. Independent DATA reads, recovery, and scrub still verify
stored dependencies.

The public `completing_a_verified_cut_does_not_reread_unavailable_allocation_pages`
regression was red before the fix. It checks an overlapping post-cut write,
exact size/allocation, and a subsequent distinct cut.

### Fallocate could fail after changing the Dirty epoch

Hole punching, Zero-Range, and ordinary allocation mutated the Dirty epoch and
then performed a full allocation-range recomputation. An unrelated unavailable
Metadata page could make the syscall return `EIO` after changing the epoch,
without updating the inode sequence or committing its quota reservation. A
subsequent read's relatime mutation reproduced a second panic:

```
ASSERT: dirty epoch must receive a contiguous inode sequence
left: 2
right: 3
```

All three operations now resolve the needed range metadata before mutation and
apply an exact allocation delta afterward. Punching subtracts allocated bytes
in the removed range; Zero-Range replaces its range's allocation; allocation
adds only the previously identified holes. The full post-mutation scan is gone.
This also avoids reading unrelated Manifest pages.

The public `fallocate_does_not_read_unrelated_allocation_pages_after_mutating`
regression covers all three modes, refusal when required pages are unavailable,
byte-exact reads, allocation counts, and a following write. No format or unsafe
code was introduced.

## Fault injection and recovery oracles

The new `every_frozen_replace_fault_excludes_later_truncate_and_unlink` test
injects failures before and after all **128 Metadata and 14 DATA operations**:
**284 cases**. Its fixture includes:

- An unaligned overwrite followed by shrink, sparse extension, and a distant tail.
- A Hardlink and atomic rename over an existing, still-open target.
- Writes to the replaced open orphan.
- A Frozen cut followed by a newer truncate, overwrite, rename, and unlink.

After dropping the appliance, both `MemoryStorageIo` instances crash and discard
unsynchronized state. Recovery must return exactly the previous generation or
the complete Frozen generation according to the WAL sync boundary. It checks
contents, EOF, zero-filled holes, inode identity, link counts, and absence of the
future name. The Active image must remain readable after publication failure.

The full workspace run additionally exercises existing Container/WAL/index
publication failpoints, paired-WAL rotation, torn Container/WAL/metadata images,
corrupt decode rejection, Metadata-tier loss and checkpoint restoration,
transient recovery I/O refusal, failed-publication retries, and capacity/quota
admission. These are different assertions within the suite, not claims that
every physical hardware error was reproduced.

## Real FUSE process crashes

All final real-daemon cases use the binary with both fixes:

```
SHA256 fb3ca35fdbe70906ad35b533b5b7a96619c89671ca4f98d6bb90b96485eeb9fc
```

Owned fresh fixtures use XFS `/dev/sdb1` for Metadata and `/dev/sdc1` for DATA,
bound beneath `.artifacts/bm` and `.artifacts/bd`. Existing repositories and the
system SMB service were not used. The test environment selects
`FASTDUP_POOL_ISOLATION=lab-allow-shared`, a 1-GiB Small-File quota, and Reduction
`off` or `dependent-v1` explicitly.

The existing harness runs 32 seeded randomized cases plus seven deadline offsets
per mode. Offsets are 0, 750, 2250, 4750, 5250, 9500, and 11000 ms. Every case
kills the daemon with SIGKILL and mounts a new daemon on the same stores. The
random harness deliberately kills between client operations, avoiding an
incorrect assertion that an in-flight, unacknowledged operation cannot persist.

The additional independent byte-model harness runs four cases per mode:

- Mixed operations, killed immediately, at 5.2 seconds, or at 11 seconds after
  the last mutation. Includes hardlinks, sparse truncate/extend, unaligned
  writes, all three fallocate modes, replacement rename, an open orphan, and
  an xattr. It compares all file names, lengths, link counts, and full-content
  hashes after each mutation and after recovery. The 11-second case also
  requires the final xattr.
- A 4-MiB committed prefix with a concurrent 64-KiB-record append stream and
  two readers, killed after 80 ms. Successful reads must return correct bytes;
  short reads and connection errors after the kill follow syscall semantics.
  Recovery must retain the committed prefix and contain only complete correct
  records, allowing at most the one issued but unacknowledged write.

Each 11-second mixed case also interrupts three restarts at 2, 15, and 50 ms
before the final successful remount. The first two attempts did not mount; the
50-ms attempts did. This covers interrupted startup and repeated ownership,
not a claim that each interruption reached a particular recovery phase.

Advanced mixed workloads actually generated **41 accepted Sparse-XOR targets
per case**. Similarity was exercised beyond simply setting a configuration flag.
Every additional scenario ends with a clean shutdown and a successful offline
scrub, eight scrubs in total.

## Reproduction and evidence

Artifacts are under `.artifacts/crash-consistency-20260906/`:

- `minimal-red.log`, `frozen-faults-diagnosis.log`: original Commit-install panic.
- `fallocate-red.log`: original failed-mutation sequence panic.
- `frozen-faults-green.log`: the 284-position matrix after the first fix.
- `final-tests.log`, `final-clippy.log`, `final-release-build.log`: final checks.
- `unusual-fuse.py`, `unusual-results.json`, `unusual-fuse-final.log`: extra real
  cases, independent byte oracle, and observed results.
- `run-existing.py`, `daemon-wrapper.py`, `all-fixes-existing-results.json`,
  `final-n2-sigkill.log`, `final-a2-sigkill.log`: final 78-case existing matrix.
- `final-verification.json`: final counts, hashes, and cleanup status.
- `final-source-sha256.json`: source provenance; `final-repositories.tar.gz`:
  final test repositories, including the armed post-SIGKILL repositories.
- `real-harness-evidence/`: copied per-case daemon and operation logs after
  removing the temporary disk directories.

The repository archive uses separate `metadata/` and `data/` trees. Harness
DATA symlinks retain their original absolute workspace paths; restoring it to
a different location requires redirecting those fixture symlinks to the
corresponding extracted DATA directories before mounting.

Cargo commands use `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`. Core regression invocations:

```
cargo test -p fastdup-posix --test commit_cut
cargo test -p fastdup-appliance --test durable_namespace_faults
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
```

Real harness scripts require fresh, short run paths and the owned bind mounts
above. An intermediate attempt exceeded Unix socket `SUN_LEN` after extending
the case label; final runs use short `n2`/`a2` roots. Early custom-harness
iterations were corrected to detach disconnected mounts using mountinfo and
to handle legal short reads/connection errors when killed. Those setup/oracle
errors are retained in earlier logs and are not filesystem corruption findings.

## Scope of the result

The durability oracle follows ADRs 0003–0006: every acknowledged mutation older
than ten seconds must survive on healthy supported storage; `fsync`, `fdatasync`,
and SMB FLUSH deliberately do not strengthen that guarantee. Younger accepted
mutations may be absent, but recovered generations may not mix their states.

SIGKILL leaves the host kernel and device caches alive. Loss of unsynchronized
state and torn images are tested in the storage model, not by physically cutting
power or resetting the storage controller. The round does not certify firmware,
flush honesty, arbitrary sector corruption, or the timing of every recovery
phase. No SMB throughput comparison is inferred from these crash tests.
