# Real XFS/FUSE full-tier and Small-File-quota qualification

Status: implemented, privileged qualification test  
Last exercised: 2026-09-01

`crates/fastdup-testkit/tests/full_tier_enospc.rs` is the public-boundary
capacity-exhaustion proof. It creates workspace-local sparse XFS images rather
than mocking `statvfs` or `StorageIo`:

- 512 MiB Metadata XFS mounted with `prjquota`;
- 320 MiB DATA XFS on a distinct loop device;
- a 64 MiB inheriting XFS project hard limit for Small-File Containers; and
- the production `fastdup-durable-fuse` and `fastdup-maintenance` binaries.

The harness writes incompressible DATA through FUSE across as many checkpoint
and five-second capacity-observation cycles as necessary. A transient
reservation failure is not accepted as evidence. The tier is full only after
three writes separated by observation intervals return `ENOSPC`, rejected
bytes remain invisible, and client-visible availability is no greater than the
minimum pessimistic growth claim. It then repeats the process with policy-
selected `.json` files against the independent Small-File admission bucket and
XFS project quota.

At exhaustion, reads, unlink, `statfs`, graceful catch-up, offline scrub, and a
fresh daemon mount must remain usable. The remount verifies every acknowledged
DATA byte, one retained Small File, and both deletions. Images and logs remain
under the supplied run root for diagnosis; cleanup unmounts only the exact
scratch mountpoints created by that run.

## Invocation

The test is ignored by default because it requires root, `/dev/fuse`, loop
mount permission, `mkfs.xfs`, `xfs_io`, and `xfs_quota`.

```bash
mkdir -p /source/fastdup/.artifacts/target /source/fastdup/.artifacts/tmp
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo build -p fastdup-appliance \
  --bin fastdup-durable-fuse --bin fastdup-maintenance

CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
FASTDUP_DAEMON_BIN=/source/fastdup/.artifacts/target/debug/fastdup-durable-fuse \
FASTDUP_MAINTENANCE_BIN=/source/fastdup/.artifacts/target/debug/fastdup-maintenance \
FASTDUP_FULL_TIER_RUN_ROOT=/source/fastdup/.artifacts/full-tier-enospc-run \
cargo test -p fastdup-testkit --test full_tier_enospc -- --ignored --nocapture
```

## 2026-09-01 evidence

The privileged run at
`.artifacts/full-tier-enospc-live-20260901-v5` passed in 178.91 seconds:

- 182,974,592 DATA bytes were acknowledged before stable `ENOSPC`;
- presented DATA availability fell from 219,353,088 bytes to zero;
- 59,768,832 retained logical Small-File bytes occupied 59,985,920 allocated
  bytes inside the 64 MiB project quota;
- offline scrub verified Commit generation 23, 229 namespace inodes, 36
  Containers, and 3,538 Container chunks; and
- the recovery daemon reproduced the accepted DATA stream and retained Small
  File byte-for-byte while both unlinked files stayed absent.

This is real filesystem/quota/FUSE evidence. It is not a lying-device-cache or
power-loss test; those remain separately modeled by deterministic torn-write
fault injection and deployment qualification of the storage stack.
