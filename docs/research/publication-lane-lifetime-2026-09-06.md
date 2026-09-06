# Truncate/checkpoint lane lifetime failure — 2026-09-06

## Production evidence

The Veeam retry on fastdup 0.6.4-6 aborted at 21:21:02 CEST in the
checkpoint worker with `detached per-inode publication sequence cannot move
backwards` (`checkpoint.rs:2516` in that package). The preceding three-minute
journal contained 23 checkpoints, with a maximum total of 0.846021244 seconds.
This is a different boundary from the ingest admission failure fixed in -6.
The journal alone does not identify which individual SMB request reset the lane.

## Reproduction and cause

`checkpoint_snapshot_survives_truncate_and_new_container_publication` exercises
public Namespace create/write/sync/open-with-truncate operations and the real
checkpoint/publication workers. It writes an 8-MiB random prefix, holds an empty
lower-inode lane, and waits until the checkpoint has frozen its Namespace cut
and captured the file's lane Arc. While that checkpoint is paused, the test
truncates the file and writes 36 MiB of different random content, forcing a new
full Container publication. Releasing the checkpoint reproduces the exact
production assertion on the old code.

The truncate barrier removed the registered lane even when a checkpoint still
owned it. A later write acquired a fresh lane, so the old captured lane could
publish an older sequence after the new lane. Normal LRU eviction already
requires exclusive registry ownership; invalidation had bypassed that rule.

The fix resets the shared lane in place, after releasing the registry lock.
Staging errors use the same reset. A failed detached publication only marks
acceleration degraded: its work has already left the lane, later lane contents
remain valid, and blocking its retirement on a producer lane could deadlock
publication backpressure. Resident Namespace data remains authoritative.
No durable format, publication sequence assertion, worker count, or queue
capacity changes. Normal sequential write admission and encoding are unchanged.

## Validation

The regression failed before the fix with the same assertion. It passes after
the fix and verifies both live replacement bytes and two crash-recovered cuts:
the first contains exactly the old prefix; the next contains the replacement.
Indexed in-memory recovery keeps this deterministic test around 1.4 seconds.

```
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test -p fastdup-appliance --lib \
  checkpoint_snapshot_survives_truncate -- --nocapture
```

Appliance library, write-through ingest, durable Namespace fault-injection, and
mount recovery tests: 97 passed, 3 intentionally ignored manual/resource tests.
Existing publication-failure tests verify resident fallback and exact recovery.
Clippy covers the appliance library and tests with warnings denied.

Local raw evidence is under `.artifacts/publication-lane/`: `before.log`,
`after.log`, `tests.log`, and `clippy.log`. The source test, rather than a
production scheduling hook, controls checkpoint overlap through its captured
lane ownership.

## Installed RPM and SMB verification

RPM `fastdup-0.6.4-7.el10.x86_64` was installed on `10.1.1.161`.
The prior runtime was stopped and its disconnected FUSE mount detached before
installation. Startup recovery completed, and the repository, control plane,
agent, and Samba were active. `rpm -V` reported only the existing repository
and generated Samba configuration modifications.

An encrypted SMB 3.1.1 test ran entirely on the VM through loopback; the VPN
carried only SSH orchestration. It wrote 10 GiB of random, repeated, locally
modified, and zero-containing content through irregular SMB request fragments.
After the first 2 GiB, a second connection repeatedly opened a file with
`FILE_OVERWRITE_IF`, alternated 8-MiB and 36-MiB random replacements, flushed,
read-verified each complete replacement by SHA-256, and closed the handle.
All 40 replacement cycles completed (880 MiB of additional writes and reads).

- Main write: 271.839 seconds, 37.669 MiB/s, including concurrent replacements.
- One-MiB write-series latency: p99 33.557 ms; maximum 50 ms. Each series
  contains several SMB requests; these are not individual request latencies.
- Complete main-file readback: 183.447 seconds, matching SHA-256
  `b42c23b5fc56ec4ff94da711aae80d39545f730ba451bcf5b8f3660113cd3fcb`.
- 483 checkpoint records across verification and cleanup, maximum total wall
  time 0.287136621 seconds; no runtime panic, critical error, or degradation.
- Runtime PID 33861 remained unchanged with zero service restarts. Temporary
  test files, share, and account were removed; only `veeam-test` remained.

The workload showed no throughput collapse. Its concurrent truncate workload
differs from the -6 handle-churn measurement, so it is not a controlled A/B
speedup claim. A complete Veeam backup still requires a client run.

Evidence: `.artifacts/publication-lane/smb-truncate.log`, `live-runtime.log`,
`verification.json`, and `verify-smb-truncate.py`. Release artifacts are under
`.artifacts/releases/v0.6.4-7/` and published on the existing v0.6.4 release.
Binary RPM SHA-256:
`697fd8e394696b65e98aa1d57352a81257efeee91fa7f55fd22265a904be5a3a`.
