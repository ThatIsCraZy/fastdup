# Structural startup and background scrub — 2026-09-07

The test VM's 0.6.4-12 startup began at 01:01:04 CEST. The full Namespace DATA
proof began at 01:02:32 and completed at 01:31:28 (1,735,354 ms). The daemon
reported its mount at 01:31:28, about 30 minutes after startup. When sampled at
01:36:24 it had read about 55.5 billion physical bytes since process start; that
sample also includes post-mount work. The process did not crash or restart.
These are host-local observations, not VPN throughput.

The startup path previously required `recover_latest_with_verified_files_using`
to reverify every required DATA Chunk before enabling the Namespace. Periodic
Recovery Checkpoint publication also verified the complete DATA graph before
copying and again when validating the copied image. ADR 0090 implements the
operator-selected structural-start/background-scrub policy and documents its
changed verification timing and limits.

## Verification boundaries

A test backed by the real Container encoder forbids every range intersecting
a 256-KiB RAW payload during structural validation; less than 16 KiB is read.
A payload bit flip leaves structural startup valid but fails full decoding,
demand reads and actual background scrub. Separate cases corrupt the seal,
Record header, Chunk Table and Recovery Index. RAW, grouped Zstd, Prefix and
Sparse-XOR structure agrees with the complete decoder. Missing independent
Bases prevent mounting even when dependent Container structure is valid.

The writable Namespace test proves inode reservation and admission work after
structural recovery despite deferred payload corruption, and proves that
checkpoint resume cannot clear a scrub failure. An asynchronous POSIX test
checks that blocked mutation waiters wake with EIO. The real paced scrub path
is tested with intact/corrupt Containers and cancellation after its first
256-KiB portion: cancellation leaves the GC gate closed without claiming data
corruption. Runtime progress survives typed telemetry/history serialization and
appears in the UI across all detail tabs, including an explicit write-blocking
failure message.

Both full and committed-graph Recovery Checkpoint publication run the
before/after-every-operation crash matrix. A new test confirms that restoring
from a committed-graph copy still refuses corrupted DATA. Existing Namespace,
Manifest growth, recovery, fault-injection and maintenance suites cover the
unchanged writer and deletion barriers. No durable format migration is needed.

Artifacts: `.artifacts/structural-recovery/` contains tests, Clippy, UI build,
package build and VM verification logs. Background progress is intentionally
not a durable skip certificate; another restart starts a fresh initial pass.

## VM deployment of RPM 0.6.4-13

Before replacement, Samba reported no sessions, tree connections or open files,
and frontend read/write counters were zero. The previous process was explicitly
stopped and killed under the operator's existing restart authorization. The
repository was not reformatted or reinitialized. The binary RPM checksum was
verified before installation.

The new process (PID 57215, no automatic restarts) started at 01:58:45 CEST.
Structural Namespace verification ran from 01:58:55 to 02:11:50, reporting
775,728 ms; inode reservation took 6 ms. The daemon reported the mount at
02:11:50, approximately 13 minutes after startup versus the preceding start's
30 minutes. At the first mounted sample, 02:11:52, process physical reads were
1,527,091,200 bytes, including index/Metadata startup and the beginning of scrub.
These consecutive observations are not a controlled cold-cache benchmark.
About 27,000 Containers still require many small structural reads; this change
does not make startup constant-time or eliminate Metadata/index recovery.

Samba, repository, agent and control services are active. A VM-loopback SMB
3.1.1 test wrote 4 MiB in 1-MiB operations, flushed, overwrote its header,
appended a tail, flushed again, and read back all 4,194,320 bytes exactly.
SHA-256: `9e69f6141f4df9959a5be514931115decd647c79c8413260f3ef7cd95cbd3262`.
The temporary share file was removed. Its 0.278-second elapsed time is a small
functional probe, not a sustained Veeam or VPN throughput measurement.

Runtime telemetry showed scrub running over 26,960 published Containers,
387 verified, 696,549,376 verified bytes and 864,981,312 bytes read (including
Base reads), with no error. All five adaptive pools were present with DATA or
Metadata fallback tiers and the 9,200-basis-point shared memory ceiling.
The scrub thread has nice level 10; process swap is zero. The initial scrub is
still running; the live sample does not claim that the entire repository has
already passed verification. UI assets served by the installed control service
match `index-B7j5Fs4P.js` and `index-1V-NjXc5.css`.
