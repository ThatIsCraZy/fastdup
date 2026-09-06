# Structural startup and background scrub — 2026-09-07

The test VM's 0.6.4-12 startup began at 01:01:04 CEST. The full Namespace DATA
proof began at 01:02:32 and FUSE became available at 01:36:24, with about
55.5 billion physical process-read bytes since process start. The process did
not crash or restart. These are host-local observations, not VPN throughput.

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
