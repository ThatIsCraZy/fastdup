# Metadata GC crash and Runtime health qualification, 9 September 2026

The test appliance running 0.7.2-1 aborted at 04:06:55 CEST with
`ASSERT: one newly committed Metadata identity enters one delta only`.
The preceding checkpoint took 77.273 s, including 67.591 s in Metadata commit.
Systemd recorded SIGABRT/core-dump, then repeatedly failed on the disconnected
FUSE mount. The agent continued reporting Online and the last 38.90 MB/s write
rate with no reachable Runtime. No OOM was recorded in the inspected window.

A deterministic public-seam regression reproduces the identical assertion:
commit a proof-bearing Manifest addition, rotate its old root out of the WAL,
start exact GC, drain an unrelated live pin during catalog publication, then
republish the collected content and commit its successor proof. The old code
kept the journal when the pin changed the epoch, although unlink had already
removed that content. The regression fails on the original assertion and passes
with exact-pass journal retirement. No assertion is disabled.

Exact GC now invalidates the old journal and catalog tail before fallible I/O.
A completed pass installs clean state only if both epoch and journal revision
remain unchanged. Pin changes or I/O failure force an exact follow-up. The fix
adds no work to the per-write DATA path and introduces no durable format change.

Qualification commands, with CARGO_TARGET_DIR and TMPDIR pointing under
`/source/fastdup/.artifacts/`:

- `cargo test -p fastdup-testkit --test end_to_end_maintenance`: 58 passed,
  including the scheduling regression and faults before/after final directory
  sync. Follow-up collection, scrub and recovery verify the committed graph.
- `cargo test -p fastdup-control --lib`: 39 passed, 1 manual benchmark ignored.
  The sample/inspect regression first failed with Online instead of Error. It
  now covers loss, stale rate invalidation, recovery, write pause, integrity
  failure, intentional unmount and one audit event per continuing outage.
- `cargo test -p fastdup-appliance --bin fastdup-durable-fuse`: 15 passed,
  including management admission status and disconnected-mount cleanup guards.
- `cargo clippy -p fastdup-store -p fastdup-control --lib -p fastdup-appliance
  --bin fastdup-durable-fuse`: passed.
- UI: 44 tests passed. The live SSE regression first failed on the missing
  alert, then verified persistent topbar failure, write pause and recovery.
- Browser: 1440 px and 390 px, repository failure badge and persistent alert;
  no page errors, horizontal overflow or overlap with the content below.

Captured logs and browser fixtures are workspace-local under
`.artifacts/crash-20260909-0407/`; fixtures are not live throughput measurements.
This qualification establishes the reproduced crash fix, not a completed Veeam
backup or a bound on every future checkpoint's duration.

Deployment qualification: RPM `fastdup-0.7.2-2.el10.x86_64` was installed on the
test appliance. SHA-256:
`feba6786d747867d9afc966a5864d99a7b10e9fb91f463bda9500078e5e22363`.
The daemon detached the pre-existing disconnected FUSE endpoint at 04:41:52
CEST and mounted at 04:42:16. Namespace Commit recovery took 7.131 s within that
24 s startup. Agent snapshots remained Mounting until Runtime telemetry became
available, then reported Online consistently in the binding and telemetry.
The deployed HTTPS endpoint served the new UI bundle. Authenticated SMB created
a temporary directory, wrote/read 8 MiB with matching SHA-256 and removed the
fixture. Repository, agent, control and Samba services remained active after
that probe. Background Scrub was still running; a full Veeam retry remains the
application-level follow-up.
