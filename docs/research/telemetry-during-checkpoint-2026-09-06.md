# Runtime telemetry during checkpoints — 2026-09-06

## Failure observed on 0.6.4-7

The UI showed no Detail Telemetry while multiple Veeam backup files were open
for writing and the runtime continued checkpointing without a restart. A direct
read-only management `inspect` request timed out after ten seconds under this
load. The control sampler's deadline is only 400 milliseconds.

Two independent dependencies made observation wait on storage work:

1. The supervisor accepted management connections inside the same `select!`
   that awaited a complete checkpoint. A selected checkpoint branch prevented
   further accepts until that branch returned.
2. `runtime_telemetry::snapshot` obtained reduction counters through the full
   `write_through_status`, which locks every ingest lane and computes buffer
   accounting. A lane can retain its lock while applying publication queue
   backpressure, although the required reduction counters are already available
   through the index's atomic telemetry interface.

The new regression `reduction_telemetry_does_not_wait_for_busy_ingest_lanes`
failed with the old status path at the actual 400-ms sampler deadline, then
passed with a direct reduction-counter accessor. It releases the held lane and
joins the observer even in the failing case.

## Change

The management listener now runs in its own task, independent of checkpoint
awaits. A `JoinSet` bounds accepted active requests to 16. Normal shutdown stops
acceptance, cancels and joins outstanding handlers before repository shutdown;
dropping the server also aborts its task and children. The existing root-only
socket permissions and bounded request read timeout remain.

Reduction telemetry bypasses ingest buffer locks. This changes neither storage
admission, queue capacities, worker parallelism, checkpoint safety thresholds,
nor durable formats. The UI's empty-state text describes unavailable telemetry
and automatic refresh rather than suggesting that the mount is insufficient.
Missing observations are not replaced with fabricated zeroes or stale values
presented as current.

## Verification

- 52 appliance library tests passed; two existing manual tests ignored.
- 10 runtime binary tests passed, including inspection within 400 ms while the
  supervisor waits, a simultaneous unfinished client, bounded active request
  count, cancellation on shutdown, and root-only socket permissions.
- Appliance library/binary/test Clippy passed with warnings denied.
- 31 UI tests passed; the production TypeScript/Vite build succeeded.

Use workspace-local Cargo target and temporary directories. In a long worktree
path, explicitly override `.cargo/config.toml`'s forced relative TMPDIR so Unix
socket fixtures fit `SUN_LEN`:

```
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo --config 'env.TMPDIR.value="/source/fastdup/.artifacts/tmp"' \
  test -p fastdup-appliance --lib --bin fastdup-durable-fuse
```

Evidence is retained under `.artifacts/telemetry-under-load/`, including the red
regression, test and Clippy output, UI checks and package build. The live Veeam
runtime remains on revision -7 while its backup handles are open. Activating
revision -8 requires a runtime restart after the backup has finished; no live
installation or successful live telemetry verification is claimed here.
