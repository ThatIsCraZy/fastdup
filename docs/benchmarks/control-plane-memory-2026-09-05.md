# Control Plane telemetry memory and UI responsiveness

The running management slice was stalled at its 768 MiB MemoryHigh limit,
with roughly 782 MiB anonymous memory. The agent accumulated decoded historical
samples during hourly aggregation. Every raw measurement also repeated up to
900 live chart points. The Control Plane shared the same memory throttle.

## Reproduction and regression coverage

`cargo test -p fastdup-control --lib historical_samples_do_not_repeat_live_chart_series`
failed before the change because a single historical measurement contained the
entire live chart. The manual ignored `telemetry_rollup_memory_probe` inserts
3,600 legacy measurements, each with 900 chart points, and runs aggregation.
It uses a temporary SQLite database under workspace-local TMPDIR.

Direct test executable measurements with `/usr/bin/time -v`, on this host:

| Measurement | Before | After |
| --- | ---: | ---: |
| Peak RSS, including fixture setup | 252,012 KiB | 11,372 KiB |
| Aggregation elapsed, excluding fixture setup | 3.244 s | 0.106 s |

The revised implementation stores only the current measurement, streams one
aggregation bucket, and strips repeated chart windows from legacy rows before
Rust decoding. Averages and retention periods remain unchanged. Historical
queries return at most 1,500 representative points distributed over the full
requested interval, rather than truncating to its earliest measurements.

Maintenance uses separate read and write SQLite connections. This avoids
SQLITE_BUSY_SNAPSHOT when the sampler commits during a historical read.
A regression test holds an older read snapshot, commits a concurrent sample,
and verifies that the aggregate can still be published. Maintenance and sampling
run as separate blocking tasks; missed ticks are skipped rather than replayed.
History queries run outside the network worker with at most two admitted queries.

## Live verification

A workspace-local probe ran the revised maintenance against the running telemetry
database with a 768 MiB virtual-address-space ceiling. This is a separate probe,
not an hour-long soak of the service's scheduled task. The normal management slice
retained MemoryHigh=768 MiB and MemoryMax=1 GiB.

- Maintenance: 2,128 ms.
- Follow-up 24-hour history query: 286 ms, 92 representative measurements.
- Probe peak RSS including history: 17,020 KiB.
- During maintenance: 24 unauthenticated HTTPS session probes, all HTTP 401 as
  expected, maximum 3.72 ms. This verifies network-worker responsiveness, not an
  authenticated storage operation's latency.

Release agent and Control Plane binaries were installed after passing checks.
Original installed binaries are backed up under
`.artifacts/control-ui/installed-backup/`. The repository/FUSE service was not
restarted. Preview UI remains available through HTTPS port 8089.

## UI checks

The Share editor is a native, centered modal dialog with a blurred backdrop,
internal scrolling, fixed actions, Escape dismissal, bounded keyboard traversal,
and focus restoration. Chromium checks cover desktop and 390 px width, including
cancel without submitting a share. The tests use project preview data.

Live ECharts updates merge options and avoid animation/reinitialization on every
sample. During a short synthetic burst of 20 live updates spaced 100 ms apart,
Chromium reported no tasks over 50 ms and no page errors. This is a focused smoke
check, not a general frame-rate guarantee for all client hardware.

Validation: 24 Rust tests, 16 UI tests, TypeScript checking, scoped Clippy,
release build, and Vite production build. The manual RSS probe is ignored during
ordinary tests. Existing Vite chart-bundle size warning remains.

Raw logs, probe sources, and popup screenshots are in `.artifacts/control-ui/`.
