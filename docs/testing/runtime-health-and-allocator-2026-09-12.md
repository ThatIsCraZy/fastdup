# Mount health and allocator retention qualification, 12 September 2026

The previous health observer classified both a missing 400-ms management sample
and ordinary closed mutation admission as Repository Error. A regression through
`AgentRuntime::sample_frontend` reproduced the write-pause false alarm:
`missing_runtime_clears_online_state_and_stale_throughput` failed with
`left: Error, right: Online`. The final test supplies real management-response
JSON with `mutation_admission_open=false` through the production parser.

Availability now uses exact FUSE mount-table presence and the repository
service's state/result. Process failure, including core-dump/assert and OOM,
is detected during the service restart delay. Expected startup and intentional
unmount are separate. Unknown probes do not create an outage. Missing counters
clear stale rates/details but do not manufacture a storage failure. Confirmed
integrity failures or process crashes cannot be cleared by missing metrics;
new successful Runtime evidence is required after those failures.

The sampled test VM reported `ActiveState=active`, `MainPID=1142`, `Result=success`
and an exact `fuse fastdup` mount at `/srv/fastdup/repository`. The probes neither
suspend the process nor issue a potentially blocking FUSE getattr. No test VM
configuration, service, mount or stored data was changed.

Validation (Cargo outputs and TMPDIR under `/source/fastdup/.artifacts/`):

- Control library: 42 passed, one manual benchmark ignored. Covers missing
  metrics, normal backpressure, absent mounts, service crash/restart, unknown
  observations, integrity failures, audit de-duplication and additive allocator
  telemetry/history serialization.
- Runtime binary: 15 tests passed, including management and scrub behavior.
- Allocator module: two targeted tests passed for resident-slack admission,
  cost backoff, single worker ownership and prompt shutdown.
- UI: 45 tests passed, including persistent real-failure alerts, online state
  without detail telemetry and allocator/cache accounting separation.
- Clippy on Store, Control and the Runtime binary passed with `-D warnings`.
- TypeScript/Vite production build passed. Real Chromium checked 1440-px and
  390-px layouts, allocator details, missing metrics, process failure and recovery:
  no browser errors or horizontal overflow.

The [allocator A/B and live census](../benchmarks/allocator-reclaim-2026-09-12.md)
record the retained-RAM cause, measured benefit and performance cost. Artifacts
are in `.artifacts/runtime-health-20260912/`. The new runtime worker has not yet
been installed on the test VM or qualified during a complete Veeam job. No new
on-disk storage invariant or format is introduced.
