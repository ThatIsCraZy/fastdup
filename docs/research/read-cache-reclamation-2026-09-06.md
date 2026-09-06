# Verified DATA-cache admission after a workload change — 2026-09-06

## Observation and cause

On the test VM running RPM 0.6.4-7, three five-second samples showed
25.08–28.48 MB/s DATA-device reads alongside 26–45 MB/s writes. During a
109-second journal interval, the verified DATA cache recorded 12 hits,
28,145 misses, zero admissions, zero evictions and 56,602 pressure rejections.
Its resident allocation was 2,866,774,290 bytes against a 2,866,775,424-byte
target. Approximately 12.4 GB of available RAM did not override that hard cap.

The admission path considered only the incoming key's set. An empty way
contributed no victim bytes, so a full global byte budget rejected the new
allocation without replacing old entries elsewhere. Shared decoded allocations
could also remain charged through other views. The cache consequently stopped
following the working set. Increasing RAM or skipping integrity verification
would not fix this replacement defect.

Advanced Reduction processed 5,135,873,443 logical base bytes in the same
interval and reported 202,131,911 bytes saved. Those logical counters include
cache/batch hits; they do not identify physical DATA reads. Writer reread/VERIFY,
reduction-base reads and other necessary storage work remain. This change does
not promise to eliminate all reads during a backup.

## Bounded replacement

The existing serialized admission state now holds a persistent round-robin
cursor. A decoded allocation group probes at most 256 slots across sets to
reclaim enough room. Each step holds only one shard lock. Later attempts
continue the cursor, including across large sparse geometries and allocations
shared by many entries. Exhaustion skips admission, without failing the read.

A shared allocation remains charged until its last cache view is removed.
Outstanding verified reader views retain their immutable bytes independently.
Already admitted views from the incoming group are protected. Hits retain
the same four-way lookup, with no new global lock or reference-bit update.
Live memory targets, Swap purges, verification, recovery and scrub policies
are unchanged. No durable format changes or unsafe code are introduced.
See [ADR 0046](../adr/0046-bound-verified-read-cache-by-live-memory-headroom.md).

## Red/green evidence

Both regressions failed against the implementation at `f9e9793`, then passed
with reclamation:

- `full_byte_budget_admits_a_new_workload_into_an_empty_set`: admission must
  replace an older backing in another set instead of rejecting forever.
- `full_shared_backing_cache_stops_rereading_after_workload_changes`: a real
  Container/Exact Index/Manifest reader fills the byte target with two views
  sharing one allocation, then reads a different 128-KiB range 32 times. Old
  code performs 32 DATA range reads (`left: 32`, `right: 1`); fixed code performs
  one. Every response is compared byte for byte.

Additional tests cover partial shared-backing eviction, surviving reader
views, bounded cursor progress over more than 256 slots, and concurrent
admissions, hits and pressure changes with exact allocation accounting.

Use `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`; the explicit Cargo config override also
keeps Unix socket fixtures inside their path-length bound in a worktree:

```sh
cargo --config 'env.TMPDIR.value="/source/fastdup/.artifacts/tmp"' test \
  -p fastdup-store --test manifest_reader \
  full_shared_backing_cache_stops_rereading_after_workload_changes
```

## Optimized A/B

Both implementations were compiled in release mode on the development VM
(10 vCPUs, reported Intel Core i7-1370P), with identical benchmark fixtures.
The old implementation was built with only the benchmark harness added; its
executables were retained before rebuilding the fix. Three alternating A/B
pairs ran pinned to CPU 0 without concurrent builds/tests. Each invocation had
seven rounds; round zero was discarded, leaving 18 samples per mode.

| Workload | Before | After |
| --- | ---: | ---: |
| 2,048 reads after working-set transition, median elapsed | 108.773 ms | 9.605 ms |
| DATA range reads per transition round | 2,048 | 1 |
| DATA range bytes per transition round | 269,221,888 | 131,456 |
| Warm cache hit, median per request | 27.42 ns | 28.57 ns |

The transition fixture reads 256 MiB logically per round and verifies the
returned bytes. It measures calls at the real `StorageIo::read_range` boundary,
not physical disk reads: the Linux page cache is involved. Its 11.3x elapsed
improvement is not an end-to-end Veeam prediction. The warm-hit measurement
uses two million lookups per round and shows about 1.15 ns higher median in
this run; no warm-hit speedup is claimed.

The ignored, reproducible benchmarks are `cache_hit_benchmark` in
`read_cache/reclamation_tests.rs` and `cache_working_set_transition_benchmark`
in `tests/manifest_reader.rs`. Run their release test executables using
`taskset -c 0 <executable> <benchmark-name> --ignored --nocapture`.
Raw logs, saved executables, red/green output and `benchmark-summary.json`
are retained under `.artifacts/read-cache-reclamation/` (not source controlled).

## Validation and rollout

- 110 store tests passed across the library, Manifest reader, record-read
  singleflight, recovery and reduction suites; 11 manual tests ignored.
- 98 appliance tests passed across the library, durable namespace faults,
  recover/mount and write-through ingest; three manual tests ignored.
- Production store-library Clippy passed with warnings denied. The test-target
  run passed with three lint allowances for pre-existing test findings:
  `unreadable_literal`, `case_sensitive_file_extension_comparisons`, and
  `manual_assert`. The unrestricted test-target run is not claimed clean.

RPM 0.6.4-9 includes this fix and the telemetry-under-checkpoint fix from
revision -8. At the last pre-package check the VM still had six writable Veeam
backup handles and runtime PID 33861 with zero restarts. The package is staged
for activation after the backup finishes; this report does not claim live
installation or a measured reduction in physical Veeam I/O with the new code.
