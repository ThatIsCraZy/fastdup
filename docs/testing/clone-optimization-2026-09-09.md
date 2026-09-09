# Clone CPU, asynchronous Samba and telemetry qualification

Date: 9 September 2026. Base: `75c98b0` (FastDup 0.7.1 source).

## Scope and invariants

Installed Manifest readers reuse repository-local verified, immutable decoded
nodes. Cache misses check object identity, CRC and structure; every traversal
still checks parent summaries. Recovery, GC, scrub and successor publication do
not use this cache. Clones retain source Chunk identities; new target metadata
still receives its own checksums. The cache participates in the existing adaptive
RAM broker at Metadata priority, below DATA-avoiding caches.

Samba dispatches at most eight independent clones concurrently with at most 64
unfinished admissions per smbd. Conflicting file identities preserve arrival
order; duplicate descriptors and source/target AIO guards protect pending work
against CLOSE and caller cleanup. Workers validate current native EOF and
Integrity metadata before one exact `copy_file_range` call. This is the managed
FastDup profile, not support for arbitrary stacked VFS metadata virtualization.

## Manifest A/B

Release-mode `manifest_cache::tests::benchmark_manifest_clone_ranges` traverses
the same multi-leaf tree for 2,000 range queries with admission disabled/enabled.
The test cache allowance is only a benchmark control, not a product RAM setting.

| Variant | Elapsed | Metadata reads |
| --- | ---: | ---: |
| Uncached | 294.670381 ms | 4,000 |
| Verified decoded cache | 5.739754 ms | 4 |

This isolates the metadata traversal: about 51× less elapsed time in that
microbenchmark. It is not an end-to-end clone or Veeam throughput prediction.
The test also verifies range contents, parent mismatch rejection on cache hits,
bad cold bytes, cache-instance isolation, retained views after eviction, and
concurrent admission without double charging.

## Real SMB qualification

A separate Samba 4.23.5 daemon bound only to `127.0.0.1:1445` on the test VM used
an isolated configuration, private state and a mount namespace containing the
candidate module. The production package, module and repository process were
not replaced or restarted. Both variants used the same existing FUSE runtime;
the new Manifest cache was measured separately, not installed for this SMB A/B.

`tests/clone_async_smb.py` issues eight outstanding requests on one connection.
A test-only `copy_file_range` delay of 250 ms demonstrated eight simultaneous
syscalls. It passed complete destination/untouched-neighbor comparisons,
overlapping clones through aliases, interleaved Integrity SET, target CLOSE,
source CLOSE, CANCEL and an abruptly disconnected client. Target CLOSE took
255.857 ms under injection and reopening exposed the complete successor.
Disconnected/unacknowledged work was checked against the complete old-or-new
content oracle. No panic/assert occurred in the isolated daemon logs.

`tests/clone_ranges_smb.py` also passed seven consecutive clones, including the
reported 7,168/5,120-byte cases, a one-byte continuation, 8,193-byte and 65,536-byte
unaligned continuations, and a range ending exactly at EOF. All 2 MiB of the
result matched before and after flush/CLOSE/reopen. Reopen requests read access
because this step verifies bytes; it does not require `GENERIC_ALL` ownership
rights. Test destinations had an explicit writable fixture creation mode.

Without injected delay, six interleaved runs each cloned 512 MiB logically:

| Run | Adapter | Logical MiB/s |
| --- | --- | ---: |
| 1 | Production synchronous | 265.42 |
| 2 | Asynchronous candidate | 366.68 |
| 3 | Asynchronous candidate | 294.64 |
| 4 | Production synchronous | 290.89 |
| 5 | Production synchronous | 35.40 |
| 6 | Asynchronous candidate | 368.60 |

Medians were 265.42 and 366.68 MiB/s. There was substantial background-work
variance, including the 35.40 MiB/s baseline outlier. Treat this as evidence from
this loopback workload, not a guaranteed Veeam speedup. Normal traces had only
one overlapping clone syscall: Samba core still performs synchronous source
and target stats before calling the adapter. Worker concurrency is proven by
the delayed test, not an assertion that ordinary clones always run eight-wide.
There were no failed/short syscalls or unfinished jobs in the A/B traces.

The isolated daemon was stopped and its eight leftover test files and extra
credential database copies removed. The production repository retained PID
241863 and package `fastdup-0.7.1-3.el10.x86_64` throughout qualification.

## Reduction and UI

The old displayed factor divided since-mount processed logical bytes by newly
published Container bytes. In the reported sample this was
`230199557902 / 15388672 = 14959.03`, which was not current physical reduction.
The occupancy-based sample is instead
`678870599109 / (76671565824 + 5906501632) = 8.22096`.

The API now returns an optional ratio from current logical allocation and both
occupied tiers. Missing/zero-denominator samples stay unavailable. History
recomputes legacy ratios from each record's own occupancy; it never substitutes
live data. Exact Dedup retains its explicitly labeled since-mount basis.

Telemetry starts with cache effectiveness, including Manifest Nodes. DATA pools
appear first; descriptions explain each cache. Five-minute/lifetime controls
change counters, not RAM gauges. Raw counts and leased reservations are optional
columns. Separate read-avoidance, GC/scrub, latency, io_uring and checkpoint views
keep related metrics together. Similarity counters describe attempts, not
physical I/O counts. Total reduction has no arbitrary percentage ring.

The selected historical sample supplies summary cards, storage use, disks and
details. Loading/empty historical intervals do not show live values. Tests cover
these boundaries, optional counters and older samples without the new fields.

## Validation commands and results

All local build, log, benchmark and browser artifacts are under
`/source/fastdup/.artifacts/clone-optimization/`; Cargo targets and temporary
files use `/source/fastdup/.artifacts/target` and `.artifacts/tmp`.

- `cargo test --release -p fastdup-store -p fastdup-appliance -p fastdup-control --lib`:
  Store 91, Appliance 53, Control 38 tests pass (Control rerun includes the new
  legacy-history regression). Explicit manual benchmarks remain ignored.
- `cargo test --release -p fastdup-appliance --test durable_namespace --test durable_namespace_faults clone`:
  seven pass, including zero DATA I/O and byte-exact crash/fault recovery.
- `cargo test --release -p fastdup-store --test manifest_reader --test generation_repository`:
  16 pass; three manual benchmarks ignored.
- `cargo clippy --release -p fastdup-store -p fastdup-control -p fastdup-appliance --lib`:
  passes (Store and Control/Appliance checked separately).
- Native C contract suite with `-std=c11 -Wall -Wextra -Werror -pedantic` passes,
  including reverse completion windows of one through 64 operations.
- Module compiles against Samba 4.23.5. Qualified candidate SHA-256:
  `2065ea23ecf1e5495595e0964746a0b8a3206f1a81e642c9c8ccf00574675084`.
- Release builds of the Control/Agent and Appliance binaries pass, including
  the final embedded UI assets.
- UI: 42 tests pass (`--maxWorkers=2` to avoid competing with release linking); TypeScript and production Vite build pass. The pre-existing
  large chart-chunk warning remains. Browser checks exercise all six tabs,
  cache counter expansion, five-minute counters and 1440/390 px viewports,
  without page overflow or browser errors. Screenshots use labeled test fixtures.

No release tag or public package is created by this qualification record.
