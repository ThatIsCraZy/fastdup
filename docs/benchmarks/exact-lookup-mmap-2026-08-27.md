# Exact activated lookup: bounded reads versus mmap

Date: 2026-08-27

The benchmark exercises the public activated Exact Index repository path. Both
readers recover the same immutable Run and execute the same deterministic hit
sequence. `FsStorageIo` selects the leased mapping; a capability adapter that
does not offer immutable-file leases selects bounded `read_exact_at`. Both use
the production membership hint and memory-governed decoded hot-page cache.

Command for the production-size result:

```bash
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo run --release -p fastdup-exact-bench \
  --bin fastdup-exact-lookup-bench -- \
  --entries 262144 --queries 200000 --rounds 7
```

Host: Intel Core i7-1370P VM, 10 online CPUs, Linux
`6.12.0-211.49.1.el10_2.x86_64`. Runs use a warm kernel page cache and alternate
backend order. Faults, RSS, and Swap come from `/proc/self/stat` and
`/proc/self/status`; fault counts are summed across seven measured rounds and
RSS/Swap are process peaks observed at round boundaries.

| Entries | Backend | Median | ns/query | Relative | Minor faults | Major faults | Peak RSS | Peak Swap |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | `read_exact_at` + cache | 144.314 ms | 1,443.1 | 1.00x | 3,378 | 0 | 49.4 MiB | 0 |
| 100,000 | mmap + bounds + cache | 77.644 ms | 776.4 | 1.859x | 3,379 | 0 | 49.4 MiB | 0 |
| 262,144 | `read_exact_at` + cache | 361.355 ms | 1,806.8 | 1.00x | 8,854 | 0 | 117.1 MiB | 0 |
| 262,144 | mmap + bounds + cache | 185.243 ms | 926.2 | 1.951x | 8,856 | 0 | 117.1 MiB | 0 |

The first prototype mapped the Run but decoded every page touched by binary
search. At 100,000 entries it measured 16,055 ns/query versus 1,531 ns/query
for the positional decoded cache and was rejected. Retaining audited first/last
keys per page removes repeated binary-search decodes and cache locks; only the
candidate page reaches the decoded cache.

## SMB guardrail

Two post-change SingleStream runs passed with all Exact readers mapped, no
positional active Runs, and zero daemon Swap:

| Run | Aggregate | Per-copy MiB/s | Completed-write p99 | Peak RSS | Reduction |
|---|---:|---|---:|---:|---:|
| first | 1,420.1 MiB/s | 987.8 / 1,799.6 / 1,836.7 | 2.001 s | 521.1 MiB | 3.105x |
| repeat | 1,447.7 MiB/s | 1,037.9 / 1,751.8 / 1,858.9 | 1.904 s | 497.0 MiB | 3.104x |

Reports:

- `.artifacts/benchmarks/smb-exact-mmap-20260827.json`
- `.artifacts/benchmarks/smb-exact-mmap-repeat-20260827.json`

These end-to-end runs establish no regression and validate production page
source telemetry. They do not isolate the full throughput change from storage
conditions: compared with the earlier 672.6/717.5 MiB/s runs, DATA write-time
also fell from roughly 22.7-24.7 seconds to 11.1 seconds. The controlled Exact
A/B above is therefore the causal performance evidence for this decision.
