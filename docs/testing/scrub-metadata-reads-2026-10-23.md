# Initial scrub Metadata reads, 2026-10-23

## Observed cause

The screenshot's 3,752 Metadata read IOPS occurred during initial integrity
verification, with Online GC still gated. The running package was
`fastdup-0.8.1-47.el10.x86_64` (runtime PID 3878537). A subsequent 120-second
sample had no frontend writes or integrity errors:

| Counter | Observation |
| --- | ---: |
| Exact Index logical reads | 239,757, all 4 KiB |
| Metadata device reads | 4,403 IOPS, 24.45 MB/s |
| Metadata read await / busy | 0.354 ms / 63.2% |
| DATA device reads | 2,076 IOPS, 48.58 MB/s |
| Newly verified Containers | 320 |

`perf` identified `scrub-resume-*` workers issuing the reads. Each dependent
record resolved its Base with a separate Exact lookup. Independent reads bypass
resident leaf pages. The syscall trace also showed an 8-KiB FDIO storage-head
read before each 4-KiB logical leaf read. This accounts for roughly twice the
device IOPS and three times the bytes of the logical lookup counters. The small
read size, rather than Metadata capacity or bandwidth, causes the high count.

The worker's thread-local reason was lost at the Rayon boundary, so these
reads appeared as `indexLookup` instead of `recoveryScrub`.

Raw samples, syscall traces and summaries are under
`.artifacts/gc-metadata-20261023/`: `runtime-analysis.json`,
`host-analysis.json`, `runtime-samples.jsonl`, `host-samples.jsonl`,
`read-perf-report.txt`, and `perf-trace-example.txt`.

## Bounded batching

One complete Container scrub now extracts structural dependency keys from the
already read image and uses the existing Exact batch lookup for at most 256
distinct Base keys. A batch deduplicates touched pages and coalesces adjacent
Direct-I/O spans. More keys retain the existing per-key resolver. No Base
payload or hint survives that Container operation; the hint-result bound is
256 keys times the existing 64-candidate limit per worker.

Only lookup hints are shared. Every selected Base still uses fresh Independent
reads, CRC checks and identity verification. Bad/negative hints retain the
verified DATA scan fallback. Full target verification still precedes certificate
creation and coverage updates. Storage-head validation, GC barriers, journal
version, candidate precedence and the two-Base-attempt limit are unchanged.
Scrub worker reads now carry the `recoveryScrub` reason explicitly.

## Regression measurements

The fixture publishes real FsStorageIo Containers and an Exact Run, reopens the
index to obtain audited page bounds, and executes `scrub_container`:

| Fixed input | Before: Exact reads | After: Exact reads |
| --- | ---: | ---: |
| 128 targets sharing one Base | 128 | 1 |
| 160 targets with distinct Bases | 165 | 1 |

These are logical storage calls on the same small fixture, not a prediction of
live IOPS or whole-scrub speed. DATA reads remain unchanged; more completed
work per second can keep device IOPS high. Live comparisons must include reads
per verified Container/byte and completion rate.

The original tests failed on both read-count assertions. Further tests cover
320 distinct Bases beyond the hint cap, fresh reads on a second scrub, Base
payload corruption after a successful scrub, damaged index pages falling back
to verified DATA, and worker read attribution. Fault injection into a leased
index deliberately bypasses normal immutable-reader write exclusion.

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test --locked -p fastdup-store --lib structural_recovery::scrub_lookup_tests
```

Full `fastdup-store` validation passed: 341 tests, zero failures, 16 intentionally
ignored. Production-library Clippy passed without lint allowances; formatting
and whitespace checks passed. Logs are retained with the measurement artifacts.
