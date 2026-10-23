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
creation and coverage updates. GC barriers, journal version, candidate
precedence and the two-Base-attempt limit are unchanged.
Scrub worker reads now carry the `recoveryScrub` reason explicitly.

An independent Exact lookup window now borrows an operation-local file reader.
It freshly validates FDIO heads and descriptor length once per leased Run, then
reads each requested span directly without cached payloads. The snapshot cannot
outlive its file lease and is discarded before the next window/Container.
Ordinary per-range independent reads still obtain fresh heads per call; no
process-wide or thread-local layout cache is introduced. Writer-born or absent
page bounds retain their existing serial fallback.

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
to verified DATA, destroyed FDIO heads followed by a missing Base, preservation
of full-verifier error precedence, and worker read attribution. Fault injection
into a leased index deliberately bypasses normal immutable-reader write exclusion.

Muse Wonder Full 1.0 (`xhigh`) correctly challenged the tiny fixture's page
adjacency. A second fixture requests 128 Bases across three Runs, including two
with 65,536 unrelated hash keys each. Test-only physical byte counters minus
logical returned bytes count actual 8-KiB head reads:

| Sparse-leaf variant | Fresh head reads | Physical bytes read |
| --- | ---: | ---: |
| Per-key lookup | 393 | 4,829,184 |
| Page batching alone | 237 | 2,990,080 |
| Batch with operation-local head validation | 3 | 1,073,152 |

The last variant reads 77.8% fewer physical bytes for index resolution in this
fixture. The regular regression requires at least 50% fewer head reads and
also verifies the complete target Container. It failed with page batching alone
before adding the operation-local reader. This does not measure total scrub
DATA traffic or claim a live throughput multiplier.

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test --locked -p fastdup-store --lib structural_recovery::scrub_lookup_tests
```

Logs are retained with the measurement artifacts. An appliance concurrency
fixture's 2-ms scheduling race was replaced with a bounded rendezvous. A separate
global cache-admission test failed amid parallel tests and passed alone; its
entire 38-test ingest suite also passed with one test thread. No production cache
policy was changed to accommodate that test.
