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

Final validation on `6d2abe4`: `cargo test --locked -p fastdup-store` passed
344 tests (16 ignored); `cargo test --locked -p fastdup-appliance --
--test-threads=1` passed 309 tests (4 ignored). Production Store Clippy,
formatting and whitespace checks passed. The sparse-leaf performance gate is
part of the ordinary Store test run.

Package `fastdup-0.8.1-48.el10.x86_64` was built in the isolated
`.artifacts/worktrees/scrub-48` checkout from `6d2abe4c7931a42725de688468f038230df43a15`.
Its SHA256 is `4143c1c1adec582bdb61b4a2eeedf9228bf3a4911c35b986a3a4905638aa6336`.
The unrelated root-workspace stylesheet change was excluded. Full provenance
and package comparison are saved in the measurement directory.

## Final storage review

Muse Wonder Full 1.0 (`xhigh`) reviewed the operation-local reader in `6d2abe4`
and found no critical correctness, lifetime or freshness issues. The reader
borrows the immutable Run lease, revalidates both storage heads and descriptor
length, and cannot outlive the current bounded lookup window. Leaf CRC/ordinal
checks, fresh Base verification, error precedence and the GC gate remain intact.
The requested Store tests and production Clippy had already passed as recorded
above; the reviewer did not rerun them.

One non-blocking performance limitation remains: pages already present in the
ordinary page cache can take a fresh scalar Independent read and lose the
batched header saving. They do not reuse cached verification. The sparse-leaf
regression measures the cold path; live results depend on the cache state and
Run layout.

## Test-VM deployment and live observation

The authorized update from `fastdup-0.8.1-47.el10.x86_64` to
`fastdup-0.8.1-48.el10.x86_64` completed at 03:33:51 CEST on 23 October,
after the preceding FastDup backup had finished successfully. The update job is
`.artifacts/test-vm-update/20261023T013154Z-ec06068041/job.json`.

- Repository, agent, control plane, Samba and the Veeam service container are
  active. The agent reports `repositoryState=online`, `runtimeIssue=null`.
- Runtime PID 3945699 runs the packaged executable. Its SHA256,
  `1f628752ce4ab934c02d9fbcece7759e54608186cd2ed7e9d55368c9d730c41d`,
  matches both the RPM file digest and the installed executable. RPM packaging
  strips the build binary; provenance distinguishes the unstripped build hash
  from this installed-runtime hash.
- The Veeam container was restarted against the new FUSE mount. Its repository
  bind has the same device/inode as the host, and two existing VBM metadata
  prefixes read identically through host and container paths. Veeam transport,
  deployment, environment and immutability services are active.

The post-mount device window excludes startup audit and covers 95.3 seconds:

| Device | Read IOPS | Read MB/s | Mean read latency | Mean queue |
| --- | ---: | ---: | ---: | ---: |
| Metadata (`sdb`) | 694 | 3.21 | 0.962 ms | 0.668 |
| DATA (`sdc`) | 417 | 5.89 | 0.826 ms | 0.345 |

There was no swap activity; runtime RSS at the end of this window was about
998 MiB. Metadata device busy time was 60.0%, so the lower byte count does not
mean that storage latency has disappeared.

A separate 130.0-second runtime window ended before frontend writes began:
343 Containers were freshly verified and 1,449 resumed from prior evidence.
All read/write/integrity error counters stayed clear. Of the 65,219 Exact
lookup reads, 65,216 were correctly attributed to `recoveryScrub` and three
to ordinary `indexLookup`.

The earlier observation had 4,403 Metadata read IOPS at 24.45 MB/s, but it
freshly verified all 320 Containers in its window. The new mount has 9,477
Containers instead of 7,774, a changed Run layout and mixed resume/fresh work.
Consequently these live figures establish a healthy deployed runtime and
correct attribution, **not a controlled percentage improvement**. In
particular, `verifiedBytes` includes resumed evidence and must not be treated
as newly scrubbed payload throughput. The controlled physical-read reduction
remains the 77.8% sparse-fixture result above.

A new backup session, `8712e800-de05-47c0-8c6a-124c3cf7e370`, was observed
starting at 03:35:34 CEST. Later samples with frontend writes are excluded from
the quiet runtime window; the session was still running at this observation.
This deployment check does not claim a completed backup or restore on RPM 48.

Evidence under `.artifacts/gc-metadata-20261023/`: `deployment-48.json`,
`container-read-after-48.json`, `host-after-48-analysis.json`,
`runtime-after-48-quiescent-analysis.json`, their input samples, and
`veeam-postdeploy-48.json`.
