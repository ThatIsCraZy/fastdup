# Shared adaptive cache budget — 2026-09-07

The live 0.6.4-11 workload had spare RAM while verified DATA payloads were
limited to about 2.87 GB and Similarity pages to 512 slots. In a 30-second
sample, Similarity recorded 7,482,749 misses and 6,363,513 evictions. DATA reads
also included reduction-base verification; frontend reads were zero in that
sample. A larger cache cannot eliminate cold reads or make reduction free.

## Implemented policy

ADR 0046 now assigns one shared, sampled byte budget to verified DATA payloads,
historical DATA proofs, Container descriptors, Exact pages, and Similarity
pages. It leaves 8% of effective host/cgroup RAM available, accounting other
working memory through live available RAM. Recent avoided bytes per resident
byte determine benefit, weighted 16:1 for DATA versus Metadata fallback.
Miss/replacement demand, decay and bounded exploration support workload changes.
These weights are priorities, not fixed byte allocations. Fully served pools
return unused allocations. Immutable generation proofs and pinned active Run
views remain working memory outside the reclaimable cache leases.

A donor retains its lease until eviction completes. Local admission gates
serialize target changes; no new broker lock is added to the normal hit path.
Partial shrink retains useful entries. Historical shards compact survivors
before returning their arena memory. Exact and Similarity allocate lazy page
maps rather than eagerly reserving their maximum geometry. The 92% ceiling is
an operating target sampled at 250 ms, not a kernel-enforced RSS limit against
concurrent allocations or allocator retention. Failed samples and process Swap
close payload admission. No durable formats or verification rules changed.

Runtime JSON and the WebUI Caches tab show the effective RAM limit, available
RAM, common budget, and each pool's DATA/Metadata fallback, hits/misses,
evictions, resident bytes, target and still-reserved bytes. Historical samples
retain these fields; older samples without them remain readable. Residency
includes charged cache bookkeeping. Existing hit rates are cumulative since
mount; the controller itself uses decayed recent benefit.

## Local A/B evidence

Baseline: cdc8aa5 (0.6.4-11), with the identical ignored benchmark fixtures.
Both optimized test binaries were rebuilt explicitly in their respective
worktrees, then copied out of the shared Cargo target directory. Three pairs
alternated before/after order on CPU 0. Each run had seven rounds; the first
round warmed the cache and was excluded. Values below are medians of 18 rounds.

| Fixture | Before | After |
| --- | ---: | ---: |
| 8,192 real Similarity entry queries, 1,024-page working set | 8.324 ms | 0.750 ms |
| Repeated Similarity hits / misses per warm round | 0 / 8,192 | 8,192 / 0 |
| 500,000 verified DATA-cache hits | 14.338 ms | 14.170 ms |

The Similarity replay validates each returned entry. Its index files and OS
page cache are local and warm: the roughly 11x lookup improvement demonstrates
avoided cache-fallback work, not physical-HDD or Veeam throughput. The DATA hit
fixture shows no median regression; its range includes scheduling noise. It
uses an explicit deterministic cache and does not measure pressure-refresh
latency. Live competing-pool and end-to-end results require installing the new
runtime after diagnosing the independently stalled checkpoint on the VM.

Reproduce with the ignored store library tests
`shared_cache_similarity_replay_benchmark` and
`adaptive_budget_hit_path_benchmark`, `--release --ignored --nocapture`.
Raw logs and JSON summary are under
`.artifacts/adaptive-cache-budget/{replay,hit}-{before,after}-{1,2,3}.log`
and `benchmark-summary.json` (generated evidence, not source-controlled).

## Validation

- Controller: DATA priority, 120-round workload turnover, cold exploration,
  process Swap, immediate external pressure, delayed donor eviction, pool drop,
  and eight concurrent borrowers without duplicate leases.
- Real caches: growth beyond old page geometry, FIFO turnover, retained reader
  ownership after eviction, historical hot-proof survival across shrink,
  release of vacant arenas, and existing concurrent DATA-cache pressure tests.
- Store/appliance library suites, selected Manifest/index/recovery integration
  suites, and every existing Namespace checkpoint fault boundary.
- Control telemetry parse/serialization and legacy-sample compatibility;
  WebUI budget, tier, pending donor reservation, empty-hit and history rendering;
  TypeScript checking and production Rust Clippy with warnings denied.

The later VM checkpoint failure is separate: the installed process still runs
0.6.4-11. It reports Metadata(InvalidObjectLength(20620288)), keeps write
admission closed, and repeats work. Cache changes were not deployed during
these observations and cannot have caused that error.
