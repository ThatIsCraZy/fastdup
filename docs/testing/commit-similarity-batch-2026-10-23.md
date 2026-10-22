# Commit Similarity batch planning, 2026-10-23

## Production observation

The test VM ran `fastdup-0.8.1-46.el10.x86_64`, source `65b3259`.
During five minutes of the Veeam container incremental backup, frontend writes
averaged 93.6 MiB/s. Mutation admission was closed for 73.68% of the window.
Commits accumulated 278.9 s, including 217.1 s in ManifestPlan; one sampled
commit took 25.9 s. The host had approximately 70% idle CPU and 17% I/O wait.
Metadata reads averaged 1.83 ms, DATA reads 3.35 ms. There were no frontend
I/O or integrity errors and no swap activity.

The completed Veeam session `ec74cd8e-ecb2-4408-8db0-e76d2f6a7a0f` reported
Success for all eight tasks, 14:07 duration, and 124.2 MB/s processing rate.
That run preceded this optimization. Its job-level rate and the frontend rate
above use different counters and windows.

Evidence: `.artifacts/performance-check-20261023/analysis.json`,
`runtime-samples.jsonl`, `perf-report.txt`, `host-samples-second.jsonl`, and
`.artifacts/commit-batch-20261023/veeam-progress.txt`.

## Change and preserved boundaries

`AdaptiveCommitWriter::flush` now uses the existing ordered, bounded
`plan_similarity_batch` path instead of planning each Chunk serially. It shares
the write-through `WorkerPermits`, including for subsequent Container encoding.
The lease is released before Container publication I/O. Partial admission is
honored when other stages hold permits.

The writer's existing target-byte bound remains in force. Borrowed target views
do not copy payloads; the planner retains at most eight verified Base owners
per codec wave. One coherent Exact/Similarity snapshot covers the batch.
The DATA reference guard still covers planning, publication and Exact activation.
Chunk order, candidate policy, codec selection, durable formats and recovery
semantics are unchanged. This applies the existing ADR 0050 execution policy
to commit residue, retaining the ADR 0093 handoff.

## Regression and controlled A/B

The fixture runs the real commit writer with 128 independently generated 64 KiB
Bases and one-byte target mutations. Similarity index reads have 2 ms injected
latency; DATA and publication use MemoryStorageIo. Timing covers `writer.finish`,
excluding corpus/index setup and independent readback. This is an isolated
commit benchmark, not a prediction of end-to-end backup throughput.

The pre-change regression test failed with all cold index reads serialized.
After the change it observes bounded overlap. A second test compares serial
and partially admitted parallel runs with independent, sparse-XOR and
different-length Prefix records. It verifies each target through the published
Exact Index, compares record decisions and Container size, and checks permit
return and the two-worker limit when two of four permits are held elsewhere.

Eight alternating baseline/optimized repetitions in release mode produced:

| Implementation | Workers | Median finish | Range | Median cold reads | Peak concurrent reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 1 | 49 ms | 46–51 ms | 10 | 1 |
| Baseline | 4 | 48.5 ms | 46–50 ms | 10 | 1 |
| Batch | 1 | 48.5 ms | 45–50 ms | 10 | 1 |
| Batch | 4 | 31 ms | 29–32 ms | 23 | 4 |

Four-worker median duration fell by 36.1% (1.56x). Every run accepted all 128
dependent targets and produced 61,440 Container bytes. Concurrent cold misses
caused 23–25 index reads instead of ten: the existing page cache does not merge
in-flight reads of the same page. These are StorageIo calls, not physical disk
IOPS. This extra traffic remains a follow-up measurement target on the VM.
The fixture does not model DATA latency, disk queueing, a large cold index,
concurrent backup streams or RSS at the maximum writer bound.

Reproduce the optimized fixture:

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test --locked --release -p fastdup-appliance --lib \
  checkpoint_similarity_benchmark -- --ignored --nocapture --test-threads=1
```

The baseline binary was preserved before changing the production path. The
baseline and optimized binaries, eight raw logs per variant, `ab-results.json`
and `ab-summary.json` are under `.artifacts/commit-batch-20261023/`. The baseline
fixture predates the final mixed-record case and IndexLookup-only attribution;
the homogeneous timed input and read count are unchanged.

## Validation

- `cargo test --locked -p fastdup-appliance`: 309 passed, zero failed, four
  intentionally ignored, including the benchmark.
- Both focused commit tests passed again after restricting the concurrency
  measurement to lookup reads, excluding asynchronous index publication.
- `cargo fmt --all` and `git diff --check` passed.
- Unfiltered Clippy is blocked by existing findings in POSIX, appliance
  publication, telemetry and integration tests. Production-library Clippy passed
  with `--no-deps --lib -- -A clippy::too_many_lines`; the existing
  `publish_generation` exceeds that limit. Broader test linting found no
  diagnostics in the new fixture but still fails on unrelated existing code.

The remaining acceptance measurement is a comparable real backup after the
updated package is running: frontend throughput, admission-closed fraction,
commit/ManifestPlan time, Metadata read count/latency, DATA read latency, RSS,
and reduction rate. A package startup check does not replace backup or restore
qualification.
