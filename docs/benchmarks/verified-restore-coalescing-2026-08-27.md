# Cold verified-restore coalescing, 2026-08-27

## Question

Does the ADR-0077 Verified Read Plan reduce DATA requests without reducing
single-stream restore throughput? This is a narrow read-path A/B. It does not
measure SMB, Prefix selection, GC, or write ingest.

## Method

`fastdup-verified-restore-bench` creates one fresh v2 Container, publishes and
activates an Exact Run, then alternates complete byte-verified restores through
the production `IoUringStorageIo` adapter:

- the production Manifest read path with adjacent-Record coalescing capped at
  the global 1-MiB request bound; and
- a scalar reference that resolves and verifies each Chunk independently.

Before every cold sample it applies `POSIX_FADV_DONTNEED` to the Container. Both
paths hash the complete logical output. One fixture report contains the median
of seven alternating rounds; the result below uses five independently created
fixtures per geometry. Warm runs omit eviction. Artifacts were written only
below `.artifacts`.

The DATA filesystem was XFS on `/dev/sdb`, a 200-GB Hyper-V virtual SCSI disk
with model `Virtual Disk`. The guest reports `ROTA=1`, scheduler `none`, and
4-MiB block-device readahead. No seek latency, rotational transfer curve, queue
limit, or other HDD behavior was emulated by this benchmark. `ROTA=1` is only a
guest topology hint; the host backing is not visible from the VM. This is a
cold-path software regression check, not evidence from a physical HDD or array.

The v2 report separates three layers:

- `*_data_reads` counts fastdup `StorageIo::read_exact_at` calls;
- `*_io_uring_submissions` samples the production ring's submitted-operation
  counter around the restore;
- `*_block_*` takes checked deltas from `/sys/class/block/sdb/stat` around each
  path and reports completed read I/Os, merged reads, sectors, and ticks.

These are guest block-layer counters, not host NVMe commands or physical seeks.
Kernel readahead and the virtual block layer may merge work below fastdup's
boundary. `POSIX_FADV_DONTNEED` is an eviction request, not proof of a physical
media read.

Example reproduction with a fresh directory:

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp

cargo run --release -p fastdup-exact-bench \
  --bin fastdup-verified-restore-bench -- \
  --root /source/fastdup/.artifacts/mnt/smb-single/containers/restore-fresh \
  --chunks 128 --chunk-bytes 65536 --rounds 7 --block-device sdb
```

## Results

### Normal 4-MiB device readahead

| Fixture | Path | DATA calls | io_uring submissions | Block reads | Sectors | Median planned/scalar throughput |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 8 MiB, 128 × 64 KiB | planned | 16 | 16 | 10 | 16,480 | 1.257× |
| 8 MiB, 128 × 64 KiB | scalar | 128 | 128 | 10 | 16,480 | baseline |
| 32 MiB, 128 × 256 KiB | planned | 64 | 64 | 33 | 65,632 | 0.880× |
| 32 MiB, 128 × 256 KiB | scalar | 128 | 128 | 34 | 65,632 | baseline |

For 64-KiB Chunks the plan removes seven eighths of production ring submissions
and is 25.7% faster, but the kernel serves both sequential streams with the same
ten block reads. For 256-KiB Chunks it halves submissions, saves only one of 34
block reads, and is 12.0% slower; planning and slice management cost more than
the saved submissions on this backing.

### Warm page cache

Both paths cause zero block reads. The 64-KiB plan is 1.311× scalar throughput;
the 256-KiB plan is 0.897×. This reproduces the small-Chunk submission benefit
and the large-Chunk planner cost without storage latency.

### Readahead perturbation and issue trace

The device `read_ahead_kb` was temporarily changed from 4096 to zero under a
shell trap and restored to 4096 after every run. Across five 8-MiB fixtures,
both paths then completed exactly 2,054 block reads and 16,432 sectors per
sample. Planned median throughput was 1.084× scalar, with materially greater
run-to-run variation. A larger fastdup read therefore does not itself become
one buffered block request; without readahead both paths fault the same pages.

One normal seven-round run under `perf stat` emitted 151
`block:block_rq_issue` events: 140 from seven planned/scalar pairs at ten reads
each plus eleven fixture-construction reads. This exactly corroborates the
per-sample sysfs deltas and rejects the hypothesis that the immediate completion
counters hid trailing readahead. The virtual driver did not emit the
`block_rq_complete` tracepoint, so completion evidence remains the sysfs field.

### Corrected adapter

The first report used `FsStorageIo`, not the daemon's required DATA adapter. Its
16.9%/19.6% planned losses are retained as historical artifacts but are not
representative production-throughput evidence. Switching the benchmark to
`IoUringStorageIo` reverses the 64-KiB result and exposes the ring-submission
boundary directly.

Representative raw reports and trace:

- `.artifacts/benchmarks/verified-restore-v2-small-20260827-run1.txt` through
  `run5.txt`
- `.artifacts/benchmarks/verified-restore-v2-large-20260827-run1.txt` through
  `run5.txt`
- `.artifacts/benchmarks/verified-restore-v2-warm-small-20260827-run1.txt`
  through `run5.txt`
- `.artifacts/benchmarks/verified-restore-v2-warm-large-20260827-run1.txt`
  through `run5.txt`
- `.artifacts/benchmarks/verified-restore-iouring-blockstats-ra0-20260827-run1.txt`
  through `run5.txt`
- `.artifacts/benchmarks/verified-restore-v2-small-perf-block-20260827-run6.txt`

## Decision

Keep bounded coalescing. The 8.0× reduction is also an 8.0× reduction in
production io_uring submissions and wins on both cold and warm low-latency
backing. It is not an 8.0× device-I/O reduction for a sequential buffered stream:
normal kernel readahead already produces the same ten block reads.

The 2.0× case loses on this host and saves only one block request, but it remains
enabled pending the fragmented physical-HDD gate because physical sorting may
avoid seeks that this one-Container sequential fixture cannot represent. Any
future activation threshold must include plan-construction cost, predicted
submission reduction, Container transitions, and seek geometry without adding
work to the write hot loop.

The implementation continues to preserve independent Record and Chunk
verification, logical output order, the 1-MiB I/O bound, and the allocation-free
scalar single-extent path. Do not add speculative readahead, parallel queues, or
durable placement fields from this result.

The next evidence gate is the same alternating cold benchmark on physical HDD
or the intended redundant HDD array, including fragmented and cross-Container
logical sequences. Any userspace prefetch proposal also needs explicit memory,
queue-depth, cancellation, and demand-I/O priority bounds.
