# Persistent reduction worker-pool benchmark

This benchmark checks the CPU scheduling refactor recorded by ADR 0050. Hash
shards, Compression Regions, Similarity fingerprints, Delta trials, and Reorder
keys now use one permanent quota-sized Rayon pool. Each pool thread retains its
own Zstd encode/decode context. No reduction phase creates and joins a fresh
thread set.

The fixture is the first 268,435,456 bytes of the pinned Rocky Linux 10.2
Minimal ISO. Both revisions used the `all` policy, a 64-MiB in-flight bound,
release builds, and the same ten-effective-CPU host. Each cell is the mean of
two consecutive runs. The policy ID and every logical/physical decision counter
were identical before and after the change.

| implementation | workers | ingest seconds | ingest MB/s | speedup vs 1 |
| --- | ---: | ---: | ---: | ---: |
| phase-local scoped threads | 1 | 8.136 | 33.0 | 1.00x |
| phase-local scoped threads | 10 | 2.157 | 124.4 | 3.77x |
| permanent pool + worker-local codec | 1 | 6.565 | 40.9 | 1.00x |
| permanent pool + worker-local codec | 10 | 1.418 | 189.3 | 4.63x |

The refactor reduced one-worker ingest time by 19.3% and ten-worker ingest time
by 34.2%. Ten-worker scaling efficiency increased from 37.7% to 46.3%. This
does not make the pipeline fully CPU-linear: FastCDC for one stream, Exact
state transitions, deterministic merges, and object-level phase barriers still
contain serial work.

Reproduce with workspace-local artifacts:

```bash
for workers in 1 10 1 10; do
  /source/fastdup/.artifacts/target/release/examples/reduction_matrix \
    --preset all --workers "$workers" --inflight-mib 64 \
    /source/fastdup/.artifacts/corpus/rocky-prefix-256m.iso
done
```

Raw pre-change rows are retained in
`.artifacts/logs/worker-scaling-reduction.csv`; post-change rows are in
`.artifacts/logs/worker-scaling-after-persistent-pool.csv`.
