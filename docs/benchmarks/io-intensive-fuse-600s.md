# Intensive FUSE ISO churn, 600 seconds

Date: 2026-08-16. Result: **PASS for byte-exact POSIX lifecycle; FAIL for the
ten-second durability SLO and bounded-memory expectations.**

This is the first sustained end-to-end run through the kernel FUSE mount,
live/dirty epochs, periodic durable checkpoint, `FastCDC`, persistent Exact
Index, parallel adaptive RAW/Zstd Container writer, immutable Manifests, and
Commit WAL. It is intentionally not a production capacity claim.

## Corpus and method

The source was the pinned 2,072,444,928-byte Rocky Linux 10.2 minimal ISO with
SHA-256
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
The source copy was on the system XFS, not the measured data tier. The harness
streamed 50 variants with eight deterministic nonzero one-byte XOR edits each,
using 400 globally distinct offsets. Four threads wrote the variants through
FUSE. A 12-second settle interval forced the completed batch behind at least
one periodic checkpoint before four reader threads read every byte and compared
a fresh BLAKE3 digest. All names were then unlinked.

Data-tier `StorageIo` telemetry was enabled for the measured daemon. In the
current binary the equivalent reproduction setting is
`FASTDUP_IO_TELEMETRY=1`; ordinary mounts leave range-classification locks and
counters disabled.

The measured workload ran for 600.118 seconds. One complete 50-file cycle
finished by second 541. The remaining window completed four more files and
began four partial files; all eight second-cycle names were removed at the
deadline. In total the harness issued 115,900,809,216 logical write bytes and
103,622,246,400 byte-verified read bytes. It completed 54 files, verified the
required 50-file batch, and removed all 58 names that had become visible.

After clean unmount, a new process recovered generation 18, activated one Exact
Index Run, mounted successfully, and exposed zero names. DATA containers remain
because GC is not implemented.

## Host and storage

- Linux `6.12.0-211.16.1.el10_2.0.1.x86_64`;
- 13th Gen Intel Core i7-1370P VM, 10 logical CPUs, one NUMA node, 64-byte cache
  lines, 4-KiB pages;
- 24.3-GB-class guest RAM plus 6 GiB swap;
- Rust 1.97.1, release profile with overflow checks and debug symbols;
- source revision `ee8c9c27e6efccc69cbc2f793e8617e9e2ad8fa6`, dirty working tree containing
  the checkpoint/index/FUSE work under test;
- `/dev/sdb`, 20 GiB XFS, metadata and Exact Index;
- `/dev/sdc`, 200 GiB XFS, immutable DATA containers.

Both test devices are Hyper-V virtual disks reporting rotational media. This
run does not establish physical NVMe-versus-HDD behavior.

## End-to-end throughput

Rates are one-second logical-byte samples. `p95 active` excludes deliberately
idle checkpoint-settle and admission-closed seconds; `p95 wall` includes them.

| path | active mean | p95 active | p95 wall | peak |
| --- | ---: | ---: | ---: | ---: |
| FUSE write | 1.364 GB/s | 2.471 GB/s | 1.623 GB/s | 2.789 GB/s |
| verified FUSE read | 0.810 GB/s | 1.187 GB/s | 0.927 GB/s | 1.292 GB/s |
| `/dev/sdc` read | — | 304.5 MB/s | 213.1 MB/s | 419.1 MB/s |
| `/dev/sdc` write | — | 211.8 MB/s | 0.067 MB/s | 221.0 MB/s |

All 50 first-cycle writes finished at second 402. Verified reads began at
second 423 and all 50 reads and deletes completed at second 541. High logical
write rates mostly measure admission into RAM-backed dirty epochs; they are not
durable-device throughput.

The data device completed 192,467 reads totaling 18,635,485,184 bytes and
1,858 writes totaling 1,994,260,480 bytes. Its active p95/peak were
3,651/4,496 read IOPS and 180/196 write IOPS. The metadata device completed
2,066 reads (395,599,872 bytes) and 741 writes (130,998,272 bytes).

## Reduction and store efficiency

The checkpoint writer processed 154,762,986,720 logical bytes, 1.335x the
accepted logical writes. A growing file currently takes the full-manifest path
when its size changes, so periodic checkpoints repeatedly re-chunk prefixes.

| counter | result |
| --- | ---: |
| checkpoint logical Chunks | 1,921,689 |
| Exact Hit Chunks / bytes | 1,896,725 / 152,675,386,466 |
| Exact Hit share of checkpoint bytes | 98.651% |
| new Chunk bytes | 2,021,840,290 |
| FILL bytes | 65,759,964 |
| immutable DATA file bytes | 1,992,007,680 |
| RAW / Zstd Records | 22,411 / 405 |
| immutable Containers | 75 |
| metadata file bytes | 127,447,040 |

DATA alone is 1.719% of accepted logical writes, or 58.18x logical/DATA
reduction. DATA plus metadata is 1.829%, or 54.68x. Adaptive compression saved
only 1.476% after Exact selection (`new Chunk bytes` versus Container file
bytes), which is expected for this mostly compressed ISO workload. These ratios
include the second partial cycle and retain unreachable Containers after
delete; there is no GC credit.

## Checkpoint CPU attribution

The table uses generations 2–13 and 16–17, excluding generations 14–15 whose
phase windows overlapped the four-thread verified read. Process CPU includes
all process threads active inside a phase, so concurrent FUSE write handling
makes each leaf an upper bound. The leaves sum to 99.15% of measured checkpoint
CPU.

| checkpoint phase | process CPU | share | wall time |
| --- | ---: | ---: | ---: |
| `FastCDC` boundary discovery/read | 264.378 s | 58.67% | 247.566 s |
| hash plus FILL classification | 38.391 s | 8.52% | 35.669 s |
| Exact lookup and verified candidate | 65.698 s | 14.58% | 52.268 s |
| parallel RAW/Zstd encode | 13.421 s | 2.98% | 4.016 s |
| Container write/reread/sync/publish | 8.593 s | 1.91% | 6.189 s |
| Exact Index publication/compaction | 0.127 s | 0.03% | 0.322 s |
| Manifest/Root/WAL metadata commit | 56.232 s | 12.48% | 71.035 s |
| other checkpoint overhead | 3.812 s | 0.85% | 1.655 s |
| total | 450.653 s | 100% | 418.719 s |

Across the complete 620-second `perf stat` attachment, the daemon consumed
1,024.952 CPU-seconds, averaging 1.653 CPUs, with 1,527,651 context switches,
50,758 CPU migrations, and 8,729,996 page faults. Hardware cycles,
instructions, branches, and cache events were not supported by this VM's PMU
and are deliberately not estimated.

All 16 DATA checkpoints exceeded both five and ten seconds. Checkpoint wall
latency was 31.820 seconds p50 and 47.279 seconds p95/maximum. This directly
fails the current ten-second durability guarantee under this load even though
mutation admission closed and catch-up eventually succeeded.

## RAM, cache, and random I/O

Peak sampled RSS was 21,838,960 KiB (20.83 GiB); peak PSS was 21,834,193 KiB.
Swap reached the full 6-GiB allocation. The adaptive writer's largest measured
Chunk buffer was only 33,549,051 bytes across 434 Chunks. The dominant memory
is therefore concurrent dirty/frozen file epochs: while one four-file snapshot
is checkpointed, four writers can build the next roughly 8-GiB epoch. The
current admission gate reacts to elapsed time, not a hard dirty-byte budget.

There is no application RAM data cache in the durable FUSE read path, so an
application-cache hit rate is **not applicable**, not zero. Every demand read
uses verified Exact-Index pages and Container records. The store issued
9,410,718 range reads totaling 362,259,666,880 bytes plus 75 whole-Container
rereads totaling 1,992,007,680 bytes: 3.515x store-level read amplification
versus verified logical bytes.

The data device read only 18,635,485,184 bytes. Comparing store-requested bytes
with device bytes gives a 94.884% *device-read avoidance proxy* from Linux page
cache/coalescing. It is not an application cache hit rate. Device bytes were
17.98% of verified logical bytes.

At the Store I/O seam, 9,398,596 of the 9,410,718 bounded range reads were
non-sequential relative to the prior range for that Container. The kernel
collapsed/cache-served those calls into 192,467 completed device reads. The
exact number of random block requests reaching `/dev/sdc` is unavailable:
`blktrace` was denied `Operation not permitted`. Therefore the report gives
both the exact application-issued random-range count and exact total device
I/O count, but does not mislabel all device I/O as random.

Container publication issued 486,405 writes totaling 1,992,314,880 bytes, with
75 non-sequential header rewrites. XFS/page cache merged them into 1,858 device
writes.

## Isolated reduction-profile matrix

Each cumulative profile ingested and freshly restored two 2.07-GB variants in
a separate process with 10 workers and a nominal 128-MiB in-flight budget.
`CPU` is user plus system time for ingest and restore; RSS is process maximum.
Profiles are cumulative, not safely subtractable marginal costs, because Exact
changes retained archive size and therefore restore/RSS work.

| profile | CPU | max RSS | ingest | restore | payload | salient work |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| RAW | 10.33 s | 7.77 GiB | 1.791 s | 3.602 s | 4.145 GB | 63,246 RAW |
| CDC | 16.57 s | 7.76 GiB | 7.835 s | 3.724 s | 4.142 GB | 51,438 Chunks |
| Exact | 14.62 s | 5.74 GiB | 7.763 s | 3.774 s | 1.980 GB | 2.162 GB Exact |
| Compression | 27.64 s | 7.72 GiB | 10.009 s | 4.169 s | 4.009 GB | 3,368 Zstd |
| Grouping | 30.17 s | 7.76 GiB | 9.748 s | 4.224 s | 4.005 GB | 934 Zstd regions |
| Similarity | 127.90 s | 5.77 GiB | 20.142 s | 4.011 s | 1.980 GB | 16 candidates |
| Delta | 122.20 s | 5.77 GiB | 19.449 s | 4.098 s | 1.978 GB | 16 Depth-1 Delta |
| Reorder | 238.80 s | 7.76 GiB | 31.589 s | 4.291 s | 4.005 GB | 8,694 reordered |
| All | 115.87 s | 5.78 GiB | 18.747 s | 4.420 s | 1.941 GB | Exact + 394 Zstd + 16 Delta |

This reference engine still owns whole input/output vectors, so its 5.7–7.8
GiB RSS is not the durable streaming checkpoint's intended memory model.

## Priority findings

1. Add a hard byte-counted dirty/in-flight budget with backpressure before four
   writers can retain two full four-file epochs. Time-only admission is too
   late.
2. Preserve an incremental FastCDC rewrite window for append/size-changing
   files. Re-chunking full growing files caused 1.335x checkpoint processing
   amplification and made `FastCDC` the 58.7% CPU bottleneck.
3. Meet the ten-second commit guarantee under admitted load. Every DATA
   checkpoint violated it; closing admission after five seconds did not make
   already accepted writes durable by ten seconds.
4. Add a bounded verified data/record cache and Exact-Index page cache. The
   current reader generated 9.4 million non-sequential range calls and 3.515x
   store-level read amplification.
5. Batch Container writes above 4 KiB. XFS merged 486,405 Store writes into only
   1,858 device writes, but syscall/open overhead remains avoidable.
6. Implement GC before interpreting post-delete physical occupancy as live
   space efficiency.

Follow-up on 2026-08-16: priority 1 now has a first pressure-control slice. The
daemon wakes at 512 MiB of unique reachable active Dirty DATA and closes
mutation admission while the pressure checkpoint catches up; the same threshold
also closes admission when a next epoch fills during a time-triggered
checkpoint. A focused 512-MiB kernel-FUSE recovery test is recorded in the
[durable checkpoint report](../testing/durable-posix-checkpoint.md). This bounds
active checkpoint batches but is not yet a complete process-RSS budget: one
frozen epoch and bounded encoder buffers coexist, and the full 10-minute
workload must be rerun before claiming a new peak-RSS result.

### Bounded-dirty rerun

The same 50-variant, four-writer harness was rerun for 600 seconds after the
512-MiB pressure trigger was connected. Peak sampled RSS was 2,512,704 KiB
(2.40 GiB), 88.5% below the original 20.83-GiB peak. Peak process Swap was
zero, `/proc/vmstat` recorded zero swap-outs, and minimum `MemAvailable` was
20,187,436 KiB. The host began with old pages in its configured swap; 200 pages
were read back and SwapFree increased by 688 KiB. This is not fastdup swap use.

The memory result is a pass, but the requested lifecycle and throughput are a
fail. The run admitted only 13,964,673,024 write bytes: four complete and four
partial files, all removed at the deadline. It did not reach the read/verify
phase because the first 50-file write batch did not complete. Recovery selected
the final empty namespace and exposed zero names.

There were 26 pressure checkpoints. They reprocessed 105,792,583,040 logical
bytes, 7.576x the accepted writes, because each size-changing checkpoint still
re-chunks the full growing file prefix. Checkpoint wall latency was 20.186
seconds p50, 50.002 seconds p95, and 67.267 seconds maximum. CDC alone consumed
481.957 CPU-seconds; Exact lookup consumed 50.845 seconds and encoding 8.786
seconds. The Store requested 337,609,452,032 bytes across 9,095,601 range reads,
of which 9,086,390 were non-sequential. The dirty bound therefore fixed the
unbounded epoch memory, but exposed full-prefix reprocessing as the dominant
CPU, read-amplification, and ten-second-durability blocker.

An allocator A/B run used nearly identical 30-second load (2.676 GB versus
2.684 GB accepted). Setting `MALLOC_MMAP_THRESHOLD_=131072` reduced sampled
peak RSS from 1,173,072 KiB to 766,356 KiB. After the empty checkpoint it used
81 MiB RSS and zero anonymous huge pages. A separate
`MALLOC_ARENA_MAX=2` run that accepted 3.049 GB still retained about 620 MiB,
including 448 MiB of anonymous huge pages, so merely limiting arena count is
not the primary remedy. Large temporary planning and codec buffers were being
retained by glibc after use; this tuning does not replace the application byte
budget. The operational no-swap and allocator requirements are recorded in
[memory and swap containment](../operations/memory-and-swap.md).

## Reproduction artifacts

All generated artifacts remain outside Git under
`/source/fastdup/.artifacts/io-intensive-600s.2iL8MO`:

- `workload.log`: one-second logical rates and lifecycle result;
- `daemon.log`: checkpoint phase/counter lines and Store I/O counters;
- `system-samples.csv`: per-second RSS/PSS/process-I/O and raw block stats;
- `device-rates.csv`: derived metadata/data device rate samples;
- `perf-stat.txt`: supported process-wide performance counters;
- `reduction-matrix/`: nine CSV outputs and GNU time reports;
- `recovery-daemon.log` and `recovered-names.txt`: remount evidence.

The immutable metadata and DATA stores are respectively
`/source/fastdup/.artifacts/tier-meta/io-intensive-600s.cDPD53` and
`/source/fastdup/.artifacts/tier-data/io-intensive-600s.5zaf2a`.

The bounded-dirty rerun artifacts are under
`/source/fastdup/.artifacts/io-intensive-pressure-600s.1TcmaG`; the allocator
A/B artifact is `/source/fastdup/.artifacts/ram-guard-mmap.ZyKgaU`.

Implementation follow-up: growing-file append checkpoints now preserve the
installed Manifest prefix and replay at most the final 256-KiB DATA Chunk before
streaming the appended suffix through FastCDC. The earlier measurements remain
the long-run baseline.

A fresh 60-second kernel-FUSE tracer run with the same four-writer, 50-variant
plan accepted 8,474,329,088 bytes and processed 8,501,778,390 checkpoint bytes:
**1.0032x** logical reprocessing instead of 7.576x. Sixteen DATA checkpoints had
3.037-second p50, 5.805-second p95, and 6.871-second maximum wall latency. FastCDC
used 5.236 CPU-seconds, while metadata graph commit became the new dominant
phase at 38.106 CPU-seconds. Store range-read amplification fell to 2.779x.
Peak RSS was 1,242,028 KiB with the allocator containment setting; process Swap
and system swap-outs remained zero. After clean shutdown a new daemon recovered
one healthy Exact-Index Run, exposed the expected empty namespace, and used
zero Swap. This tracer proves the incremental path but does not replace another
complete 600-second lifecycle run.

Tracer artifacts are under
`/source/fastdup/.artifacts/fuse-append-stream.cPIEM7` with immutable stores in
`/source/fastdup/.artifacts/tier-meta/fuse-append-stream.eEJPVU` and
`/source/fastdup/.artifacts/tier-data/fuse-append-stream.XRcHtR`.

The following Successor Graph Proof tracer used the same 60-second workload
after [ADR 0036](../adr/0036-compose-successor-data-proofs-from-the-installed-generation.md)
was implemented. It accepted 20,538,982,400 bytes (8 complete and 4 partial
files), 2.42x the prior append tracer. Thirty-nine DATA checkpoints processed
20,551,224,744 bytes, only **1.0006x** the accepted writes. Checkpoint p50/p95/max
fell to 1.158/1.717/1.933 seconds. Metadata commit consumed 13.301 CPU-seconds
despite 2.44x as many checkpoints, versus 38.106 seconds before proof reuse.
Store range-read amplification fell from 2.779x to **0.673x**.

Peak RSS was 832,252 KiB, process Swap and system swap-outs remained zero, and
minimum `MemAvailable` was 21,698,080 KiB. A new daemon then performed the
deliberately complete recovery proof, selected six healthy Exact-Index Runs,
exposed the expected empty namespace, and used zero Swap. Recovery still issued
86,416,806,976 DATA range-read bytes while walking historical generations; it is
now the clearly separated startup/recovery bottleneck rather than normal commit
work.

Successor-proof artifacts are under
`/source/fastdup/.artifacts/fuse-successor-proof.D8f2wQ`, with immutable stores
in `/source/fastdup/.artifacts/tier-meta/fuse-successor-proof.sjDqi2` and
`/source/fastdup/.artifacts/tier-data/fuse-successor-proof.YNUmr5`.

The recovery follow-up from
[ADR 0037](../adr/0037-separate-structural-recovery-from-current-data-proof.md)
reused that exact immutable DATA store and a workspace-local copy of its
metadata. WAL/root/transition validation still covered the complete reachable
prefix, while complete Manifest/DATA proof considered only the current and
immediately previous live generations. The production FUSE daemon reached a
mounted, non-degraded state with all six Exact-Index Runs in **1.829 seconds**.
DATA telemetry changed as follows:

| Recovery DATA I/O | Historical forward-graph walk | Current/previous candidates | Paired generation envelopes |
| --- | ---: | ---: | ---: |
| Whole reads | 90 | 90 | **0** |
| Whole-read bytes | 1,964,171,264 | 1,964,171,264 | **0** |
| Range reads | 2,281,356 | **0** | 180 |
| Range-read bytes | 86,416,806,976 | **0** | **737,280** |
| Random range reads | 2,277,974 | **0** | 90 |

The generation-recovery amplification is therefore eliminated for this final
empty namespace without weakening structural transition validation or turning
old WAL history into snapshots. Peak RSS in a separate 20-second lifecycle run
of the current/previous-candidate slice was 72,260 KiB and Swap was zero. That
run isolated the remaining 1.964-GB whole-read cost to Container-generation
high-water discovery. The subsequent
[ADR 0038](../adr/0038-recover-container-generations-from-paired-envelopes.md)
implementation replaced it with physical length plus paired fixed envelopes:
90 Containers required 180 bounded reads and 737,280 bytes, with no payload
read. A hot-cache production-daemon smoke reached the mounted state in 54 ms;
this latency is not a cold-device benchmark. Evidence is in
`/source/fastdup/.artifacts/recovery-ready-time.StyvAD` and
`/source/fastdup/.artifacts/recovery-current-only.Syb50b`, with the final
envelope run in `/source/fastdup/.artifacts/recovery-envelope.EA85FW`.

## Incremental-streaming and bounded-recovery 600-second rerun

The complete 50-variant lifecycle was repeated after incremental append
FastCDC, Successor Graph Proofs, current/previous recovery selection, and paired
Container-generation envelopes were all connected. The same pinned ISO, edit
plan, four workers, 512-MiB dirty-pressure threshold, XFS tiers, and production
FUSE daemon were used. This rerun is a **PASS** for byte-exact lifecycle,
bounded memory, recovery, and the ten-second durability SLO under this workload.

The harness ran for 601.116 seconds. It wrote 124,228,337,664 logical bytes,
completed 58 files, byte-verified the required first 50 files (103,622,246,400
bytes), completed one full create/write/read/delete cycle, and removed all 62
complete or partial names. A fresh process mounted the final empty generation
in 55 ms, activated ten Exact-Index Runs without degradation, and exposed zero
names.

| logical path | p95 active | p95 wall | peak |
| --- | ---: | ---: | ---: |
| FUSE write | 545.3 MB/s | 541.1 MB/s | 1,019.2 MB/s |
| byte-verified FUSE read | 922.7 MB/s | 801.1 MB/s | 994.1 MB/s |

There were 236 DATA checkpoints plus one final namespace-only checkpoint. DATA
checkpoint wall latency was 1.562 seconds p50, 2.496 seconds p95, and 2.846
seconds maximum. No checkpoint exceeded five or ten seconds. The 512-MiB
pressure trigger closed and reopened mutation admission 226 times. Incremental
planning processed 1.000480x accepted logical writes; the old bounded-dirty
rerun processed 7.576x and had 20.186/50.002/67.267-second p50/p95/max latency.

### Reduction and CPU

| reduction counter | result |
| --- | ---: |
| Exact Hit bytes/share | 122,188,162,852 / 98.311% |
| new Chunk bytes/share | 2,059,162,318 / 1.657% |
| FILL bytes | 40,630,148 |
| immutable DATA bytes | 2,030,473,216 |
| RAW / Zstd Records | 23,008 / 427 |
| immutable Containers | 287 |
| logical/DATA reduction | 61.18x |
| logical/(DATA + metadata) reduction | 42.95x |

The current namespace is empty after delete and GC is not implemented, so the
ratios measure ingest reduction while retaining unreachable physical history;
they are not a live-capacity ratio. Container bytes were 98.607% of new Chunk
bytes, a modest 1.393% post-dedup compression saving expected for an ISO.

Checkpoint process CPU was 399.282 seconds. Phase counters are process-wide
upper bounds when FUSE requests overlap them:

| checkpoint phase | process CPU | share of checkpoint total |
| --- | ---: | ---: |
| FastCDC | 77.864 s | 19.50% |
| hash + FILL | 31.694 s | 7.94% |
| Exact lookup/verification | 118.532 s | 29.69% |
| parallel RAW/Zstd encode | 7.618 s | 1.91% |
| Container publication | 4.096 s | 1.03% |
| Exact-Index publication/compaction | 1.411 s | 0.35% |
| Manifest/Root/WAL metadata | 153.440 s | 38.43% |

The whole daemon consumed 971.649 task-clock seconds, averaging 1.616 CPUs.
Metadata construction/commit is now the largest attributed stage, followed by
Exact verification and FastCDC; compression is no longer a material CPU cost
for this corpus. Similarity, Delta, and Reorder are not enabled in the durable
FUSE checkpoint and therefore have no attributed time in this run.

### RAM, cache proxy, and physical I/O

Peak sampled RSS/HWM was 2,385,016 KiB (2.275 GiB), 89.1% below the original
20.83-GiB run and below the earlier bounded-dirty 2.40-GiB rerun. Fastdup
`VmSwap` remained zero throughout; minimum system `MemAvailable` was 20,326,528
KiB. The largest measured encoder Chunk buffer was only 33,553,656 bytes across
434 Chunks. The remaining RSS is not the pre-pipeline dirty payload alone; it
also includes live/installed file state, Manifest and Exact-Index state, Linux
page cache mappings, and allocator retention.

There is still no application data-cache hit counter. The Store requested
317,274,720,576 bytes through whole/range reads, while `/dev/sdc` completed only
597,917,696 read bytes. The resulting **99.812% device-read-avoidance proxy**
reflects Linux page cache/coalescing and must not be presented as an application
cache hit rate. Against the 103,622,246,400 verified client-read bytes alone,
the device-read avoidance lower bound is 99.423%.

Store telemetry classified 8,109,667 bounded reads as non-sequential, but the
data device completed only 5,633 physical read requests during the complete
run. It completed 2,630 writes totaling 2,036,637,696 bytes. The metadata device
completed two reads (110,592 bytes) and 6,739 writes (910,647,296 bytes).

Fresh recovery over 287 Containers performed zero whole reads and exactly 574
Header/Footer range reads totaling 2,351,104 bytes; 287 Footer seeks were
classified non-sequential. This matches the 8-KiB-per-Container envelope rule.

Complete evidence is under
`/source/fastdup/.artifacts/iso50-recovery-600s.KAjVkw`; immutable Metadata and
DATA stores are respectively
`/source/fastdup/.artifacts/tier-meta/iso50-recovery-600s.RwsFSD` and
`/source/fastdup/.artifacts/tier-data/iso50-recovery-600s.Wz2LXa`.
