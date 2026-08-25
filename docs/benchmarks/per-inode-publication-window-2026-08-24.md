# Per-inode Container publication window

Date: 2026-08-24

The baseline allows one detached Container publication per inode. The
challenger allows two when only one inode is active, then returns to one per
inode when another stream appears. Both variants use the same two publication
workers, 64-MiB detached-payload budget, SeqCDC-v1 profile, and required
`io_uring` backend. Per-inode retirement remains ordered.

The test used `Rocky-10.2-x86_64-minimal.iso`, 2,072,444,928 bytes, on the local
SMB/FUSE path. Metadata and Container data used separate XFS disks. Every run
used a fresh repository and reported zero host and cgroup Swap. Baseline and
challenger binaries were frozen before the comparison:

- Baseline SHA-256: `71b2ecfa73bba065491e6bad44e6ecae8cb9b17aa84da04aa3041bc53ea69e97`
- Challenger SHA-256: `972a3d74c8c9c81f6a0fdf71988fce348292bb6485f7907a2bb53a6c80381d24`

## SingleStream

Each run uploaded the ISO three times in sequence. The table reports the
challenger's paired change against its immediately preceding baseline.

| Pair | Aggregate | First upload | Second upload | Third upload | Completed-write p99 | Daemon CPU | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | +15.2% | +41.9% | -3.8% | -13.6% | -29.5% | -2.5% | -7.3% |
| 2 | +13.2% | +26.7% | -1.0% | +2.5% | -21.1% | +2.4% | -16.3% |
| Paired median | +14.2% | +34.3% | -2.4% | -5.6% | -25.3% | -0.1% | -11.8% |

The physical first upload gains consistently. Exact-repeat uploads do not;
their result ranges from a 13.6-percent loss to a 2.5-percent gain. Overall
throughput still rises because the first upload has the largest amount of
publication work. CPU time stays flat.

## Two simultaneous streams

Each run started two uploads together. Four alternating pairs reduce the
impact of the large host variance seen in the first pair.

| Pair | Baseline aggregate | Challenger aggregate | Aggregate change | Slowest-stream change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 219.7 MiB/s | 285.6 MiB/s | +30.0% | +30.0% | -23.1% |
| 2 | 450.2 MiB/s | 460.2 MiB/s | +2.2% | +2.2% | -2.2% |
| 3 | 442.7 MiB/s | 471.3 MiB/s | +6.5% | +6.5% | -6.1% |
| 4 | 379.1 MiB/s | 501.4 MiB/s | +32.3% | +32.3% | -24.4% |
| Paired median | 410.9 MiB/s | 465.8 MiB/s | +18.2% | +18.2% | -14.6% |

All four pairs pass the no-regression gate. Median daemon CPU changed by
+1.9 percent, peak RSS by -9.8 percent, and context switches by -8.5 percent.
Repository allocation moved in both directions because changed publication
timing also changed checkpoint boundaries. This short, identical-input test
cannot separate that timing effect from deduplication quality.

`peak_active_verifications` was one in SingleStream and two in MultiStream.
The SingleStream gain therefore comes from overlapping the wider publication
path, not from running two Container verifier jobs at once.

## Raw reports

The JSON reports are under `.artifacts/benchmarks/` with prefixes
`smb-publication-window-` and `smb-publication-window-multistream-`.
