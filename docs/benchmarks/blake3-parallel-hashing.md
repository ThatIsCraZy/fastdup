# BLAKE3 parallel-hashing gate

Measured on 2026-08-22 with the workspace-local release toolchain and the ten
effective Rayon workers of the benchmark host. Each microbenchmark variant
hashed at least 512 MiB of deterministic input. Results are retained under
`.artifacts/blake3-hash-bench/results-v2.txt`.

| Input | serial GiB/s | `update_rayon` GiB/s | speedup |
|---:|---:|---:|---:|
| 64 KiB | 3.378 | 0.131 | 0.039x |
| 128 KiB | 3.563 | 0.189 | 0.053x |
| 256 KiB | 3.516 | 0.323 | 0.092x |
| 1 MiB | 3.360 | 1.173 | 0.349x |
| 2 MiB | 3.322 | 3.865 | 1.164x |
| 4 MiB | 3.303 | 7.654 | 2.317x |
| 8 MiB | 3.009 | 10.458 | 3.476x |
| 32 MiB | 2.729 | 16.691 | 6.116x |

The v1 gate is therefore 2 MiB. FastCDC Chunks remain serial internally and
are distributed as independent hashes over the existing CPU pool. Large
Container writers and readers may use BLAKE3 tree parallelism only when they
hold the full-pool permit budget.

The final three-copy Rocky ISO SMB tracer is retained at
`.artifacts/benchmarks/smb-single-stream-blake3-parallel-final.json`. It reports
371 FastCDC hash batches, a maximum of ten hash workers, 71 of 72 pooled
Container verifications using parallel BLAKE3, zero verification failures,
zero daemon Swap, 417.7 MiB/s aggregate completed-write throughput, and an
8.083-second completed-file p99. A second active-io_uring run measured
293.1 MiB/s, so these two end-to-end samples demonstrate correct activation but
are not sufficient to attribute an SMB throughput delta independently of host
and checkpoint timing variance.

## Retained Chunk identities

The follow-up implementation carries each identity from the stable FastCDC
batch into Container preparation. It also computes the RAW candidate's encoded
cost without serializing the losing candidate and removes the redundant Zstd
decode immediately before the mandatory publication reread. The format and
repository tests require the ordinary and proof-bearing writers to emit
byte-identical valid images and require a deliberately wrong carried identity
to be rejected before canonical publication.

The corresponding profile and benchmark reports are retained at
`.artifacts/profiles/smb-single-stream-prehashed-final.perf.data`,
`.artifacts/benchmarks/smb-single-stream-prehashed-profile.json`, and
`.artifacts/benchmarks/smb-single-stream-prehashed-final.json`. BLAKE3 fell from
26.83% to 20.13% of sampled CPU, a 25.0% relative reduction. In the unique-data
phase it accounted for 19.61%; in the duplicate phase it remained 23.70%
because exact lookup must still establish content identity.

The end-to-end rates are not an optimization verdict. DATA-device write time
rose from 11.067 seconds in the earlier profile to 24.731 seconds in the
follow-up profile, and the non-profile repeat still spent 20.231 seconds there.
Even duplicate copies, which do not use the changed new-Container writer, were
slower. The follow-up daemon used zero Swap. A controlled A/B run with stable
device service time is required before attributing an SMB throughput change to
this CPU optimization.
