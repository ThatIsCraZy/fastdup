# Single-stream online dependency proof

Date: 2026-08-21

The benchmark uploaded the pinned Rocky Linux 10.2 minimal ISO three times in
sequence through Samba and the durable FUSE mount. Every run used a fresh
repository, separate XFS metadata and DATA devices, the synchronous DATA
adapter, a release build with debug symbols, `perf` at 399 Hz, and zero process
Swap. The local reports remain under `.artifacts/benchmarks/` and are not source
controlled.

| Measurement | Before online proof | Commit proof only | Commit and ingest reuse | Production S3-FIFO |
|---|---:|---:|---:|---:|
| Aggregate SMB write rate | 172.13 MiB/s | 199.02 MiB/s | 285.05 MiB/s | 316.34 MiB/s |
| Copy 1 | 141.90 MiB/s | 162.59 MiB/s | 175.29 MiB/s | 208.20 MiB/s |
| Copy 2 | 188.36 MiB/s | 224.71 MiB/s | 375.06 MiB/s | 423.80 MiB/s |
| Copy 3 | 197.15 MiB/s | 223.57 MiB/s | 464.34 MiB/s | 430.89 MiB/s |
| Completed-file p99 | 13.93 s | 12.16 s | 11.28 s | 9.49 s |
| Daemon CPU | 68.24 s | 49.64 s | 38.60 s | 34.33 s |
| Peak RSS | 1,215.6 MiB | 1,130.4 MiB | 1,138.0 MiB | 1,189.8 MiB |
| DATA range reads | 15.59 GB | 6.54 GB | 0 B | 0 B |
| Metadata-commit wall sum | 13.45 s | 0.83 s | 0.72 s | 0.63 s |

The first change lets an online successor commit consume the mandatory writer
reread proof. The second also lets later writes reuse the same verified
immutable Location while the bounded process-local proof remains resident.
Eviction falls back to the ordinary Exact candidate and Container verifier.
Recovery, scrub, rebuild, and demand reads do not trust the online cache.

In the final profile, the largest self-time symbols are BLAKE3 at 24.74%,
`memmove` at 21.25%, FastCDC at 10.58% including the segmented-tail path, and
Zstd double-fast at 7.39%. DATA I/O and Exact-page lookup are no longer the
single-stream limit for this repeated-ISO workload.

The production S3-FIFO run used the same pinned ISO, Samba configuration, and
separate XFS tiers. Its final Historical Cache contained 24,267 proofs with
5,435,808 accounted resident bytes against a 2,171,804-entry live target. It
recorded 50,751 hits and 50,766 misses, zero admission or allocation rejection,
zero eviction, and zero Swap. The nearly 50-percent final hit rate is expected:
the first copy is cold and the later copies reuse its verified Locations. All
Generation Proof Sets were empty after the final commit. One run is not enough
to attribute the throughput difference solely to the policy, but it rules out
the obvious regression and confirms that repeated streams no longer read the
DATA tier.

The corresponding report names are:

- `smb-single-stream-memmove-hybrid-profile.json`
- `smb-single-stream-online-proof-profile.json`
- `smb-single-stream-online-proof-reuse-profile.json`
- `smb-single-stream-s3-fifo.json`
