# Linux 6.12 TAR reduction benchmark

> Historical frozen-snapshot result. The later
> [permanent online-ingest A/B](linux-6.12-online-similarity-2026-09-05.md)
> uses all 50 versions sequentially without an intermediate remount/rebuild and
> reaches 23.84:1 versus 6.59:1 with Similarity disabled.

Date: 2026-09-02  
Revision: `916248dbc6f38919ca78d43facdbe3157e972e79` plus the uncommitted
publication-ordering and Similarity bucket-boundary fixes described below.

## Result

Fifty uncompressed Linux kernel TAR streams contain 77,363,015,680 logical
bytes (72.05 GiB). The final repository with Advanced Reduction enabled uses
12,533,534,720 allocated bytes (11.67 GiB) across DATA and Metadata, for a
conservative whole-repository data-reduction factor of **6.17:1**. Exact-only
measured **6.174:1** and `dependent-v1` measured **6.172:1**; the tiny reversal
is run-to-run allocation variation in the independently rebuilt base halves.

The controlled target-half comparison isolates the actual Similarity effect:

| Target policy | Target logical bytes | Additional repository bytes | Factor |
| --- | ---: | ---: | ---: |
| Exact/independent only | 38,682,009,600 | 5,802,106,880 | 6.6669:1 |
| `dependent-v1` | 38,682,009,600 | 5,798,232,064 | 6.6713:1 |

Similarity therefore saved **3,874,816 physical bytes (3.70 MiB, 0.0668%)**.
That is not a practically meaningful storage effect, even though every target
version had immediately adjacent kernel versions in the candidate base. Exact
Dedup, ordinary compression, and sparse/FILL handling account for effectively
all useful reduction on this corpus.

The 6.17:1 figure is an aggregate data-reduction factor, not a separately
isolated Exact-Dedup ratio. This probe did not disable ordinary compression or
FILL representation, so their individual contributions cannot honestly be
split from Exact Dedup.

## Corpus

The corpus is `linux-6.12.1.tar.xz` through `linux-6.12.50.tar.xz` from the
official kernel.org v6.x archive. All 50 downloads were checked once against
the official signed SHA-256 manifest before ingest. Their compressed size is
7,404,375,084 bytes (6.90 GiB).

Each archive was decompressed to one rotating `.tar` file and copied as an
opaque uncompressed TAR stream. The rotating TAR was deleted before preparing
the next version. Maximum allocated staging usage was 8,952,954,880 bytes
(8.34 GiB), below the authorized 10,000,000,000-byte limit.

The source archives remain outside source control at
`/source/fastdup/.artifacts/kernel-benchmark-source`. The earlier 3.5-GiB
Ubuntu source corpus was deleted as requested.

## Controlled A/B method

- Metadata: `/dev/sdb1`, 20-GB XFS at
  `/var/lib/fastdup/repository/metadata`.
- DATA: `/dev/sdc1`, 200-GB XFS at
  `/var/lib/fastdup/repository/data`.
- Odd versions 6.12.1 through 6.12.49 formed a 25-version independent base.
- After every destination copy, `sync FILE` crossed fastdup's file-`fsync`
  durability boundary. There were no destination readback hashes.
- The base received one clean stop because offline index publication rejects a
  recovery-required repository. Exact and Similarity were then rebuilt as one
  coherently bound pair.
- Even versions 6.12.2 through 6.12.50 formed the 25-version target half. One
  arm used `FASTDUP_ADVANCED_REDUCTION=off`; the other used
  `dependent-v1`. There were no intermediate remounts during either target
  half.
- The final stop used `SIGTERM` plus FUSE detach after every target file had
  completed `fsync`, avoiding the redundant full-namespace shutdown catch-up.
- Online GC was disabled. Final allocation is `du -s -B1` on both physical
  tier roots after unmount.
- No final offline scrub was run, per the instruction to rely on the existing
  write/read-path checks and avoid repeated full verification.

The two bases were built independently, so absolute base allocation differs by
7,700,480 bytes. Similarity effectiveness is therefore calculated within each
arm as `final allocation - that arm's indexed-base allocation`, then compared.
The index itself is present and counted identically in both arms.

## Allocated-byte results

| Policy and stage | Logical bytes | DATA bytes | Metadata bytes | Repository bytes | Factor |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off, base before index | 38,681,006,080 | 5,585,682,432 | 436,195,328 | 6,021,877,760 | 6.423:1 |
| Off, base indexed | 38,681,006,080 | 5,585,682,432 | 1,141,919,744 | 6,727,602,176 | 5.750:1 |
| **Off, final** | **77,363,015,680** | **10,903,552,000** | **1,626,157,056** | **12,529,709,056** | **6.174:1** |
| Similarity, base before index | 38,681,006,080 | 5,590,728,704 | 435,871,744 | 6,026,600,448 | 6.419:1 |
| Similarity, base indexed | 38,681,006,080 | 5,590,728,704 | 1,144,573,952 | 6,735,302,656 | 5.743:1 |
| **Similarity, final** | **77,363,015,680** | **10,900,332,544** | **1,633,202,176** | **12,533,534,720** | **6.172:1** |

The final Similarity repository is 3,825,664 bytes larger than the final
Exact-only repository because its independently built base started larger than
the Exact-only base. Within the controlled target interval, Similarity is
3,874,816 bytes smaller. This is why the incremental comparison, rather than a
raw subtraction of final totals, is the valid Similarity result.

## Similarity telemetry

| Metric | Value |
| --- | ---: |
| Queries | 367,873 |
| Candidates | 9,051 |
| Base reads / bytes | 1,248 / 63,413,611 |
| Sparse-XOR trials / accepted | 1,248 / 6 |
| Prefix trials / accepted | 1,248 / 561 |
| Independent fallbacks | 58 |
| No-candidate fallbacks | 367,248 |
| Reported payload bytes saved | 7,535,913 |
| Errors | 0 |

Only 2.46% as many candidates as queries were returned, and accepted dependent
encodings represented 0.154% of queries. The codec reported 7.19 MiB of payload
savings, but Metadata and allocation granularity reduced the observed net
physical saving to 3.70 MiB.

Target copy plus file-`fsync` time was 245.8 seconds with Similarity off and
229.5 seconds with Similarity enabled. This single order-fixed run is a capacity
probe, not a throughput qualification; the apparent timing improvement must
not be attributed to Similarity without repeated randomized runs.

## Operational findings

The current write-through path pins one immutable Exact/Similarity pair for the
mount lifetime. New independent chunks do not enter the queryable Similarity
candidate universe until an offline rebuild and remount. Proposed ADR 0089
records the requirement to refresh a coherently bound pair online without
weakening bounded queries, dependency depth, crash safety, or generation
leases.

The one required clean stop after 25 base TARs performed a full namespace
catch-up before `mount.unmount()` and took minutes while consuming sustained
CPU and logical read activity. Index rebuild and subsequent activation also
took minutes. These are operational costs of the current immutable-snapshot
lifecycle and are separate from the measured storage reduction.

## Defect exposed by the corpus

The first full Odd/Even Similarity attempt panicked on its first target in
`BucketOrdinals::get`: a full 64-representative bucket queried its end cursor at
index 64. `bool::then_some(self.values[index])` evaluated the out-of-bounds
array access eagerly even though the method needed to return `None`.

The implementation now uses lazy `then(|| self.values[index])`. The new
`bucket_ordinals_returns_none_at_capacity` regression test failed
deterministically before the change and passes afterward. Verification:

- `fastdup-store` library: 51 passed, 2 ignored;
- Similarity repository integration suite: 14 passed;
- original end-to-end corpus: all 25 target TARs completed, with zero Advanced
  Reduction errors.

The earlier uncommitted publication-ordering fix from the Ubuntu image corpus
also remains in the tested binary.

## Evidence

Raw artifacts remain outside source control in
`/source/fastdup/.artifacts/kernel-benchmark-results`:

- `kernel-oddeven-off-r2`: final controlled Exact-only arm;
- `kernel-oddeven-sim-r2`: final controlled Similarity arm;
- `kernel-oddeven-sim-r1`: original 64-entry bucket panic;
- `kernel-baseline-r5` and `kernel-similarity-r1`: preliminary one-base runs;
- `download-sha256.log`: one-time verification of all 50 source archives;
- `run_kernel_oddeven_probe.sh`: final A/B orchestration.
