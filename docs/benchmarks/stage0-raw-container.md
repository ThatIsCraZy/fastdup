# Stage-0 RAW container baseline

Date: 2026-08-15. Format and policy: container v1, RAW, 512 records of 64 KiB,
32 MiB logical input, no CDC/dedup/compression/grouping/similarity/delta/reorder.

The benchmark writes a direct-file durability baseline, then measures the
production container path including encoding, writer-side full verification,
file sync, atomic no-replace publication, and directory sync. The read follows
immediately and is therefore explicitly a page-cache-hot full verification, not
a restore-media benchmark.

## Host and storage

- Linux `6.12.0-211.16.1.el10_2.0.1.x86_64`
- Rust `1.97.1`, release profile with debug info and overflow checks
- XFS/xfsprogs `6.16.0`, 4 KiB physical sectors, `noatime,nodiratime`
- metadata-role device: 20 GiB, serial
  `600224800e9dda3792cf03142ff497f4`
- data-role device: 200 GiB, serial
  `600224801b29ebf95cd76334f578fcd5`

Both devices identify as Microsoft virtual SCSI disks and report `ROTA=1`.
Their role is assigned by size and the test-machine contract. These results can
validate correctness, sync cost, and separated placement, but must not be used
as a physical NVMe-versus-HDD forecast.

## Results

Three consecutive runs per tier after release compilation; values are median
MiB/s:

| tier | direct write + file/directory sync | container publish + verify + sync | hot read + full verify |
| --- | ---: | ---: | ---: |
| metadata role | 1,464.63 | 311.16 | 1,015.50 |
| data role | 1,510.79 | 323.01 | 1,047.40 |

A subsequent three-run data-tier check of the implemented startup discovery
path (directory enumeration, canonical-name parsing, identity pairing, and full
container verification) measured a median 1,004.32 MiB/s for a single hot
container. The later [ten-ISO sustained run](stage1-iso-raw-ingest.md) measures a
620-container, 20.83-GB full startup audit without retaining payloads.

The sealed file is 33,730,560 bytes for 33,554,432 logical bytes: 176,128 bytes,
or about 0.525%, of v1 record/index/footer overhead.

## Finding: XFS speculative EOF allocation

The initial offset-write implementation left 130,944 allocated 512-byte blocks
(67,043,328 bytes) behind a 33,730,560-byte file. XFS had retained an unwritten
extent beyond EOF. Reasserting the known exact length before reader verification
and `fsync` reduced allocation to 65,880 blocks (33,730,560 bytes) on both
devices. `SetLen` is consequently a named storage operation and part of the
exhaustive crash sequence rather than an untested performance tweak.

## Reproduction

```bash
cargo run --release -p fastdup-store --example raw_store_bench -- \
  /source/fastdup/.artifacts/tier-data/bench 32
```

The command leaves each exact run directory in the supplied artifact root so
file length, allocated blocks, and extents remain inspectable. Raw outputs and
corpora are intentionally ignored by source control.
