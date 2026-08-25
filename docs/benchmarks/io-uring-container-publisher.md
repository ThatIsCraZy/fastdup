# `io_uring` Container publisher microbenchmark

Date: 2026-08-21

The benchmark exercises the complete immutable Container publication protocol,
not isolated `pwrite`: encode, Building/Body/Sealed writes, exact-length fixup,
complete writer reread and VERIFY, file fsync, no-replace rename, and root
directory fsync. Ten independent publisher threads create 1,000 Containers with
one 128-KiB RAW Chunk each. Both modes use the same `ContainerRepository`; only
the `StorageIo` adapter changes.

The reproducible command is the workspace example. The optional final argument
sets logical payload bytes; payloads larger than 256 KiB are split into bounded
RAW Chunks inside the same Container.

```console
cargo run --release -p fastdup-io-uring --example publisher_bench -- MODE ROOT 1000 10 [PAYLOAD_BYTES]
```

All build and profile output belongs under `.artifacts`. The measured kernel was
6.12.0-211.47.1.el10_2.x86_64 with `kernel.io_uring_disabled=0`. Both tested
filesystems were XFS on Microsoft virtual SCSI disks.

| XFS target | Backend | Wall time | Containers/s | Max RSS | Voluntary context switches |
|---|---:|---:|---:|---:|---:|
| root/system disk | synchronous | 1.719 s | 581 | 9,136 KiB | 8,249 |
| root/system disk | `io_uring` | 2.918 s | 342 | 9,528 KiB | 16,474 |
| 200-GiB data disk | synchronous | 1.848 s | 541 | 9,064 KiB | 8,038 |
| 200-GiB data disk | `io_uring` | 3.807 s | 262 | 9,156 KiB | 17,004 |

The data-disk Ring run submitted and completed 6,526 SQEs, coalesced 1,000
logical root-sync callers into 526 directory-fsync submissions, and peaked at
1,290,240 submitted buffer bytes. It used no Swap. Despite successful
coalescing, this first adapter is 41% slower on the root disk and 52% slower on
the data disk. The synchronous `StorageIo` interface forces a caller/worker
round trip for every individual phase and requires a bounded copy to give a
borrowed payload a kernel-completion lifetime. Those costs dominate on these
devices.

These rows are the historical shallow adapter. It copied borrowed payloads and
returned to the caller after each operation phase.

## Owned multi-phase publisher

The deep publisher transfers each prepared image once and runs all dependent
phases within the ring worker. Complete writer verification retains only the
expected full-image Container BLAKE3: the sealed allocation is dropped before
the reread allocation. The 256-MiB budget therefore charges one kernel-I/O
image rather than two. All normal runs below reported
`borrowed_write_copy_bytes=0`, equal
owned-started/completed counts, and zero in-flight bytes at quiescence. The
host's historical Swap state was not sampled around these short runs, so no
process-Swap conclusion is drawn from this microbenchmark.

Three interleaved runs on the 200-GiB XFS produced these medians:

| Workload | Backend | Median wall | Median containers/s | Peak charged bytes |
|---|---:|---:|---:|---:|
| 1,000 x 128 KiB, 10 publishers | synchronous | 0.617 s | 1,620 | n/a |
| 1,000 x 128 KiB, 10 publishers | owned `io_uring` | 1.021 s | 979 | 1,433,600 |

The owned Ring path is still 39.6% lower throughput for small Containers. It
coalesced 1,000 durable names into 174--179 directory-sync submissions, but
the single worker also performs every complete Container decode serially.

One full-size run used 50 Containers with 63 MiB logical payload each, split
into 256-KiB RAW Chunks:

| Backend | Wall | Logical throughput | Max RSS | Root sync submissions |
|---|---:|---:|---:|---:|
| synchronous | 4.786 s | 690 MB/s | 2,452,568 KiB | n/a |
| owned `io_uring` | 8.006 s | 413 MB/s | 1,583,284 KiB | 14 |

The Ring path used 264,601,600 peak charged bytes, below its 256-MiB hard
limit, and cut process peak RSS by about 35% while remaining about 40% slower.
That was useful memory evidence but failed the throughput gate.

## Bounded CPU verifier pool

The publisher now sends complete rereads of at least 1 MiB to a permanent pool
of at most eight CPU workers. Workers receive no file descriptor or durability
state. They return only a verified Container or an integrity error; the ring
worker alone may then submit file sync, rename, and root sync. Smaller images
stay inline after measurement showed that scheduling overhead dominates their
decode cost.

The same 50-by-63-MiB workload on the same XFS produced:

| Backend | Wall | Logical throughput | Max RSS | Peak active verifiers |
|---|---:|---:|---:|---:|
| synchronous | 5.530 s | 570 MiB/s | 1,942,668 KiB | n/a |
| pooled `io_uring` | 5.919 s | 532 MiB/s | 1,583,060 KiB | 4 |

Relative to the preceding 8.006-s owned-Ring run, verifier parallelism reduced
wall time by about 26%. In the simultaneous comparison it is about 7% behind
the synchronous backend while retaining an 18% lower process peak RSS. All 50
jobs completed without a VERIFY failure, the publication budget peaked at
264,601,600 bytes, and 50 names used 14 root-directory syncs. `pswpin`,
`pswpout`, and host Swap usage were unchanged at zero across the run.

Three interleaved 1,000-by-128-KiB runs confirmed that all small verification
stayed inline (`verification_started=0`). Median wall time was 0.673 s
synchronous and 1.542 s Ring. The verifier pool therefore fixes a real
large-Container serialization bottleneck without pretending to solve the
remaining small-file Ring overhead.

The adapter is correct and available. These measurements originally kept it
opt-in. ADR 0058 supersedes that implementation and its setup fallback.

## CQE-driven publisher

On 2026-08-24 the worker changed from a phase-wide `submit_and_wait(batch)`
barrier to one persistent CQE-driven loop. An `eventfd` wakes the ring for new
commands and completed CPU verification, large verification runs asynchronously
on the existing Rayon pool, and Container length fixup uses
`IORING_OP_FTRUNCATE`. The production daemon now requires this adapter.

`SINGLE_ISSUER | DEFER_TASKRUN` was tested first. Removing `DEFER_TASKRUN`
improved the 1,000-by-128-KiB publisher by 7.3 and 17.8 percent in alternating
pairs, so the final ring retains only `SINGLE_ISSUER`.

Two final Rocky-ISO SingleStream pairs used opposite order, fresh repositories,
the same Samba configuration, and zero Swap at admission. Aggregate throughput
changed by -15.9 and +19.4 percent, a paired median of +1.7 percent. Completed
write p99 changed by +16.1 and -20.1 percent, a paired median improvement of
2.0 percent. The long VM series had strong time-order drift, so these numbers
support throughput neutrality rather than a precise gain.

The two-stream gate also used opposite order. Aggregate throughput and the
slower stream changed by -2.2 and +33.5 percent, a paired median of +15.6
percent. Peak verifier activity reached two, every run started and ended with
zero Swap, and no synchronous fallback was available. The JSON reports are
`smb-io-uring-final-*.json` and `smb-io-uring-multistream-nodefer-*.json` under
`.artifacts/benchmarks/`.
