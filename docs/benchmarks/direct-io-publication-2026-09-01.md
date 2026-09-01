# Direct I/O Container publication benchmark

Date: 2026-09-01

This A/B series compares the same CQE-driven publisher with a buffered
temporary Container descriptor and with `O_DIRECT`. Direct mode covers the
Building write, Body write, Sealed-Header overwrite, and all three 4-KiB
publication sample reads. It does not replace file fsync, rename, or root
directory fsync, and later demand reads remain buffered.

The format writer serialized directly into a 4-KiB-aligned owned allocation;
both modes transferred that allocation to the ring without a full-image copy.
Every row is the median of three interleaved runs in alternating order. Each
run used a fresh directory, eight publisher threads, Linux
6.12.0-211.49.1.el10_2.x86_64, and XFS on `/dev/mapper/rl_fastdup-root`.
Raw logs are under `.artifacts/benchmarks/direct-io-2026-09-01/`.

```console
cargo run --release -p fastdup-io-uring --example publisher_bench -- \
  ring-buffered ROOT COUNT 8 PAYLOAD_BYTES
cargo run --release -p fastdup-io-uring --example publisher_bench -- \
  ring-direct ROOT COUNT 8 PAYLOAD_BYTES
```

| Payload | Buffered containers/s | Direct containers/s | Throughput change | Buffered p99 | Direct p99 | p99 change | Buffered system CPU | Direct system CPU |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 128 KiB | 1,411 | 1,181 | -16.3% | 8.57 ms | 9.25 ms | +7.9% | 0.15 s | 0.15 s |
| 512 KiB | 1,168 | 1,042 | -10.8% | 9.32 ms | 12.06 ms | +29.3% | 0.09 s | 0.09 s |
| 1 MiB | 1,071 | 972 | -9.2% | 9.87 ms | 11.68 ms | +18.3% | 0.09 s | 0.09 s |
| 2 MiB | 702 | 675 | -3.8% | 17.04 ms | 19.59 ms | +15.0% | 0.09 s | 0.08 s |
| 4 MiB | 402 | 429 | +6.7% | 29.47 ms | 23.48 ms | -20.3% | 0.12 s | 0.10 s |
| 8 MiB | 226 | 232 | +2.7% | 48.37 ms | 47.80 ms | -1.2% | 0.19 s | 0.15 s |

Direct I/O is therefore not a safe global default: its fixed cost is visible
through 2 MiB. At 4 MiB and 8 MiB it has no measured throughput or p99 penalty,
and it reduces system CPU in both rows. Production selects Direct publication
at a sealed Container length of 4 MiB or greater and keeps smaller Containers
buffered. `ring-buffered` and `ring-direct` remain explicit benchmark and
diagnostic challengers.

Max RSS was effectively unchanged in this short benchmark because concurrent
writer images dominate process RSS and the host page cache is outside process
RSS. Avoiding duplicate cache residency is consequently an expected
large-appliance benefit, not a result claimed by these measurements. Host-level
cache pressure needs a long-running appliance workload with memory accounting.
