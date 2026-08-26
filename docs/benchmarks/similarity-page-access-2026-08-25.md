# Similarity page access: `read_exact_at` versus mmap

Date: 2026-08-25

The benchmark compares only the immutable Similarity Run page-access seam. Both
backends select the same deterministic random 4-KiB page sequence and call the
same checksummed format-v2 entry/bucket decoder. The `read_exact_at` path copies
into one reusable 4-KiB buffer; the mmap path passes a borrowed slice directly
to the decoder.

Command:

```bash
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo run --release -p fastdup-similarity-bench \
  --bin fastdup-similarity-page-bench -- --generate
```

Fixture and workload:

- 100,000 deterministic entries;
- 26,206,208-byte immutable Similarity Run;
- 4,000 entry pages and 2,396 bucket pages;
- 1,000,000 deterministic random page reads per round;
- seven rounds in alternating backend order;
- warm Linux page cache; median wall time reported.

Results on the current development host:

| Backend | Median | ns/query | Relative |
|---|---:|---:|---:|
| `read_exact_at` + decode | 1.478 s | 1,478.1 | 1.00× |
| mmap slice + decode | 1.046 s | 1,046.1 | 1.413× |

The measured 29.2% latency reduction proves that avoiding the syscall and
4-KiB copy is material at this seam. It does not by itself prove the same
end-to-end gain behind the process page cache.

The benchmark keeps a read-only file descriptor alive for the mapping, checks
that the source is a regular file with the descriptor-authenticated length,
and rechecks length and modification time after all samples. Its only `unsafe`
operation is the `memmap2::MmapOptions::map` call in the benchmark crate.

Production activation now uses the same read-only mapping strategy behind an
explicit generation lease (ADR 0061). All filesystem adapters for one canonical
root share the lease registry and reject write, truncate, replacement, and
reclamation until the last mapped reader is dropped. Recovery fully audits the
mapped bytes before exposing them. Generic adapters, publication, and offline
scrub retain `read_exact_at`. External mutation of the appliance-owned storage
directory remains unsupported because it can bypass the in-process lease and
violate `memmap2`'s safety contract or cause Linux `SIGBUS`.

References:

- [`memmap2::MmapOptions::map` safety](https://docs.rs/memmap2/latest/memmap2/struct.MmapOptions.html#method.map)
- [Linux `mmap(2)`](https://man7.org/linux/man-pages/man2/mmap.2.html)
