# AVX-512 runtime dispatch qualification — 2026-09-06

Target: Rocky Linux 10.2 VM, Intel Xeon Silver 4215, AVX-512F/BW/DQ/VL,
AVX2 and BMI2 exposed by VMware. Release-mode tests pinned to vCPU 0,
one test thread. Nine samples per direct-kernel variant, median shown.
These are kernel microbenchmarks, not end-to-end SMB throughput guarantees.

| Kernel / input | Safe scalar | AVX2 | AVX-512 / adaptive | Unit |
| --- | ---: | ---: | ---: | --- |
| Similarity votes (512 counters) | see full fingerprint below | 44 | 31 | ns/update |
| Sparse-XOR (256 KiB, one changed byte / 4096) | see scan below | 14,418 | 11,641 | ns/chunk |
| SeqCDC, changing bytes | 40,162 | 3,863 | 3,874 adaptive | ns/cut |
| SeqCDC, constant bytes | 207,651 | 34,000 | 9,541 adaptive | ns/cut |

For changing SeqCDC input, forcing the 512-bit implementation took 5,094 ns;
it is **not** selected for that input. A bounded 64-byte equal-input probe
chooses wide comparisons for equal stretches. Segmented inputs use the same
choice without changing slice-boundary semantics. SIMD instruction availability
is checked at runtime, including OS register-state support through Rust's
feature detection. No global `target-cpu=native` or AVX-512 requirement is added.

The existing safe-scalar vs dispatched full-fingerprint benchmark measured
35.733 versus 1.955 ns/byte (18.277×). Sparse-XOR safe-scalar scan versus
runtime-dispatched scan measured 176,922.7 versus 12,228.7 ns/chunk (14.468×);
complete trials measured 182,165.8 versus 13,484.9 (13.509×). Those historical
benchmark function names and output labels still say `avx2`; on this machine
the dispatched branch is AVX-512. The direct comparisons in the table explicitly
call each ISA's kernel, so their AVX2 baseline is independent of dispatch.

Correctness: all vote extrema through 4096 additions match the scalar oracle;
AVX2/AVX-512 sparse runs and payloads agree at 31/32/33, 63/64/65,
127/128/129 and larger lengths, with multiple sparse patterns and cost caps.
SeqCDC contiguous, complete-stream and hostile segmented-split comparisons
match scalar cuts. Local i7-1370P tests exercise the AVX2 fallback without
AVX-512. BLAKE3 1.8.7 already builds and runtime-selects its AVX-512 backend;
no extra vector-width cap was found there.

Safety boundaries remain in `similarity_simd.rs` and `seqcdc.rs`: initialized
fixed vote arrays, bounded unaligned loads, equal sparse-XOR input lengths,
feature checks and bounded signed vote counts. Durable fingerprints, chunk cuts,
Sparse-XOR encodings and decoding semantics are unchanged.

Test executable SHA-256:
`0d2bb7dbf43c0099a0c932c4e997ccfa11166371d23a9d8c39919b1caa865775`.
Built with `cargo test --locked --release -p fastdup-store --lib --no-run`
using workspace-local Cargo target and temporary directories. Run that executable
on an AVX-512 host using:

```sh
taskset -c 0 "$test_binary" similarity_simd::tests:: --nocapture --test-threads=1
taskset -c 0 "$test_binary" seqcdc::tests:: --nocapture --test-threads=1
taskset -c 0 "$test_binary" avx512 --ignored --nocapture --test-threads=1
taskset -c 0 "$test_binary" scalar_and_avx2_microbenchmark --ignored --nocapture --test-threads=1
```
