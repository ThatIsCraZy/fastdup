# Compressed Verified Read cache qualification — 9 September 2026

Implementation follows ADR 0046. No durable format, Similarity fingerprint,
Samba setting, storage-read concurrency or Historical Proof policy changes.
This qualifies the code locally; no new RPM or test-VM deployment is claimed.

## Correctness and integration

- Store library: 101 passed; appliance library: 53 passed; control library: 38 passed.
- Format cache round-trip/fault tests: 2 passed. Damaged compressed bytes,
  wrong length and wrong identity cannot create a verified decoded payload.
- Owned-record provenance: 1 passed, including real decompression after the
  original shared owner disappears, full coordinate matching and rejecting a
  relocated Location with the same logical Chunk identity.
- Manifest reader integration: 14 passed. Covers complete dependent reads with
  a cached Base, independent full verification still reading storage, cold
  coalescing, source/slice ownership, live index turnover and workload changes.
- Cache tests cover compressed/direct ownership accounting, live-view reuse,
  surviving purge, shrinking to zero, mixed concurrent admissions/pressure,
  workspace release, hot promotion, cold demotion, rejected promotion at a full
  budget, and invalidation racing a newer replacement.
- Library Clippy and runtime/control binary checks passed.
- UI: 43 tests passed; old telemetry remains readable without inventing zeros.
  Gauge values stay at the selected historical sample when the hit-rate window
  changes. Codec counters are explicitly since-mount values.
- Playwright: all six tabs, expanded codec costs, 5-minute selection, 1440px and
  390px viewports, no page overflow or browser errors. Screenshots under the
  artifact directory are generated fixture data, not live appliance measurements.

## Fixed-budget cache A/B

Release-mode production cache with compression enabled/disabled, identical
geometry and 512 KiB payload budget. Corpus: 48 distinct 64-KiB chunks, each
containing eight repetitions of an 8-KiB deterministic noise pattern. The full
3-MiB working set is read cyclically 32 times (1,536 requests), starting empty.
Five rounds alternate the comparison order. The byte oracle is unchanged.

The miss path constructs and verifies RAW records from the in-memory corpus;
it does **not** time an HDD, SMB, Veeam or durable publication. Input fixtures
retain no verified owners, so the compressed side cannot borrow fixture RAM
through weak decoder references. The comparison includes identity lookup and
cold-record construction costs and is not a pure cache-lookup nanobenchmark.

| Compression | Hits | Misses | Hit rate | Resident bytes | Median elapsed |
|---|---:|---:|---:|---:|---:|
| Disabled | 0 | 1,536 | 0.000% | 524,288 | 102.073 ms |
| Enabled | 1,488 | 48 | 96.875% | 473,261 | 58.683 ms |

With compression, one hot chunk is promoted using spare capacity. The remaining
47 compressed chunks represent 3,080,192 logical bytes in 407,725 charged bytes;
total resident payload/compact-owner charge is 473,261 bytes. Fixed slot metadata
is separate and identical between the two sides. Peak codec workspace reservation
is 720,896 bytes, returns to zero, and is reported separately from residency.
Promotion cannot trade another DATA-saving entry for less CPU work.

## Codec comparison and incompressible data

LZ4 default block mode versus Zstd fast level -1. Five alternating rounds; 32
iterations per input. Decode measurement includes full logical hashing and a
byte comparison. These are wall-clock durations, not sampled process CPU time.
The table shows medians and encoded-payload ratio before cache owner overhead.
The cache separately rejects representations without a net allocation saving.

| Corpus | Codec | Logical / encoded | Compression | Decode + verify |
|---|---|---:|---:|---:|
| pattern | LZ4 | 125.5479× | 0.483 ms | 0.672 ms |
| pattern | Zstd -1 | 236.5921× | 0.506 ms | 0.614 ms |
| noise | LZ4 | 0.9961× | 0.442 ms | 0.669 ms |
| noise | Zstd -1 | 0.9998× | 0.636 ms | 0.637 ms |
| iso | LZ4 | 1.0422× | 75.159 ms | 60.021 ms |
| iso | Zstd -1 | 1.0475× | 90.422 ms | 59.402 ms |

`pattern` and `noise` each measure 2 MiB per run. `iso` measures 128 MiB per run,
using 64 evenly spaced 64-KiB windows of a 2,072,444,928-byte Rocky 10.2 minimal ISO.
SHA-256 of the ordered 64 sampled windows:
`c26fdd3dd4a9154682ebe670c17e81db5e49680fd01d0298c315021edc38a4e2`.

LZ4 is the initial RAM codec: it has lower measured compression time here and
keeps decode work bounded using an existing dependency. Zstd is denser, with
small absolute additional savings on this already compressed ISO sample. This
is not evidence that LZ4 is universally better. Noise expands and is retained
in its original verified representation. Real Veeam contents may also compress
poorly; the favorable cyclic-pattern cache result must not be extrapolated to
an unknown backup workload. Shared groups may remain decoded when one sibling
is incompressible or workspace is unavailable.

## Reproduction

Host: x86-64 VM, 10 visible CPUs, Intel Core i7-1370P model. Rust release profile
with overflow checks; no new unsafe code. All temporary outputs are under
`/source/fastdup/.artifacts/compressed-read-cache/`.

Set `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`; this workspace also needs Cargo's
`--config 'env.TMPDIR.value="/source/fastdup/.artifacts/tmp"'` override.

```sh
cargo test --release -p fastdup-store --lib compressed_cache_workload_benchmark -- --ignored --nocapture
FASTDUP_CACHE_BENCH_CORPUS=/source/fastdup/.artifacts/benchmark-source/Rocky-10.2-x86_64-minimal.iso \
  cargo test --release -p fastdup-format --lib cache_codec_benchmark -- --ignored --nocapture
```

The codec corpus environment variable is optional; absent it, only the
reproducible synthetic pattern/noise comparisons run.
