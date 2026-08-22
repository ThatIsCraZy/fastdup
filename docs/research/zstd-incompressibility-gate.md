# Versioned incompressibility gate before Zstd

Status: implemented but not production-promoted by ADR 0052. This note retains
the evidence and benchmark promotion gates.

## Decision summary

The first production candidate should be an **OpenZFS-style bounded trial**,
not a byte-entropy-only classifier:

1. Regions below 128 KiB bypass the gate and use the existing Zstd level-3
   trial.
2. Eligible regions first run bounded LZ4 against the exact largest output
   that could still satisfy fastdup's 3%-and-4-KiB acceptance rule.
3. If LZ4 fits, run the existing Zstd level-3 encoder.
4. If LZ4 does not fit, run a bounded Zstd level-1 rescue trial. If it fits,
   run Zstd level 3; otherwise emit RAW without running Zstd level 3.
5. Never persist either predictor output. The durable choices remain RAW and
   the existing Zstd-v1 representation.

Call this runtime policy `incompressibility-gate-v1`. The policy version and
its counters belong in benchmark/observability output. It need not change the
container format because the gate only decides whether the existing writer
attempts an existing codec.

This is the safest first candidate because OpenZFS already uses this exact
shape for sufficiently large Zstd writes. Its source explains that LZ4 alone
lost as much as 8.5% of compression savings on some highly compressible data,
while a Zstd-1 rescue recovered that blind spot. OpenZFS currently applies the
gate from a 128-KiB input threshold and exposes separate pass/reject counters
for both stages. See [the OpenZFS implementation][openzfs-gate].

## Why an entropy-only gate is not the first choice

Byte entropy describes symbol frequency, not LZ77 repetition. A buffer made
from repeated blocks of random-looking bytes can have nearly maximal byte
entropy and still compress well. The FAST '13 study states this limitation
explicitly and therefore combines entropy with coreset and order-sensitive
pair statistics. Its online classifier samples at most 2 KiB, spread across
the write, and reported up to 35% CPU savings while keeping capacity savings
within 2% on its workloads. Those are useful design ideas, but its published
thresholds target zlib and a substantially different acceptance ratio; they
must not be copied into fastdup unchanged. See [the FAST '13 paper][fast13].

Linux Btrfs is further evidence that sampling can be cheap in a filesystem
write path. Its current heuristic samples 16 bytes every 256 bytes, keeps
worker-reusable histogram storage, and checks repeated patterns, byte/core-set
sizes, and fixed-point Shannon entropy before compression. It also documents
that pair analysis would be more accurate but too costly for this path. See
[the Btrfs compression source][btrfs-heuristic]. This is a worthwhile
challenger after the bounded-trial baseline, not a reason to adopt Btrfs's
thresholds for fastdup.

## Exact fastdup gate

Let:

- `R` be the complete RAW alternative cost, including its known record and
  index metadata;
- `Mz` be the known non-payload cost of the Zstd alternative;
- `C` be the compressed Zstd payload length.

The current v1 rule accepts Zstd only if it saves at least 4,096 bytes and at
least 3% of `R`. Therefore the largest useful Zstd payload is:

```text
required_saving = max(4096, ceil(3 * R / 100))
useful_payload_cap = R - required_saving - Mz
```

All arithmetic must be checked integer arithmetic. If either subtraction is
not representable, select RAW. An output exactly equal to the cap is useful
because the existing percentage rule is inclusive.

The same helper must drive:

- the LZ4 destination capacity;
- the Zstd-1 rescue destination capacity;
- the final Zstd-3 acceptance decision.

That prevents the predictor and durable writer from drifting. The current
rule lives in `accept_zstd_v1`; the independent format encoder has a second
copy in `zstd_record_wins`. Before enabling the gate, these must share one
cost-policy implementation or be covered by differential tests.

The official LZ4 API guarantees that compression with a restricted
destination stops immediately and returns zero when the input cannot fit.
That is the property wanted here; merely compressing to a full-size temporary
buffer and checking its length afterwards is not a gate. See the
[LZ4 API documentation][lz4-api]. The Rust Zstd bulk API likewise accepts a
caller-owned destination and reports an insufficient destination rather than
requiring a `compressBound`-sized allocation. See the
[zstd crate bulk API][zstd-rust-bulk] and the underlying
[Zstd API][zstd-api].

## Hot-path layout

The gate belongs in the adaptive Container writer used by checkpoint and
write-through ingest, immediately before its Zstd-3 call. Each reduction
worker should own:

- one bounded LZ4 invocation with no per-region heap allocation;
- one reusable 512-KiB output buffer, exposed only up to the useful cap;
- the existing reusable Zstd context, reconfigured between level 1 and 3;
- cache-line-separated local counters.

There must be no global predictor lock and no per-region heap allocation. The
LZ4 reference API provides external-state variants; `lz4_flex` also exposes a
reusable `CompressTable` and caller-provided output slices. The latter is a
reasonable Rust-only benchmark candidate, but its bounded-output failure path
must be measured against the reference LZ4 implementation before selection;
the reference API explicitly promises immediate termination. See
[lz4_flex's reusable-table API][lz4-flex].

The Container writer concatenates one bounded region once, then gives that
same contiguous slice to LZ4, Zstd-1, and Zstd-3. Gate rejection does not add
a second input copy. The reference-only reduction engine must not become an
independent source of durable writer policy.

Dictionary, prefix, and delta candidates bypass gate v1. Plain LZ4 can reject
bytes that only compress with a dictionary or base, so using it to veto those
codecs would be an invalid inference. A later dictionary-aware policy needs
its own trace evidence and policy version.

## Result semantics and invariants

The gate returns only one of:

```text
TryTargetZstd
StoreRaw(reason)
```

It never returns “verified compressible”, never authorizes durable bytes, and
never changes chunk identity. Final Zstd output is still decoded and hashed by
the existing writer self-check. Gate failure is an optimization result, not
corruption and not an assertion.

Production assertions should cover only internal impossibilities:

- predictor destination length equals the calculated useful cap;
- worker-local state is never concurrently borrowed;
- a `TryTargetZstd` result cannot bypass the final cost comparison;
- observed compressed output never exceeds its destination;
- per-reason counters sum to the number of eligible regions.

## Telemetry and false-negative audit

At minimum record:

- eligible and size-bypassed regions/bytes;
- LZ4 pass/reject counts and CPU nanoseconds;
- Zstd-1 rescue pass/reject counts and CPU nanoseconds;
- Zstd-3 trials avoided, attempted, accepted, and rejected;
- logical bytes skipped and RAW bytes emitted after a gate reject;
- predictor scratch high-water per worker.

A deterministic sampled audit should still run Zstd 3 for, for example, one
out of every 1,024 gate rejections, selected from immutable chunk IDs rather
than scheduling order. It records the counterfactual encoded length but does
not change the chosen RAW representation. This continuously measures missed
savings without making output dependent on sampling or thread order.

## Benchmark gate

Compare these policies under the same region trace and worker count:

- `off`: current full Zstd-3 trial;
- `bounded-lz4`: LZ4 then Zstd 3, no rescue;
- `bounded-lz4-zstd1`: the recommended v1 candidate;
- `sampled-heuristic`: a later 2-KiB stratified classifier challenger;
- `sampled-plus-bounded`: a later three-stage challenger.

The corpus must include the pinned Rocky ISO, representative VM backup data,
XML/JSON, already-compressed/encrypted bytes, uniform random bytes, mixed
compressible/incompressible regions, and repeated random blocks (the entropy
trap). Measure CPU cycles and LLC misses per logical byte, p95/p99 region
latency, worker scratch RSS, full Zstd trials avoided, and physical bytes.

Recommended promotion requirements for v1:

- at least 10% lower compression-stage CPU on the Rocky SingleStream trace;
- no more than 0.25 percentage points loss in total physical reduction there;
- at least 99% of baseline compression savings retained for XML/JSON;
- zero swap and no increase in the existing per-worker memory bound;
- deterministic decisions and identical restore bytes at 1, 2, and maximum
  worker counts;
- sampled-audit missed savings below 0.25% of audited logical bytes.

The latest local Rocky reduction measurement saved only about 3.37% of input
bytes while the CPU profile attributed roughly 19.7% of sampled CPU to Zstd.
That makes this gate promising, but it is not evidence for hard-coding a
classifier threshold. The trace replay above remains the selection gate.

## Initial implementation validation

The checked-in `incompressibility_gate_matrix` example streams at most 32 MiB
of input per Container, verifies every emitted Container byte-exactly, and
compares the public policies without retaining output images. On 2026-08-22,
an eight-worker release-build pass over the pinned 2,072,444,928-byte Rocky
10.2 minimal ISO produced:

| policy | elapsed | logical throughput | target Zstd trials | Container bytes |
| --- | ---: | ---: | ---: | ---: |
| `v1` | 6.376 s | 325.0 MB/s | 434 | 2,012,680,192 |
| `lz4-only` | 5.858 s | 353.8 MB/s | 407 | 2,014,371,840 |
| `off` | 6.182 s | 335.3 MB/s | 3,953 | 2,012,602,368 |

The OpenZFS-shaped `v1` avoided 3,519 target trials but was 3.1% slower than
`off`: its 3,546 mostly unsuccessful Zstd-1 rescue trials cost more than the
bounded Zstd-3 trials they replaced. `lz4-only` was 5.5% faster than `off`, but
added 1,769,472 Container bytes (0.088%) and deliberately fails the repeated
random-block entropy trap covered by the tests. Production therefore remains
`off`. This single format-level pass is not the required optimized
SingleStream promotion run and does not establish p95/p99 latency, hardware
counters, structured-data savings, or the sampled false-negative audit.

## Sources

- [OpenZFS Zstd early-abort implementation][openzfs-gate]
- [FAST '13: *To Zip or Not to Zip*][fast13]
- [Linux Btrfs compression heuristic][btrfs-heuristic]
- [Official LZ4 API][lz4-api]
- [Official Zstd API][zstd-api]
- [Rust `zstd::bulk::Compressor` API][zstd-rust-bulk]
- [`lz4_flex` reusable compression table][lz4-flex]

[openzfs-gate]: https://github.com/openzfs/zfs/blob/master/module/zstd/zfs_zstd.c#L3057-L3149
[fast13]: https://www.usenix.org/system/files/conference/fast13/fast13-final38.pdf
[btrfs-heuristic]: https://github.com/torvalds/linux/blob/master/fs/btrfs/compression.c
[lz4-api]: https://github.com/lz4/lz4/blob/dev/doc/lz4_manual.html
[zstd-api]: https://github.com/facebook/zstd/blob/dev/doc/zstd_manual.html
[zstd-rust-bulk]: https://docs.rs/zstd/0.13.3/zstd/bulk/struct.Compressor.html
[lz4-flex]: https://docs.rs/lz4_flex/0.13.1/lz4_flex/block/fn.compress_into_with_table.html
