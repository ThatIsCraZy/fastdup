# Codec buffer reuse qualification, 12 September 2026

Verified Read now reuses initialized compression scratch and decoded payload
allocations. The benchmark compares the existing allocating LZ4 API with the
pooled API, including full BLAKE3 verification for every new decode. Six
alternating pairs each execute 2,000 operations on a compressible 256-KiB chunk.
These are development-host CPU measurements, not a Veeam throughput test.

```sh
mkdir -p /source/fastdup/.artifacts/tmp /source/fastdup/.artifacts/buffer-pool
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
/source/fastdup/.artifacts/cargo/bin/cargo \
  --config 'env.TMPDIR.value="/source/fastdup/.artifacts/tmp"' \
  test -p fastdup-format --release --lib buffer_pool \
  -- --ignored --nocapture --test-threads=1
```

The retained harness lives in `container/cache_payload.rs`; local output is
`.artifacts/buffer-pool/benchmark-final.log`.

| 2,000 operations, median wall time | Allocating | Pooled |
| --- | ---: | ---: |
| Decompress and verify | 186.053 ms | 183.460 ms |
| Compress into an independent cache copy | 24.284 ms | 27.229 ms |

Each pooled run made one new large buffer and reused it 1,999 times. Decoding
retained 262,144 idle bytes; compression retained 263,188. These counts concern
the large scratch/payload buffers, not every allocation: immutable Arc owners
and compact compressed output still allocate. The buffer pool does not turn a
decode into a trusted hit; each expired decoded owner still requires a new
decode and identity check.

The small decode-time difference is not evidence of a general throughput gain.
Compression paid about 1.47 microseconds extra per operation in this sample.
The demonstrated trade-off is less large-buffer allocation/free churn for some
additional pool/ownership/copy work. No VM-wide RSS or allocator-trim reduction
has been measured yet. The implementation uses safe Rust; no new unsafe
interface is justified by this experiment.

## Correctness and operating bounds

- Warm reuse checks stable allocation addresses; simultaneous users own
  disjoint mutable buffers, followed by immutable verified views.
- Final response-owner release returns capacity. Pool destruction, pressure
  shrink and late returns cannot invalidate live readers or resurrect weak
  verification evidence. Explicit `into_payload` ownership transfer preserves
  the allocation without a copy.
- A corrupt compressed copy returns workspace without publishing its bytes.
  Pool bytes never stand in for logical identity or durable evidence.
- Best-fit reuse rejects buffers exceeding twice the requested length; small
  reads cannot pin a maximum-size compression buffer merely to avoid malloc.
- Actual idle capacities and fixed bookkeeping are leased from the shared
  broker, below both DATA- and Metadata-saving demand. At most 32 idle slots
  can be retained; buffers are created on demand. Process Swap or failed
  pressure sampling closes retention on the existing refresh path.
- Production cache-hit pressure tests retain a reader through eviction and
  verify that its late return cannot refill a revoked pool. Concurrent
  checkout/return/shrink tests check bounded idle bytes and zero final active
  bytes.
- Control API/history roundtrips preserve additive counters; old samples remain
  readable. UI tests keep allocation reuse out of disk-cache hit-rate rows.

Validation: format/store unit tests, owned RAW/read-view/record-transplant
integration tests, the control telemetry tests, UI telemetry tests, TypeScript,
UI production build, and production Clippy. No storage format or verification
semantics changed. Existing allocator reclamation remains enabled for other
unused heap pages.
