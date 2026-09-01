# CPU cache, SIMD, and lock-free parallelism audit

> Status update, 2026-09-02: Sparse-XOR has since become durable codec 4 in
> the opt-in dependent-codec path; see [ADR 0088](../adr/0088-persist-sparse-xor-as-a-depth-one-dependent-codec.md).

This review follows the complete frontend DATA path:

`FUSE -> POSIX mutation/read -> Ingest Lane -> SeqCDC -> Chunk ID -> Exact /
Similarity -> encode -> io_uring publication -> Commit -> Exact resolve ->
Container Record decode -> read cache -> FUSE reply`

It also checks rebuild, scrub, and GC. The production DATA Tier is assumed to
be HDD-backed. Therefore “more parallel” is not automatically better: parallel
random DATA reads can convert a sequential restore into seeks. The safe rule is
to parallelize independent CPU work and Metadata-tier work while keeping one
HDD stream physically ordered.

## Executive result

The code already has strong cache-local foundations: 64-byte blocked Bloom
filters, a 256-byte pointer-free per-worker Location cache, struct-of-arrays
Similarity residents, cache-line-separated cache shards and Exact pages,
THP-advised long-lived dense arenas, AVX2/BMI2 SeqCDC with a scalar oracle,
bounded Rayon stages, parallel Container verification, and one shared io_uring
publisher.

The P0/P1 implementation slice selected from this audit is complete:

1. verified payload ownership removes the extra full-Chunk copies on cache hit
   and admission;
2. Exact lookup appends into reusable caller scratch, reuses duplicate keys,
   and resolves keys in page-local Chunk-ID order;
3. Similarity uses a 2-KiB byte-hash table and runtime-dispatched AVX2 vote
   accumulation, with the scalar profile as its durable oracle; and
4. verified-read, descriptor, and historical-proof cache telemetry is
   shard-local, while the lock-free persistent reduction path uses 64
   cache-line-separated stripes.

Immutable Exact-Run mmap and ordered-I/O/parallel-decode remain P2 experiments;
they are not required to close this P0/P1 slice.

The new Verified Read Plan already implements one immediate result of this
audit: it uses one flat contiguous vector plus one sort instead of a pointer-rich
tree of Record groups, reuses one verified Container descriptor across adjacent
Records, reads/decode-shares an Encoding Record once, and retains the
allocation-free scalar path for one DATA extent.

## Findings by stage

| Stage | Current cache/parallel shape | Finding | Priority |
| --- | --- | --- | --- |
| FUSE/POSIX dispatch | `Inode` and checkpoint counters are 64-byte aligned; state is inode-local | Good. Per-inode locks express real POSIX ordering. Replacing them with atomics would complicate correctness without removing the logical serialization. | Keep |
| Dirty extents | ordered maps and owned `MutationPayload` slices preserve sparse edits | Map pointer chasing is acceptable for mutation bookkeeping, not for byte scanning. Do not flatten whole files merely for locality. | Keep |
| Ingest/Checkpoint | per-inode work is dispatched through bounded worker pools; large buffers move by ownership | Good. Parallelism is already at independent inode/region granularity, with deterministic ordered publication. | Keep |
| SeqCDC | scalar oracle plus AVX2/BMI2 32-byte classifier | Good SIMD seam. Under ADR 0078 only x86-64 is supported; no non-x86 implementation is planned. | Keep |
| Chunk identity | BLAKE3 crate | Already runtime-dispatched SIMD. Custom intrinsics would duplicate a stronger implementation. | Reject |
| Similarity fingerprint | scalar rolling shingles plus a 512-element `i32` vote array | A compile-time byte-hash table removes repeated mixing; AVX2 updates eight vote lanes per instruction after runtime detection. Scalar remains the byte-exact oracle. | Done |
| Similarity resident index | metadata, superfeatures, 64-byte Sketches, and activity are separate dense vectors | Good struct-of-arrays design. Ranking touches the Sketch line without dragging Chunk IDs and mutable metadata into L1. | Keep |
| Exact membership | one aligned 64-byte blocked Bloom block per probe, dense filters in THP-advised arenas | Good. Seven bit probes share one line; no SIMD is warranted. | Keep |
| Exact lookup | immutable page readers, direct-mapped 64-byte-separated page slots | Activated sets and physical Runs now append into one caller-owned 64-candidate scratch vector. A Read Plan resolves sorted keys and reuses adjacent duplicate results. | Done |
| Similarity lookup | immutable mapped Runs and bounded page decode | Good lock-free read shape after activation. Shared telemetry atomics remain a minor contention source. | P2 |
| Prefix selection | at most four Bases, sequential verified reads and codec trials | Do not issue these random HDD Base reads in parallel. Depth-one bounded latency is preferable to four simultaneous seeks. CPU trials may run independently only when Bases are already cache-resident. | Keep |
| Independent encode | Zstd/LZ4 contexts are worker-owned; regions use bounded Rayon | Good. Codec libraries already use optimized native loops. No shared codec lock exists. | Keep |
| Sparse XOR | scalar oracle plus AVX2 changed-run scan; durable codec 4 in the opt-in dependent path | The durable decision is complete. Preserve scalar/AVX2 equivalence and measure further changes against the production path. | Done |
| Container verification | structural scan plus Rayon record verification for large work | Good. CRC32C, Zstd, BLAKE3, and memory copies already use optimized implementations. | Keep |
| DATA publication | one bounded CQE-driven io_uring worker and separate verifier pool | Good ownership and concurrency boundary. More rings or a ring per worker would add queue contention and ordering complexity. | Keep |
| Verified Manifest read | bounded Read Plan, Exact resolution, ordered Record reads, decode, final assembly | `VerifiedChunkPayload` adopts the decoder `Vec`; cache hits clone only its owner and final assembly performs the one required reply copy. | Done |
| Descriptor cache | 256 aligned shards and immutable descriptor values | Shared cache is sound, but repeated Records in one read used to retake its shard lock. The Read Plan now carries one verified descriptor plan-locally. | Done |
| Read/descriptor/proof/reduction telemetry | observation-only counters formerly shared object-wide cache lines | Read, descriptor, and historical-proof caches update ordinary counters under their existing shard lock. Persistent reduction uses 64 aligned Atomic stripes selected by Chunk ID; cold status aggregation preserves lock-free planning. | Done |
| Memory governor | one 250-ms sampler, atomically published snapshot | Correctly cold. Its atomics are not worth padding individually; caches only refresh through the bounded interval. | Keep |
| GC/Scrub | ordered Container reads with bounded Rayon verification and worker-local results | Good for HDD. Parallel full-Container reads should remain off; verification/decode after each sequential read is the correct parallel boundary. | Keep |

## Concrete changes

### Done — P0: owned verified payloads end to end

The internal cache/read seam now uses a small verified payload
capability backed by `Arc<Vec<u8>>` (or an equivalent owner that can adopt a
`Vec` without copying). A cache miss can move the decoder's `Vec` into the Arc;
a hit clones only the Arc; Manifest assembly performs the one unavoidable copy
into the final FUSE reply. Admission continues to verify Chunk ID and length
before publishing the owner.

This removes:

- one complete Chunk copy on every cache hit;
- one complete Chunk copy on every admitted miss; and
- temporary per-Chunk `Vec` ownership churn in multi-extent reads.

The API must remain internal: an Arc is verification evidence only while its
constructor stays behind the verified Container decoder. The cache's byte
accounting charges the adopted `Vec` capacity; logical length and identity
checks continue to use its initialized payload length.

### Done — P0/P1: reusable and batched Exact lookup

The internal `lookup_transitions_into` seam accepts caller-owned scratch
and appends at most 64 candidates. Refactor the physical Run reader to append
directly instead of returning a second temporary `Vec`. The scalar public seam
may wrap this for compatibility; one Read Plan reuses its scratch across all
Chunks.

The Read Plan sorts demand keys by `(ChunkId, logical length, ordinal)`, which
keeps immutable Exact pages monotonic within a Run, and scatters bounded active
candidates back to logical ordinals. Adjacent duplicate keys reuse one result.

Benchmark an immutable Exact-Run mmap reader as a second implementation, using
the same descriptor audit and immutable-file lease already proven for
Similarity. A successful mapping would let parallel readers share clean kernel
pages without a userspace page-slot Mutex or payload copy. Keep positional reads
as the independent scrub and adapter fallback.

### Done — P1: Similarity fingerprint SIMD

Two changes were implemented against the scalar v1 oracle:

1. Make `byte_hash` a compile-time 256-entry `[u64; 256]` table. The current
   rolling loop mixes the same possible byte values repeatedly; a 2-KiB table
   fits comfortably in L1/L2 and removes repeated integer mixing.
2. Update `sketch_votes` in 8 or 16 `i32` lanes at a time. For each 64-bit word,
   expand its bit mask to vector lanes and add `+1` or `-1`. AVX2 is the useful
   baseline on the current x86 production target; AVX-512 must remain optional,
   not a requirement.

Required evidence is byte-identical fingerprints for every existing golden and
random differential case, plus cycles/byte and L1-miss measurements on Rocky,
structured, incompressible, and near-duplicate Chunks. The rolling dependency
itself is not a good SIMD target; optimize its repeated byte hash and the
independent vote lanes instead.

### Done — P1: remove telemetry false sharing without new locks

`VerifiedReadCache`, `ContainerDescriptorCache`, and `HistoricalProofCache`
store hit/miss/admission counters in shard state already protected by the
existing shard lock. `status()` sums them on the cold telemetry path. No lock
was added and the hot lookup no longer performs a shared atomic RMW for
observation-only counters.

Persistent Prefix selection retains lock-free telemetry through 64
cache-line-separated Atomic counter stripes selected by target Chunk ID.
`status()` sums them. This avoids request objects and thread-local destructors;
correctness counters that participate in budgets remain atomic.

### P2: ordered I/O plus parallel CPU, not parallel HDD seeks

Extend the storage seam only when HDD evidence justifies it:

1. Read Plan emits Records in `(Container, offset)` order.
2. One bounded producer issues ascending reads with a small queue depth.
3. Completed encoded buffers move to worker-local decode jobs.
4. Jobs write to preassigned result ordinals; the request thread joins in
   logical order.

This uses ownership and ordinal slots rather than a shared result map. It adds
no global lock. Queue depth should start at one for a single HDD and increase
only when device telemetry shows a benefit. Across independent spindles or
already-resident file-backed pages, CPU work may scale to the existing memory
and worker budget. Within one physical HDD, never parallelize Prefix Base reads
or random Exact-selected Records merely because futures are available.

## Explicit non-actions

- Do not add manual SIMD around BLAKE3, CRC32C, `copy_from_slice`, Zstd, or LZ4.
- Do not pad every struct. Pad independently written hot state, not immutable
  payload or fields always consumed together.
- Do not replace per-inode POSIX locks with lock-free structures; those locks
  encode required mutation order.
- Do not parallelize one HDD restore by Chunk. Ordering and shared Record reads
  have higher value than request concurrency.
- Do not add a global work-stealing or read-plan lock. Use immutable inputs,
  worker-local outputs, and deterministic ordinal scatter.

## Measurement gates

The focused release-mode Similarity benchmark on the implementation host
measured the complete 256-KiB fingerprint path with the median of seven
alternating samples of 16 rounds: 25.557 ns/byte for the scalar oracle and
2.318 ns/byte for AVX2, an 11.025x speedup. The differential sweep remained
byte-identical through 256 KiB. This is a CPU microbenchmark, not a substitute
for the next SingleStream/HDD acceptance run.

Two identical SingleStream SMB acceptance runs then uploaded the Rocky 10.2
x86-64 minimal ISO three times with the same Samba configuration and separate
XFS Metadata/DATA devices. They measured:

- 672.619 and 717.511 MiB/s aggregate write throughput;
- 4,193.640 and 4,000.695 ms completed-file p99/max latency;
- 3.104x and 3.103x repository data reduction; and
- zero bytes of fastdup process Swap in both runs.

The immediately preceding comparable MemoryBudgetGovernor pair measured
493.923 and 571.098 MiB/s with 7,460.751 and 6,005.707 ms p99/max. Thus even
the weaker new throughput run is 17.8% above the stronger prior run, while the
weaker new latency is 30.2% below the stronger prior latency. Host variance
prevents assigning that gain to one change, but the gate shows no SingleStream
write regression. Advanced Reduction was `off`, so these runs exercised the
Historical Proof Cache telemetry change but not Similarity AVX2; the latter is
covered by the differential and focused CPU evidence above. Reports are kept
at `.artifacts/benchmarks/smb-p0-p1-cache-simd-20260827.json` and
`.artifacts/benchmarks/smb-p0-p1-cache-simd-repeat-20260827.json`.

For each P0/P1 change, record at least:

- cycles and instructions per logical MiB;
- L1 data-cache and last-level-cache misses;
- allocator calls/bytes per FUSE read and per Chunk ingest;
- shared atomic RMW counts or cache-to-cache transfers where the host exposes
  them;
- physical DATA reads, average request size, nonsequential request count, and
  queue depth; and
- SMB SingleStream throughput plus completed-write p99/max latency.

Cache/CPU microbenchmarks must not replace the end-to-end HDD result. A change
is accepted only if it preserves physical restore order, Process Swap zero, the
MemoryBudgetGovernor reserve, and the existing durability/fault matrices.
