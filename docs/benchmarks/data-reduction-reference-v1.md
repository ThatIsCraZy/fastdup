# Data-reduction reference pipeline v1

Date: 2026-08-15

This report records functional and performance evidence for the experimental
in-memory reduction engine. It is not an appliance throughput claim. The
engine does not yet serialize these records into a durable container format,
stream a 100-TB object, or expose a POSIX/FUSE namespace. All ratios below are
encoded payload only: container framing, indexes, alignment, dictionaries,
manifests, replicas, and filesystem allocation are excluded unless stated.

## Implemented policy stages

The versioned policy exposes RAW, FastCDC, Exact, Zstd, Compression Grouping,
Similarity, Depth-1 sparse-XOR Delta, and bounded Reorder independently.
Constant-byte FILL detection is part of CDC preprocessing. An immutable
BLAKE3-identified Zstd dictionary is an optional dependency of compression.

The important v1 bounds are:

- FastCDC `16 KiB / 64 KiB / 256 KiB`, v2020 Level 1, seed 0;
- Compression Regions at most 512 KiB;
- Placement Windows 64 MiB;
- 64 representatives per `(profile, slot, length, superfeature)` bucket;
- at most 256 representatives examined, 16 candidates returned, and four
  Delta trials per target;
- Delta dependency depth exactly one;
- Zstd/Delta admission requires at least 4 KiB and the versioned percentage
  saving (3% for Zstd, 5% for Delta).

Similarity representatives and Bloom positives never authorize content
identity. Exact reuse and every restored chunk are verified with BLAKE3-256.

## Rocky ISO family: complete policy

The input is the ten-file, 20,724,449,280-byte family from
[corpus.md](corpus.md): each file is the pinned Rocky Linux 10.2 minimal ISO
with exactly eight deterministic one-byte XOR edits. One engine ingested all
files sequentially so later objects could use earlier Exact and Similarity
state. It then freshly read every source and required byte-exact restoration.

Command profile: `all`, 10 workers, 128-MiB nominal worker scheduling budget.

| metric | result |
| --- | ---: |
| logical bytes | 20,724,449,280 |
| encoded payload bytes | 1,941,262,007 |
| payload/logical | 9.367% |
| logical/payload reduction | 10.676x |
| Exact Hit bytes | 18,723,442,588 (90.345%) |
| logical chunks / Exact Hits | 257,190 / 232,878 |
| RAW chunks | 22,095 |
| Zstd regions / dictionary regions | 394 / 0 |
| accepted Delta chunks | 80 |
| Similarity candidates / Delta trials | 80 / 80 |
| Delta logical / payload bytes | 7,565,722 / 3,600 |
| maximum Delta depth | 1 |
| FILL extents / logical bytes | 60 / 15,165,910 |
| reordered regions / placement windows | 4,582 / 101 |
| ingest / verified restore | 46.042 s / 22.168 s |
| logical ingest / restore rate | 450.1 MB/s / 934.9 MB/s |
| maximum RSS | 6,077,156 KiB |
| swap | 0 |

`perf stat` reported 140.791 CPU-seconds over 68.283 elapsed seconds, or 2.062
CPUs averaged over ingest, serial restore, source rereads, and comparison. The
maximum scheduled worker count was 10; it must not be confused with average
utilization.

The final counters are identical to the pre-optimization reference run.
Parallel BLAKE3 chunk hashing reduced complete-policy ingest from 50.521 s to
46.042 s (8.87%) without changing any reduction decision.

## Worker scaling

The same two ISO variants (4,144,889,856 logical bytes) were measured with the
Delta preset. Both runs produced exactly the same byte, Chunk, Exact,
Similarity, Delta, FILL, and placement counters.

| configured workers | ingest | verified restore | wall | maximum RSS |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 56.288 s | 4.179 s | 60.53 s | 6,044,404 KiB |
| 10 | 14.664 s | 4.221 s | 18.95 s | 6,047,512 KiB |

Ingest speedup is 3.84x. Restore remains deliberately serial in this reference
harness. CDC boundary discovery, FILL scanning, ordered Exact planning, index
publication, and phase barriers remain serial fractions. BLAKE3 chunk hashing,
region encoding, Similarity fingerprinting, Delta trials, and Reorder key work
use deterministic worker-local batches. Chunk hashes use contiguous shards;
worker results merge by immutable logical ordinal with paired production
assertions against omission or duplicate completion.

The Hyper-V host exposes one NUMA node, ten logical CPUs, and 64-byte cache
lines, but no usable hardware PMU events. Cache misses, branch misses, and
false sharing therefore were not measured and are not inferred from task-clock
data. Bloom blocks are exactly one aligned 64-byte line, Location Hint cache
sets are aligned 256-byte pointer-free arrays, and mutable worker statistics
are separately 64-byte aligned.

## Structured XML/JSON matrix

All six files from [corpus.md](corpus.md) were run through every named preset
with 10 configured workers. Every row passed fresh-read, byte-exact restore.
Times are intentionally omitted here because the 3.77-MB corpus completes in
milliseconds; the decision counters are the useful evidence.

| preset | payload bytes | Exact bytes | RAW | Zstd | Delta | candidates/trials | reordered |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 3,766,257 | 0 | 63 | 0 | 0 | 0 / 0 | 0 |
| cdc | 3,766,257 | 0 | 44 | 0 | 0 | 0 / 0 | 0 |
| exact | 3,766,257 | 0 | 44 | 0 | 0 | 0 / 0 | 0 |
| compression | 701,630 | 0 | 2 | 42 | 0 | 0 / 0 | 0 |
| grouping | 683,023 | 0 | 0 | 12 | 0 | 0 / 0 | 0 |
| similarity | 3,766,257 | 0 | 44 | 0 | 0 | 14 / 0 | 0 |
| delta | 2,515,205 | 0 | 30 | 0 | 14 | 14 / 14 | 0 |
| reorder | 683,023 | 0 | 0 | 12 | 0 | 0 / 0 | 12 |
| all | 609,992 | 0 | 0 | 10 | 2 | 14 / 2 | 12 |

The complete policy retains 16.196% of logical bytes (6.174x payload
reduction). Exact contributes zero because these generated versions change
every CDC chunk; that is an observed workload property, not a disabled stage.
In `all`, multi-chunk compression regions remain independent while eligible
single-chunk regions may become Delta.

## Dictionary experiment

JSON v1/v2 trained a 65,536-byte dictionary and JSON v3 was the unseen target.
XML used the equivalent separate family. Training files are deterministically
split into ordered 16-KiB samples, and training is outside timed ingest.

| target | plain grouped payload | dictionary payload | dictionary object | total first-use cost |
| --- | ---: | ---: | ---: | ---: |
| JSON v3 | 112,657 | 98,104 | 65,536 | 163,640 |
| XML v3 | 116,301 | 104,659 | 65,536 | 170,195 |

Both targets selected the dictionary for both regions and restored byte
exactly. Payload alone improved by 14,553 bytes for JSON and 11,642 for XML,
but a dictionary retained for only one target loses after its own object cost.
Under the same per-file saving it amortizes after approximately five JSON or
six XML targets. This is evidence for family-level dictionaries with explicit
retention accounting, not for per-file training.

Dictionary IDs were
`d4e2372cde9bcd24ee8070c0e35fa67b511006e71d06d4a6bbfa4761c251af31`
for JSON and
`390ccad0a2b3630db77195a9009ddb21e84994de1dc187abc54370ef2e13b003`
for XML.

## Integrity and test gates

Production `assert!`/`expect` checks are used only for writer-internal states
that must be impossible: complete partitions, bounded ranges, one result per
ordinal, and deterministic merge ownership. There are no `debug_assert!`
checks in the reduction integrity path. Untrusted/stored identities, lengths,
record tables, codec output, dictionary IDs, Delta runs, and reconstructed
Chunk IDs return defined `Corruption`/codec errors.

Current tests cover:

- deterministic worker decisions after Base ingest and accepted Delta work for
  1, 2, 4, and 8 workers;
- eight deterministic differential seeds over every valid feature prefix;
- FastCDC resynchronization after insertion and Exact suffix reuse;
- Zstd threshold/fallback, grouped region bounds, immutable dictionary
  identity and wrong-dictionary rejection;
- FILL threshold/boundary cases;
- Similarity golden fingerprints, local edits, bounded candidate ordering, and
  a 10,000-insert hot-bucket test with hard storage/query limits;
- 256 sparse-XOR writer/reader sweeps plus malformed, overlapping,
  out-of-bounds, wrong-Base, and wrong-target cases;
- Depth-1-only logical Base references without physical codec dependency;
- bounded Reorder over two placement windows and byte-exact restore.

The general immutable-container fail-before/fail-after crash matrix remains
separate because the reduction records do not yet have a durable on-disk
format.

## Non-negotiable gaps before durability

- The whole-file `&[u8]` ingest and `Vec<u8>` restore interfaces retain several
  GiB and cannot represent an appliance-scale 100-TB stream.
- `--inflight-mib` is a nominal worker scheduling bound, not a proven peak-RSS
  bound; completed archive bytes, indexes, source buffers, Zstd contexts, and
  self-check decodes are outside it.
- The current acceptance report is payload-only. Durable Delta selection still
  needs exact record/index/alignment costs and the versioned Read Distance,
  Base Load, and Fanout cost model required by ADR 0018. No arbitrary weights
  are introduced without restore and fault-injection evidence.
- The in-memory Exact and Similarity indexes use ordered maps for deterministic
  reference behavior. The complete production indexes remain NVMe-resident;
  the 64-byte Bloom and worker Hint Cache are acceleration only.
- A durable reader must pre-validate all counts and lengths, use fallible
  bounded allocations, and stream restore. It must never reserve storage from
  an untrusted object-length field.
- POSIX/FUSE, the ten-second commit scheduler, manifests/WAL, GC, rebuild, and
  device-loss protection are not implemented by this reduction slice.

These are stage gates, not hidden benchmark exclusions. The next durable step
is a versioned multi-record container/recipe format whose writer, recovery
reader, and offline scrub pair the same invariants.
