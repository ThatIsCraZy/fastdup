---
status: accepted
---

# Overlap reduction stages under one memory and CPU budget

fastdup separates ordered per-inode FastCDC, Container preparation, DATA
publication, Exact-Index maintenance, and commit metadata into bounded stages.
A complete Container payload becomes immutable detached work. This releases its
Ingest Lane before compression or storage I/O, so the same inode may continue
chunking while its preceding Container is persisted. Container completion for
one inode remains ordered. `Sync`, `Release`, and a Frozen Commit Cut wait for
both accepted Ingest jobs and detached Container work through the sampled inode
mutation sequence before they consume reduction evidence.

The process retains at most two detached 32-MiB Container payloads. The existing
384-MiB write-through budget includes these payloads, the 16-MiB admission
queue, every registered lane, the overflow lane, and each incomplete FastCDC
suffix. The extra overlap therefore reduces the number of registered hot lanes
from ten to eight plus one overflow lane. A full detached-work budget blocks a
reduction worker. Frontend admission closes only after that blockage also fills
the separate 16-MiB queue. Queue and lane byte counts are conservative even
when they share the same immutable Mutation Payload backing with POSIX Dirty
DATA.

CPU-heavy reduction and maintenance verification use one permanent Rayon
work-stealing pool sized from the effective CPU quota. It owns worker-local
codec state. Stage-specific byte bounds remain authoritative: write-through,
io_uring publication, maintenance read-ahead, and caches each charge their
documented input before submitting CPU work. No fixed eight-worker or
64-worker process cap may leave effective CPUs unused; an individual job may
still use fewer workers when it has fewer independent regions or its stage's
memory bound requires it. CPU-only stages use userspace queues. io_uring remains
confined to kernel I/O boundaries. Maintenance job admission and coordinator
priority retain ADR 0048's policy; the common CPU pool itself does not claim a
separate priority scheduler.

FastCDC Chunk identities are parallelized across a bounded batch of complete
Chunks; one 16--256-KiB Chunk never creates nested Rayon work. Container-image
BLAKE3 may use the tree hasher across the shared pool only at or above the
measured 2-MiB crossover and only when the caller owns permits for the complete
pool. A verifier batch with several Containers parallelizes across Containers;
only a single large Container may consume the full pool internally. This keeps
thread count fixed and prevents nested jobs from claiming more CPU than their
admission budget.

The identity computed for a stable FastCDC Chunk is carried through the
Container writer together with the immutable Chunk bytes. It is optimization
evidence, not authority: the writer may use it for the Chunk table and avoid
rehashing or serializing a losing RAW candidate, but publication must reread
the completed image and recompute every Chunk identity before making the
canonical Container name visible. Recovery and scrub retain the same
independent verification. The writer does not immediately decompress a Zstd
record merely to repeat the mandatory publication reread.

Exact lookup accepts a sorted, deduplicated Chunk batch. It groups probes by
immutable Run page, uses Bloom negatives only to skip persistent lookup, and
verifies every selected Container Location before reuse. Newly verified
locations enter a bounded recent-location overlay before asynchronous L0
publication. The overlay is acceleration only and never authorizes unverified
bytes. One serialized activation step still installs a complete Run Set.

Checkpoint planning remains ordered because its common Exact set and Container
packing cross inode boundaries. Most long streams have already been reduced by
the overlapped write-through path before that commit tail. The metadata writer
publishes all new immutable objects without per-object directory sync, then
shares one directory durability barrier before publishing the Namespace Root
and syncing the Commit WAL last. FUSE operations that can perform storage I/O,
wait for a queue, or cross a sequence fence run on a bounded blocking executor
instead of Tokio runtime workers.

Scrub, Exact-Index rebuild, and GC replacement use bounded read, verify, and
ordered-reduce stages. HDD reads remain sequential or use bounded read-ahead;
CPU verification runs in parallel; the reducer preserves Container-generation
and Chunk ordering. Maintenance keeps ADR 0048's low-priority and promotion
rules and shares the same memory and CPU admission policy as foreground work.

## Paired invariants and evidence

- Detaching recomputes the complete Chunk byte sum, requires an ordered nonempty
  payload at or below one Container target, and clears the lane's Pending Chunk
  accounting in the same critical section. Publication completion must match
  the queue's active inode and mutation sequence.
- A public blocked-Sync test writes 70 MiB to one inode and requires admission,
  live visibility, and FastCDC progress beyond the first Container while DATA
  durability is stopped. `Release` and `Sync` tests require the same work to
  complete before the handle fence returns.
- Writer publication rereads and verifies the immutable Container before it
  produces externalization evidence. Recovery and scrub continue to verify the
  same Container envelope, records, Chunk identities, and manifest dependency.
- The ordinary writer and the proof-bearing writer produce byte-identical
  images for the same inputs. A deliberately incorrect carried identity can
  create only a non-authoritative prepared image: the publication reread must
  reject it, and no canonical Container name may become visible.
- Queue accounting asserts that detached work never exceeds 64 MiB and that all
  write-through state remains within 384 MiB. Backpressure tests fill both
  detached slots and the admission queue before requiring a writer to wait.
- Exact batch lookup treats Bloom and recent-overlay results as hints. Writer,
  demand reader, recovery, and scrub all require verified Container evidence
  before using the selected Location.
- A single-stream tracer requires more than one worker to hash a stable
  FastCDC batch. Serial and four-worker Container writers must produce
  identical bytes, and the budgeted parallel reader must accept that exact
  image. io_uring telemetry separately counts large parallel Container-hash
  verifications.
- Staged metadata publication cannot select visibility. The single commit writer checks
  deterministic results, synchronizes immutable dependencies, then performs
  the unchanged Commit-WAL sync as the final visibility operation.

## Consequences

One long stream can overlap chunking of Container N+1 with compression and DATA
I/O for Container N. Many streams can also use all effective CPUs without each
CPU-only stage creating its own maximum-sized thread set. The fixed memory
budget may apply backpressure sooner when DATA persistence stalls for a long
time, but it cannot grow into Swap. Exact and metadata batching reduce repeated
NVMe page reads and directory syncs without changing any source-of-truth or
crash-recovery rule.
