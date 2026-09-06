---
status: accepted
---

# Overlap reduction stages under one memory and CPU budget

fastdup separates ordered per-inode SeqCDC, Container preparation, DATA
publication, Exact-Index maintenance, and commit metadata into bounded stages.
A complete Container payload becomes immutable detached work. This releases its
Ingest Lane before compression or storage I/O, so the same inode may continue
chunking while its preceding Container is persisted. ADR 0057 permits two
storage publications for one active inode but keeps their retirement ordered.
`Sync`, `Release`, and a Frozen Commit Cut wait for
both accepted Ingest jobs and detached Container work through the sampled inode
mutation sequence before they consume reduction evidence.

The process retains at most two detached 32-MiB Container payloads. The
400-MiB write-through budget includes these payloads, the 32-MiB SingleStream admission
queue, every registered lane, the overflow lane, and each incomplete SeqCDC
suffix. The extra overlap therefore reduces the number of registered hot lanes
from ten to eight plus one overflow lane. A full detached-work budget blocks a
reduction worker. Frontend admission closes only after that blockage also fills
the separate admission queue. Queue and lane byte counts are conservative even
when they share the same immutable Mutation Payload backing with POSIX Dirty
DATA.

The SingleStream admission queue is organized as a per-inode ring of immutable,
segmented Ingest Batches. One active writable inode may occupy eight 4-MiB
slots. A sealed slot enters ordered reduction while the producer immediately
fills another slot. The ring stores `MutationPayload` views rather than copying
bytes into a circular byte array. A slot seals when it reaches 4 MiB, after
10 ms, at a discontinuity, or before a mutation-sequence fence. Writable-handle
lifetime, not transient queue occupancy, selects the policy. At two or more
open writable inodes, admission returns to the pre-ring MultiStream path: one
job per 1-MiB fragment under the original global 16-MiB queue window and the
existing per-inode round-robin worker order. This keeps SingleStream burst
headroom from shrinking or serializing the established MultiStream pipeline.

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

SeqCDC Chunk identities are parallelized across a bounded batch of complete
Chunks; one 16--256-KiB Chunk never creates nested Rayon work. Container-image
BLAKE3 may use the tree hasher across the shared pool only at or above the
measured 2-MiB crossover and only when the caller owns permits for the complete
pool. A verifier batch with several Containers parallelizes across Containers;
only a single large Container may consume the full pool internally. This keeps
thread count fixed and prevents nested jobs from claiming more CPU than their
admission budget.

Hash admission first classifies FILL under one CPU permit and carries the
non-FILL ordinals forward, so no FILL scan is repeated. The hash request uses
both non-FILL byte count and runnable four-Chunk groups. The measured policy
requests at most `floor(sqrt(ceil(hash_bytes / 256 KiB)))` workers, with a
minimum of one and the configured CPU budget as the upper bound. This gives
two workers for 1 MiB, four for 4 MiB and permits a 32-MiB batch to use all ten
workers on the measured host. It places no fixed process-wide CPU cap. The
serial-pass permit is released before requesting parallel admission; partial
grants preserve the same Chunk identities and order. All-FILL batches retain
one worker. The crossover evidence is in the sixth hotpath audit.

Advanced fingerprint preparation separately requests at most one worker per
64 KiB of target bytes, rounded up and bounded by target count and the shared
CPU budget. The measured 128-KiB batch favors two workers over eight; larger
batches retain access to the full pool. This limit applies only to candidate
preparation, not to the separate Base/codec-trial waves. Partial grants and
permit return preserve the original plan order and coherent batch snapshot.
The seventh hotpath audit records the component measurements; this policy does
not impose a process-wide thread cap.

CPU admission registers Condvar waiters under the same mutex that protects
available permits. A release broadcasts only while a waiter is registered;
uncontended retirement does not notify an empty wait set. Registration precedes
the atomic unlock-and-wait operation and remains registered until reacquiring
that mutex, preserving wakeups, partial grants and RAII permit return. Tests
cover several blocked coordinators, spurious notification and worker unwind.
The eighth hotpath audit records permit-cycle and admitted-work measurements.

Ingest Tail segments pair their immutable payload with its mutation sequence.
A consumed whole segment transfers its owner; a partial consumption uses a
checked consuming split that retains the original backing charge. Single-part
Chunks store their payload inline. Multi-part extraction reserves all required
fragment or coalescing storage before changing the Tail, including the existing
1,024-fragment fallback. Each constructed Chunk independently validates its
complete byte sum; local mutations preserve Tail accounting and stable-batch
boundaries independently audit the remaining Tail. This removes per-Chunk
rescans of the retained suffix without dropping the boundary audit.

Normal Reduction directly borrows the already prepared Compression Regions.
Only Advanced Reduction constructs a target-selection mask and splits Regions
at codec choices. Both preserve the same Chunk order and partitions. Record
ordering indexes the full caller-owned Chunk IDs by compact hash-table ordinals
and computes each Record's sort key once. Full-key comparison, duplicate-ID
rejection and the complete final partition/order check remain mandatory.

Container assembly appends each Record's writer-carried Recovery Index entries
directly to the already reserved Container-wide vector. Publication Locations
are derived from that appended slice. This removes a temporary allocation and
copy per Record without changing field serialization, Record CRCs, independent
verification or the durable Recovery Index order.

Pending staging validates each new Chunk's length, checked range end and
ordering against its predecessor, and updates its byte sum in the same append
operation. Ordinary lane-bound checks use that preserved sum. Detach transfers
the chunks and accounting together and independently validates all lengths,
range ordering, the full byte sum and the Container bound. This same pass
derives the minimum included mutation sequence for the publication fence.
Thus growing a Pending Container does not repeatedly rescan its already
checked prefix; malformed detach accounting and reordered chunks still fail.

The identity computed for a stable SeqCDC Chunk is carried through the
Container writer together with the immutable Chunk bytes. Under ADR 0059, the
encoder also returns publication Locations from the layout it serialized.
Publication trusts this prior writer work and does not immediately decompress
Zstd or recompute Chunk identities. Recovery, ordinary reads, and scrub retain
independent verification.

Exact lookup uses Bloom negatives to skip persistent lookup and verifies every
selected Container Location before reuse. Under ADR 0059, detached publication
trusts the first negative result instead of repeating the lookup. Newly
verified locations enter a bounded recent-location overlay before asynchronous
L0 publication. The overlay remains acceleration only. One serialized
activation step still installs a complete Run Set.

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
  live visibility, and SeqCDC progress beyond the first Container while DATA
  durability is stopped. It also requires a second Ingest Ring slot while the
  first publication is blocked. `Release` and `Sync` tests require the same
  work to complete before the handle fence returns. Separate public tests
  require sequential 1-MiB writes to coalesce up to 4 MiB, a partial slot to
  seal after its age bound, and multiple open writers to restore the unbatched
  1-MiB path.
- Writer publication carries encoder-produced Location evidence through sampled
  storage publication. Recovery and scrub independently verify the Container
  envelope, records, Chunk identities, and manifest dependency.
- The ordinary writer and the proof-bearing writer produce byte-identical
  images for the same inputs. A deliberately incorrect carried identity may
  publish under ADR 0059, but the first independent read must reject it.
- Queue accounting asserts that detached work never exceeds 64 MiB and that all
  write-through state remains within 400 MiB. Backpressure tests fill both
  detached slots and the admission queue before requiring a writer to wait.
- Exact lookup treats Bloom and recent-overlay results as hints. Publication
  trusts its first result; demand reader, recovery, and scrub independently
  verify selected Container bytes.
- A single-stream tracer requires more than one worker to hash a stable
  SeqCDC batch. Serial and four-worker Container writers must produce
  identical bytes, and the budgeted parallel reader must accept that exact
  image. Publication performs no second Container verification.
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

## Shared planning and demand-decode admission (2026-09-05)

Write-through Advanced Reduction materializes a Compression Region once and pins
one coherent Exact/Similarity view for its bounded detached Container batch.
Fingerprint, candidate lookup and independent-codec preparation use admitted
parallel jobs; the publication coordinator then reads at most eight verified
Base owners per wave, with CPU permits released. Admitted codec-trial jobs
consume those Bases and return ordered results. Trial budgets, savings thresholds,
independent fallback and the publication/retirement guard remain unchanged.
Receive fragmentation does not choose a compression boundary. Prepared records
are assembled in logical input order, including the ordered checkpoint tail.

Hash and encode jobs acquire the next small task dynamically within the granted
worker count. Demand reads keep ascending physical coalescing capped at 1 MiB;
independent decode batches of at least 256 KiB may opportunistically acquire up
to four of the same CPU permits. Saturated pools and callers already executing
inside Rayon decode synchronously, so a nested Base read never waits for its
own permit. Singleflight leaders still complete or release on every outcome.

Advanced phase telemetry separates fingerprint, candidate lookup, verified Base
read, and codec-trial elapsed time. Summed parallel elapsed time is neither pure
CPU time nor end-to-end latency; the coordinator's planning wall time also
includes admission waits and I/O. Existing encode timing keeps its prior scope.

Compression Region preparation now uses the same admission for independent
materialization jobs. Cheap borrowed-view construction stays with the
coordinator; only regions requiring owned contiguous bytes enter CPU jobs.
The existing detached-payload/region bounds are unchanged, and completed owners
are collected in region order before any planning borrows them. Empty batches
take no permits; every batch caps its requested permits by its actual job count
before admission, including small Base-trial waves. Preparation wall telemetry
includes admission waiting and is reported separately from encoding.

## Runnable work and worker retirement (2026-09-05)

CPU requests are capped before acquisition by the actual hash shards or ordinary
Compression Regions. Ingest jobs waiting for storage do not divide the CPU share;
the common permit pool accounts for runnable CPU work. Finished map workers
retire their own permits. Hash and encode workers retire at their worker boundary,
including errors; one permit remains through serial result/Container assembly.
The complete lease still releases on every failure path.

Advanced Base waves refill a finished target slot before the next CPU phase,
retain at most eight independently verified Base owners, and share identical
Bases within the wave. They retain candidate order, codec-trial budgets, savings
thresholds and logical result order. There is no speculative next-candidate read
or parallel HDD I/O. Differential tests compare 25 mixed targets with serial
planning at one and four workers; prepared records must be byte-identical.


## Copy work granularity (2026-09-06)

Compression Region materialization requests at most one worker per 512 KiB of
aggregate copied input, rounded down with a one-worker minimum. The ordinary
job-count cap still applies. Small batches retain CPU admission while avoiding
Rayon/queue/result-assembly costs that exceed their copy work. Fingerprint and
codec stages keep their separate worker policies. Large materialization batches
retain parallelism; no durable chunk or compression boundary changes.
