---
status: accepted
---

# Bound and version similarity search

Similarity Fingerprints are computed per logical chunk under an explicit profile
that fixes the algorithm, seeds, sampling, and buckets. The complete NVMe index
is rebuildable acceleration; in-memory state contains only representatives and
caches. An ingest initially examines at most 16 candidates and performs at most
four trial encodes, preventing bucket popularity from making latency unbounded.

The in-memory v1 bucket key is `(fingerprint profile, Superfeature slot, logical
length, Superfeature)`. Each bucket retains the 64 smallest full BLAKE3 Chunk
IDs. This is a deterministic, insertion-order-independent min-hash sample rather
than a claim of content identity. A query streams a four-way merge over the
sorted bucket representatives: at most 256 stored representatives are examined,
no temporary representative-ID collection is built, and only the best 16
candidates by complete 512-bit Sketch distance and Chunk ID remain. Changing
the key, sample policy, or any bound requires a new bucket profile.

## Consequences

Selection compares complete physical bytes and versioned Read Distance, Base
Load, and Fanout costs. DELTA/PREFIX must beat the best independent encoding by
both 5% and 4 KiB. Container fingerprints accelerate rebuild but may be
recomputed and checked. An optional 512-bit scalar/SIMD-identical sketch remains
a separate measured ranking feature; it is not a content identity or required
candidate source. Base fanout is measured and costed rather than rejected at a
fixed threshold.

## Base fanout

Version 1 imposes no hard fanout cap. `BaseLoadCost` may reward a popular warm
base, while `FanoutPenalty` accounts for the number of otherwise valid dependent
chunks made unreadable if that base becomes unavailable. Telemetry publishes the
full fanout distribution and scrub prioritizes high-fanout bases. A cap or extra
physical location requires measured restore and fault-injection evidence rather
than an arbitrary threshold. The evidence and benchmark gate are recorded in the
[delta depth and fanout research note](../research/delta-chain-depth.md).

## Adaptive cold Base admission (9 September 2026)

Ranking alone does not justify a speculative DATA read. Before loading a cold
Base, the writer now learns marginal encoded-byte savings per measured
Base-resolution plus codec-trial nanosecond. Exact authority and a matching
verified cache entry are checked first; current batch reuse and verified RAM
hits bypass admission. Ordinary reads, recovery and scrub never use this gate.

The bounded, volatile learner groups candidates by 512-bit Sketch distance
(16 bands), logical length (up to 16 KiB, 64 KiB, or 256 KiB), independent
encoded/logical size (four bands), and whether a better dependent candidate
already exists. Eight observed cold attempts warm each group. Groups with no
success in their last 32 observations, or under one quarter of the recent
portfolio's savings/time efficiency, skip cold reads. One in 32 rejected
candidate decisions still probes the workload. Savings and cost use an EWMA
with weight 1/32; warm hits do not bias the cold-read model. The time includes
resolution and verification, not just the storage syscall. Failed Base loads
contribute zero benefit. These are version-one policy bounds, not a fixed
Sketch acceptance threshold or a promised storage throughput target.

Learning uses a short try-lock and fails open on contention. Its 384 groups
have fixed memory and restart empty; no on-disk format or recovery authority
changes. Trial caps, depth one, exact decoding and the 5%/4 KiB acceptance rule
remain unchanged. A false rejection loses only optional compression savings.
A later candidate is credited only for improvement over the current best.

Telemetry separates skipped cold candidates, exploration read attempts,
backend Base read attempts (including failed attempts), RAM reuse and Base
trials that improve the encoded result. Backend attempts count record-load
calls, not device commands or bytes: one verified load may issue several I/Os,
and an OS cache may satisfy them. Legacy `base_reads` counts candidate resolution
attempts and must not be displayed as physical disk reads. Older telemetry
without the new counters remains unavailable in the UI.
