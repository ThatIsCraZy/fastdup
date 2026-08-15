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
