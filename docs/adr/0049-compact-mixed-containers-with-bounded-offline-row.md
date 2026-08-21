---
status: accepted
---

# Compact mixed Containers with bounded offline RoW

Offline GC may select two or more partially live Containers as one Compaction
Victim Set when their unique uncovered live Chunks are predicted to need fewer
replacement Containers. It decodes and verifies the victims, deduplicates live
Chunk identities, adaptively re-encodes bounded replacement batches, publishes
and verifies every replacement first, activates an Exact Index excluding the
victims, revalidates the GC Scrub Plan, and only then unlinks the victims.
Manifests remain unchanged because replacement records preserve logical Chunk
IDs.

## Consequences

One replacement batch retains at most 48 MiB of decoded payload and 32,768
Chunks, and Compression Regions remain bounded to 512 KiB. A fully live
Container outside the Victim Set already counts as replacement coverage, so a
retry can collect old victims without writing another copy. Deterministic GC
replacement identities and resumable non-authoritative temporary objects make
interrupted publication retryable. The more-than-20% priority rule uses victim
bytes minus a conservative independent-RAW physical upper bound for replacement
bytes; adaptive compression may improve but never inflate that estimate. This
decision authorizes only exclusive offline compaction; online relocation still
requires ADR 0021 RETIRING states, Location-Set generations, and reader/writer
pin drain.
