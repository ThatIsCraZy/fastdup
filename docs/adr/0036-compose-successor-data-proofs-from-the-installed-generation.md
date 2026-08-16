---
status: accepted
---

# Compose successor DATA proofs from the installed generation

Normal in-process commits form a Successor Graph Proof: unchanged Manifest
extents retain the complete DATA proof of the immediately preceding verified
and installed generation, while every newly introduced Chunk dependency is
fully verified through the ordinary nonauthoritative Exact-Index/Container
path. This removes graph verification proportional to a growing file without
making the Exact Index authoritative or retaining a complete Chunk map in RAM.

## Consequences

Proof reuse is valid only across the single serialized `DurableNamespace`
successor transition. Common Manifest prefixes and suffixes identify preserved
dependencies; the changed middle is reread from the published Manifest and
verified completely before the Commit WAL append. A failed commit retains the
same installed predecessor proof for retry. The rule is part of the versioned
writer Policy Set.

Process restart, recovery, offline scrub, a missing installed predecessor, or
an unusable Exact candidate performs a fresh complete proof or fails closed.
Immutable Container corruption discovered later still fails demand reads and
is handled by scrub/quarantine; repeatedly rereading every historical Chunk at
five-second commit cadence is not a substitute for scrub.
