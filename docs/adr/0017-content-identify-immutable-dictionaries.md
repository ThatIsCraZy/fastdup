---
status: accepted
---

# Content-identify immutable dictionaries

Current-state note (2026-09-05): this is an accepted dependency contract for a
future durable Dictionary path. Current Dictionary training and encoding are
experimental under ADR 0047; production Container readers/writers implement
RAW, Zstd, ZSTD_PREFIX, and Sparse-XOR, with no durable Dictionary activation.

Zstd dictionary records reference an immutable Dictionary Object identified by
BLAKE3-256 of its exact bytes. Retraining always creates a new ID and old records
retain their original dependency. A missing or corrupt dictionary makes that
encoding unusable; fastdup never substitutes a similar dictionary because only
the named bytes can guarantee exact reconstruction.

## Consequences

Dictionary Locations are verified and tracked like other physical dependencies,
and GC retains the last active copy of every dictionary reached from a selected
live encoding. After dictionary decode, each target Chunk ID is still verified.
