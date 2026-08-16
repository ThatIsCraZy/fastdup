---
status: accepted
---

# Recover Container generations from paired envelopes

Until a persistent Container-generation allocator exists, writable startup
discovers the greatest durable generation by reading each canonical published
object's physical length plus fixed 4-KiB Header and 4-KiB Footer. Both blocks
must pass their checksums, agree on identity, generation, layout, and length,
and match the canonical filename. Startup must not decode every payload merely
to choose the next monotonic generation.

## Consequences

This Container Envelope Proof is deliberately structural. It may advance the
allocator or make it skip numbers, but it cannot authorize a Chunk Location,
satisfy a Manifest dependency, rebuild the Exact Index, or declare the whole
Container uncorrupted. Those operations retain complete record/Chunk/hash
verification or bounded candidate verification as specified.

A malformed claimed name, bad Header/Footer, length disagreement, or identity
mismatch fails writable startup rather than being skipped. Thus ambiguous
durable publication cannot cause generation reuse, while ordinary startup I/O
is bounded to 8 KiB per Container instead of all stored payload bytes. The scan
remains O(number of Containers) and is replaced later by a separately durable
high-water record; that future record remains rebuildable from these envelopes.
