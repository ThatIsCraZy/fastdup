---
status: superseded by ADR 0072
---

# Recover Container generations from paired envelopes

This record originally authorized a one-time writable-startup migration into
the persistent allocator. ADR 0072 now forbids that migration: a nonempty DATA
repository without allocator state fails closed. The envelope scan remains a
diagnostic/rebuild primitive that discovers the greatest durable generation by
reading each canonical published object's physical length plus fixed 4-KiB
Header and 4-KiB Footer. Both blocks
must pass their checksums, agree on identity, generation, layout, and length,
and match the canonical filename. Startup must not decode every payload merely
to choose the next monotonic generation.

## Consequences

This Container Envelope Proof is deliberately structural. It may advance the
allocator or make it skip numbers, but it cannot authorize a Chunk Location,
satisfy a Manifest dependency, rebuild the Exact Index, or declare the whole
Container uncorrupted or initialize missing writable allocator state. Those
operations retain complete record/Chunk/hash verification or bounded candidate
verification as specified.

A malformed claimed name, bad Header/Footer, length disagreement, or identity
mismatch fails the diagnostic rather than being skipped. The scan remains
O(number of Containers); it is not part of ordinary writable startup.
