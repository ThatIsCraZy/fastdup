---
status: accepted
---

# Cap delta dependencies at depth one

DELTA and ZSTD_PREFIX records should initially contain exactly one target chunk
and at most one Base Chunk ID whose selected location is independently decodable.
This caps restore amplification, corruption radius, GC liveness closure, and
rebuild complexity. [Primary-source research](../research/delta-chain-depth.md)
shows that deeper chains can matter on favorable evolving backups, but does not
isolate depth 2 from depth 3 at fastdup's geometry. They therefore remain an
explicitly gated future experiment rather than a v1 format capability.

## Considered Options

- **Depth 1:** simple bounded dependency and predictable one-base read.
- **Depth 2–3:** potentially better reuse, but compounds reads and lifecycle
  dependencies.
- **Unbounded chains:** rejected because worst-case restore and recovery cease to
  be locally bounded.

## Consequences

An offline replay may prototype depth 2 only after showing at least 5% physical-
byte savings on the capacity-weighted target mix, or 10% on a named family that
will occupy at least 20% of capacity. Depth 3 requires at least 2% further saving
beyond a qualifying depth-2 result. Production additionally requires sequential
restore throughput of at least 90% of depth 1, cold random-read p99 below 1.5x,
GC/scrub/rebuild time below 1.2x, and complete integrity fault coverage.
