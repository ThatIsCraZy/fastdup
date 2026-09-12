---
status: accepted
---

# Use S3-FIFO for the historical proof cache

Amended by [ADR 0046](0046-bound-verified-read-cache-by-live-memory-headroom.md)
on 12 September 2026: all reusable read content shares one application cache;
repository I/O and FUSE file data use direct paths. File-backed mappings and
separate replacement policies described below are superseded. Immutable leases,
verification and publication ordering remain required.

fastdup uses S3-FIFO as the production replacement policy for verified
historical dependency proofs. The choice follows trace replay with identical
byte budgets over three unchanged Rocky ISO ingests and 50 minimally changed
ISO streams. S3-FIFO retained more useful proofs for the repeated unchanged
stream; SIEVE won the most constrained variant case, but the policies converged
once the working set fit. S3-FIFO's separate Small, Main, and Ghost queues also
let fastdup use proof origin directly: newly published proofs enter Small,
while an Exact-reused and physically reverified proof enters Main.

The cache remains an optimization. Active and Frozen Generation proofs live in
a separate pinned set and cannot be evicted. Its combined admission cap may
reject a new proof, including promotion from history; an uncached successor
dependency must then undergo complete storage verification before commit, as
specified in ADR 0036. Cache admission exhaustion is not a runtime failure. Historical S3-FIFO state starts
empty after restart, may shrink to zero under memory pressure, and never
authorizes data without a complete Chunk ID and logical-length match. A bounded
eviction scan may reject an admission but may not fail a write or commit.

The production implementation uses sharded slot arenas with stable indices and
FIFO rings without a global hit-path lock. Arenas grow lazily under their shard
lock and reserve before eviction, so allocation failure rejects admission
without losing a resident proof. Capacity comes from a byte budget
assigned by the shared reuse-based broker in ADR 0046, with DATA-fallback
priority, the shared memory reserve and Process-Swap admission closure. The 192-byte
charge used by the replay is a comparison model, not a fixed production entry
size or cache limit. SIEVE remains available only as a replay challenger.
