---
status: accepted
---

# Bound read amplification and background I/O

Current-state note (2026-08-27): ADR 0077 implements bounded demand Record
planning and restore-local Location selection. The per-handle speculative
prefetch described below is not implemented; kernel readahead under ADR 0073
does not satisfy that separate userspace policy.

Version 1 decodes a complete bounded Compression Region, and at most one
independent Depth-1 Base, even for a small logical read. A shared cache entry
becomes visible only after stored CRC, decode, length, and complete Chunk-ID hash
validation. Per-handle sequential prefetch is limited to one or two Placement
Windows and stops on direction or locality change.

## Consequences

One failed preferred Location may trigger one fully verified alternate before
`EIO`; retries are bounded and repair is RoW. Commit durability and demand reads
outrank repair, GC, prefetch, and scrub, which are throttled as commit age grows.
Caches are bounded, sharded per worker/NUMA node, and avoid global pointer-heavy
LRU state. Read amplification, queue bytes, latency, reuse distance, and remote-
NUMA hits are measured per operation class.
