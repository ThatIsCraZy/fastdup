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

## Record-local fallback verification (2026-09-12)

A missing, stale, or unusable Exact hint must not turn a demand Cache Miss or
an online dependency check into a full payload scan of unrelated Containers.
Fallback discovery now reads each selectable Container's paired envelope and
checksum-checked compact Recovery Index, then reads only Records containing
still-required Chunk identities. Header/Footer and Index discovery use the
structure-read adapter to avoid speculative payload readahead. Each physical
range remains capped by the storage adapter's one-MiB bound.

A Recovery Index entry is a candidate, never a content proof. Before payloads
escape, its full Record coordinates, codec, CRC, ordinal, decoded range and
identity must match the independently decoded Record. Complete Record CRC,
decoded lengths and all sibling Chunk-ID hashes remain mandatory. Dependent
Records also resolve and verify one independent Base under ADR 0075. Base
retention is scoped to one Container, not the entire Repository scan.

One dependency-verification pass discharges every required sibling decoded in
the same Record. The indexed verifier keeps valid hints even when another hint
is missing and sends only unresolved identities to one fallback scan. Evidence
is not retained between passes. This supersedes references to a "complete
verified Container scan" for demand and required-Chunk proof fallback in ADRs
0036 and 0077; it does not create a Container publication or structural proof.

Scrub, complete Container verification, and index reconstruction still validate
the full Container, its Index/Record bijection and structural commitment.
Corruption in an unrelated payload is therefore found by scrub rather than
forcing an otherwise independently valid demand Chunk read to fail. Corrupt
selected payloads, Bases, local Index CRCs and malformed envelopes fail closed.
Retiring Containers remain excluded from fallback Location selection. No durable
format field, migration, writer invariant or trust in a Similarity hash changes.

Evidence: `fastdup-store/tests/prefix_recovery_index.rs` catches the former four
whole-object reads for two 8-KiB demand/commit resolutions. The bounded path
performs zero whole-object reads and requests 50,560 bytes including discovery.
See `docs/testing/bounded-fallback-reads-2026-09-12.md` for scope and limitations.
