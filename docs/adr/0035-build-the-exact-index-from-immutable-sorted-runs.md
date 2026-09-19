---
status: proposed
---

# Build the Exact Index from immutable sorted runs

Represent the persistent Exact Index as immutable, page-checksummed sorted Run
families selected by a hash-chained activation log. Container publication adds
level-zero Runs; bounded redirect-on-write compaction replaces selected Runs and
activates the old or complete new Run Set atomically.

The Exact Index is acceleration, never content or liveness authority. A lookup
returns an untrusted Location candidate that still requires identity,
transition, admission, and Container checks. An absent or corrupt index may lose
deduplication or take the bounded fallback path, but cannot make a committed
Manifest unreadable or roll back the Namespace.

## Current implementation

- No complete Chunk-to-Location map is retained in RAM.
- Lookup selects at most one key-disjoint partition per active family; the
  active family count is bounded at 64.
- Four oldest same-level families compact into a deterministic higher level;
  262,144 entries is an output partition target, not a format limit.
- Activation, compaction, and external rebuild have old-or-complete-new fault
  coverage. Rebuild streams verified Container Recovery Indexes and activates
  only after the global cross-family invariants pass.
- Optional RAM membership hints are rebuildable, contain no Locations, share a
  headroom-bound cache budget, and are disabled for a new Run Set when process
  Swap is observed.

The durable layouts are defined by [Exact Index Run v1](../specs/exact-index-run-v1.md),
[Run Set v1](../specs/exact-index-run-set-v1.md), and
[Activation Log v1](../specs/exact-index-activation-v1.md). Rebuild procedure and
limitations live in [scrub and Exact-Index rebuild](../operations/scrub-and-exact-index-rebuild.md).

## Acceptance gate

The design remains proposed until Rocky/structured-corpus tests establish
throughput and restore cost, discovery/worker-order canonicality at scale, and
acceptable index write amplification. Existing implementation and fault tests
do not close those workload gates.
