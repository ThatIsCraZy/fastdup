---
status: accepted
---

# Map active Exact Runs and index their page bounds

Amended by [ADR 0046](0046-bound-verified-read-cache-by-live-memory-headroom.md)
on 12 September 2026: all reusable read content shares one application cache;
repository I/O and FUSE file data use direct paths. File-backed mappings and
separate replacement policies described below are superseded. Immutable leases,
verification and publication ordering remain required.

Filesystem-backed Exact activation and recovery expose every physical Run in
the selected Run Set through a read-only mapping held by an immutable-file
lease. Before a reader becomes selectable, recovery verifies the expected
length, Header/Footer identity, every page checksum and ordering invariant, and
the complete Run hash directly over the mapping. All Runs in one active set
must select the same page-source mode; a partial mapping fails activation.

The complete audit also retains only the first and last `(Chunk ID, logical
length)` key of each page. Binary page search uses these compact bounds without
page I/O, page decoding, or decoded-cache locks. Only candidate pages are
field-wise decoded and admitted to the existing memory-governed Exact hot-page
cache. The bounds and cache are acceleration only: neither makes an Exact
negative authoritative, and every selected DATA Location still requires its
normal verification.

Adapters without immutable-file leases retain bounded `read_exact_at` reads.
Publication, compaction input, and offline scrub remain positional so the
independent persistence verifier does not share the mapped query
implementation. `write_at`, truncation, replacement, and removal remain denied
until the final mapped reader drops. Unsafe code stays confined to the mapping
module and relies on the appliance-owned Metadata directory not being mutated
outside `StorageIo`.

## Why

A naive mapping that decoded every page visited by binary search was rejected:
it measured 16,055 ns/query versus 1,531 ns/query for the decoded positional
cache. Adding audited page-key bounds reduced the 262,144-entry production-size
workload to 926 ns/query versus 1,807 ns/query for positional reads, a 1.951x
speedup, with zero major faults and zero process Swap. This evidence justifies
the mapping and the small per-page bounds, while retaining the decoded cache
only at candidate pages.

## Evidence

- Mapped and positional readers return identical bounded candidates.
- Corrupt pages fail both mapped activation and independent positional audit.
- Adapters without leases report and exercise the positional page source.
- Independently opened adapters cannot write, truncate, replace, or remove a
  mapped Run; reclamation succeeds only after the final reader drops.
- Page-source telemetry reports mapped/positional Run counts and resident
  page-bound bytes.
- The repeatable benchmark and results are recorded in
  `docs/benchmarks/exact-lookup-mmap-2026-08-27.md`.

## Reuse during an in-process L0 append (2026-09-06)

The serialized L0 append reads and validates the durable Activation Record and
its stored Run Set before matching the installed generation. It may reuse that
generation's already audited Mapping owners and Page Bounds only while their
immutable-file leases remain alive. Each successor Run reference must match
the owner's profile, generation, complete Run hash and length. New or unknown
Runs receive a complete audit. The final Activation-Log sync and generation
retirement fences are unchanged.

This avoids auditing every unchanged Run once to reload the predecessor and
again to activate its successor. Positional adapters, changed selectors and
process restart retain complete verification. Public activation, recovery and
offline scrub independently audit; the append optimization does not change
their corruption-detection boundary. The stored Run Set is still reread even
when its Runs can be shared. New membership admission resamples memory
pressure: a reused Bloom hint is dropped from the successor if it exceeds the
remaining budget, including when Swap disables admission. The Mapping may
still be reused without that optional hint.

Tests pair shared Mapping identity with independent recovery, reject a
different Run hash and stale installed selector, and verify pressure-driven
hint removal. Existing positional, activation fault, immutable-file mutation
and generation-drain tests remain required. The source audit and measurement
basis are in `docs/research/hotpath-audit6-2026-09-06.md`.
