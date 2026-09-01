---
status: accepted
---

# Map active Exact Runs and index their page bounds

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
