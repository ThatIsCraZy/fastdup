---
status: accepted
---

# Build Exact and Similarity indexes from one verified pool scan

The advanced-reduction rebuild decodes each immutable Container once and feeds
the verified record stream to two bounded builders. Verified Locations produce
hidden Exact Runs; decoded logical bytes produce externally sorted Similarity
entries. Neither builder retains a pool-sized map.

The Similarity family format stores the content-addressed Exact Run Set ID from
that same scan. Identical occurrences of one Chunk ID collapse to one logical
Similarity entry. A second occurrence with different length, fingerprint, or
sketch is corruption. An empty scan creates an empty bound Similarity family,
so a prior non-empty snapshot cannot remain selected accidentally.

Publication has three ordered phases:

1. publish and audit hidden Exact Runs and Similarity partitions;
2. activate the complete Exact Run Set;
3. publish and sync the bound Similarity family manifest.

The family manifest is the advanced-reduction commit point. A failure before
Exact activation exposes neither new index. A failure after Exact activation
may expose the new Exact acceleration alone, which is safe and independently
useful. Similarity is never selected before the Exact Run Set needed to resolve
its candidates. Paired readers and offline scrub compare the family's stored
Run Set ID with the ID selected by Exact recovery and do not select an older or
unbound family.

## Why

Scanning the full Data Pool twice would duplicate the dominant Container read,
decompression, checksum, and Chunk-ID verification work. Coupling the builders
through a shared verified-record visitor preserves deep module boundaries: the
Container repository owns verification, each index owns its bounded staging
format, and maintenance owns only ordering and activation.

A shared mutable transaction or embedded database is unnecessary. Both indexes
are rebuildable immutable acceleration. A content binding plus ordered atomic
publication gives recovery enough evidence without making either physical file
format depend on the other's layout.

## Consequences

- Similarity format v2 authenticates an optional Exact Run Set ID in both the
  header and footer. Standalone snapshots remain possible but cannot be opened
  through the paired-reader seam.
- Orphan Similarity partitions count toward the generation high-water mark, so
  a retry never reuses their physical names.
- The external sort removes identical duplicate Chunk IDs before assigning
  snapshot ordinals and before representative selection.
- Complete fault matrices cover failures before and after every metadata I/O;
  a recovered Similarity family must always name the active Exact Run Set.
- Activating dependent prefix encodings remains a later step. This record only
  makes their pool-wide candidate index and Exact resolver a coherent pair.
