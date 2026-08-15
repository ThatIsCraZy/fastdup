---
status: accepted
---

# Use hierarchical immutable manifests

A file Manifest is an immutable ordered extent tree rather than a flat chunk
list. Leaves partition the complete file range into DATA, HOLE, and FILL extents;
inner nodes summarize disjoint ordered ranges. Updating a bounded range rewrites only
affected leaves and paths to a new Manifest Root, making metadata work
proportional to the change rather than to a file that may contain billions of
chunks.

## Consequences

Manifest validation rejects arithmetic overflow, gaps, overlap, ranges beyond
EOF, and DATA references whose lengths disagree with their logical chunks. HOLE
extents carry no physical Location. FILL stores a single repeated byte and length
as structural encoding, remains allocated DATA even for zero, and carries no
Location. Other user-file bytes are never inline; policy-selected small files
use ordinary chunks in NVMe encoding containers.
