---
status: accepted
---

# Authenticate allocation totals in Manifest subtrees

Manifest Inner Node v2 stores the allocated-byte total of every direct child
next to that child's logical range and Metadata Object ID. The total counts
DATA and FILL bytes and excludes HOLE bytes. It is authenticated by the
content-addressed Metadata Object envelope and is part of the immutable child
capability used by path-local Manifest successors.

This summary lets truncate and later length-changing splice operations derive
the successor file's allocated-byte total from the touched path and discarded
subtrees. They no longer flatten or scan the retained or removed file recipe,
and callers never supply an asserted allocation scalar. Logical range,
subtree allocation, level, and child identity are verified together.

## Compatibility and pairing

Readers continue to accept Manifest Inner Node v1. A v1 child has no trusted
allocation summary, so an operation that requires one may perform a complete
verified scan or refuse the optimized path; it must not invent a total. New
trees and every rewritten ancestor use v2. An untouched v1 subtree can be
referenced by a v2 parent only after its allocation total has been completely
verified.

The writer computes leaf totals from canonical extents and parent totals with
checked addition. Recovery and offline scrub traverse the complete selected
tree and require every v2 child summary to equal the verified child subtree.
Demand reads may use a summary only through an installed
`ManifestSuccessorProof` derived from that complete predecessor proof.

Truncate rewrites only the cutoff path, drops right-hand child capabilities,
and preserves all remote left-hand subtree IDs. A cutoff inside HOLE or FILL
splits that extent. A cutoff inside DATA first replaces the complete boundary
Chunk extent with a newly encoded prefix plus a HOLE suffix, then applies the
same structural truncate. New DATA is verified before the Commit WAL append;
the WAL remains the sole visibility point.

Middle splice/concat applies the same authenticated capabilities to both edit
boundaries. Fully removed child totals are subtracted without descending into
their descendants; complete prefix and suffix children are reused exactly.
Their parent offsets are recomputed, so a suffix may move in the file without
changing its child object identity. New parents authenticate the recomputed
logical ranges and unchanged child totals. Recovery and scrub independently
descend through those parents and pair every total with the actual subtree.

## Consequences

Inner v2 keeps the existing 64-byte header and child-record size. It consumes
eight previously reserved child bytes for `allocated_bytes`; the remaining
eight bytes stay zero. `allocated_bytes <= logical_length` is required for
every child and checked before arithmetic or allocation. A v2 node with a
wrong authenticated total is corruption even when all child IDs and ranges
are otherwise valid.

The format change increases no object or fanout size. It adds checked scalar
work per visited child and makes path-local shrink or middle splice cost
proportional to tree height plus the edit frontier and replacement recipe,
rather than total file recipe size.
