# Mixed overwrite and growth checkpoint failure

The test VM logged `checkpoint failed: Generation(ManifestTree(TreeTooLarge))`
at 22:40:30 on 2026-09-06, before the operator-authorized hard stop. Mutation
admission then stayed closed, so this is a storage checkpoint failure rather
than evidence of a Samba credential problem.

A deterministic regression builds a real persistent Manifest with 262,144
extents, overwrites its header, and appends bytes in the same checkpoint. On
`ac3f3eb`, the original reduced fixture failed with the exact `TreeTooLarge`
error in 0.48 seconds. The complete Manifest was being flattened through the
legacy single-leaf reader when a file both changed its existing prefix and
grew. Pure appends and equal-size replacements already had tree-local paths.

The writer now composes replacements below the old EOF with a tree-local
append above it. Untouched subtrees retain their identities. The opaque
successor proof retains all newly introduced DATA and Metadata dependencies
from both phases, and allocation is derived from removed, replaced and appended
extents. No durable format or recovery validation was weakened. ADR 0036 records
the combined path.

The final large fixture represents a 128 MiB file with 262,144 alternating FILL
extents. It verifies successful checkpoint, fewer than 64 Metadata reads during
the checkpoint, exact header and tail bytes after simulated power loss, and a
complete offline DATA scrub. This guards against accidentally restoring a full
Manifest scan; it is not an end-to-end Veeam throughput measurement.

A separate proof-composition test publishes only the new tail DATA and leaves
the overwritten header DATA missing: commit must fail and the old generation
remain selected. Fault injection covers both sides of every checkpoint Metadata
operation for header rewrite plus ordinary append, a write crossing the old
EOF, and an append after a sparse gap. Recovery must return one complete old or
new layout, never a mixed generation.

Validation passed: 127 appliance tests (library, durable namespace, fault
injection, large Manifest growth, recovery and write-through ingest), plus
93 store tests (library, filesystem Generation repository and Manifest reader).
Production Clippy for both libraries passed with warnings denied. Ignored
benchmarks were not counted as passing tests. Reproduction and validation logs
are retained under `.artifacts/manifest-growth/`.

Revision 0.6.4-11 includes this fix and the cache/recovery improvements from
revisions 9 and 10. The operator requested prevention rather than recovery of
the existing test data. The repository runtime remains stopped, and existing
data is not deleted or reset. A fresh live Veeam run is still needed to validate
the combined workload; these tests prevent recurrence of the reproduced
checkpoint defect, not every possible storage or network failure.
