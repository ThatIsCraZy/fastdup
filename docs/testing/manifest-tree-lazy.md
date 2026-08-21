# Manifest tree lazy-read and path-update evidence

The ordinary installed view retains only an immutable Manifest Root, logical
length, and verified allocated-byte total. It does not retain a flattened
extent vector. A demand read or partial allocation query verifies and decodes
only tree nodes intersecting the requested logical range.

Equal-length checkpoint updates derive bounded rewrite ranges from the frozen
dirty map. Each range is expanded to whole DATA extents, because a Chunk ID is
indivisible, then only intersecting leaves and their ancestors are
content-addressed and published. HOLE and FILL extents may be split. A boundary
inside DATA fails before publication. Unchanged subtree IDs are copied exactly;
metadata objects are durable before the Namespace Root and Commit WAL make the
successor visible.

The public-seam tests in
`crates/fastdup-testkit/tests/manifest_tree_lazy.rs` cover:

- a one-byte tail read in a three-level, roughly 68-GiB FILL tree;
- corruption of the touched tail leaf and depth-bounded rejection;
- one middle-leaf replacement adding exactly the leaf and two ancestor objects;
- exact identity retention for a remote subtree; and
- rejection of a replacement boundary inside DATA without object publication;
  and
- offline rejection of a reauthenticated but false v2 subtree allocation
  summary.

Checkpoint crash tests additionally require the Commit WAL sync to remain the
final fallible metadata operation and recover only the previous or complete
generation.

A sequential append now carries an opaque store-constructed successor proof.
It encodes the new suffix into fresh local-coordinate leaves, reads and rewrites
only the installed tree's right spine, retains every remote subtree identity,
and verifies only newly introduced DATA dependencies before the WAL commit. A
68-GiB sparse fixture with 1,088 materialized 64-MiB boundaries originally
performed 2,443 Metadata reads for a 17-byte append. The accepted public-seam
test now bounds the same `dispatch -> checkpoint` operation to at most 64
Metadata reads and verifies recovery through the ordinary complete reader.

Equal-length replacements now extend the same opaque successor proof while
rewriting only touched paths. On the same 68-GiB/1,088-boundary shape, a
one-byte tail replacement fell from 1,282 to 194 Metadata reads; its public
oracle caps path plus bounded commit overhead at 256, crashes both stores, and
reads the changed byte through complete recovery.

Arbitrary length-changing middle splice/concat is also tree-native. Public
repository tracers cover a 64-MiB-to-96-MiB middle replacement in a three-level
tree, insertion at an exact subtree boundary, and deletion spanning child
ranges. They require exact result lengths and allocation totals, bounded new
Metadata objects, and exact identity retention for complete shifted suffix
subtrees. A DATA-boundary tracer verifies that a partial Chunk is retained as
two bounded v2 DATA_SLICE extents around the replacement without DATA ingest.
The exhaustive fail-before/fail-after matrix spans splice Metadata publication,
directory durability, Namespace publication, and the final WAL sync; crash
recovery exposes only the byte-exact predecessor or complete successor.
Metadata GC remains explicit follow-up work.

Manifest Inner Node v2 authenticates each child's allocated-byte total. A
truncate can therefore account for and discard complete right-hand subtrees
without reading their descendants, while partial allocation queries sum fully
covered children directly. The 68-GiB/1,088-boundary public tracer reduced one
32-GiB sparse truncate from 102,152 to 14 Metadata reads, crashes both stores,
and verifies the exact EOF and retained byte. A separate DATA-boundary tracer
cuts inside a FastCDC Chunk, re-encodes only its retained prefix, and recovers
all 333,333 retained bytes exactly. The exhaustive metadata fail-before and
fail-after matrix recovers only the previous length or the complete new cut.

The Successor Proof is additionally fenced to the exact installed Commit
Record. A public `GenerationRepository` tracer commits generation two, then
attempts another commit with a proof still bound to generation one. It requires
`StaleSuccessorPredecessor`, observes no second dependency-verifier call, crashes
the metadata adapter, and recovers generation two unchanged. The complete
generation fault matrix remains the paired recovery reader for this writer
invariant.
