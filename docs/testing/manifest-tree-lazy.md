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
- rejection of a replacement boundary inside DATA without object publication.

Checkpoint crash tests additionally require the Commit WAL sync to remain the
final fallible metadata operation and recover only the previous or complete
generation. Length-changing path-local splice/concat and an opaque subtree
successor proof that removes the current full structural commit traversal are
explicit follow-up work recorded in ADR 0036.
