---
status: accepted
---

# Compose successor DATA proofs from the installed generation

An in-process commit reuses unchanged DATA dependencies from the immediately
installed generation and proves only dependencies introduced by the successor.
New dependencies require verified writer evidence, guarded Exact selection, or
independent storage verification. Exact remains acceleration, never authority.

## Generation fence

Every `ManifestSuccessorProof` names the complete predecessor Commit Record.
All inode proofs in one Namespace successor must name the same record, and that
record must equal the serialized writer's current WAL head before verification,
Metadata publication, and WAL append. A stale or mixed predecessor fails closed;
the writer does not silently substitute another proof path.

The capability is process-local and has no public constructor. Restart,
recovery fallback, disaster recovery, and offline scrub rebuild evidence
independently. Normal committed startup follows ADR 0091: the Metadata graph is
proved before mount, while DATA is verified on demand and by background scrub.

## Manifest edits

Installed state is an opaque Manifest Root plus verified logical-length and
allocation summaries, never a flattened recipe. Persistent edits preserve
untouched subtree identities and publish changed nodes child-first:

- replacement rewrites only intersecting paths;
- append starts a new leaf sequence at the committed EOF and rewrites the right
  spine;
- truncate uses authenticated subtree allocation summaries and rewrites the
  cutoff path;
- splice composes prefix, replacement, and suffix forests using node-local
  coordinates, so shifted suffix subtrees keep their identities; and
- mixed grow/shrink operations compose those primitives in one successor.

A boundary may split HOLE or FILL directly. Cutting DATA requires reconstructing
and independently reducing the retained fragments. A logical layout may exceed
one Metadata Object; the store partitions it into bounded leaves without
changing its proof. Recovery and scrub always validate the complete selected
tree and full Chunk/slice bounds.

After fresh recovery, the same freshly built evidence may authorize the
immediate inode-reservation Commit when no mutation intervenes and the WAL-head
fence still matches. It never reuses evidence from the previous process.

Fault matrices cover append, replacement, truncate, splice, mixed size changes,
sparse gaps, and publication interruption; recovery must expose the complete old
or complete new generation.
