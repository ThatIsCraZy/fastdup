---
status: accepted
---

# Start every repository with one current Policy Set

Before production, every new repository uses the current Policy Set from its
first Commit; daemon modes with Prefix selection disabled or enabled and
offline maintenance all require that same identity. Independent RAW/Zstd is
the defined fallback inside the current policy when Prefix selection is off or
no coherent Similarity snapshot is available, so runtime activation does not
need a second durable Policy Set.

The writer accepts no predecessor Policy Set and performs no Policy migration.
A repository created by an obsolete prototype policy is rejected and must be
recreated. Object-local format versions and the repository Format Epoch remain
independent compatibility boundaries.
