---
status: accepted
---

# Rebuild indexes as new generations

After NVMe index loss, fastdup inventories and structurally verifies Data-Tier
containers, builds provisional Location Sets from Recovery Indexes, selects the
highest complete Recovery Checkpoint, traverses its namespace and dependency
closure, and builds Exact and Similarity indexes as one new hidden generation.
Only the complete generation is atomically activated; interrupted rebuild state
is never queried online.

## Consequences

Normal startup need not rehash 500 TB: it validates the commit chain, object
checksums, container seals, and structural references, then verifies complete
decoded Chunk IDs on every actual read while resumable background scrub rehashes
the corpus. Operators may require a fully verified offline start. Independently
valid later objects may be exposed only through `lost+found`.
