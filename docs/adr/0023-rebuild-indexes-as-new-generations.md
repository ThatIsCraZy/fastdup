---
status: accepted
---

# Rebuild indexes as new generations

Current policy (2026-09-07): [ADR 0090](0090-mount-after-structural-validation-and-scrub-in-background.md)
implements structural normal startup followed by background content scrub.
It supersedes mandatory startup rehashing and the runtime's repeated DATA proof
when copying an already committed graph to a Recovery Checkpoint. Full demand
reads, disaster recovery and offline scrub retain independent content checks.
The original rationale and remaining contracts follow below.

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

When the complete Metadata Tier is absent, startup first installs the selected
checkpoint's Commit and immutable Metadata graph into the empty replacement
tier. It then performs the same verified pool scan and publishes either a fresh
Exact generation or, under `dependent-v1`, one coherently bound Exact/Similarity
pair before the recovered namespace becomes available.
