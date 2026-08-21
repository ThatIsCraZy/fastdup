---
status: accepted
---

# Partition Exact Index compaction into key-disjoint Run families

Exact Index compaction produces one logical **Run family** containing one or
more immutable physical Runs. A family is one precedence generation and one
compaction input. Its physical partitions are ordered, complete, and strictly
disjoint by Chunk ID. No Chunk ID may cross a partition boundary.

The output target is 262,144 entries per physical Run. The writer may exceed
that target to keep all transitions for one Chunk ID together, but every Run
must remain within the Exact Index Run v1 one-GiB object bound. An unsplittable
hot key that exceeds that format bound is a checked failure, not a partial
family.

The first partition uses `family_generation`; later partitions use contiguous
Run generations `family_generation + partition_ordinal`. Run-Set v2 pins the
family generation, partition ordinal, partition count, and exact key bounds.
Singleton families retain the byte-identical Run-Set v1 representation.

## Why

A single Run v1 cannot grow beyond one GiB. Treating partitions as independent
Runs would also make lookup and compaction work grow with physical object
count, consume the 64-way precedence budget, and let a later partition appear
newer than another family merely because it has a larger physical generation.

Family precedence avoids all three problems. Lookup chooses at most one
physical partition per family from authenticated key bounds. K-way compaction
keeps one page cursor per source family and opens its partitions sequentially.
The operational bound is therefore 64 active families, not 64 physical Runs.

## Publication and recovery

Compaction performs one complete verified merge pass to determine canonical
partition boundaries, then a second verified pass that streams every output.
Each partition is reread, fully audited, file-synchronized, and published by
no-replace rename. One directory sync after the complete family is the family
publication point. Published but unselected partitions are harmless orphans.

The Run Set is the atomic selection unit. Its writer, recovery reader, and
offline activation audit reject missing ordinals, inconsistent counts or
generations, overlapping/equal adjacent Chunk-ID bounds, or a missing/corrupt
physical dependency. The activation-log slot sync remains the only point at
which a new family becomes visible to lookup. A crash therefore selects the
old Run Set or the complete new Run Set, never a subset of one family.

## Consequences

- The checkpoint policy pins `l0-runs-v2`, fan-in four, and the 262,144-entry
  partition target. Changing this target requires a new policy identity.
- Compaction selects complete same-level families and applies transition
  precedence by `family_generation`, never physical partition generation.
- A range lookup reads at most one Run per active family and still returns only
  unverified Location candidates.
- Physical Run count is observable but is not an admission or lookup bound.
- Future Bloom/Fuse indexes may summarize partitions, but cannot weaken the
  authenticated range or complete-family invariants.

