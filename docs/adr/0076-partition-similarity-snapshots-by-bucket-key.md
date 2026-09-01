---
status: accepted
---

# Partition Similarity snapshots by complete BucketKey ranges

This record was originally committed under the already occupied number 0046.
It was renumbered to 0076 during the 2026-08-27 ADR audit; the decision and its
chronology are unchanged.

One logical Similarity generation is represented by one or more immutable
physical Runs plus one small family manifest. Physical partitions own complete,
strictly disjoint `BucketKey` ranges. A bucket is never split. The family
manifest pins every Run hash, length, cardinality, Chunk-ID bounds, BucketKey
bounds, and partition ordinal.

Representative selection remains global: the builder first retains the 64
smallest complete Chunk IDs for every bucket and only then chooses partition
boundaries. Each partition carries the Chunk fingerprint entries referenced by
its bucket range. An entry may consequently occur in up to four partitions,
but a query maps each of its four BucketKeys to at most one partition and still
examines at most 256 representatives in total.

The family manifest is the publication point. Writers fully audit and sync all
physical Runs before publishing and syncing the manifest. Recovery and offline
scrub reject missing partitions, ordinal/count disagreement, overlapping
BucketKey ranges, descriptor disagreement, or corrupt Runs. Physical Runs left
without a manifest by a crash are unselected rebuild artifacts.

## Why

Chunk-ID partitioning would require every Similarity query to probe every
physical Run, making lookup work grow with pool size. Independent per-partition
sampling would also retain 64 representatives per partition rather than the
versioned global 64-representative policy.

BucketKey partitioning preserves bounded query work and lets one logical pool
snapshot exceed the one-GiB physical Run limit. Duplicating derived fingerprint
metadata is preferable to duplicating payload bytes or adding mutable cross-Run
pointers. Similarity state remains rebuildable acceleration and carries no
content, Location, or liveness authority.

## Consequences

- The production partition target is 262,144 retained bucket references and a
  boundary may move only between complete BucketKeys.
- Partition Entry ordinals are local; query merging deduplicates candidates by
  complete Chunk ID rather than comparing ordinals across Runs.
- Singleton families use the same manifest protocol as multi-part families so
  recovery has one atomic selection rule for externally built snapshots.
- Direct singleton Run publication is unsupported; singleton and multipart
  snapshots both use the family-manifest publication point.
- A future change to partition target, sampling policy, or key geometry requires
  a new policy/profile decision, not an in-place reinterpretation.
