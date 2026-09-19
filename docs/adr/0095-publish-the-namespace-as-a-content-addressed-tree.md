---
status: accepted
---

# Publish the Namespace as content-addressed record shards

Whole-payload Namespace encoding made every checkpoint O(Namespace), even for a
one-record change. Epoch 3 replaces that payload with independently decodable,
content-addressed record shards. Epoch-2 pools are refused rather than migrated;
they must be re-ingested.

## Durable shape

- An **Inode Shard** contains a key-ordered run of `DurableInode` records and
  their xattrs and POSIX metadata.
- An **Entry Shard** contains a key-ordered run of `NamespaceEntry` records.
- The **Namespace Root** contains counters, root metadata, and one ordered
  reference per shard: kind, first key, record count, and object identity.

Shard boundaries depend only on record keys: a record starts a shard when
`splitmix64(key) < u64::MAX / 1024`. A shard is additionally capped at 8,192
records and 4 MiB of payload. Equal key ranges therefore produce equal objects,
while the positional caps keep every object bounded even with large xattrs.

The writer publishes shards in graph order, makes all unique shards durable,
then publishes the root and Commit Record. Recovery, transition validation,
Metadata GC, Recovery Checkpoints, and offline scrub authenticate and validate
the complete selected graph. A shard is never an independently mutable or
visible Namespace partition.

## Evidence

At 120,000 entries, `encode_graph` fell from 29.3 ms to 13.7 ms. Changing one
inode replaces exactly one shard and leaves every other shard identity and
partition unchanged. The format tests cover that locality property, oversized
graphs, large-xattr byte bounds, missing/corrupt children, and the Metadata-GC
view.

## Remaining work

Publication reuses unchanged shard objects, but commit construction still walks
the complete Namespace. `begin_commit` copies the catalog and
`namespace_root_for_commit` rebuilds every durable inode and entry, losing the
dirty record set before encoding. Checkpoint CPU therefore remains O(Namespace),
with the operational warning expected around a few million files.

The next step is a commit-side mirror updated from the dirty delta. It may skip
unchanged ranges only after incremental summaries prove reachability, directory
link counts, and `(parent, name)` uniqueness. Recovery and offline scrub retain
the complete-graph proof, and fault injection must cover every new durable
writer boundary.
