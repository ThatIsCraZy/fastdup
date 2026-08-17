---
status: accepted
---

# Prepublish streaming Containers and coalesce generation commits

Long sequential writes pass through bounded process-local FastCDC Ingest Lanes
before the Namespace generation is frozen. Each registered lane retains at most
one adaptive Container target plus its bounded CDC suffix. Complete Chunks are
grouped into 512-KiB Compression Regions, encoded by the existing bounded worker
pool, and the resulting immutable Container is fully verified, synchronized,
published, and directory-synchronized immediately. Only then may its verified
Locations be published into the rebuildable Exact Index.

Container publication makes DATA durable but does not make a file version
recoverable. Manifest objects, the Namespace Root, and the final Commit-Log sync
remain the sole transactional visibility path. After a stable FastCDC Chunk is
published and reread successfully, its resident dirty payload may be replaced
atomically by a range-local source containing its verified physical Location.
Exact hits receive the same treatment only after a bounded index lookup has
been paired with the immutable Container record. FILL Chunks use a compact
value-and-length source. Live reads merge these sources with the committed
Manifest, the incomplete resident CDC suffix, and later writes.

Externalization is acceleration, not a new visibility boundary. Before dropping
resident bytes the Namespace compares the complete current live range with the
source's verified content identity, checks that no later hole covers the range,
and verifies that the source belongs to the active mutation epoch. Container
sources use their BLAKE3 Chunk identity so this check does not turn every Chunk
into another random data-tier read; sources without a content identity fall back
to a complete verified reread. Any mismatch, I/O failure, concurrent commit cut,
allocation failure, random/discontinuous write, or staging failure retains the
resident Dirty Extent unchanged.

The scheduler starts a generation commit when any of these conditions holds:

- the first published-but-uncommitted Container has waited 500 milliseconds;
- eight published-but-uncommitted Containers exist;
- the oldest admitted mutation reaches two seconds; or
- the existing 512-MiB active Dirty DATA fallback is reached.

At five seconds it also closes mutation admission until durable progress catches
up. The externally guaranteed durability window remains ten seconds until fault
and workload evidence justifies changing it. Exact/FILL-only streams and partial
Containers always depend on the age trigger, so Container fill never replaces
the time bound.

## Consequences

The normal checkpoint re-runs bounded FastCDC over its frozen logical ranges,
reading externalized ranges through their verified Container sources. The
prepublished DATA resolves as Exact Hits and is not recompressed or rewritten.
This deliberately keeps one chunking implementation authoritative for Manifest
construction while moving compression and data-tier synchronization out of the
metadata commit critical path. The 512-MiB pressure trigger now measures only
resident active Dirty DATA; external range recipes and already durable payloads
do not consume that byte budget. The streaming stage remains independently
bounded to one Container target plus one FastCDC suffix per lane. Format-v1
admits ten registered lanes and one serialized overflow lane under an asserted
384-MiB process-local pipeline budget, so inactive
state is evicted without ever evicting an in-flight lane. The overflow lane may
lose CDC continuity when different inodes alternate, but never mixes their
bytes.

The process may crash with durable orphan Containers or Exact-Index entries that
are not reachable from a Commit Record. They are never visible file versions;
later exact reuse is safe only after complete Container verification, and GC may
eventually reclaim unreachable objects. A successful generation commit retires
only Container trigger evidence captured before its atomic cut, never Containers
published for the next active epoch.

Concurrent generation and lane ordering are fixed by
[ADR 0041](0041-overlap-one-frozen-commit-with-bounded-ingest-lanes.md).
