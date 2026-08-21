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

The Container writer persists one Building Header, the complete bounded Body
and Footer in one positional write, and the final Sealed Header in that order.
It then fixes length, rereads and verifies the complete image, synchronizes the
file, publishes by no-replace rename, and synchronizes the directory. Splitting
the Body into format-page-sized syscalls provides no additional crash boundary:
the Building Header already makes every partial Body unpublishable. The
synchronous Storage seam and bounded publisher worker concurrency remain the v1
kernel-I/O adapter; `io_uring` requires a measured multi-object batching seam and
the identical fault matrix before adoption.

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

## Checkpoint recipe adoption

The Frozen Commit Cut retains process-local evidence for every externalized
range. A Container-backed source contributes the complete Chunk ID and length;
a FILL source contributes its value and length. The checkpoint may translate
that evidence directly into logical Manifest extents without reading, hashing,
or chunking those bytes again. Physical Locations remain absent from the
Manifest and are not part of this capability.

Recipe adoption is paired at three boundaries:

- Externalization compares the complete current bytes with the independently
  verified source before resident payload is released.
- The frozen reader exposes a content-addressed Chunk recipe only while the
  external extent still covers that complete source. A later partial overwrite
  splits the source and makes every remaining Chunk fragment ineligible; the
  checkpoint re-reads and FastCDCs the complete resulting gap. FILL is the sole
  recipe that may be clipped because every subrange has the same exact value.
- Generation publication treats every adopted Chunk ID as a changed DATA
  dependency. The normal incremental graph verifier must prove an exact durable
  Location before the Commit-Log sync can make the generation recoverable.

The recipe seam is an optimization proof, not durability or visibility. A
missing recipe falls back to the ordinary byte-exact planner. Conflicting
lengths, overlapping/out-of-range evidence, or an oversized Chunk fail closed.
Checkpoint metrics separate `recipe_reuse_bytes` from
`checkpoint_rechunk_bytes`, so regressions cannot hide behind Exact-hit counts.

Forming a Frozen Commit Cut briefly takes the Namespace mutation-admission write
fence. It waits for every mutation that already holds an admission permit to
finish both its live mutation and its Ingest-Lane observer, then freezes the
generation and immediately releases the fence. Container I/O, reduction,
Manifest planning, and metadata publication never run under that global fence.
This ordering prevents the cut from overtaking an admitted write while still
allowing the next Active Dirty Epoch to advance throughout persistence.

After the cut, every Ingest Lane drains all complete FastCDC Chunks even when
its pending payload is below the normal Container-fill threshold. Publication
uses the existing immutable Container writer and reader verification. The
resulting recipes may attach to the Frozen Cut, the next Active Dirty Epoch, or
both when one Container crosses the boundary; the late attachment never changes
frozen bytes or ordering. The lane retains at most one boundary Chunk plus the
incomplete FastCDC suffix (at most twice the 256-KiB maximum Chunk size). A
drain failure aborts the checkpoint before metadata visibility and leaves
resident bytes authoritative for retry.

## Consequences

The normal sequential checkpoint adopts complete write-through recipes and
runs bounded FastCDC only over the incomplete suffix and invalidated boundary
Chunks. One chunking implementation remains authoritative: recipes originated
from that same versioned write-through FastCDC profile, while every fallback
range goes through the checkpoint implementation. The 512-MiB pressure trigger
measures only resident active Dirty DATA; external range recipes and already
durable payloads do not consume that byte budget. The streaming stage remains
independently bounded. Under ADR 0050, eight registered lanes and one serialized
overflow lane share the asserted 384-MiB process-local pipeline budget with at
most two detached 32-MiB Container payloads. Inactive state is evicted without
ever evicting an in-flight lane. The overflow lane may lose CDC continuity when
different inodes alternate, but never mixes their bytes.

The process may crash with durable orphan Containers or Exact-Index entries that
are not reachable from a Commit Record. They are never visible file versions;
later exact reuse is safe only after complete Container verification, and GC may
eventually reclaim unreachable objects. A successful generation commit retires
only Container trigger evidence captured before its atomic cut, never Containers
published for the next active epoch.

Concurrent generation and lane ordering are fixed by
[ADR 0041](0041-overlap-one-frozen-commit-with-bounded-ingest-lanes.md).
