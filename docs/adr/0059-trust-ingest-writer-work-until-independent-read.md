---
status: accepted
---

# Trust ingest writer work until an independent read

Owned ingest publication trusts work already completed by the in-process
writer. Stable SeqCDC Chunk IDs pass into the Container encoder. The encoder
returns the immutable sealed image together with the exact Locations, logical
lengths, codec counts, and Recovery Index coordinates it serialized. The
publisher does not decode that image, decompress its Zstd records, recompute
Record CRCs, or hash its logical Chunks again.

The publisher still writes a BUILDING Header, writes the body, seals the
Header, fixes the length, and compares the stored 4 KiB Header, one aligned
4 KiB midpoint block, and 4 KiB Footer with the retained image. File sync,
no-replace rename, and root sync remain unchanged. These samples detect wrong
length, gross misdirection, and corruption in the sampled ranges. They do not
turn the writer's RAM into an independent source of truth.

Ordinary reads, recovery, Exact Index rebuild, and scrub remain independent.
They read stored encoding records, verify CRCs, decode them, and recompute
Chunk IDs. They therefore detect a wrong carried identity, an encoder defect,
or unsampled storage corruption when they first consume the affected data.
The prototype accepts that detection boundary because repeating the same work
over the same resident bytes does not protect against defective RAM or a
shared writer defect.

Write-through ingest also trusts its first negative online-proof and Exact
lookup. Publication does not repeat those lookups for the same detached
Container work. An in-process singleflight table keyed by Chunk ID and logical
length assigns the missing Chunk to one publisher. Concurrent writers wait for
that publisher's active proof and reuse its Location. Claims are acquired in
key order to prevent cycles between partially overlapping Container batches;
a failed publisher releases its claims so a waiter can retry. This coordination
does not read storage or recompute content evidence.

Installing the resulting external extent does not read and hash the resident
bytes again. Each resident dirty extent carries the mutation sequence that
created it. Each Chunk carries the maximum sequence of all fragments that
formed it, including when one Container crosses a frozen-epoch boundary. The
POSIX layer installs a candidate only when its whole file range is still
covered and every covering resident or already external extent has a sequence
at or below the Chunk sequence. A later overlapping write therefore rejects
the stale candidate. A later non-overlapping write does not force unrelated
bytes through BLAKE3 again.

## Paired invariants and evidence

- The encoder constructs publication Locations from the same record plans and
  Index entries that it serializes. Container ID, generation, lengths, codec,
  record coordinates, and checksums stay paired in one consumed result.
- Storage publication compares all three samples before file sync and cannot
  return the writer evidence before root sync completes.
- A deliberately wrong prehashed Chunk identity may publish, but the first
  independent read must fail with `ChunkHashMismatch`. Recovery and scrub use
  the same independent format verifier.
- A detached write-through Container contains only Chunks whose first
  proof-and-Exact lookup was negative. Duplicate Chunk IDs inside that work are
  collapsed before encoding, and concurrent in-process publishers coalesce the
  same missing Chunk through singleflight ownership.
- Externalization requires complete range coverage by extents no newer than
  the candidate Chunk. Tests cover both the overlapping-write rejection and
  the non-overlapping-write acceptance without calling the external source's
  byte or segment matching methods.
- SMB SingleStream and MultiStream benchmarks remain the performance and
  concurrency acceptance tests.

## Consequences

The hot path no longer performs immediate Zstd decompression, per-Chunk BLAKE3,
Record CRC verification, Recovery Index comparison, or a second negative
Exact lookup. Publication now proves ordered durable placement of the writer's
image, not independent correctness of every byte in that image. Independent
read and scrub verification retain end-to-end stored-data checking.
