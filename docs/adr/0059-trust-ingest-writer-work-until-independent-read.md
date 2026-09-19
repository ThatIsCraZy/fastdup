---
status: accepted
---

# Trust ingest writer work until an independent read

Owned ingest publication trusts the in-process writer's result. Stable SeqCDC
Chunk IDs enter the encoder; it returns one sealed immutable image plus the
exact Locations, lengths, codec counts and Recovery Index coordinates it
serialized. The publisher does not decode, decompress, rehash or recompute CRCs
over the same resident bytes.

Publication writes the BUILDING Header, body, sealed Header and final length,
then compares the stored 4-KiB Header, one aligned midpoint block and 4-KiB
Footer with the retained image. File sync, no-replace rename and root sync are
unchanged. These samples prove ordered placement and catch gross misdirection;
they are not independent content verification.

Ordinary reads, recovery, Exact rebuild and scrub independently decode stored
records, validate CRCs and recompute Chunk IDs. A wrong carried identity,
encoder defect or unsampled corruption must fail at that first independent
boundary.

## Ingest reuse and externalization

Write-through ingest performs one online-proof and Exact lookup. A process-local
singleflight table keyed by Chunk ID and logical length assigns each miss to one
publisher; waiters reuse its active Location. Claims are acquired in key order,
duplicates within one batch collapse, and failure releases claims for retry.

A proof hit is only a preference for current Exact selection, never sufficient
physical evidence by itself. Successful selection promotes it to Active;
checkpoint-only selection promotes it to Frozen. Selection preserves full
identity, length, proof bounds, reuse origin, generation ownership and GC
admission. Independent demand, recovery and scrub verification cannot be
bypassed.

The resulting publication may construct a shared virtual read recipe directly
from writer-carried Locations. Range-local external extents retain their Chunk
recipe and view coordinates. The first independent live read uses the normal
verified Record plan, cache and Base resolver, including dependent Targets not
yet visible through asynchronous Exact activation. Retired snapshots use the
verified scan fallback and dormant sources hold no long-lived operation pin.

Externalization also avoids rereading resident file bytes. Every dirty extent
carries its mutation sequence; a Chunk carries the maximum sequence of all
forming fragments, including across a frozen boundary. Installation requires
complete range coverage by resident or external extents no newer than that
Chunk. A later overlapping write rejects the candidate; an unrelated write
does not force another hash. Overlapping candidates retain input order, and
valid Frozen recipes may be attached independently of Active acceptance.

Active and Frozen Generation Proofs store each identity and Exact Location once
in a contiguous arena. Hash buckets contain bounded ordinals and compare full
Chunk identity and logical length. Freeze moves ownership; successful completion
sorts the arena into Historical policy order. The combined 65,536-proof limit
and publication-claim bounds include actual arena and table capacities.

## Verification contract

- Encoder Locations derive from the exact serialized plans and Index entries.
- Writer evidence is returned only after all samples and root sync succeed.
- An intentionally wrong prehashed identity may publish, but its first
  independent read must fail with `ChunkHashMismatch`.
- Externalization requires complete sequence-safe coverage.
- SMB SingleStream and MultiStream remain performance and concurrency gates.

Online GC applies the same trust boundary after its one independent victim
proof. Removal rereads only the sealed Header/Footer identity, generation and
length; it does not reverify payloads about to be destroyed or resolve Bases.
Replacement-before-deletion, RETIRING activation and pin drain provide liveness
safety. The restart finalizer retains full cold-path verification because no
in-process proof survives restart.
