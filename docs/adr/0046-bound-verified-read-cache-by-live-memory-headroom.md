---
status: accepted
---

# Own repository reads through one unified cache

One process-wide engine owns identity, admission, replacement, and accounting
for every reusable Metadata, DATA, Index, proof, descriptor, range, and handle
representation. Typed namespaces are views into that owner, not private caches
or quotas. Generation pins, Dirty DATA, codec work, in-flight I/O, and serialized
writer state remain separately bounded required working state.

Cached content is acceleration only. It never selects a generation, pins a
root, proves liveness or durability, authorizes a Location, or substitutes for
independent recovery, scrub, collision, or deletion verification.

## Memory policy

The common target follows effective host/cgroup headroom with an 8% operating
reserve. Sampling failure or process Swap disables admission and reclaims
evictable residents. Shared backing is charged once; readers may retain views
after eviction, and that memory reduces future sampled headroom.

Weighted replacement prefers reused DATA, proofs, and descriptors. Admission is
bounded and may be declined without affecting correctness. Current class rules:

- Verified DATA occupies at most 20% of the evictable target and cannot displace
  protected Exact acceleration.
- Protected Exact pages and page bounds occupy at most 70%; they remain
  reclaimable under their class and host pressure.
- Active-Run `ExactMembership` filters are the sole non-evictable class. They
  remain charged by the common owner but outside the evictable target until the
  Run retires. Construction failure is false-negative-safe and retryable.

An optional background worker rebuilds missing Exact membership/page bounds and
warms bounded pages only after startup scrub, during frontend idleness, with
open mutation admission and the same cache and I/O limits.

## Read intent

- **Demand** uses hits and may admit validated representations.
- **Scan** uses hits and may fill only eviction-free headroom; it never chooses
  a victim or exceeds a class ceiling.
- **Independent** bypasses resident and in-flight reuse and admits nothing.

Verification-only Scan reads retain neither payload nor Location evidence.
Demand reads may retain payload plus compact evidence for a fully verified,
currently eligible physical Location. Location evidence is keyed by complete
logical and physical identity and is rechecked against the newest Exact
transition before reuse. A completed scrub may offer such evidence only after
leaving its Independent scope; later scrub remains Independent.

## Writer evidence

Validated Exact pages, Metadata images, storage length heads, and logical Chunk
payloads may enter the common cache after their owning publication and required
syncs succeed. They do not gain liveness or physical-source authority. Ambiguous
activation I/O revokes the serialized writer cursor; the next operation
reconstructs stored selection before reuse. Existing-name collisions, startup,
recovery, and scrub retain independent stored-byte validation.

Online GC may reuse cached ordinary inputs and an installed Exact generation
only while it matches the exclusive writer's last synchronized activation.
Final victim proof, retirement barriers, invalidation, and deletion ordering are
independent of cache residency.

## Direct I/O boundary

All repository file-content I/O uses `O_DIRECT`; regular FUSE handles use
`FOPEN_DIRECT_IO`. The supported XFS profile requires 4 KiB geometry and has no
buffered fallback. Generic unaligned objects use the
[aligned storage envelope](../specs/aligned-storage-v1.md): checked logical
length heads, content, and physical padding. Padding is not logical format data.
Repository Format Epoch compatibility follows ADR 0071.

This controls repository file-content caching, not kernel code, filesystem
metadata, or device/controller caches. File/directory sync order and the stable
storage requirements of ADR 0028 remain unchanged.

Qualification and limits are recorded in the
[unified-cache report](../testing/unified-read-cache-2026-09-12.md),
[Metadata I/O report](../testing/metadata-read-amplification-2026-09-12.md),
[Location evidence report](../testing/location-proof-reuse-2026-09-13.md), and
[remaining I/O report](../testing/remaining-io-amplification-2026-09-13.md).
