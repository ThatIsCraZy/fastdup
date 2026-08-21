---
status: accepted
---

# Expose metadata range clones for Veeam Fast Clone

fastdup supports one atomic `CloneRange` content mutation through the shared
POSIX dispatch seam. The operation snapshots a stable immutable source view,
overwrites an equal-length target range by reference, receives exactly one
target-inode mutation sequence, and enters the ordinary ten-second generation
commit window. It allocates no resident dirty payload and must never fall back
to reading and re-ingesting DATA while reporting clone success.

The FUSE adapter maps Linux `copy_file_range` with zero flags to this operation.
The SMB integration uses a small `vfs_fastdup` Samba module: it advertises
`FILE_SUPPORTS_BLOCK_REFCOUNTING` only when the complete integration is active,
maps `FSCTL_DUPLICATE_EXTENTS_TO_FILE` to `copy_file_range` on the mounted
fastdup descriptors, and implements the SMB-visible integrity-information
state. Stock Samba's `FICLONERANGE` path cannot reach a FUSE
`copy_file_range` callback, so capability spoofing or a generic Samba share
configuration is not an accepted substitute.

## Manifest Chunk slices

Veeam aligns clone ranges to 4 KiB or 64 KiB, while FastCDC boundaries are
content-defined. Manifest Leaf v2 therefore adds DATA_SLICE extent kind `4`.
It retains the existing 64-byte extent record and stores the full immutable
Chunk identity and length plus a checked byte offset. The logical slice must be
nonempty and `chunk_offset + logical_length <= chunk_length <= 256 KiB`.

Readers verify and decode the complete named Chunk before returning only the
slice. Dependency verification, recovery, successor proofs, and scrub use the
full `chunk_length`; allocation accounting uses the slice's `logical_length`.
Slicing an existing DATA or DATA_SLICE extent changes only Manifest metadata
and never creates a new logical Chunk identity or physical Location.

Manifest Leaf v1 remains readable. A leaf containing no DATA_SLICE continues
to encode as v1 for stable bytes; a leaf containing at least one slice encodes
as v2. Unknown versions and kind/version mismatches fail closed.

## Admission and edge semantics

The source handle must permit reads and the target handle must permit writes.
The complete source range must be inside EOF, fully allocated, and expressible
as verified immutable Manifest/FILL recipes. A source with an active mutable
epoch returns unsupported until snapshotting such an epoch can remain bounded;
the Veeam source file is expected to be an older committed restore point.
Sparse-source cloning is likewise deferred rather than silently materializing
zeroes.

Same-file overlapping ranges return unsupported; disjoint ranges are allowed
when the source view is stable. The FUSE primitive may extend a target for
ordinary POSIX callers, while the Samba Duplicate Extents adapter retains the
stricter SMB rule that Veeam pre-sizes the destination and a request never
extends it. Any unsupported recipe, invalid bound, short clone, or integrity
state mismatch fails without changing the target.

Metadata-only does not mean unbounded. A clone allocates no dirty DATA payload,
but every accepted operation can create Dirty Extent Map entries, successor
Manifest objects, WAL work, and CPU demand. Clone operations therefore remain
inside the bounded filesystem mutation-admission domain. A future dedicated
metadata admission lane may have a much larger budget than DATA ingest, but it
must still have explicit entry/byte limits and checkpoint-age backpressure.
The Samba adapter additionally caps one Duplicate Extents request and executes
it as exactly one filesystem `copy_file_range` operation; it never converts an
unsupported clone into a buffered copy.

Samba CLOSE is an apply fence, not a durability command. For each open target
handle, every accepted Integrity or Duplicate Extents operation must reach a
terminal applied-or-failed result before the next CLOSE hook runs. Because the
v1 adapter executes these operations synchronously, CLOSE cannot overtake a
successful clone. CLOSE does not add an implicit checkpoint or `fsync`: an
acknowledged successful mutation remains governed by the ordinary checkpoint
target and hard durability/admission window.

The v1 Integrity Information state is deliberately immutable:
`CHECKSUM_TYPE_NONE`, enforcement enabled, zero chunk size, and the configured
clone alignment as cluster size. SET succeeds only for NONE or UNCHANGED with
zero flags. This avoids an unauthenticated per-file state that could disagree
after restart.

## Crash and verification pairing

Writer admission validates a complete contiguous prepared recipe and the
checkpoint writer re-verifies every full Chunk dependency before publishing
new Manifest nodes. Recovery validates DATA_SLICE bounds, full Chunk length
consistency, and content availability before selecting the generation. Offline
Manifest scrub independently performs the same structural validation. The WAL
sync remains the sole visibility point, so a crash exposes either the complete
predecessor or the complete cloned successor, never a partially cloned range.

We do not claim Veeam compatibility until a real SMB 3.1.1 trace and Samba
protocol test confirm the Integrity FSCTL state machine, alignment, error
mapping, locks, rename/close ordering, and zero DATA-container I/O during a
synthetic full.
