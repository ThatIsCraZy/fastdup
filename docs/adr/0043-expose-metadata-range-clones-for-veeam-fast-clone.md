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

Veeam aligns clone ranges to 4 KiB or 64 KiB, while SeqCDC boundaries are
content-defined. Manifest Leaf v2 therefore adds DATA_SLICE extent kind `4`.
It retains the existing 64-byte extent record and stores the full immutable
Chunk identity and length plus a checked byte offset. The logical slice must be
nonempty and `chunk_offset + logical_length <= chunk_length <= 256 KiB`.

Readers verify and decode the complete named Chunk before returning only the
slice. Dependency verification, recovery, successor proofs, and scrub use the
full `chunk_length`; allocation accounting uses the slice's `logical_length`.
Slicing an existing DATA or DATA_SLICE extent changes only Manifest metadata
and never creates a new logical Chunk identity or physical Location.

Manifest writers emit and readers accept only Leaf v2, including leaves
without DATA_SLICE. ADR 0074 removed the earlier v1 compatibility path.
Unknown versions and invalid extent kinds fail closed.

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

The original fixed NONE Integrity Information state rejected Veeam's request
to enable integrity with `STATUS_INVALID_DEVICE_REQUEST`. As of the 0.6.4-2
hotfix, NONE, CRC32, CRC64 and UNCHANGED requests with zero flags are supported.
Following the ReFS-v2 wire convention, either CRC request selects the filesystem's
native integrity mechanism; GET uses the CRC32 identifier for 4 KiB geometry and
CRC64 otherwise. fastdup's existing verified reads provide the integrity check;
this does not introduce a second ReFS checksum stream or change stored DATA.
NONE changes SMB-visible policy, never the mandatory native corruption checks.
Disabling checksum enforcement remains unsupported.

The selected policy is an ordinary inode xattr, `user.fastdup.smb-integrity.v1`,
encoded as exactly two little-endian algorithm bytes (0, 1 or 2). A missing xattr
means NONE, preserving existing repositories. Both GET and SET reject malformed
attributes instead of inventing a state. Reserved request bytes are ignored;
unknown algorithms and unsupported flags fail before mutation. SET requires a
writable share and a handle with data-write or write-attributes permission,
including directory and attribute-only handles. UNCHANGED does not write back a
possibly stale value. Rename and reopen retain the inode policy; Duplicate
Extents rejects differing source/target policies before cloning.

This is opaque POSIX metadata, not a new durable storage invariant or a checksum
authority. Atomic xattr replacement enters the existing mutation/checkpoint
path; recovery and offline scrub preserve and verify its containing metadata
objects using their existing byte-exact xattr contract. No separate state file,
per-open cache, privileged xattr write or format migration is introduced.
The SMB adapter validates the policy when interpreting it, even after recovery.
Directory policy is retained on that directory; child inheritance is not added
by this hotfix. Veeam's explicit per-file SET is the qualified operation.

References: [MS-FSCC SET request](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/a4517cd5-3f5a-4058-a457-bcff2baac011),
[Veeam KB4381](https://www.veeam.com/kb4381).

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
