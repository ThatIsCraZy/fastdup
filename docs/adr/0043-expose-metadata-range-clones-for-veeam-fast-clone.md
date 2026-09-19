---
status: accepted
---

# Expose metadata range clones for Veeam Fast Clone

`CloneRange` is one atomic target mutation that reuses a stable immutable source
recipe. It allocates no dirty DATA payload, performs no buffered fallback, gets
one target mutation sequence, and follows the ordinary Commit durability window.
FUSE exposes it through `copy_file_range`; Samba maps
`FSCTL_DUPLICATE_EXTENTS_TO_FILE` through `vfs_fastdup` only when the complete
integration is active.

## Manifest slices

Manifest Leaf v2 `DATA_SLICE` names the full immutable Chunk identity and length
plus a checked byte offset. Readers decode and verify the complete Chunk before
returning the slice; allocation counts only the slice length. Writer, recovery,
successor proof, and scrub validate full-Chunk bounds and dependencies. Slicing
creates no Chunk or Location.

## Admission

- Source must be readable, stable, fully allocated, inside EOF, and expressible
  as verified Manifest/FILL recipes. Mutable or sparse sources are unsupported.
- Target must be writable. Same-file overlap, arithmetic overflow, policy
  mismatch, unsupported recipes, and short native clones fail without mutation.
- Arbitrary byte offsets and positive byte lengths are accepted; requests are
  never rounded, split, padded, or converted to buffered copies.
- POSIX may extend the target. SMB requires a pre-sized destination and performs
  exactly one native `copy_file_range` operation.
- Clone Metadata work remains under bounded mutation and capacity admission.

## SMB Integrity and ordering

The inode xattr `user.fastdup.smb-integrity.v1` stores the two-byte little-endian
NONE/CRC32/CRC64 policy. Missing means NONE; legacy `0100` and `0200` both mean
native integrity enabled. Unknown, malformed, disabled-enforcement, or
source/target-mismatched policies fail closed. This policy selects SMB-visible
native integrity; it is not a second checksum stream or content authority.

Samba dispatch permits at most eight active and 64 accepted unfinished clones
per smbd. Workers own duplicated descriptors; the event thread owns Samba
handles and completion ordering. Conflicting file identities serialize by
arrival, independent files may overlap, and source and target CLOSE wait until
their accepted prefixes are terminal. CLOSE is an apply fence, not a durability
or `fsync` promise.

## Proof and qualification

The checkpoint writer verifies every introduced Chunk before child-first
Manifest publication; WAL sync is the only visibility point. Recovery and scrub
independently validate the complete tree, so interruption exposes the old or
complete cloned range with neighboring bytes unchanged.

Implementation and fault coverage do not establish Veeam compatibility. The
open gate is a real SMB 3.1.1 synthetic-full qualification covering simultaneous
IOCTLs, Integrity interleaving, aliases/overlap, CLOSE and disconnect races,
error mapping, exact neighboring bytes, and zero DATA-container I/O. See the
[qualification record](../testing/clone-optimization-2026-09-09.md).
