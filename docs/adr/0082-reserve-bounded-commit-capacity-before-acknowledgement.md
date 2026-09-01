---
status: accepted
---

# Reserve bounded commit capacity before acknowledgement

Before changing the live Namespace, fastdup atomically claims the pessimistic
additional Metadata and DATA footprint of that mutation against the latest
physical observation. The request path performs no filesystem call and takes
no global capacity lock. A failed claim returns `ENOSPC` before the mutation is
visible; reads and capacity-reducing cleanup remain available.
Operations that resolve successfully without changing the Namespace acquire no
claim: opening an existing `O_CREAT` target, empty `setattr`, unchanged file
flags, and same-object rename cannot strand Active capacity without a Dirty
Commit that could retire it.

Creation claims remain attributed to their new Inode throughout the Active
Dirty Epoch. If the final name is removed by unlink, rmdir, or replacement
before the Inode enters a Frozen Commit Cut, its Metadata creation claim is
released immediately because that creation cannot contribute to a durable
Namespace Root. Forming the cut severs this Active attribution before later
mutations are admitted, so removing the name in the next epoch can never
release capacity still required by the Frozen generation. DATA claims are not
released by this rule: write-through publication may already have consumed
physical DATA capacity even when the new Inode later becomes an open orphan.

The v1 bound permanently withholds 64 MiB of Metadata headroom for one complete
Namespace Root, Commit WAL rotation, bounded Manifest path publication, and
cleanup. A Metadata-changing operation or discontinuous write additionally
claims 2 MiB of Metadata, covering the maximum v1 path-copy of one 1,024-entry
Manifest leaf and every possible 1,024-way ancestor below the `u64` logical-size
bound with margin.

Growing a file through `SetLength` is a Metadata-allocating thin allocation and
claims one complete path before changing the live size. Shrinking a file is
capacity-reducing cleanup and remains admitted against the protected floor.
An optional `relatime` update claims the same path and advances the Inode
Version; when the claim is unavailable, the read succeeds without changing
`atime`.

Strict EOF appends in one Active Dirty Epoch amortize that same bound over their
later Manifest complexity rather than over client syscall count. One inode-local
credit reserves four complete 2-MiB path claims for each 16 MiB of sequential
coverage. The 16-KiB minimum SeqCDC Chunk permits at most 1,025 appended Chunks
in that coverage including one drained boundary Chunk; four paths cover the
prior tail leaf, two 1,024-entry leaves, and one early close at a 64-MiB logical
window. A strict append from an empty base whose resulting file is no larger
than 8 MiB instead claims the exact 40-KiB upper bound of its single root leaf:
at the same minimum Chunk size it contains at most 513 extents including the
drained boundary, and its 64-byte header plus 64-byte entries, 4-KiB Metadata
envelope, and alignment fit that bound. Growing beyond 8 MiB claims the
remaining bytes of the ordinary four-path credit before extending 16-MiB
coverage. Nonempty committed bases retain the complete path claim because a
small logical size does not prove a shallow pre-existing tree. Flattening
mutations discard unused coverage, and every new Active Dirty Epoch starts with
no credit. This keeps tiny files as well as 128-KiB and 1-MiB representations of
the same sequential stream admission-equivalent without weakening the
per-mutation bound for random writes.

A write independently claims twice the larger of its payload and the 256-KiB
v1 maximum Chunk, plus 4 KiB of DATA. This covers write-through bytes, one
bounded boundary rechunk, raw fallback, record framing, alignment, and
sealed-Container publication.
Metadata-only clone and sparse allocation claim no DATA. Removal operations
use the protected floor rather than consuming ordinary admission headroom.

Claims accepted before a Namespace Commit Cut move atomically from Active to
that Frozen Commit token. A publication failure retains them. Durable Commit
completion marks them releasable, but they remain counted until a later
successful `statvfs` sample includes the physical writes. This prevents stale
five-second capacity data from authorizing the same bytes twice. A failed
sample closes new capacity admission without affecting reads, cleanup, or
already admitted recovery.

An open orphan can accept write-through DATA after its unlink is already
durable even though no later Namespace Commit can name that inode. When such an
Active epoch has no checkpointable mutation, its Metadata claim is released,
but its DATA claim moves to an uncheckpointed-completion bucket tagged with the
current observation epoch. Only a successful physical observation begun after
that tag releases the DATA claim. Multiple claims may be conservatively
coalesced behind the newest tag; this may delay reuse but can never authorize
the same physical bytes twice.
