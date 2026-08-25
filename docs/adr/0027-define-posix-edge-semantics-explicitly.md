---
status: accepted
---

# Define POSIX edge semantics explicitly

fastdup preserves byte-exact case-sensitive names without Unicode normalization,
uses monotonically increasing never-reused 64-bit Inode IDs, and retains an
unlinked inode while live handles exist. After a crash no handle survives, so a
durably unlinked open orphan need not reappear. Samba handles its Unicode and
case policy above this byte-oriented namespace.

## Consequences

Hole punching creates HOLE extents; zero-range creates allocated FILL(0) DATA.
Default fallocate preserves existing DATA, replaces only holes with FILL(0),
and extends the logical file unless `KEEP_SIZE` is set. As a thin-provisioned
dedup appliance, fastdup records this allocation as logical metadata and makes
no physical-capacity promise. A later write may therefore still fail with
`ENOSPC`. `KEEP_SIZE` allocation beyond EOF has no retained physical effect;
inside EOF it converts holes to allocated FILL(0).

Collapse and insert range are byte-granular metadata splices. Collapse removes
the selected range and shifts the suffix left; insert adds a HOLE and shifts
the suffix right. Complete DATA, DATA_SLICE, FILL, and HOLE recipes move without
payload reads or new Chunk identities. The stock Linux FUSE kernel path rejects
both structural flags before sending a FUSE request, so they are available at
the shared POSIX seam but a standard `fallocate(2)` call on the FUSE mount still
returns `EOPNOTSUPP` for those two modes. `O_SYNC` and `O_DSYNC` share the
deliberate ten-second durability semantics of `fsync`. Version 1 uses relatime;
mtime, ctime, and explicitly set times remain ordinary atomic metadata
mutations.

Range clone is the exception to byte-copy write admission defined by
[ADR 0043](0043-expose-metadata-range-clones-for-veeam-fast-clone.md): an
accepted fully allocated immutable source range becomes one target metadata
mutation and allocates no frontend DATA pages. Same-file overlap and sparse
source cloning fail explicitly.

POSIX record locks are advisory, process-owned, and volatile. `F_GETLK`,
`F_SETLK`, and `F_SETLKW` use inclusive byte ranges, with `u64::MAX`
representing a range through EOF. Read locks conflict only with another owner's
write lock; write locks conflict with another owner's read or write lock.
Changing or unlocking part of an owner's range splits and merges that owner's
records as POSIX requires. `flush` and final release discard every record for
the supplied lock owner. Blocking acquisition waits outside the FUSE runtime's
blocking worker pool. Locks neither enter a Namespace Root nor make ordinary
reads and writes mandatory-lock operations, and no lock survives process loss
or remount. Version 1 advertises FUSE POSIX locks, not BSD `flock` locks.

Regular files and symbolic links may have multiple directory entries. A hard
link increments the shared inode link count and never duplicates Manifest or
symlink-target data; removing or replacing one name reclaims the inode only
after its final link and live reference disappear. Directory hardlinks are
rejected. Symbolic-link targets are byte-exact metadata of at most 4,096 bytes
and never enter the DATA ingest pipeline.

UID, GID, mode, atime, mtime, and ctime are atomic inode metadata. Only root may
change UID; an owner may select its request primary GID, while root may select
any GID. Ownership changes clear setuid and setgid. Explicit atime and mtime
accept nanosecond precision and advance ctime; successful nonempty content
mutations advance mtime and ctime with one wall-clock sample after admission.

Extended attributes are byte-exact, bounded copy-on-write inode metadata.
Supported names are `user.*`, `trusted.*`, `security.*`, and the two POSIX ACL
names. POSIX ACL values retain the Linux version-2 xattr wire form; an access
ACL projects its owner, mask/group, and other classes into the inode mode.
`chmod` updates those ACL class entries, and a directory default ACL is copied
atomically into each new child while its mask is intersected with the requested
creation mode. A default ACL replaces the process umask as POSIX requires.

The Linux `FS_IMMUTABLE_FL` inode flag is durable and is exposed through both
the classic flags ioctls and the `fsxattr` ioctl family used by XFS-compatible
tools. Setting or clearing it requires the root request context. While set, it
blocks content writes, writable/truncating opens, allocation and range-clone
targets, mode and xattr changes, unlink/rmdir/rename, and namespace changes
inside an immutable directory. The `user.immutable.until` attribute is stored
byte-exactly but is not a clock-driven policy: management software records the
retention time and explicitly sets or clears the independently enforced flag.
