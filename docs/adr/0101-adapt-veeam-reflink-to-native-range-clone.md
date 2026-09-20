---
status: accepted
---

# Adapt Veeam reflink to native Range Clone

The Veeam Service Container must use FastDup's native metadata-only Range Clone.
Linux consumes FICLONE/FICLONERANGE before an ordinary FUSE server can handle
those ioctls. The existing FUSE copy_file_range operation already reaches the
atomic Namespace CloneRange contract (ADRs 0003, 0027 and 0043).

Ship an x86_64 EL10 process adapter, loaded only by the container's Veeam
Transport and Environment services and their children. Translate a supported
reflink request to one raw copy_file_range syscall on the verified FastDup
mount. Verify the FUSE magic, mount ID and `fastdup` source at `/repository`;
retain an open mount descriptor and restrict both file descriptors to its device.
Other mounts retain their original filesystem identity and ioctl behavior.
Never emulate success by copying payload, split a request, round offsets, or
patch vendor executables. Reject requests above Linux MAX_RW_COUNT before
mutation. A successful result must cover the whole requested range.

Veeam capability discovery requires an XFS view in statfs/statvfs and a matching
xfs_info block geometry query. Scope that compatibility view to the managed
mount in these processes. Geometry uses the real filesystem block size and
capacity. The companion xfs_info delegates unrelated paths to vendor tooling.
This explicitly supersedes ADR 0100's prohibition of a filesystem magic view;
it does not turn FastDup into XFS or establish vendor support or hardened
immutability. There is no global loader preload.

Reject invalid pointers, descriptor modes, cross-filesystem requests, source
bounds, overflow and overlapping ranges. Range Clone must remove overwritten
target dirty payload from both the per-file overlay and global dirty ledger.
Readers and recovery continue to resolve the same committed backing references;
no durable format changes. Regression coverage includes a dirty destination,
neighbor preservation, bidirectional Copy-on-Write, checkpoint/recovery and
zero Data Pool I/O for committed-source cloning.

Package the adapter and geometry query outside the persistent guest rootfs and
bind them read-only into it. Preserve vendor state across upgrades. Emit clone
success byte/count and failure counters to syslog for end-to-end evidence.
Direct adapter tests alone do not qualify Veeam: an actual vendor clone action
and successful resulting backup verification are separate acceptance evidence.
Synchronous protection commits and retention authorization remain open gates.
