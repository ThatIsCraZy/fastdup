---
status: accepted
---

# Cache read-only FUSE handles and invalidate DATA explicitly

The v1 kernel-cache policy uses cached I/O only for regular files opened
read-only. Those opens return `FOPEN_KEEP_CACHE`, so clean pages and kernel
readahead can serve repeated reads and read-only shared mappings across handle
lifetimes. Any handle that can write returns `FOPEN_DIRECT_IO`. FUSE writeback
remains disabled, and `FUSE_DIRECT_IO_ALLOW_MMAP` is not negotiated. Shared
writable mappings are therefore rejected while read-only mappings remain
available.

The session negotiates `FUSE_EXPLICIT_INVAL_DATA` when the kernel offers it.
Once a regular inode has returned a cacheable read-only handle, the inode is
marked as potentially DATA-cached until its in-memory lifetime ends. After a
successful userspace content mutation of such an inode, the FUSE adapter sends
`FUSE_NOTIFY_INVAL_INODE` before returning the operation's reply. Mutations of
an inode that has never returned a cacheable handle skip the notification;
there can be no kernel DATA pages for that inode to invalidate. Ordinary writes
invalidate the actual accepted range, including the placement selected under
`O_APPEND`; `copy_file_range`, hole punch, zero range, and extending thin
allocation invalidate their target ranges. Collapse and insert invalidate from
the splice offset to EOF. Truncate and successful `O_TRUNC` invalidate the
complete inode. A zero-byte write sends no notification. An offset or length
that cannot be represented by the signed FUSE notification fields falls back
to complete-inode invalidation.

Attribute and entry TTLs remain zero in v1. Namespace-only mutations therefore
need no unsolicited dentry invalidation: the next lookup or attribute request
already crosses the FUSE boundary. Rename and unlink do not change the bytes
observed through an existing inode or open orphan, so they do not invalidate
DATA pages.

The notification channel is installed by the FUSE session before `init` and
before any request dispatch. Notification planning and delivery live entirely
in the kernel adapter. The synchronous `Namespace::dispatch` seam, mutation
admission, Ingest Lanes, SeqCDC, Exact lookup, encoding, and checkpoint workers
do not acquire a cache lock or perform a notification syscall. The only added
namespace reply datum is the actual write offset already selected while the
inode write lock is held; this avoids a second lookup and permits exact append
invalidation. The cache-exposure marker is one inode-local atomic: cacheable
open performs a release store, and the ordinary write path carries one acquire
load out with its existing reply. It adds no catalog lookup, cache lock, or
telemetry update to the Write Hot Loop.

## Evidence

Linux defines direct I/O as bypassing the page cache and disabling shared mmap
unless `FUSE_DIRECT_IO_ALLOW_MMAP` is negotiated. Cached write-through mode
supports readahead and mappings while keeping writes consistent:
<https://docs.kernel.org/filesystems/fuse/fuse-io.html>.

The FUSE protocol defines `FOPEN_KEEP_CACHE`, `FUSE_EXPLICIT_INVAL_DATA`, and
`FUSE_NOTIFY_INVAL_INODE`; explicit invalidation also expires inode attributes:
<https://github.com/torvalds/linux/blob/master/include/uapi/linux/fuse.h> and
<https://github.com/torvalds/linux/blob/master/fs/fuse/inode.c>.

The release-mode `/dev/fuse` harness warms read-only pages through an
independent descriptor, overwrites across a page boundary through a direct
writer, appends, truncates, punches and zeroes ranges, and immediately rereads
the affected boundaries. Rename, unlink, and a later open-orphan write retain
the same coherent cached inode. The harness also faults a read-only shared
mapping before and after an overwrite and confirms that a shared writable
mapping on a writable handle is rejected.

The adapter boundary test records kernel notifications and proves that a
Write-only inode emits none before its first cacheable open, then still emits
the exact accepted range after that reader has closed. An ABBA SMB SingleStream
run against the immediately preceding binary measured 5.7% higher aggregate
write throughput, 2.0% higher throughput for the warm second and third copies,
3.3% less daemon CPU, and 9.1% lower per-run p99/max latency after notification
elision. The reports keep Swap at zero and use separate XFS Metadata and DATA
devices.

## Consequences

Repeated restores and shared readers can use the kernel's clean page cache and
readahead in addition to fastdup's bounded verified userspace caches. Writes
retain the existing direct, acknowledged-mutation path and cannot become hidden
kernel-dirty state. Each successful content mutation of a cache-exposed inode
adds one bounded allocation-free notification at the FUSE edge before
acknowledgement; never-exposed Write-only inodes pay only one atomic load. This
adds no cache lock, telemetry atomic, or work to CPU reduction loops. Enabling
cached writable handles, positive metadata TTLs, or
writeback would require a separate decision and new crash, memory-pressure,
and coherence evidence.
