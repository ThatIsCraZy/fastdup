# Veeam synthetic full over SMB and fastdup

Status: research note, not an accepted architecture decision
Last reviewed: 2026-08-21

## Question

Which operation reaches a Linux filesystem when Veeam Backup & Replication
builds a synthetic full on an SMB repository, and which POSIX/FUSE operations
must fastdup provide for that operation to remain metadata-only?

This note covers Veeam Backup & Replication 13.1.1.18 and the current Samba,
Linux, and libfuse implementations reviewed on the date above. Exact wire
ordering and the request values emitted by Veeam remain to be confirmed with a
Veeam packet trace; the public Veeam contract and the server-side paths are
unambiguous.

## Conclusion

Veeam SMB Fast Clone requires `FSCTL_DUPLICATE_EXTENTS_TO_FILE` **and**
`FSCTL_SET_INTEGRITY_INFORMATION`. `FSCTL_SRV_COPYCHUNK` is useful server-side
copy offload, but it is neither Veeam's advertised Fast Clone interface nor a
guarantee that data remains shared. Veeam uses Fast Clone for synthetic full
and GFS creation, merge, reverse-incremental transformation, and compact
operations. [Veeam Fast Clone](https://helpcenter.veeam.com/docs/vbr/userguide/backup_repository_block_cloning.html)

Stock Samba over a FUSE mount is not sufficient:

1. Current Samba handles `FSCTL_DUPLICATE_EXTENTS_TO_FILE` through its VFS
   offload-read/offload-write seam. Since Samba 4.22, the generic default
   implementation issues Linux `FICLONERANGE`.
2. Linux consumes `FICLONERANGE` in the generic ioctl layer and calls the
   filesystem's `remap_file_range` operation.
3. FUSE implements `copy_file_range`, `fallocate`, and a generic userspace
   `ioctl` callback, but its regular-file operations do not implement
   `remap_file_range`. Consequently, `FICLONERANGE` fails before a FUSE ioctl
   callback could emulate it.
4. Current Samba has no built-in server implementation of
   `FSCTL_SET_INTEGRITY_INFORMATION`; unknown filesystem FSCTLs are offered to
   the Samba VFS `fsctl` hook and otherwise become
   `STATUS_INVALID_DEVICE_REQUEST`.

The maintainable integration is therefore a small **fastdup Samba VFS module**:

- advertise `FILE_SUPPORTS_BLOCK_REFCOUNTING` only while the complete clone
  and integrity contract is active;
- handle the existing Samba duplicate-extents offload seam and invoke
  `copy_file_range(src_fd, ..., dst_fd, ...)` on fastdup file descriptors;
- implement fastdup's FUSE low-level `copy_file_range` request as one atomic,
  tree-native range clone and return the complete requested length or an
  error; and
- implement and persist the Windows integrity state through the Samba VFS
  `fsctl` hook.

This use of `copy_file_range` is an adapter implementation choice. It does not
change the SMB-visible contract: duplicate extents must remain an atomic CoW
clone, not a possibly short physical copy.

## Veeam's public contract

For a shared-folder repository, Veeam explicitly requires
`FSCTL_DUPLICATE_EXTENTS_TO_FILE` and `FSCTL_SET_INTEGRITY_INFORMATION`.
Windows-hosted shares additionally require SMB 3.1.1 and ReFS 3.1 or later;
the job infrastructure requires a supported Windows gateway or mount server.
Veeam automatically aligns its data blocks to either 4 KiB or 64 KiB according
to the volume/share configuration. [Veeam Fast Clone requirements](https://helpcenter.veeam.com/docs/vbr/userguide/backup_repository_block_cloning.html),
[Veeam SMB repository configuration](https://helpcenter.veeam.com/docs/vbr/userguide/smb_repository_repository.html?ver=13)

Veeam KB4381 records a real `ReFs.SetFileIntegrity` failure on an SMB
repository whose advertised feature did not work. This is evidence that
accepting the clone FSCTL while rejecting or falsely acknowledging the
integrity FSCTL is not a usable compatibility mode. The documented
`UseCifsVirtualSynthetic=0` switch disables the feature and is only a fallback,
not the fastdup target behavior. [Veeam KB4381](https://www.veeam.com/kb4381)

The public documentation does not specify every capability-probe request or
the exact `SET_INTEGRITY` payload used by every Veeam build. Capture an SMB
3.1.1 trace from the target Veeam build before freezing an interoperability
ADR or declaring compatibility.

## SMB operations are not interchangeable

### Duplicate extents: the required metadata clone

`FSCTL_DUPLICATE_EXTENTS_TO_FILE` is sent as an SMB2 IOCTL on the **target**
handle. Its payload names an open source handle plus source offset, target
offset, and byte count. Source and target must be on one volume; the ranges
have equal logical length; same-file ranges must be disjoint.
[MS-FSCC request](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/78e67b4b-84a4-4730-a34f-dcdcb2224e49)

The normative operation changes the target extent map so source and target
refer to the same storage, without copying bytes. Later writes must preserve
file isolation with copy-on-write. Support is optional, but a server that
claims it must return `STATUS_INVALID_DEVICE_REQUEST` when the object store
cannot perform it rather than silently copying data.
[MS-FSA duplicate-extents algorithm](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/4623bf0a-ab5f-4ab6-9c03-b8372c7aa06b)

The ReFS compatibility envelope Veeam is designed around is:

- source offset, target offset, and byte count begin/end on the advertised
  cluster boundary;
- one clone request is shorter than 4 GiB;
- the source range is within source EOF;
- the destination range does not extend past destination EOF, so the caller
  grows the target first;
- source and destination are on the same volume and have the same Integrity
  Streams state;
- a sparse source requires a sparse destination; and
- same-file source and target ranges do not overlap.

Microsoft also documents a maximum of 8,175 file regions referring to one
physical region for ReFS. That is a ReFS implementation limit, not an SMB
protocol limit and not automatically a fastdup limit.
[Microsoft ReFS block cloning](https://learn.microsoft.com/en-us/windows-server/storage/refs/block-cloning)

The common protocol errors include `STATUS_NOT_SUPPORTED` for overlapping
same-file ranges, sparse-source/non-sparse-target, or a source range beyond its
allocation; `STATUS_INVALID_PARAMETER` for an invalid/different-volume source
handle; `STATUS_DISK_FULL`; `STATUS_MEDIA_WRITE_PROTECTED`; and
`STATUS_INVALID_DEVICE_REQUEST` when extent duplication is unsupported.
[MS-FSCC reply](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/6c268ef7-11c5-4830-ba0a-20fbe0557646)

Samba additionally checks integer wrap, source EOF, target EOF, same-file
overlap, sparse compatibility, and exact returned byte count. Its current code
caps a request at target EOF instead of extending the target. Veeam should
pre-size its files, but fastdup must test this observed Samba behavior rather
than infer that clone extends a file.
[Samba duplicate-extents handler](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/source3/smbd/smb2_ioctl_filesys.c)

### CopyChunk: optional server-side copy

`FSCTL_SRV_COPYCHUNK`/`FSCTL_SRV_COPYCHUNK_WRITE` uses a source resume key and
one or more source/target/length tuples. It requires appropriate source-read
and destination-write access, rejects zero-length/oversized chunks, checks
conflicting byte-range locks, and may copy the data by any server-specific
means. It does **not** promise shared extents or CoW.
[MS-SMB2 server-side copy handling](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/cd0162e4-7650-4293-8a2a-d696923203ef)

Current Samba attempts `copy_file_range` for CopyChunk and falls back to a
buffered server-side read/write loop when the accelerated call is unavailable.
Thus implementing FUSE `copy_file_range` makes CopyChunk and ordinary Linux
range copies efficient too, but CopyChunk alone cannot satisfy Veeam Fast
Clone. [Samba default offload implementation](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/source3/modules/vfs_default.c)

## Samba-to-Linux path

Samba rejects duplicate extents unless the share has
`FILE_SUPPORTS_BLOCK_REFCOUNTING`. The current Btrfs VFS module adds this
capability. A custom share can add capability bits through
`share:fake_fscaps`, but advertising the bit before the full operation works
causes exactly the false-capability failure Veeam documents.
[Samba capability check](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/source3/smbd/smb2_ioctl_filesys.c),
[Samba `vfs_btrfs`](https://www.samba.org/samba/docs/current/man-html/vfs_btrfs.8.html),
[Samba `share:fake_fscaps`](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/docs-xml/smbdotconf/protocol/sharefakefscaps.xml),
[generic reflink change](https://lists.samba.org/archive/samba-cvs/2024-August/123260.html)

For duplicate extents, Samba resolves the source handle, then calls the VFS
offload-read and offload-write hooks with `FSCTL_DUP_EXTENTS_TO_FILE`. Its
default offload-write implementation calls `copy_reflink`, which is exactly a
Linux `FICLONERANGE` ioctl on the destination descriptor.
[Samba offload path](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/source3/modules/vfs_default.c),
[Samba `copy_reflink`](https://github.com/samba-team/samba/blob/5553b34fe47477285769f40999ff7127ddcf6faa/lib/replace/replace.c#L1252)

Linux `FICLONERANGE` guarantees shared storage with CoW isolation and an atomic
snapshot with respect to concurrent source writes. Both files must be on one
filesystem; ordinary disk filesystems may require block alignment; overlapping
same-file ranges fail; and unsupported filesystems report `EINVAL` or
`EOPNOTSUPP`. [Linux `FICLONERANGE`](https://man7.org/linux/man-pages/man2/ioctl_ficlonerange.2.html)

However, Linux handles `FICLONE`/`FICLONERANGE` in `fs/ioctl.c` by calling
`vfs_clone_file_range`, which requires the filesystem's `remap_file_range`
operation. The current FUSE regular-file operations contain
`copy_file_range`, but no `remap_file_range`. This is why adding handling for
the standard `FICLONERANGE` number to fastdup's FUSE `ioctl` callback cannot
fix the path: the generic kernel handler consumes it first.
[Linux generic ioctl path](https://github.com/torvalds/linux/blob/77ae27fd98f3b548797c9f22c10ab5cf1c4ada53/fs/ioctl.c),
[Linux FUSE file operations](https://github.com/torvalds/linux/blob/77ae27fd98f3b548797c9f22c10ab5cf1c4ada53/fs/fuse/file.c)

## POSIX/FUSE operations to implement

### `copy_file_range`: required adapter primitive

Linux `copy_file_range` overwrites a target range, may return a short count,
returns zero at source EOF, allows non-overlapping ranges in one file, and
currently requires flags zero. It gives a filesystem the opportunity to use a
reflink or server-side copy, but does not itself require either technique.
[Linux `copy_file_range(2)`](https://man7.org/linux/man-pages/man2/copy_file_range.2.html)

libfuse exposes both inode/handle pairs, source and target offsets, length, and
flags through the low-level `copy_file_range` callback. `ENOSYS` permanently
turns future requests into `EOPNOTSUPP`. The Linux FUSE client writes back the
source and target ranges before sending the request and invalidates the target
page cache after success.
[libfuse low-level API](https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html),
[Linux FUSE implementation](https://github.com/torvalds/linux/blob/77ae27fd98f3b548797c9f22c10ab5cf1c4ada53/fs/fuse/file.c)

For fastdup, one callback must:

- validate regular files, open modes, zero flags, checked offset+length,
  same appliance/volume, advertised clone alignment, source EOF, pre-sized
  destination EOF, sparse/integrity compatibility, and disjoint same-file
  ranges;
- freeze or otherwise serialize the exact source live view against concurrent
  writes and update the destination live view atomically;
- reuse source manifest references rather than read and re-ingest bytes;
- update destination size/allocation/timestamps and invalidate stale cached
  frontend ranges;
- return the complete length for the SMB duplicate-extents route, never a
  successful short clone; and
- make later destination/source writes copy-on-write by construction.

There is a format consequence: Veeam aligns clones to 4 KiB or 64 KiB, while
FastCDC chunk boundaries are content-defined. A metadata-only clone will often
start or end inside a DATA extent. If a manifest DATA extent can only name an
entire logical chunk, the current tree splice cannot represent that clone
without reading/re-encoding both boundary chunks. Pure metadata Fast Clone
therefore requires a versioned manifest representation for a byte slice of an
immutable logical chunk/encoding region, or an equivalent fixed-granularity
clone-addressability layer. This is an inference from the two documented
alignment contracts and must be resolved by ADR before claiming metadata-only
Veeam compatibility.

### `fallocate`: useful POSIX completeness, not Veeam Fast Clone

`FALLOC_FL_COLLAPSE_RANGE` removes a range and shifts the suffix down;
`FALLOC_FL_INSERT_RANGE` inserts a hole and shifts the suffix up. Both normally
require filesystem-block alignment, reject incompatible flags, and reject a
range reaching/passing EOF (collapse) or an insertion at/past EOF (insert).
`PUNCH_HOLE|KEEP_SIZE` creates a zero-reading hole without changing EOF.
[Linux `fallocate(2)`](https://man7.org/linux/man-pages/man2/fallocate.2.html)

These operations map naturally to fastdup's tree-native splice/concat support
and should be exposed for POSIX completeness. They are not substitutes for
`FSCTL_DUPLICATE_EXTENTS_TO_FILE`: Veeam synthetic full needs an equal-length
range overwrite that shares the source's immutable data, not a shift of the
target suffix.

## Integrity FSCTL

`FSCTL_SET_INTEGRITY_INFORMATION` uses an eight-byte little-endian payload:
`u16 ChecksumAlgorithm`, reserved `u16`, and `u32 Flags`. Defined algorithms
include NONE (`0`), CRC32 (`1`), CRC64 (`2`), and UNCHANGED (`0xffff`); the
defined flag is `CHECKSUM_ENFORCEMENT_OFF` (`1`). Invalid sizes, algorithms,
and flag combinations must be rejected, and success returns status only.
[MS-FSCC request](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/a4517cd5-3f5a-4058-a457-bcff2baac011),
[MS-FSA validation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/a9070f6f-3f19-4461-8596-2788321d6220)

Fastdup cannot safely acknowledge every request as a stateless no-op. Persist
the SMB-visible integrity algorithm/enforcement state per inode, return it
consistently if `GET_INTEGRITY_INFORMATION` is queried, and require compatible
source/target state for duplicate extents. Fastdup's existing BLAKE3/content
verification and container checksums may implement stronger internal integrity,
but they do not remove the externally visible state-machine obligation.
[MS-FSCC GET reply](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/72640484-66fb-4b8f-aec6-6ab56d63831b)

## Acceptance evidence before claiming support

1. Capture a real Veeam 13 SMB 3.1.1 trace for repository probing, target
   creation/pre-sizing, integrity requests, duplicate-extents batches, flush,
   rename, and close.
2. Run Samba protocol tests for duplicate extents plus fastdup tests at 4 KiB
   and 64 KiB boundaries, including boundaries inside FastCDC chunks,
   same-file disjoint ranges, overlap, sparse files, EOF, invalid handles,
   conflicting concurrent writes, and >4 GiB work split into legal requests.
3. Fault every stage of the destination manifest publication and prove crash
   recovery exposes only the predecessor or the complete cloned successor.
4. Measure container reads/writes during a Veeam synthetic full. The success
   criterion for cloned ranges is zero data-container I/O, not merely less SMB
   network traffic.
5. Do not enable `FILE_SUPPORTS_BLOCK_REFCOUNTING` until clone, CoW isolation,
   integrity state, recovery, and error mapping all pass end to end.
