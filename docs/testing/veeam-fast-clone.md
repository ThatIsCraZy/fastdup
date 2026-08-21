# Veeam Fast Clone POSIX/FUSE checkpoint

Status: POSIX/FUSE clone primitive and experimental Samba interoperability
module implemented; a real Veeam trace remains a release blocker.

## Implemented seam

- `Namespace::dispatch(Operation::CloneRange)` validates both handles, checked
  ranges, source EOF, full allocation, stable immutable source provenance, and
  same-file overlap before one target mutation sequence becomes visible.
- The target Dirty Extent Map retains immutable source readers and prepared
  Manifest recipes. Admission allocates zero resident payload bytes.
- Manifest Leaf v2 DATA_SLICE references arbitrary byte ranges inside verified
  FastCDC Chunks, so 4-KiB/64-KiB clone boundaries require no boundary reads or
  rechunking.
- Successor proof transfer accepts a source range only when its Manifest Root
  is named by the exact predecessor Namespace Root. It verifies touched
  metadata paths and removes only matching full-Chunk dependencies from the
  introduced set.
- FUSE `copy_file_range` with flags zero maps to this seam. Atomic rename and
  `RENAME_NOREPLACE` are also wired through the shared dispatch seam.

Sparse-source clone and an actively mutable source epoch currently return
unsupported. The Veeam path is expected to clone older, committed,
fully-allocated backup files into a pre-sized destination.

## Automated evidence

`partial_fastcdc_range_clone_publishes_only_metadata_and_recovers_byte_exact`
clones a 96-KiB range beginning at byte 4,096 from a FastCDC-backed source into
a sparse pre-sized target. It asserts zero frontend dirty payload, unchanged
Container count, a durable DATA_SLICE, successful offline Manifest scrub,
atomic final rename, crash, and byte-exact recovery.

`every_metadata_clone_fault_recovers_the_previous_or_complete_range` injects
fail-before and fail-after at every metadata operation in the clone checkpoint.
The only recovery outcomes are the all-zero predecessor target or the complete
cloned range. It additionally asserts that the successful clone checkpoint
performs no Container StorageIo operation at all.

The predecessor-provenance test rejects a valid but foreign Manifest object,
and POSIX model tests cover handle modes, same-file overlap, delayed source
reads, target copy-on-write isolation, atomic replacement rename, and open
replaced-inode lifetime.

## Real FUSE evidence (2026-08-21)

An actual kernel FUSE mount was exercised with a 4-MiB source, a pre-sized
4-MiB target, and Python's direct `os.copy_file_range` syscall:

```text
source_offset=4096 target_offset=1048576 length=2097152
copied=2097152
live-byte-oracle=ok
recovery-byte-oracle=ok
```

The clone generation reported:

```text
logical_chunks=25 logical_bytes=2109378
new_chunks=0 new_bytes=0 containers=0 container_file_bytes=0
peak_buffered_bytes=0 checkpoint_rechunk_bytes=0
recipe_reuse_chunks=25 recipe_reuse_bytes=2109378
```

The two pre-existing `.fdc` files and their byte sizes were identical before
and after clone, commit, unmount, and recovery. All generated mount/test data
stayed under `.artifacts/`.

## Samba adapter evidence (2026-08-21)

`samba/vfs_fastdup` contains the VFS module and a dependency-free contract
test. The contract test covers fixed Integrity SET/GET state, input bounds,
alignment, checked source/target bounds, pre-sized destination, same-file
overlap, request-size cap, monotonically ordered per-handle operation fences,
and CLOSE readiness.

The module was compiled as `libvfs_module_fastdup.so` against the unmodified
Samba 4.23.5 tag. Its VFS hooks cover capability advertisement, Integrity
FSCTLs, Duplicate Extents offload token creation/consumption, and CLOSE. A
Duplicate Extents request invokes one `copy_file_range` call and rejects a
short or unsupported result without buffered fallback.

The same build-tree `smbd` then loaded `bin/modules/vfs/fastdup.so` on an
isolated loopback-only SMB 3.1.1 share. A client PUT, directory listing, GET,
CLOSE, byte comparison, and delete completed successfully. This proves module
discovery, registration, connect/close chaining, and ordinary-I/O delegation;
it does not replace the still-missing protocol-level Duplicate Extents and real
Veeam trace gates.

Metadata clones remain subject to bounded filesystem mutation admission. They
do not consume frontend dirty DATA bytes, but their Dirty Extent Map and
Manifest/WAL work are not free or unbounded. CLOSE waits for application of all
previously accepted operations on its handle; it is intentionally not an
implicit durability checkpoint.

## Remaining Veeam release gates

Veeam SMB Fast Clone uses `FSCTL_DUPLICATE_EXTENTS_TO_FILE` and
`FSCTL_SET_INTEGRITY_INFORMATION`, not ordinary CopyChunk. Stock Samba maps
Duplicate Extents to `FICLONERANGE`, which cannot reach FUSE's
`copy_file_range`. The `vfs_fastdup` adapter supplies that mapping and a fixed
Integrity Information state.

Do not claim Veeam Fast Clone compatibility until a real SMB 3.1.1 Veeam trace,
Samba protocol tests, alignment/error/lock cases, and Integrity FSCTL behavior
are green. The module only advertises `FILE_SUPPORTS_BLOCK_REFCOUNTING` when
the share explicitly enables it.
