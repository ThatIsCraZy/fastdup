# Veeam Integrity hotfix qualification — 2026-09-06

The reported `ReFs.SetFileIntegrity` / "Incorrect function" error was reproduced
against the installed 0.6.4-1 package using authenticated, encrypted SMB 3.1.1.
An otherwise identical SET succeeded for NONE and UNCHANGED, but CRC32 and CRC64
returned `STATUS_INVALID_DEVICE_REQUEST`. The adapter explicitly rejected those
two algorithms while advertising block refcounting. No SELinux change was needed.

The portable contract regression was changed to expect CRC64 enablement before
the implementation was changed and failed on that assertion. The fixed adapter
persists the selected policy in an ordinary inode xattr and reads it on GET.
See [ADR 0043](../adr/0043-expose-metadata-range-clones-for-veeam-fast-clone.md)
for native integrity semantics and limitations.

## Results

- Portable C11 contract tests: passed with `-Wall -Wextra -Werror -pedantic`.
- Samba 4.23.5 VFS module ABI build: passed.
- Committed `samba/vfs_fastdup/tests/integrity_smb.py`: passed on the test VM
  over SMB 3.1.1 with required encryption. Covers CRC32/CRC64 SET, GET,
  UNCHANGED, NONE reset, malformed requests, data write/read, rename/reopen,
  denied SET on a read-only handle, directory and attribute-only handles.
- Duplicate Extents integration: differing policies returned
  `STATUS_INVALID_PARAMETER` before target mutation. After matching the policies
  and committing the source, a 64 KiB clone succeeded with byte-exact readback.
- Fault injection: a malformed stored policy made GET and SET return
  `STATUS_DATA_ERROR`; file contents remained readable and unchanged.
- Existing `xattrs_posix_acl_and_immutable_flag_survive_checkpoint_recovery_and_scrub`
  regression: passed, qualifying the reused byte-exact metadata persistence path.
  A live repository remount was not needed for the adapter change.
- An optional sanitizer run could not link because the local ASan/UBSan runtime
  libraries were missing; no sanitizer result is claimed.

All SMB fixtures and temporary accounts were removed. The complete Veeam backup
job remains a separate user-run check; this qualifies the failing operation and
its surrounding SMB behavior, not full Veeam compatibility.

## Package

Hotfix package: `fastdup-0.6.4-2.el10.x86_64.rpm`.

SHA-256:
`7881efd00bd91ab419f7c8914cdd7a9714f4952ff1578376e5ddc61366a18410`.

The general package-build attempt ran out of local disk space during Rust
linking. The hotfix was assembled from the verified 0.6.4-1 RPM payload and the
newly built VFS module using the revision-2 spec. All four Rust executables were
verified byte-identical to the published 0.6.4-1 payload. Rust and WebUI source
are unchanged. The new RPM was installed on the Rocky Linux 10.2 test VM, with
only Samba stopped/restarted for loading the module; the repository stayed
mounted. The SMB integration checks were repeated against the installed package.
