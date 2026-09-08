# Veeam Duplicate Extents alignment regression — 9 September 2026

Reported failure: `Transform.CompileFIB`, invalid parameter, source offset
1,617,920, target offset 1,609,728, length 8,192.

Read-only inspection of 10.1.1.161 found fastdup `0.6.4-14`, an explicit Samba
`clone alignment = 65536`, and a 10,753,359,872-byte source file with Integrity
xattr `0200`. The temporary target had already been removed. Both repository
and Samba services were active. No matching Samba journal entry was available;
previous contract rejection lacked a diagnostic. Rejection now logs the reason,
geometry and bounds without filenames or credentials.

A C harness invoking the actual VFS contract with the reported offsets returns
`FASTDUP_CONTRACT_MISALIGNED` (4) at 64 KiB. Changing only alignment to 4 KiB
returns `FASTDUP_CONTRACT_OK` (0). The generated profile and module default now
both use 4 KiB. Legacy native-integrity-enabled xattrs are interpreted under
current geometry for GET and source/target comparisons without rewriting files.
Disabled/enabled differences and malformed xattrs remain rejected.

Validation:

- Portable C contract suite: exact Veeam offsets, 64 KiB rejection, 4 KiB
  success, alignment and pre-sizing failures, legacy/current enabled metadata,
  NONE separation, UNCHANGED, malformed state and handle fences.
- Three control-plane Samba configuration tests passed.
- Two appliance integration tests passed: the exact reported clone plus its
  checkpoint issue zero DATA operations and recover the correct bytes with
  intact neighbors; existing fault injection exposes only the old or complete
  clone across every injected metadata failure.
- VFS module compiled and linked successfully against Samba 4.23.5.

Artifacts: `.artifacts/clone-8192/`. The fix is not installed on the test VM;
no repository or Samba service was restarted. Updating only the default cannot
replace a share's explicit 65536 setting. Deployment must update the VFS module
and regenerate the managed configuration using the updated agent, then refresh
SMB connections. A complete Veeam synthetic-full retry remains necessary after
activation; these tests do not claim full Veeam qualification.
