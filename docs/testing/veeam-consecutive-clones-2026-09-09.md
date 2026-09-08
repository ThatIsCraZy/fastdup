# Consecutive byte-granular Veeam clones — 9 September 2026

The 01:05:15 Veeam failure used source offset 1,625,088, target offset 1,616,896,
and length 5,120. Both starts are exactly the ends of the previous 7,168-byte
clone. Allowing partial lengths while retaining aligned starts was incomplete.

## Before/after evidence

On installed package 0.7.1-2, a real SMB 3.1.1 replay of the exact request returns
`STATUS_INVALID_PARAMETER` (`0xc000000d`). Both handles report matching native
integrity and 4,096-byte geometry. The immutable existing source is opened
read-only and a unique disposable target is pre-sized. A consecutive SMB replay
succeeds on the first 7,168-byte range and fails on the following 5,120-byte range.
The new C regression also fails before production code changes.

The fix removes offset alignment as a CloneRange admission condition. Geometry
remains validated for reporting/Integrity purposes, independently of byte-range
admission. No format change, rounding, buffering or DATA copy is introduced.
EOF/pre-sizing, overflow, overlap, size, capacity and integrity checks remain.
ADR 0043 records the intentional extension beyond strict Windows rules.

## Validation

- Portable C contract suite passes. It covers every starting-byte residue within
  4 KiB on both sides, lengths 1 through 8,193, exact file boundaries, overflow,
  overlap, policy/geometry validity and the existing maximum request size.
- Six appliance clone tests pass in the isolated worktree, including the previous
  aligned and partial-length regressions. A sequence of 7,168/5,120/1/8,193/65,536
  bytes plus different source/target residues and exact EOF endings is recovered
  after a simulated crash. Every byte of the 2 MiB target is compared using
  bounded 64 KiB reads. Clone plus checkpoint issues zero DATA operations.
- Fault injection checks failures before/after every metadata checkpoint operation
  for aligned, partial-length and unaligned-offset ranges; only the complete old
  or new range may recover.
- Samba 4.23.5 module build passes. RPM payload comparison with 0.7.1-1 confirms
  only the Samba module and generated build-id links change. Runtime binaries
  are reused from the previously verified package.

Run the native regressions with the workspace-local Cargo target and TMPDIR:
`cargo test --release -p fastdup-appliance --test durable_namespace_faults clone`.
The C suite is `samba/vfs_fastdup/tests/run.sh` with `FASTDUP_WORKSPACE` selecting
the checkout. The permanent real-SMB regression is
`samba/vfs_fastdup/tests/clone_ranges_smb.py`; its environment contract is in the
script and Samba README. It uses a read-only existing source and removes only
its unique target. Local logs/harness/package outputs: `.artifacts/clone-offsets/`.

## Test appliance activation

Installed `fastdup-0.7.1-3.el10.x86_64` on `10.1.1.161`. RPM SHA-256:

```
1c454f36fb64ec33385cfa502d9c887412c392c063d69e921d4ac09449564896
```

No files were open at the Samba restart. Repository PID 241863 remained
unchanged, all four services are active, and RPM verification differs only for
the two pre-existing customized configuration files.

The exact 5,120-byte request now succeeds in 4.346 ms with byte-exact readback,
intact neighbors and flush/reopen persistence. The complete seven-request SMB
sequence also succeeds (roughly 4.4–4.9 ms per request), including the first two
reported operations in the same target. All 2 MiB of the target and its file
size match before and after flush/reopen. Temporary targets are removed.

These measurements validate the adapter and recovery behavior; they are not an
end-to-end throughput benchmark or a completed Veeam synthetic-full qualification.
