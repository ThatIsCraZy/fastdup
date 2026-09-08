# Veeam partial-cluster Clone length, 9 September 2026

Veeam failed `Transform.CompileFIB` on the test appliance at 00:50:52 with
`STATUS_INVALID_PARAMETER` for source offset 1,617,920, target offset 1,609,728,
and length 7,168. The source VIB size was 19,907,710,976 bytes. This range was
inside the source, not a special EOF tail.

## Diagnosis and regression

A real SMB 3.1.1 replay opened the existing source read-only and created only a
unique disposable destination. Both handles reported enabled native integrity
and 4,096-byte volume geometry. On package 0.7.1-1 the exact request returned
`0xc000000d`, matching Veeam. A new portable contract assertion also failed on
that request before changing production code.

The remaining length-alignment check rejected a range the native Manifest slice
implementation can represent exactly. Remove only the length-multiple condition;
retain offset alignment, exact EOF bounds, pre-sizing, overlap and size limits.
There is no rounding, tail copy, or on-disk format change. ADR 0043 explicitly
records the extension beyond the strict Windows cluster-length contract.

Validation from the isolated worktree:

- `sh samba/vfs_fastdup/tests/run.sh` passes with `FASTDUP_WORKSPACE` pointing
  at that worktree. It covers lengths 1 through 8,193 and exact EOF/pre-sizing
  failures, plus overflow, overlap, offset alignment and the existing 1 GiB cap.
- `cargo test --release -p fastdup-appliance --test durable_namespace_faults clone`
  passes all four tests. The partial-range cases include 1, 1,024, 4,095, 4,096,
  7,168, 8,193 and 65,536 bytes at the reported offsets. Clone plus checkpoint
  causes zero DATA operations. Crash recovery preserves exact bytes and neighbors.
- Fault injection retains the aligned-range oracle and adds a 7,168-byte oracle:
  failures before/after each metadata checkpoint operation expose only the
  complete predecessor or complete cloned range.
- The VFS module builds against Samba 4.23.5. Package 0.7.1-2 reuses the verified
  0.7.1-1 application binaries; payload comparison finds only `fastdup.so` changed
  (and its generated build-id links).

All build/test/temp outputs use `/source/fastdup/.artifacts`, with Cargo target
and TMPDIR configured there. Logs and replay harness: `.artifacts/clone-tail/`.

## Installed verification

Installed `fastdup-0.7.1-2.el10.x86_64` on `10.1.1.161`. RPM SHA-256:

```
6dbde3b4ac217d17f657ff09c4d5a8b17527690ab93ae2bf602650dd6c3d9d9d
```

Samba restarted after checking that no files were open. Repository PID 241863
remained unchanged; agent, control, repository and Samba are active. RPM
verification reports only the two pre-existing customized configuration files.

The same SMB replay now succeeds for 7,168 bytes in 4.457 ms. Readback matches
source bytes exactly, adjacent bytes are intact, and flush/reopen retains the
result. Additional real SMB requests for 1, 1,024, 4,095, 4,096, 8,192, 8,193
and 65,536 bytes pass the same checks. Temporary targets are removed; existing
backup data is opened read-only. These small checks validate correctness, not
end-to-end throughput or a complete Veeam synthetic-full job.
