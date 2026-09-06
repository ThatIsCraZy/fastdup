# Recovery Record evidence and inode reservation — 2026-09-06

## Live observation

After the explicitly requested hard restart, revision 0.6.4-9 remained in
recovery beyond five minutes. The management startup deadline then stopped the
Runtime, even though it was still reading and verifying. A second start with
the management services stopped continued beyond fifteen minutes; that process
was then explicitly killed at the user's request. No completed baseline mount
time is available.

A 15-second, 49-Hz profile of the second recovery captured 707 samples without
loss. Zstd decompression accounted for about 43% inclusive CPU samples, BLAKE3's
AVX-512 implementation 15% self, and the two leading CRC32C routines about 8%
self. These are CPU samples, not elapsed-time or physical-I/O attribution.

## Reproduced defects and changes

1. `IndexedRequiredChunkVerifier` discarded the other verified Chunk payloads
   produced when reading a compressed Record. Sixteen required Chunks sharing
   one Record caused sixteen reads and decompressions. A fresh proof now
   retains only the identities of co-verified future required Chunks with
   matching lengths. This pass-local set cannot exceed the required dependency
   set and retains no decoded payload allocations. It never survives another
   proof invocation. Unusable index candidates retain the complete scan
   fallback. The regression also corrupts DATA after a successful proof and
   requires the next proof to reject it.
2. Writable startup discarded the recovered file capabilities and performed a
   second full graph proof while advancing the inode reservation. For a healthy
   newest generation it now composes an inode-only successor from the fresh
   opaque Manifest proofs. The complete recovered Commit Record must match the
   current WAL head under the existing commit lock. Rejected-newer-generation
   fallback retains the former complete verification path. The regression
   models process loss, reopens fresh repositories, requires exactly one DATA
   read, and checks that new inode IDs skip the old reservation.
3. A management response timeout no longer stops a Runtime that is still
   active. It returns an explicit pending-recovery result and keeps Mounting
   state. Sampling completes share activation only after verified frontend
   readiness and successful share configuration. Actual failures retain Error
   state. The state distinction has a unit regression; the original destructive
   five-minute timeout was observed live, not reproduced by a privileged CI
   service test.

Startup now logs completion times for the fresh namespace/DATA proof and inode
reservation. No durable format changes, new persisted trust flags, bypass of
post-restart DATA verification, or ingest hot-path work are introduced.
[ADR 0037](../adr/0037-separate-structural-recovery-from-current-data-proof.md)
and [ADR 0036](../adr/0036-compose-successor-data-proofs-from-the-installed-generation.md)
record the proof lifetime and reservation fence.

## Verification and performance

The two regression commands were run against the old paths and failed with
`left: 16, right: 1` and `left: 2, right: 1`, respectively. Both pass after the
changes:

```sh
cargo test -p fastdup-store --test manifest_reader \
  recovery_verifier_decodes_a_shared_record_once_and_rechecks_on_the_next_pass
cargo test -p fastdup-appliance --test recover_mount \
  writable_restart_proves_data_once_before_reserving_new_inode_ids
```

Always set `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`. Long worktree paths additionally require
`cargo --config 'env.TMPDIR.value="/source/fastdup/.artifacts/tmp"' ...` for
Unix socket fixtures.

The ignored `recovery_record_proof_benchmark` builds the same real Container,
Exact Index and sixteen-Chunk compressed Record fixture, then executes 256
independent fresh proofs per round. Release binaries for baseline `f1b92ce`
and the fixed implementation were retained separately. Three alternating A/B
pairs ran pinned to CPU 0 without concurrent builds; seven rounds per invocation,
first round discarded, yielded 18 samples per mode on the development VM
(reported Intel Core i7-1370P).

| Metric per 256 fresh proofs | Before | After |
| --- | ---: | ---: |
| Median elapsed | 370.723 ms | 23.063 ms |
| DATA range reads | 4,096 | 256 |

This is about 16.1x faster in this shared-Record fixture. The OS page cache is
involved; range-call reduction is not a measured physical-disk or Veeam
throughput multiplier. RAW records containing one Chunk cannot obtain the same
co-verification benefit. Every mount still needs its one complete fresh graph
proof; the result is not a promise of constant-time startup for arbitrary data.

Selected checks passed: 99 store tests (library, Manifest reader, recovery,
filesystem Generation repository), 99 appliance tests (library, writable
recovery, durable fault injection, write-through ingest), and 32 control tests.
An existing warm-index test now expects one bounded DATA read instead of two,
reflecting the removed reservation proof. Production Clippy for all three
libraries passed with warnings denied. Raw profiles, red/green logs, saved
benchmark binaries and package logs are under `.artifacts/recovery-startup/`.

Revision 0.6.4-10 carries these changes on top of the DATA-cache reclamation and
telemetry fixes. The package was installed on the test VM. Recovery was then stopped at the
operator's request because recovery of the existing test data was unnecessary.
No complete live mount-time comparison was obtained; the measurements above
remain controlled fixture results. Existing test data was left intact.
