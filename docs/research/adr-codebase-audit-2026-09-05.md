# ADR-to-code alignment, 2026-09-05

Baseline: commit `a2c59d1` and its clean workspace before this documentation
update. The repository contains 89 numbered ADRs. This pass checked the ADR
inventory for stale implementation, format, and supersession statements, then
traced the affected claims into current source, tests, and recorded evidence.
It is a focused refresh of the [2026-08-27 audit](adr-codebase-audit-2026-08-27.md),
not a new correctness or hardware qualification of every accepted invariant.
The old audit remains a dated snapshot; its open-work list is not current.

## Confirmed drift and corrections

| ADRs | Current implementation and correction | Evidence |
| --- | --- | --- |
| 0008 | Writable Container generations recover from paired high-water reservations, not just the largest published generation. | [allocator](../../crates/fastdup-format/src/container_generation_high_water.rs), ADR 0072 |
| 0008, 0075, 0088 | Container envelope and intrinsic summary are v3; structural commitment remains algorithm 2. The rejected v3 lookup-filter experiment in 0075 is historical and unrelated to the later Sparse-XOR version bump. | [Container format](../../crates/fastdup-format/src/container.rs), [codec fault/format tests](../../crates/fastdup-format/tests/sparse_xor_record.rs) |
| 0011, 0042, 0043 | Manifest Leaf and Inner writers/readers use only v2. DATA_SLICE is part of the extent model; v1 compatibility prose contradicted strict decoders and ADR 0074. | [Leaf format](../../crates/fastdup-format/src/manifest.rs), [Inner format](../../crates/fastdup-format/src/manifest_inner.rs) |
| 0014, 0040, 0041 | Historical user-DATA FastCDC descriptions are superseded by SeqCDC-v1. FastCDC in Namespace sharding remains intentional. | ADRs 0054 and 0085; no global FastCDC text replacement was applied. |
| 0017 | Dictionary identity/dependency rules are an accepted design contract, not implemented durable Dictionary activation. | [experimental Dictionary path](../../crates/fastdup-store/src/reduction_dictionary.rs), production codec dispatch in the Container format |
| 0032 | DATA-tier Recovery Checkpoints and Small-File placement are implemented. Samba adapter/contract testing does not establish Veeam qualification. | ADR 0020, [tiered storage test](../../crates/fastdup-store/tests/tiered_container_repository.rs), [v0.6 limitations](../releases/v0.6.md) |
| 0036 | Writer publication evidence replaced the historical mandatory reread; restart/read/scrub still independently verify. | ADR 0059; existing SeqCDC, S3-FIFO, and Appliance Lease supersession notes retained. |
| 0062, 0063, 0076, 0089 | Offline paired rebuild remains, but live Similarity uses immutable online generations with current Exact pins per planning batch. Families may overlap across chronological generations. Mount-lifetime Exact pairing is not the live writer contract. | [online repository](../../crates/fastdup-store/src/online_similarity.rs), [batch pinning](../../crates/fastdup-store/src/persistent_reduction.rs), [publication/compaction faults](../../crates/fastdup-testkit/tests/online_similarity.rs) |
| 0063, 0088 | Fragmented targets can participate in dependent planning after materialization; Sparse-XOR and Prefix share the trial budget. | [persistent reduction planner](../../crates/fastdup-store/src/persistent_reduction.rs), [Sparse-XOR format tests](../../crates/fastdup-format/tests/sparse_xor_record.rs) |
| 0081 | Metadata no longer contains only commit-critical state: Small-File Containers have an inheriting XFS project hard quota. | [quota preparation](../../crates/fastdup-appliance/src/small_file_tier.rs), [physical pool checks](../../crates/fastdup-appliance/src/pool_isolation.rs) |
| 0082 | Small-File writes charge physical Metadata headroom and a separate Small-File bucket, rather than DATA. Claims survive until a later physical observation accounts for publication. | [admission ledger](../../crates/fastdup-appliance/src/commit_capacity.rs), [Small-File and orphan capacity tests](../../crates/fastdup-appliance/tests/commit_capacity.rs) |
| 0084 | Placement, quota, tier-neutral reads, and runtime suffix replacement exist. The 8-MiB selector is re-evaluated; it does not persist a one-way spill state. Frozen cuts retain their policy. | [placement policy](../../crates/fastdup-posix/src/lib.rs), [runtime policy tests](../../crates/fastdup-posix/tests/small_file_placement.rs), [tiered storage](../../crates/fastdup-store/src/tiered_storage.rs) |

## Evidence and open boundaries

- ADRs 0081/0082/0084 are no longer unimplemented follow-ups. The recorded
  [2026-09-01 XFS/FUSE run](../testing/full-tier-enospc.md) exercised real
  project-quota and DATA exhaustion, scrub, cleanup, and byte-exact remount.
  It used loop-mounted XFS images and does not prove device power-loss behavior.
- ADR 0089 stays `proposed`. The [Linux online Similarity A/B](../benchmarks/linux-6.12-online-similarity-2026-09-05.md)
  and [Share-policy implementation report](../benchmarks/online-similarity-share-policy-2026-09-05.md)
  provide implementation and performance evidence; the record still calls for
  default-on L0 latency, negative-probe and compaction-I/O qualification.
- ADR 0035 retains its explicit scale, throughput, and write-amplification gates.
  No status was promoted merely because the code exists.
- Per-handle userspace speculative prefetch in ADR 0030 remains unimplemented.
  Kernel readahead and bounded demand coalescing are separate mechanisms.
- Dictionary activation, real Veeam compatibility, and device-loss protection
  remain open. Small-File records stored only on Metadata are not redundant
  copies; implementing Recovery Checkpoints does not imply all DATA survives
  losing that tier.
- `docs/specs/container-v1.md` still has historical v1/unassigned-codec language
  alongside later additions. This ADR refresh does not certify that document
  as a complete current Container-v3 byte specification. A field-by-field spec
  reconciliation remains separate work; use the strict format implementation
  and ADR 0088 for the current version/codec boundary.

## Validation of this update

Only documentation changed. Source and existing tests were inspected, not
executed; prior benchmark and qualification results above are attributed to
their dated reports. The update checks whitespace, numbered ADR uniqueness,
and relative Markdown file links in changed documents. No storage format,
runtime policy, or acceptance status was changed.
