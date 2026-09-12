# Architecture decision records

ADRs record decisions and their reasons. An accepted decision is not a claim
that every implementation or production qualification gate is complete.
Later records refine earlier ones; dated current-state notes identify those
relationships without erasing the original trade-offs.

The latest [code-alignment audit](../research/adr-codebase-audit-2026-09-05.md)
records verified drift, source evidence, and remaining gaps. The
[2026-08-27 inventory](../research/adr-codebase-audit-2026-08-27.md) is historical.

## Current decision map

| Area | Current records and qualifications |
| --- | --- |
| Compatibility | 0002, 0071, 0074: current pre-production formats only; no general migration guarantee. |
| Container integrity and allocation | 0008, 0059, 0060, 0072, 0075, 0088: Container v3, structural commitment algorithm 2, paired generation high-water reservations. |
| Normal startup and background verification | 0091: committed Metadata start; 0090: paced scrub, GC gate and sticky integrity failure; 0092: durable incomplete-round resume; 0023 retains full disaster rebuild. |
| Manifest and Namespace graphs | 0011, 0036, 0042, 0043, 0085: Manifest Leaf/Inner v2, authenticated successors, one sharded Namespace Root. |
| User-DATA chunking | 0054: SeqCDC-v1. Namespace sharding uses its separate FastCDC profile in 0085. |
| Advanced Reduction | 0010, 0018, 0088, 0089: depth-one Prefix/Sparse-XOR, bounded online Similarity; 0062 retains the offline rebuild contract. |
| Exact Index | 0035, 0044, 0045, 0079: immutable generations, activation, compaction, leased mappings; 0035 remains proposed with scale/performance gates. |
| Read and publication paths | 0046, 0051, 0058, 0073, 0077: one unified application cache, Direct I/O, direct FUSE handles, io_uring DATA publication and restore locality. Userspace prefetch from 0030 remains open. |
| Recovery and GC | 0020, 0037, 0064–0072: DATA-tier checkpoints, graph proof, local victim proofs, Metadata marks, lease and recovery latch. |
| Capacity and placement | 0080–0084: pool identity, distinct XFS filesystems, Small-File project quota, physical admission and cached capacity reporting. |
| Management and Share policy | 0086, 0087, 0089: separate Control Plane, logical quota at Namespace admission, live per-Share dependent-encoding policy. |

Dictionary activation (0017/0047), Veeam qualification (0043), and device-loss
protection (0001) remain distinct open work. ADR 0089 is implemented and has
device-backed A/B evidence, but remains proposed pending its stated default-on
performance qualification. Benchmark availability alone does not close those
gates.
