# Architecture decision records

ADRs record current durable decisions, their non-obvious reasons, and explicit
acceptance gates. `accepted` does not imply production qualification. A newer
ADR wins when records conflict; implementation detail belongs in specs, tests,
and measurement reports.

The [2026-09-05 code-alignment audit](../research/adr-codebase-audit-2026-09-05.md)
is dated evidence, not the current decision map.

## Current decision map

| Area | Current records and qualifications |
| --- | --- |
| Compatibility | 0002, 0071, 0074: epoch 3 only; older pre-production pools require re-ingest, not migration. |
| Container integrity and allocation | 0008, 0059, 0060, 0072, 0075, 0088: Container v3, structural commitment algorithm 2, paired generation high-water reservations. |
| Normal startup and background verification | 0091: committed Metadata start; 0090: paced scrub, GC gate and sticky integrity failure; 0092: durable incomplete-round resume; 0023 retains full disaster rebuild. |
| Manifest and Namespace graphs | 0011, 0036, 0042, 0043, 0094, 0095: Manifest trees plus one record-range-sharded Namespace Root. ADR 0085's FastCDC layout is superseded. |
| User-DATA chunking | 0054: SeqCDC-v1. Namespace record partitioning is a separate key-derived rule under 0095. |
| Advanced Reduction | 0010, 0018, 0088, 0089: depth-one Prefix/Sparse-XOR, bounded online Similarity; 0062 retains the offline rebuild contract. |
| Exact Index | 0035, 0044, 0045, 0079: immutable generations, activation, compaction, leased mappings; 0046's dated 2026-09-16 section bounds protected warming; 0035 remains proposed with scale/performance gates. |
| Read and publication paths | 0046, 0058, 0077: one Unified Read Cache, Direct I/O, direct FUSE handles, io_uring DATA publication, and restore locality. ADRs 0051 and 0073 are superseded; userspace prefetch from 0030 remains open. |
| Write ingestion | 0041, 0053, 0054, 0057, 0099: owned FUSE payloads feed ordered SeqCDC Lanes and ordered Container retirement; bounded parallel FUSE writes remain proposed pending Veeam qualification. |
| Recovery and GC | 0020, 0037, 0064–0072: DATA-tier checkpoints, graph proof, local victim proofs, Metadata marks, lease and recovery latch. |
| Capacity and placement | 0080–0084: pool identity, distinct XFS filesystems, Small-File project quota, physical admission and cached capacity reporting. |
| Commit cut | 0096–0098: announce before fencing, admit the backlog during the cut, and charge the pending-region gate only for live Lane payload. |
| Commit cost | 0094, 0095: prove once, publish record-range shards, and reuse unchanged objects. Commit construction remains O(Namespace) until the commit-side mirror is incremental. |
| Management and Share policy | 0086, 0087, 0089: separate Control Plane, logical quota at Namespace admission, live per-Share dependent-encoding policy; 0086's dated 2026-09-16 section makes SMB a stop-propagated, mount-conditioned Runtime frontend. |

Open gates: Exact Index workload qualification (0035), userspace prefetch
(0030), Dictionary activation (0017/0047), Veeam qualification (0043),
device-loss protection (0001), the Namespace commit-side mirror (0095), and
default-on Advanced Reduction qualification (0089), and bounded parallel FUSE
write qualification (0099).
