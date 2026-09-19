---
status: accepted
---

# Require a CQE-driven io_uring DATA publisher

The DATA tier requires `io_uring`; ring setup failure aborts startup rather than
selecting a synchronous publisher. One ring-owning thread receives bounded
commands, submits independent publication state machines, and advances only the
operation named by each CQE. `SINGLE_ISSUER` is enabled; `DEFER_TASKRUN` remains
disabled by measured Linux 6.12/XFS performance.

Container publication uses aligned `O_DIRECT` I/O under ADR 0046. The ring owner
continues servicing other operations while CPU-side preparation completes.
Writer evidence follows ADR 0059; file sync, no-replace rename, root-directory
sync, bounded ownership, and error propagation remain mandatory. No cache hit or
CQE completion alone establishes durability.

This decision concerns the DATA publication engine only. Metadata writers,
recovery, scrub, and application caching retain their own boundaries. Benchmark
evidence lives in
[`direct-io-publication-2026-09-01.md`](../benchmarks/direct-io-publication-2026-09-01.md).
