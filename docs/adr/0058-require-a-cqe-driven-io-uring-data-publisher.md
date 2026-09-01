---
status: accepted
---

# Require a CQE-driven io_uring DATA publisher

The DATA tier requires `io_uring`; ring setup failure aborts daemon startup
instead of selecting a synchronous adapter. One ring-owning thread receives
commands through a bounded channel and an `eventfd` poll, keeps independent
publication state machines in flight, and advances only the operation named by
each CQE. `SINGLE_ISSUER` is enabled because that thread also creates the ring.
`DEFER_TASKRUN` is not enabled: on the Linux 6.12 XFS benchmark it reduced the
1,000-by-128-KiB publisher by 7.3 and 17.8 percent in two alternating pairs.

The short-lived Container publication descriptor uses `O_DIRECT` only when
the sealed image is at least 4 MiB. Smaller publications remain buffered. The
writer allocates the complete image at a 4-KiB-aligned address, so Direct mode
does not add a full-image alignment copy. Building, Body, Sealed Header, and
the three publication samples use the selected descriptor; file fsync,
no-replace rename, and root-directory fsync remain mandatory. After rename the
descriptor is dropped, and ordinary demand reads use buffered descriptors with
kernel readahead. The threshold is the first tested size without a throughput
or p99 regression on the Linux 6.12 XFS A/B series recorded in
`docs/benchmarks/direct-io-publication-2026-09-01.md`.

Writer-image verification at or above 1 MiB runs asynchronously through a
bounded queue on the existing process Rayon pool. Smaller images remain inline
because dispatch costs exceed their short verification time. The ring owner
never waits for a large verification batch; it continues to accept commands,
consume CQEs, and submit durability work for other operations. Container
length fixup uses `IORING_OP_FTRUNCATE`, and root-sync completion still releases
only the callers captured before that submission.

This supersedes ADR 0046's batch-barrier worker, separate verifier-pool, setup
fallback, and operator-selectable synchronous DATA adapter. It does not change
the Container publication order, byte budget, sampled-storage policy, metadata
tier, recovery, or scrub semantics.
