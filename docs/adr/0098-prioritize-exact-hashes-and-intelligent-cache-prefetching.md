---
status: accepted
---

# Prioritize Exact hashes and intelligent cache prefetching

This record refines ADR 0046 without changing any durable format or readback
verification rule. Cache residency remains volatile acceleration.

FastDup does not use the Linux page cache as a correctness or performance
strategy. The process uses Direct I/O and owns residency decisions because it
knows Exact hashes, manifest extent order, record siblings, and read intent.

Exact dedup hashes are the highest-priority cache content. Membership filters
and page-key bounds for active Exact Runs are pinned while the Run is active,
and Exact structure building measures its budget against stable effective
capacity so it starts immediately after activation. Missing Exact structures
warm ahead of ordinary page warming, at background I/O priority.

Class shares in the unified cache are guarantees under contention, not fixed
partitions. A class may borrow uncontested capacity, and the largest borrower
above its guarantee yields first. Protected Exact keeps the dominant guarantee,
while DATA keeps a smaller guarantee and borrows only while capacity is
uncontested.

Admission records frequency and ghost evidence. Unproven DATA may replace only
entries that never earned hit credit; proven or ghost-returned DATA may use the
normal CLOCK path. Credit ages by halving. Pinned Exact acceleration has a hard
process-memory cap because it is false-negative-safe when absent.

Restore prefetching is application-owned. A manifest reader recognizes a
sequential logical stream from its recipe, grows a bounded lookahead window,
and batch-reads only cache-missing following chunks into the application cache.
It never asks the kernel to populate page cache.

Pressure sampling includes ancestor cgroup limits and Linux PSI memory
pressure. Small process Swap residue quarters cache capacity instead of
flushing it; Swap beyond the grace band or sustained PSI pressure still closes
admission promptly.

## Proportional pressure shrink (18 October 2026)

Live ingest showed a sawtooth: one below-reserve available-memory sample
could collapse a warm multi-gigabyte target, after which refill took minutes
and every cycle re-read Exact and Similarity index pages from the metadata
device. The applied budget now moves toward a lower raw budget at a bounded
rate — at most 1/16 of the current budget per second under hard pressure and
1/64 under soft pressure — and a hard-pressure pool target moves only one
bounded step per refresh while growth is still rejected immediately. A lower
effective cgroup/host limit remains a hard ceiling at the unchanged 92%
boundary. Pinned Exact membership filters and page-key bounds stay outside
the evictable target, so the governor can shrink payload capacity to its
fixed floor without zeroing the Exact fast path. Sustained pressure still
reclaims the full payload; only the one-sample flush is removed.
