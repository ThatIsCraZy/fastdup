---
status: accepted
---

# Overlap two publications for one active inode

The detached Container queue may publish two Containers concurrently when only
one inode has publication work. Two is both the worker count and the existing
64-MiB detached-payload budget divided by the 32-MiB Container target. The
queue does not add workers or memory for this policy.

When two or more inodes have pending or in-flight publication work, the queue
admits at most one publication per inode. A second publication that already
started before another inode became active may finish; the queue does not
cancel storage operations. This bounded exception avoids preemption inside the
Container publication protocol.

Publication and retirement are separate steps. Workers may prepare, write,
verify, sync, and rename two Containers for the same inode concurrently. Each
work item receives a per-inode ordinal at admission. The queue retires results
in ordinal order before it externalizes extents or degrades the inode. `Sync`,
`Release`, and commit fences wait until every admitted ordinal through their
mutation sequence has retired. Publication failures obey the same order.

## Evidence

The public storage seam test writes 70 MiB to one inode and pauses file sync.
It requires two Container publications to reach the durability barrier before
the test releases either one. Existing release, sync, recovery, fault, and
deduplication tests cover the ordered retirement boundary.

The 2026-08-24 Rocky ISO SMB comparison used frozen binaries, fresh repositories,
active `io_uring`, zero Swap, and alternating baseline/challenger order. Two
SingleStream pairs improved aggregate throughput by 13.2 and 15.2 percent.
Completed-write p99 fell by 21.1 and 29.5 percent. Four two-stream pairs all
improved aggregate throughput, from 2.2 to 32.3 percent, with a paired median of
18.2 percent. The slower stream changed by the same median, and p99 fell by
14.6 percent. The result therefore passes the no-MultiStream-regression gate.

## Consequences

One stream can keep both existing publication workers busy. Multiple active
inodes retain one slot each after the scheduler observes them. Per-inode
metadata effects remain ordered even when storage completion order differs.
The adaptive window changes checkpoint timing, so repository allocation and
data-reduction readings vary between short benchmark runs. Those values are
reported but are not treated as a deduplication comparison.
