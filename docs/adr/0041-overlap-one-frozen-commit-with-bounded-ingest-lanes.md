---
status: accepted
---

# Overlap one Frozen Commit Cut with bounded Ingest Lanes

fastdup keeps exactly one Active Dirty Epoch and at most one Frozen Commit Cut.
Freezing does not stop later mutations: they remain visible in a new Active
epoch while the single commit writer persists the frozen prefix. Failed
publication retains the same frozen token and bytes for retry; crash recovery
selects one complete WAL generation and discards the Active epoch.

## Ordered ingest

Each hot inode has one ordered SeqCDC Ingest Lane fed by immutable views of the
same payload owned by its Dirty Extent Map. Per-inode observer ordering prevents
mutation sequences from overtaking during queue admission; different inodes may
reduce and publish concurrently. Sync, Release, and Commit wait through the
fixed ingest/publication prefix at or below their sampled mutation sequence.

At most eight registered Lanes are retained. When all are active, other inodes
use one serialized overflow Lane, trading reduction continuity for bounded
memory. Lane reset is in place: truncate, clone, unlink, rename-overwrite,
placement change, offset/sequence discontinuity, and staging failure may drain
stable work but cannot create two streams for one inode. Eviction requires idle
registry-only ownership; an in-flight Lane is never evicted.

The Dirty Extent Map remains authoritative until verified immutable recipes
replace its ranges. Advisory ingest failure or eviction therefore loses only
background reduction work, not accepted bytes or durability.

## Publication and resource ownership

Container generation reservation is separate from Container I/O. Exact
publication has one serialized lock from predecessor selection through Run-Set
activation, preventing concurrent publishers from dropping each other's hint
updates. Exact failure degrades acceleration only.

All encoders use counted permits from the shared CPU pool and release them
before slow storage durability. Queue, Lane, detached-publication, Drain Residue,
and resident Dirty-DATA accounting are independent bounded owners; ADRs 0050 and
0098 define their current limits and settlement. ADR 0093 defines commit-cut
Drain Residue; the former partial commit publication queue is superseded.

Stable work drained at a discontinuity may publish through the detached queue.
Prepared in-memory Exact/FILL recipes attach before releasing the Lane lock, so
a cut observes either the staged bytes or their recipe. Frozen completion uses
already verified size/allocation summaries and performs no fallible Metadata
read after the Commit WAL is durable.

The Exact publisher may coalesce up to eight queued ACTIVE-addition commands and
16,384 entries without crossing Flush or non-ACTIVE transitions. Allocation
names are monotonic; gaps and unselected temporary objects are valid.

Tests pair writer ordering and bounded ownership with recovery of the Frozen
prefix, later Active mutations, invalidation races, publication failure, and
multi-inode progress.
