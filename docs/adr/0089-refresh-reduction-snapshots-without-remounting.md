---
status: proposed
---

# Maintain Similarity as an online immutable-run index

ADR 0063 deliberately pins one immutable Exact/Similarity pair for an entire
mount. Independently encoded Chunks written after that mount therefore cannot
become Similarity Candidates until an offline full-pool rebuild and remount.
That makes Advanced Reduction progressively less useful on a long-lived
appliance.

The Exact index already solves the corresponding online-publication problem:
verified updates become immutable level-zero runs, bounded fan-in compaction
folds runs into higher levels, a Run Set atomically selects the serving state,
and generation leases keep the displaced state alive for in-flight readers.

The [50-version Linux kernel benchmark](../benchmarks/linux-6.12-tar-reduction-2026-09-02.md)
also shows why an online design must be cheap and bounded. Preparing one
25-version candidate universe required an offline full-pool build and remount,
while Similarity saved only 0.0668% of target allocation with the current
codec and corpus arrangement.

The supporting [online-index research](../research/online-similarity-index-without-remount-2026-09-02.md)
compares LevelDB/RocksDB, Linux VDO/UDS, OpenZFS DDT/Fast Dedup, and Lucene's
near-real-time segment lifecycle against fastdup's constraints.

## Decision

Maintain Similarity incrementally using the same immutable-run and atomic
generation pattern as Exact. Do not require a remount, repeated full-pool
rebuilds, or a separately queried mutable online overlay.

After a Container is durably published, the ordinary Exact publication batch
contains every new Exact Location transition. The associated Similarity batch
contains only newly published independent Chunks, because dependent encodings
must never become bases. Each Similarity mutation contributes its four Bucket
Keys and the complete candidate identity needed for ranking and verification.

Unlike Exact's point values, a Similarity run stores a replacement value:

```text
BucketKey -> complete newest BucketState containing at most 64 Chunk IDs
```

For every affected Bucket Key, the serialized publisher reads the current
effective state, merges all new independent Chunk IDs from the batch, sorts
deterministically, and retains the smallest 64. The L0 run contains only
changed keys, but their values are complete rather than candidate deltas.

Publishing a batch performs these conceptual steps:

1. Publish and activate the Exact L0 transition using the existing Exact
   repository path.
2. Materialize and publish an immutable Similarity L0 run containing the
   complete replacement state of every affected Bucket Key.
3. Compact only selected Similarity runs when the fan-in or active-run bound
   requires it; never rescan the full Chunk pool during ordinary publication.
4. Durably activate a Reduction Head naming the complete ordered set of active
   Similarity runs and recording the Exact identity observed by publication.
5. Swap the process-local immutable Similarity view. Each query retains that
   view and then pins the current Exact generation for candidate resolution.

If steps 2 through 5 fail, Exact remains usable and the previously active
Reduction generation remains selected. The unpublished Similarity artifacts
are reclaimable garbage. Similarity may safely lag Exact because a missing
candidate only loses an optimization opportunity.

## Durable shape

Physical Similarity runs are immutable, sorted, independently checksummed
files of replacement Bucket states. The binding to an Exact generation moves
out of an individual physical Similarity family and into a small Reduction Run
Set manifest. This separation allows old Similarity runs and a new L0 run to be
reused under a newly selected Exact Run Set without rewriting the old runs.

The Reduction Head is the candidate-state activation boundary. It names:

- the Exact Run Set identity observed by publication, as provenance;
- chronological run references, their compaction levels and manifest hashes;
- generation and format identities needed for recovery and scrub.

The active view owns leases on mapped Similarity runs, not a long-lived Exact
pin. Code inspection found that every Exact activation closes admission to
the displaced Exact snapshot. Keeping a mount-lifetime Exact partner would
therefore disable queries after the next ordinary write, or hold GC back.

Similarity entries are hints, not authoritative Locations. A query acquires
its immutable Similarity view first and then a short current Exact pin. A
missing/deleted hint is a miss; a usable base must resolve to a verified
independent record. The existing depth-one codec and GC dependency closure
remain mandatory. The publisher captures only an Exact identity and releases
the pin before metadata compaction. It does not scan or resolve DATA.

Two checksummed activation slots select an old or new complete candidate run
set after a torn head write. Recovery audits all dependencies of the newest
complete head and fails the optional index on a corrupt selected dependency;
it does not silently fall back in that case. Exact can advance independently.
The head's provenance identity is not a lease on historical Exact files.

A dependent-write publication guard additionally covers the interval from
Base selection through durable target Exact activation. It shares the
Container lifecycle state with the maintenance I/O view. GC's final selection
barrier rejects a stale proof while such a writer is active; a writer starting
during RETIRING immediately falls back to independent encoding. This closes
the gap that a short candidate-read pin alone cannot cover. Successful target
Exact publication invalidates any earlier GC proof before releasing the guard.
If that publication fails, one guard is retained until owner teardown and
further advanced selection is disabled, keeping GC from deleting an unindexed
target's base. Similarity queue/compaction work never retains this guard.

## Query and compaction bounds

A query probes newest state first for each of its four Bucket Keys and decodes
only the first replacement value found for each key. It therefore examines at
most `4 * 64 = 256` stored representatives regardless of the number of runs.
Candidate ranking remains capped at 16 and trial encoding at 4 as required by
ADR 0018.

Run count still bounds negative point probes. Publication must compact or omit
Similarity admission before activating a generation that exceeds the
hard active-run bound. The implementation uses at most 24 families, batches
of at most 4096 entries, and chronological tiered fan-in four. Partitions
within each family have disjoint Bucket-Key ranges; separate families may
overlap even at the same level. A capacity preflight rejects additional hint
admission before writing when compaction cannot satisfy the family bound.
The two-batch queue is nonblocking for Exact/DATA publication; excess hints
are counted and dropped. One background worker owns Similarity compaction.

Compaction is a local newest-value-wins merge of selected runs. It drops
shadowed Bucket states. Chronological first/last sequence intervals, not the
physical generation number of a newly compacted family, determine freshness.
Stale representatives are currently resolved as misses at query time, not
pruned during compaction. It is not a full index rebuild.

## Process interface

The appliance should depend on a small dynamic Reduction repository rather
than storing the mount-time `Arc<PersistentReductionIndex<_>>`. Its interface
needs only to pin the current coherent generation, publish one verified batch,
and report status. Run layout, compaction selection, activation, recovery, and
lease retirement stay hidden behind that boundary.

There is no separately queried RAM-only overlay. A bounded publication queue
may batch verified mutations until an immutable L0 can be committed, as the
Exact publisher already does. Entries in that queue are deliberately invisible
to Similarity queries; they become candidates only when their Reduction Run
Set is durably activated.

There is deliberately no second Similarity WAL. An acknowledged DATA write
does not promise that its hint has reached the index. The immutable L0 and
head are the durable admission unit; a queued tail can be lost after a crash
or omitted under load without losing file contents or Exact deduplication.
This refines the earlier journal proposal in favour of write-path performance.

## Share writer policy

Each Share can explicitly select `off` or `dependent_v1`; an absent override
inherits the repository default for compatibility. Both the default and Share
overrides change live. New UI-created Shares start explicitly off. Policy is
resolved once per Container planning batch by inode membership, not per Chunk
by a path walk. The disabled path bypasses fingerprinting, candidate lookup,
trial encoding and new hint admission. Exact deduplication and ordinary
compression remain enabled; existing dependent records remain readable.

Enabled Shares share the pool-wide candidate index and Exact deduplication
domain. This is an encoding policy, not a new security or storage-isolation
boundary. Cross-policy-subtree hardlinks/renames are rejected with `EXDEV` to
avoid an inode with ambiguous ownership. In-flight planning can finish with
the policy it already acquired. Policy is saved with Share settings and
reapplied by inode before the recovered mount serves requests.

## Rejected alternatives

### Mutable on-disk hash table

An extensible or open-addressed hash table could provide direct online
insertion. It would also require a WAL or copy-on-write page tree, crash replay,
checksummed mutable-page updates, concurrent resize/split handling, independent
scrub logic, and a safe snapshot mechanism. It conflicts with fastdup's
existing immutable Run, redirect-on-write publication, and generation-lease
model without removing the need for journaling and batching.

### Periodic full-pool rebuild

This preserves the current file format but repeatedly rereads all eligible
data, consumes minutes on the measured corpus, and makes visibility depend on
rebuild cadence. It remains an offline repair/bootstrap mechanism, not the
steady-state update path.

### Separate online overlay

A mutable overlay makes recent entries visible but creates two different
query, durability, recovery, and memory-governance paths. An immutable L0 run
provides the same freshness while remaining part of one durable index model.

### Candidate-delta LSM

Appending up to 64 representatives per Bucket Key in every run looks simple,
but a query over `R` runs could inspect `4 * 64 * R` representatives. Complete
replacement Bucket states preserve ADR 0018's fixed 256-representative bound.

## Acceptance constraints

- Candidate query and trial-encode bounds from ADR 0018 remain unchanged.
- Each visible Bucket Key resolves to exactly one newest complete
  `BucketState64`; older values are shadowed rather than unioned at query time.
- Dependent encodings never become Base Chunks, preserving dependency depth
  one.
- Every visible Similarity view names one complete recoverable run set; every
  candidate DATA lookup uses a pinned Exact generation and verifies eligibility.
- Crashes at every publication boundary recover an old or new complete
  candidate run set; Exact may advance independently.
- Old mappings and files remain protected until the last generation pin is
  released.
- Writer, recovery, and offline scrub validate the new durable identities and
  invariants, with fault injection at each activation boundary.
- Initial A/B CPU/allocation and blocked-publisher tests are recorded with the
  implementation. Real-device L0 latency, negative-probe amplification and
  compaction I/O still require workload qualification before default-on use.

## Consequences

New independent Chunks become eligible Similarity bases after the next small
L0 publication, without stopping or remounting the namespace. The price is a
second tiered immutable-run repository, complete-value Bucket updates, and a
small two-slot activation record. This reuses the
failure and lifetime model already paid for by Exact instead of introducing a
mutable metadata subsystem.

The implementation is available while this ADR remains proposed pending the
real-device performance qualification above. Normal retirement respects both
head slots and reader leases. Unselected artifacts from an interrupted
publication may remain on disk for offline maintenance; no DATA or source
corpus is removed by this feature. Existing offline rebuilds remain an
explicit bootstrap/repair path and can replace the candidate universe.

See [the implemented head format](../formats/online-similarity-head-v1.md).
The [implementation and A/B evidence](../benchmarks/online-similarity-share-policy-2026-09-05.md)
records the tests, measured costs and remaining real-device qualification.
The subsequent [50-version Linux-kernel A/B](../benchmarks/linux-6.12-online-similarity-2026-09-05.md)
demonstrates the permanent queue on separate physical Metadata/DATA disks:
23.84:1 total repository reduction versus 6.59:1 with the Share disabled,
at 42.25% lower effective write-through throughput.

The existing checkpoint Policy Set identity remains unchanged: its historical
`paired-exact-v1` and `contiguous-only` text does not introduce new codec bytes
or permitted dependency depth. Fragmented targets are materialized once before
the existing fingerprint/codec oracle; readers require no new DATA format.
