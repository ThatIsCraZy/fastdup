# Exact publisher: bounded work per activation

The [live pipeline diagnosis](pipeline-live-2026-09-13.md) found repeated Exact
queue saturation, 6–13 second checkpoint publication waits, and closed mutation
admission for 60% and 42% of two 39-second windows. Every new L0 enumerated the
Metadata directory to select its next Run number. That directory contained
80,706 names; an independent filename listing alone took 30.920 ms. These were
measurements of RPM 0.7.4-14, before this change.

## Implemented changes

- One constant-sized Run generation allocator is shared by Repository clones.
  Its first allocation scans canonical names, including unselected orphan Runs
  across profiles; steady online append performs no directory enumeration.
  Standalone publication and compaction advance its observed high-water before
  I/O. Automatic compaction reserves its entire output partition range before
  publishing any partition. A failed reservation is not reused by that owner;
  a fresh owner independently discovers published names.
- The Exact queue coalesces at most eight already available ACTIVE-addition
  commands and 16,384 total entries. A larger single command runs alone. There
  is no batching delay, and neither Flush nor a non-ACTIVE transition is crossed.
  Identical additions coalesce; distinct Locations survive. The resulting L0,
  Run Set and activation amortize their file/directory/WAL syncs. The channel
  remains bounded at eight commands, with at most one pending boundary command
  held by the worker in addition to the current batch.
- Every original DATA-reference admission remains held through activation,
  Similarity handoff and overlay retirement. Failure retains one shared GC
  admission; a later success does not release protection for that earlier error.
- L0 page bounds and Bloom hints use the validated Run entries directly. Newly
  encoded RAM pages no longer undergo an immediate decode/CRC pass. The same
  encoded bytes enter the Unified Read Cache. Stored-page validation remains at
  independent demand-read, recovery and scrub boundaries.
- Online predecessor selection no longer clones the complete activation slot
  or reconstructs its already installed lookup-family directory. The WAL writer
  consumes its bounded snapshot, validates the successor and extends its buffer.
  Rotation moves the last encoded record as the exact bridge within that same
  allocation. It no longer copies and decodes the full known prefix on every
  append. Any write/sync failure discards the snapshot; standalone activation,
  recovery and offline audit still decode stored bytes independently.
- Recent-overlay cleanup uses one BTree search per entry and direct first-entry
  removal at the bound.

The generation-publication lock, transition validation, final activation sync,
immutable no-replace publication, and Namespace durability protocol remain.
This change requires no new storage format or cache layer.

## Controlled A/B result

One test submits the same 32 ACTIVE entries through the actual publisher worker
and StorageIo interface. The baseline places a Flush after each command; the
combined variant makes those commands available together. Both independently
recover all 32 exact references and pass the activation audit.

| Work | Individual | Combined |
| --- | ---: | ---: |
| Input publication commands | 32 | 32 |
| L0/Run-Set activations | 32 | 4 |
| StorageIo calls, all kinds | 2,082 | 201 |
| WriteAt | 106 | 13 |
| File sync | 108 | 15 |
| Root sync | 75 | 10 |

That is 87.5% fewer activations and 86.3% fewer sync calls. Counts include the
compaction work induced by each layout. These are deterministic in-memory
StorageIo operation counts, not physical device IOPS, XFS write bytes, or a live
SMB throughput measurement. A workload with an empty queue has no coalescing
opportunity; a saturated queue gains the most.

The real-filesystem allocator test performs 18 appends, standalone publication,
standalone family compaction, automatic compaction and Repository-clone access
with exactly one directory scan. Reopening the Repository performs one new
scan and skips a higher orphan Run from another profile. Failed rename before
and after effect consumes its number on retry; generation exhaustion leaves
existing Runs and activation intact.

The WAL test compares every one of 130 owned appends against independently
loaded storage, spanning two rotations. Both byte and decoded-record allocations
remain stable after the first append; the slot capacity stays at 256 KiB.

## Monitoring

`pipeline.operations` adds two rows:

- `exactGenerationDiscovery`: initialization scans of the online Run allocator.
  Normally one completion per writer owner, then flat throughout ingest. Explicit
  offline rebuild discovery is a separate operation.
- `exactPublishBatch`: one processing interval for each combined publication.
  `exactPublish` continues counting original publication commands. Their completed
  count ratio indicates commands per activation attempt; failures also count.

Queue wait still includes blocked senders and a dequeued boundary command until
its processing begins. Batch and per-command processing times overlap and must
not be added. UI labels cover both new rows. Existing admission-closure time,
queue depth, checkpoint publication waits, Run publish, compaction and activation
metrics provide the post-deployment comparison.

## Validation

- 15 Exact repository unit tests: writer evidence/cache independence, directory
  scan bound, orphan/reopen handling, reservation errors/exhaustion, existing
  WAL error recovery, shared readers and corrupt stored pages.
- One WAL buffer/recovery test spanning two rotations.
- 16 Exact repository/activation integration tests, including generation pins,
  transition-state rejection, partitioned compaction and corrupt peers.
- 12 Exact fault-matrix tests covering fail-before/fail-after publication,
  replacement activation, online append, compaction and paired-slot rotation.
- 63 appliance library tests passed; two existing ignored tests remain ignored.
  New cases cover real Flush/transition boundaries, entry/command bounds,
  duplicate additions, failed combined publication with retained GC protection,
  and the operation-count comparison above.
- 12 durable-Namespace integration tests: read-free cold Exact reuse, memory
  pressure, more than 64 compacting checkpoints, index-publication failure,
  sparse/clone/update behavior and byte-exact recovery.
- Two pipeline UI tests; TypeScript check and production UI build.
- Clippy for store, appliance and format libraries/binaries with warnings denied;
  `git diff --check` clean.

Logs are workspace-local under `.artifacts/tmp/exact-publisher-*.log`; the UI
build is under `.artifacts/exact-publisher-ui/`. No live Repository restart or
package installation was performed during this change. The live improvement
must be measured after deployment with the Veeam job running.
