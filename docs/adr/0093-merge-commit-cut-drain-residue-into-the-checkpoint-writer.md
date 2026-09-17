---
status: accepted
---

# Merge commit-cut Drain Residue into the checkpoint Writer

This record supersedes ADR 0041's dated 2026-09-16 section "Shared commit-cut
drain batching". The per-inode publication queue keeps its two remaining jobs:
immediate publication when staging reaches the Container flush threshold, and
detached publication when an Ingest Lane resets at a discontinuity.

At the Frozen Commit Cut, complete Pending Chunks no longer travel through the
publication queue as partial Containers. They are detached as Drain Residue and
handed directly to the single-writer `AdaptiveCommitWriter`. The Writer seeds
its Container buffer with the already-hashed staged bytes at `begin_inode`, and
the Manifest planner emits a DATA extent — or a clipped DATA_SLICE — at the
staged Offset instead of rereading and re-cutting those bytes. Skipping the
queue eliminates one Container encode, one durable write, one externalization
round trip, and the group/barrier/reservation machinery that partial-batch
publication required.

Re-chunking the residue would not be equivalent: checkpoint planning starts its
SeqCDC anchor where each planned Range starts, while the Lane cut where its
retained suffix ends. Re-cutting would produce different Chunk identities for
identical bytes and emit duplicate physical payload, so the planner must honor
the staged boundaries. Because a suffix-anchored Chunk legitimately reaches
below a changed Range's first Offset, the planner clips a straddling Chunk to
the Range and references the staged Chunk as a slice. Bytes outside the Range
are already committed at those Offsets by an earlier Manifest; a Residue Span
that no planned Range touches is dropped, and its seeded buffer entry
deduplicates against the active Exact Index.

The drain filters by the frozen cut. Only Chunks whose through-sequence is at
or below the cut's per-inode mutation sequence become Residue; newer post-cut
staging stays resident in the Lane for the next cut instead of being charged to
this commit. This is what makes a checkpoint captured before a truncate and a
same-inode rewrite safe: the drained Residue always describes exactly the bytes
the frozen view still owes.

Resident staging bytes and Drain Residue share one gated region of 304 MiB, the
400 MiB write-through budget minus the 32 MiB ingest queue and the 64 MiB
detached-publication budget. Staging workers reserve worst-case growth before
taking a Lane lock and block only on this gate; the drain is a net-zero
transfer that never blocks, and checkpoint planning is the absorption path, so
a blocked staging worker always has an independent release route through the
active checkpoint. Lane release on reset and residue Drop (canceled checkpoint
or an inode excluded from the commit) return unused charge. Status totals count
the Residue region against the unchanged process memory budget.

Metrics replace the retired `publicationEnqueue` phase with
`checkpointDrainResidue` and count merged payload in `drain_merged_chunks` and
`drain_merged_bytes`; those bytes are no longer visible as Exact recipe reuse.
A drained Residue Container becomes part of the committed generation instead of
a sealed uncommitted partial Container, so post-cut evidence of a partial Lane
clears within the same checkpoint.

## Paired invariants and evidence

- Writer: the commit drain detaches every Chunk the frozen cut still needs and
  retains only post-cut Chunks, asserted against the cut's per-inode fence.
  Reader/recovery: `truncate_and_new_container_publication` freezes a cut, then
  truncates and rewrites the same inode, and both generations recover
  byte-exactly.
- Writer: every Residue emission clips inside its planned Range and consumes
  one seeded Chunk. Recovery: `append_checkpoint_rechunks_only_bounded_suffix`
  and `append_graph_proof_reads_are_bounded_by_changed_suffix` exercise the
  suffix-anchored slice across the plan boundary and recover byte-exactly.
- Writer: staging admission, drain transfer, absorption, and reset release keep
  the gated ledger exact (`settle_staging` asserts retained growth against the
  reservation). Runtime test: a full gate blocks one staging reservation until
  a release path frees space; Drop releases unabsorbed charge.
- End-to-end: `checkpoint_flushes_stable_partial_lane_before_forming_the_frozen_cut`
  and `sequential_writes_publish_reduced_data_before_the_namespace_commit`
  require the drained prefix to appear in `drain_merged_bytes` and leave zero
  sealed uncommitted Containers after the commit.

## Admission closure drains advisory ingest admission (17 September 2026)

The preceding release route is independent only after a Frozen Commit Cut exists.
A checkpoint timeout can request a mutation-admission fence while already
admitted writes still hold their per-inode Observer fences and are waiting for
Ingest queue space. Those writes cannot be drained by the checkpoint until the
fence is granted, while the checkpoint cannot grant the fence until they finish,
so blocking indefinitely on advisory Ingest admission is a deadlock.

A transient pause now closes admission atomically, notifies the write-through
observer, and seals each affected Ingest Lane without admitting new advisory
fragments. The Namespace mutation already owns the Dirty Extent Map; skipping
the remaining advisory queue fragments merely loses background reduction until
admission reopens. Admission closure is therefore a drain signal for already
admitted work, not another unbounded memory gate. Management and Online-GC state
reads use the atomic admission state and cannot queue behind the draining fence.

The writer/recovery invariant remains unchanged: `begin_commit` still takes the
admission fence and cannot overtake an accepted observer. Closure only makes the
observer complete that handoff by leaving bytes resident instead of blocking
forever. Fault-injection test:
`checkpoint_pause_releases_writers_blocked_by_ingest_backpressure` fills the
ingest queue while Container durability is blocked, closes admission, requires
the admitted writer to finish and remain live without DATA durability, and then
reopens admission.
