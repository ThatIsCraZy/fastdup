---
status: accepted
---

# Keep the Base of every stored dependent record

A dependent record (codec 3, 4 or 5; ADRs 0010, 0088, 0103) names a Base
Chunk that must stay decodable as an independent record. Every full Container
decode resolves the Base of every dependent record in it: publication
verification, scrub, offline GC classification and online-GC victim reads.
A stored dependent without its Base therefore makes its whole Container
undecodable, and the background scrub of ADR 0090 turns that into a sticky
integrity failure.

On 20 October 2026 the test appliance reached that state: nine codec-5 records
had lost both of their Bases. Regression tests reproduced three GC defects:

- Offline GC (`scrub_for_gc` and `garbage_collect`) classified Containers only
  against Manifest-reachable Chunks. A Container that held only a Base used by
  a live dependent was removed as unreachable, and the live target became
  unreadable.
- Online GC protected only Bases of live dependents. A dead dependent outside
  the victim set kept its record while its Base was retired.
- Online GC recorded the Base under the dependent's logical length. A codec-5
  Base, whose length differs, then failed the proof with a length mismatch.

## Decision

A Base is required while any stored dependent record names it, whether or
not that dependent is reachable. The rule holds for every DATA-GC path.

- Online proofs collect every candidate Base named by an effective ACTIVE,
  RETIRING or QUARANTINED Exact Location: a RETIRING Container still stores
  its dependents until it is unlinked. A victim record is relocated when it is
  a protected Chunk or such a Base. The Base keeps the length of its own
  record; a dependent's length never describes its Base. The set is bounded by
  the proof's candidate Chunks.
- Offline planning first reads the authenticated structure of every published
  Container and collects the Bases named by its dependents. The full
  verification pass then classifies each Container as it streams and rejects
  any dependency the structural pass did not see. Bases named anywhere count
  as required Chunks with their own record lengths. Only Bases are held, not
  every stored Chunk of the pool.

A dependent that is removed in the same cycle still pins its Base for that
cycle. A pin ends exactly when every Container holding a dependent on that Base
has been removed or compacted; the following cycle may then release the Base.
The rule only over-retains and cannot livelock, but it promises no cycle count:
a victim whose remaining live data is only pinned Bases can be unprofitable and
is then left in place until its dependents' Containers go.

Scrub keeps its strict invariant: every observed dependent must resolve its
Base as an independent Chunk. A violation is structural corruption, because
the Container can no longer be decoded at all. Repairing a repository that
already contains such orphans is a rebuild, not an online policy.

## Dependents the Exact Index does not show

Exact activation is asynchronous (ADR 0015). A Container published before a
process loss, or one whose activation failed while the process degraded its
Exact publication, stores dependents that online proofs cannot see, and normal
startup does not scan Containers (ADR 0091). Such a dependent would lose its
Base to a later online cycle, and the next scrub would latch a sticky integrity
failure.

The startup scrub closes this. For every Container it certifies, freshly or by
journal resume, it looks up each dependent in its pinned Exact generation, one
batched lookup per Container. A dependent whose own Location in that Container
is not effective there pins its Base, keyed by the pinning Container. Without
an Exact generation every dependent pins. Each dependent is checked on its own
rather than letting one stand for its Container: Exact activates a Container's
Locations together today, but a missed pin would become a sticky integrity
failure, so the check does not depend on that. The pins are installed into the online maintenance repository
before the scrub opens the GC gate, so every proof that sees the open gate also
sees the pins and relocates a pinned Base like any other required Chunk. A pin
ends when its Container is unlinked and synced by GC. Pins are process-local;
a restart repeats the scrub with the gate closed.

The set is bounded by the in-flight work of one crash window or one degraded
activation. More than 262,144 distinct pinned Bases, or a failed Exact lookup,
keeps online GC closed for the process. That is not corruption: the scrub
still completes, writes stay admitted, and the warning names the reason.
Offline planning needs no pins because it reads every Container.

## Consequences

Codec-5 Bases survive online and offline GC with their own lengths. Garbage
dependents keep their Base's DATA until their own Containers are collected.
Tests in `crates/fastdup-testkit/tests/gc_dependent_bases.rs` cover live and
dead dependents under both GC paths, two targets of different lengths on one
Base, release of a Base after its last dependent, recovery finalization after
process loss during retirement, a dependent in a RETIRING Container, a scrub
pin for an unindexed dependent and its release, and a final scrub after every
collection.
