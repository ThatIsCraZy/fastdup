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

- Online proofs collect, for every ACTIVE Exact location whose dependency is
  a candidate Chunk, the edge from that Base to its dependent in the proof's
  reverse-dependency map. A victim record is relocated when it is a protected
  Chunk or a Base in that map. The Base keeps the length of its own record; a
  dependent's length never describes its Base.
- Offline planning reads the dependency of every location in every verified
  Container and classifies Containers only after that complete pass. Bases
  named anywhere count as required Chunks with their own record lengths.

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

## Open gap

Online proofs see dependents only through ACTIVE Exact locations. A Container
published before a process loss but never activated in Exact is invisible to
them, and startup deliberately does not scan the Container inventory (ADR
0091). Its dependents could lose a Base to a later online cycle, which scrub
then reports as an orphan. Offline planning is unaffected because it reads
every Container. The intended closure is for the background scrub, which reads
every Container before it opens the GC gate, to pin the Bases of dependents
outside the active Exact generation until the next completed round.

## Consequences

Codec-5 Bases survive online and offline GC with their own lengths. Garbage
dependents keep their Base's DATA until their own Containers are collected.
Tests in `crates/fastdup-testkit/tests/gc_dependent_bases.rs` cover live and
dead dependents under both GC paths, two targets of different lengths on one
Base, release of a Base after its last dependent, recovery finalization after
process loss during retirement, and a final scrub after every collection.
