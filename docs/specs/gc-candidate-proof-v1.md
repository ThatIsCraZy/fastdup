# GC Candidate Proof v1

Status: implemented for bounded same-process online execution. Candidate
discovery and proof construction do not require a preceding End-to-End Scrub or
DATA-pool directory scan. Automatic scheduling and the cross-process durable
Appliance Lease remain deferred.

This module implements the bounded proof tier from
[ADR 0064](../adr/0064-discover-gc-candidates-incrementally-and-prove-victims-locally.md).
The `GcCandidateCatalog` is only an input shortlist. No catalog estimate,
negative Exact lookup, Bloom result, or serialized victim score becomes
deletion authority.

## Proof bindings

One opaque process-local proof binds:

- the exact current and immediately previous Commit Records;
- the catalog descriptor and its incorporated generations;
- the selected Exact activation record, profile, and Run Set;
- at most 64 fully verified victim Container identities, generations, and
  physical lengths;
- the unique logical Chunk identities and lengths found in those victims;
- the independent-RAW replacement upper bound and positive projected gain.

The protected Commit pair is derived from immutable Namespace/Manifest metadata
only. Every victim is then read and fully verified through the selected Exact
generation, including Record CRCs, decoded Chunk IDs, Recovery Index bijection,
structural commitment, and codec-3 Base resolution.

## Conservative dependency closure

The current Exact Index is rebuildable acceleration and cannot prove that a
Location or reverse Base dependency is absent. Proof v1 therefore requires
replacement of every unique logical Chunk found in every victim, not only the
Chunks directly reachable from protected Manifests.

This deliberately over-preserves dead-looking Chunks. It guarantees that an
unknown live dependent encoding outside the victim set cannot lose a Base when
the victim disappears. It also means a candidate set is accepted only when:

```text
independent_raw_replacement_upper_bound < total_victim_physical_bytes
```

Proof construction stops before its cumulative publication-derived RAW bound
would exceed 64 MiB. A first candidate exceeding that bound is rejected. The
replacement writer retains its existing 48-MiB/32,768-Chunk batch bounds.

## Execution order

Online maintenance execution performs:

1. revalidate the exact protected Commit pair and Exact activation record;
2. reread victims and publish verified replacements for the complete proof
   Chunk closure;
3. revalidate both bindings again;
4. exclude victims from new scan-fallback selection;
5. atomically activate one Exact L0 generation containing ACTIVE replacements
   and RETIRING transitions for every victim Location;
6. close admission to the displaced Exact generation and drain reader, writer,
   and in-progress reduction pins from every still-live predecessor Exact
   generation;
7. reread exact victim identities, unlink them, and sync the DATA directory;
8. append REMOVED tombstones in a subsequent Exact L0 generation.

Any changed Commit pair or Exact activation rejects the proof as stale before
deletion. A failure before replacement/index activation retains old coverage; a
failure afterward leaves either additional verified Locations or only a subset
of already safe victim removals.

The newly activated Exact generation necessarily differs from the proof's input
binding. The proof's old Exact binding is checked immediately before the atomic
replacement/RETIRING transition and is not required afterward. Ordinary L0
publication and GC use the same repository-wide generation transaction, so a
concurrent write-through activation cannot omit a RETIRING transition. The L0
writer validates each physical Location's durable state machine and rejects a
late `ACTIVE` append after `RETIRING`, preventing transient write-through work
from resurrecting a victim.

## Boundary invariants

| Boundary | Required evidence |
| --- | --- |
| liveness producer | clean contiguous Commit WAL; exact current/previous Namespace roots; complete Manifest traversal; Chunk-length consistency |
| catalog delta | base Commit generation matches descriptor; selected Exact generation is recorded; underflow becomes unknown |
| proof constructor | bounded shortlist; full victim verification; exact identity/length match; all victim Chunks included; positive conservative gain |
| executor | Commit and Exact revalidation before work and after replacements; replacement-first Exact activation; identity reread before unlink |
| offline scrub | independently verify the resulting Namespace, Containers, and active Exact object graph |
| fault injection | every interrupted replacement, activation, or deletion recovers with complete live Chunk coverage |
