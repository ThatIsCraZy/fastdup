---
status: accepted
---

# Discover GC candidates incrementally and prove victims locally

Online GC separates cheap candidate discovery from destructive authority.
Immutable Container summaries and a rebuildable `GcCandidateCatalog` may rank
likely victims without a preceding full Scrub. Only a generation-bound
`GcCandidateProof` may authorize RETIRING, Location replacement, pin drain and
unlink. This replaces ADR 0048's full-Scrub prerequisite, not its pressure,
ordering, identity, revalidation or directory-sync invariants.

## Hints and authority

Container envelope version 2 mirrors a 96-byte, 64-byte-aligned intrinsic
summary in Header and Footer: encoded/decoded bytes by codec, record and Chunk
geometry, and dependency shape. Decoding validates both copies and their layout;
recovery and scrub derive the facts again from authenticated records. Mutable
liveness, pin state and victim scores never enter this summary.

`GcCandidateCatalog` is immutable-run acceleration updated from publication,
Metadata-liveness and Location-generation changes. Its v1 rows are 96-byte,
Container-ID sorted, protected by paired 4-KiB envelopes, freshness bindings and
a whole-stream BLAKE3 digest. Publication merge-joins bounded updates with the
previous generation. Empty generations are durable tombstones. A stale,
missing or approximate catalog may waste work or suppress a cycle, never
authorize deletion.

Without a catalog, Online GC snapshots sorted published names, counts them and
streams summaries without payload reads or a pool-sized map. Concurrent
publication belongs to a later refresh. Liveness advances as a delta between
the catalog's protected two-generation window and the current one. Exact
lookups only attribute likely Containers; incomplete/negative results remain
hints and count underflow clears the estimate.

A `GcCandidateProof` binds:

- current and previous Commits, pinned Active/Frozen Manifest roots and open
  orphan DATA dependencies;
- Exact and paired Similarity generations;
- victim identities and Recovery Indexes; and
- the complete target/Base dependency closure and replacement coverage.

The proof uses the same Metadata Root Pin registry that protects immutable
graphs. A process-local Reverse Dependency Generation resolves every protected
target through a complete effective ACTIVE Location prefix and authenticated
Base edges. Missing or incomplete resolution fails closed. The projection is
bound to the Commit pair and Exact activation, held during a running proof and
otherwise evictable under ADR 0046.

Final revalidation holds the Metadata publication barrier and Commit lock until
the selection barrier and RETIRING Exact generation are active. No new root or
Namespace commit may enter that interval. The victim set must still fit the
bounded replacement budget; ADR 0065 defines the current profitability estimate
and keeps the independent-RAW bound as proof data rather than a collection gate.
A protected graph with no DATA Chunks permits replacement-free retirement.

## Retirement and recovery

Replacement publication and RETIRING transitions activate atomically in one
Exact L0 generation. New scan fallback closes before activation; displaced
generation admission closes at activation. Unlink waits all reader, writer and
reduction-operation pins, verifies victim identity, removes DATA, syncs its
directory, then publishes REMOVED tombstones. Scrub authenticates all transition
bytes but follows only effective ACTIVE dependencies.

Readers select and pin the current Exact generation per bounded DATA read.
Dormant file objects do not pin predecessor generations; explicit old snapshots
fall back to verified discovery after admission closes. Every path still checks
Container identity, Record integrity and reconstructed Chunk identity.

Before writable admission after restart, the recovery finalizer completes any
effective RETIRING transition. Present victims must fully verify and reproduce
the RETIRING Location set; already absent victims may reflect a completed,
directory-synced unlink. Finalization is idempotent and failure blocks writable
mount.

Candidate proof also consumes both Recovery Checkpoint graphs required by ADR
0020. A process-local projection may cache their protected Chunks by immutable
head identity `(generation, file length, body hash)`. Every hit rereads both
4-KiB heads and checks object length. The cache starts empty, retains only active
heads, is bounded by Chunk count, and never serves recovery or scrub.

Normal scans use bounded leased Direct I/O; adapters without a lease use bounded
positional reads and re-audit. Batches and shortlist retention are limited to
4,096 rows. No path casts file bytes to Rust structs. Fault tests cover stale
bindings, incomplete dependency closure, activation races, every retirement
interruption and restart completion.
