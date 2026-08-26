# Online GC container summaries and victim selection

Status: research note, not an accepted architecture decision  
Last reviewed: 2026-08-25

## Question

Which immutable per-Container metadata and which rebuildable pool metadata would
let fastdup identify completely dead Containers and profitable partially-live
compaction sets quickly, while permitting online, candidate-local proof instead
of requiring a complete End-to-End Scrub before every GC plan?

## Conclusion

Add a small, exact, writer-derived **intrinsic summary** to both the Container
Header and Footer, but do not put liveness, reference counts, `RETIRING`, pin
counts, or GC scores there. Those values change after an immutable Container is
published and cannot be recovered from that Container alone.

The useful split is:

1. The Container carries facts that remain true for its entire lifetime:
   record and Chunk counts, encoded and decoded bytes by codec, independent and
   dependent target counts, and outgoing dependency counts. These facts make a
   4 KiB envelope read sufficient for a first-pass I/O/CPU classification.
2. A rebuildable, mmap-friendly `GcCandidateCatalog` tracks generation-bound
   live/garbage estimates and exact writer-known totals. It can be refreshed
   incrementally from namespace and Location-generation changes and periodically
   replaced by a complete scrub result. It discovers candidates but never
   authorizes retirement or deletion.
3. A `GcCandidateProof` is built for only the selected Containers. It binds a
   fixed generation pair, verifies the candidate envelopes and Recovery Indexes,
   proves logical reachability and dependency closure for the candidate Chunk
   set, and proves replacement coverage. This is the destructive authority.
4. Speculative relocation may happen from hints before that proof because it
   only creates additional verified Locations. `RETIRING`, removal from the
   active Location generation, pin drain, and unlink require the candidate-local
   proof and revalidation.

This deliberately changes ADR 0048. A new ADR must partially supersede its rule
that DATA GC may consume only a plan produced by one complete successful
End-to-End Scrub. ADR 0048's pressure thresholds, exact identity check,
replacement-before-deletion ordering, and directory `fsync` remain sound. Full
scrub remains a periodic audit and catalog-rebuild source, not a per-cycle gate.

Do **not** add per-Container Bloom/Xor filters, Chunk-ID min/max bounds,
HyperLogLog, Count-Min sketches, or immutable live-byte counters in format v2.
At fastdup's Container and Chunk geometry they either duplicate an exact local
Recovery Index, answer the wrong question, or become stale. Approximate filters
remain useful in rebuildable pool-wide indexes, where a false positive only
causes extra work.

## Primary-source evidence

### Immutable summaries identify contents; mutable usage selects victims

Sprite LFS writes a segment summary with the identity of every stored item. The
cleaner uses that identity to consult current metadata and determine whether the
item is still live. Separately, its segment usage table records live bytes and
the most recent modification time for victim selection, and that mutable table
is checkpointed and repaired during roll-forward. In other words, the segment
does not claim its own current liveness. [Rosenblum and Ousterhout, Sections 3.3,
3.6, and 4](https://www.cs.cmu.edu/~15712/papers/rosenblum92.pdf)

The same separation survives in F2FS. Its Segment Summary Area stores owner
information, while the Segment Information Table stores valid-block counts and
validity bitmaps. Foreground GC uses a greedy valid-block policy; background GC
uses age plus valid blocks. The upstream `seg_entry` has current and checkpoint
validity maps, valid-block counts, and modification time, and the victim policy
has bounded-search state. [Linux F2FS documentation](https://docs.kernel.org/filesystems/f2fs.html),
[upstream `segment.h`](https://github.com/torvalds/linux/blob/master/fs/f2fs/segment.h)

This is directly applicable to fastdup: the Recovery Index is already the local
content summary, while generation-derived liveness estimates belong to a side
structure and exact liveness belongs to a bound proof.

RocksDB's integrated BlobDB is an even closer immutable-file example. Static
blob-file facts include total blob count and bytes. Garbage blob count and bytes
are tracked in the MANIFEST/Version metadata, and live size is computed as total
minus garbage for the current Version. Blob files themselves remain immutable
and participate in lock-free Versions. Its GC can relocate blobs during ordinary
compaction, and a garbage threshold can force targeted compaction of old blob
files. [RocksDB BlobDB design](https://github.com/facebook/rocksdb/wiki/BlobDB),
[Version blob statistics](https://github.com/facebook/rocksdb/blob/main/db/version_set.h)

This supports an incrementally maintained fastdup candidate catalog: exact
Container totals belong to the immutable envelope; generation-relative garbage
estimates belong to the activated metadata Version. Unlike BlobDB, fastdup must
still derive final authority from namespace roots plus dependency closure rather
than treating counters as liveness truth.

### Physical enumeration scales with stored data, not logical references

Data Domain's production physical GC replaced file-by-file logical enumeration
with sequential passes over physical metadata. Its Container metadata contains
Chunk fingerprints; selection checks those fingerprints against a live vector,
computes Container liveness, sorts the results, and selects a threshold based on
the amount of space it intends to reclaim. The reported redesign improved the
mark phase by roughly 10--60% for common cases and by up to two orders of
magnitude for extreme high-deduplication or high-file-count workloads.
[Douglis et al., FAST '17](https://www.usenix.org/system/files/conference/fast17/fast17-douglis.pdf)

That system also demonstrates a safe use of approximation. A Bloom false
positive retains a dead Chunk but never deletes a live one. When the whole live
set does not fit in memory, it samples fingerprints to shortlist candidate
Containers, then performs another exacting pass focused on those candidates.
Its later PGC+ design uses perfect-hash live vectors, per-Container duplicate
counters, reverse-order processing, prefetching, and NUMA-aware memory layout.
These are side structures for one GC run, not immutable Container claims.
[FAST '17, Sections 3.1--3.4](https://www.usenix.org/system/files/conference/fast17/fast17-douglis.pdf)

### Cleaning cost is more than live-byte ratio

The LFS write-cost model reads victim segments, writes their live bytes, and
divides the work by the new free space. Purely greedy utilization selection can
repeatedly clean hot data. Its cost-benefit policy therefore incorporates age
and groups rewritten blocks of similar age; its hot/cold simulation reduced
write cost by as much as 50% relative to greedy selection. This is evidence for
separate urgent and background policies, not a transferable threshold.
[Rosenblum and Ousterhout, Sections 3.4--3.6](https://www.cs.cmu.edu/~15712/papers/rosenblum92.pdf)

F2FS makes exactly that operational distinction: on-demand cleaning minimizes
immediate movement by choosing the fewest valid blocks, while background
cleaning uses cost-benefit age information. It also bounds how many candidates
one search examines. [F2FS documentation](https://docs.kernel.org/filesystems/f2fs.html),
[victim policy in `segment.h`](https://github.com/torvalds/linux/blob/master/fs/f2fs/segment.h)

Ceph SeaStore exposes the same choice in current upstream design: `GREEDY`
chooses low utilization, while `COST_BENEFIT` uses `(1-u) * age / (2u)` and a
third formula weights age more aggressively. SeaStore also maintains hot and
cold rewrite generations rather than treating every rewritten extent alike.
[SeaStore SegmentCleaner documentation](https://docs.ceph.com/en/latest/dev/crimson/seastore/)

For fastdup's first policy, use Sprite LFS's conventional benefit/cost shape
`(1-u) * age / (1+u)` when `u` is the exact candidate utilization: `(1-u)` is
space gained, `age` discourages repeatedly cleaning young/hot data, and
`(1+u)` represents reading the victim plus writing its live fraction. The exact
denominator must then be replaced by measured compressed-record work, as below.

For compressed deduplicated data, the cost must include codec and record
geometry. Data Domain's active-tier copy-forward reads source Containers,
checks their metadata against the live vector, decompresses compression
regions, and recompresses live Chunks. Its cloud implementation instead copies
whole live compression-region byte ranges without decode/re-encode, accepting
that a region cannot be reclaimed until all its Chunks are dead. This is direct
evidence for tracking fully-live, fully-dead, and partially-live records
separately. [Duggal et al., ATC '19, Section 7](https://www.usenix.org/system/files/atc19-duggal.pdf)

### Online resurrection and snapshots must be explicit

Data Domain first snapshots the namespace for a consistent GC view. During
online GC, a new write may resurrect a Chunk that the snapshot considered dead.
The system feeds incoming Chunks into the live vectors; if a resurrection races
the range currently being copied, it writes a duplicate for safety and removes
it in a later cycle. Open files not yet reachable from the root are separately
enumerated. [Douglis et al., Sections 3.1 and 3.5](https://www.usenix.org/system/files/conference/fast17/fast17-douglis.pdf)

A different commercial deduplicating CAS uses epoch-based, failure-tolerant
reference counting while allowing reads, writes, and deduplication to continue.
It demonstrates that concurrent deletion is possible, but also how much
protocol machinery is needed before a counter can authorize reclamation.
[Strzelczak et al., FAST '13](https://www.usenix.org/system/files/conference/fast13/fast13-final91.pdf)

For fastdup, root-derived reachability remains the liveness authority, but it
need not be coupled to a complete data-pool scrub. A candidate-local traversal
can mark only the finite Chunk-ID set present in selected Containers, provided
it starts from every root protected by the bound generation pair and closes all
encoding dependencies. Online writes should publish into a generation newer
than the proof or be covered by the `RETIRING` barrier; they should not mutate a
counter in an old Container.

### Activation and pin drain are metadata-generation concerns

RocksDB installs compaction output by recording file additions and removals in
its MANIFEST. Atomic groups are buffered during recovery and are applied only
when the whole group was decoded. It explicitly warns that embedded table-file
metadata cannot reconstruct the last consistent database state after arbitrary
filesystem failures. [RocksDB MANIFEST design](https://github.com/facebook/rocksdb/wiki/MANIFEST)

RocksDB also keeps old immutable files after a new Version becomes current when
an existing `get` or iterator still pins the previous Version. New readers use
the current Version; physical deletion follows release of the old one.
[RocksDB live-SST lifecycle](https://github.com/facebook/rocksdb/wiki/How-we-keep-track-of-live-SST-files)

This closely matches `ACTIVE -> RETIRING -> REMOVED`: the durable Location-Set
generation selects the files, and in-memory generation pins delay unlink. The
Container Header must not attempt to represent either state.

Badger's value log supplies a compact implementation example of the drain. GC
rewrites live values, but if iterators are active it adds the old file ID to
`filesToBeDeleted`. The last iterator decrement drains that list and deletes the
files. Its source describes `numActiveIterators` as the refcount that gates the
pending deletes. [Badger `value.go`](https://github.com/dgraph-io/badger/blob/main/value.go),
[iterator lifecycle](https://github.com/dgraph-io/badger/blob/main/iterator.go)

fastdup needs generation-scoped pins rather than Badger's coarse global count,
but the sequencing is the same: logical replacement first, physical deletion
only after readers that can still name the old file have exited.

### Similar-size grouping reduces repeated rewrite work

Cassandra's size-tiered strategy waits for a default of four similarly sized
SSTables and merges them into a larger run. RocksDB Universal Compaction
likewise offers a similar-size stopping rule and explicitly trades lower write
amplification against higher read/space amplification. [Cassandra STCS](https://cassandra.apache.org/doc/stable/cassandra/managing/operating/compaction/stcs.html),
[RocksDB Universal Compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction),
[RocksDB compaction options](https://github.com/facebook/rocksdb/wiki/Compaction)

The transferable idea is bucketing by **predicted live replacement bytes**, not
original Container file size. fastdup Containers have a common upper bound, but
two 64-MiB victims can contain 2 MiB and 30 MiB of live data respectively. Merge
sets should combine similar relocation sizes and compatible codec/dependency
classes so a small candidate is not repeatedly rewritten with a much larger one.

### Dependencies change liveness and relocation cost

The first storage prototype combining deduplication and delta compression notes
that GC must account for both ordinary references and delta-base references. It
rejects reference counts as the sole resilient truth and uses mark-and-sweep;
it also observes that copy-forward changes physical locality and can reduce
later similarity opportunity. [Shilane et al., HotStorage '12](https://www.usenix.org/system/files/conference/hotstorage12/hotstorage12-final38_0.pdf)

Data Domain's cloud copy-forward shows the benefit of preserving an already
encoded region byte-for-byte when every contained Chunk is live. fastdup can use
the same principle only when the record and every dependency are valid under the
new Location generation; partially-live multi-Chunk records require verified
decode and regrouping under ADR 0009. [Duggal et al., ATC '19](https://www.usenix.org/system/files/atc19-duggal.pdf)

Incoming Base fanout, dependent logical bytes, and Base hotness are therefore
valuable score inputs, but are mutable graph properties. A Container can
durably summarize only its **outgoing** dependency edges.

## Technique assessment for fastdup

| Technique | What it answers | Fit for <=64 MiB Containers | Placement |
|---|---|---|---|
| Estimated live bytes and live record classes | Early candidate discovery | Useful when incrementally maintained; staleness may only waste work | Candidate catalog |
| Exact candidate-local live bytes and dependency closure | Whether retirement/removal is safe | Essential; derived from bound roots, Location generation, and verified candidate Recovery Indexes | Candidate proof |
| Age/utilization cost-benefit | Avoids repeatedly moving young/hot data in background | Good, using monotonic generations rather than wall-clock time | Derived side score |
| Greedy lowest-live-byte selection | Minimizes urgent foreground relocation | Good under hard space pressure | Derived side score |
| Exact codec/record counters | Predicts read, decode, recompress, and zero-copy work without reading payload | Cheap and permanently true | Mirrored Header/Footer summary |
| Per-record live-state histogram | Distinguishes dead, whole-live-copyable, and partially-live records | High value; estimates in catalog, exact state in proof | Side metadata/proof |
| Bloom filter | One-sided approximate membership; false positives retain garbage | Useful for pool/run miss filtering, but not for exact liveness or dependency closure | Rebuildable index only |
| Xor/Binary Fuse filter | Faster/smaller static approximate membership | Attractive for immutable Exact/Similarity Run families; cannot be updated and does not provide liveness cardinality | Rebuildable immutable run only |
| Chunk-ID min/max | Range pruning | Poor unless physical placement is Chunk-ID ordered | Do not add |
| HyperLogLog | Approximate distinct cardinality | Recovery Index already gives exact local cardinality; error buys nothing | Do not add |
| Count-Min sketch | Approximate frequency/heavy hitters | Base fanout can be counted exactly from the dependency-edge generation or candidate proof; overestimation would distort victim cost | Do not add initially |
| Immutable reference/live count | Would be cheap to query | Incorrect after namespace or Location-Set changes | Never in Container |

The filter tradeoff is concrete. A Bloom filter near a 1% false-positive rate
requires about 9.6 bits per key. With roughly 1,024 64-KiB-average Chunks in a
64-MiB Container, that is about 1.2 KiB; with 16,384 small Chunks it is about
19 KiB and no longer fits in the fixed Header. Xor filters are static and use
about 23% over the information-theoretic bound; Binary Fuse filters reduce that
overhead further while retaining fast queries. Neither structure computes exact
intersection cardinality, and fastdup already has all exact Chunk IDs in its
Recovery Index. [Xor filters](https://arxiv.org/abs/1912.08258),
[Binary Fuse filters](https://arxiv.org/abs/2201.01174)

**Inference for fastdup:** min/max is especially weak for BLAKE3-distributed
IDs. For `n` uniform IDs, the
expected covered fraction from minimum through maximum is `(n-1)/(n+1)`; at
1,024 IDs that is about 99.8% of the key space. The bounds would reject almost
nothing unless Containers were deliberately range-partitioned, which would
conflict with ingest and restore locality.

HyperLogLog can estimate very large cardinalities to about 2% using roughly
1.5 KiB, and Count-Min supports mergeable approximate frequency queries.
Those are useful pool-scale streaming tools, but a fastdup Container already
stores exact IDs and normally has only thousands of them. [HyperLogLog paper](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf),
[Count-Min paper](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)

## Recommended immutable format summary

Use one fixed 96-byte `ContainerIntrinsicSummaryV1`, serialized field by field
and mirrored in the existing 4 KiB Header and Footer:

| Field | Bytes | Meaning |
|---|---:|---|
| summary version and flags | 4 | Explicit decoding and compatibility |
| RAW/Zstd/Prefix record counts | 12 | Exact codec work classes |
| independent/dependent Chunk counts | 8 | Target and Base-eligibility split |
| encoded record bytes by codec | 24 | Headers, tables, payload, and record padding; sums to the record area |
| decoded logical bytes by codec | 24 | Decode/re-encode upper-bound input |
| single-/multi-Chunk record counts | 8 | Likelihood of partial-record regrouping |
| outgoing dependency edges | 4 | Exact codec-3 edge count |
| unique outgoing Base IDs | 4 | Base-read/retention diversity |
| reserved, zero | 8 | Future compatible extension |

This consumes 192 bytes of space that is already reserved across Header and
Footer, so it adds no Container bytes and no extra I/O. At seal time all fields
can be accumulated while records are emitted; no second scan and no new hash is
required. A Header-only classifier still reads one 4 KiB page. The Footer mirror
and existing structural commitment protect the summary with the envelope.

Every new invariant must be checked at all current durable boundaries:

- the writer recomputes counts and byte sums from emitted records;
- the ordinary reader verifies Header/Footer equality before trusting them;
- recovery treats the summary as structural evidence, never as liveness;
- offline scrub recomputes the complete summary from record headers, Chunk
  tables, dependency IDs, and the Recovery Index;
- fault injection flips each field and exercises a valid-CRC but inconsistent
  summary, a torn Footer mirror, and a writer-side miscount.

Do not serialize score weights or age buckets. `container_generation` already
provides a stable creation order. If GC later needs age lineage across
copy-forward, keep it in the rebuildable candidate catalog first; a lineage field should
enter the durable format only after its recovery semantics are defined.

## Two-stage side metadata and proof

### Stage 1: incremental candidate discovery

Maintain an immutable-run family named `GcCandidateCatalog`, analogous to Exact
Index generations rather than one mutable database. New commits and Location
generations append small delta runs; background compaction folds them into
larger runs. A complete End-to-End Scrub may replace the family with an exact
checkpoint, but is not required before every selection cycle.

Container publication seeds the static totals directly from
`ContainerIntrinsicSummaryV1`. Each namespace commit emits a rebuildable
Metadata-liveness delta for Chunk IDs entering or leaving the protected
current/previous pair; the catalog joins those IDs to the activated Location
Sets and adjusts per-Container estimates. Location relocation emits a separate
delta moving estimated coverage between Containers. Deltas must distinguish
logical reachability from physical encoding dependency edges. If a delta is
missing, out of order, or cannot be joined, mark the affected row unknown and
lower its selection priority until a local proof or full scrub repairs it.

Use fixed-width rows suitable for sequential construction, mmap, SIMD-friendly
integer comparisons, and bounded parallel scans. A 96-byte core row can hold:

- Container ID, creation generation, physical bytes, and immutable-summary
  checksum;
- estimated reachable target count and encoded coverage;
- conservative independent-RAW replacement upper bound;
- estimated dead-record, wholly-live-record, and partial-record bytes;
- estimated live independent-Base count;
- incoming Base fanout and dependent logical bytes;
- outgoing dependency count;
- active/retiring/quarantined eligibility flags;
- the newest Commit and Location generation incorporated into the estimate.

At an average 32-MiB Container size, 96 bytes per Container costs about 3 MiB
per TiB of pool capacity: roughly 300 MiB at 100 TiB and 3 GiB at 1 PiB. Keep
per-Base and per-record detail in separate sparse runs only for shortlisted
Containers.

Each catalog family binds its input-generation interval, row count, byte length,
checksum, and structural commitment. Rows may lag the current Commit generation;
that makes a score inaccurate but cannot authorize deletion. Do not patch rows
in place. A newer immutable delta supersedes an older estimate, and a missing or
corrupt family falls back to envelope/inventory sampling.

This is the fast path for answering "worth examining?", including finding likely
zero-live Containers. It is not the answer to "safe to remove?".

### Stage 2: generation-bound candidate-local proof

For a bounded victim set, build a durable `GcCandidateProof` that contains or
commits to:

- the current and immediately previous Commit Record hashes, every protected
  Active/Frozen root, and every open-orphan DATA dependency;
- the active Location-Set generation and Exact/Similarity family IDs;
- the exact victim Container IDs, generations, canonical lengths, and complete
  independent verification of their envelope, record CRCs, decoded Chunk IDs,
  dependencies, Recovery Index, and structural commitment;
- targeted verification of an external independent Base record when decoding a
  victim Prefix target requires it; unrelated records in that Base Container do
  not need a full scrub;
- the finite set of Chunk IDs physically present in the victims;
- the result of traversing all bound logical roots while filtering marks to
  that finite set;
- the transitive encoding dependency closure, which is depth-one for Prefix
  under ADR 0010 but must still include incoming edges to victim Bases;
- the verified replacement Location for every reachable victim Chunk and every
  independently decodable Base required by the selected encodings;
- proof format/algorithm versions and one structural commitment.

The local mark may still traverse namespace metadata, but it does not read or
scrub unrelated DATA Containers. Candidate membership can use an in-memory exact
hash set or sorted IDs; a Bloom filter may reject most unrelated manifest Chunks
only if every positive is confirmed against the exact set.

Incoming dependency discovery needs an exact `BaseChunkId -> dependent
Location` edge run selected by the active Location generation. If that run is
missing, the safe fallback is to scan all active Recovery Index metadata for
dependency IDs. An approximate filter may shortlist edge partitions, but a
false negative must be impossible and the final edge set must be exact.

This proof, not the candidate catalog and not a stale reference count, is the
authority consumed by `RETIRING` and Location-Set activation. If any bound root
or generation changes before the barrier is committed, discard/rebase the proof.

## Victim and merge policy

Maintain two policies, as LFS and F2FS evidence suggests:

### Urgent space pressure

1. Shortlist estimated zero-live Containers first, then prove them locally. A
   proved zero-live candidate requires no replacement write, although the
   candidate-local proof still verifies that selected Container completely.
2. Then minimize relocation cost per net byte reclaimed. Use integer bytes and
   a bounded candidate scan.
3. Reject any set whose conservative RAW replacement upper bound plus reserved
   publication headroom cannot fit.

### Background compaction

Rank candidates approximately by:

```text
net_reclaim = victim_file_bytes - replacement_raw_upper_bound

cost = victim_read_bytes
     + replacement_raw_upper_bound
     + codec_cpu_units
     + dependency_and_locality_penalty

score = net_reclaim * age_weight / max(cost, 1)
```

Use `current_container_generation - candidate_generation` as the initial age
signal. Exact weights need workload benchmarks; they are policy, not format.
Keep utilization bands (for example, zero, then eighths) only in the side-run
selection code so thresholds can change without a format migration.

For a merge set, first bucket candidates by similar conservative live
replacement bytes and compatible codec/dependency class, then use bounded
first-fit decreasing into <=64-MiB output bins. Cassandra and RocksDB's
similar-size strategies motivate avoiding merges in which a small run is
repeatedly rewritten with a much larger run. A set is profitable only
when predicted output Container count is lower and net reclaimed bytes exceed
publication headroom and policy thresholds. Two nominally "half-full"
Containers are not automatically a good pair: partially-live multi-Chunk
records, codec CPU, shared Chunks already covered elsewhere, Base dependencies,
and restore locality can make their replacement cost very different.

During speculative verified relocation or proof construction, refine the
estimate from exact candidate record states:

- fully dead record: skip;
- fully live independent record: byte-copy only if the format supports a
  verified record-preserving path;
- partially live multi-Chunk record: decode, verify every retained Chunk, and
  regroup;
- Prefix target: preserve only with a verified independently-decodable Base in
  the new Location generation, otherwise materialize independently;
- independent Base: relocate once by logical Base Chunk ID; do not rewrite its
  children, but include incoming fanout and dependent bytes in risk/locality
  scoring.

## Crash-safe online sequence

Candidate selection must remain restartable and non-authoritative:

1. Acquire the durable Appliance Lease.
2. Read `GcCandidateCatalog` hints and choose a bounded victim set. Stale hints
   may cause a no-op but not deletion.
3. Optionally read and verify selected Containers and publish speculative
   replacements. Until activation, these are harmless additional Locations.
4. Build `GcCandidateProof` by scanning current+previous Manifest metadata (or
   an exact Live Vector cryptographically bound to those roots), the active
   dependency-edge run, and the selected Containers completely.
5. Serialize with Commit/Location activation, revalidate every proof binding,
   and commit victims as `RETIRING`. New Exact reuse and Similarity Base
   selection must exclude them, while old-generation readers may continue.
6. If the final local mark found newly live or resurrected victim Chunks, publish
   and verify their replacement Locations before proceeding. Failure leaves the
   victim readable in `RETIRING`.
7. Atomically activate a Location-Set generation that includes replacements and
   excludes victims; rotate coherent Exact/Similarity and dependency-edge
   snapshots.
8. Wait for reader, writer, and reduction-snapshot pins on the old generation to
   drain.
9. Recheck the durable activated generation, verify exact canonical Container
   identity, unlink victims, and fsync the Container directory.
10. Reclaim obsolete metadata generations only after their own pins drain.

A crash before `RETIRING` or activation leaves extra verified replacements. A
crash after activation but before unlink leaves harmless retired garbage. A crash during
unlink leaves a durable subset of deletions, recoverable from the activated
Location generation. This follows the same general rule as RocksDB: immutable
files are outputs, but one separate transactional generation determines which
files are current. [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST),
[RocksDB live-SST lifecycle](https://github.com/facebook/rocksdb/wiki/How-we-keep-track-of-live-SST-files)

## Concrete recommendation

Proceed in this order:

1. Accept a new ADR that defines Header/Footer facts, incremental catalog hints,
   and candidate-local proof as separate trust levels. It must explicitly
   supersede ADR 0048's mandatory complete-scrub gate while retaining its
   destructive ordering and space-pressure policy.
2. Add and fully validate the 96-byte intrinsic summary; use it only for cost
   classification and observability.
3. Build `GcCandidateCatalog` from Container publication facts plus immutable
   Metadata-liveness delta runs. Let periodic End-to-End Scrub audit and repair
   the catalog rather than gate each cycle.
4. Implement `GcCandidateProof` for likely zero-live Containers: scan the bound
   current+previous Manifest metadata (or their exact bound Live Vector), close
   dependencies, and verify only selected Containers completely.
5. Add the durable dependency-edge run and candidate-local proof cases for live
   Prefix targets and victim Bases.
6. Enable online `RETIRING`, replacement activation, generation-pin drain, and
   physical unlink for proved zero-live candidates.
7. Add speculative relocation, bounded partial-live victim-set packing,
   similar-live-size buckets, and conservative RAW estimates.

This gives fast selection without making approximate metadata a deletion
authority. Complete scrub remains a periodic repair/audit path, and the Container
format remains independently recoverable if every GC side index is lost.
