# Delta chain depth: compression benefit and system cost

Status: research note, not an accepted architecture decision
Last reviewed: 2026-08-15

## Question

Should fastdup permit a DELTA or ZSTD_PREFIX encoding to depend on another
dependent encoding (maximum decode depth 2 or 3), instead of requiring every
base chunk to be independently decodable (maximum depth 1)?

This note is deliberately narrower than the general question of whether delta
compression is useful. Delta compression itself has strong evidence. The open
question is the *incremental* value of dependency levels 2 and 3 after exact
deduplication and local compression.

## Conclusion

Keep maximum decode depth 1 for format v1 and the first implementation, but do
not close the decision permanently. Depth 2 is worth one controlled benchmark;
there is no direct evidence yet that depth 3 adds enough beyond depth 2 to
justify production complexity.

Do not add a universal hard cap on the number of depth-1 deltas that may share
one base. The reviewed work supports tracking fanout and dependent bytes, using
them in cost-aware selection, caching, verification, placement, and protection,
and re-anchoring when locality degrades. It does not identify a workload-stable
fanout threshold whose benefits exceed the compression and cache reuse it can
forfeit. This is distinct from the strict depth-1 rule: depth has an immediate,
serial decode and validation cost on every target, whereas fanout is chiefly a
shared-risk and workload-ordering property.

The closest published backup result found here reports that unrestricted
multi-level candidate selection adds `1.03x` to `1.18x` compression over depth
1 on multi-terabyte, multi-month backup sets. In terms of output bytes, those
factors correspond to about `2.9%` to `15.3%` less storage (`1 - 1/factor`). The
paper's figure caption separately describes the improvement as `6%` to `30%`;
the paper does not publish isolated depth-2 and depth-3 results. This is enough
to reject the claim that deeper chains are always negligible, but not enough to
make them a v1 invariant.

There is no directly applicable study comparing depths 1, 2, and 3 separately
with approximately 64 KiB CDC chunks, Zstd, immutable 32--64 MiB containers,
and fastdup's restore/GC rules. All conclusions beyond the cited measurements
are therefore identified below as inference or project policy.

## Direct evidence

### Closest match: multi-terabyte backup streams

Shilane et al. evaluate five backup data sets collected over three to seven
months, sized 4.6--12.9 TB. The default experiment uses 8 KiB average CDC
chunks, 4.5 MiB containers, and roughly 128 KiB local-compression regions. The
workloads are large tar-like backup streams containing repeated full and
incremental backups, source repositories, workstations, email, system logs,
and home directories. This is the closest workload and architecture match in
the sources reviewed here. [FAST '12 paper, Sections 5 and 6](https://www.usenix.org/legacy/events/fast/tech/full_papers/Shilane.pdf)

Their unrestricted experiment allows a chunk that was previously delta encoded
to become the logical base of a later delta. A depth-1 variant invalidates the
sketch of every delta-encoded chunk so it can never become a base. Compared
with that depth-1 variant, unrestricted multi-level selection provides:

- `1.03x` to `1.18x` additional total compression in the paper text;
- `6%` to `30%` improvement in the associated figure caption; and
- for the source-code set, a total compression factor increase from `178x` to
  `194x` (about `1.09x`, or `8.25%` fewer output bytes).

The authors call depth 1 a reasonable approximation when multi-level storage
is impractical. They also state that decoding an n-level stored delta entails n
base reads. Crucially, their replication destination decodes transferred deltas
before storing the chunk. It receives the compression-selection benefit of
logical multi-level history without paying a persistent multi-level decode cost.
[FAST '12, Section 6.3](https://www.usenix.org/legacy/events/fast/tech/full_papers/Shilane.pdf)

This source does **not** tell us how much of the unrestricted improvement comes
from depth 2, how much comes from depth 3, or whether still deeper candidates
matter.

### A storage prototype chose depth 1 after that measurement

A follow-up storage prototype uses 8 KiB average CDC chunks and explicitly
implements only depth 1 based on the `1.03x`--`1.18x` result. It reports that
arbitrary depth makes throughput unpredictable because an n-level delta needs
n base-chunk reads. [HotStorage '12 paper, Section 2.1](https://www.usenix.org/system/files/conference/hotstorage12/hotstorage12-final38_0.pdf)

The same prototype quantifies the cost of delta storage even at depth 1:

- after the first full backup, ingest throughput averaged `53%` (standard
  deviation `18%`) of its dedup-only baseline;
- synchronous, single-stream base reads on HDD achieved only `1`--`5 MB/s`
  across its data sets; and
- it attributes the low end to roughly 10 ms seeks for individual 8 KiB chunks.

These are unoptimized 2012 prototype results, not predictions for fastdup and
not costs caused solely by chain depth. They do show that base locality and I/O
can dominate the delta path. [HotStorage '12, Section 3.1](https://www.usenix.org/system/files/conference/hotstorage12/hotstorage12-final38_0.pdf)

The prototype also sweeps CDC chunk size from 1 KiB to 1 MiB and finds that
64 KiB retains total compression similar to its normal 8 KiB configuration
because delta compression offsets some exact-dedup loss. It does not repeat the
depth experiment at 64 KiB. [HotStorage '12, Section 3.3](https://www.usenix.org/system/files/conference/hotstorage12/hotstorage12-final38_0.pdf)

### Whole-file archives show a larger possible effect, with weak transferability

PRESIDIO compares depth 1 with unbounded whole-file delta chains. Stored size
falls from `28.3%` to `4.3%` of input for ten Linux source releases, from
`24.9%` to `7.7%` for HTML, and from `47.0%` to `20.0%` for mail. Other sets
show smaller differences. This proves that deeper chains can be decisive for
some incremental, evolving archives. [PRESIDIO, ACM TOS 2011, Sections 6.4.2--6.4.3](https://ssrc.us/media/pubs/33ef15dd54e7d4175fe554cff674cb6e386038c6.pdf)

The result is not a prediction for fastdup: it uses whole-file xdelta, mostly
small files, and an incremental model in which a file already stored as a delta
cannot otherwise serve as a reference. It also compares depth 1 to unbounded
depth rather than depths 2 and 3. In the same Linux experiment, careful base
selection reduces average chain length from 24.26 to 5.83 with almost unchanged
stored size, and a hard cap of 8 costs about 3% relative to longer chains. This
supports a bounded, cost-aware policy, not unlimited chaining.
[PRESIDIO, DOI 10.1145/1970348.1970351](https://doi.org/10.1145/1970348.1970351)

An older backup analysis reaches the complementary conclusion: a star-like
"version jumping" layout, equivalent to depth 1 between full anchors, loses only
a small amount of compression against a linear chain for its modeled backup
policies while bounding restore to two stored-file accesses. The result is an
analytical model based on adjacent-version change rate, not a 64 KiB chunk
experiment. [Burns and Long, Efficient Distributed Backup with Delta Compression](https://research.ibm.com/publications/efficient-distributed-backup-with-delta-compression)

### Current chunk-level work still protects independent bases

LoopDelta (ACM TOS 2025) uses Rabin CDC with 2 KiB minimum, 8 KiB average and
64 KiB maximum chunks, Xdelta, Zstd, and 4 MiB containers. It explicitly treats
delta-compressed chunks as unsuitable bases because of inline decode cost and
prefetches only non-delta chunks as candidates. Despite that depth-1 constraint,
its locality, cache, rewrite, and inverse-delta policies achieve `1.28x` to
`11.33x` compression over basic deduplication and up to `3.57x` restore
improvement, depending on workload and comparison. These numbers establish the
importance of base choice and locality; they are not depth comparisons.
[LoopDelta author PDF](https://shuibing9420.github.io/assets/pdf/LoopDelta-TOS25.pdf),
[DOI 10.1145/3721485](https://doi.org/10.1145/3721485)

LoopDelta also demonstrates that a duplicate referring to a delta which refers
to its base already creates a two-level lookup path even though the delta codec
itself is depth 1. Each relationship may require a query and I/O. Its
cache-aware handling of base fragmentation improves restore under rewriting by
`10.3%`--`46%`. This is evidence that locality work can be more valuable than
adding another codec dependency level.

Git is a useful implementation counterexample, not a fastdup baseline. Git pack
files default to maximum delta depth 50 (maximum 4095), while the official
documentation warns that every additional level must be applied during unpack.
Git's object sizes, repacking model, access patterns, and recovery contract are
materially different from a POSIX backup appliance.
[git-pack-objects documentation](https://git-scm.com/docs/git-pack-objects)

## Base fanout: shared benefit and shared risk

Here, **logical delta fanout** means the number of live DELTA/PREFIX encodings
that directly name one independently decodable chunk as their base. It is not
the same as (a) the number of manifests that exact-deduplicate to a chunk, (b)
the number of likely future similarity matches, (c) the number of requests to a
base's physical container, or (d) the number of physical locations holding the
base. Those quantities are correlated in some streams but require separate
counters and policies.

### Direct evidence: popular bases can improve reduction and cache reuse

REBL shows why selecting the individually best resemblance match is not always
globally best. It reports that such a policy can leave roughly half the chunks
as independent reference blocks. Allowing a target to use a base whose match is
within `80%`--`90%` of the best match concentrates more version blocks on fewer
bases and improved total encoding effectiveness in its experiment. This is
direct evidence for a compression benefit from base reuse, but REBL used
1--4 KiB CDC chunks and did not measure restore cache behavior or reliability.
[REBL, USENIX ATC 2004, Sections 3.1 and 5.3.2](https://www.usenix.org/legacy/event/usenix04/tech/general/full_papers/kulkarni/kulkarni_html/paper.html)

Git's primary implementation documentation explicitly caches fully decompressed
base objects that are referenced by multiple deltified objects to avoid repeated
unpacking and decompression; the default cache budget is 96 MiB per thread. This
establishes a real implementation benefit from repeated-base reuse, not a
quantitative result transferable to 64 KiB backup chunks.
[Git `core.deltaBaseCacheLimit`](https://git-scm.com/docs/git-config#Documentation/git-config.txt-coredeltaBaseCacheLimit)

MeGA provides a more relevant caution: it counts base-*container* references
within each 20 MiB segment and declines delta compression when candidates reside
in containers referenced too sparsely to read efficiently. Thus the useful unit
for I/O is often the number and density of base containers, not logical fanout
of one base. A popular base is cache-friendly only when its children are decoded
close enough in time for the base or container to remain resident.
[MeGA, USENIX ATC 2022, Section 4.2](https://www.usenix.org/system/files/atc22-zou.pdf)

ShieldReduce similarly batches chunks whose bases occupy few containers and
reconstructs locality by making recent, adjacent chunks new bases when old
bases become scattered across backup generations. Its locality/offloading design
reduces enclave boundary calls by up to `83.3%` and raises data reduction per
such call by up to `4.6x`; those are ingest/SGX measurements, not restore-cache
speedups. Most importantly for fanout policy, its offline re-anchoring orders
old bases by number of associated delta children and processes the *least*
popular first, stopping at a configurable storage-reduction target. Re-anchoring
a popular base is expensive because its children must also be rewritten. This
is direct evidence for a fanout-aware cost function, not a hard admission cap.
[ShieldReduce, USENIX ATC 2025, Sections 3.3 and 5.2](https://www.usenix.org/system/files/atc25-yang-jingyuan.pdf)

### Direct evidence: fanout enlarges dependency and retention consequences

PRESIDIO demonstrates an extreme dependency concentration in whole-file delta
graphs: only five independent reference files anchored 88,323 Linux source
files. It explicitly warns that losing a highly depended-on root loses every
file with a path to it, and proposes materializing a version as an independent
reference when the system detects a large degree of dependence. Its base
selection nevertheless prefers higher dependence after resemblance and chain
length, because concentration also improves storage. This simultaneously shows
the risk and the reason a simple cap is unattractive. It is whole-file,
multi-level evidence rather than a depth-1 chunk result.
[PRESIDIO, Sections 6.4.3 and 6.4.4](https://ssrc.us/media/pubs/33ef15dd54e7d4175fe554cff674cb6e386038c6.pdf)

Bhagwat et al. quantify the analogous risk for shared CDC chunks in a 9.8 GB
evolving web archive distributed over 179 devices. With `6%` of devices failed,
about `99.5%` of chunks remained available but only `96%` of original data was
reconstructable. Rather than limit sharing, they assign replicas as a
logarithmic function of either dependent-file count or dependent logical bytes.
Their selective scheme exceeded the robustness of mirrored LZ-compressed files
while using about half as much storage in reported configurations. This is the
strongest reviewed evidence for protecting high-impact shared records according
to consequence; it concerns exact shared chunks, but the same dependency applies
to a depth-1 delta base.
[Bhagwat et al., MASCOTS 2006, Sections 3, 4, and 6](https://www.ssrc.us/media/pubs/b12095ad1fd69f6b792f37a40298cd3c005df4f7.pdf)

Windows Server Data Deduplication is useful primary implementation evidence for
the same response. Its `ChunkRedundancyThreshold` defaults to 100 references;
crossing the threshold duplicates the hot chunk into a hotspot area so it has
multiple access paths. The product promotes/protects a popular chunk instead of
forbidding further references. The value 100 is a Microsoft workload default,
not a demonstrated universal threshold and not specific to delta bases.
[Microsoft advanced deduplication settings](https://learn.microsoft.com/en-us/windows-server/storage/data-deduplication/advanced-settings#available-volume-wide-settings)

Retention has the same graph property. Burns and Long keep an expired base in
an inactive-dependent state until its last delta child is gone, then reclaim it
with two-phase deletion. Their local reference-count algorithm relies on an
acyclic temporal graph. fastdup should derive the equivalent liveness from
manifests and encoding dependencies rather than trust refcounts, but cannot
avoid retaining a base while any live encoding needs it.
[Burns and Long, Sections 3.2--3.3](https://ssrc.us/media/pubs/16ebd13a478545ec198444a87aac277282f5d494.pdf)

### Inference for fastdup

- High fanout can amortize one base read and decode across many nearby targets,
  but does nothing for a cold random read and may yield no cache reuse when its
  children are far apart. Base-container density and reuse distance are better
  immediate predictors of I/O benefit than fanout alone.
- At depth 1, loss of an unrecoverable base makes every exclusive child
  encoding unavailable. The blast radius is therefore proportional to live
  dependent logical data, not merely the child count. End-to-end hashes detect
  this loss but do not repair it. Protection must cover exact-dedup hubs too;
  a delta-only fanout cap would leave the same systemic risk untreated.
- With logical `BaseChunkId` references, relocating one base to a verified new
  location need not rewrite any child, regardless of fanout. A physical-location
  reference would make relocation cost and risk scale with fanout and is ruled
  out by fastdup's existing design. Retiring the old location remains illegal
  until the replacement is durable and the dependency closure remains readable.
- GC copies a live base once, not once per child, but high fanout can keep an
  otherwise unreferenced base alive for a long time. Re-encoding or changing the
  logical base of existing children is `O(fanout)` immutable rewrite work, as
  ShieldReduce's ordering policy illustrates.
- A hard cap bounds direct delta children but can create more independent
  anchors, lose repeated-base cache value, and reject the best compression
  candidate. No reviewed primary source measures a universal fanout cap across
  backup workloads. The hard limits found in the literature apply to chain
  depth or replica count, while logical fanout is handled by cost-aware
  selection, selective protection, promotion, or re-anchoring.

## Costs for fastdup

The following are analytical consequences of fastdup's proposed format unless a
source is cited. They are not measured performance results.

### Cold read and restore

Let codec depth `d` count DELTA/PREFIX edges, so independent RAW/ZSTD data has
depth 0. A cold read of one target needs the target record plus every record on
the path to its independent anchor:

| Maximum depth | Critical records | Relative record count vs depth 1 |
| --- | ---: | ---: |
| 1 | 2 | 1.0x |
| 2 | 3 | 1.5x |
| 3 | 4 | 2.0x |

The table is a dependency count, not an I/O prediction. Container colocation,
base caches, coalescing, and fanout can turn several record dependencies into
one device read; poor placement can turn every edge into a random read. Delta
decoding itself remains serial along a single path. A sequential restore may
amortize shared bases, while a cold random 64 KiB read cannot assume that cache
benefit.

### Garbage collection and relocation

At depth 1, physical-encoding liveness is the manifest's live logical chunks
plus a one-hop set of bases and dictionaries. At depth 2 or 3, GC must compute a
recursive transitive closure, prove it is acyclic, and retain or relocate every
member before retiring an old location. Copying immutable records remains safe
only if the committed location switch preserves the entire closure.

HotStorage '12 directly warns that duplicate copies combined with delta
references can create multiple reconstruction paths; incorrect cleaning can
form reference loops and lose data. LoopDelta separately notes that recording a
base's container directly becomes stale when GC relocates the base. These are
reasons to reference a logical `BaseChunkId`, as fastdup plans, but logical IDs
do not remove recursive liveness or cycle validation.

### Corruption and integrity blast radius

If record failures were independent with probability `p`, reconstruction of a
depth-`d` target succeeds only when all `d + 1` critical records are sound. The
path failure probability would be:

`1 - (1 - p)^(d + 1)`, approximately `(d + 1) * p` for small `p`.

Thus depth 3 exposes a target to twice as many critical records as depth 1. This
is a model, not a field failure estimate: device-level correlation, replicas,
container placement, and checksums dominate real outcomes. Fanout is orthogonal
and can make one failed anchor affect many descendants even at depth 1.

Every decoded target still requires BLAKE3 verification of its logical bytes.
For diagnosis and scrub, validating each intermediate logical base separately
is preferable; otherwise the system can detect a bad final target without
locating the bad edge or record.

### Rebuild, scrub, and recovery

Depth 1 permits local paired checks at the target and its independent base.
Depth 2/3 requires rebuild and scrub to reconstruct a directed graph, reject
self-references and cycles, verify declared depth against computed depth, order
checks topologically, and distinguish a corrupt delta from a corrupt ancestor.
An offline index rebuild can remain deterministic, but more otherwise-valid
records become unusable when any ancestor is missing.

Recovery must never "repair" a broken chain by silently choosing a different
similar base: a delta is byte-exact only with the base named when it was encoded.
Any replacement encoding has to be created and verified as a new immutable
location before an atomic metadata switch.

## Recommendation for fastdup

1. Keep the v1 writer and reader at maximum codec depth 1. A DELTA or PREFIX
   base must be independently decodable.
2. Make the restriction explicit and paired: the writer records a declared
   dependency depth/profile; normal reads, recovery, and scrub recompute it and
   reject values other than 0 or 1 in the v1 profile. Reserved space may permit
   a later profile, but v1 must not silently accept future depths.
3. Treat depth 2 as an experimental feature flag only after the depth-1 delta
   path, GC, scrub, and rebuild are correct and measured. Do not implement depth
   3 until depth 2 passes the gate below.
4. Do **not** put `max_children_per_base` into the v1 format or make a hard
   fanout cap a correctness rule. Keep fanout as rebuildable derived state; a
   stale counter must never make a live base reclaimable.
5. Track at least direct delta children, dependent logical bytes, dependent
   manifest/file count, base age, physical failure-domain coverage, observed
   reuse distance/cache hits, base-container density, and relocation/re-encode
   cost. Counts may be saturating hints online, but scrub/rebuild derives the
   authoritative dependency closure from live manifests and immutable
   encodings.
6. Make base admission cost-aware. Compression gain and likely cache reuse can
   favor an already popular/resident base; dependent-byte exposure, poor
   locality, old age, weak failure-domain coverage, and projected maintenance
   work penalize it. Consequently the fanout term need not be monotonically
   negative. Measure it together with `ReadDistanceCost`, `BaseLoadCost`, and
   physical container density.
7. Before adding a dependency that crosses an empirically chosen exposure tier,
   verify that the base satisfies fastdup's single-device-loss policy, whether
   through independent `Location[]` entries or the declared lower storage
   failure domain. If it does not, store the target independently until the base
   is protected. High-exposure bases are also priority scrub and hot-tier/cache
   candidates. The Microsoft threshold of 100 references is a useful benchmark
   input, not a default fastdup invariant.
8. If locality or risk later becomes unacceptable, use offline RoW promotion or
   re-anchoring: write and verify the new independent base and any new child
   encodings, atomically switch authoritative locations/encodings, then make old
   records GC-eligible. Budget this work by child count and dependent bytes.
   With v1 depth 1, ordinary relocation of unchanged base bytes by logical
   `BaseChunkId` requires no child rewrite.

This recommendation is intentionally conservative because fastdup prioritizes
data integrity and measurable restore performance over feature breadth. It is
not a conclusion that depth 2 can never pay off or that unlimited unprotected
fanout is harmless.

## Required benchmark and stop rules

### Corpus

A single Rocky Linux ISO cannot reveal chain-depth value: it supplies no
evolutionary sequence. Use at least:

- 8 or more related Rocky minimal ISO versions, or reproducible variants with
  dispersed replacements, insertions, and deletions at several change rates;
- 20 or more versions each of generated XML and JSON families with small,
  localized edits and occasional structural insertions; and
- one captured backup-like stream with repeated full and incremental versions.

The Rocky fixture remains useful for RAW/CDC/container throughput, but not by
itself for this decision.

### Controlled comparison

Replay the identical CDC output and identical ordered top-K similarity
candidates through four policies:

- `D1`: a base must have depth 0;
- `D2`: a base may have depth 0 or 1;
- `D3`: a base may have depth 0, 1, or 2; and
- `oracle`: unbounded depth, offline only.

Use the same delta encoder, Zstd settings, container target, placement window,
and cost function. Charge all bytes: records, promoted independent anchors,
indexes, checksums, alignment, and metadata. Report the depth histogram and
base-fanout distribution so a small global saving cannot hide a dangerous hub.

Measure at minimum:

- physical bytes and compression by codec/depth;
- ingest throughput and base-read I/O;
- cold and warm 64 KiB random-read p50/p95/p99 latency;
- full sequential restore throughput and read amplification;
- GC bytes moved, write amplification, and elapsed time;
- scrub and metadata-loss rebuild throughput; and
- fault injection at every edge and ancestor, including missing locations,
  checksum failures, cycles, interrupted relocation, and loss of a high-fanout
  anchor.

For fanout, replay the same candidate lists under (a) gain-only selection, (b)
cost-aware selection, and (c) exploratory hard caps such as 32, 100, 256, and
1024 children. Also vary child ordering so warm-cache sequential restore is not
mistaken for cold-read behavior. Publish fanout and dependent-byte histograms,
base/container cache-hit ratios, unique containers read, anchors created,
retained-dead bytes, and RoW re-anchoring work.

### Project policy gates

These thresholds are proposed fastdup policy, not claims from the literature:

- **Stop before implementing deeper on-disk reads** if D2 saves less than 3% of
  total physical bytes versus D1 on every evolutionary corpus, or if D3 saves
  less than an additional 1% versus D2. Retain depth 1.
- **Prototype D2** only if the offline replay saves at least 5% on the projected
  capacity-weighted workload mix, or at least 10% on a named workload family
  expected to occupy 20% or more of capacity.
- **Ship D2** only if it still saves at least 5% end to end and, versus D1,
  retains at least 90% sequential restore throughput, keeps cold random-read
  p99 below 1.5x, keeps GC/scrub/rebuild elapsed time below 1.2x, and passes all
  integrity fault cases without enlarging the set of silently returned bytes
  (which must remain zero).
- **Consider D3** only if it adds at least 2% physical-byte saving beyond a D2
  implementation that already passed those gates. Apply the same performance
  and integrity limits independently.
- **Do not ship a hard fanout cap** unless a stable threshold improves cold or
  sequential restore p99, or bounded maintenance elapsed time, by at least 10%
  versus the cost-aware policy on more than one representative workload while
  costing no more than 1% physical bytes and preserving the single-device-loss
  guarantee. If it merely makes a fault-injection child count smaller, prefer
  exposure-aware protection: the same shared-record risk exists in exact
  deduplication and is not solved by a delta-only cap.

Do not average away an important workload: publish per-family results alongside
the capacity-weighted total. If only one narrow family benefits, a policy-scoped
codec profile is safer than raising the global maximum depth.

## Evidence gaps

- No reviewed source isolates depths 1, 2, and 3 on 64 KiB CDC chunks.
- The strongest depth result uses an unrestricted replication history whose
  destination stores decoded chunks, not persistent chains.
- Published restore results depend heavily on cache size, placement, container
  size, and rewrite policy, so no literature number can substitute for fastdup's
  own HDD/NVMe measurements.
- No source reviewed here evaluates fastdup's immutable-location relocation,
  manifest-as-source-of-truth rebuild, or ten-second commit contract.
- No reviewed source isolates a universal logical delta-fanout cap. Published
  thresholds either limit chain depth, cap replica count after diminishing
  returns, or trigger promotion/protection of a popular record.

Accordingly, the safe current statement is: depth 2 might recover a low-to-mid
teens percentage of stored bytes on favorable versioned workloads, depth 3 has
no separately demonstrated incremental benefit, and both must remain behind an
evidence gate until measured in fastdup's actual format and placement model.
