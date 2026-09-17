---
status: accepted
---

# Own repository reads through one unified cache

FastDup owns file-content caching for the Metadata and DATA tiers. One
process-wide cache owns the directory, allocation accounting, admission,
replacement and pressure reclamation for every reusable storage representation.
A memory broker distributing quotas to separate caches does not satisfy this
decision. This revision of 12 September 2026 replaces the earlier independent
DATA, descriptor, Metadata-object, Manifest, Exact, Similarity and proof-cache
policies in this record.

## One owner, typed views

`ReadCacheNamespace` supplies identity and observations to one shared engine.
Namespaces are not separate resident maps, quotas or replacement queues.
Verified DATA (decoded or independently compressed), historical proofs,
Container descriptors, decoded Metadata/Manifest/Index pages, Similarity fences,
Exact membership filters and page bounds, reverse-dependency projections,
encoded storage ranges and reusable file handles all
compete in that engine. Generation pins, Dirty DATA, active codec work and
in-flight I/O remain required working state; cache eviction cannot release them.
The serialized Exact writer also retains its last successfully synchronized
Activation Log snapshot (one bounded slot, at most 256 KiB encoded plus its
bounded decoded record vector). This is required
generation/rotation state, not another content-cache directory or budget.
Codec buffer reuse retains its separately identified memory-reuse lease because
it contains no reusable storage content.

A hit locks one of 64 directory shards. A weighted CLOCK gives reused DATA,
proofs and Container descriptors more protection than Metadata acceleration
(16 versus one credit, bounded at 32). Admission and pressure eviction share
one serialized owner ledger; no I/O, codec work or user callback runs under
that lock. Ordinary admission searches at most 4096 victim steps and may reject
admission. Pressure reclamation continues until the common target is met,
preferring unprotected bytes before protected Exact acceleration. Verified DATA
is subject to the dated 20% class ceiling below; protected Exact acceleration
cannot be displaced by Verified DATA but remains reclaimable by its own class
and under host memory pressure once unprotected bytes cannot meet the target.
Exact membership filters remain in the common owner but are pinned non-evictable
working acceleration while their Run is active, as amended below.

The ledger charges retained representation capacity, shared backing once per
admission group, and conservative directory/owner overhead. Returning immutable
views remain valid after eviction. Memory retained exclusively by readers then
reduces sampled process headroom as working memory. This is a sampled operating
ceiling, not an exact bound on all process allocations or a kernel OOM guarantee.
The effective host/cgroup limit, available memory, outstanding working memory and
8% reserve retain the 92% operating ceiling. Sampling failure and Process Swap
revoke admission and reclaim evictable residents. Active-Run membership filters
are the non-evictable exception dated below. Production uses one `unifiedRead` broker
lease; representation observations must not be summed as independent quotas.
Externally supplied deterministic test/replay configurations are not production
memory policies.

Verified DATA is compressed on admission when the complete group's retained
allocation becomes smaller; otherwise its original verified backing is kept.
Independently compressed siblings reserve their combined admission before any
is inserted, but each releases its own charge on eviction. The previous private
DATA cache's hot-promotion/cold-demotion controller is retired. The initial
unified policy keeps the admitted representation and reuses live decoded views;
the old controller remains a test/replay comparator, not a second production
cache. Changing resident representation later requires the common owner ledger.

Identities include a fresh repository/representation namespace and complete
immutable object identity, generation/hash and page ordinal as applicable.
Filesystem ranges additionally bind the name, device, inode, length, change
stamp and a process-owned per-file mutation revision. Revisions advance before
and after every mutation, including failure; equal timestamps cannot preserve
stale ranges and reads racing a mutation cannot populate its final revision.
Mutable Reduction Heads bypass range/length caching. Cooperating mutation
invalidates retained handles under the per-name
barrier. Immutable-file leases prohibit mutation until their last owner drops;
retirement and root pins retain their existing authority. Cached bytes never
select a generation, pin a root, establish liveness or authorize a Location.
External mutation is not a supported live coherency mechanism; independent
verification must still detect changed physical bytes with a warm cache.

The common engine also coalesces overlapping identical cold storage requests.
Its bounded in-flight directory holds at most 128 requests; errors reach every
waiter and loader unwind releases waiters. Saturation uses bounded ordinary I/O.
It does not create a second resident cache. The limit on 128 idle Container file
handles is a resource ceiling; their replacement belongs to the common engine.

## Read intent and Online GC

Every path explicitly preserves one of these intents:

- **Demand** permits hits and admission, after the representation's validation.
- **Scan** permits hits and declines admission for one-pass work.
- **Independent** bypasses every cache and shared in-flight result, and declines
  admission. It obtains current storage evidence.

Online GC may reuse cached Metadata graphs, candidate/index pages and verified
relocation source bytes. Its reverse-dependency projection is also reclaimable
by the common cache; a running proof retains its required immutable view.
Online Exact reuse and Commit reference checks follow ADR 0015: a valid ACTIVE
Exact mapping needs no DATA read even with an empty cache. Historical references
remain in the common owner and require current selection; they never become
physical verification evidence. Per-epoch GC admission is required writer
workspace, bounded independently of cache residency.

Independent DATA verification paths and demand readers share compact Location
evidence independently of payload residency. Evidence
is admitted only after complete Record/Chunk verification (including a Base
when required), and matches every coordinate of a currently eligible ACTIVE
Exact candidate. The newest transition for that Location must still allow it.
An eligible cached Location is preferred before cold alternatives; ordinary
backend attempts remain bounded at two. Independent Record decoding can emit
opaque sibling Locations without retaining their decoded allocation.

Verification-only misses use Scan intent for the payload load, then admit
the checked Locations under the caller's restored intent. They do not retain
or recompress DATA payloads or populate encoded storage ranges merely to
remember that verification succeeded. Demand reads retain their normal payload
admission and also contribute checked Location evidence. This additional typed
view has no private map, quota, lease or replacement list: pressure evicts it
through the same owner, and telemetry exposes its hits and charged bytes.
Evidence eviction causes revalidation where physical evidence is required; it
does not add a payload check to trusted Exact ingest. Explicit Independent scopes
bypass every hit and admission. Logical bytes without matching physical-source
evidence still cannot authorize a Location. Recovery, scrub and final deletion
validation keep their fresh-media contracts.

Location evidence is keyed by the complete logical identity and physical
Location, with full entry comparison on a hit. Different verified copies of
one Chunk coexist in the common directory and share its existing accounting
and replacement policy. A logical-ID-only slot let an old resident block
admission of its verified replacement indefinitely. Lookup now checks the newest
transition for each candidate Location, prefers any eligible warm proof, and
reuses the same bounded Exact lookup result for cold fallback. Retiring a
Location never makes its cached proof selectable through a newer generation.

A successful full online scrub can offer its compact Location evidence to this
same cache after its Independent verification scope ends. The complete image,
logical identities and dependent Bases must have verified before any such
handoff; neither encoded nor decoded payloads are admitted by the scrub. Caller
Scan/Independent intent and the common pressure owner can still decline the
handoff. A subsequent scrub remains fully Independent even with warm evidence.
Resumed historical certificates supply no current Location evidence. This
avoids discarding fresh verification work before other physical-proof consumers,
without promoting persisted progress into current payload truth. The paired
tests and sampled live limits are recorded in
[the Location reuse report](../testing/location-proof-reuse-2026-09-13.md).

The payload-only Scan scope does not suppress ordinary admission of reusable
Exact pages or compact descriptors. Pass-local sibling discharge remains
available even when the caller's intent or memory pressure forbids admission.
Its current Commit binding, live root pins, publication
barrier, victim validation and final deletion proof remain mandatory. Independent
recovery, offline/background scrub and fresh deletion verification
use Independent intent. A successful cached read never counts as a fresh scrub
of the physical medium. Synchronous intent scopes nest, restore on unwind and
cannot cross threads/awaits; dispatched scrub workers establish their own scope.

An online Exact append under the same exclusive Repository owner carries
validated encoder output through successful file writes, file sync, no-replace
publication and directory sync into activation. Its serialized writer advances
the known WAL snapshot only after the final slot sync succeeds. It does not
reread either WAL slot or the selected Run Set on each append. Newly written
Runs and compaction outputs carry their checked descriptors and page bounds;
unchanged Runs retain their immutable leases and exact identity checks.
Compaction may consume those Runs' common-cache pages. Encoded writer pages,
optional bounds and membership hints share the existing common cache and remain
reclaimable. Cold page reads still decode and validate their checksums.

Successful Direct-I/O writes similarly admit the resulting storage length head
under the final mutation revision in the common range namespace. An owned
no-replace rename can transfer that known head under the destination identity
while holding both name barriers. A failed mutation cannot admit its proposed
head, and a cache miss reads the physical heads normally.

An activation write or sync error revokes the online WAL cursor, including
ambiguous errors after effective synchronization. The next append reconstructs
the stored selector and validates its dependencies before reuse. Recovery also
revokes the cursor before independent verification. Initial/unknown objects,
publication collisions, public standalone Exact activation, recovery and scrub
retain independent stored-byte validation. An immediate readback is not proof
of power-loss durability: successful synchronization and publication ordering
remain mandatory. This distinction supersedes the repeated online readback
requirements in ADRs 0044, 0045 and 0079 without changing any durable byte format.

Similarity and candidate-Run activation still audit the complete current stored
hash, checksums, ordering and dependencies. Similarity may reuse
individually verified pages for its subsequent semantic cross-reference pass.
A failed activation cannot become authoritative through a later cache hit.
Independent Similarity audits may retain up to 32 MiB of their freshly decoded
Entry input for their own Bucket-reference validation; larger inputs use the
bounded reread path. This working vector is discarded with the audit, has no
admission/replacement policy and cannot reuse an earlier audit's evidence.

## Direct I/O and the format boundary

All repository file-content reads and writes use `O_DIRECT`, including small
Metadata/control files, WAL updates, bounded unaligned reads and every size of
owned Container publication. File-backed Index/catalog mappings are replaced
by bounded direct readers retaining the same immutable leases. Unsupported
alignment/filesystem geometry fails closed; no buffered fallback exists.
`DONTNEED`, `RWF_DONTCACHE`, `drop_caches` and periodic eviction are insufficient
substitutes. `fsync`, file-before-directory publication and recovery checks
retain their durability obligations.

Every regular FUSE handle, including read-only handles, uses `FOPEN_DIRECT_IO`.
There is no kernel file-data retention through `KEEP_CACHE` or writeback caching;
attribute/name TTL remains zero. Shared file mappings are consequently refused
under the existing unsupported-mmap policy. This supersedes ADR 0073 and the
kernel-read-cache rationale in ADR 0058, as well as the mapping decisions in
ADRs 0061 and 0079. ADR 0051's independent historical-proof S3-FIFO is replaced
by common replacement; generation proof pins remain outside eviction.

On XFS, truncating to an unaligned physical EOF can instantiate a buffered tail
folio even on an O_DIRECT descriptor. The generic filesystem adapter therefore
uses the [aligned storage envelope](../specs/aligned-storage-v1.md): two checked
length blocks, followed by logical content and physical block padding. Native
owned Container images are already aligned and self-describing. Readers expose
logical lengths and bytes to existing format decoders; padding is never a WAL
record or logical object byte. Repository Format Epoch two fences the change.
Existing pre-stable repositories require rebuilding; no in-place migration or
buffered legacy mode is provided.

The supported promise concerns **repository file content**. XFS/VFS metadata,
inode/dentry caches, kernel code and block-device/controller caches cannot all be
replaced from an application using an ordinary filesystem. Eliminating those
would require a different raw-block storage architecture. Device durability and
ordering remain governed by ADRs 0001, 0028 and the supported storage stack.

## Qualification and consequences

The completed checks and their limits are recorded in the
[qualification report](../testing/unified-read-cache-2026-09-12.md).
The Exact publisher's online readback correction is qualified separately in
[the Metadata I/O report](../testing/metadata-read-amplification-2026-09-12.md).

Regression gates cover cross-class eviction, shared-backing ownership,
concurrent admission, pressure, bounded shared misses, warm-cache corruption,
Online-GC reuse, independent recovery/scrub and direct I/O at arbitrary logical
boundaries. Storage-head failures must be checked below the logical fault model
as well as through normal recovery/scrub entry points. On the supported XFS
geometry, `fincore` must report zero resident file-content bytes after small
writes, shortening and reads, without prior cache dropping.

The new envelope costs 8 KiB plus tail padding per generic repository file.
Length-head synchronization can increase small-write latency. Direct I/O needs
aligned bounce buffers; scans need explicit application policy instead of kernel
readahead. These costs are accepted for one controlled file-content cache.
Immutable publication uses one shared bounded writer implementation for
Metadata images, Exact Runs and Run Sets, streamed Exact compaction families,
Similarity partitions, and DATA-tier Recovery Checkpoints. Already encoded
images are written in at most one-MiB slices; streamed output uses one temporary
one-MiB buffer, starting at offset zero and batching across format fields and
object boundaries. This buffer is required writer workspace, not retained read
acceleration or a separate write-back cache. It is explicitly drained before
verification, file synchronization or publication; failure leaves only an
unselected temporary object.

A four-KiB loop would advance and synchronize the storage length head for every
page, doubling body/head write traffic and multiplying barriers. Unaligned
checkpoint-entry writes additionally read existing edge sectors to preserve
their untouched bytes. Batching changes neither encoded bytes nor the required
staging verification, file sync, no-replace rename and directory-before-WAL
ordering. A Recovery Checkpoint patches its aligned fixed Header after computing
the complete body hash and still independently audits the resulting image.
The measured write counts and remaining cold-read limits are recorded in
[the remaining I/O report](../testing/remaining-io-amplification-2026-09-13.md).
Telemetry reports the common memory lease and direct backend causes; logical
requested bytes are not physical disk IOPS or device bytes.

The primary-source investigation and the XFS truncate experiment are recorded in
[the research note](../research/unified-read-cache-2026-09-12.md). The relevant
contracts are [Linux open(2)](https://man7.org/linux/man-pages/man2/open.2.html),
[statx(2)](https://man7.org/linux/man-pages/man2/statx.2.html),
[Linux FUSE I/O](https://docs.kernel.org/filesystems/fuse/fuse-io.html) and
[the XFS size-change implementation](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/xfs/xfs_iops.c).


## Exact writer evidence (2026-09-13)

Ordinary L0 publication derives page bounds and membership hints directly from
the validated immutable Run entries and the encoder's fixed page partition, as
streamed compaction already does. It does not decode and checksum its newly
encoded RAM pages a second time. Their byte representations still enter the
Unified Read Cache, and independent demand, recovery and scrub readers retain
stored page CRC, full Run hash and ordering checks. The shared format page-entry
constant prevents the evidence partition from drifting from serialization.

## Metadata writer image handoff (2026-09-13)

New content-addressed Metadata images carry their validated encoder bytes
through complete writes, length finalization, file synchronization and successful
no-replace publication. The writer does not reread its own temporary image.
After successful publication it offers those bytes to the existing Metadata
object namespace in the Unified Read Cache. Admission remains subject to the
caller's read intent and the common pressure budget; it adds no private cache.

This handoff supplies immutable content, not a durable root or liveness proof.
The caller still owes the Metadata directory barrier before Commit/WAL
visibility. A failed write, file sync or publication cannot admit the proposed
image, including an error reported after the operation took effect. Existing
same-name objects still undergo independent stored-byte collision verification.
Recovery and scrub continue to bypass warm bytes; GC keeps its publication
barrier and invalidates object bytes before deletion. This supersedes immediate
Metadata staging readback, without changing the durable format or synchronization
requirements.

## Online maintenance selection (2026-09-13)

Ordinary GC selection, candidate proof/refresh and activation-binding checks
reuse an installed Exact generation only when it matches the exclusive
writer's last synchronized activation snapshot. Selection is serialized with
generation publication. A remaining old pin alone is insufficient after an
ambiguous activation error: absent/mismatched writer evidence reconstructs the
selector and dependencies. Explicit Independent intent and startup GC recovery
continue to reconstruct storage. Final victim verification, retirement
barriers and deletion ordering remain unchanged. Cold selected pages still
need their normal verified reads; this removes repeated whole-generation
recovery, not those demand reads.

## Writer payload handoff (2026-09-13)

Before replacing resident ingest bytes with a location-backed read recipe,
write-through offers the logical Chunk bytes to the existing Unified DATA
cache. The checkpoint writer uses the same admission seam after successful
Container publication or confirmed Exact reuse. Complete logical identity and
bounded Chunk length are checked before admission. The common pressure budget,
replacement and Scan/Independent intent still govern residency; admission can
be declined without affecting publication or correctness.

These payloads deliberately carry no physical Record provenance. They can
satisfy logical demand reads but cannot certify a Location, establish liveness
or count as independent media verification. Recovery and scrub bypass them;
proof consumers still require their own eligible publication/activation or
physical evidence. No second cache or unbounded writer-retained payload map is
introduced, and no on-disk format changes.

## Verified DATA ceiling and Exact displacement guard (2026-09-15)

Verified DATA retains its normal high reuse credit but may occupy no more than
20% of the common `unifiedRead` target. The ceiling uses one minimum DATA charge
as a floor so a tiny deterministic or low-pressure target remains usable. It is
a class ceiling inside the one common owner, not a separate cache, quota, lease,
resident map or replacement queue. The common target remains governed by live
memory headroom and can fall below the ceiling.

Exact index pages, membership filters and page bounds form protected Exact
acceleration. Admission of Verified DATA may reclaim Verified DATA but must not
displace protected Exact bytes. Admission of other unprotected acceleration may
reclaim unprotected bytes only; it cannot displace protected Exact bytes. When
total pressure must reclaim bytes, unprotected Verified DATA and other
acceleration are reclaimed before protected Exact acceleration. If protected
Exact alone exceeds the target, host memory pressure can still reclaim it.
Protected Exact admission may reclaim its own older Exact entries before
unprotected entries, but it never weakens the Verified DATA ceiling or permits
Verified DATA to become the dominant resident class.

The guard concerns placement within the common cache. Exact bytes remain
acceleration, never durable index, Location or liveness authority; an empty or
pressure-reduced Exact page cache preserves correctness through its normal
verified cold-read path.

## Protected Exact ceiling and bounded proactive warming (2026-09-16)

Protected Exact acceleration may occupy no more than 70% of the common
`unifiedRead` target. One minimum protected charge is the floor for a tiny
target. The ceiling is enforced before exact admission and after a lowered
target; it is one class limit inside the same owner ledger, not a private Exact
cache, quota, resident map or replacement queue. This leaves the Verified DATA
20% ceiling and at least the remaining headroom room for descriptors, Similarity
and other acceleration.

An optional appliance worker may proactively rebuild missing active-Run
membership/page-bounds structures and warm bounded nonresident Exact pages. It
uses Demand verified reads and the same admission, accounting and replacement
owner. Its default work budget is bounded by pages, structures and per-Run pages,
advances a process cursor round-robin, and stops on cancellation, budget, cache
admission rejection or memory pressure. It waits for completed startup scrub,
idle POSIX frontend read/write submission and open mutation admission. It does
not wait for Online-GC quiescence: bounded Exact warming may overlap a running
GC while its background I/O priority and common-cache admission limit its
interference. Frontend idleness is measured from successful POSIX read and write
counts, so GC and other backend I/O are not mistaken for frontend demand.
The worker is background acceleration only: an active Run remains lookup-correct
with dormant wrappers and a cold verified page read. It neither selects a
generation, pins a root, certifies a Location, nor changes the durable Exact
format. Telemetry distinguishes protected limit/resident bytes, constructed and
missing filters/page bounds, and the last warm/skip state.

## ExactMembership residency floor (2026-09-16)

Active-Run `ExactMembership` filters are the sole cache class exempt from
replacement. They stay in the common owner and remain visible in protected Exact
residency accounting, but admission bypasses the common target and class ceilings
and no admission, pressure, swap or ceiling reclamation can select one. Demand,
Scan and Independent work may all populate this one pinned class after the
audited filter has been constructed; no other class may use it to bypass its
normal intent rule. Dropping the
representation namespace or retiring its Run remains the only release. Exact
pages and page bounds remain ordinary protected Exact entries and can still yield
to or be reclaimed by pressure.

Pinned membership is charged outside the evictable common target so a non-zero
target cannot prevent it from becoming resident. The sampled governor still
observes and reports it; if it alone consumes available headroom, evictable
classes shrink to zero while the filter stays. The warm budget also prioritizes
membership independently of the remaining page-bound budget, and protected Exact
page budget subtracts the pinned charge so the classes cannot jointly overshoot
their ceiling. Run publication evidence and active-Run audit construct a missing
membership before consulting the evictable budget rather than leaving it dormant
until a later warm cycle. This makes filter residency a bounded working-state
floor rather
than cache acceleration: at roughly 10 bits per active key, it costs less than
one percent of the active Exact page charge while preserving warm lookup and GC
reuse without waiting for a cold filter rebuild. An out-of-memory filter
construction remains false-negative-safe and is retried by later warm cycles.

## Scan fills eviction-free headroom (2026-09-16)

Both tiers are read through `O_DIRECT`, and this common cache is the only
file-content reuse owner. The Linux page cache is not a fallback for any miss,
so a Scan pass that categorically declined admission reread the same immutable
Metadata graph, candidate/catalog pages, storage ranges and descriptors from the
device on every GC quantum even while the common budget was empty. Keeping the
right bytes resident is this owner's job; a cold Scan miss against free budget
wasted the headroom it should have used.

Scan intent now admits within eviction-free headroom only:

- A Scan miss may be admitted only when the current common target and the
  class ceilings are already satisfied. Its admission skips the victim search
  entirely, so a Scan admission never evicts a resident entry, never grows a
  class past its ceiling and never exceeds the common target. Rejection under
  a full target is a reuse miss, never a correctness event.
- Demand admission and reclamation are unchanged. A Scan-admitted entry starts
  at zero reuse credit, so one-pass bytes that earn no later hit are reclaimed
  before bytes that did. Governor-disabled admission (zero target, Process Swap
  or sampling failure) leaves Scan filling nothing.
- The Verified DATA payload rule and Location-evidence rule stand for
  verification-only work: a verification-only Scan load retains no payload,
  encoded storage range or Location evidence, and that evidence is handed off
  only after the caller restores Demand intent. Only a content read whose
  bytes actually return to the caller may retain verified DATA within the same
  eviction-free headroom, so Online-GC relocation rereads hit instead of
  refetching. Writer-provenance `admit_validated` offers remain Demand-only.

This supersedes the Scan bullet of "Read intent and Online GC" above: Scan
permits hits and fills eviction-free headroom, while Independent still bypasses
everything and declines admission. GC, checkpoint planning and catalog passes
keep Scan intent; their publication, deletion and verification duties never
depend on residency, and an empty or full cache changes only latency, never
outcome. Paired tests: the unified-engine headroom-fill/no-displace case and
the Metadata-object scan-fill cases in `fastdup-store`.
