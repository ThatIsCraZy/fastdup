# fastdup Storage Appliance

fastdup is a single-node POSIX storage appliance that reduces physical storage
while preserving byte-exact file contents. This glossary names the concepts at
the product boundary; implementation and format details belong in specifications
and ADRs.

**MVP**:
The first usable FUSE appliance with crash-safe manifests, bounded updates,
SeqCDC, Exact Dedup, bounded in-memory acceleration, RAW/Zstd encodings, and
adaptive DATA/Metadata GC. A durable, rebuildable Similarity Index and
Depth-1 ZSTD_PREFIX/Sparse-XOR encodings form the optional Advanced Reduction
path; Dictionary encoding, Reorder, production Samba hardening, and device-loss
protection remain later stages.
_Avoid_: Container-store milestone, production appliance

**Advanced reduction**:
The optional writer path that pins one coherent Exact/Similarity snapshot and
uses bounded Similarity Candidates to trial Depth-1 ZSTD_PREFIX and Sparse-XOR
encodings behind one dependent-codec policy.
Unavailable or stale acceleration falls back to independent encoding without
weakening content truth.
_Avoid_: Delta mode, authoritative Similarity Index

## Stored data

**Logical chunk**:
A content-defined byte range that is the unit of exact deduplication.
_Avoid_: Block, extent

**Chunk ID**:
The content identity of a logical chunk, independent of where or how its bytes
are stored.
_Avoid_: Block address, location ID

**Location**:
One physical encoding of a logical chunk. A logical chunk may have several
locations over its lifetime without changing its identity.
_Avoid_: Chunk, canonical copy

**Location set**:
The locations authorized by one committed metadata generation for a logical
chunk, including their active, retiring, or quarantined state. No physical
location is intrinsically canonical.
_Avoid_: Replica set, manifest

**Exact index**:
The complete persistent mapping from a Chunk ID and logical length to its current
Location Set. It is rebuildable acceleration rather than the source of content
or liveness truth.
_Avoid_: Bloom filter, recovery index

**Exact Index activation log**:
The bounded hash-chained selection history that identifies one immutable Exact
Index Run Set as active. It is acceleration state and never content or liveness
authority.
_Avoid_: Commit WAL, Exact Index, Manifest

**Exact Index Run family**:
One logical, immutable Exact Index generation represented by one or more
strictly key-disjoint Runs selected together. A family is the unit of
compaction precedence; an individual partition is only its physical range.
_Avoid_: Run Set, shard replica, independent Run generation

**Exact hit**:
A Chunk ID and logical-length match against the Exact Index. It permits location
reuse without asserting that a physical copy is canonical.
_Avoid_: Bloom hit, similarity candidate

**Similarity fingerprint**:
A versioned, non-cryptographic description used to retrieve likely compression
bases for one logical chunk. It never establishes content identity or integrity.
_Avoid_: Chunk ID, Exact hit

**Similarity candidate**:
A logical chunk proposed by the Similarity Index as a possible Base Chunk. It
becomes a dependency only after bounded trial encoding and cost evaluation.
_Avoid_: Exact hit, selected base

**Base fanout**:
The number of active dependent encodings naming one Base Chunk. It can improve
cache reuse while also concentrating recovery dependency.
_Avoid_: Reference count, duplicate count

**Reverse dependency generation**:
A generation-bound projection from protected logical chunks to every Base Chunk
required by their active dependent Locations. It is deletion-proof input bound
to one Commit pair and Exact Index generation, not durable liveness authority by
itself.
_Avoid_: Similarity fanout, mutable reference count, GC candidate estimate

**Encoding record**:
One physical payload that decodes to a contiguous region partitioned into one or
more complete logical chunks. It is the unit of compression and integrity
checking, not logical file identity.
_Avoid_: Logical chunk, compression group

**Base chunk**:
An independently decodable logical chunk required to reconstruct one dependent
DELTA or PREFIX encoding.
_Avoid_: Dictionary, previous container

**Dictionary object**:
An immutable content-identified byte sequence required by a dictionary encoding.
It is a physical decoding dependency, not a mutable workload setting.
_Avoid_: Base chunk, training profile

**Dictionary family**:
A policy-selected group of sufficiently similar content whose prior samples may
train one Dictionary Object. Membership affects compression opportunity only,
never content identity, integrity, placement authority, or visibility.
_Avoid_: File type, codec, trust class

**Dictionary catalog**:
A bounded acceleration mapping Dictionary Families to measured immutable
Dictionary Objects. It is neither a cross-Container compression history nor a
source of durable content or liveness truth.
_Avoid_: Compression catalog, shared Zstd stream, dictionary index

**Container**:
An immutable collection of physical chunk encodings with enough local metadata
to validate and rediscover its contents.
_Avoid_: Pack file, mutable segment

**Container ID**:
A stable random 128-bit identity assigned to one immutable physical container.
It carries no content identity or ordering meaning.
_Avoid_: Chunk ID, container generation

**Container generation**:
A monotonic appliance-local creation order for containers. It is recoverable
from durable containers and is not a logical content identity.
_Avoid_: Container ID, commit generation

**Container generation reservation**:
A durably reserved monotonic range whose generations may be assigned to new
Containers. Recovery skips the unused suffix after a crash so an acknowledged
or ambiguous publication generation is never reused.
_Avoid_: Container count, next generation, Commit reservation

**Container envelope proof**:
The paired, checksummed Container Header and Footer plus physical length and
canonical filename. It proves structural identity, layout, and generation for
allocator recovery, but neither the payload hash nor any logical Chunk bytes.
_Avoid_: Verified Container, verified Location

**Container publication proof**:
Evidence carried from the Container writer through sampled durable publication,
or produced later by a complete independent Container read. Writer evidence
binds prior Chunk identities to the exact serialized Locations. Independent
read, recovery, and scrub evidence additionally recomputes checksums and logical
Chunk identities from stored bytes.
_Avoid_: Read cache entry, Container envelope proof, Exact-Index hit

**Manifest**:
An immutable recipe for reconstructing one file version from logical chunk IDs
and file-layout metadata.
_Avoid_: File, index entry

**Manifest root**:
The identity of an immutable extent tree representing one complete file version,
including data and sparse holes.
_Avoid_: Flat chunk list, inode

**Inode version**:
The immutable content and POSIX state of one inode in a commit generation. Names
are separate directory entries, so every hardlink observes the same inode version.
_Avoid_: Path, manifest

**Open orphan**:
An unlinked inode retained only by live handles. It remains accessible to those
handles but has no recoverable namespace name after all handles or the appliance
process are gone.
_Avoid_: Lost-and-found object, deleted container

**Inode ID reservation**:
A durable high-water range assigned before its Inode IDs may become visible, so
IDs acknowledged inside a later lost durability window are skipped after crash
rather than reused.
_Avoid_: Next inode, handle range

**Thin allocation**:
An allocated logical range represented by metadata, usually FILL(0), without a
promise that physical capacity is reserved for a later write.
_Avoid_: Capacity reservation, preallocation guarantee, file size

**Namespace root**:
The identity of the immutable directory and inode state selected by one commit
generation.
_Avoid_: Mount point, recovery checkpoint

**Metadata object**:
An immutable, content-identified metadata blob referenced by namespace or inode
nodes when it is too large to represent inline.
_Avoid_: User data chunk, online index

**Namespace shard**:
A bounded immutable piece of one Namespace Root's canonical byte stream. Only
the ordered, hash-bound shard set selected by that Root is meaningful; a shard
is not an independently mutable directory or inode partition.
_Avoid_: Directory shard, namespace database page, independent root

**Commit record**:
The checksummed object that atomically selects a generation, its predecessor, and
one namespace root. Durable objects not reachable from a commit record are not
visible versions.
_Avoid_: Commit group, journal entry

**Commit WAL**:
The hash-chained sequence of commit records stored in bounded paired slots from
which normal crash recovery selects the newest wholly valid namespace
generation. Rotation rewrites only an inactive slot and preserves an exact
bridge record.
_Avoid_: Recovery checkpoint, online index

**Recovery checkpoint**:
A self-contained immutable copy of one committed Namespace graph stored on the
Data Tier for complete Metadata-Tier loss. Paired selector heads retain the
current and previous complete copies; each copy embeds its exact Commit Record
and every transitively reachable Metadata object, but no online index.
_Avoid_: Commit WAL, normal checkpoint, snapshot, backup

**Visible version**:
The single committed file version exposed through the POSIX namespace. A version
is never visible before all data on which it depends is durable.
_Avoid_: Latest write, dirty version

**Live view**:
The current in-memory file state observed while the appliance remains running.
It includes every successfully acknowledged write even when that write is not
yet crash-durable.
_Avoid_: Durable version, recovery state

**Incomplete ingest**:
A newly created file still being written. After interruption, its latest wholly
committed prefix remains a valid storage object even when the application regards
that partial content as useless.
_Avoid_: Disposable staging, corrupt file

**Atomic update**:
A complete file version produced by applying a contiguous prefix of accepted
mutations in one commit generation. A longer write session may yield several
atomic updates; no individual generation is partly recovered.
_Avoid_: Open-to-close transaction, in-place recovery

**Opaque content**:
File content whose meaning, syntax, and claimed type are irrelevant to storage
correctness. Every accepted file must be reconstructed byte-for-byte, including
malformed or mislabeled structured data.
_Avoid_: Parsed document, normalized content

## Placement and recovery

**Metadata tier**:
NVMe capacity reserved for namespace metadata, indexes, journal, recovery state,
hot data, and policy-selected small files.
_Avoid_: Cache disk

**Data tier**:
HDD capacity that holds the bulk of immutable containers for large-file data.
_Avoid_: Archive tier, cold tier

**Small-file tier**:
Protected metadata-tier capacity for important small files selected by policy,
with a size-based fallback when the final size is not known at creation.
_Avoid_: Metadata, generic cache

**Metadata reserve**:
Capacity that only commit-critical metadata, WAL, indexes, and rebuild state may
consume. Other tiers cannot borrow it even when they are full.
_Avoid_: Free NVMe space, cache quota

**Physical Pool isolation**:
The production proof that Metadata and Data Pools occupy distinct XFS
filesystems, so exhaustion of one cannot consume the other's reserved blocks.
Persistent Pool identity alone does not establish this capacity boundary.
_Avoid_: Different directory, Pool ID, reported capacity

**Commit capacity claim**:
A process-local pessimistic claim against cached physical Metadata and DATA
headroom, acquired before a mutation becomes visible. It follows the mutation
from the Active epoch into its Frozen Commit Cut and is retired only after the
durable Commit is included in a newer physical capacity observation.
_Avoid_: Logical quota, file preallocation, dirty-memory budget

**Appliance ID**:
The persistent random identity shared by every storage pool belonging to one
appliance. It is independent of host names, mount paths, and process lifetimes.
_Avoid_: Appliance lease, machine ID, repository path

**Pool ID**:
The persistent random identity of one storage pool. A mount path, device name,
or Pool Role is not a pool identity.
_Avoid_: Mount point, device serial

**Pool role**:
The immutable purpose of one Pool within its Appliance: Metadata or Data. A
Pool cannot change roles by being mounted at a different path.
_Avoid_: Mount order, directory argument, storage class

**Cache location**:
A removable extra physical encoding that can accelerate reads but never provides
the only durable coverage for a live logical chunk.
_Avoid_: Small-file location, active location

**Verified cache entry**:
A decoded chunk or region admitted to a shared read cache only after its complete
stored encoding and logical content identity were verified.
_Avoid_: Kernel-dirty page, cache location

**Verified read plan**:
A bounded demand-read plan that maps logical Manifest extents to verified
physical Locations, shares one Encoding Record read and decode across its
Chunks, and returns bytes in logical order. It is acceleration, never content,
liveness, or Location authority.
_Avoid_: Reorder, prefetch, Location Set

**Restore-local placement**:
Physical ordering of newly published independent Locations that favors long
ascending reads of logically adjacent data on the HDD Data Tier. It changes
only placement, never Manifest order, Chunk identity, or liveness.
_Avoid_: Similarity Reorder, canonical Location, defragmentation

**Generation proof set**:
The bounded in-memory set of verified DATA Locations required by the Active and
Frozen Commit Generations. Its entries remain pinned until the owning generation
commits, aborts, or rolls back; a historical cache policy cannot evict them.
_Avoid_: Historical proof cache, Exact Index authority

**Historical proof cache**:
Process-local acceleration for previously verified immutable DATA Locations.
It uses S3-FIFO, starts empty after restart, and may be purged under memory
pressure without changing correctness or durability.
_Avoid_: Generation proof set, persistent Exact Index, source of truth

**Exact Index hot page**:
An independently verified, decoded page from an immutable Exact Index Run,
retained as bounded RAM acceleration. It is distinct from the generation-pinned
Run view and never authorizes Chunk reuse or replaces DATA Location verification.
_Avoid_: In-memory Exact Index, hash table

**Cache memory reserve**:
Host/cgroup headroom that DATA and Exact Index caches are forbidden to consume.
It protects Dirty DATA, reduction workers, XFS clean/writeback pages, and device
queues; pressure shrinks or disables cache admission rather than borrowing it.
_Avoid_: Cache capacity, free RAM, metadata reserve

**Process Swap**:
Swap currently attributable to the running fastdup process. It closes
rebuildable cache admission until cleared. Host or shared-cgroup Swap is not
Process Swap; a dedicated no-Swap cgroup remains the hard production boundary.
_Avoid_: Host Swap, cgroup Swap, memory pressure

**Recovery index**:
Container-local metadata from which stored logical chunk identities and physical
locations can be rediscovered without the online deduplication index.
_Avoid_: Dedup index

**Recovery index evidence**:
The paired Container envelope plus one complete compact, checksummed Recovery
Index. It bounds candidate discovery without reading record payloads, but the
selected Encoding Record must still verify before yielding logical bytes and
the evidence never establishes liveness.
_Avoid_: Exact Index, Container proof, GC deletion proof

**Quarantined location**:
A physical location excluded from reads because its integrity could not be
verified. Quarantine never changes the logical identity of the affected chunk.
_Avoid_: Bad chunk

**Retiring container**:
An immutable container excluded from new location selection while GC constructs
and commits verified replacement coverage. Existing pinned generations may still
read it.
_Avoid_: Deleted container, quarantined location

**Scrub**:
An offline or background integrity pass that verifies durable sources of truth
and reports or repairs invalid physical locations.
_Avoid_: Garbage collection, rebuild

**GC scrub plan**:
Opaque, generation-bound evidence from one successful Scrub that names verified
physical garbage without making an Exact Index or reference count authoritative.
It is one complete proof source, not a prerequisite for every candidate search.
_Avoid_: Refcount snapshot, deletion list

**Container intrinsic summary**:
Immutable exact facts about one Container's physical and logical shape. It can
rank GC work but never describes current reachability or authorizes deletion.
_Avoid_: Live-byte counter, GC score, Container reference count

**GC candidate catalog**:
Rebuildable, generation-lagging estimates used to find Containers worth proving
or relocating. Stale entries may waste work but can never authorize retirement.
_Avoid_: GC truth, Location set, deletion list

**GC candidate proof**:
Generation-bound evidence that proves reachability and dependency closure for a
bounded victim set and verifies its replacement coverage before retirement. An
empty protected DATA set is authoritative evidence that no replacement coverage
is required.
_Avoid_: GC hint, reference count, complete pool scrub

**Metadata garbage collection**:
The removal of immutable Metadata Objects outside every durable recovery graph
and every live Manifest-reader root. It does not remove Commit Records, online
indexes, or user DATA Containers.
_Avoid_: DATA GC, index compaction, metadata cleanup

**Metadata root pin**:
A temporary liveness root retained by a Manifest reader or an unpublished
successor proof. Its complete immutable Manifest graph remains protected until
the final owner releases it.
_Avoid_: Metadata reference count, Exact generation pin, file handle

**Metadata mark catalog**:
Rebuildable acceleration describing one previously exact Metadata reachability
mark. It may suppress redundant collection work but never authorizes deletion.
_Avoid_: Metadata reference count, deletion list, recovery root

**Metadata mark delta**:
An immutable additive extension from one Metadata Mark Catalog generation to
the next. It records newly durable roots without granting deletion authority.
_Avoid_: Reference-count update, Metadata deletion proof, WAL record

**Metadata GC mark mode**:
The way one Metadata-GC quantum established its retained-object view: reuse,
an additive delta, or an exact snapshot. Only the exact snapshot carries
deletion authority.
_Avoid_: Exact-mark Boolean, catalog state, GC result

**Online GC recovery finalizer**:
The idempotent startup operation that derives effective `RETIRING` Locations
from the active Exact generation, completes any remaining verified victim
unlinks, and records `REMOVED` after the DATA directory is durable. A missing
victim is expected evidence of an interrupted earlier finalization, not proof
of corruption by itself.
_Avoid_: GC retry, scrub repair, candidate proof

**Adaptive Online GC quantum**:
A bounded maintenance attempt whose pace depends on Data Pool pressure and
recent frontend activity. Background, idle, and urgent pace change admission
frequency, CPU priority, and candidate count; none grants deletion authority or
permission to compete in the frontend I/O class.
_Avoid_: Full GC, scrub cycle, frontend throttle

**Online GC request**:
An operator request delivered to the currently writable appliance to start one
urgent Adaptive Online GC quantum. It does not create a second storage owner
and does not mean exclusive full-speed maintenance.
_Avoid_: Offline gc-now, scheduled GC, Appliance Lease

**Reclaimable container**:
A verified Container with no Chunk reachable from any currently pinned online
generation. A partially live Container is not reclaimable until relocation.
_Avoid_: Empty container, retiring container

**Compaction victim set**:
Verified partially live Containers whose uncovered live Chunks can be moved to
fewer replacement Containers before the originals become reclaimable.
_Avoid_: Garbage list, empty Containers

**Rebuild**:
The generation-building recovery process that derives new online indexes from
durable containers, a Recovery Checkpoint, and reachable object dependencies.
_Avoid_: Scrub, normal startup

**Degraded start**:
A mount after complete structural validation but before every stored chunk has
been rehashed. Every chunk is still fully verified when read.
_Avoid_: Unverified read, rebuild

**Re-anchoring**:
The explicit maintenance rewrite that changes the logical Base Chunk used by
dependent encodings. It is separate from relocating a base's physical Location.
_Avoid_: Garbage collection, relocation

## Write and encoding

**Dirty extent map**:
The bounded set of modified file ranges awaiting conversion into a new immutable
manifest, without rechunking the entire file.
_Avoid_: Dirty file, write buffer

**Checkpoint pressure**:
The unique DATA bytes in reachable active Dirty Extent Maps that can be moved
into the next Commit Group. It excludes holes, duplicate overwrites, a frozen
Commit Group, encoder work buffers, and process RSS.
_Avoid_: Logical bytes, resident memory, bytes written

**Mutation sequence**:
The monotonic per-inode order in which accepted content and metadata changes are
applied to the live view and partitioned into commit generations. Later accepted
overlapping writes supersede earlier ones.
_Avoid_: SMB ordering, wall-clock order

**Admitted mutation**:
A content or namespace change that passed permission and lock checks, reached
fastdup, received a mutation sequence, and may therefore be acknowledged.
_Avoid_: Kernel-dirty page, attempted write

**Active dirty epoch**:
The current ordered mutations accepted after the most recent commit cut. They
remain immediately readable and may advance while an earlier cut is persisted,
but are not included in that earlier recoverable generation.
_Avoid_: Frozen commit cut, write transaction

**Frozen commit cut**:
The single immutable prefix of accepted Namespace and inode mutations currently
being persisted or retained for retry. Later mutations never change its token,
bytes, names, or ordering. Forming the cut waits for every already admitted
mutation to finish its Ingest-Lane observer; it does not hold mutation admission
closed while the generation is persisted.
_Avoid_: Active dirty epoch, filesystem freeze

**Ingest lane**:
A bounded process-local SeqCDC and Container-staging stream for one hot inode.
It controls reduction continuity but has no durable identity and does not define
commit visibility.
_Avoid_: Commit group, worker thread

**Ingest queue**:
The bounded process-local payload accepted for asynchronous reduction but not
yet completed through its Ingest Lane. It defines RAM backpressure, not commit
visibility or durability.
_Avoid_: Dirty epoch, durable journal

**Commit group**:
A batch of ordered content and namespace mutations whose durability and recovery
visibility advance atomically. It does not determine compression boundaries.
_Avoid_: Compression group

**Compression region**:
A byte-exact encoding unit built from one or more adjacent logical chunks.
It does not determine transactional visibility.
_Avoid_: Commit group, container

**Encoding policy**:
A versioned rule that chooses RAW or a codec using complete physical-byte cost,
CPU cost, and read cost. It does not change logical content identity.
_Avoid_: Codec, chunking profile

**Policy set**:
An immutable, identified collection of writer, placement, and maintenance rules
selected by a commit generation. It never replaces decode parameters stored with
existing objects.
_Avoid_: Feature flags, software version

**Placement window**:
A bounded input range within which physical ordering may be changed to improve
compression while retaining restore locality.
_Avoid_: Global reorder, compression region

**Forced chunk boundary**:
A versioned manifest boundary introduced to cap rechunking work when content-
defined boundaries do not resynchronize in time. It is not a change to the Gear
algorithm.
_Avoid_: CDC profile change, sparse boundary

**Chunking profile**:
A versioned byte-to-boundary rule used by one DATA region. Different regions of
the same file may use different profiles without changing logical chunk identity.
_Avoid_: Compression profile, file type

**Prepared extent recipe**:
Process-local proof carried from a byte-verified externalized dirty range into
the Frozen Commit Cut. It may avoid re-reading a complete Chunk or FILL extent,
but it neither contains a physical Location nor makes a generation visible;
generation publication must still verify every changed DATA dependency.
Stable SeqCDC Chunks still below the normal Container-fill threshold are
drained immediately after the cut and may attach this proof to the frozen range;
only one boundary Chunk plus the incomplete bounded CDC suffix remain resident
for checkpoint replay.
_Avoid_: Manifest, Exact-Index hint, commit record

**Range clone**:
One atomic target mutation that reuses the verified immutable recipe of an
equal-length source byte range. It is metadata-only: it neither copies source
bytes through the frontend nor creates new logical Chunk identities. Veeam SMB
Fast Clone reaches it through the Samba Duplicate Extents adapter and FUSE
`copy_file_range`.
_Avoid_: Buffered copy, Exact-Dedup hit, snapshot

**Fill extent**:
A DATA range consisting of one repeated byte value. It is reconstructed without
a chunk location but remains distinct from a sparse hole, including when the
value is zero.
_Avoid_: Hole, zero chunk

## Guarantees

**Durability window**:
The acknowledged history that may be lost after a process crash or power loss.
Every successful write must become part of a recoverable commit within ten
seconds, regardless of open handles or application-level file completeness.
_Avoid_: Flush interval, retention period

**Recovery checkpoint**:
A data-tier copy of namespace and manifest recovery state used to rebuild after
the complete loss of the metadata tier. It may intentionally lag normal crash
durability.
_Avoid_: Online metadata index, commit group

**Recovered generation**:
The newest wholly valid commit generation selected after a crash. Recovery does
not combine independently newer fragments from different commit generations.
_Avoid_: Best-effort merge, live view

**Recovery transition prefix**:
The contiguous selected Commit-Log prefix whose Namespace Root bindings and pairwise
namespace, inode, allocation, and reservation transitions all validate. It
proves structural ordering but not Manifest or DATA completeness; the current
and immediately previous generations inside it are separate graph-proof
candidates.
_Avoid_: Recovered generation, complete DATA graph

**Successor graph proof**:
The complete DATA-dependency proof for one new Commit Group formed by retaining
unchanged dependencies from the immediately preceding verified generation and
fully verifying every newly introduced dependency. It is valid only while that
predecessor remains the installed generation and immutable storage stays under
the same appliance process; recovery and scrub construct fresh complete proofs.
_Avoid_: Exact-Index authority, cache hit, partial verification

**Generation pin**:
A temporary liveness root retained for readers, normal rollback, or data-tier
recovery. Historical existence alone does not make an old generation live.
_Avoid_: Snapshot, reference count

**Exact Index generation pin**:
A temporary authorization to select and read physical Locations through one
immutable Exact Index generation. A retiring generation admits no new pins;
pins from every still-live predecessor generation must drain before any
shadowed Container is removed. Cached or dormant Manifest readers retain only
an uncounted generation snapshot and acquire a pin for each bounded DATA read;
after retirement closes admission they fall back to verified Container
discovery.
_Avoid_: Exact Index reference, Container reference count

**Corruption**:
A state in which stored bytes or metadata fail their integrity contract. Corrupt
bytes are never returned as valid file content.
_Avoid_: Expected I/O error

**Supported failure envelope**:
The failures covered by one declared release profile while its remaining storage
continues to make bounded I/O progress. The MVP covers process and power loss on
functioning storage; single-device loss belongs to a later production profile.
_Avoid_: Every hardware failure, disaster recovery

**Appliance state**:
The explicit health state controlling whether new mutations may be admitted.
Background activities such as scrub are flags rather than mutually exclusive
health states.
_Avoid_: Metric, process status

**Control plane**:
The operator-facing configuration and orchestration view of one Appliance. It
may start work and retain observations, but it is never content, liveness, Pool
identity, or recovery authority.
_Avoid_: Repository database, storage authority, metadata tier

**Logical Share quota**:
A hard upper bound on allocated logical bytes reachable below one managed Share
root. DATA, FILL, Exact-Dedup and Clone ranges count at their full logical
length; sparse holes do not. Admission returns `ENOSPC` before acknowledging a
mutation that would exceed the bound. It is independent of physical Repository
capacity admission and does not reserve physical bytes.
_Avoid_: Transfer limit, physical reservation, reduction allowance

**Share capacity presentation**:
The `statfs` geometry derived from one Logical Share quota. Total bytes equal
the quota; free and available bytes are the minimum of remaining logical quota
and current Repository-wide physical availability.
_Avoid_: Storage authority, physical capacity, independent quota

**Provisioning target**:
A currently discovered Block-Layer device eligible to become exactly one
Metadata or Data Pool after a fresh topology and ownership check.
_Avoid_: Device path, Pool, repository

**Repository runtime**:
The single live mount owner bound to one verified Metadata/Data Pool pair. Its
process state is replaceable and does not define the Repository's durable state.
_Avoid_: Repository, Appliance ID, Control plane

**Repository format epoch**:
A compatibility fence carried by the authoritative Commit chain. Every current
repository begins at epoch one; epoch zero and unknown epochs are unsupported
pre-production state rather than migration inputs. The fence prevents an older
writer from silently advancing the repository.
_Avoid_: Object version, software version, Policy Set

**Appliance recovery latch**:
A durable marker requiring the next appliance owner to complete recovery or
offline scrub before it may mutate the repository. Its presence is conservative
evidence of an unproven shutdown, not evidence of corruption.
_Avoid_: Health record, PID file, dirty bit

**Appliance lease**:
Exclusive cross-process ownership permitting one daemon or offline maintenance
process to advance generations for an appliance. A persistent lease object
names the ownership seam while its held kernel lease supplies live authority.
_Avoid_: POSIX lock, generation pin

**POSIX state**:
The file content and structural metadata needed to reproduce sparse extents,
links, ownership, permissions, timestamps, ACLs, and extended attributes.
Internal allocation choices are not part of this state.
_Avoid_: File bytes, XFS layout

**Extended attribute**:
A bounded byte-exact name/value pair versioned with one inode. POSIX ACL wire
values and retention hints are extended attributes; they are not DATA extents.
_Avoid_: File stream, immutable policy

**Immutable inode flag**:
The enforced `FS_IMMUTABLE_FL` state that rejects content, metadata, and name
mutations until an authorized request clears it. A retention-time xattr may
explain when management software intends to clear the flag but does not replace
the flag itself.
_Avoid_: Read-only mount, retention timestamp
