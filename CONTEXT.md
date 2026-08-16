# fastdup Storage Appliance

fastdup is a single-node POSIX storage appliance that reduces physical storage
while preserving byte-exact file contents. This glossary names the concepts at
the product boundary; implementation and format details belong in specifications
and ADRs.

**MVP**:
The first usable FUSE appliance with crash-safe manifests, bounded updates,
FastCDC, Exact Dedup, bounded in-memory acceleration, and RAW/Zstd encodings.
Similarity, Delta, automatic GC, production Samba hardening, and device-loss
protection are later stages.
_Avoid_: Container-store milestone, production appliance

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

**Container envelope proof**:
The paired, checksummed Container Header and Footer plus physical length and
canonical filename. It proves structural identity, layout, and generation for
allocator recovery, but neither the payload hash nor any logical Chunk bytes.
_Avoid_: Verified Container, verified Location

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

**Capacity reservation**:
Committed worst-case physical capacity promised for later file writes, without
claiming that logical zeroes or dedup savings have already consumed it.
_Avoid_: File size, quota estimate

**Namespace root**:
The identity of the immutable directory and inode state selected by one commit
generation.
_Avoid_: Mount point, recovery checkpoint

**Metadata object**:
An immutable, content-identified metadata blob referenced by namespace or inode
nodes when it is too large to represent inline.
_Avoid_: User data chunk, online index

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

**Pool ID**:
The persistent random identity and declared role of one storage pool belonging to
an appliance. A mount path or device name is not a pool identity.
_Avoid_: Mount point, device serial

**Cache location**:
A removable extra physical encoding that can accelerate reads but never provides
the only durable coverage for a live logical chunk.
_Avoid_: Small-file location, active location

**Verified cache entry**:
A decoded chunk or region admitted to a shared read cache only after its complete
stored encoding and logical content identity were verified.
_Avoid_: Kernel-dirty page, cache location

**Recovery index**:
Container-local metadata from which stored logical chunk identities and physical
locations can be rediscovered without the online deduplication index.
_Avoid_: Dedup index

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

**Appliance lease**:
Exclusive durable ownership permitting one daemon or offline maintenance process
to advance generations for an appliance.
_Avoid_: POSIX lock, generation pin

**POSIX state**:
The file content and structural metadata needed to reproduce sparse extents,
links, ownership, permissions, timestamps, ACLs, and extended attributes.
Internal allocation choices are not part of this state.
_Avoid_: File bytes, XFS layout
