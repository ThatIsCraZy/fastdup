# Metadata generation format v1

Status: experimental and implemented; pre-`format-v1-stable`.

This specification records the exact durable bytes and publication behavior of
the first metadata-generation checkpoint. It makes the current implementation
of immutable Manifest leaves, the flat Namespace Root, and the Commit WAL
auditable without claiming the complete POSIX Exact-Dedup MVP.

The governing decisions are [ADR 0011](../adr/0011-use-hierarchical-immutable-manifests.md),
[ADR 0012](../adr/0012-publish-one-namespace-root-per-commit.md),
[ADR 0019](../adr/0019-commit-only-after-data-and-metadata-are-durable.md),
[ADR 0029](../adr/0029-version-writer-policy-without-stranding-old-data.md),
and [ADR 0034](../adr/0034-reserve-inode-ids-before-visibility.md).
Container data bytes and their separate publication protocol remain defined by
[Container format v1](container-v1.md) and
[Container Store v1](container-store-v1.md).

## Scope and byte conventions

All multibyte integers are unsigned little-endian. Byte strings have no byte
order. Rust layout, enums, pointer widths, padding, and collection layout are
never serialized. Every addition, multiplication, offset, length, count, slice,
and allocation is validated with checked arithmetic or against an already
validated bound.

The checksum algorithm is CRC-32C (Castagnoli), using the conventional reflected
initial and final XOR and the same algorithm defined by Container v1. The hash
algorithm is unkeyed BLAKE3 with its default 32-byte output. Unless a table
assigns a field, it is reserved, written as zero, and rejected when nonzero.

The implemented constants are:

| Name | Value |
| --- | ---: |
| Metadata Object alignment | 4,096 bytes |
| Metadata Object header | 4,096 bytes |
| Maximum Metadata Object file length | 16,777,216 bytes (16 MiB) |
| Manifest payload header | 64 bytes |
| Manifest extent entry | 64 bytes |
| Namespace Root payload header | 128 bytes |
| Durable Inode record | 96 bytes |
| Namespace Entry fixed header | 24 bytes |
| Namespace Entry alignment | 8 bytes |
| Commit Record | 4,096 bytes |
| Maximum Commit WAL length | 67,108,864 bytes (64 MiB) |

## Generic Metadata Object envelope

Manifest leaves and Namespace Roots use the same content-addressed envelope.
The file layout is:

```text
[4 KiB envelope header][exact payload][zero padding to 4 KiB]
```

The actual file length is a multiple of 4,096, is no greater than 16 MiB, and
equals `align_up(4096 + payload_length, 4096)`. The payload starts at absolute
offset 4,096. Every byte after the payload through EOF is zero.

### Envelope header

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDMDOBJ1` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `4,096` |
| 12 | 2 | `object_kind` | `1` = Manifest Leaf, `2` = Namespace Root |
| 14 | 2 | `object_id_algorithm` | `1` = unkeyed BLAKE3-256 |
| 16 | 8 | required-flags slot | zero |
| 24 | 8 | compatible-flags slot | zero |
| 32 | 8 | `payload_length` | exact unpadded payload bytes |
| 40 | 8 | `file_length` | exact actual aligned file length |
| 48 | 32 | `object_id` | nonzero Metadata Object ID described below |
| 80 | 4 | `payload_crc32c` | CRC-32C of exactly the payload |
| 84 | 4 | `header_crc32c` | CRC-32C of all 4,096 header bytes with this field zero |
| 88 | 4,008 | reserved | zero |

The header field widths sum to 4,096 bytes. The Header CRC covers the stored
Object ID, payload length, file length, and Payload CRC. It does not cover the
payload directly; the Payload CRC and Object ID provide those checks.

### Metadata Object ID

The Metadata Object ID is the unkeyed BLAKE3-256 output over this exact sequence
of updates, without separators beyond the bytes shown:

1. the 27-byte ASCII/domain byte string
   `fastdup-metadata-object-v1\0`, including the trailing NUL;
2. `object_kind` as a two-byte little-endian integer;
3. `payload_length` as an eight-byte little-endian integer;
4. the exact unpadded payload bytes.

Envelope header bytes and file padding are not part of the Object ID. The
all-zero 32-byte value is invalid. Reader verification requires the stored ID,
the recomputed ID, and the ID encoded by an internal published filename to
agree where that filename is used.

## Manifest Leaf v1 payload

A Manifest Leaf is Metadata Object kind `1`. It is currently the complete
Manifest Root for one file; Manifest inner nodes do not yet exist. Its payload
contains one header followed by `extent_count` fixed entries and no trailing
bytes:

```text
payload_length = 64 + extent_count * 64
```

### Manifest header

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDMANL01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `64` |
| 12 | 2 | `extent_entry_length` | `64` |
| 14 | 2 | reserved | zero |
| 16 | 8 | required-flags slot | zero |
| 24 | 8 | `file_length` | complete logical file length |
| 32 | 4 | `extent_count` | number of entries |
| 36 | 4 | `payload_length` | exact header-plus-entries length |
| 40 | 24 | reserved | zero |

The field widths sum to 64 bytes.

### Manifest extent entry

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `logical_offset` | exact start in the file |
| 8 | 8 | `logical_length` | nonzero extent length |
| 16 | 2 | `extent_kind` | `1` = DATA, `2` = HOLE, `3` = FILL |
| 18 | 2 | reserved | zero |
| 20 | 4 | `chunk_length` | DATA length; otherwise zero |
| 24 | 32 | `chunk_id` | DATA Chunk ID; otherwise zero |
| 56 | 1 | `fill_byte` | FILL value; otherwise zero |
| 57 | 7 | reserved | zero |

The field widths sum to 64 bytes. Entries are ordered by logical offset and
must form an exact, gap-free, non-overlapping partition of `[0, file_length)`.
The first offset is zero, each later offset is the checked end of its
predecessor, and the final checked end equals `file_length`. An empty file has
zero extents; a nonempty file has at least one extent. Zero-length extents are
invalid.

DATA requires `chunk_length == logical_length`, a zero `fill_byte`, and a
logical length no greater than 262,144 bytes (256 KiB). HOLE requires a zero
Chunk ID, zero `chunk_length`, and zero `fill_byte`. FILL requires a zero Chunk
ID and zero `chunk_length`; all 256 fill-byte values, including zero, are valid.
FILL remains allocated logical data and is not a sparse HOLE.

The 16-MiB envelope bound permits at most 262,079 extent entries in one current
leaf. A larger or hierarchical file recipe requires future Manifest inner nodes
rather than an oversized v1 object.

## Namespace Root v1 payload

A Namespace Root is Metadata Object kind `2`. Version 1 is deliberately flat
and bounded: inode versions and directory entries are embedded in one object,
and the only directory is the implicit root Inode ID `1`. Every encoded inode
record represents a regular file. This is a pre-stable checkpoint format, not
the final scalable namespace tree.

The payload layout is:

```text
[128-byte header]
[inode_count * 96-byte Durable Inode records]
[entry_count variable Namespace Entry records]
```

There are no bytes after the final Namespace Entry. Empty namespaces consist of
the 128-byte payload header alone.

### Namespace Root header

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDNSRT01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `128` |
| 12 | 2 | `inode_record_length` | `96` |
| 14 | 2 | `entry_header_length` | `24` |
| 16 | 8 | required-flags slot | zero |
| 24 | 8 | compatible-flags slot | zero |
| 32 | 8 | `root_inode` | `1` |
| 40 | 8 | `inode_reservation_end` | at least `2`, and greater than every inode record ID |
| 48 | 8 | `namespace_mutation_sequence` | committed namespace mutation cutoff |
| 56 | 4 | `inode_count` | number of Durable Inode records |
| 60 | 4 | `entry_count` | number of Namespace Entry records |
| 64 | 8 | `inodes_offset` | `128` |
| 72 | 8 | `entries_offset` | `128 + inode_count * 96` |
| 80 | 8 | `payload_length` | exact payload length through the final entry |
| 88 | 8 | `inode_allocation_cursor` | at least `2`, no greater than `inode_reservation_end`, and greater than every inode record ID |
| 96 | 32 | reserved | zero |

The field widths sum to 128 bytes. Before allocating record vectors, a reader
proves the inode byte equation, `entries_offset <= payload_length`, and that
`entry_count` can fit in the remaining payload using the minimum 32-byte aligned
entry length. This prevents corrupt counts from selecting an unbounded
allocation.

`inode_reservation_end` is the exclusive end of the durably reserved range;
`inode_allocation_cursor` is the first ID not yet consumed in the selected
namespace generation. Thus all visible inode IDs are strictly below the cursor,
and the cursor may equal the reservation end when the range is exhausted.
Recovery-side allocator integration must begin at the greatest reservation end
in the structurally valid WAL prefix, not at the selected root's cursor or the
largest visible inode. This deliberately skips unused and crash-lost IDs and
prevents reuse after falling back to an older metadata graph.

### Durable Inode record

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `inode_id` | greater than `1` |
| 8 | 2 | `mode` | stored POSIX mode bits |
| 10 | 2 | reserved | zero |
| 12 | 4 | `uid` | stored owner UID |
| 16 | 4 | `gid` | stored owner GID |
| 20 | 4 | `link_count` | nonzero and exactly cross-checked against entries |
| 24 | 8 | `mutation_sequence` | committed per-inode mutation sequence |
| 32 | 8 | `logical_size` | exact file length expected from its Manifest |
| 40 | 32 | `manifest_root` | nonzero Metadata Object ID |
| 72 | 24 | reserved | zero |

The field widths sum to 96 bytes. Records are strictly increasing by numeric
`inode_id`; duplicate or reordered records are invalid. No record is written
for root Inode ID `1`, a directory, a handle, a FUSE lookup reference, or an
Open Orphan.

### Namespace Entry record

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 4 | `record_length` | `align_up(24 + name_length, 8)` |
| 4 | 2 | `name_length` | `1..=255` |
| 6 | 2 | reserved | zero |
| 8 | 8 | `parent_inode` | `1` |
| 16 | 8 | `target_inode` | greater than `1` and present in the inode table |
| 24 | variable | `name` | exact component bytes |
| after name | variable | padding | zero through `record_length` |

The fixed field widths sum to 24 bytes. Names are case-sensitive byte strings;
they need not be UTF-8 and undergo no Unicode normalization. Empty names, NUL,
slash, `.` and `..` are invalid.

Entries are strictly ordered by `(parent_inode, unsigned-bytewise name)`. Since
v1 accepts only parent `1`, this is bytewise name order. Duplicate names are
invalid. Every entry target must exist, every inode must have at least one
entry, and the number of entries targeting each inode must equal that inode's
`link_count`. These checks preserve hardlink identity and prevent durable Open
Orphans or dangling entries.

## Commit Record v1

The Commit WAL is a byte concatenation of fixed 4,096-byte Commit Records. A
Commit Record is not a Metadata Object and has no envelope or filename-derived
identity.

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDCMIT01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `record_type` | `1` = Namespace commit |
| 12 | 2 | `header_length` | `176` |
| 14 | 2 | `record_hash_algorithm` | `1` = unkeyed BLAKE3-256 |
| 16 | 4 | `record_length` | `4,096` |
| 20 | 2 | `checksum_algorithm` | `1` = CRC-32C |
| 22 | 2 | reserved | zero |
| 24 | 8 | reserved | zero |
| 32 | 8 | reserved | zero |
| 40 | 8 | `generation` | nonzero commit generation |
| 48 | 32 | `previous_record_hash` | exact prior-record hash, or zero for generation 1 |
| 80 | 32 | `namespace_root` | nonzero Namespace Root Metadata Object ID |
| 112 | 32 | `policy_set` | nonzero Policy Set ID |
| 144 | 8 | `namespace_mutation_cutoff` | must equal the selected Namespace Root value |
| 152 | 8 | `inode_reservation_end` | at least `2`; must equal the selected Namespace Root value |
| 160 | 8 | `inode_allocation_cursor` | in `2..=inode_reservation_end`; must equal the selected Namespace Root value |
| 168 | 4 | `record_crc32c` | CRC-32C of all 4,096 bytes with this field zero |
| 172 | 4 | header reserved | zero |
| 176 | 3,920 | record padding | zero |

The field widths sum to 4,096 bytes, and the declared header ends at offset
176. The Record CRC authenticates all fields and padding against accidental
damage. `CommitRecordHash` is unkeyed BLAKE3-256 of all exact finalized 4,096
record bytes, including the stored Record CRC; it has no additional domain
prefix. That hash is stored only in the next record.

Generation 1 requires a zero predecessor hash. Every later generation requires
a nonzero predecessor hash. WAL validation additionally requires record ordinal
`n` to contain generation `n + 1` and its predecessor hash to equal BLAKE3-256
of the immediately preceding exact record. Namespace mutation cutoff, inode
reservation end, and inode allocation cursor must each be monotone
nondecreasing across the structurally valid record chain. A Commit Record
contains no mutable root pointer and no separately stored self-hash.

`PolicySetId` is currently an opaque nonzero 32-byte identity supplied when the
repository is opened. Recovery requires every record it reaches to equal the
configured supported Policy Set. An unknown Policy Set is a refused mount, not
permission to roll back to an older policy and continue. This checkpoint does
not yet serialize or transitively verify a Policy Set object; durable decoders
remain governed by their object-local format fields.

## Canonical internal names

A Metadata Object ID is encoded as exactly 64 lowercase hexadecimal characters
in byte order.

- published Metadata Object: `<object-id>.fdm`
- temporary Metadata Object: `.<object-id>.building`
- Commit WAL: `commit.wal`

Metadata publication looks up these exact ASCII names. Recovery follows IDs
referenced from Commit Records and Namespace Roots; it does not currently scan
or reject unrelated or malformed unreferenced `.fdm` names. Temporary and
unreferenced valid objects are not selected by a Commit Record and remain
invisible metadata orphans.

## Writer and publication protocol

Only one external writer may use a repository. The current repository assumes
that exclusivity; it does not yet implement the durable Appliance Lease.

### Immutable Metadata Object publication

For each Manifest Leaf, and later for the Namespace Root, the writer:

1. Encodes and validates the complete generic envelope and derives its Object
   ID.
2. If the exact published name already exists, reads it through the generic
   envelope verifier and requires both byte-for-byte equality and the same
   Object ID. Identical content is reused only after synchronizing the
   containing directory again; invalid or different bytes under the same name
   are a Metadata identity collision.
3. Otherwise creates the non-replacing temporary name.
4. Writes the complete encoded object in consecutive blocks of at most 4,096
   bytes.
5. Sets the exact file length.
6. Re-reads the complete file and requires exact byte equality plus the same
   production-verified Object ID.
7. Synchronizes the temporary file.
8. Renames it to the published name without replacement.
9. Synchronizes the containing directory.

Unlike Container v1, a Metadata Object has no BUILDING/SEALED state in its
header. Its temporary filename controls publication. A failure may leave a
temporary object or a fully published but unreferenced object; neither becomes
visible namespace state without a later valid Commit Record.

### Namespace generation commit

The implemented `commit_namespace` and `commit_namespace_with_data` sequence is:

1. For every Durable Inode, read the exact published Manifest object named by
   `manifest_root`, verify its envelope and Manifest payload, require its
   `file_length` to equal the inode's `logical_size`, and collect every unique
   DATA Chunk ID with its required logical length. Conflicting lengths for one
   Chunk ID are rejected. The plain method accepts only a graph with no DATA;
   `commit_namespace_with_data` additionally verifies every required Chunk
   against the supplied durable Container Repository before proceeding.
2. Encode and publish the complete Namespace Root using the immutable-object
   protocol above.
3. Ensure `commit.wal` exists. Initial creation sets its length to zero,
   synchronizes the file, and synchronizes the containing directory. If the WAL
   name already exists, the implementation checks its bounded size and
   synchronizes the containing directory again before relying on the name.
4. Read the complete WAL, reject a length above 64 MiB, and require a wholly
   clean decoded chain with no torn, invalid, or broken tail.
5. For generation 1, require an empty namespace and
   `inode_allocation_cursor == 2`. The root may advance
   `inode_reservation_end`; that reservation-only generation must become
   durable before any inode from the range is made visible. For a later
   generation, load the preceding Namespace Root and verify the transition
   rules below.
6. Construct the next generation and predecessor hash. Generation 1 uses zero;
   otherwise generation and hash derive from the last valid record.
7. Construct a Commit Record using the repository's supported Policy Set and
   the Namespace Root's exact namespace mutation sequence, reservation end, and
   allocation cursor.
8. Write the record at the prior EOF, set the exact new length, and reject a new
   length above 64 MiB.
9. Re-read the complete WAL and require the old prefix unchanged, the appended
   record byte-exact, and the entire chain clean.
10. Synchronize `commit.wal`. Only this final successful WAL sync publishes the
   generation for normal crash recovery.

For every noninitial transition, namespace mutation sequence, reservation end,
allocation cursor, and every retained inode's mutation sequence are monotone
nondecreasing. The proposed allocation cursor may not exceed the *previously*
durable reservation end, so extending a range and consuming it in the same
generation is forbidden. A newly appearing inode below the previous allocation
cursor is rejected as ID reuse. Deleting an inode is permitted, but these rules
prevent it from later reappearing under the consumed ID.

The 64-MiB cap admits exactly 16,384 Commit Records. There is no implemented WAL
segmentation, rotation, compaction, or automatic tail repair. A dirty tail
blocks further commits with `WalNeedsRepair`.

The normal ordering required by ADR 0019 is data containers, immutable metadata
objects, then the Commit Record. `commit_namespace_with_data` implements that
proof boundary by accepting a DATA graph only after all referenced Chunk IDs
and lengths are found in fully verified, already published containers. The
plain `commit_namespace` intentionally fails closed on such a graph rather than
assuming a location source. The current proof is correct but expensive: it
scans published containers because the future persistent Exact Index is not yet
connected as a verified acceleration structure.

### Verified Manifest demand reads

`VerifiedManifestFile` is the implemented immutable read seam for one decoded
Manifest and one Container Repository. `fastdup-appliance::recover_mount`
type-erases it behind the POSIX `CommittedFile` boundary and mounts the recovered
Namespace through the same dispatch seam as FUSE. The read-only recovery path
and the durable commit path consume opaque inode-associated readers emitted by
the same complete graph verification, avoiding an immediate duplicate
Container scan. The standalone constructor still collects DATA dependencies,
rejects conflicting logical lengths for the same Chunk ID, and batch-verifies
that every dependency has a matching record in a fully verified published
container. No caller can construct the graph-proof type or attach an unrelated
Manifest. A reader retains the Manifest and repository, not a materialized copy
of the logical file.

`read_at(offset, length)` accepts at most 1 MiB per request, clips at EOF, and
reconstructs only the requested range. HOLE produces zero bytes and FILL
produces the stored byte. For every touched DATA extent, the implementation
locates the Chunk again and fully verifies its immutable container and exact
logical length before copying any requested bytes. A post-construction
container corruption therefore fails the demand read instead of returning
previously trusted bytes. Both dependency verification and demand location are
currently full directory/container scans; there is not yet a persistent
verified Chunk-to-Location index on this path.

## Crash outcomes

- A crash before Metadata Object file synchronization may leave incomplete or
  apparently complete temporary bytes. No Commit Record can name them through
  the published object name.
- A crash after object synchronization but before or during the no-replace
  rename may leave either the temporary object or the published object. A later
  Commit Record still has not been synchronized.
- A crash after directory synchronization but before WAL publication leaves a
  verified published metadata orphan.
- A crash during the 4-KiB WAL append may leave the prior clean WAL, a short
  tail, an invalid complete record, or a complete valid record. Recovery uses
  only the contiguous valid prefix and independently validates its referenced
  graph.
- A complete record whose synchronization reports failure has an uncertain
  durability outcome. Recovery may select it only if its exact bytes and full
  graph validate after restart; the writer must not infer success from the
  attempted append alone.
- After the final WAL synchronization succeeds, the exact Namespace Root and
  every accepted transitive metadata dependency are recoverable under the
  supported stable-storage assumptions. For a `_with_data` commit, this claim
  also depends on the independently synchronized and verified Container
  Repository supplied to the operation.
- A later Commit Record may durably advance `inode_reservation_end` even when
  its metadata graph is subsequently found corrupt. Recovery retains the
  greatest reservation end from the structurally valid WAL prefix and must not
  reuse the now skipped range while exposing an older complete graph.

## Recovery order

Normal recovery performs these steps:

1. Read `commit.wal`. A missing WAL means no generation; an empty WAL also means
   no generation. Reject a WAL above 64 MiB.
2. Divide the WAL into complete 4,096-byte records and a possible short tail.
   Decode records from offset zero. Stop at the first invalid record, unexpected
   generation, or wrong predecessor hash. Classify remaining bytes as `Clean`,
   `Torn`, `InvalidRecord`, or `BrokenChain`. A nonempty WAL with no valid first
   record has no recoverable generation.
3. Compute the maximum `inode_reservation_end` over the structurally valid WAL
   prefix. This remains the allocator high-water mark even if graph verification
   later selects an older generation.
4. Traverse the structurally valid prefix **forward**, oldest to newest. Refuse
   recovery immediately if any reached record has a Policy Set ID other than
   the repository's supported Policy Set; do not hide an unknown writer policy
   by rolling back.
5. Read `<namespace_root>.fdm`; enforce the 16-MiB bound, generic envelope
   identity, kind `2`, complete Namespace Root payload invariants, link counts,
   reservation bound, and allocation cursor bound.
6. Require the root's namespace mutation sequence, reservation end, and
   allocation cursor to equal all three values copied into the Commit Record.
7. For every Durable Inode, read `<manifest_root>.fdm`; validate the filename
   identity, generic envelope, kind `1`, Manifest partition, and equality of
   Manifest `file_length` with inode `logical_size`. Collect every DATA Chunk ID
   and exact length. `recover_latest_with_data` verifies them against the
   supplied Container Repository; plain `recover_latest` fails closed with
   `DataLocationsNotConnected` if any are present.
8. Verify the same monotone namespace, reservation, allocation, inode-mutation,
   and never-reuse transition rules as the writer. Generation 1 must have
   allocation cursor `2` and no visible inode.
9. After each wholly verified record, advance the selected complete generation.
   At the first missing or corrupt graph or invalid transition for which
   fallback is safe, stop the forward walk and retain the immediately preceding
   complete generation. Do not skip the failed generation and attempt a later
   root, and never merge a Namespace Root, inode, Manifest, or DATA dependency
   from different generations.
10. Report the observed WAL tail, the number of structurally valid generations
   newer than the selected record, and the prefix reservation high-water mark.
   If no record has a complete accepted graph, return `NoRecoverableGeneration`
   rather than exposing a partial namespace.

Missing objects, format/identity damage, length disagreement, missing verified
Chunks, or an invalid generation transition permit atomic fallback to the last
complete earlier graph. Unknown Policy Sets, transient I/O, and using the plain
recovery method for a DATA-bearing graph are refused rather than classified as
corruption. Recovery does not truncate or repair the WAL; because commits
require a clean tail, a separate future repair protocol is required before that
repository can advance again.

## Paired invariants and implemented evidence

| Invariant | Writer boundary | Reader/recovery boundary | Fault evidence |
| --- | --- | --- | --- |
| Object ID identifies exact kind and payload | derive after canonical payload encoding; re-read exact bytes before sync | recompute domain-separated ID and pair it with every referenced filename | substituted or mutated valid-looking bytes fail identity/equality checks |
| Manifest is one complete byte-exact recipe | validate extent kinds and full partition before envelope encoding | repeat envelope, entry, offset, length, and partition validation | every truncated prefix and every single-byte mutation is rejected without panic |
| Namespace has no dangling or reused inode state | canonicalize, validate target existence, cross-check every link count, and bound every ID below the allocation cursor | reject reordered/duplicate IDs and names, dangling targets, orphans, bad names, decreasing cursors, or IDs reused below a prior cursor | reauthenticated order/count corruption, invalid transitions, and exhaustive truncation/byte corruption are rejected |
| Inode reservation precedes visibility | generation 1 is reservation-only; later allocation cannot cross the preceding record's durable reservation end | validate forward transitions and retain the structurally valid WAL reservation high-water across graph fallback | premature use of a newly extended range and reuse of a removed inode both fail |
| Commit Record is atomic visibility | publish and sync dependencies before append; re-read exact append; sync WAL last | accept only a contiguous CRC/hash chain and then verify complete graphs in forward order | fail-before/fail-after publication and WAL operations recover only an older or wholly valid generation |
| DATA is durable before visibility and verified before reads | `_with_data` commit verifies every referenced ID and length in published containers before WAL append | `_with_data` recovery repeats dependency verification; demand reads re-verify the containing container | missing DATA prevents commit, corrupt newest DATA falls back atomically, and corruption after file construction fails demand reads |
| Counts cannot select unbounded allocations | preflight payload lengths and record equations | prove counts against the bounded payload before vector allocation | `entry_count = u32::MAX` fails as invalid payload under a constrained address space |
| Policy selection is explicit | store the configured nonzero Policy Set ID in every record | refuse any reached record whose ID is not exactly supported | an unknown newer policy refuses recovery instead of silently rolling back |

## Explicit limitations

This implemented checkpoint intentionally does **not** provide:

- a fake-clock proof that the implemented five-second scheduler and admission
  backpressure always meet the ten-second contract under the supported bounded
  I/O envelope; `fsync` deliberately remains no stronger than that contract;
- an indexed DATA-location lookup: proof-carrying `_with_data`
  commit/recovery and `VerifiedManifestFile` are implemented, but dependency
  and demand lookup scan published containers and the plain methods deliberately
  refuse DATA;
- Manifest inner nodes, bounded path rewriting, or files whose complete recipe
  exceeds one 16-MiB Metadata Object;
- a scalable Namespace tree: NamespaceRoot v1 rewrites one flat bounded object,
  contains only regular inodes below the implicit root directory, and has no
  nested directories, symlinks, ACLs, xattrs, timestamps, or directory metadata
  record beyond its reservation end and allocation cursor;
- WAL repair, segmentation, rotation, checkpointing, or growth beyond 64 MiB;
- serialization or transitive verification of the Policy Set itself;
- durable Appliance Lease enforcement, concurrent-writer coordination,
  metadata GC, data-tier Recovery Checkpoints, or metadata-tier-loss rebuild.

These omissions are stage boundaries, not implicit format promises. Unknown
future kinds, versions, flags, tree nodes, and record types require explicit
versioned readers and crash-safe transitions.
