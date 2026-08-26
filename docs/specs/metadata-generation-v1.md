# Metadata generation format v1

Status: experimental and implemented; pre-`format-v1-stable`.

This specification records the exact durable bytes and publication behavior of
the first metadata-generation checkpoint. It makes the current implementation
of immutable Manifest trees, the bounded Namespace Root, and the paired Commit Log
auditable without claiming the complete POSIX Exact-Dedup MVP.

The governing decisions are [ADR 0011](../adr/0011-use-hierarchical-immutable-manifests.md),
[ADR 0012](../adr/0012-publish-one-namespace-root-per-commit.md),
[ADR 0019](../adr/0019-commit-only-after-data-and-metadata-are-durable.md),
[ADR 0029](../adr/0029-version-writer-policy-without-stranding-old-data.md),
[ADR 0034](../adr/0034-reserve-inode-ids-before-visibility.md), and
[ADR 0037](../adr/0037-separate-structural-recovery-from-current-data-proof.md),
plus the bounded-log protocol in
[ADR 0039](../adr/0039-rotate-namespace-commit-log-through-paired-slots.md) and
Metadata-object liveness in
[ADR 0066](../adr/0066-mark-metadata-from-durable-graphs-and-live-root-pins.md).
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
| Manifest inner-node payload header | 64 bytes |
| Manifest child-range entry | 64 bytes |
| Namespace Root payload header | 128 bytes |
| Durable Inode record | 96 bytes |
| Namespace Entry fixed header | 24 bytes |
| Namespace Entry alignment | 8 bytes |
| Commit Record | 4,096 bytes |
| Ordinary Commit Log slot length | 262,144 bytes (64 records) |
| Legacy `commit.wal` migration limit | 67,108,864 bytes (64 MiB) |

## Generic Metadata Object envelope

Manifest leaves, Manifest inner nodes, and Namespace Roots use the same
content-addressed envelope.
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
| 12 | 2 | `object_kind` | `1` = Manifest Leaf, `2` = Namespace Root, `3` = Exact Index Run Set, `4` = Manifest Inner Node |
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

## Manifest Leaf v1 and v2 payload

A Manifest Leaf is Metadata Object kind `1`. It is either the complete root of
a small file recipe or a child of a Manifest Inner Node. Its payload contains
one header followed by `extent_count` fixed entries and no trailing bytes:

```text
payload_length = 64 + extent_count * 64
```

### Manifest header

| Offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDMANL01` |
| 8 | 2 | `format_version` | `1` or `2` |
| 10 | 2 | `header_length` | `64` |
| 12 | 2 | `extent_entry_length` | `64` |
| 14 | 2 | reserved | zero |
| 16 | 8 | required-flags slot | zero |
| 24 | 8 | `file_length` | complete node-local logical length; equal to the whole file only when this leaf is the root |
| 32 | 4 | `extent_count` | number of entries |
| 36 | 4 | `payload_length` | exact header-plus-entries length |
| 40 | 24 | reserved | zero |

The field widths sum to 64 bytes.

### Manifest extent entry

| Relative offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `logical_offset` | exact start inside this leaf's node-local range |
| 8 | 8 | `logical_length` | nonzero extent length |
| 16 | 2 | `extent_kind` | `1` = DATA, `2` = HOLE, `3` = FILL, v2 also permits `4` = DATA_SLICE |
| 18 | 2 | reserved | zero |
| 20 | 4 | `chunk_length` | full DATA/DATA_SLICE Chunk length; otherwise zero |
| 24 | 32 | `chunk_id` | DATA/DATA_SLICE Chunk ID; otherwise zero |
| 56 | 4 | `chunk_offset` / `fill_byte` | v2 DATA_SLICE offset; FILL uses byte 56 only; otherwise zero |
| 60 | 4 | reserved | zero |

The field widths sum to 64 bytes. Entries are ordered by logical offset and
must form an exact, gap-free, non-overlapping partition of the leaf-local
`[0, file_length)` range. A parent supplies the leaf's position in its own
range; moving or reusing an immutable subtree therefore does not change IDs
inside that subtree.
The first offset is zero, each later offset is the checked end of its
predecessor, and the final checked end equals `file_length`. An empty file has
zero extents; a nonempty file has at least one extent. Zero-length extents are
invalid.

DATA requires `chunk_length == logical_length`, a zero `fill_byte`, and a
logical length no greater than 262,144 bytes (256 KiB). HOLE requires a zero
Chunk ID, zero `chunk_length`, and zero `fill_byte`. FILL requires a zero Chunk
ID and zero `chunk_length`; all 256 fill-byte values, including zero, are valid.
FILL remains allocated logical data and is not a sparse HOLE.

DATA_SLICE is valid only in version 2. It stores the full immutable Chunk's
`chunk_id` and `chunk_length`, a `chunk_offset` at bytes `56..60`, and a
node-local `logical_length`. Writer, recovery, demand reader, and scrub require
checked `chunk_offset + logical_length <= chunk_length <= 262,144`; bytes
`60..64` remain zero. Dependency verification and physical lookup use the full
`chunk_length`, while allocation and file partitioning use `logical_length`.
The complete Chunk is authenticated before a reader returns its selected
slice. A leaf without DATA_SLICE is encoded as v1; a leaf containing any
DATA_SLICE is encoded as v2.

The 16-MiB envelope bound permits at most 262,079 extent entries in one leaf.
The implemented tree publisher uses substantially smaller leaves: it closes a
leaf at a stable 64-MiB logical window boundary or 1,024 extents, whichever
comes first after a complete extent. Tree edits may split DATA into DATA_SLICE
metadata records without splitting or rewriting the immutable Chunk itself.

## Manifest Inner Node v1 and v2 payload

A Manifest Inner Node is Metadata Object kind `4`. Its children are immutable
Metadata Object IDs and form an exact ordered partition of the node-local
logical range. Level `0` is reserved for leaves; a level-`1` node names leaves,
and every higher node names only nodes at exactly one lower level.

The payload equation is `64 + child_count * 64`. Its 64-byte header is:

| Offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDMANI01` |
| 8 | 2 | `format_version` | `1` or `2` |
| 10 | 2 | `header_length` | `64` |
| 12 | 2 | `child_record_length` | `64` |
| 14 | 2 | `level` | nonzero |
| 16 | 8 | required-flags slot | zero |
| 24 | 8 | `file_length` | nonzero node-local logical length |
| 32 | 4 | `child_count` | nonzero number of child records |
| 36 | 4 | `payload_length` | exact header-plus-record length |
| 40 | 24 | reserved | zero |

Each 64-byte child record contains `logical_offset` at `0..8`, nonzero
`logical_length` at `8..16`, and the child Metadata Object ID at `16..48`.
Version 1 requires `48..64` to be zero. Version 2 stores the child's
`allocated_bytes` at `48..56` and keeps `56..64` zero. The allocation total is
no greater than `logical_length`; it counts DATA and FILL and excludes HOLE.
Records start at offset zero, are strictly ordered, and cover
`[0, file_length)` without gaps or overlap. Count and payload equations are
proven before allocation.

New trees and every rewritten ancestor use version 2. Version 1 remains
readable, but its absent allocation summaries cannot authorize the optimized
truncate/splice path. A v2 parent may name only a child whose exact allocation
total was established by the writer. Recovery and metadata scrub completely
traverse the selected graph and require each authenticated v2 child total to
equal the verified child subtree; a mismatch is corruption.

The publisher uses a maximum fanout of 1,024 and a maximum level of 16. It
publishes unique children before parents and synchronizes their directory names
as one batch. Content-identical leaves are published once even when referenced
at several logical positions. Installed demand reads and allocation queries
descend only intersecting paths and may consume a fully covered v2 child's
summary without decoding its descendants. Complete recovery and scrub do not
take that shortcut. Append, equal-length replacement, and truncate retain
untouched subtree identities and rewrite only their affected frontier.
Length-changing middle splice replaces one predecessor-coordinate range with a
canonical extent sequence of arbitrary length. Empty-old means insertion;
empty-new means deletion. The writer checks
`new_file_length = old_file_length - old_range_length + new_extent_length`,
derives allocation from v2 summaries plus the replacement, and retains complete
prefix and shifted-suffix child IDs. HOLE/FILL may split at either boundary;
DATA may not.

## Namespace Root v1, v2, v3, and v4 payload

A Namespace Root is Metadata Object kind `2`. Both versions embed inode
versions and directory entries in one bounded object. Version 1 is the legacy
flat form with only the implicit root Inode ID `1`; every v1 inode record is a
regular file. Version 2 adds directory inode records and nested parent IDs.
Version 3 adds root-inode metadata, per-inode immutable flags, and bounded
inline extended attributes including POSIX ACL wire values. Version 4 adds
nanosecond timestamps and byte-exact symbolic-link targets. Writers emit v3
when v4 state is absent and v4 otherwise; readers retain v1 and v2 support. This remains a pre-stable, bounded format
rather than the final scalable namespace tree.

The payload layout is:

```text
[128-byte header]
[inode_count * 96-byte Durable Inode records]
[entry_count variable Namespace Entry records]
[xattr_count variable Xattr records, v3/v4 only]
[inode_count + 1 variable POSIX metadata records, v4 only]
```

There are no bytes after the final Xattr record. A v1 or v2 payload ends after
its final Namespace Entry. An empty v3 namespace without root attributes
consists of the 128-byte payload header alone.

### Namespace Root header

| Offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDNSRT01` |
| 8 | 2 | `format_version` | `1`, `2`, `3`, or `4`; writers emit `3` or `4` |
| 10 | 2 | `header_length` | `128` |
| 12 | 2 | `inode_record_length` | `96` |
| 14 | 2 | `entry_header_length` | `24` |
| 16 | 2 | `root_mode` | v3/v4: root permission and special bits; v1/v2: zero |
| 18 | 2 | reserved | zero |
| 20 | 4 | `root_uid` | v3/v4: root owner UID; v1/v2: zero |
| 24 | 4 | `root_gid` | v3/v4: root owner GID; v1/v2: zero |
| 28 | 4 | `root_file_flags` | v3/v4: zero or `FS_IMMUTABLE_FL` (`0x10`); v1/v2: zero |
| 32 | 8 | `root_inode` | `1` |
| 40 | 8 | `inode_reservation_end` | at least `2`, and greater than every inode record ID |
| 48 | 8 | `namespace_mutation_sequence` | committed namespace mutation cutoff |
| 56 | 4 | `inode_count` | number of Durable Inode records |
| 60 | 4 | `entry_count` | number of Namespace Entry records |
| 64 | 8 | `inodes_offset` | `128` |
| 72 | 8 | `entries_offset` | `128 + inode_count * 96` |
| 80 | 8 | `payload_length` | exact payload length through the final entry |
| 88 | 8 | `inode_allocation_cursor` | at least `2`, no greater than `inode_reservation_end`, and greater than every inode record ID |
| 96 | 4 | `xattr_count` | v3/v4: total root-plus-inode Xattr records; v1/v2: zero |
| 100 | 2 | `xattr_record_header_length` | v3/v4: `24`; v1/v2: zero |
| 102 | 2 | reserved | zero |
| 104 | 8 | `xattrs_offset` | v3/v4: exact end of Namespace Entry records; v1/v2: zero |
| 112 | 4 | `posix_metadata_count` | v4: `inode_count + 1`; older versions: zero |
| 116 | 2 | `posix_metadata_header_length` | v4: `64`; older versions: zero |
| 118 | 2 | reserved | zero |
| 120 | 8 | `posix_metadata_offset` | v4: exact end of Xattr records; older versions: zero |

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

| Relative offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `inode_id` | greater than `1` |
| 8 | 2 | `mode` | stored POSIX mode bits |
| 10 | 2 | `inode_kind` | v1: zero and implicitly regular; v2/v3: `1` = regular, `2` = directory; v4 also permits `3` = symlink |
| 12 | 4 | `uid` | stored owner UID |
| 16 | 4 | `gid` | stored owner GID |
| 20 | 4 | `link_count` | regular/symlink: incoming names; directory: `2 + immediate child directories` |
| 24 | 8 | `mutation_sequence` | committed per-inode mutation sequence |
| 32 | 8 | `logical_size` | regular: exact Manifest length; symlink: target length; directory: zero |
| 40 | 32 | `manifest_root` | regular: nonzero Metadata Object ID; directory/symlink: zero |
| 72 | 4 | `file_flags` | v3/v4: zero or `FS_IMMUTABLE_FL` (`0x10`); v1/v2: zero |
| 76 | 20 | reserved | zero |

The field widths sum to 96 bytes. Records are strictly increasing by numeric
`inode_id`; duplicate or reordered records are invalid. No record is written
for root Inode ID `1`, a handle, a FUSE lookup reference, or an Open Orphan.

### Namespace Entry record

| Relative offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 4 | `record_length` | `align_up(24 + name_length, 8)` |
| 4 | 2 | `name_length` | `1..=255` |
| 6 | 2 | reserved | zero |
| 8 | 8 | `parent_inode` | v1: `1`; v2: `1` or a reachable directory inode |
| 16 | 8 | `target_inode` | greater than `1` and present in the inode table |
| 24 | variable | `name` | exact component bytes |
| after name | variable | padding | zero through `record_length` |

The fixed field widths sum to 24 bytes. Names are case-sensitive byte strings;
they need not be UTF-8 and undergo no Unicode normalization. Empty names, NUL,
slash, `.` and `..` are invalid.

Entries are strictly ordered by `(parent_inode, unsigned-bytewise name)` and
duplicate names under one parent are invalid. Every target must exist. A
regular inode's incoming entry count equals its `link_count`. Each non-root
directory has exactly one incoming parent entry, and its `link_count` equals
two plus its immediate child-directory count. Full traversal from root must
reach every inode exactly once except that several names may reach one regular
inode. Recovery and scrub reject dangling parents, non-directory parents,
cycles, disconnected subtrees, and incorrect link counts.

### Xattr record (v3)

| Relative offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 4 | `record_length` | `align_up(24 + name_length + value_length, 8)` |
| 4 | 2 | `name_length` | `1..=255` |
| 6 | 2 | reserved | zero |
| 8 | 8 | `inode_id` | root `1` or an inode present in the inode table |
| 16 | 4 | `value_length` | `0..=65,536` |
| 20 | 4 | reserved | zero |
| 24 | variable | `name` | exact non-NUL Xattr name bytes |
| after name | variable | `value` | exact Xattr value bytes |
| after value | variable | padding | zero through `record_length` |

Records are strictly ordered by `(inode_id, unsigned-bytewise name)`. Names are
limited to `user.*`, `trusted.*`, `security.*`,
`system.posix_acl_access`, and `system.posix_acl_default`; default ACLs are
valid only on directories. Each inode has at most 1,024 attributes and at most
1,048,576 aggregate name-plus-value bytes. POSIX ACL values use Linux xattr
version 2 and canonical entry order: owner, increasing named users, owning
group, increasing named groups, optional mask, and other. Named entries require
a mask. Writer, recovery, and offline scrub enforce the same bounds and ACL
grammar.

### POSIX metadata record (v4)

Each inode, including the implicit root first, has one record ordered by inode.
The 64-byte header stores record length, optional symlink-target length and
flag, inode ID, and atime/mtime/ctime as signed seconds plus nanoseconds and a
zero reserved word. The byte-exact target follows the header and the record is
zero-padded to eight-byte alignment. Non-symlinks have no target; symlink
targets are 1..=4,096 bytes and their Durable Inode kind is `3`.

## Commit Record v1/v2

The Commit WAL is a byte concatenation of fixed 4,096-byte Commit Records. A
Commit Record is not a Metadata Object and has no envelope or filename-derived
identity.

| Offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDCMIT01` |
| 8 | 2 | `format_version` | `1` for epoch 0; `2` for a nonzero epoch |
| 10 | 2 | `record_type` | `1` = Namespace commit |
| 12 | 2 | `header_length` | `176` |
| 14 | 2 | `record_hash_algorithm` | `1` = unkeyed BLAKE3-256 |
| 16 | 4 | `record_length` | `4,096` |
| 20 | 2 | `checksum_algorithm` | `1` = CRC-32C |
| 22 | 2 | `repository_format_epoch` | zero in v1; nonzero in v2 |
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
a nonzero predecessor hash. Within one Commit Log slot, every record after the
first has generation exactly one greater than its predecessor and its predecessor
hash equals BLAKE3-256 of that immediately preceding exact record. The first
record may be a bridge copied byte-for-byte from the other slot. Namespace
mutation cutoff, inode
reservation end, and inode allocation cursor must each be monotone
nondecreasing across the structurally valid record chain. A Commit Record
contains no mutable root pointer and no separately stored self-hash.

Repository Format Epoch is monotonic across the retained Commit chain. The
current reader accepts epochs zero and one; the current writer emits only epoch
one. Append, recovery, and Scrub reject an unsupported or decreasing epoch
before graph fallback. A writable upgrade appends and syncs its epoch-one v2
Commit before publishing any state whose interpretation depends on that epoch.

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
- paired Commit Log slots: `commit.wal` and `commit.1.wal`

Metadata publication looks up these exact ASCII names. Recovery follows IDs
referenced from Commit Records and Namespace Roots; it does not currently scan
or reject unrelated or malformed unreferenced `.fdm` names. Temporary and
unreferenced valid objects are not selected by a Commit Record and remain
invisible metadata orphans.

## Writer and publication protocol

Only one external writer may use a repository. Every writable daemon and
offline maintenance process acquires the exclusive kernel-backed Appliance
Lease before recovery or repository mutation and retains it for its complete
lifetime, as required by ADR 0069.

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
   against the supplied durable Container Repository before proceeding. The
   `_using` variant accepts a complete `RequiredChunkVerifier`; the implemented
   indexed verifier pairs every positive candidate with bounded immutable
   Container reads and falls back to one complete verified scan on any miss or
   unusable candidate.
   The Appliance's normal in-process successor adapter may satisfy this
   complete-proof interface compositionally: dependencies in byte-identical
   prefix/suffix Manifest extents retain the immediately preceding installed
   generation's complete proof, while every dependency in the changed middle
   is passed to the ordinary verifier. The adapter asserts that this delta is a
   subset of the independently reread proposed graph. It is not used by
   recovery or offline verification.
2. Encode and publish the complete Namespace Root using the immutable-object
   protocol above.
3. Ensure both fixed Commit Log names exist. Initial creation sets each length
   to zero and synchronizes each file, then synchronizes the containing
   directory. Retry synchronizes the directory again before relying on either
   live name.
4. Read both slots, validate their bounded prefixes, and select one unambiguous
   current chain by the exact bridge-overlap rule in ADR 0039. Require the
   selected tail to be clean.
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
8. Below 64 records, append to the selected slot. At 64 records, rewrite only
   the inactive slot with the exact selected final record as bridge followed by
   the new record. Set the exact intended length.
9. Re-read the target slot and require the complete intended bytes and chain.
10. Synchronize only the target slot. This final file sync is the sole commit
    point for both ordinary append and rotation.

For every noninitial transition, namespace mutation sequence, reservation end,
allocation cursor, and every retained inode's mutation sequence are monotone
nondecreasing. The proposed allocation cursor may not exceed the *previously*
durable reservation end, so extending a range and consuming it in the same
generation is forbidden. A newly appearing inode below the previous allocation
cursor is rejected as ID reuse. Deleting an inode is permitted, but these rules
prevent it from later reappearing under the consumed ID.

Each ordinary slot is capped at 64 Commit Records. `commit.wal` may exceed this
only while importing a clean legacy v1 chain and remains bounded by 64 MiB.
Rotation retains one bridge plus at most 63 newer records. A dirty selected tail
still blocks further commits with `WalNeedsRepair`.

The normal ordering required by ADR 0019 is data containers, immutable metadata
objects, then the Commit Record. `commit_namespace_with_data` implements that
proof boundary by accepting a DATA graph only after all referenced Chunk IDs
and lengths are found in fully verified, already published containers. The
plain `commit_namespace` intentionally fails closed on such a graph rather than
assuming a location source. With a healthy activated Exact Index, the appliance
uses bounded Run pages, Container envelopes, complete selected Records, decoded
lengths, and BLAKE3 Chunk IDs instead of listing or whole-reading every
Container. A missing, corrupt, stale, or unsupported candidate triggers one
complete verified Container scan; an index negative never proves DATA absence.

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
previously trusted bytes. An activated persistent Exact Index can be pinned
behind the ordinary `read_at` interface. It reads only the paired 4-KiB
Container Footer, 4-KiB Header, and selected at-most-1-MiB Record before
rehashing its complete Chunk. Both read-only recovery and the writable FUSE
appliance pin the newest valid Run Set for the lifetime of installed Manifest
readers. Missing activation, recovery failure, a negative lookup, or unusable
candidates fall back to the verified scan without rolling the Namespace back.
Read-only recovery, writable recovery/reservation, and every later checkpoint
use the same indexed graph verifier. The only remaining allocator directory
scan is the one-time bounded Container-envelope migration when both Generation
High-Water slots are absent. A healthy migrated repository performs no
Container directory listing or whole-Container read for allocator recovery or
indexed graph proof.

## Metadata Mark Catalog v1/v2

The rebuildable catalog name is
`metadata-mark-catalog-<20-digit-generation>.run`; temporary publication uses
`.metadata-mark-catalog-<20-digit-generation>.building`. It is not a Metadata
Object and never participates in reachability.

Each immutable run contains a 4,096-byte header, `row_count` strictly
Object-ID-sorted 32-byte rows, zero padding to 4,096 bytes, and one mirrored
4,096-byte footer. Header magic is `FDMMARK1`; footer magic is `FDMMARKF`.
Both envelopes encode fields explicitly:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | header/footer magic |
| 8 | 2 | format version `1` or `2`; writers emit `2` |
| 10 | 2 | envelope bytes `4096` |
| 12 | 2 | row bytes `32` |
| 14 | 2 | v1: reserved zero; v2: run kind (`1` Snapshot, `2` Addition) |
| 16 | 8 | nonzero catalog generation |
| 24 | 32 | domain-separated BLAKE3 binding over the selected Commit Record bytes |
| 56 | 8 | row count |
| 64 | 8 | row offset `4096` |
| 72 | 8 | aligned footer offset |
| 80 | 8 | exact file length |
| 88 | 32 | domain-separated BLAKE3 row-stream hash |
| 120 | 8 | v1: reserved zero; v2: base generation (`0` for Snapshot, immediate predecessor for Addition) |
| 128 | 32 | domain-separated BLAKE3 envelope hash with this field zeroed |
| 160 | 3936 | reserved zero |

The writer streams rows in 256-KiB batches, rereads and audits the complete
temporary run, syncs it, and publishes without replacement. A Snapshot contains
one complete exact mark. An Addition contains only newly published Metadata
Objects classified by a successful proof-bearing, nonrotating Commit and names
the immediately preceding catalog generation. Addition rows are sorted exact
identities, but the run is acceleration only and cannot authorize unlink.

The frontend Commit path journals these identities in memory and performs no
catalog file I/O. A later maintenance quantum publishes and directory-syncs the
Addition. Unclassified publication, unpublished-pin drain, uncertain Commit
durability, Commit-WAL rotation, process restart, a broken chain, or 32
consecutive Addition runs forces an exact Snapshot. Exact GC publishes a higher
generation, removes all older catalog names and verified `.fdm` garbage, then
performs one directory sync. Scrub validates every published run and requires
each Addition to extend the immediately preceding published generation; a later
Snapshot starts a new valid chain. Normal collection may discard a corrupt old
run because only the exact Commit/Pin graph can authorize deletion.

Proof-bearing complete, append, replacement, truncate, and splice writers carry
every newly published Manifest-node identity into this journal. Releasing a pin
for a root still named by the durable predecessor does not create a liveness
contraction; releasing an unpublished intermediate root does and therefore
forces an exact Snapshot.

## Crash outcomes

- A crash before Metadata Object file synchronization may leave incomplete or
  apparently complete temporary bytes. No Commit Record can name them through
  the published object name.
- A crash after object synchronization but before or during the no-replace
  rename may leave either the temporary object or the published object. A later
  Commit Record still has not been synchronized.
- A crash after directory synchronization but before WAL publication leaves a
  verified published metadata orphan.
- A crash during an ordinary 4-KiB append or inactive-slot rotation may leave
  the prior selected slot, a short replacement, an invalid record, or a complete
  successor. Recovery accepts a successor only after exact bridge continuity
  and independently validates its referenced graph.
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

1. Read both canonical Commit Log slots. Two missing or empty slots mean no
   generation. Reject `commit.1.wal` above 256 KiB and `commit.wal` above the
   64-MiB legacy migration bound.
2. Divide each slot into complete 4,096-byte records and a possible short tail.
   Decode from offset zero, requiring internal generation/hash continuity and
   monotone fields. Classify remaining bytes as `Clean`, `Torn`,
   `InvalidRecord`, or `BrokenChain`. A nonempty slot with no valid first record
   fails closed. Select the higher head only when its first exact record equals
   the other slot's final exact record; conflicting heads are corruption.
3. Compute the maximum `inode_reservation_end` over the structurally valid WAL
   prefix. This remains the allocator high-water mark even if graph verification
   later selects an older generation.
4. Check the Repository Format Epoch and Policy Set ID of every record in the
   valid WAL prefix. Refuse recovery immediately on an unsupported or decreasing
   epoch or on any Policy Set different from the supported one; do not hide a
   newer writer contract by rolling back.
5. Build the Recovery Transition Prefix **forward**, oldest to newest. For each
   record, read `<namespace_root>.fdm`; enforce the 16-MiB bound, generic
   envelope identity, kind `2`, complete Namespace Root payload invariants,
   link counts, reservation bound, and allocation cursor bound.
6. Require the root's namespace mutation sequence, reservation end, and
   allocation cursor to equal all three values copied into the Commit Record.
   Verify the same monotone namespace, reservation, allocation, inode-mutation,
   and never-reuse transition rules as the writer. Generation 1 must have
   allocation cursor `2` and no visible inode. A missing or corrupt root or an
   invalid transition ends this prefix because no later transition can be
   proven.
7. From that transition prefix, consider only the latest WAL generation and its
   immediate predecessor, the two generations pinned for atomic recovery.
   Traverse the available candidates **backward**, newest first. Older WAL
   history is diagnostic and must not become an implicit snapshot.
8. For the current candidate, traverse every `<manifest_root>.fdm`; validate
   filename identity, generic envelope, leaf/inner kind, levels, child ranges,
   Manifest partitions, and equality of root `file_length` with inode
   `logical_size`. Collect every DATA Chunk ID and exact length.
   `recover_latest_with_data` verifies them
   against the supplied Container Repository. The `_using` recovery path may
   instead use a complete indexed verifier with verified scan fallback; plain
   `recover_latest` fails closed with `DataLocationsNotConnected` if any DATA
   dependency is present.
9. Select the first candidate whose complete Manifest and DATA graph verifies.
   An explicitly classified missing or corrupt graph may fall back atomically
   to the immediately previous candidate. Transient I/O and unsupported
   capabilities abort recovery. Never merge a Namespace Root, inode, Manifest,
   or DATA dependency from different generations.
10. Report the observed WAL tail, the number of valid WAL generations newer
    than the selected record, and the prefix reservation high-water mark. If
    neither live candidate has a complete accepted graph, return
    `NoRecoverableGeneration` rather than exposing a partial namespace or older
    unpinned history.

Missing objects, format/identity damage, length disagreement, or missing
verified Chunks permit atomic fallback from the current graph to its immediate
predecessor. Structural root or transition damage truncates the Recovery
Transition Prefix instead. Unknown Policy Sets, transient I/O, and using the
plain recovery method for a DATA-bearing graph are refused rather than
classified as corruption. Recovery does not truncate or repair a dirty selected
slot; because commits require a clean tail, a separate future repair protocol
is required before that repository can advance again.

## Paired invariants and implemented evidence

| Invariant | Writer boundary | Reader/recovery boundary | Fault evidence |
| --- | --- | --- | --- |
| Object ID identifies exact kind and payload | derive after canonical payload encoding; re-read exact bytes before sync | recompute domain-separated ID and pair it with every referenced filename | substituted or mutated valid-looking bytes fail identity/equality checks |
| Manifest is one complete byte-exact recipe | validate leaf extents, inner levels, and full child partitions before child-first publication | repeat envelope, level, child range, extent, length, and partition validation | every inner-node prefix and single-byte mutation is rejected without panic; one changed leaf creates only that leaf and its new root path |
| Namespace has no dangling or reused inode state | canonicalize, validate target existence, cross-check every link count, and bound every ID below the allocation cursor | reject reordered/duplicate IDs and names, dangling targets, orphans, bad names, decreasing cursors, or IDs reused below a prior cursor | reauthenticated order/count corruption, invalid transitions, and exhaustive truncation/byte corruption are rejected |
| Inode reservation precedes visibility | generation 1 is reservation-only; later allocation cannot cross the preceding record's durable reservation end | validate forward transitions and retain the structurally valid WAL reservation high-water across graph fallback | premature use of a newly extended range and reuse of a removed inode both fail |
| Commit Record is atomic visibility | publish and sync dependencies before append or rotation; re-read exact target; sync its slot last | accept only internally valid slots with exact cross-slot bridge continuity, validate transitions forward, then prove live graph candidates backward | exhaustive fail-before/fail-after rotation recovers only the previous or complete next generation; a 16,400-Commit lifetime gate crosses the old cap |
| DATA is durable before visibility and verified before reads | initial/recovered graphs verify every referenced ID and length; a serialized successor composes unchanged predecessor proof with complete changed-dependency verification before WAL append | recovery completely verifies the selected current/previous candidate; demand reads re-verify the containing container; any unusable index candidate invokes the complete scan | healthy recovery proves only the newest graph, missing newest DATA falls back atomically once, unpinned history is never exposed, index-page corruption takes the scan path, suffix-proof work is independent of preserved-prefix size, and corruption after file construction fails demand reads |
| Length-changing splice is one immutable successor | check predecessor range/result-length/allocation equations; encode partial DATA as bounded v2 DATA_SLICE extents; publish replacement leaves and rewritten parents child-first; append WAL last | completely traverse the selected root, recompute every partition and v2 allocation total, verify full-Chunk identity/length/offset for every slice, and verify every replacement DATA dependency | insertion, cross-child deletion, shifted-suffix identity, DATA-slice boundaries, and exhaustive fail-before/fail-after recover only the predecessor or complete successor |
| Metadata GC never removes a live object | mark every selected Commit graph and every live Metadata Root Pin while holding the publication/commit barriers; only classify proof-bearing nonrotating commits as additive hints; verify every candidate before unlink; publish the audited mark catalog and sync the combined directory transition last | recovery and scrub traverse the same retained Commit graphs; live readers retain their root pins independently; scrub audits snapshot/addition chains without treating them as roots | an inflight publication blocks collection, a reader survives WAL rotation and collection, every snapshot/delta publication fault retries, rotation and unpublished-pin drain force exact proof, and fail-before/fail-after deletion always leaves a scrub-valid graph |
| Counts cannot select unbounded allocations | preflight payload lengths and record equations | prove counts against the bounded payload before vector allocation | `entry_count = u32::MAX` fails as invalid payload under a constrained address space |
| Policy selection is explicit | store the configured nonzero Policy Set ID in every record | refuse any reached record whose ID is not exactly supported | an unknown newer policy refuses recovery instead of silently rolling back |
| Writer compatibility is monotonic | write epoch one in Commit Record v2 before epoch-dependent publication | validate every retained epoch before graph fallback and reject unsupported or decreasing values | fail-before/fail-after upgrade recovers only epoch zero or the complete epoch-one fence; a legacy-only writer cannot append beyond it |

## Explicit limitations

This implemented checkpoint intentionally does **not** provide:

- reconstruction of process-local Metadata Root Pins after restart: immutable
  Snapshot and Addition runs suppress unchanged and safely additive online
  cycles, but the first cycle after process start deliberately rebuilds the
  exact mark because those unpublished pins ended with the process;
- a scalable Namespace tree: NamespaceRoot v4 rewrites one bounded object and
  keeps xattrs, ACLs, timestamps, and symlink targets inline; external
  large-metadata objects remain future work;
- automatic dirty-tail repair;
- serialization or transitive verification of the Policy Set itself;
- data-tier Recovery Checkpoints or metadata-tier-loss rebuild.

These omissions are stage boundaries, not implicit format promises. Unknown
future kinds, versions, flags, tree nodes, and record types require explicit
versioned readers and crash-safe transitions.
