# Container format v1

Status: draft, pre-`format-v1-stable`

This document specifies the first durable fastdup data container. It makes the
accepted container, logical-chunk, exact-index, durability, and integrity ADRs
concrete for Stage 1 and the first adaptive-compression checkpoint. Version 1
assigns independent RAW records and dependency-free Zstd level-3 Compression
Regions containing one or more complete logical chunks. Dictionaries, prefix
encoding, and delta encoding remain unassigned.

Canonical filesystem naming and startup discovery are defined separately in
[`container-store-v1.md`](container-store-v1.md).

All offsets below are relative to the first byte of the container. Half-open
ranges are written `[start, end)`. Implementations must use checked integer
arithmetic before allocating memory, slicing a buffer, seeking, or adding an
offset and a length.

## Constants and primitive encoding

| Name | Value |
| --- | ---: |
| `FORMAT_VERSION` | `1` |
| `HEADER_BYTES` | `4,096` |
| `FOOTER_BYTES` | `4,096` |
| `RECORD_HEADER_BYTES` | `128` |
| `CHUNK_TABLE_ENTRY_BYTES` | `64` |
| `INDEX_HEADER_BYTES` | `64` |
| `INDEX_ENTRY_BYTES` | `128` |
| `RECORD_ALIGNMENT` | `64` |
| `MAX_CONTAINER_BYTES` | `67,108,864` (64 MiB) |
| `MAX_RECORD_BYTES` | `1,048,576` (1 MiB), including header, table, payload, and padding |
| `MAX_DECODED_RECORD_BYTES` | `524,288` (512 KiB) |
| `MAX_LOGICAL_CHUNK_BYTES` | `262,144` (256 KiB) |

Every multibyte integer is unsigned and little-endian. Byte arrays have no byte
order. There are no native-width integers, Rust enums, implicit padding, or
serialized Rust layouts on disk.

`align_up(value, alignment)` is the least multiple of `alignment` greater than
or equal to `value`; it is valid only after a checked-add implementation has
proved that it cannot overflow.

The algorithms used by version 1 are:

- CRC-32C (Castagnoli), with the conventional reflected initialization/final
  XOR and check value `0xe3069283` for ASCII `123456789`. The stored `u32` is
  little-endian.
- Unkeyed BLAKE3 with its default 32-byte output. No context string, keyed mode,
  or truncated digest is used.

All reserved fields and all alignment padding are zero when written and must be
zero when read. A v1 reader rejects an unknown format version, unknown required
flag, nonzero reserved field, nonzero required padding, or unknown codec. There
are no defined required or compatible flags in this revision, so conforming v1
writers emit both flag words as zero.

## File layout

```text
0                                                                    4096
+----------------------------- container header ------------------------+
| encoding record 0 | encoding record 1 | ... | encoding record N-1     |
+------------------------------ recovery index --------------------------+
| zero padding to a 4096-byte boundary                                   |
+----------------------------- container footer ------------------------+
```

The first Encoding Record begins at offset `HEADER_BYTES`. Records are packed
without gaps, and every record length is a multiple of `RECORD_ALIGNMENT`.
Consequently every record begins at a 64-byte-aligned offset. The Recovery Index
begins immediately after the final record. Only the gap between the end of the
Recovery Index and the footer may contain layout padding, and every byte of that
gap is zero.

For a sealed container the following equations hold exactly:

```text
index_offset  = HEADER_BYTES + sum(record_length[0..record_count])
index_length  = INDEX_HEADER_BYTES + chunk_entry_count * INDEX_ENTRY_BYTES
footer_offset = align_up(index_offset + index_length, FOOTER_BYTES)
file_length   = footer_offset + FOOTER_BYTES
```

`file_length` is the actual file length and is at most `MAX_CONTAINER_BYTES`.
A sealed v1 container has at least one record and at least one chunk-table entry.

## Container header

The header occupies exactly `[0, 4096)`.

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII bytes `FDCTNR01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `4,096` |
| 12 | 2 | `lifecycle_state` | `1` = `BUILDING`, `2` = `SEALED`; published files contain `2` |
| 14 | 2 | `crc_algorithm` | `1` = CRC-32C |
| 16 | 2 | `chunk_id_algorithm` | `1` = unkeyed BLAKE3-256 |
| 18 | 2 | `container_hash_algorithm` | `1` = unkeyed BLAKE3-256 |
| 20 | 2 | `record_alignment` | `64` |
| 22 | 2 | reserved | zero |
| 24 | 8 | `required_flags` | zero |
| 32 | 8 | `compatible_flags` | zero from v1 writers |
| 40 | 16 | `container_id` | random, nonzero 128-bit Container ID |
| 56 | 8 | `container_generation` | nonzero monotonic appliance-local generation |
| 64 | 4 | `record_count` | number of Encoding Records |
| 68 | 4 | `chunk_entry_count` | sum of every record's `chunk_count` |
| 72 | 8 | `index_offset` | exact offset of the Recovery Index |
| 80 | 8 | `index_length` | exact header-plus-entries length, excluding footer padding |
| 88 | 8 | `footer_offset` | exact offset of the footer |
| 96 | 8 | `file_length` | exact total file length |
| 104 | 4 | `header_crc32c` | header checksum described below |
| 108 | 3,988 | reserved | zero |

The header CRC covers all 4,096 header bytes with bytes `[104, 108)` treated as
zero. A `BUILDING` header may have zero counts and offsets while construction is
in progress, but it is never accepted by a published-container reader, Recovery
Index rebuild, or scrub as a sealed container.

## Encoding Record

Every record starts with this fixed 128-byte header.

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII bytes `FDRECD01` |
| 8 | 2 | `record_version` | `1` |
| 10 | 2 | `header_length` | `128` |
| 12 | 2 | `codec_id` | `1` = RAW, `2` = dependency-free Zstd |
| 14 | 2 | `dependency_kind` | `0` = none |
| 16 | 8 | `required_flags` | zero |
| 24 | 8 | `compatible_flags` | zero from v1 writers |
| 32 | 4 | `record_length` | complete padded record length |
| 36 | 4 | `decoded_length` | total decoded region length |
| 40 | 4 | `payload_offset` | offset of encoded payload relative to record start |
| 44 | 4 | `payload_length` | stored payload bytes, excluding padding |
| 48 | 4 | `chunk_table_offset` | `128` |
| 52 | 2 | `chunk_entry_size` | `64` |
| 54 | 2 | reserved | zero |
| 56 | 4 | `chunk_count` | `1` for RAW; nonzero for Zstd |
| 60 | 4 | `record_crc32c` | record checksum described below |
| 64 | 32 | `dependency_id` | zero for `dependency_kind = none` |
| 96 | 16 | `codec_parameters` | all zero for RAW; signed little-endian `i32` level `3` followed by twelve zero bytes for Zstd |
| 112 | 16 | reserved | zero |

The Chunk Table begins immediately after the record header. Each fixed 64-byte
entry has this layout:

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 32 | `chunk_id` | BLAKE3-256 of the decoded logical chunk bytes |
| 32 | 4 | `decoded_offset` | start of the chunk in the decoded region |
| 36 | 4 | `logical_length` | decoded logical chunk length |
| 40 | 8 | `required_flags` | zero |
| 48 | 8 | `compatible_flags` | zero from v1 writers |
| 56 | 8 | reserved | zero |

General structural validation treats Chunk Table entries as an ordered,
gap-free, non-overlapping partition of `[0, decoded_length)`: the first
`decoded_offset` is zero, each later offset equals the checked end of the prior
entry, every length is nonzero and at most `MAX_LOGICAL_CHUNK_BYTES`, and the
final checked end equals `decoded_length`. No logical chunk spans records.

For RAW, all of these stronger rules also apply:

```text
chunk_count       = 1
chunk_table_offset = 128
payload_offset     = 128 + 64
payload_length     = decoded_length = logical_length
decoded_offset     = 0
1 <= decoded_length <= MAX_LOGICAL_CHUNK_BYTES
record_length      = align_up(payload_offset + payload_length, 64)
```

The RAW payload is the logical chunk byte-for-byte. Bytes from the checked end of
the payload to `record_length` are zero padding. The complete `record_length`,
including this padding, is at most `MAX_RECORD_BYTES`.

For codec `2`, the Chunk Table partitions a decoded region of at most 512 KiB.
`payload_offset` is exactly `128 + chunk_count * 64`; the payload is one complete
dependency-free Zstd frame encoded at level 3. Decompression must finish at
exactly `decoded_length`, after which every table slice is independently hashed
and compared with its stored Chunk ID. The writer groups only adjacent complete
chunks and never permits one chunk to span records. The complete padded record
remains bounded by `MAX_RECORD_BYTES`.

The adaptive v1 writer compares one Zstd record with the complete set of RAW
records for the same chunks, including record headers, Chunk Tables, and record
alignment. Zstd is selected only when it saves at least 4,096 bytes and at least
3%; otherwise each chunk remains one RAW record. Recovery-Index cost is equal
per logical chunk in both alternatives.

Independent regions may be encoded by permanent-pool workers. Each worker owns disjoint
region ordinals and private output buffers; the writer merges completed records
strictly by original ordinal before computing Container layout, Recovery Index,
Footer hash, or CRCs. Worker count and completion order therefore do not affect
the encoded Container bytes.

The Record CRC covers all `record_length` bytes, including the header, Chunk
Table, stored payload, and zero padding, with bytes `[60, 64)` of the record
treated as zero. The Chunk ID is a separate end-to-end check over decoded bytes;
a passing Record CRC never substitutes for the Chunk-ID check.

The dependency field remains reserved: both codecs require dependency kind zero
and a zero dependency ID. A v1 parser rejects unknown codec IDs, unknown codec
parameters, and every nonzero required flag or reserved byte.

## Recovery Index

The index starts at `index_offset`. Its 64-byte header is:

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII bytes `FDINDX01` |
| 8 | 2 | `index_version` | `1` |
| 10 | 2 | `header_length` | `64` |
| 12 | 2 | `entry_length` | `128` |
| 14 | 2 | `sort_order` | `1` = tuple order defined below |
| 16 | 8 | `required_flags` | zero |
| 24 | 8 | `compatible_flags` | zero from v1 writers |
| 32 | 4 | `entry_count` | equals header `chunk_entry_count` |
| 36 | 4 | `index_crc32c` | index checksum described below |
| 40 | 24 | reserved | zero |

Each 128-byte entry maps exactly one Chunk Table entry to its physical record:

| Relative offset | Width | Field | Required cross-check |
| ---: | ---: | --- | --- |
| 0 | 32 | `chunk_id` | equals Chunk Table `chunk_id` |
| 32 | 4 | `logical_length` | equals Chunk Table `logical_length` |
| 36 | 4 | `decoded_offset` | equals Chunk Table `decoded_offset` |
| 40 | 8 | `record_offset` | exact 64-byte-aligned record start |
| 48 | 4 | `record_length` | equals Record Header value |
| 52 | 4 | `chunk_ordinal` | zero-based position in the Chunk Table |
| 56 | 2 | `codec_id` | equals Record Header value |
| 58 | 2 | `dependency_kind` | equals Record Header value |
| 60 | 4 | `record_crc32c` | equals Record Header value |
| 64 | 32 | `dependency_id` | equals Record Header value |
| 96 | 4 | `record_decoded_length` | equals Record Header value |
| 100 | 4 | `record_payload_length` | equals Record Header value |
| 104 | 8 | `record_required_flags` | equals Record Header value |
| 112 | 8 | `record_compatible_flags` | equals Record Header value |
| 120 | 8 | reserved | zero |

Entries are sorted lexicographically by unsigned bytewise `chunk_id`, then
numeric `logical_length`, `record_offset`, and `chunk_ordinal`. Multiple physical
records for the same Chunk ID and length are legal and describe multiple
Locations. Two entries with the same Chunk ID but different logical lengths are
Corruption. Duplicate entries naming the same `(record_offset, chunk_ordinal)`
are invalid.

The index is a bijection: every Chunk Table entry in every record has exactly one
matching Recovery Index entry and every Recovery Index entry resolves to exactly
one Chunk Table entry. The index CRC covers exactly `index_length` bytes with
bytes `[36, 40)` of the Index Header treated as zero.

## Container footer and whole-container hash

The footer starts at `footer_offset`, occupies exactly 4,096 bytes, and ends at
`file_length`.

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII bytes `FDFOOT01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `footer_length` | `4,096` |
| 12 | 4 | `seal_marker` | ASCII bytes `SEAL` |
| 16 | 8 | `required_flags` | equals Container Header value |
| 24 | 8 | `compatible_flags` | equals Container Header value |
| 32 | 16 | `container_id` | equals Container Header value |
| 48 | 8 | `container_generation` | equals Container Header value |
| 56 | 8 | `file_length` | equals Container Header and actual file length |
| 64 | 4 | `record_count` | equals Container Header value |
| 68 | 4 | `chunk_entry_count` | equals Container Header value |
| 72 | 8 | `index_offset` | equals Container Header value |
| 80 | 8 | `index_length` | equals Container Header value |
| 88 | 8 | `footer_offset` | equals Container Header and actual footer offset |
| 96 | 32 | `container_hash` | whole-container BLAKE3-256 |
| 128 | 4 | `footer_crc32c` | footer checksum described below |
| 132 | 3,964 | reserved | zero |

The whole-container BLAKE3 covers exactly `[0, file_length)`, with the footer's
`container_hash` bytes `[footer_offset + 96, footer_offset + 128)` and
`footer_crc32c` bytes `[footer_offset + 128, footer_offset + 132)` treated as
zero. This avoids a self-reference while protecting the header, all records and
padding, the complete Recovery Index, footer identity fields, and footer
reserved bytes.

The Footer CRC covers all 4,096 footer bytes with only its own bytes
`[128, 132)` treated as zero. It therefore detects damage to the stored
container hash as well as to the duplicated structural fields. Verification
first checks cheap bounds and the Footer CRC, then compares duplicated fields;
full-container verification recomputes BLAKE3 using the zeroing rule above.

## Lifecycle and durability protocol

`BUILDING`, `SEALED`, and `PUBLISHED` are storage lifecycle states, not three
mutable values in an immutable published file. `BUILDING` and `SEALED` are
encoded in the temporary file's header; `PUBLISHED` means that a `SEALED` file
has a durable directory entry in the container-publish namespace.

The writer performs these steps in order:

1. Create a new non-replacing temporary file in the same directory as its final
   name. Assign a fresh random Container ID and the next nonzero Container
   Generation. Write a zero-reserved `BUILDING` header.
2. Write complete Encoding Records. Calculate each Record CRC only after its
   header, table, payload, and zero padding are final.
3. Construct the complete sorted Recovery Index from the records. Verify the
   index-to-record bijection in memory and calculate the Index CRC.
4. Calculate final offsets and length with checked arithmetic. Construct the
   zero gap and footer, compute whole-container BLAKE3 using the defined zeroed
   footer fields, and finalize the Footer CRC. The on-disk header remains
   `BUILDING` during these writes.
5. Write the complete record/index/footer body, then write the final `SEALED`
   header last. Reassert the already known exact file length; on XFS this also
   releases speculative unwritten allocation beyond EOF.
6. Re-read and validate the sealed structure using the reader parser, including
   all Record CRCs, codec completion, the Recovery Index bijection, and every
   decoded Chunk ID. This is
   the writer-side paired verification.
7. `fsync` the container file. Only now is it `SEALED`.
8. Atomically rename it, without replacement, from its temporary name to its
   published name in the same directory, then `fsync` that directory. Only now
   is it `PUBLISHED`.
9. Only after publication may immutable metadata and a later Commit Record
   refer to a Location in this container. Data publication precedes metadata
   synchronization, which precedes Commit-WAL synchronization.

An expected allocation, write, sync, or rename failure aborts publication and
is returned as an operational error; it is not an assertion. A target-name or
Container-ID collision with different bytes is an impossible writer state and a
VERIFY failure during recovery. Implementations must never overwrite the
existing target.

Crash outcomes are deliberately simple:

- A crash before container `fsync` may leave a `BUILDING`, invalid, or apparently
  complete `SEALED` temporary file; recovery ignores it because it is not in the
  published namespace.
- A crash after container `fsync` but before rename leaves a valid sealed orphan;
  it is not published and cannot be referenced by a valid Commit Record.
- A crash during rename or before directory `fsync` may leave either namespace
  outcome. Any visible file must still pass all container validation, and no
  Commit Record may reference it because metadata commit has not begun.
- A crash after directory `fsync` but before metadata commit leaves a valid
  published orphan. It is invisible to the POSIX namespace and may be reclaimed
  later.
- A valid Commit Record may reference only a previously published container, so
  no recovered visible manifest points at undurable container data.

## Reader, recovery, and scrub invariants

A parser validates in this order so corrupt counts cannot cause large reads or
allocations:

1. Obtain actual file length; reject lengths below `HEADER_BYTES + FOOTER_BYTES`,
   above `MAX_CONTAINER_BYTES`, or not divisible by 4,096.
2. Read the fixed footer at `actual_length - FOOTER_BYTES`; validate magic,
   version, fixed length, seal marker, reserved bytes, required flags, and Footer
   CRC before trusting any footer offset or count.
3. Read the fixed header; validate magic, version, `SEALED` state, algorithms,
   alignment, reserved bytes, required flags, and Header CRC.
4. Pair every duplicated header/footer field, require `footer_offset` to equal
   `actual_length - FOOTER_BYTES`, and prove all layout equations with checked
   arithmetic before allocating an index or record vector.
5. Walk exactly `record_count` consecutive records from offset 4,096. Before
   reading a record body, validate its fixed header and prove that its length is
   aligned, at most `MAX_RECORD_BYTES`, and ends no later than `index_offset`.
   Validate record structure, zero padding, Record CRC, codec-specific rules,
   exact decoded length, and Chunk Table partitioning. The walk must end exactly
   at `index_offset`.
6. Validate the Index Header and prove its length equation from `entry_count`.
   Check zero footer padding, Index CRC, sort order, and the complete
   index-to-record bijection without trusting index offsets first.
7. When whole-container verification is required, recompute and compare the
   container BLAKE3. When returning a logical chunk, always compute BLAKE3 over
   its decoded bytes and compare it with both table and index Chunk IDs before
   returning any byte to the caller.

Normal crash recovery may structurally validate published containers and then
verify individual records and decoded chunks on demand. It must not call such a
container wholly verified until its container hash has also been recomputed.
Offline scrub and a rebuild that promotes recovered Locations into a new Exact
Index generation perform full-container verification and verify every decoded
Chunk ID. A rebuild may stage unverified discoveries, but it must not publish
them as verified active Locations.

The required paired checks are:

| Invariant | Writer boundary | Reader/recovery boundary | Offline scrub / fault case |
| --- | --- | --- | --- |
| offsets and lengths do not overflow or escape the file | checked before every write | checked before every allocation/read/seek | mutate each length and boundary bit; expect VERIFY, never panic or OOM |
| only complete sealed files can be published | final parser pass, file `fsync`, rename, directory `fsync` | reject `BUILDING`, bad/missing footer, and unpublished paths | crash after every write/sync/rename; accept only the outcomes listed above |
| Record CRC covers exactly stored bytes | compute after all record bytes are final | recompute before decode | flip header, table, payload, and padding bytes independently |
| Chunk ID identifies exact decoded bytes | compute from input chunk and re-read after encode | recompute after RAW copy or Zstd decode before returning bytes | rehash every decoded slice; a mismatch quarantines that Location |
| Recovery Index is a complete, sorted bijection | build from final record tables and cross-check | resolve and compare every entry with its record/table | delete, duplicate, reorder, or redirect one entry; expect VERIFY |
| header/footer describe the same immutable object | emit duplicate values from one construction state | compare every duplicate after both block CRCs pass | corrupt each copy independently; do not choose one copy as canonical |
| the footer authenticates the complete sealed byte sequence | hash the finalized byte sequence using the zeroing rule | recompute for whole-container verification | flip any byte class, including zero padding and reserved fields |
| one Chunk ID never has two logical lengths | check entries while sorting | reject an in-container mismatch during index validation | cross-container scrub reports the same mismatch as Corruption |

Persistent-integrity failures are `VERIFY` failures: the smallest isolatable
Location is quarantined and unchecked bytes are never returned. Impossible
in-memory writer states use production-active `ASSERT`. Full hash, bijection,
and all-chunk rechecks are `AUDIT` work when redundant with the demanded read;
they are exhaustive in writer tests, deterministic fault injection, rebuild,
and offline scrub.

## Explicitly deferred integration points

This Stage-1 specification intentionally leaves the following decisions open
rather than assigning accidental durable meanings:

- Numeric codec IDs and exact parameters for Zstd-dictionary, Zstd-prefix, and
  Delta; whether a later compatible specification can reuse this record
  envelope or needs a new record/container format version.
- Numeric dependency kinds and the durable representation of Dictionary
  Objects. The single 32-byte dependency field is sufficient for the accepted
  depth-one direction, but its future semantics must be specified with the
  codec rather than inferred by current readers.
- Similarity fingerprints in recovery metadata. They are derived acceleration
  and are not part of the authoritative Stage-1 Recovery Index.
- Allocation and persistence of the appliance-wide Container Generation
  counter, published-directory naming, Pool/Appliance identity binding, and
  duplicate-generation handling. The store/WAL specification must define these;
  this format only records the nonzero ID and generation and permits rebuild to
  find their maximum.
- Stable-storage certification and exact filesystem syscall wrappers. This
  format requires the ordered file and directory synchronization above; the I/O
  layer is responsible for meeting that contract on supported XFS storage.
