# Exact Index Run v1

Status: draft, pre-stable format. The canonical full-run writer/reader, bounded
Header/Footer/page reader, immutable publication repository, Run Set
activation, and bounded active lookup are implemented and corruption/fault
tested; compaction and benchmark gates are not yet complete.

This document defines an immutable sorted-run building block for the persistent
Exact Index. It follows
[ADR 0015](../adr/0015-keep-exact-dedup-correct-without-index-authority.md),
[ADR 0023](../adr/0023-rebuild-indexes-as-new-generations.md), and the proposed
[ADR 0035](../adr/0035-build-the-exact-index-from-immutable-sorted-runs.md).
Containers and committed Manifests remain the durable sources of physical
content and liveness. No result from this format alone authorizes returning
bytes or deleting a Container.

All integers are little-endian. Every reserved byte is zero. Writers serialize
fields explicitly; Rust layout is never the format. Every offset and size is
checked before allocation, arithmetic, or I/O.

## File geometry

An Exact Index Run is:

```text
+----------------------+ offset 0
| 4-KiB Run Header     |
+----------------------+ offset 4,096
| 4-KiB Entry Page 0   |
+----------------------+
| ...                  |
+----------------------+
| 4-KiB Entry Page N-1 |
+----------------------+ footer_offset
| 4-KiB Run Footer     |
+----------------------+ file_length
```

Constants:

| Name | Value |
| --- | ---: |
| `RUN_HEADER_BYTES` | 4,096 |
| `RUN_PAGE_BYTES` | 4,096 |
| `RUN_PAGE_HEADER_BYTES` | 128 |
| `RUN_ENTRY_BYTES` | 128 |
| `RUN_ENTRIES_PER_PAGE` | 31 |
| `RUN_FOOTER_BYTES` | 4,096 |
| `MAX_RUN_BYTES_V1` | 1 GiB |

Every non-final page contains exactly 31 entries. The final page contains 1 to
31 entries. An empty run has no pages and exists only to support deterministic
rebuild/checkpoint machinery; it is never selected for lookup.

```text
page_count   = ceil(entry_count / 31)
pages_offset = 4,096
footer_offset = 4,096 + page_count * 4,096
file_length   = footer_offset + 4,096
```

The equations must hold exactly and `file_length <= MAX_RUN_BYTES_V1`.

## Run Header

The 4-KiB Header contains:

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDXIRN01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `4,096` |
| 12 | 2 | `page_length` | `4,096` |
| 14 | 2 | `entry_length` | `128` |
| 16 | 8 | `required_flags` | zero |
| 24 | 8 | `compatible_flags` | zero from v1 writers |
| 32 | 8 | `run_generation` | nonzero appliance-local RoW generation |
| 40 | 8 | `entry_count` | exact total entry count |
| 48 | 8 | `page_count` | exact page count from the geometry equation |
| 56 | 4 | `entries_per_page` | `31` |
| 60 | 4 | `sort_order` | `1`, the tuple order below |
| 64 | 32 | `index_profile_id` | nonzero immutable profile identity |
| 96 | 32 | `minimum_chunk_id` | first Chunk ID; zero only for an empty run |
| 128 | 32 | `maximum_chunk_id` | last Chunk ID; zero only for an empty run |
| 160 | 8 | `pages_offset` | `4,096` |
| 168 | 8 | `footer_offset` | exact computed offset |
| 176 | 8 | `file_length` | exact computed and physical length |
| 184 | 4 | `header_crc32c` | Header CRC defined below |
| 188 | 3,908 | reserved | zero |

The Header CRC covers all 4,096 Header bytes with `[184,188)` treated as zero.
The minimum and maximum IDs are duplicated in the Footer and are lookup hints;
page/entry validation remains mandatory.

## Entry Page

Each independently readable 4-KiB page begins with:

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDXPG001` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `page_header_length` | `128` |
| 12 | 2 | `entry_length` | `128` |
| 14 | 2 | `entry_count` | 31 except the final page; never zero |
| 16 | 4 | `page_ordinal` | zero-based physical ordinal |
| 20 | 4 | `page_crc32c` | complete-page CRC defined below |
| 24 | 8 | `first_entry_ordinal` | `page_ordinal * 31` |
| 32 | 32 | `first_chunk_id` | equals the first entry key |
| 64 | 4 | `first_logical_length` | equals the first entry key |
| 68 | 4 | reserved | zero |
| 72 | 32 | `last_chunk_id` | equals the last entry key |
| 104 | 4 | `last_logical_length` | equals the last entry key |
| 108 | 20 | reserved | zero |

Entries begin at relative offset 128. Unused final-page entry slots are all
zero. The page CRC covers all 4,096 page bytes with `[20,24)` treated as zero.
Binary lookup reads and validates the Header, then independently validates only
the pages it probes. A page is never trusted merely because its key bounds
appear plausible.

The implemented `ExactIndexRunDescriptor` owns only the verified Header/Footer
facts. It computes page offsets without a page directory and decodes at most 31
entries per requested page. `ExactIndexPage::position` supports a lower-bound
binary search; `candidates` returns only transitions for the exact
`(chunk_id, logical_length)` key on that page. The later active-run reader must
fall back safely when a key spans pages or no usable candidate is found; a
bounded page lookup is acceleration, not proof of absence.

## Location-transition entry

Each 128-byte entry contains one complete physical Location identity and its
newest transition in this run:

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 32 | `chunk_id` | BLAKE3-256 logical content identity |
| 32 | 4 | `logical_length` | nonzero; equals Container Chunk Table |
| 36 | 2 | `transition` | `1=ACTIVE`, `2=RETIRING`, `3=QUARANTINED`, `4=REMOVED` |
| 38 | 2 | `codec_id` | equals Container Record codec |
| 40 | 16 | `container_id` | nonzero immutable Container identity |
| 56 | 8 | `container_generation` | nonzero and equals Container Header |
| 64 | 8 | `record_offset` | exact aligned Container record offset |
| 72 | 4 | `record_length` | exact bounded stored record length |
| 76 | 4 | `chunk_ordinal` | zero-based Chunk Table ordinal |
| 80 | 4 | `decoded_offset` | exact offset within decoded record |
| 84 | 4 | `record_crc32c` | equals Container Record Header |
| 88 | 4 | `record_decoded_length` | equals Container Record Header |
| 92 | 4 | `record_payload_length` | equals Container Record Header |
| 96 | 32 | `dependency_id` | zero for independent RAW; otherwise exact dependency identity |

Version 1 writers initially emit only independent RAW records: `codec_id=1`,
`chunk_ordinal=0`, `decoded_offset=0`, and zero `dependency_id`. Retaining the
complete record coordinates avoids redefining Location identity when bounded
range reads replace current whole-container reads. A reader rejects unsupported
codec/dependency combinations; it does not reinterpret them as RAW.

Entries are strictly sorted by unsigned bytewise `chunk_id`, numeric
`logical_length`, unsigned bytewise `container_id`, `record_offset`, and
`chunk_ordinal`. The same complete Location key may occur at most once per run.
Multiple Locations for one `(Chunk ID, logical length)` are legal. Any one Chunk
ID paired with two logical lengths is Corruption, including across pages, runs,
rebuild input, and active run-set generations.

When merging runs, the transition from the newest selected run wins for the
complete Location key. `REMOVED` is a lookup tombstone, not proof that deleting
the named Container is safe. GC requires committed Manifest liveness and pin
rules independently of this index.

## Run Footer and complete hash

The 4-KiB Footer contains:

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDXFTR01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `footer_length` | `4,096` |
| 12 | 2 | `page_length` | `4,096` |
| 14 | 2 | `entry_length` | `128` |
| 16 | 8 | `required_flags` | equals Header |
| 24 | 8 | `compatible_flags` | equals Header |
| 32 | 8 | `run_generation` | equals Header |
| 40 | 8 | `entry_count` | equals Header |
| 48 | 8 | `page_count` | equals Header |
| 56 | 4 | `entries_per_page` | equals Header |
| 60 | 4 | `sort_order` | equals Header |
| 64 | 32 | `index_profile_id` | equals Header |
| 96 | 32 | `minimum_chunk_id` | equals Header |
| 128 | 32 | `maximum_chunk_id` | equals Header |
| 160 | 8 | `pages_offset` | equals Header |
| 168 | 8 | `footer_offset` | equals Header and physical Footer offset |
| 176 | 8 | `file_length` | equals Header and physical length |
| 184 | 32 | `run_hash` | BLAKE3 hash defined below |
| 216 | 4 | `footer_crc32c` | Footer CRC defined below |
| 220 | 3,876 | reserved | zero |

The run hash is BLAKE3-256 over the complete file with Footer bytes `[184,220)`
treated as zero. The Footer CRC covers all Footer bytes after `run_hash` is
final, with `[216,220)` treated as zero. Normal random lookup validates Header,
Footer, and every touched page. Full run-hash verification is mandatory before
publishing a rebuilt/compacted run as active and during offline scrub.

## Publication and activation protocol

This run format is immutable and self-validating but deliberately does not
define authority or activation by filename. The
[Run Set](exact-index-run-set-v1.md) and
[Activation WAL](exact-index-activation-v1.md) formats use this order:

1. write a temporary run with explicit maximum length and checked offsets;
2. reread and run the complete writer-side parser, including all page CRCs and
   the run hash;
3. synchronize the run file;
4. publish it by no-replace rename and synchronize the index directory;
5. publish an immutable Run Set naming only already durable runs;
6. append, reread, verify, and synchronize a hash-chained activation record.

A crash before activation leaves an unselected run. A crash after activation
may select only the complete new Run Set. Failure or loss of every index object
triggers Container-Recovery-Index rebuild or the bounded verified slow path; it
never rolls the Namespace Commit WAL back.

New DATA may become Namespace-visible even if its acceleration entry is absent,
because Containers and Manifests are authoritative. Conversely, an ACTIVE index
entry for an otherwise unreachable durable orphan is not live until a committed
Manifest names its Chunk ID.

## Reader, rebuild, and scrub pairing

| Invariant | Writer | Random reader / recovery | Rebuild / offline scrub |
| --- | --- | --- | --- |
| geometry is bounded and exact | derive counts and offsets with checked arithmetic before allocation | validate Header/Footer equations before page allocation or I/O | reject every truncated, oversized, overlapping, or out-of-range object |
| each page is canonical and independently protected | sort entries, emit exact key bounds, zero unused slots, compute page CRC | validate page ordinal/count/bounds/CRC before binary search | walk every page and compare adjacent last/first keys |
| Location matches immutable Container evidence | copy fields from the verified Container Recovery Index | treat lookup as candidate; cross-check Container ID/generation/record/table and rehash decoded Chunk before return | reconstruct entries only from fully verified sealed Containers |
| one Chunk ID has one logical length | reject before sorting and across run-builder batches | reject within a run and across every probed active run | global external sort reports cross-Container conflicts as Corruption |
| run activation is RoW | synchronize dependencies before activation record | accept one contiguous activation-log prefix and one complete Run Set | fault after every write/sync/rename; observe old or complete new set only |
| Index is not liveness authority | never delete data while publishing or compacting index state | missing/negative/corrupt lookup falls back or fails index use, not Namespace visibility | rebuild may discard all prior index state and derive a new hidden generation |

Production `ASSERT` covers impossible writer cursor/order disagreement after
preflight. Persistent bytes, unsupported fields, missing objects, checksum
failure, and Location mismatch are `VERIFY` failures. Full run hash, global
cross-run length checks, and canonical rebuild comparison are exhaustive AUDIT
work in tests and scrub.

The current writer exposes `VerifiedRawLocation` only after a complete
Container-v1 decode has paired Header, Records, Recovery Index, Footer, CRCs,
container hash, and Chunk IDs. `ExactIndexEntry::from_verified_raw` consumes
that opaque evidence and rechecks shared coordinate invariants. Tests exhaust
all truncated prefixes and every single-byte mutation of a two-page run; no
mutation is accepted and no prefix panics.

## Deferred formats and benchmark parameters

Run Sets and activation records are assigned separately by
[Exact Index Run Set v1](exact-index-run-set-v1.md) and
[Exact Index Activation WAL v1](exact-index-activation-v1.md). The following
are policy/benchmark choices and must not be inferred from this file: level
count, level size ratio, level-zero cap, compaction
concurrency, Bloom/Binary-Fuse bits per key, page cache size, sharding prefix,
prefetch, mmap versus explicit range I/O, and GC retention. CPU multiversioning
and SIMD filters may accelerate lookup but cannot change canonical keys or
verification rules.
