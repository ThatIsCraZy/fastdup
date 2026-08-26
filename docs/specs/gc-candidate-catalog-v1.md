# GC Candidate Catalog v1

Status: draft, pre-stable format. The field-by-field writer and reader,
streaming successor merge, immutable publication, mmap and bounded-read scans,
empty-generation handling, corruption fallback, and storage failpoint tests are
implemented. Metadata-liveness deltas and the generation-bound candidate-local
`GcCandidateProof` are implemented. The shared repository path also implements
same-process `RETIRING`, Exact-generation pin drain, victim unlink, and
`REMOVED`. Envelope-only catalog bootstrap, adaptive daemon scheduling, the
local online control command, and cross-process Appliance Lease exclusion are
implemented.

This format implements the non-authoritative discovery tier from
[ADR 0064](../adr/0064-discover-gc-candidates-incrementally-and-prove-victims-locally.md).
A catalog row may prioritize a Container for local proof. It never authorizes
`RETIRING`, Location replacement, or deletion.

All integers are little-endian. Writers serialize every field explicitly; Rust
layout is not the file format. Reserved bytes are zero. Rows are strictly
ordered by unique `container_id`.

## File geometry

```text
+----------------------+ offset 0
| 4-KiB Header         |
+----------------------+ offset 4,096
| N * 96-byte rows     |
+----------------------+
| zero alignment       |
+----------------------+ footer_offset, aligned to 4 KiB
| 4-KiB Footer         |
+----------------------+ file_length
```

```text
rows_end      = 4,096 + row_count * 96
footer_offset = align_up(rows_end, 4,096)
file_length   = footer_offset + 4,096
```

`row_count` may be zero. Such a catalog has `rows_end == footer_offset ==
4,096` and `file_length == 8,192`; it is a present empty generation, not a
missing hint.

## Paired envelope

Header and Footer contain the same descriptor except for their magic.

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Header `FDGCC001`; Footer `FDGCF001` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `envelope_length` | `4,096` |
| 12 | 2 | `row_length` | `96` |
| 14 | 2 | `hash_algorithm` | `1`, BLAKE3 |
| 16 | 2 | `crc_algorithm` | `1`, CRC32C |
| 18 | 14 | reserved | zero |
| 32 | 8 | `catalog_generation` | nonzero RoW generation |
| 40 | 8 | `incorporated_commit_generation` | newest Commit generation reflected by estimates |
| 48 | 8 | `incorporated_location_generation` | newest Location generation reflected by states |
| 56 | 8 | `row_count` | exact number of rows, including zero |
| 64 | 8 | `rows_offset` | `4,096` |
| 72 | 8 | `footer_offset` | exact geometry value |
| 80 | 8 | `file_length` | exact geometry and physical length |
| 88 | 32 | `catalog_hash` | digest defined below |
| 120 | 4 | `envelope_crc32c` | CRC defined below |
| 124 | 3,972 | reserved | zero |

The envelope CRC covers all 4,096 bytes with `[120,124)` treated as zero.
Header and Footer descriptors must match exactly.

The catalog hash is BLAKE3 over domain separator
`fastdup-gc-candidate-catalog-v1\0`, followed by a 48-byte little-endian block
containing generation, incorporated Commit generation, incorporated Location
generation, row count, Footer offset, and file length, followed by every
96-byte row in physical order. Alignment padding is excluded and independently
required to be zero.

## Candidate row

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 16 | `container_id` | nonzero, strictly increasing |
| 16 | 8 | `container_generation` | nonzero immutable publication fact |
| 24 | 8 | `physical_bytes` | nonzero exact Container length |
| 32 | 4 | `summary_checksum` | structural checksum of the intrinsic Container summary |
| 36 | 4 | `flags` | exactly one Location state plus optional estimate flags |
| 40 | 4 | `reachable_target_count` | zero unless liveness estimate is known |
| 44 | 4 | `live_independent_bases` | zero unless dependency estimate is known |
| 48 | 4 | `incoming_base_fanout` | zero unless dependency estimate is known |
| 52 | 4 | `outgoing_dependency_count` | immutable intrinsic summary fact |
| 56 | 8 | `estimated_encoded_coverage` | no greater than the record-area upper bound |
| 64 | 8 | `raw_replacement_upper_bound` | nonzero conservative publication-derived bound |
| 72 | 4 | `dead_record_bytes` | encoded record-byte estimate |
| 76 | 4 | `wholly_live_record_bytes` | encoded record-byte estimate |
| 80 | 4 | `partial_record_bytes` | encoded record-byte estimate |
| 84 | 12 | reserved | zero |

Flag bits are: bit 0 `estimate_known`, bit 1 `ACTIVE`, bit 2 `RETIRING`, bit 3
`QUARANTINED`, and bit 4 `dependency_estimate_known`. Exactly one of bits 1–3
is set. No other bits are accepted. The three record-byte classes and estimated
coverage must not exceed `physical_bytes - 8,192`. When the respective known
bit is clear, its estimate fields are zero.

Container publication creates an `ACTIVE`, unknown-liveness seed from verified
writer Location evidence. Updating an existing ID may change only estimate and
Location-state fields. Generation, physical length, summary checksum, RAW
replacement bound, and outgoing dependency count must still match.

When no usable catalog exists, the daemon counts canonical published Container
names, then performs one Container-ID-ordered streaming pass over paired
Header/Footer intrinsic summaries. Bootstrap reads no record payload and keeps
no pool-sized row map. It publishes unknown-liveness ACTIVE rows at a catalog
generation strictly above every canonical published name, including corrupt
hints ignored by recovery. Metadata liveness deltas then refine that seed.

## Metadata liveness deltas

For a catalog incorporating Commit generation `G`, the delta producer rebuilds
the logical Chunk union reachable from protected generations `(G-1, G)` and
compares it with the union reachable from the current protected pair `(N-1,
N)`. `G` must still be retained by the bounded Commit WAL. `G = none` denotes
an empty base and produces a complete initial population.

Additions and removals carry `(Chunk ID, logical length)` and are attributed to
Containers through bounded lookups in one explicitly selected active Exact
generation. Only the first transition for each physical Location is considered;
only `ACTIVE` transitions contribute, and one logical target counts at most
once per Container. Lookup truncation or missing catalog rows may reduce hint
quality but cannot affect proof correctness.

A positive delta initializes an unknown row. A removal from unknown state keeps
it unknown. If a removal would underflow a known count, all mutable estimate
fields are cleared and the row becomes unknown. The successor descriptor binds
the new Commit generation and the selected Exact activation generation used as
its Location attribution generation.

## Publication, recovery, and access

Files are published as
`gc-candidate-catalog-<16 lowercase hex generation>.run` with no-replace RoW
semantics. A successor generation must increase the catalog generation and may
not decrease either incorporated generation. The writer:

1. validates a sorted bounded update stream;
2. count-scans and merge-joins it with the pinned predecessor;
3. hashes and writes rows in batches of 8,192 rows (768 KiB);
4. writes paired envelopes and exact length;
5. independently rereads and audits the complete temporary object;
6. syncs file, publishes without replacement, and syncs the directory.

Recovery examines published names newest first. It validates paired envelopes,
all rows, global order, zero padding, and the catalog hash. A corrupt generation
is ignored because this structure is only acceleration; recovery may use the
next older valid generation. A valid empty newest generation therefore blocks
fallback to stale rows.

`FsStorageIo` scans a read-only mmap while holding the exact immutable-file
lease. The one unsafe mapping call is valid only because the shared lease blocks
cooperating write, truncate, replace, and unlink operations until unmap. The
mapping is still decoded field by field. Other storage adapters re-audit using
at most 8,192 rows per positional read. Shortlisting uses a bounded heap with a
maximum of 4,096 retained candidates.

## Boundary invariants

| Boundary | Required check |
| --- | --- |
| writer | exact row count/order, row invariants, monotonic successor freshness, whole-stream hash, reread before durability acknowledgement |
| reader/recovery | exact physical geometry, paired CRC-checked envelopes, row invariants/order, zero padding, whole-stream hash |
| offline scrub | rebuild publication facts from authenticated Containers and liveness estimates from protected Metadata/Location generations; compare or replace the hint generation |
| fault injection | after every failed storage operation and crash, recovery exposes either no catalog or one completely audited generation |
