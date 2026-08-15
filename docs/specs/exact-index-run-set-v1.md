# Exact Index Run Set v1

Status: draft, pre-stable format; canonical writer/reader, activation-WAL
publication, recovery, and corruption/fault tests are implemented.

An Exact Index Run Set is the immutable, content-addressed list of already
durable Exact Index Runs selected together by one later activation record. It
is acceleration state, never content or liveness authority. Missing or corrupt
Run Sets disable that Exact Index generation; they do not roll back the
Namespace Commit WAL or invalidate committed Manifests.

This format follows [ADR 0015](../adr/0015-keep-exact-dedup-correct-without-index-authority.md),
[ADR 0023](../adr/0023-rebuild-indexes-as-new-generations.md), the proposed
[ADR 0035](../adr/0035-build-the-exact-index-from-immutable-sorted-runs.md),
and [Exact Index Run v1](exact-index-run-v1.md). The selecting record and crash
protocol are specified in
[Exact Index Activation WAL v1](exact-index-activation-v1.md).

## Generic Metadata Object envelope

The Run Set uses the generic Metadata Object v1 envelope defined by
[Metadata generation format v1](metadata-generation-v1.md), with
`object_kind = 3`. Consequently:

- the complete object is aligned to 4,096 bytes and at most 16 MiB;
- the 4-KiB envelope Header contains exact payload/file lengths, payload CRC,
  Header CRC, and the content-derived nonzero Metadata Object ID;
- the Object ID domain includes object kind `3`, the exact payload length, and
  every unpadded payload byte; and
- all envelope padding is zero and rejected when nonzero.

`ExactIndexRunSetId` wraps that Metadata Object ID so a Namespace metadata ID
cannot accidentally be used as an Exact Index activation target.

## Payload geometry

The unpadded payload is exactly:

```text
[128-byte Run Set Header][run_count * 128-byte Run References]
payload_length = 128 + run_count * 128
```

Every count, multiplication, and allocation is validated against the physical
payload and the generic 16-MiB object bound before allocation.

### Run Set Header

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDXRST01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_length` | `128` |
| 12 | 2 | `run_entry_length` | `128` |
| 14 | 2 | reserved | zero |
| 16 | 8 | required flags | zero |
| 24 | 8 | `run_set_generation` | nonzero monotonic appliance-local generation |
| 32 | 32 | `index_profile_id` | nonzero; shared by every referenced Run |
| 64 | 4 | `run_count` | exact number of following entries |
| 68 | 4 | `payload_length` | exact header-plus-entries length |
| 72 | 56 | reserved | zero |

An empty Run Set is valid and can explicitly activate an index generation with
no lookup Runs. Empty individual Runs remain rebuild machinery and cannot be
referenced by an active Run Set.

### Run Reference

| Relative offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 2 | `level` | opaque compaction level selected by versioned policy |
| 2 | 6 | reserved | zero |
| 8 | 8 | `run_generation` | nonzero and unique within this Run Set |
| 16 | 32 | `run_hash` | exact BLAKE3 complete-file hash from the Run Footer |
| 48 | 8 | `file_length` | exact Run length; 4-KiB aligned, 8 KiB through 1 GiB |
| 56 | 8 | `entry_count` | nonzero exact Run entry count |
| 64 | 32 | `minimum_chunk_id` | equals the referenced Run Header/Footer |
| 96 | 32 | `maximum_chunk_id` | equals the referenced Run Header/Footer; not below minimum |

The Run filename is derived from the Run Set profile and `run_generation`; the
opened Run envelope must match every pinned field before lookup. The complete
Run hash is verified before publishing the Run Set and by offline scrub. Normal
lookup validates Header, Footer, and every touched page, and treats all returned
Locations as unverified candidates until Container pairing and Chunk rehash.

Run References are serialized in strict unsigned tuple order:

```text
(level, minimum_chunk_id, maximum_chunk_id, run_generation)
```

The format deliberately does not enforce LSM overlap ratios, maximum Runs per
level, level-zero fanout, or compaction thresholds. Those are benchmarked
policy. A duplicated `run_generation` is always Corruption because canonical
Run names use `(index_profile_id, run_generation)`.

## Publication and recovery

The Run Set writer must:

1. fully audit every referenced Run and pair all pinned descriptor fields;
2. encode and reread the complete Run Set through the production parser;
3. synchronize the temporary Run Set object;
4. publish by no-replace rename and synchronize the index directory; then
5. append, reread, and synchronize one activation record naming its exact
   `ExactIndexRunSetId`.

A crash before step 5 leaves an unselected object. A crash after the activation
record's final WAL sync selects the complete Run Set and only already durable
Runs. Retry may reuse an existing content-addressed object only after its bytes,
kind, and ID are fully verified.

Recovery accepts a contiguous valid activation-record prefix and selects only
its newest complete record. If that record's named Run Set or any named Run
fails verification, the Exact Index is disabled rather than silently selecting
an older generation. Unknown required format or policy is handled the same
way. The Namespace remains recoverable without any index generation.

## ASSERT, VERIFY, and AUDIT pairing

| Invariant | Writer | Reader/recovery | Offline scrub/rebuild |
| --- | --- | --- | --- |
| payload geometry is exact and bounded | checked equation before allocation | pair count, payload length, physical envelope and padding before allocation | reject every prefix, excess byte, and impossible count |
| Run identity is unambiguous | reject duplicate generations and profile mismatch | pair profile/generation/hash/length/count/bounds with opened Run | audit every complete Run hash and canonical filename |
| serialization is deterministic | sort strict canonical tuple and reject duplicates | reject noncanonical input; never resort persistent bytes | rebuild with varied discovery/worker order and require identical Object ID |
| activation dependencies are durable | synchronize every Run and Run Set before activation WAL | select only a complete dependency graph | fail before/after every write, sync, rename, and activation append |
| index remains nonauthoritative | never couple Namespace commit to Run Set success | negative/corrupt index falls back to duplicate storage or verified slow path | discard all index objects and rebuild from Containers/Manifests |

Impossible writer cursor/order disagreement is a production `ASSERT`.
Persistent bytes, I/O failures, unsupported fields, dependency mismatch, and
checksum/hash failure are `VERIFY` results. Exhaustive prefix/bit-corruption,
cross-order rebuild comparison, and full dependency traversal are `AUDIT` work.
