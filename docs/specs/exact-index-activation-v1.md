# Exact Index Activation Log v1

Status: draft, pre-stable format; canonical record writer/reader, paired-slot
rotation, legacy migration, Run Set publication, bounded lookup, recovery,
offline audit, and fail-before/fail-after crash matrices are implemented.

This log selects exactly one immutable
[Exact Index Run Set v1](exact-index-run-set-v1.md). It is acceleration state,
not content or Namespace authority. Losing or rejecting the complete Exact
Index graph may reduce ingest performance, but cannot make a committed
Manifest disappear and never authorizes Container deletion.

This format follows
[ADR 0015](../adr/0015-keep-exact-dedup-correct-without-index-authority.md),
[ADR 0023](../adr/0023-rebuild-indexes-as-new-generations.md), and
[ADR 0035](../adr/0035-build-the-exact-index-from-immutable-sorted-runs.md),
with lifetime rotation defined by
[ADR 0044](../adr/0044-rotate-the-exact-index-activation-log-through-paired-slots.md).
All integers are little-endian. Every reserved byte is zero and rejected when
nonzero. Rust layout is never serialized.

## Slot and record geometry

The canonical slot names inside the Exact Index storage root are
`exact-index.activation.wal` and `exact-index.activation.1.wal`. Both are
created, file-synchronized, and made directory-durable before the first
Activation Record. Each ordinary slot is a sequence of at most 64 complete
4-KiB records with no file header:

```text
[first retained Activation Record, possibly a bridge]
[successor Activation Record]
...
```

The first record in a slot may be generation 1 with a zero predecessor hash, or
a bridge copied byte-for-byte from the previously selected slot's last record.
An ordinary slot is at most 262,144 bytes. For migration only, the first slot
may contain the former single-file v1 chain up to 64 MiB (16,384 records). The
first subsequent activation rotates from that legacy chain into the bounded
second slot; subsequent reuse makes both files ordinary bounded slots.

Two nonempty slots form one valid topology only when the first record of the
newer slot is byte-identical to the final record of the older slot. Equal final
generations require byte-identical final records. A longer valid prefix is
selected when those final records agree; equal-length prefixes must be wholly
byte-identical. Missing overlap or divergent bytes are corruption.

## Activation Record

Each record is exactly 4,096 bytes:

| Offset | Width | Field | Version 1 requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FDXACT01` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `record_type` | `1`, activate Run Set |
| 12 | 2 | `header_length` | `160` |
| 14 | 2 | hash algorithm | `1`, BLAKE3-256 |
| 16 | 4 | `record_length` | `4,096` |
| 20 | 2 | checksum algorithm | `1`, CRC32C |
| 22 | 18 | reserved | zero |
| 40 | 8 | `activation_generation` | contiguous, starts at `1` |
| 48 | 32 | `previous_record_hash` | zero only for generation 1; otherwise BLAKE3-256 of the complete preceding encoded record |
| 80 | 32 | `run_set_id` | nonzero content ID of the exact Metadata-object bytes |
| 112 | 32 | `index_profile_id` | nonzero and equal to the selected Run Set and every Run |
| 144 | 8 | `run_set_generation` | nonzero and strictly increasing in the log |
| 152 | 4 | `record_crc32c` | CRC32C over the complete record with this field zero |
| 156 | 3,940 | reserved | zero |

The predecessor hash includes the preceding record's stored CRC field. The
reader validates each record's structure and CRC before using it to validate
the next hash link.

## Publication and commit protocol

One process owns one repository writer and serializes Run publication and
activation through the shared repository lock. Cross-process appliance
ownership remains the responsibility of the later mount/lease layer.

To activate Run Set generation `N`, the writer must:

1. fully audit every referenced immutable Run, including every page and the
   complete Run hash, and pair profile, generation, hash, length, entry count,
   and key bounds with its Run Reference;
2. encode the canonical content-addressed Run Set, reread it through the
   production parser, synchronize it, publish with no-replace rename, and
   synchronize the index directory;
3. create, synchronize, and make both empty slot names directory-durable when
   they do not yet exist;
4. validate both slot-local chains and select one unique overlapping current
   prefix with a clean tail;
5. if fewer than 64 records are selected, append the successor to that slot;
6. otherwise truncate only the inactive slot, write the selected final record
   as a bridge followed by the successor, and set its exact length to 8 KiB;
7. reread the complete target slot, validate its chain and exact intended
   bytes, then synchronize that slot.

The target-slot synchronization in step 7 is the only activation and rotation
commit point and the last fallible storage operation before success is
returned. Both names were made directory-durable before any record mutation.
Retrying the exact already-active Run Set rereads its dependencies and
synchronizes the selected slot again; it does not append another record.

## Recovery and lookup

Recovery reads no more than the two bounded slots after migration. It validates
each local chain and their cross-slot overlap before selecting the newest final
record. A trailing incomplete record is ignored for recovery, but a nonempty
slot without one valid record, an invalid complete record, broken hash link,
noncontiguous activation generation, non-increasing Run Set generation,
oversized ordinary slot, or divergent topology disables the index. It does not
silently select an unrelated older index generation and never affects Namespace
recovery.

For the newest complete record, recovery loads the exact content-addressed Run
Set, pairs its profile, ID, and generation with the record, then fully audits
every pinned Run before exposing the active reader. The active set permits at
most 64 logical Run families; a complete family may contain multiple
key-disjoint physical Runs as defined by
[ADR 0045](../adr/0045-partition-exact-index-compaction-into-run-families.md).

Lookup visits families newest-generation-first, selects at most one partition
per family from verified minimum/maximum Chunk IDs, validates every touched
4-KiB page, and
returns at most 64 Location transitions. `complete=false` means the bounded
candidate prefix truncated possible transitions. `complete=true` is complete
only for this activated Run Set; a negative Exact Index result is never proof
that content does not exist. Every returned ACTIVE candidate remains
unverified until its immutable Container coordinates, record checksum, decoded
length, and BLAKE3 Chunk ID are paired on the data path.

The implemented indexed Chunk reader merges repeated physical Locations in
this newest-Run-first order, attempts at most one preferred and one alternate
ACTIVE Location, and demand-verifies each through the bounded Container reader.
An index lookup failure, negative result, or unusable bounded candidates falls
back to the fully verified Container scan. This slow path is intentionally
expensive: it preserves the rule that losing every index object cannot make a
valid committed Manifest unreadable. It is not the normal hit path.

Each committed Manifest reader pins one immutable `ActivatedExactIndex`; a later
activation cannot change a read halfway through its physical-location view.
The writable checkpoint publisher activates a successor Run Set after new
Containers are durable and before publishing the Namespace generation, then
installs that new pin behind subsequently committed readers. An activation that
outlives a failed Namespace commit may name durable orphan Locations but cannot
make them live. Activation failure marks acceleration degraded and leaves the
Namespace commit independent.

The current writer compacts four oldest same-level dependencies before a
successor would approach the 64-Run reader bound. Compacted Runs and the new Run
Set are immutable RoW objects. A deterministic fail-before/fail-after matrix
over source audit, output publication, and replacement activation observes only
the prior active Run Set or the complete replacement; no mixed dependency graph
is recoverable. The same matrix now covers an inactive-slot rewrite at the
rotation boundary. An explicit offline audit repeats slot selection and fully
audits the selected dependency graph.

## ASSERT, VERIFY, and AUDIT pairing

| Invariant | Writer | Reader/recovery | Offline scrub/rebuild |
| --- | --- | --- | --- |
| activation selects only durable dependencies | full-audit Runs; sync Run Set and directory before append | pair record, Run Set, and every Run descriptor | traverse and hash every selected object |
| committed activation is one atomic cut | reread exact target slot; final slot sync is commit point | accept only one contiguous overlapping durable prefix | fail before and after every rotation operation; observe old or complete new set |
| chain and bounds are exact | checked generation/hash/length arithmetic; rotate at 64 records | validate both complete local chains, bridge equality, and bounded lengths | reject corruption in either selected or inactive peer through `audit_activation_log` |
| lookup work is bounded | cap active families and format candidate fanout | range-read at most one partition per family; cap result at 64 | exercise hot keys spanning pages and partitioned families |
| index is nonauthoritative | never couple Namespace commit to index success | disable corrupt index; preserve Namespace recovery | discard and rebuild the complete index graph |

Poisoned writer locks or contradictions in already verified fixed geometry are
production `ASSERT` failures. Persistent bytes, I/O, chain, checksum, identity,
and dependency failures are `VERIFY` results. Full-run hashing, crash matrices,
and rebuild comparison are `AUDIT` work.
