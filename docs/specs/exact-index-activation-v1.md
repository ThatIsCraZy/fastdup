# Exact Index Activation WAL v1

Status: draft, pre-stable format; canonical record writer/reader, Run Set
publication, activation, bounded lookup, recovery, and fail-before/fail-after
crash matrices are implemented, including replacement activation after bounded
Run compaction.

This WAL selects exactly one immutable
[Exact Index Run Set v1](exact-index-run-set-v1.md). It is acceleration state,
not content or Namespace authority. Losing or rejecting the complete Exact
Index graph may reduce ingest performance, but cannot make a committed
Manifest disappear and never authorizes Container deletion.

This format follows
[ADR 0015](../adr/0015-keep-exact-dedup-correct-without-index-authority.md),
[ADR 0023](../adr/0023-rebuild-indexes-as-new-generations.md), and
[ADR 0035](../adr/0035-build-the-exact-index-from-immutable-sorted-runs.md).
All integers are little-endian. Every reserved byte is zero and rejected when
nonzero. Rust layout is never serialized.

## File and record geometry

The canonical WAL name inside the Exact Index storage root is
`exact-index.activation.wal`. It is an append-only sequence of complete 4-KiB
records with no file header:

```text
[Activation Record generation 1]
[Activation Record generation 2]
...
```

Version 1 bounds the WAL at 64 MiB, or exactly 16,384 records. A writer must
reject an activation that would cross that bound before performing the append;
it must never acknowledge a WAL that recovery will reject. Rotation or a
checkpointed successor WAL is a later format and protocol change. Reaching the
bound disables new index activations, not Namespace commits.

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
| 144 | 8 | `run_set_generation` | nonzero and strictly increasing in the WAL |
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
3. create and durably publish the empty WAL if it does not exist;
4. validate the complete existing activation chain and require a clean record
   boundary, contiguous activation generations, and increasing Run Set
   generations;
5. preflight that the next complete record remains within 64 MiB;
6. append one record, reread the exact 4-KiB range, compare the bytes and parsed
   fields, and then synchronize the WAL.

The WAL synchronization in step 6 is the only activation commit point and the
last fallible storage operation before success is returned. Retrying the exact
already-active Run Set rereads its dependencies and synchronizes the WAL again;
it does not append another record.

## Recovery and lookup

Recovery ignores a trailing incomplete record below the physical WAL bound.
It rejects an invalid complete record, broken hash link, noncontiguous
activation generation, non-increasing Run Set generation, or oversized WAL; it
does not silently select an older index generation. This disables the index,
not the Namespace.

For the newest complete record, recovery loads the exact content-addressed Run
Set, pairs its profile, ID, and generation with the record, then fully audits
every pinned Run before exposing the active reader. Version 1 permits at most
64 active Runs.

Lookup visits Runs newest-generation-first, skips key-disjoint Runs using
verified minimum/maximum Chunk IDs, validates every touched 4-KiB page, and
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
is recoverable.

## ASSERT, VERIFY, and AUDIT pairing

| Invariant | Writer | Reader/recovery | Offline scrub/rebuild |
| --- | --- | --- | --- |
| activation selects only durable dependencies | full-audit Runs; sync Run Set and directory before append | pair record, Run Set, and every Run descriptor | traverse and hash every referenced object |
| committed activation is one atomic cut | reread record; final WAL sync is commit point | accept only the contiguous durable prefix | fail before and after every storage operation; observe old or complete new set |
| chain and bounds are exact | checked generation/hash/length arithmetic before append | validate every complete record and reject WAL over 64 MiB | exhaustive record prefix and byte-mutation tests |
| lookup work is bounded | cap active Runs and format candidate fanout | range-read only touched pages; cap result at 64 | exercise hot keys spanning pages and Runs |
| index is nonauthoritative | never couple Namespace commit to index success | disable corrupt index; preserve Namespace recovery | discard and rebuild the complete index graph |

Poisoned writer locks or contradictions in already verified fixed geometry are
production `ASSERT` failures. Persistent bytes, I/O, chain, checksum, identity,
and dependency failures are `VERIFY` results. Full-run hashing, crash matrices,
and rebuild comparison are `AUDIT` work.
