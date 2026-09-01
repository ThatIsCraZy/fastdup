# DATA-tier Recovery Checkpoint v1

This specification defines the only supported pre-production DATA-tier
Recovery Checkpoint format. It is a disaster-recovery copy of one committed
Namespace graph, not the online Commit WAL, an index, or a user snapshot. No
legacy decoder or migration path exists.

## Published names and selection

An immutable checkpoint has the name
`recovery-checkpoint.<generation-as-16-lowercase-hex>.fdrc`. Two fixed 4-KiB
selector files, `recovery-checkpoint.0.head` and
`recovery-checkpoint.1.head`, name the current and immediately previous
complete checkpoints. Recovery reads only these two heads; it never discovers
authority by listing the DATA directory.

Each valid head contains:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `FDRCH001` |
| 8 | 2 | little-endian format version `1` |
| 10 | 2 | record length `4096` |
| 12 | 2 | BLAKE3-256 algorithm ID `1` |
| 14 | 2 | zero |
| 16 | 8 | selected Commit generation |
| 24 | 8 | immutable checkpoint file length |
| 32 | 32 | checkpoint body BLAKE3-256 |
| 64 | 8 | previous selected generation, or zero |
| 72 | 32 | BLAKE3-256 of the complete previous head, or zero |
| 104 | 4 | CRC32C with this field zeroed |
| 108 | 3988 | zero |

The previous generation and hash are either both zero or both present. A
present previous generation is lower than the selected generation. With two
nonempty heads the newer head must link exactly to the older one. Scrub rejects
any malformed retained head; recovery may fall back atomically to the valid
older head.

## Immutable checkpoint file

The file is, in order:

1. one 4-KiB descriptor header;
2. the selected 4-KiB Commit Record encoded in its native current format;
3. one entry for every transitively reachable Metadata object, including every
   Namespace Shard named by the Namespace Root descriptor, sorted by
   `MetadataObjectId` with no duplicate, missing, or extra object;
4. one 4-KiB descriptor footer.

Header and footer have distinct magics (`FDRCV001` and `FDRCF001`) and otherwise
the same fields:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | descriptor magic |
| 8 | 2 | little-endian format version `1` |
| 10 | 2 | descriptor length `4096` |
| 12 | 2 | BLAKE3-256 algorithm ID `1` |
| 14 | 2 | zero |
| 16 | 8 | complete file length |
| 24 | 8 | Commit generation |
| 32 | 8 | Metadata object count, nonzero |
| 40 | 32 | Namespace Root object ID |
| 72 | 32 | Policy Set ID |
| 104 | 32 | BLAKE3-256 of Commit plus all entry headers, payloads, and padding |
| 136 | 4 | CRC32C with this field zeroed |
| 140 | 3956 | zero |

Header and footer must decode identically. Their generation, Namespace Root,
and Policy Set must equal the embedded Commit Record. The descriptor file
length must equal the physical file length.

Each entry starts with a 64-byte header followed by its encoded Metadata object
and zero padding to the next 64-byte boundary:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `FDRCM001` |
| 8 | 2 | format version `1` |
| 10 | 2 | entry-header length `64` |
| 12 | 4 | payload length, `1..=MAX_METADATA_OBJECT_BYTES` |
| 16 | 32 | Metadata object ID |
| 48 | 4 | payload CRC32C |
| 52 | 4 | header CRC32C with this field zeroed |
| 56 | 8 | zero |

The payload must hash to the named Metadata object ID. Readers use bounded
range reads of at most 1 MiB and retain only the object-to-file-span map plus
the graph traversal state; they do not materialize the complete checkpoint.

## Publication authority

Before graph traversal, the publisher briefly holds the Metadata-GC
publication barrier and Commit lock, chooses at most the newest two valid
Commit candidates, and installs process-local Namespace-root pins. It releases
both locks before traversing Metadata, verifying required DATA, or writing the
DATA tier. The pins are included in Metadata- and DATA-GC liveness and are
revalidated before DATA retirement.

For the highest complete candidate, publication:

1. writes a same-directory `.building` file from length zero;
2. writes the Commit and sorted Metadata entries incrementally;
3. writes the authenticated header and footer and fixes the file length;
4. independently rereads the entire file, exact graph, and required DATA;
5. syncs the temporary file, publishes it without replacement, and syncs the
   DATA directory;
6. overwrites only the inactive head, rereads it exactly, and syncs that head;
7. removes a checkpoint no longer named by either valid head and syncs the
   directory.

Only step 6 changes selection authority. Every fail-before/fail-after point
therefore recovers either the preceding selection or one complete new
checkpoint. The current and previous selected files are retained.

## Recovery and scrub

Recovery evaluates selected heads newest first. A candidate is accepted only
if descriptor, Commit, every entry, body hash, the Namespace Root's exact
ordered shard partition and reconstructed full-state hash, exact transitive graph, Manifest
length, Chunk length, and every reachable DATA Chunk verify. Candidate
corruption permits whole-generation fallback; transient I/O is reported.

No replacement Metadata object is written before checkpoint and DATA
verification completes. Installation then publishes the exact embedded
objects, syncs their directory, verifies the installed graph again, and
installs the embedded Commit as the final anchor. The target must be empty or
contain exactly that already-installed anchor from an interrupted idempotent
retry. The daemon rebuilds nonauthoritative online indexes before opening the
namespace.

Scrub strictly verifies both retained head/checkpoint pairs. DATA GC includes
their reachable Chunks in its protected set, including Chunks named only by the
previous checkpoint. Metadata GC protects an in-progress publisher's root pin.

## Scheduling boundary

The daemon attempts publication every 90 seconds with delayed missed ticks and
once at orderly shutdown. It uses a dedicated asynchronous worker and executes
the blocking scan and DATA I/O outside the normal five-second Commit loop. A
blocked checkpoint write may delay the next disaster-recovery point, but it
must not delay an ordinary Commit.
