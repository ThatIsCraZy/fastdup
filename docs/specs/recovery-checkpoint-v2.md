# DATA-tier Recovery Checkpoint v2

This specification replaces v1 and defines the only supported pre-production
Recovery Checkpoint format (ADR 0104). It is a disaster-recovery copy of
committed Namespace graphs, not the online Commit WAL, an index, or a user
snapshot. No v1 decoder or migration path exists.

All integers are little endian. BLAKE3 means BLAKE3-256. CRC32C fields are
computed with the field itself zeroed.

## Published names and selection

An immutable checkpoint file has the name
`recovery-checkpoint.<generation-as-16-lowercase-hex>.fdrc`. Two fixed 4-KiB
selector files, `recovery-checkpoint.0.head` and `recovery-checkpoint.1.head`,
name the current and immediately previous selected tip. Recovery reads only
these heads and the links reachable from them; it never discovers authority by
listing the DATA directory.

Each valid head contains:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `FDRCH002` |
| 8 | 2 | format version `2` |
| 10 | 2 | record length `4096` |
| 12 | 2 | BLAKE3 algorithm ID `1` |
| 14 | 2 | zero |
| 16 | 8 | selected tip generation |
| 24 | 8 | tip file length |
| 32 | 32 | tip body BLAKE3 |
| 64 | 8 | previous selected generation, or zero |
| 72 | 32 | BLAKE3 of the complete previous head, or zero |
| 104 | 4 | CRC32C |
| 108 | 3988 | zero |

Previous generation and hash are both zero or both present; a present previous
generation is lower than the selected generation. With two nonempty heads the
newer head must link exactly to the older one.

## Chains

A file is either a Base (`chain index` zero) or a Delta (`chain index` one to
255). A Delta links to its parent file; following parents from any file reaches
exactly one Base after `chain index` steps. The files from that Base through a
file F are F's chain prefix.

- Object identities are unique within a chain: no file repeats an object of its
  own prefix.
- Prefix closure: every Metadata reference of every object in F's prefix names
  an object in that same prefix, and F's Namespace Root is in it.
- Reference facts: a Manifest inner-node child range must equal the child's
  level plus one, logical length and allocated bytes; an inode-shard Manifest
  root must equal the regular file's logical size.

A Base may name a previous-chain tip: the selected tip of the chain it replaced.
Recovery discovers that link from the newest head's tip header or footer.

## Immutable checkpoint file

The file is, in order:

1. one 4-KiB descriptor header;
2. the 4-KiB Commit Record in its native current format;
3. the payload region: every embedded Metadata object sorted by
   `MetadataObjectId`, each zero padded to the next 64-byte boundary;
4. the object table: one 80-byte record per embedded object, same order;
5. one 4-KiB descriptor footer.

Header and footer use magics `FDRCV002` and `FDRCF002` and otherwise identical
fields:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic |
| 8 | 2 | format version `2` |
| 10 | 2 | descriptor length `4096` |
| 12 | 2 | BLAKE3 algorithm ID `1` |
| 14 | 2 | zero |
| 16 | 8 | complete file length |
| 24 | 8 | Commit generation |
| 32 | 8 | embedded object count; nonzero for a Base |
| 40 | 32 | Namespace Root object ID |
| 72 | 32 | Policy Set ID |
| 104 | 32 | body BLAKE3 over Commit, payload region and table |
| 136 | 32 | table BLAKE3 |
| 168 | 8 | chain index |
| 176 | 8 | Base generation |
| 184 | 8 | chain object count through this file |
| 192 | 8 | chain payload bytes through this file |
| 200 | 8 | parent generation, or zero for a Base |
| 208 | 8 | parent file length, or zero |
| 216 | 32 | parent body BLAKE3, or zero |
| 248 | 8 | previous-chain tip generation, or zero |
| 256 | 8 | previous-chain tip file length, or zero |
| 264 | 32 | previous-chain tip body BLAKE3, or zero |
| 296 | 4 | CRC32C |
| 300 | 3796 | zero |

A Base has Base generation equal to its own generation, zero parent fields, and
chain totals equal to its own totals. A Delta has a lower parent generation, a
Base generation not above the parent generation, and chain totals equal to its
parent's totals plus its own. Every file of a chain repeats its Base's
previous-chain link, whose generation is below the Base generation, so the tip
alone discovers the previous chain. A link is all zero or
names a lower generation with nonzero length and hash. Header and footer decode
identically; generation, Namespace Root and Policy Set equal the embedded
Commit; the file length equals the physical length.

Each table record:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 32 | Metadata object ID |
| 32 | 8 | payload offset |
| 40 | 4 | payload length, `1..=MAX_METADATA_OBJECT_BYTES` |
| 44 | 4 | payload CRC32C |
| 48 | 1 | kind: 1 graph root, 2 inode shard, 3 entry shard, 4 Manifest leaf, 5 Manifest inner node |
| 49 | 1 | flags: bit 0 allocated bytes present; other bits zero |
| 50 | 2 | Manifest level; zero for other kinds |
| 52 | 4 | zero |
| 56 | 8 | Manifest logical length; zero for other kinds |
| 64 | 8 | Manifest allocated bytes, or zero when absent |
| 72 | 8 | zero |

Records are strictly ascending by object ID. The first payload begins at offset
8192; each next payload begins at the previous payload's 64-byte padded end; the
table begins at the last padded end; the footer begins right after the table.
Leaves always carry allocated bytes; inner nodes carry them exactly when every
child range does; Namespace objects carry none. Every payload must hash to its
object ID and decode to the recorded kind and facts. Readers use bounded range
reads of at most 1 MiB and never materialize a complete file.

## Publication

Candidate selection and root pinning are unchanged from v1: the publisher
briefly holds the Metadata-GC publication barrier and Commit lock, pins at most
the newest two valid Commits, and releases both locks before traversal or DATA
I/O.

For the highest complete candidate the publisher selects Base or Delta (ADR
0104), traverses the source graph while treating chain objects as complete
subtrees validated by their table facts, and then:

1. removes an unselected file of the same name and writes a same-directory
   `.building` file from length zero;
2. writes the Commit, payload region and table in bounded batches, hashing each
   emitted object against its selected identity;
3. writes the authenticated footer, patches the header and fixes the length;
4. syncs the temporary file, publishes it without replacement, and syncs the
   DATA directory;
5. overwrites only the inactive head, rereads it exactly, and syncs that head;
6. removes checkpoint names outside the retained chains and syncs the directory.

Only step 5 changes selection authority. Every fail-before/fail-after point
recovers the preceding selection or one complete new tip. New files are not
reread; an existing head-selected file of the candidate generation must audit
completely before reuse.

## Recovery, scrub and GC

Recovery evaluates the newest head, the older head and the newest head's
previous-chain link. A candidate is accepted only if every file of its chain
verifies completely, and if its Commit graph verifies: Namespace partition and
reconstructed state, Manifest structure and lengths, Chunk lengths, and every
reachable DATA Chunk. Candidate corruption permits whole-candidate fallback;
transient I/O is reported. No replacement Metadata object is written before
that verification. Installation then publishes exactly the reachable objects,
syncs their directory, verifies the installed graph and installs the Commit as
the final anchor.

Scrub strictly verifies every retained candidate. DATA GC protects the union of
Chunks reachable from every retained candidate; Online GC may compute it from
cached per-file references of fully verified files.
