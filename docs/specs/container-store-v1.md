# Container Store v1

Status: experimental, implemented for Stage 1.

This specification binds immutable Container v1 bytes to the XFS publication
namespace. It is deliberately smaller than the later metadata/WAL store: no
manifest or commit record may refer to a container until this protocol has
returned success.

## Canonical names

A Container ID is encoded as exactly 32 lowercase hexadecimal characters in
byte order.

- published: `<container-id>.fdc`
- temporary: `.<container-id>.building`

The all-zero ID is invalid. A published filename and the Container ID in both
the validated header and footer must agree. This is a paired durable identity,
not a lookup hint.

Any name ending in `.fdc` claims to be published and must have the exact
canonical form. A malformed claimed published name is a recovery failure. Exact
temporary names and unrelated names without the published suffix are not data
and are ignored. A non-UTF-8 directory name currently fails recovery rather than
being guessed at or silently lost.

## Publication

The writer uses a temporary and final name in the same directory:

1. create the temporary name without replacement;
2. write a BUILDING header;
3. write complete sealed body pages and the footer;
4. write the SEALED header last;
5. set the exact logical length, fully re-read through the production Container
   v1 verifier, require byte-for-byte equality with the intended sealed image
   plus the intended ID and generation, and sync the file;
6. issue one Linux `renameat2(RENAME_NOREPLACE)` within an open directory;
7. sync the directory.

The exact-length operation is both a correctness boundary and an XFS allocation
boundary: it removes speculative unwritten extents beyond EOF before the object
becomes durable. A collision never replaces existing bytes. Failure may leave a
temporary sealed orphan, but metadata commit has not started and recovery does
not treat that name as published.

## Durable generation allocator

The DATA root contains two fixed 4-KiB slots,
`container-generation.wal` and `container-generation.1.wal`. Each record has
magic `FDCGHW01`, format version `1`, a monotonic sequence, the BLAKE3-256 hash
of the exact predecessor record, a nonzero `reserved_through` generation, and a
CRC-32C over the complete record. Bytes after the checksum are reserved zero.

| Offset | Width | Field | Requirement |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `FDCGHW01` |
| 8 | 2 | format version | `1` |
| 10 | 2 | header length | `128` |
| 12 | 2 | record type | `1` |
| 14 | 2 | hash algorithm | `1` = BLAKE3-256 |
| 16 | 4 | record length | `4,096` |
| 20 | 2 | checksum algorithm | `1` = CRC-32C |
| 22 | 2 | reserved | zero |
| 24 | 8 | sequence | nonzero |
| 32 | 32 | previous record hash | zero only for sequence 1 |
| 64 | 8 | reserved through | nonzero, monotone |
| 72 | 4 | record CRC-32C | checksum with this field zero |
| 76 | 4,020 | reserved | zero |

The writer reserves 1,024 generations at a time. It alternates slots, writes
and fixes the exact 4-KiB length, rereads and decodes the intended bytes, and
syncs the target before returning any generation in that range. The selected
pair has adjacent sequences, an exact predecessor link, and a nondecreasing
high-water. Before the first record, missing or empty slots are an interrupted,
nonauthoritative initialization and retry resumes legacy migration. Once a
record exists, both names are required; a sequence-one record with an empty peer
is the only accepted single-record state. Gaps after a crash are valid; reuse is
not.

Both absent slots identify a legacy repository. Its first epoch-one writer
lists and sorts canonical published names once, then obtains each object's
physical length and reads only its fixed Header and Footer. Both envelopes must
validate and agree on identity, generation, layout, and length. Their greatest
generation seeds the first durable high-water record. Later healthy opens read
only the two fixed slots and perform no Container directory listing.

The legacy envelope proof and the allocator high-water only prevent generation
reuse. They do not authorize a Location or verify Container payload, Recovery
Index, records, or Chunk IDs. Publication, Scrub, and Exact-Index rebuild retain
their complete verification boundaries. Scrub also requires the durable
high-water to cover the greatest fully verified Container generation.

## Whole-container recovery and scrub

Recovery lists and sorts directory names, then handles each canonical published
name in Container-ID order. Before allocating file contents, the production XFS
reader rejects a declared length above 64 MiB and caps the actual read at 64 MiB
plus one byte to close a metadata/read race. Every accepted published file then
passes the complete Container v1 verifier, including all CRCs, the recovery-index
bijection, every decoded RAW/Zstd Chunk ID, and whole-container BLAKE3.

The scalable verification API decodes one container at a time and retains only
its ID, generation, chunk count, and file length. Its payload memory is therefore
bounded by one v1 container rather than total stored data. The payload-returning
recovery helper retains every decoded container and is suitable only for small
tests or callers that explicitly budget the complete payload set.

Recovery stops on a malformed published name, invalid published container, or
filename/header identity mismatch. It never silently skips claimed published
data. Quarantine and degraded continuation require a later explicit policy;
they are not implicit Stage-1 behavior.

Temporary files are ignored regardless of whether their bytes happen to form a
valid sealed container. They cannot be referenced by committed metadata and may
be reclaimed only by a later maintenance policy. Unrelated operator files are
ignored if their names do not claim the `.fdc` suffix.

## Paired checks and fault outcomes

| Invariant | writer | recovery | deterministic fault case |
| --- | --- | --- | --- |
| published bytes are the intended fully sealed image | exact production re-read, ID, and generation before file sync | complete Container v1 decode | substitute another valid image on writer reread; publication fails |
| filename identity equals durable identity | both names derive from writer ID | compare canonical name with verified header/footer ID | rename a valid file to another canonical ID; recovery fails |
| allocator never reuses a returned or observed generation | durably reserve a 1,024-generation range before returning its first value | verify the paired hash chain and require its high-water to cover every fully verified Container; use the bounded envelope scan only for legacy migration | fail-before/fail-after at every initial publication and range extension recovers legacy or one valid old/new pair and skips all pre-crash values |
| publication never replaces | `RENAME_NOREPLACE` | at most one canonical name per ID | duplicate publish returns `AlreadyExists`; original remains byte-valid |
| no unbounded read from corrupt length | writer preflights 64 MiB | metadata check plus capped read | sparse file above 64 MiB fails before format decode |
| acknowledged namespace entry is durable | file sync, atomic rename, directory sync | enumerate durable directory state | effective directory sync followed by returned error still recovers a valid orphan |

The deterministic memory backend models independent live and durable file and
directory state. It exhaustively injects both failure-before and
operation-effective-then-failure at every current publication operation. Torn
writes and real power-loss testing remain additional Stage-0 work; they do not
weaken the required outcomes above.

## Bounded Exact-Location demand reads

An Exact Index hit does not use startup's whole-object reader. The demand-read
path derives the canonical Container name from the candidate, obtains the exact
physical length, then reads only the 4-KiB Footer, 4-KiB Header, and the selected
at-most-1-MiB Encoding Record. The Header and Footer are independently
checksummed and must agree on Container ID, generation, layout, and physical
length before the candidate range is used. The selected independent RAW or Zstd
Record must then match every candidate coordinate, codec, Chunk-Table entry, and
stored Record CRC. The complete bounded Compression Region is decoded and the
selected logical Chunk is rehashed before any byte is returned.

This is record-level demand verification, not a claim that the complete
Container hash or Recovery Index was reread. Whole-container hashing and the
Recovery-Index bijection remain mandatory for publication, rebuild, and scrub.
Consequently the bounded reader does not emit `VerifiedChunkLocation`, which is
reserved for complete Container verification. Persistent envelope, Record, or
Chunk failures return a `VERIFY` error without partial data.
