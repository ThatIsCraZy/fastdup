---
status: accepted
---

# Resume incomplete scrubs from durable progress

A stopped initial Scrub round resumes fully checked immutable Containers instead
of rereading their payloads on every mount. This supersedes ADR 0090's
process-local progress rule and extends ADR 0091's deferred DATA requirements.
The persisted result means **checked during this round**, never healthy forever,
current payload proof, writer publication evidence, or GC deletion authority.

## Progress and reconciliation

The Metadata Pool holds one append journal, `.fastdup-scrub-progress-v1`. Its
header binds the validator version, round start, durable Appliance/Pool identities
and local Metadata/DATA directory device/inode identities. A copied pool pair
with different local identities starts over. Restoring files in place under the
same identities cannot be detected from this binding alone. Unknown versions,
foreign or damaged headers, clock reversal, and rounds at least seven days old
start a fresh pass. A durable completion marker also starts a new full pass on
the next mount. There is no permanent healthy flag or new periodic scrub timer;
explicit offline Scrub continues to force complete content verification.

Only a successful full Container check, including decoded identities and
independent Bases, mints an opaque progress entry. Each entry contains Container
ID, generation, byte length, structural fingerprint, check time, Chunk identities
and logical lengths, and dependent Base identities. The Chunk map is extracted
from the same verified image, without extra DATA I/O. The journal is larger than
a boolean-per-Container table because a new mount may select a newer Namespace
graph; old verified counts alone cannot discharge its DATA requirements.

The next worker inventories current Containers after mounting. A saved entry
can skip payload work only after two uncached, random-advice Header/Footer reads
validate the current ID, generation, length, structure fingerprint and Chunk
count. Changed valid envelopes require full verification. Missing or damaged
DATA still fails closed. Current required Chunk identities are reconstructed
from the selected Metadata graph; only currently selectable Locations discharge
them. Coverage also tracks independent Base availability across the pass,
regardless of whether a Base precedes or follows its dependent Container.
RETIRING Locations cannot satisfy either requirement. An entirely absent
Container or Base therefore cannot be hidden by an old journal entry.

This is historical verification, not detection of new payload bitrot: corruption
that leaves the envelope unchanged may wait until a demand read, offline Scrub,
or a subsequent full round. Demand reads always verify payload and dependencies.
The startup GC gate remains closed until current graph and Base coverage finish;
ordinary generation-bound GC authorization remains separate. Integrity failure
retains the existing sticky write-admission failure.

## Durability and resource bounds

All fields are encoded explicitly in little endian. The fixed 80-byte header
has an eight-byte magic/version, 32-byte binding, eight-byte round start and
BLAKE3 checksum. Each length-framed entry has a type, explicit fields and a
BLAKE3 checksum bound to the header. Complete readback precedes acceptance;
unknown, torn or corrupt suffixes are truncated to the accepted prefix before
appending. A reset synchronizes truncation before writing the new header.
File and directory synchronization publish new journals. No DATA formats or
Commit-WAL semantics change. Validator changes must invalidate the journal version.

The worker synchronizes after 64 newly checked Containers or five seconds at a
Container boundary, and when orderly cancellation joins the worker. A crash may
repeat the unsynchronized suffix; it cannot turn partial verification into an
accepted entry. Journal I/O failure disables persistence and reuse for the rest
of that process while full verification continues, with a journal warning.
Offline verification never consults this auxiliary journal.

The in-memory journal index stores offsets, not all saved Chunk maps. Replay and
lookup allocate at most one format-bounded entry at a time. Current graph and
independent-Base coverage remain pass-local memory. Payload work retains the
single paced worker; envelope read pacing is batched into 256-KiB groups rather
than imposing a delay on each 4-KiB block. Telemetry distinguishes carried-forward
checks, new checks and remaining Containers, including historical UI samples.
