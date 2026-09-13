---
status: accepted
---

# Resume recent scrub checks from durable progress

A stopped or completed initial Scrub round resumes fully checked immutable Containers instead
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
start a fresh pass. A durable completion marker retains the preceding checks on
the next mount. It records historical successful coverage, not current health.
Subsequent mounts may append checks for new or changed Containers and another
completion marker. Neither completion nor reuse moves the original round start,
so restarting cannot indefinitely postpone the seven-day full-pass threshold.
There is no permanent healthy flag or new periodic scrub timer;
explicit offline Scrub continues to force complete content verification.

This amends the original completion policy (2026-09-12): a successful scrub
previously forced a new full payload pass on the very next restart, even after
an orderly shutdown. Because that discarded every reusable certificate, the
parallel envelope-resume pool had no work and startup instead used the single
paced payload verifier. Completion is now replayed as a checksummed marker;
the journal format and conservative behavior of older readers remain unchanged.

Only a successful full Container check, including decoded identities and
independent Bases, mints an opaque progress entry. Each entry contains Container
ID, generation, byte length, structural fingerprint, check time, Chunk identities
and logical lengths, and dependent Base identities. The Chunk map is extracted
from the same verified image, without extra DATA I/O. The journal is larger than
a boolean-per-Container table because a new mount may select a newer Namespace
graph; old verified counts alone cannot discharge its DATA requirements.

Fresh complete checks also hand their typed physical Location evidence to the
existing online unified cache after Independent verification has ended (ADR
0046). This evidence comes directly from the full verifier in the current
process, is evictable, and does not retain Container payloads. Journal recording
does not create it: resumed entries, failed checks and envelope reconciliation
never seed that cache. Later scrubs still bypass all cached evidence. Thus the
handoff avoids a second ingest verification of freshly scrubbed Locations
without changing the journal's historical meaning or the GC gate.

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

The in-memory journal index stores offsets, not all saved Chunk maps. Replay
still allocates one format-bounded entry at a time. Resume prefetch groups at
most 32 entries and at most one MiB of encoded certificate bytes; an individually
larger entry is handled alone under the existing format bound. Current graph
and independent-Base coverage remain pass-local memory.

Envelope reconciliation uses a dedicated persistent pool of 32 workers, separate
from frontend and encoding pools. Each worker issues only one synchronous storage
operation at a time, so asynchronous batches have at most 32 outstanding resume
I/Os, not 64 for the Header/Footer pair. Idle I/O scheduling applies to every
worker. Header and Footer checks remain unchanged: the Footer supplies the
fingerprint, while cross-checking both envelopes detects mismatched identity,
generation and layout. No new durable format or validation rule is introduced.

Idle envelope reconciliation has no artificial read-duty sleep. On observed
frontend DATA activity, the coordinator reduces subsequent batches to one entry
for the existing five-second activity window; already dispatched envelope reads
back off before continuing. Payload verification retains its single paced
worker and existing duty limits. Envelope tasks fully join before coverage is
merged or payload verification begins, including on error or shutdown. A failed
batch contributes no coverage. A real envelope or I/O failure takes precedence
over a concurrent stop request. The ordinary scrub gate and full on-demand checks
remain authoritative.

Telemetry distinguishes carried-forward checks, new checks and remaining
Containers, including historical UI samples. Regressions hold 32 actual envelope
reads in flight, reject oversized batches, verify failure leaves coverage
incomplete, and cancel an in-flight batch before any Footer request is issued.
Completion/restart regressions also prove envelope-only reads after repeated
successful passes, replay of appended checks after completion, unextended expiry,
current-envelope failure, and before/after-I/O crashes during completion replay
and torn-tail repair. The complete marker alone never opens the GC gate.
