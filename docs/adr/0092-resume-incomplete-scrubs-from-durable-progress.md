---
status: accepted
---

# Resume recent scrub checks from durable progress

Initial Scrub may reuse fully checked immutable Containers from the current
round instead of rereading their payloads after every mount. Persisted progress
means only **checked during this round**. It is neither current health, writer
evidence, a payload cache nor GC deletion authority. This replaces ADR 0090's
process-local-only progress rule and extends ADR 0091's deferred DATA checks.

## Journal and validity

The Metadata Pool stores `.fastdup-scrub-progress-v1`. Its header binds validator
version, original round start, Appliance/Pool identities and local Metadata/DATA
directory device/inode identities. Unknown version, foreign/damaged header,
clock reversal or age of at least seven days starts a fresh round. Copying a pool
pair changes local identities; an in-place restore may remain undetected.

A durable completion marker allows later mounts to retain the round and append
checks for new or changed Containers. It never advances the original start, so
restart cannot postpone the seven-day full pass indefinitely. Explicit offline
Scrub always performs complete verification.

Only a successful full check mints an entry. It records Container identity,
generation, byte length, structural fingerprint, check time, Chunk identities
and lengths, and dependent Base identities extracted from the same verified
image. These facts let a later Namespace graph prove current coverage; counts
alone cannot.

On resume, the worker inventories current Containers. Two uncached random-advice
Header/Footer reads must match saved identity, generation, length, fingerprint
and Chunk count before payload work may be skipped. Changed valid envelopes are
fully checked. Current graph requirements are reconstructed independently, only
selectable non-RETIRING Locations satisfy them, and Base availability is tracked
across the whole pass. Missing or damaged DATA fails closed.

Envelope-only reconciliation cannot detect payload corruption that leaves the
envelope unchanged. Demand reads, offline Scrub and the next full round retain
independent payload verification. The startup GC gate remains closed until
current Chunk and Base coverage completes; integrity failure keeps the existing
sticky write-admission failure.

Fresh full checks may hand typed Location evidence to the process-local unified
cache after Independent verification. Resumed entries, envelope checks and
journal replay never seed it. Later scrub still bypasses cached evidence.

## Durable encoding and bounds

All fields are explicitly little endian. The 80-byte header contains magic and
version, a 32-byte binding, round start and BLAKE3 checksum. Length-framed entries
carry a type, explicit fields and a checksum bound to the header. Acceptance
requires complete readback. Torn or corrupt suffixes are truncated to the valid
prefix before append; reset syncs truncation before the new header. File and
directory sync publish the journal. Validator changes require a new version.

The worker syncs after 64 new Containers, five seconds at a Container boundary,
or orderly cancellation. Journal failure disables persistence for the process
but full verification continues. Offline verification never reads the journal.

The in-memory index holds offsets, not saved Chunk maps. Replay allocates one
format-bounded entry at a time. Prefetch groups at most 32 entries and 1 MiB of
certificates; a larger legal entry runs alone.

## Concurrent verification

A dedicated idle-priority pool of 32 workers performs envelope reconciliation
and, in later batches, full verification. A worker issues one synchronous
storage operation at a time, so no phase exceeds 32 outstanding operations.
Frontend activity reduces subsequent batches to one item for the existing
five-second window.

Full-check input batches contain at most 32 Containers and split again at 64 MiB
of primary images. Reads use at most 256-KiB portions with cancellation and the
activity-sensitive duty delay. Dependency, decoder, certificate and coverage
state retain their existing separate bounds.

Every submitted task joins before coverage merge, journal append, phase change,
error return or shutdown. A failed batch contributes no coverage; real
corruption or I/O errors outrank cancellation. The coordinator alone merges
certificates and writes ordered progress.

Tests cover 32 actual outstanding reads, batch and memory limits, cancellation,
completion replay, non-extended expiry, changed envelopes, corrupt/torn tails,
I/O interruption and the rule that completion alone never opens the GC gate.
