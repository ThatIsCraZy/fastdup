---
status: accepted
---

# Start from the committed Metadata graph

Normal writable startup selects the current valid Commit-WAL prefix and checks
its compatibility, Namespace transitions and complete selected Metadata graph.
It does not enumerate or read Container files. ADR 0090's structural Container
scan still took 775,728 ms for 26,960 Containers on the test VM: millions of small
reads kept startup proportional to the entire DATA pool. We defer that scan to
the initial background scrub, relying on ADR 0019's synchronized DATA, Metadata,
then Commit publication for crash consistency.

## Recovery boundary

The newest valid Commit must have a valid complete Metadata graph. A valid
newer Commit with missing or corrupt Metadata does not silently fall back to an
older graph. Existing two-slot bounded WAL selection remains authoritative;
there is no new checkpoint file or trusted Exact-Index flag. Once the selected
graph has passed, an invalid WAL suffix may be truncated under the commit lock,
fenced by the exact selected Commit. Only bytes after the accepted prefix are
removed; the prefix is never rewritten. Reread and sync precede writable inode
reservation. Crash injection before and after every recovery I/O must preserve
the same committed contents and allow another recovery attempt.

The selected Namespace and Manifest graph is still checked eagerly. Complete
lazy Metadata loading is a separate change; normal startup retains the bounded
WAL history and active-index recovery costs. Paired Container-generation
reservation files are still checked before new Container IDs are issued.

## Deferred DATA requirements

Recovery transfers the selected graph's required Chunk identities and lengths
to the initial scrub. This is outstanding verification work, not a content proof.
The worker inventories published Containers after mounting, fully verifies each
Container and its independent dependent-codec Bases, then discharges matching
requirements only for selectable Containers. Chunk identities are extracted
from the same fully checked in-memory image; no second structural disk pass is
needed. RETIRING Locations cannot satisfy the startup requirements.

The scrub is incomplete if any required Chunk remains, even if every file in
the directory snapshot passed. This catches entirely missing Containers as well
as corrupt existing files. Online GC remains gated for the whole pass, so the
startup graph's Containers cannot be deleted while foreground commits advance.
Newly published Containers retain the existing writer proofs. The initial
requirements consume RAM proportional to the selected graph's unique DATA
identities, shrink as scrub advances and are discarded on completion or exit.
ADR 0092 adds durable round progress: each start reconstructs these requirements
from the current graph, then reconciles prior full checks with present Container
envelopes and independent Base availability. The journal never replaces the
selected Metadata graph or grants current payload/deletion evidence.

Missing or damaged DATA may therefore be discovered after the mount becomes
available. Demand reads always verify content and dependencies; scrub failure
latches new writes closed and keeps GC disabled as in ADR 0090. No automatic
rollback or repair of DATA occurs. Offline scrub and Metadata-loss restoration
retain full validation. Physical formats and write ordering are unchanged.

The explicit structural-start API remains available. This supersedes only the
normal-start Container preflight requirement in ADRs 0023, 0036 and 0090; their
remaining writer, recovery-copy and background-scrub rules remain in force.
