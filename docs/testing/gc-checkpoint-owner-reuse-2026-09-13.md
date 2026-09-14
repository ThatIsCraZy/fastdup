# Avoid repeated online GC recovery and unchanged checkpoint work

## Change

Ordinary maintenance selects Exact through `pin_online_generation`. It holds
serialization against activation publication, and reuses a pinned generation
only when its complete activation record matches the writer's synchronized
snapshot. Missing or revoked evidence reconstructs storage. Explicit Independent
intent always reconstructs. Candidate proof, refresh, retirement binding checks
and ordinary GC finalization share this selection boundary. Public startup GC
recovery remains independent; final victim verification and deletion barriers
are unchanged. Cold selected pages can still need reads.

Recovery Checkpoint owners share one mutex-protected successful-publication
receipt across clones. Full Commit Record equality allows `publish_committed`
to skip graph enumeration and destination I/O after normal source candidate
selection. A changed Commit performs publication. The receipt is taken before
fallible work and restored only on successful reuse or publication; fresh owners
have none. Explicit verified publication, recovery and scrub revoke it before
independent work. The receipt is constant-sized owner state, not another content
cache. Format and synchronization ordering are unchanged.

## Qualification

Build and tests delegated to GPT 5.6 Luna with xhigh reasoning as requested.
Focused regression checks cover zero destination I/O on an unchanged Commit,
new Commit publication, zero Exact reads with matching writer evidence and
fresh recovery reads after revoking that evidence. Existing checkpoint crash,
generation crash and maintenance suites remain part of qualification.
Detailed logs are under `.artifacts/tmp/gc-checkpoint-*.log`.

Results: Store library 145 passed / 13 ignored; Recovery Checkpoint suite 14
passed; end-to-end maintenance 59 passed; Generation fault suite 27 passed /
one ignored. The unchanged-Commit regression directly measures zero destination
StorageIo operations; the source still performs candidate validation. The
Exact regression measures zero physical read bytes for a matching snapshot,
then positive physical reads after revocation. Additional checkpoint regressions
cover a new Commit, scrub revocation and retry after corrupt independent recovery.

The appliance debug build and Clippy on production libraries/binaries succeeded.
A broader Clippy run including existing tests reports unrelated test-code lint
violations; those were not changed. The initial overbroad test build exhausted
local disk space; reproducible target artifacts were removed with a manifest at
`.artifacts/tmp/gc-checkpoint-target-cleanup.txt`, then scoped builds succeeded.

No VM deployment or live post-change throughput measurement was performed in
this task. This removes the two diagnosed repeated-work paths, not all Metadata
reads or every source candidate-validation read.
