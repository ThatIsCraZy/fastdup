# Online scrub concurrency

The online coordinator previously reconciled saved envelopes on a dedicated
32-worker pool, but verified each new Container serially. The full verifier now
uses that same pool for missing or changed certificates. Envelope and payload
batches never run concurrently with each other. Each worker performs one
blocking StorageIo call at a time; asynchronous dispatch is worker-backed, not
a switch to a different kernel I/O API.

Input batches are capped at 32 Containers. Validated file lengths further split
payload work into groups containing at most 64 MiB of primary Container images;
decoder, dependency and certificate state are additional bounded verifier work.
Frontend activity reduces the next batch to one. Existing Independent reads,
256-KiB portions, idle I/O priority and per-worker duty delays remain.

All submitted tasks join on success, failure and cancellation. Integrity errors
take precedence over an interrupted peer. Only a complete successful verification
batch merges current graph coverage; the coordinator then persists certificates
and records progress serially. The existing completion boundary still controls
the GC gate. Failed work cannot create a successful progress certificate.

GPT 5.6 Luna xhigh validated the change. A blocking test backend observed exactly
32 simultaneous payload reads, rejects a 33-entry batch before any read, and
confirms zero outstanding reads on return. A mixed corruption/cancellation test
preserves the corruption error and leaves coverage incomplete. Structural
recovery passed 35 tests, runtime scrub passed 5, and the complete Store test
run passed (library: 147 passed, 13 ignored). Release build and production
Clippy passed after extracting coordinator helpers. Logs are saved under
`.artifacts/scrub-async-*.log`; `*-final.log` records the final runtime, build
and production-Clippy qualification. This change is not deployed by this task.
