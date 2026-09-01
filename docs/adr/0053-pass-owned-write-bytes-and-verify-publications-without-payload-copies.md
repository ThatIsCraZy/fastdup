---
status: accepted
---

# Pass owned write bytes and verify publications without payload copies

The ingest path transfers the FUSE session's receive allocation into one
`MutationPayload`, keeps SeqCDC Chunks as immutable fragments through hashing
and Exact lookup, and coalesces only Chunks that must be newly encoded. The
adaptive Container writer serializes selected RAW or Zstd records directly into
their final image ranges. Mandatory publication verification returns complete
Location and metric evidence without retaining decoded Chunk payload copies.

This requires a narrow local extension to `fuse3`. On `FUSE_WRITE`, the session
moves the complete receive `Vec<u8>` out of its decoder, installs a fresh
receive buffer, and passes a bounded `bytes::Bytes` slice plus the allocation
size to `write_owned_request`. The fastdup adapter turns that slice directly
into a `MutationPayload`. The default method copies into the existing owned
write API, so other `Filesystem` implementations remain source-compatible.
The receive-allocation test compares pointers before and after dispatch to
prove that the fastdup path does not copy the payload.

A bounded recycler starts with two initialized replacement buffers. Moving a
write buffer into ingest therefore normally does not allocate and zero its
successor on the Tokio dispatch thread. The immutable `bytes::Bytes` owner
returns the complete allocation only after its final clone is dropped; a full
or disconnected recycler drops that surplus allocation instead. If more than
two writes retain their request backing at once, dispatch uses the original
synchronous allocation as a correctness fallback. Recycled request buffers
may contain stale suffix bytes, so dispatch requires the decoded FUSE header
length to equal the completed vectored-read length before exposing a payload.

The memory budget charges the complete receive allocation, not just the
payload slice. This matters for short writes because the immutable slice keeps
the backing allocation alive until ingest releases it.

CRC32C checksums with embedded checksum fields use incremental prefix, four
zero bytes, and suffix updates. They never clone the complete durable object.
ADR 0059 supersedes the immediate writer-image verification rule. Publication
uses evidence produced by the encoder and rereads only the Header, one aligned
midpoint block, and the Footer before file sync, rename, and root sync.
Ordinary reads, recovery, and scrub keep their independent Record, Chunk-ID,
Recovery-Index, envelope, and Container-hash verification paths.

Five process-local byte counters measure remaining avoidable copies:
checksum scratch, publication-verify materialization, FUSE request adaptation,
Container assembly, and new-Chunk fragment coalescing. Each counter occupies a
separate 64-byte cache line. Counter saturation loses telemetry only and cannot
change data, admission, durability, or recovery decisions.
