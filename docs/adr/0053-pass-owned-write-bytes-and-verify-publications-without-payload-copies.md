---
status: accepted
---

# Pass owned write bytes and verify publications without payload copies

The ingest path transfers the FUSE session's owned write buffer into one
`MutationPayload`, keeps FastCDC Chunks as immutable fragments through hashing
and Exact lookup, and coalesces only Chunks that must be newly encoded. The
adaptive Container writer serializes selected RAW or Zstd records directly into
their final image ranges. Mandatory publication verification returns complete
Location and metric evidence without retaining decoded Chunk payload copies.

This requires a narrow local extension to `fuse3`: the session calls an owned
write method whose default forwards to the existing borrowed method. The
fastdup adapter overrides it and adopts the `Vec<u8>`. This fork is preferable
to a second full copy on every SMB write, but it must stay source-compatible
with the pinned upstream release and retain the borrowed fallback.

CRC32C checksums with embedded checksum fields use incremental prefix, four
zero bytes, and suffix updates. They never clone the complete durable object.
Publication still rereads and verifies the complete Container before file sync,
rename, and root sync. Payload-free evidence is not a read cache and cannot
return file bytes; ordinary reads, recovery, and scrub keep their independent
verification paths.

Five process-local byte counters measure remaining avoidable copies:
checksum scratch, publication-verify materialization, FUSE request adaptation,
Container assembly, and new-Chunk fragment coalescing. Each counter occupies a
separate 64-byte cache line. Counter saturation loses telemetry only and cannot
change data, admission, durability, or recovery decisions.
