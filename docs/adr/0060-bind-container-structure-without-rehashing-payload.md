---
status: accepted
---

# Bind container structure without rehashing payload

Container sealing no longer computes BLAKE3 over the complete encoded image.
That pass reread every RAW or compressed payload immediately after the writer
had already hashed logical Chunks and calculated each Record CRC. On the ingest
hot path it duplicated memory-bandwidth work without adding an independent
observation of storage.

The Container structural-commitment algorithm uses identifier `2` for a
domain-separated BLAKE3 digest. It covers the Header, every Record header and
Chunk Table, the complete Recovery Index, and the Footer with its commitment
and CRC fields zeroed. It excludes encoded payload and alignment gaps.

The omitted bytes retain paired integrity checks. Record CRC32C covers each
complete stored record, readers and scrub recompute BLAKE3 for every decoded
logical Chunk, and all omitted padding must be zero. Consequently a normal read
or scrub still rejects payload corruption, a wrong decode, forged Chunk
metadata, index redirection, and nonzero padding. The structural commitment
cryptographically binds the identities, coordinates, codec parameters, Record
CRCs, and Recovery Index without a second full payload scan at the writer.

This narrows the statement in ADR 0008 that BLAKE3 protects the sealed
container: BLAKE3 protects its durable structure and content identities, while
Record CRC plus decoded Chunk BLAKE3 protect stored payload. Existing prototype
containers with algorithm identifier `1` are intentionally incompatible; the
project has not declared a stable on-disk release and existing data is
disposable.
