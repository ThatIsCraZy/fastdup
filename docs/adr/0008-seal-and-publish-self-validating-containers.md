---
status: accepted
---

# Seal and publish self-validating containers

Container format v1 uses explicit little-endian serialization, zeroed reserved
fields, fixed 4 KiB header/footer blocks, and 64-byte record alignment. A
container moves through `BUILDING`, `SEALED`, and `PUBLISHED`; only a fully
checksummed, synchronized, atomically renamed, and directory-synchronized
container may be referenced by metadata. This makes torn construction detectable
without relying on Rust memory layout or filenames.

## Consequences

The total container size is capped at 64 MiB and an individual encoded record at
1 MiB. CRC32C protects stored record headers and payloads, BLAKE3-256 identifies
decoded logical chunks, and BLAKE3-256 protects the sealed container. Recovery
ignores building files, invalid footers, unknown required flags, and nonzero
reserved fields. A random 128-bit container ID identifies the immutable object;
a separate monotonic 64-bit container generation records creation order and can
resume above the maximum generation found during rebuild.
