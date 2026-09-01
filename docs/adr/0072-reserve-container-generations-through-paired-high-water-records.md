---
status: accepted
---

# Reserve Container generations through paired high-water records

The DATA root stores two fixed 4-KiB Container Generation High-Water slots,
`container-generation.wal` and `container-generation.1.wal`. Each checksummed
record carries a monotonic sequence, the exact hash of its predecessor, and the
greatest generation durably reserved. A writer alternates slots, rereads the
complete intended record, and synchronizes the target before returning any
generation from the new 1,024-generation range. A crash may waste the unused
suffix, but recovery starts strictly above it and never reuses an acknowledged
or ambiguous generation.

The slots are created, file-synchronized, and directory-synchronized before
their first record, and only while the DATA repository contains no published
Container. Absent or empty slots beside any published Container fail closed
before creating or changing allocator files. Once either slot contains a
record, both names must exist. A healthy
pair contains adjacent sequences, an exact hash link, and a nondecreasing
high-water. One initial record with an empty peer is valid; any other malformed,
forked, or decreasing pair fails closed.
Offline Scrub completely verifies Containers and requires the selected durable
reservation to cover their greatest embedded generation. Range extension and
Scrub serialize through one process-local allocator barrier shared with the
maintenance adapter.

There is no migration from a Container population without allocator slots.
Container Envelope Proof scanning remains a diagnostic and rebuild primitive,
not writable-startup authority. Healthy reopen reads the fixed records without
a Container directory listing.
The allocation fast path retains one process-local mutex per Container and
performs extra DATA-root I/O only when a 1,024-generation range is exhausted;
no work enters the POSIX mutation or Ingest-Lane admission hot loops.
