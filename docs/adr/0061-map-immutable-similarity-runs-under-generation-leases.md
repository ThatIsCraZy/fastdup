---
status: accepted
---

# Map immutable Similarity Runs under generation leases

Filesystem-backed Similarity recovery uses a read-only memory map for every
physical Run in the selected generation. Recovery first verifies the
descriptor-authenticated file length and then performs the complete format-v2
audit directly over the mapped bytes: Header/Footer identity, page checksums,
cross-page ordering, Bucket-to-Entry relationships, and the complete Run hash.
Only a successfully audited mapping becomes queryable. Query cache misses pass
borrowed 4-KiB slices to the existing field-wise decoders.

Every mapping owns an immutable file lease. All `FsStorageIo` adapters opened
on the same canonical root share a process-wide lease registry. While any
reader holds a name, `write_at`, `set_len`, `publish_noreplace`, and
`remove_file` reject operations involving that name. The mapping is unmapped
before its lease is released. Generation reclamation must use this storage
interface and retry a lease rejection after the last reader drops.

The only Unsafe operation is `MmapOptions::map` in the dedicated
`similarity_mmap` module. Its safety argument depends on the appliance owning
the storage directory: out-of-process mutation, direct writes that bypass
`StorageIo`, and administrative truncation of live files are unsupported.
Keeping the read-only file descriptor open alone would not protect against
truncation and is not treated as a lease.

Adapters that cannot provide the immutable-file capability return no lease and
retain the bounded `read_exact_at` path. Publication and offline scrub also
retain positional reads, so the independent persistence verifier does not
share the mapped query implementation. Recovery status reports which page
source was selected and rejects a partially mapped family.

## Why

The reproducible page benchmark measured mmap plus the same decoder at 1.413
times the throughput of `read_exact_at` plus decode, a 29.2 percent latency
reduction. This is sufficient to justify the narrow Unsafe seam, provided file
lifetime and mutation are enforced rather than documented as caller folklore.

## Paired invariants and evidence

- Writer publication remains immutable, no-replace, fully audited, and synced
  before a Run or family manifest becomes selectable.
- Recovery maps the exact leased descriptor and repeats every durable and
  semantic Run invariant before exposing a query object.
- Offline scrub keeps the independent bounded-read audit.
- Fault tests corrupt entry pages before recovery and require rejection.
- Fault tests use a separately opened filesystem adapter and require write,
  truncate, replacement, and removal to fail while mapped readers live, then
  require removal to succeed only after the last reader drops.
