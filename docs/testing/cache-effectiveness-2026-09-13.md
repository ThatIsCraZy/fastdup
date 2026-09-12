# Verification-only cache retention and Metadata write amplification

## Live baseline

Two read-only 20-second measurements on `10.1.1.161` observed
`fastdup-0.7.4-9.el10.x86_64`, Repository PID 156542. Thus the preceding
DATA-cache wiring corrections were already installed. This investigation
neither installed a package nor restarted the Repository or the running SMB
backup. Raw samples and qualification logs are in
`.artifacts/tmp/cache-pressure-20260913/`.

| Guest device measurement | Sample 1 | Sample 2 |
| --- | ---: | ---: |
| DATA reads, MB/s | 40.02 | 26.20 |
| DATA writes, MB/s | 1.66 | 1.32 |
| Metadata reads, MB/s | 2.51 | 2.88 |
| Metadata writes, MB/s | 3.38 | 3.47 |
| Metadata write IOPS | 546 | 593 |

Ingest threads again dominated process reads and syscall snapshots directly
observed DATA Container paths. The Exact publisher read zero process bytes;
the previous Exact-WAL correction did not regress in these samples. Startup
scrub was complete. The last reported GC cycle had no profitable candidates
and was unchanged across the intervals; this is not an exact live per-purpose
I/O ledger. Similarity counters remained zero, and frontend reads were zero
at the sample endpoints. Frontend writes were active.

The unified cache grew from 7.98 to 10.07 GB in sample 1 and from 11.37 to
12.75 GB in sample 2. Verified payload representations alone grew from
4.48 to 5.55 GB in sample 1, with 10,600 additional cache decompressions.
Process Swap was zero. Host RAM and cached payload residency are different
measurements; the UI's steep RAM curve cannot by itself identify a memory leak
or distinguish allocator retention from resident cache owners.

## Confirmed causes and corrections

### Verification retained the wrong representation

The previous fix connected ingest and Commit checks to the payload cache,
but those callers only need evidence that an eligible exact physical Location
has already passed verification. They retained and compressed full Record
siblings, alongside lower-level encoded storage ranges. A warm check could
decompress a payload merely to compare its source coordinates. Once payloads
were evicted, the same verification required storage again.

The common cache now retains compact physical-source evidence independently
of payload backing. `VerifiedChunkPayload::verified_location` extracts the
existing opaque independent Location capability without retaining bytes.
Verification-only misses load DATA payloads under Scan intent, which allows
existing hits and coalescing but declines payload-range admission. Exact pages
and compact Container descriptors retain their ordinary admission policy;
suppressing index admission together with DATA would cause Metadata rereads.
After successful complete
verification, the caller's restored intent controls compact evidence admission.
Independent Record siblings contribute their checked Locations; a dependent
candidate is admitted only after its selected Record and Base verification
succeed. Demand payload reads retain their existing payload policy and also
contribute checked independent evidence.

The new `LocationProof` namespace uses the same global directory, allocation
ledger, pressure limit and replacement mechanism. It has no independent map,
budget or eviction list. It stores complete verified candidate coordinates,
not just a logical content hash. Reuse still checks the newest transition of
that physical Location against the currently pinned Exact generation. An
eligible warm Location is considered before cold alternatives; the ordinary
two-attempt backend bound remains. Evidence does not select a generation,
retain liveness or return file bytes.

The real `IndexedManifestReaders` regression was first red: one verification
retained 1,750 bytes of compressed DATA for two highly compressible 32-KiB
Chunks. It now retains zero DATA payload bytes, performs zero cache compression
attempts, and keeps two Location entries totaling less than 2 KiB. The next
sibling, recent-overlay reuse and the online Commit graph require zero
additional DATA backend operations. This fixture deliberately verifies reuse
without relying on retained payload bytes.

The same test also covers a later warm candidate behind a cold alternative,
different physical coordinates with the same Chunk ID, pressure eviction,
newer RETIRING transitions, Independent bypass, and injected physical corruption
under warm evidence. Independent Location and graph verification reject the
corruption. Pass-local sibling grouping still avoids repeated Record reads
when Independent/Scan intent or pressure prevents evidence admission. The
format regression pairs extracted evidence with every original
RAW/Zstd candidate coordinate.

Runtime telemetry exposes `locationProofs` hits, misses, evictions and charged
bytes. The Cache panel shows them as part of the common allocation, without
adding their bytes to the total a second time. These proof counters are labeled
as lifetime observations; no five-minute proof window is fabricated.

### Small Metadata writes multiplied length-head updates

`stage_metadata_with_status` split an already encoded immutable Metadata object
into 4-KiB writes. Every growing write advanced a 4-KiB storage length head and
synchronized that body/head pair. Its Independent scope also required fresh
length-head reads during the repeated operations. A large object therefore
generated one body write and one header write per page, plus repeated barriers.

Publication now uses at most one-MiB chunks, matching the bounded backend I/O
quantum. A 520,192-byte encoded Manifest fixture previously made 256 physical
write-helper calls: two initial heads and 127 body/head pairs. It now makes
exactly four: two initial heads, one complete body and one new length head.
Thus growing-head synchronizations drop from 127 to one for that fixture.
The red and green tests use the real filesystem Direct-I/O adapter.

There is no durable-format or alignment change. The independent staging
readback, file sync, no-replace publication and directory-before-Commit-WAL
ordering remain. The generation and checkpoint fault matrices validate
old-or-complete-new recovery at the publication boundaries. This correction
reduces head writes and synchronization frequency; it does not remove logical
Metadata output or all XFS journal/inode traffic.

## Qualification and limits

Passed locally: 157 Store tests, 56 Appliance unit tests, 45 Testkit
Container/generation/checkpoint/cache tests, two Format tests, and 14 existing
UI telemetry tests. Sixteen explicitly manual benchmark/lifetime gates were
ignored. Store and Appliance library Clippy passed with `-D warnings`;
UI TypeScript checking passed. Build/test artifacts remained workspace-local.

Reproduce Cargo commands with
`CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`:

```sh
cargo test -p fastdup-store --lib --test record_read_singleflight --test exact_index_location --test generation_repository --test prefix_recovery_index --test storage_io_range
cargo test -p fastdup-appliance --lib
cargo test -p fastdup-testkit --test generation_repository_faults --test recovery_checkpoint_faults --test container_repository_faults --test verified_read_cache
cargo test -p fastdup-format --lib compact_record_provenance_keeps_every_candidate_coordinate
cargo test -p fastdup-format --test owned_record_read
cargo clippy -p fastdup-store -p fastdup-appliance --lib -- -D warnings
```

These fixes were not deployed by this investigation. No reduction of the live
26–40 MB/s DATA baseline is claimed until a comparable backup interval runs
with the new implementation. Cold Locations without retained evidence still
need verification, and evidence is evictable. Demand reads, partial-write
reconstruction, independent scrub and recovery still read DATA when required.
The previous audit's checkpoint-copy readbacks, missing-hint fallback and GC
relocation opportunities remain separate from these two confirmed defects.
