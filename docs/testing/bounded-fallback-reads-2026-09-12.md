# Bounded fallback reads and disk IOPS — 2026-09-12

## Reproducer and change

A fresh two-Container fixture contains unrelated 256-KiB RAW payloads and one
requested 8-KiB payload. One index-free demand read and one required-Chunk
verification previously caused four complete `StorageIo::read` calls. The
regression failed with `left: 4, right: 0` before the change. The updated real
Store path makes no whole-object reads and requests 50,560 bytes through bounded
reads, including both discovery passes. This is an API-byte/operation result,
not a physical-disk throughput benchmark; filesystem caching and readahead can
change guest block I/O.

The indexed commit verifier also used to abandon remaining Exact hints after
one miss and recheck every required identity. It now continues valid hints and
falls back once for only the unresolved identities. The regression with valid
hints on either side of one missing hint checks that each of three selected
Records is read once. Shared compressed-record siblings likewise require only
one decode per proof pass. Base resolver retention ends at each Container.

The selected Record is independently checked, including decoded Chunk hashes.
Tests inject corruption into the target, its independent Base, and the local
Recovery Index. Each must fail demand/dependency verification and full scrub.
A corrupt unrelated payload does not block an independently valid Chunk read,
but still fails full scrub. No unchecked content proof or new durable field is
introduced. Existing tests cover verification on the next pass, retiring
Locations, Manifest reads, concurrent Record misses and generation publication.

## VM observations

Read-only inspection of test VM 10.1.1.161 found mount owner PID 1142 active.
A five-minute journal sample contained 12 reports that a checkpoint exceeded
five seconds and closed mutation admission. Comparison Base reads also continue
to accumulate. Thus write gaps have an explicit backpressure mechanism; this
sample does **not** prove that whole-Container fallback caused those checkpoints.
No profiler was attached, no service was restarted, and this change was not
installed on the VM. End-to-end Veeam throughput still requires activation and
a comparable job. A sparse POSIX MB/s chart alone cannot identify DATA reads.

## UI

The backend already derives `readIops` and `writeIops` from completed device
operations divided by elapsed sample time. Disk activity and the initialized
Storage overview now display both next to MB/s, ordered Read / Write. Missing
historic fields remain unknown (`—`); zero is a measured value. The disk table
explains that rates cover an interval, whereas Outstanding I/O is an instantaneous
sample and can be zero between completed requests.

## Validation

Commands run with `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`, plus Cargo's explicit `env.TMPDIR.value`
override. Build, test and browser artifacts are under
`.artifacts/read-amplification-20260912/`.

- Store: 31 passing tests (3 manual benchmarks ignored): prefix_recovery_index, manifest_reader, exact_index_location,
  generation_repository, record_read_singleflight.
- UI: 46 passing Vitest tests, TypeScript/Vite production build, desktop/mobile
  Chromium review with numeric IOPS visible and no document overflow.
- Clippy: format/store libraries with warnings denied.

Full scans remain intentional for complete scrub and Container verification.
Without a usable Exact index, discovery still costs a namespace/compact-index
scan; this change reduces payload amplification, not directory cardinality.
