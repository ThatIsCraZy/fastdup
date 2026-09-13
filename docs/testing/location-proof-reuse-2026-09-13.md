# DATA reads and physical Location evidence

## Live observation

Read-only samples on `10.1.1.161` found `fastdup-0.7.4-11.el10.x86_64`,
repository PID 167516. The process started at approximately 23:52:59 UTC on
12 September. This investigation did not install, restart, stop, trace with
ptrace, or mutate the running repository.

| Guest device rate, decimal MB/s | 20-second sample | 30-second offset sample |
| --- | ---: | ---: |
| DATA read | 77.37 | 69.80 |
| DATA write | 1.43 | 2.38 |
| Metadata read | 1.35 | 1.73 |
| Metadata write | 1.84 | 2.23 |
| Ingest-thread reads, across devices | 64.48 | 61.33 |

The intervals ended at 00:00:58 and 00:02:24 UTC on 13 September. They confirm
substantial ingest reads but are not measurements of the screenshot's exact
146 MB/s interval or a before/after comparison of these local fixes.

The offset sampler polls `/proc/<pid>/task/*/syscall` every 20 ms. It records
observed changes in `pread64` arguments, with thread, file, offset and length;
it neither intercepts every call nor infers completions from a repeated sample.
Of 1,801 observed Container reads larger than 4 KiB, 1,800 have distinct
file/offset/length tuples. The repeated tuple was observed in ingest and a
Tokio worker about 7.5 seconds apart. Short calls can be missed, and a continuous
sequence of identical syscall arguments can appear as one observation. These
numbers are sampled evidence, not an exhaustive duplicate-read percentage.

Of 861 sampled Container paths, 856 predate process start; they account for
1,796 of the 1,801 payload observations. Location-proof evictions stay at zero
in both intervals. In the second interval, proof hits increase by 31,981 and
misses by 17,965. This supports predominantly first verification of older
Locations during ingest, rather than pressure eviction of current proofs.
It does not establish that every cold read is unavoidable.

The startup scrub reports 17,478 Containers covered, including 13,004 resumed
checks and 4,474 fresh full checks, with 5,135,790,080 read bytes. Its fresh
Location evidence was discarded even though the full verifier had already
produced it. Resumed checks deliberately carry no current payload evidence.

Artifacts and the offset sampler are under
`.artifacts/tmp/data-reads-next-20260913/` (`live-1.json`, `live-offsets.json`,
`sample-offsets.py`). The earlier write-batching findings remain documented in
[the preceding I/O report](remaining-io-amplification-2026-09-13.md).

## Two reproduced defects

The compact proof view previously keyed its resident by logical Chunk ID and
length. The common immutable directory correctly refuses to overwrite an
existing identity, but two physical Locations of one Chunk shared that key.
After replacement or retirement, the old resident prevented admission of the
newly verified Location. Every later request could reread the replacement,
even with ample memory and zero cache evictions.

Proof keys now bind the complete logical identity and physical coordinates;
hits also compare the full entry. Both verified copies can coexist in the same
cache owner. Candidate selection checks the newest transition of each physical
Location and searches eligible warm evidence before bounded cold fallback.
That fallback reuses the already obtained Exact lookup result. No private map,
quota, retained payload or new durable format is introduced. Location hit/miss
telemetry now counts candidate-proof probes, so raw counts across the change
must not be interpreted as identical logical-request rates.
Commit graph verification shares that lookup too. A targeted review regression
(`graph-red.log`) caught duplicate uncached Exact reads in the intermediate
implementation; the final path performs one lookup for proof probing and
payload verification together, including under Independent intent.

The full scrub path now keeps the verifier's typed Location result long enough
to offer it to the existing online cache. Every complete Container check,
including dependent Bases, runs under Independent intent. Only after this
scope finishes can the online caller admit compact evidence; its own
Scan/Independent intent or memory pressure still prevents admission. A failed
full check produces no handoff. Historical certificate replay never takes this
path. Physical scrub still detects damage with warm evidence. Current Exact
selection and GC authorization remain mandatory.

| Deterministic scenario | Before | After |
| --- | ---: | ---: |
| Three repeated replacement reuses after retirement | 3 extra DATA adapter reads | 0 |
| Three alternating reuses of two checked copies | 3 extra DATA adapter reads | 0 |
| First ingest reuse after a full current-process scrub | 4 DATA adapter operations | 0 |

The last row includes descriptor/length work and a Record read; it is not four
full payload reads. These are deterministic adapter counts, not device IOPS.
`proof-red.log` and `scrub-red.log` show failures before the corresponding fix.
The scrub reproduction initially exposed the new explicit handoff API as a
delegate to the old path, then asserted that subsequent ingest performs no
storage operations. Final behavior is exercised in `location_proof_cache.rs`.

## Qualification

All **301 tests** passed across the commands below; 19 explicitly ignored
manual tests were not run. Store/Appliance library and runtime-binary Clippy
passed with `-D warnings`. Artifacts are `store-tests.log`, `testkit-tests.log`,
`appliance-tests.log` and `clippy.log`.

The tests cover both copies and retirement, common memory pressure, caller
intent, corruption with warm evidence, failed and resumed scrub, ordinary
Manifest readers, shared concurrent Record reads, generation faults and scrub
progress crash recovery. The actual background-worker stop/restart test proves
that fresh checks seed its supplied common cache while a fully resumed round
does not seed a newly empty cache.

With `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`:

```sh
cargo test -p fastdup-store --lib --test record_read_singleflight --test exact_index_location --test manifest_reader
cargo test -p fastdup-testkit --test location_proof_cache --test generation_repository_faults
cargo test -p fastdup-appliance --lib --bin fastdup-durable-fuse --test structural_recovery
cargo clippy -p fastdup-store -p fastdup-appliance --lib --bin fastdup-durable-fuse -- -D warnings
```

## Remaining boundary

These changes are local and were not deployed by this investigation. Their
effect on the complete backup workload requires another live measurement after
an update. They do not establish a percentage reduction of the screenshot's
read rate. A fresh process has no current Location evidence for resumed old
Containers until a full check supplies it. Under ADRs 0046, 0059 and 0091/0092,
an Exact hint or historical scrub certificate alone cannot replace that check.
Payload demand after proof-only verification also still needs bytes unless the
common cache retains them. Full independent scrub, recovery and final deletion
checks retain their distinct fresh-storage obligations.
