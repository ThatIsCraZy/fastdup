# DATA read-path audit during SMB ingest

## Live observations

Read-only measurements on `10.1.1.161` found `fastdup-0.7.4-8.el10.x86_64`,
repository PID 151309. This is newer than the build in the preceding Metadata
diagnosis. This investigation did not restart services, install packages, drop
caches, or change the running backup. Existing unrelated workspace changes
were preserved.

Two 20-second samples combine guest diskstats, per-thread I/O, management
telemetry and nonintrusive syscall snapshots. The sampler and raw results are
under `.artifacts/tmp/data-read-diagnosis-20260913/`.

| Measurement | Sample 1 | Sample 2 |
| --- | ---: | ---: |
| DATA device read MB/s | 17.56 | 24.85 |
| DATA device read IOPS | 186.0 | 252.7 |
| DATA device write MB/s | 2.48 | 1.56 |
| Metadata device read MB/s | 1.77 | 1.46 |
| Ingest threads, process read MB/s | 10.68 | 19.46 |
| Tokio workers, process read MB/s | 6.15 | 4.87 |
| Recovery-scrub thread, process read MB/s | 2.45 | 1.93 |

Thread counters cover all devices, so they cannot be summed as DATA-only
attribution. Syscall snapshots directly observed DATA `.fdc` reads in all three
reader groups. The Exact publisher contributed zero process read bytes in
these intervals. No frontend reads were reported at either interval endpoint;
frontend throughput is instantaneous telemetry, not an integrated byte counter.

Similarity queries, candidate attempts and Base reads were all zero. The
startup scrub was running and had verified roughly 2,900 of 8,256 Containers.
GC telemetry was unavailable (`null`), not a measured zero. The current runtime
gates Online GC behind startup scrub completion.

The unified cache retained about 10.2–11.1 GB with 7.9–9.4 GB available RAM.
The exposed verified-DATA and Container-descriptor views reported no evictions.
This does not prove every encoded range remained resident, but it does not
support blaming these samples on exhausted RAM. Ingest's Exact verification
is the principal identified call path: the checkpoint phase telemetry also
showed approximately 2.1 seconds in Exact lookup versus 7 ms in Container
publication in one observed checkpoint. These observations do not establish
that every measured DATA byte is avoidable.

## Confirmed defects, corrected locally

1. **Ingest bypassed the verified DATA view.**
   `IndexedManifestReaders::verified_location` used uncached Location lookup;
   its recent overlay directly loaded the candidate Record. It discarded the
   decoded, verified sibling results instead of admitting them for later reuse.
   Both paths now use the same `VerifiedReadCache` as demand and Base readers.
   Full physical candidate matching and current Exact/overlay eligibility remain
   mandatory. This adds neither another resident map nor a replacement policy.

2. **Online Commit verification had the same missing connection.**
   `graph_verifier` constructed an `IndexedRequiredChunkVerifier` without the
   shared DATA view. It now supplies that view explicitly. The verifier still
   pins its Exact generation, checks every required identity and length, and
   performs one authoritative fallback scan for unresolved identities. The
   default constructor retains its previous behavior for callers without a
   cache, and Independent intent never accepts cache evidence.

3. **Independent I/O fetched storage heads twice per operation.**
   Bounded reads obtained the length during opening and again before reading;
   whole-object reads repeated the same pattern. A 4-KiB payload therefore
   caused 20 KiB of actual Direct-I/O reads. The corrected operations obtain
   one fresh 8-KiB head pair and read the payload using that exact layout:
   12 KiB total. Every subsequent Independent operation still checks fresh
   storage. The length check, alignment and one-MiB bounded-read limit remain.

The red/green tests exercise these real call paths:

| Regression | Before | After |
| --- | ---: | ---: |
| Ingest verifies a second Chunk already decoded in the preceding Record | 1 additional DATA backend operation | 0 |
| Online graph proof after the same verified Record is resident | 1 additional DATA backend operation | 0 |
| Independent 4-KiB range/object read | 20,480 bytes read | 12,288 bytes read |

The ingest test uses two different 32-KiB Chunks in one compressed Record,
separate Metadata and DATA adapters, and the real `IndexedManifestReaders`
policy. It also covers recent-overlay reuse, rejects a RETIRING candidate and
a different physical Location with the same logical identity, and requires
real backend reads under Independent intent for both Location and graph checks.
The byte-count test uses the actual filesystem Direct-I/O adapter, not just
logical request counters.

## Other inspected paths and remaining amplification

| Path | Assessment |
| --- | --- |
| Cold Exact hit in an old Container | The index is acceleration, so the selected Record must be verified when no matching live proof or cached physical-source evidence exists. The fix retains all verified independent siblings for reuse. A physical write rate much smaller than the read rate can accompany successful deduplication and is not sufficient evidence of a bug. |
| Demand reads and Base resolution | Existing common-cache lookup, Record grouping and singleflight remain. Similarity's adaptive cold-Base gate was inactive in both samples. One independent Base may still be required for a dependent Target. |
| Dependent Target reuse | The current cached payload's independent-source matcher deliberately rejects dependent Target Locations. Resident logical bytes alone cannot establish the Target's physical identity. Extending the carried source evidence would remove further revalidation; this change does not weaken that contract. |
| Candidate ordering | A cold eligible candidate can be attempted before a later candidate whose exact source is cached. Ranking eligible matching cache hits ahead of cold alternatives is a further optimization, subject to current-transition and bounded-retry rules. It is not quantified by these samples. |
| Missing Exact hint | The existing fallback scans envelopes and compact Recovery Indexes, then only selected Records; it no longer scans unrelated full payloads. Hints can still require discovery I/O. Missing-hint graph fallback currently receives no verified-DATA cache argument; the new cache connection covers the indexed path. |
| Owned Container publication | ADR 0059 still requires Header/midpoint/Footer storage samples. The io_uring thread read only about 0.04–0.05 MB/s in these intervals. Eliminating samples would require an explicit publication-contract change; they do not explain the measured bulk read rate. |
| Metadata publication and Recovery Checkpoint copies | New Metadata objects still have publication readback. Recovery Checkpoint publication on DATA audits the complete newly written copy and updates/reads selector heads. No sampled syscall identified a checkpoint-copy file as the dominant reader. Carrying checked writer evidence through these publication paths is a remaining opportunity with separate durability/fault gates. |
| Online GC | Graph/projection/source reads may use the common cache. Current relocation reads complete Container images, so it can read dead payload as well as survivors. Final victim verification and deletion/liveness fences retain their independent contract. Reading only relocation Records requires pairing that change with the final proof protocol. |
| Startup/background scrub | About 2 MB/s of process reads in the samples, with actual DATA reads observed. Scrub must bypass cached content to detect damage on the medium. Existing 256-KiB portions, foreground-aware pacing and idle I/O priority remain; the duplicate-head fix reduces overhead without faking scrub progress. |
| Full recovery, offline scrub and index rebuild | Intentionally independent, complete verification. They cannot be made zero-read by returning cached writer bytes. |
| Kernel metadata and device behavior | `O_DIRECT` excludes Linux file-content retention; it does not remove XFS directory/inode reads, block rounding, or device-level effects. Guest device counts are not host physical-media attribution. |

The remaining rows are an audit inventory, not a claim that their additional
optimizations have been implemented. In particular, this change does not make
all DATA activity during a backup zero. Direct backend cause attribution is
currently much richer for Metadata than for DATA; thread/sample attribution
cannot supply an exact per-purpose DATA byte ledger.

## Qualification and deployment boundary

Local gates passed: 156 Store tests (including independent warm-cache checks,
Record singleflight, fallback, Exact Locations and filesystem races), 56
Appliance unit tests, 23 Testkit recovery/activation/cache tests, and one format
test for cached physical-source coordinates. Fifteen explicitly manual
benchmarks were ignored. Store and Appliance library Clippy pass with
`-D warnings`. Raw red/green and suite logs remain beside the live samples.

Reproduce using `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`:

```sh
cargo test -p fastdup-store --lib --test record_read_singleflight --test exact_index_location --test generation_repository --test prefix_recovery_index --test storage_io_range
cargo test -p fastdup-appliance --lib
cargo test -p fastdup-testkit --test recovery_checkpoint_faults --test verified_read_cache --test exact_index_repository_faults
cargo test -p fastdup-format --test owned_record_read
cargo clippy -p fastdup-store -p fastdup-appliance --lib -- -D warnings
```

These corrections change no durable byte format. They were not installed on
the VM by this investigation. A comparable backup interval after installation
is required before claiming a measured reduction from the 17.6–24.9 MB/s live
DATA baseline. Local zero-read reuse tests do not establish that live reduction.
