# Remaining read and write amplification during SMB backup

## Live evidence on build 0.7.4-10

Read-only measurements on `10.1.1.161`, repository PID 162330, observed
`fastdup-0.7.4-10.el10.x86_64`. No installation, restart, cache dropping,
ptrace stop, or repository mutation was performed on the VM. Local fixes in
this report are not deployed by this investigation.

The first two 20-second samples captured ingest. The third captured a later
Recovery Checkpoint, so it is a different workload phase, not a before/after
performance comparison.

| Guest device rate, decimal MB/s | Ingest 1 | Ingest 2 | Checkpoint phase |
| --- | ---: | ---: | ---: |
| DATA reads | 51.90 | 77.47 | 3.43 |
| DATA writes | 3.46 | 3.26 | 7.40 |
| Metadata reads | 1.11 | 1.58 | 0.02 |
| Metadata writes | 3.57 | 2.37 | 0.00 |

In ingest sample 1, ingest threads account for about 45 MB/s of process reads.
The Exact publisher writes 1.01 MB/s in sample 1 and 0.56 MB/s in sample 2,
with zero process read bytes in both. These per-thread counters span devices;
they do not include XFS journal writes attributed to kernel threads.

The common cache grows from 1.21 to 1.33 GB in the first interval and from
1.49 to 1.61 GB in the second. Compact Location-proof evictions remain zero;
proof misses rise by 11,757 and 12,777 respectively. This evidence does not
support blaming these intervals on Location-proof eviction. Startup scrub
coverage was complete, and frontend read throughput was zero at the sampled
endpoints. Those endpoint gauges are not integrated request counters.

All 56 DATA Containers observed in syscall snapshots from the ingest samples
have mtimes between 21:13:03 and 22:51:20 UTC, before the current process start
at 23:21:10 UTC. Together with proof misses and zero evictions, this points to
first verification of old DATA Locations after restart. It is sampled evidence,
not a complete per-purpose byte ledger. A resumed scrub certificate or Exact
hint cannot be promoted to a physical Location proof: neither proves the
selected Record payload in this process. ADRs 0090/0091 intentionally do not
read all DATA before mount. This change does not remove that cold verification
contract or claim that all ingest reads have been eliminated.

In sample 3, one Tokio worker makes 16,481 write syscalls and writes about
99.8 MB in 20 seconds. Snapshots identify
`.recovery-checkpoint.00000000000002a0.fdrc.building` during `pwrite64`,
`pread64` and `fdatasync`. DATA sees about 1,219 write IOPS. Another Online-GC
thread reads about 35.3 MB, with snapshots on distinct DATA Containers.
No Metadata write load was observed during this particular interval.

Raw samples, the extended syscall sampler and the Container mtime check are
under `.artifacts/tmp/remaining-reads-20260913/`.

## Confirmed defects and correction

The aligned storage envelope advances a length head and synchronizes each
file extension. Exact publication, both compaction paths, and Similarity
partition output still extended their temporary files once per 4-KiB page.
The final file sync could not undo the thousands of preceding body/head
writes and synchronization calls. Ordinary Metadata publication had already
been batched, but these writers did not use that mechanism.

Recovery Checkpoints added a second defect: their entry headers and object
payloads are not all sector-aligned. Writing each field and each 4-KiB payload
piece independently forced the Direct-I/O adapter to read existing edge
sectors just to preserve bytes. Those reads were caused by writing, not by a
read-cache miss. Increasing only the inner payload chunk size would still
leave entry-boundary read-modify-write operations.

`immutable_write.rs` now supplies the common whole-image and sequential writer
mechanism. Immutable Metadata, Exact Runs/Run Sets, both Exact compaction
paths, Similarity partitions and Recovery Checkpoints use it. Streamed output
holds at most one MiB of additional temporary writer workspace; it never
retains data after publication and does not add a cache or eviction policy.
The checkpoint stream begins at zero, crosses object boundaries in aligned
batches and patches its fixed aligned Header once the body hash is known.
All required audits, collision handling, file/directory syncs and final
selector/WAL ordering remain. No durable bytes or format epoch change.

## Deterministic reproduction

Filesystem tests count calls to the physical Direct-I/O writer and reads of
existing sectors performed inside that writer. These are operation counts at
the helper boundary, not device IOPS or timing benchmarks.

| Real publication path | Encoded bytes | Before physical writes | After physical writes |
| --- | ---: | ---: | ---: |
| Exact Run | 2,125,824 | 1,040 | 8 |
| Streamed single-Run compaction | 2,125,824 | same per-page algorithm | 8 |
| Streamed family compaction | 2,125,824 | same per-page algorithm | 8 |
| Similarity partition | 2,105,344 | 1,030 | 8 |
| Recovery Checkpoint | 2,374,144 | 1,246 | 15 |

The checkpoint's write-induced edge reads fall from **616 to zero**. Its full
publication audit is retained and still reads the resulting checkpoint image.
Index and Similarity tests compare complete independently read output against
the canonical format encoder; both compaction variants produce the same Run
bytes as ordinary publication. Test sizes cross multiple one-MiB flushes.

The red runs are `batches-red.log`, `checkpoint-red.log`,
`checkpoint-rmw-red.log`, and `similarity-red.log`. The checkpoint edge-read
regression was also run against the original publication implementation with
the same unique large Manifest fixture, then the fixed implementation was
restored. Early fixture/compiler corrections are not counted as reproductions.
`batches-green.log` contains the corrected counts.

## Qualification

All **209 tests** in the qualification commands pass: 155 Store tests and 54
Testkit tests, including Exact activation, online Similarity, generation faults
and checkpoint recovery. Fourteen explicitly ignored manual tests were not run.
Store library Clippy passes with `-D warnings`. A broader initial `--tests`
Clippy attempt encountered pre-existing literal-format warnings in unrelated
`candidate_read_gate.rs`, `seqcdc.rs` and `similarity_simd.rs` tests; the qualified
Clippy command below covers the library.
Raw final logs are `store-tests.log`, `faults-final.log` and `clippy-lib.log`.

The new multi-batch checkpoint fault test interrupts every body batch, the Header patch
and selector write, both before and after the operation. Crash must select no
partial checkpoint; retry must publish and recover the complete original
Namespace. Existing fault matrices also cover synchronization, no-replace
publication, activation, compaction, WAL rotation and Metadata installation.

Reproduce with `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`:

```sh
cargo test -p fastdup-store --lib --test exact_index_repository --test exact_index_activation --test generation_repository
cargo test -p fastdup-testkit --test exact_index_repository_faults --test recovery_checkpoint_faults --test generation_repository_faults --test online_similarity
cargo clippy -p fastdup-store --lib -- -D warnings
```

## Remaining limits and primary sources

These fixes target demonstrated amplification. They do not establish the
fraction of the screenshot's 8 MB/s Metadata writes that the new writer removes;
that requires a comparable backup interval on the updated VM. Cold Exact
verification, independent scrub, GC reads, complete checkpoint-copy audits,
WAL commits and XFS metadata/journal operations remain distinct sources of I/O.
The historical-proof cache's eviction counter also includes explicit
remove-before-reinsert operations; it must not be treated as proof of memory
pressure without further attribution.

Linux documents that `O_DIRECT` alone does not provide the persistence
semantics of synchronized I/O: [open(2)](https://man7.org/linux/man-pages/man2/open.2.html).
XFS batches journal transactions until buffer pressure or a synchronous
operation forces them: [XFS logging design](https://www.kernel.org/doc/html/v6.7/filesystems/xfs-delayed-logging-design.html).
Those contracts support batching unpublished output before the existing
persistence boundaries. They do not justify removing required syncs, trusting
an unverified cold Location, or reporting logical syscall bytes as physical
media traffic.
