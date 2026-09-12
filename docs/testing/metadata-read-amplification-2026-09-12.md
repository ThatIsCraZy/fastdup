# Metadata read amplification during a live SMB backup

## Observation

Read-only inspection of `10.1.1.161` on 12 September 2026 found
`fastdup-0.7.4-7.el10.x86_64`, repository PID 138767, with Metadata on
`/dev/sdb1` and DATA on `/dev/sdc1`. A separate Veeam server was writing through
SMB. No service was restarted, no cache was dropped, and no package was changed.

Two approximately 20-second samples combined guest diskstats, the repository
process and thread I/O counters, the existing management telemetry, and
nonintrusive `/proc/<pid>/task/<tid>/syscall` samples. Raw artifacts and the
read-only sampler are in `.artifacts/tmp/metadata-read-diagnosis-20260912/`.

| Sample | Metadata read MB/s | Metadata read IOPS | Main reader |
| --- | ---: | ---: | --- |
| 1, ending 21:14:38 UTC | 12.147 | 1824.3 | `fastdup-exact-p` |
| 2 | 19.473 | 3423.0 | `fastdup-exact-p` |

These are guest block-device observations, not host physical-media I/O. The
first sample attributed essentially all process read bytes to the Exact
publisher. Its active reads included staged `.fdx.building` outputs and existing
`.fdx` compaction inputs. The second sample also directly observed reads from
both `exact-index.activation*.wal` files.

The common cache was active, with approximately 1.7 GB resident. Available RAM
was approximately 17.45 GB and process Swap was zero. Cache memory pressure is
not the explanation for these samples. The first sample happened during
compaction with no measured frontend write throughput; the second included
approximately 323 MB/s frontend write throughput at its final management sample.
No claim of a steady throughput average follows from that instantaneous sample.

## Identified amplification

The observed build's ordinary L0 append traverses these production paths:

1. `append_level_zero` calls `recover_for_append`, which loads both Activation
   Log slots and rereads the selected Run Set.
2. `activate_with_readers` calls `load_for_append`, which loads both slots again.
3. `ExactActivationLog::append` rereads the complete target slot after appending.

This is five full slot reads per activation, even if only one new 4-KiB record
was appended. Each slot may contain 64 records. The filesystem cache eligibility
filter recognizes immutable suffixes but excludes `.wal`, so the existing
mutation revision and unified range owner cannot avoid those reads.

The second sample recorded 1,025 `other/other/directFile` calls and 205
`other/exactIndex/directFile` calls: exactly five slot reads per selected Run Set
read. The former accounted for 192,999,424 requested bytes. The user's UI sample
likewise showed approximately 75 unclassified whole-file reads/s alongside
15 Run Set reads/s. Activation WALs currently appear as “Other files” because
the object classifier does not recognize their suffix or activation role.

Other amplification remains in the Direct-I/O and Index paths:

- `FsStorageIo::read` reads the physical length heads, then
  `read_cached_file_range` obtains them again. For an uncached WAL these are
  two separate 8-KiB reads before the body.
- Bounded independent reads similarly obtain the length heads when opening the
  range and again in the range reader. The staging-file exclusion also prevents
  ordinary reuse there.
- Generic direct writes reread length heads; unaligned edge preservation can
  read additional blocks. These operations are absent from the existing logical
  Metadata read table, explaining part of the difference from disk counters.
- New Exact Runs are audited from storage before publication and audited again
  when opened for activation. Compaction also reads its inputs.

The process journal contained checkpoint admission closures above five seconds
and subsequent reopening. This establishes actual backpressure during the job;
the samples alone do not assign every checkpoint delay to Metadata reads.

## Corrective design

The live owner already knows the validated input, intended serialized bytes,
current generation and results of its own writes. A serialized writer should
advance an authoritative in-process Activation Log cursor after the required
sync succeeds, and reuse that state on subsequent appends. Such a cursor is
active writer state, like the installed generation, not a separately evicting
content cache. All optional retained bytes still belong to the one common cache.

New Run publication should carry the Pipeline's validated writer evidence into
activation. Unchanged, leased Runs should retain their existing evidence.
Neither case needs to reconstruct its proof by repeatedly rereading storage
during the same exclusive ownership epoch. Ambiguous or failed operations must
invalidate the corresponding writer state; unknown objects, collisions and
recovery need a fresh validation path.

The implementation revises ADRs 0044, 0045, 0046 and 0079 explicitly. Independent
verification continues to bypass the cache; the online writer now has a
separate, explicit evidence boundary rather than calling a cached read a fresh
physical verification.

## Implemented correction and local qualification

The serialized Exact writer carries one bounded `ActivationLogSnapshot` and
advances it only after the final slot sync succeeds. Subsequent online appends
reuse the corresponding installed Run Set and leased readers. Unknown state,
an activation I/O error, or recovery requires independent reconstruction before
reuse. An ambiguous error after effective sync cannot leave the old RAM cursor
in force. Recovery is serialized against the complete append operation.

New L0 Runs, streamed compaction partitions and Run Sets no longer receive
online disk readback of bytes just validated by their writer. Required file
sync, no-replace publication, directory sync and final WAL sync remain. New
Run readers receive validated descriptors and immutable leases. Their encoded
pages, bounds and membership hints use typed namespaces in the common cache;
they have no private eviction policy or resident map. Known compaction inputs
reuse these readers. Public standalone publication/activation and unknown or
colliding outputs retain independent audits.

The Direct-I/O adapter admits successfully written length-head state into the
existing common range cache under the completed mutation revision. It discards
superseded head entries, admits no proposed state on failure, and transfers a
known head through its own no-replace rename while holding both name barriers.
Immutable lease acquisition can reuse that head. WALs are now classified as
control files in Metadata read telemetry; lookup attribution also covers page
bounds misses, and compaction page reads retain their compaction reason.

The regression loop was run red before the corresponding fixes:

| Case | Before | After |
| --- | ---: | ---: |
| 69 online activations after initial activation, including first WAL rotation | 345 full WAL reads | 0 full WAL reads |
| 16 aligned staging-page writes plus final length update | 139,264 Direct-I/O read bytes | 0 read bytes |
| Three new online L0 Runs | 12 Index-audit read operations | 0 audit reads; extended gate covers 70 appends and compaction |

The extended warm test runs activations 71–140, including compaction and WAL
rotation, with zero bytes returned by actual Direct-I/O file reads. It uses
one deterministic common engine for the fixture's storage and Index views.
It therefore tests resident-state reuse, not an unconditional zero-read promise
under system memory pressure. A separate pressure test evicts writer pages,
requires cold reads during compaction and verifies unchanged lookup results.

Fault injection covers failures before and after every operation of an online
append that compacts four L0 families and an append that rotates the WAL.
After simulated crash, a newly constructed repository must recover and scrub
only the previous or complete new selection. Another test retries directly
after an ambiguous final sync without explicit recovery and requires the next
generation to follow the actually written successor. Warm writer pages do not
hide injected physical corruption from recovery or offline activation audit;
failed recovery also prevents the next append from trusting the old readers.

Commands and complete logs are retained below
`.artifacts/tmp/metadata-read-diagnosis-20260912/`, including the red WAL,
storage-head and Run tests. This change leaves all durable byte formats intact.
The local correction has not been installed on the live VM during the backup.

Local gates passed: 190 distinct tests across the Store unit suite, Exact
activation/repository, generation, recovery, single-flight and filesystem-range
tests, plus Testkit Exact/GC/checkpoint fault and verified-cache tests. Thirteen
explicit manual benchmarks remain ignored. The Direct-I/O gates include the
XFS `fincore` assertion and an error after an effective length-head update.
Library Clippy and the Exact fault-test target pass with `-D warnings`.
All-target Clippy also encountered existing test-only lint findings in
`candidate_read_gate`, `persistent_reduction_gate_tests`, `seqcdc`,
`similarity_simd` and the pre-existing Direct-I/O head-corruption fixture; that
broader lint invocation is not a passing gate.

Reproduce the main gates from the repository root:

```sh
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
cargo test -p fastdup-store --lib --test exact_index_repository --test exact_index_activation --test storage_io_range --test generation_repository --test recovery --test record_read_singleflight
cargo test -p fastdup-testkit --test exact_index_repository_faults --test verified_read_cache --test gc_candidate_catalog_faults --test recovery_checkpoint_faults
cargo clippy -p fastdup-store -p fastdup-testkit --lib -- -D warnings
cargo clippy -p fastdup-testkit --test exact_index_repository_faults -- -D warnings
```

## Durability and the zero-read expectation

RAM defines the live logical view under exclusive ownership. Persistence is
established by complete successful writes, the required file synchronization,
directory synchronization and Commit ordering. `write()` completion alone does
not promise persistence, and synchronizing a file does not automatically make
its directory entry durable. See the primary contracts for
[write(2)](https://man7.org/linux/man-pages/man2/write.2.html) and
[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html).

An immediate readback is an additional integrity check, not a substitute for
those synchronization guarantees. The supported storage stack must honor
[flush/FUA semantics](https://docs.kernel.org/block/writeback_cache_control.html).
Media verification belongs to explicit scrub and recovery/uncertain-state
boundaries; ordinary cold reads still validate their bytes before use.

Zero resident Linux file-content pages, as measured by the earlier FUSE gate,
does not mean zero block reads. Removing repeated online readbacks should reduce
avoidable Metadata I/O. Cold reads, unavailable cached representations, recovery
and filesystem metadata can still cause reads. The corrected build must be
measured against the live workload before claiming an achieved VM IOPS reduction.
