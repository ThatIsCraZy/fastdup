# Remaining Metadata reads during live Veeam ingest

Read-only observation on the isolated test appliance, 2026-09-13 CEST. Running package:
`fastdup-0.7.4-15.el10.x86_64`; Repository PID 259763. Metadata is `sdb1`
(on sdb), DATA is `sdc1`. No service restart, configuration change or cache drop.

Two approximately 40-second recordings sample agent telemetry, `/proc/diskstats`,
Repository thread state and occasional in-progress read/write syscalls. Overview
responses sometimes omit details; cumulative cause deltas below use the first
and last responses with runtime details, with matching disk counters.

| Measurement | 14:01:54–14:02:32 (38.02 s) | 14:03:52–14:04:31 (39.02 s) |
| --- | ---: | ---: |
| `other/control/directFile` requested reads | 0.804 MB/s | 0.693 MB/s |
| `manifest/metadataObject/directFile` requested reads | 0.602 MB/s | 0.537 MB/s |
| Namespace requested reads | 0.006 MB/s | 0.007 MB/s |
| Exact read counter increases | zero | zero |
| Metadata device reads | 1.680 MB/s | 1.506 MB/s |
| Metadata device read IOPS | 71.56 | 73.57 |

The complete first overview window (39.02 s) measured 1.637 MB/s and 69.73 read
IOPS on sdb. Requested object bytes and physical device bytes are different
counters; alignment, storage-envelope heads and filesystem metadata may account
for part of the difference. This recording does not separately quantify them.

## Concrete code path

`GenerationRepository::stage_metadata_with_status` in
`crates/fastdup-store/src/generation/metadata.rs` enters `ReadIntent::Independent`
unconditionally. For an existing object it reads and compares the entire `.fdm`.
For a new object it writes `.<object-id>.building`, sets its length, then reads
and compares the entire temporary image before file sync and no-replace rename.
This explicitly bypasses reusable application bytes.

Five sampled Metadata `pread64` calls in the first window target exactly that
`.building` filename pattern on `tokio-rt-worker` threads. These are point
samples, not counts of every syscall. Such names are classified as `control` by
`metadata_read_telemetry::object_class`, so the UI's control category includes
fresh Manifest/Namespace images, not just WAL/administrative records. The
category is broader than this one path; its full byte count cannot be assigned
to temporary-image verification from point sampling alone.

Publication does not admit its retained image into `MetadataObjectCache`.
The later `read_manifest_node` path can therefore miss and issue another full
object read before `MetadataObjectCache::read` validates and inserts that image.
This is a concrete missing write-to-read-cache handoff, rather than evidence
that all measured Manifest reads revisit newly written objects: cold existing
nodes and other Manifest consumers can also contribute. The recordings measure
0.54–0.60 MB/s in this category but do not distinguish those subcauses.

The prior Exact fix is running: `exactGenerationDiscovery.completed == 1`,
`exactPublishBatch` is present and coalesces commands, and Exact lookup/audit/
compaction/envelope read counters remain flat in both windows. Independent scrub
reports `complete`. Online GC reports running; this alone does not attribute the
remaining Manifest reads to GC.

## Consequence

The next targeted correction is the owned Metadata publication boundary:
carry validated writer bytes through successful publication into the same
Unified Read Cache, and distinguish owned new publication from collision,
recovery and independent scrub verification. Publication ordering, uncertain
sync failures, deletion/GC invalidation and independent corruption detection
must remain paired. More cache capacity alone cannot remove an explicit
Independent read or populate a missing publication handoff.

Artifacts: `.artifacts/tmp/metadata-live-current/{sample.json,sample-2.json,
inspect.json,journal.log,summary.json}`. No source-code behavior was changed in
this diagnostic pass.

## Follow-up correction

The new-image branch of `stage_metadata_with_status` now carries its validated
encoder bytes through writes, length finalization, file sync and no-replace
publication without reading the temporary image. Only a successful publication
offers the bytes to `MetadataObjectCache`, the existing typed namespace in the
Unified Read Cache. The caller's directory/Commit barriers remain mandatory.
Existing-object collisions still use Independent intent; recovery, scrub and
GC deletion retain their verification and invalidation boundaries.

This removes the identified new-image readback and the initial cache miss for
admitted images. It does not establish that every measured control/Manifest read
disappears: existing-object collisions, cold objects and declined admissions can
still read storage. The live measurements above predate the correction.

Validation by the requested GPT 5.6 Luna subagent:

- Store library build and Clippy (`-D warnings`) succeeded; the final complete
  Store library suite passed 144 tests with 13 existing ignored tests.
- The strengthened publication regression counts **all** Metadata backend
  telemetry classes, including temporary images classified as `control`. It
  confirms zero reads for publication and for the first admitted-object read,
  and retains warm-cache collision/scrub corruption checks.
- A new adapter regression verifies Demand handoff, Scan/Independent admission
  rejection, zero-capacity fallback and invalidation.
- A publication fault regression verifies that errors before and after
  no-replace rename cannot admit an unsuccessful or ambiguous writer image.
- Generation recovery fault suite: 27 passed, one existing ignored test. Its
  rotation matrix now uses Independent intent so probe and replay operation
  positions cannot vary with pressure-dependent cache residency. The initial
  run's failure occurred during seeding due to this unstable operation count;
  the deterministic rerun preserves before/after failure injection at every
  rotation operation and complete-generation recovery assertions.

Focused logs: `.artifacts/tmp/metadata-writer-generation.log`,
`metadata-writer-cache.log`, and `metadata-writer-faults-independent.log`.
Final build/lint/suite logs use the `metadata-writer-*-final.log` prefix; the
publication fault regression is in `metadata-writer-publication-faults.log`.
These are local code-level results; the correction has not been deployed or
measured against a new live backup in this follow-up.
