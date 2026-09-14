# DATA reads during ingest, 2026-09-13

Read-only sampling of VM 10.1.1.161, RPM 0.7.4-16, repository PID 268873.
The recent online GC/checkpoint changes are not installed in this process.

39.018-second window starting 14:50:45: DATA 1.193 MB/s reads, 68.66 read
IOPS, 8.918 MB/s writes; Metadata 0.219 MB/s reads. Scrub complete, GC running,
Similarity/advanced-reduction base reads and queries zero. Sampled GC preads
were Exact files on Metadata; one Tokio-worker pread targeted a DATA Container.

A subsequent 25-second /proc syscall sample at approximately 40ms intervals
captured 13 observations of DATA preads on Tokio workers. These are point
observations, not completed-call counts: five observations of one 237568-byte
read at offset 3710976 may represent one blocked call. Other requests were
49152, 61440, 90112, 110592, 131072 and 184320 bytes at interior offsets, plus
one 4096-byte offset-zero read. They are bounded Container ranges, not whole
images. The 237568-byte read targets a 7512064-byte Container. Its mtime is
14:52:13; another sampled Container has mtime 14:51:59, while an older sampled
Container dates from 05:33:22. Thus both recent and historical images are read.

Checkpoint logs show recipe reuse plus remaining rechunk work (generation2428:
1990900012 recipe-reuse bytes, 171995496 checkpoint-rechunk bytes). The concrete
matching code path is manifest_planning CommitRangeReader -> CommitInode::read_at
-> frozen version reader, which can request committed content while assembling
new chunks. These counters and syscall samples do not uniquely attribute each
read to that path; a caller-level trace is required to exclude other verification
consumers. No claim that all reads are unavoidable follows from this sample.

Next targeted investigation: record the read reason around checkpoint rechunk
and materialization; verify the write-to-committed-recipe transition keeps bytes
or supplies the unified DATA cache for imminent reuse. Do not reintroduce payload
verification for trusted Exact pointer reuse.

Evidence: .artifacts/tmp/data-write-reads/{sample.json,offsets.json,journal.log,
read-offsets.py}. No production changes, restart or deployment performed.

## Reproduced planner amplification

The follow-up diagnostic `diagnostic_partial_overwrite_reads_untouched_committed_data_edges`
in `crates/fastdup-appliance/tests/write_through_ingest.rs` writes and commits
1 MiB, then overwrites one byte at offset 12,345 without any intervening frontend
read. The next checkpoint rechunks 262,144 bytes. The DATA operation sequence
starts with ObjectLen and three ReadExactAt operations before CreateNew for the
replacement Container. Three further ReadExactAt operations are publication
samples; they are separate from the old-Record reads. Final bytes match the oracle.
The diagnostic was run by the requested GPT 5.6 Luna test agent; log:
`.artifacts/tmp/data-reread-diagnostic.log`.

`manifest_rewrites` expands touched ordinary DATA extents to their complete
boundaries. `plan_manifest_range_with_prepared` exports only dirty external and
late prepared recipes, not the unchanged predecessor edges introduced by that
expansion. The gaps therefore reach `plan_allocated_range` and CommitRangeReader,
which loads the old committed Chunk. This happens before Exact reuse decisions.
ADR 0036 still prescribes whole-DATA expansion, whereas ADRs 0011/0043 already
support authenticated DATA_SLICE references. Correcting it requires pairing the
planner with untouched predecessor slice recipes and updating that older policy;
cache growth alone does not remove this extra work.

A second static finding is that `externalize_chunks` constructs location-backed
readers from pending chunks without handing pending payload bytes to the Unified
DATA cache, and POSIX externalization removes resident overlaps. Thus a subsequent
byte reader can miss even for newly published data. This is distinct from the
reproduced planner expansion and has not been separately quantified here.

The diagnostic proves a concrete unnecessary read path under write-only input.
It does not attribute every live DATA read to it. No production fix or deployment
was made during this follow-up investigation.

## Correction

The planner no longer expands overwrite/truncate ranges to complete old DATA
extents or rounds mutations outward to 256-KiB CDC cells. Both expansion
steps had to be removed. Existing persistent-tree slicing retains untouched predecessor bytes
as DATA_SLICE metadata; only the actual mutation reaches rechunking. This also
preserves offsets when a later mutation intersects an existing slice.

Writer payload handoff now uses the shared VerifiedReadCache admission seam:
write-through publication, immediate Exact-hit externalization, checkpoint
publication and checkpoint Exact reuse offer their available logical bytes.
The bounded payload constructor verifies the full Chunk hash and length and
supplies no physical Record provenance. Demand reads can reuse these bytes;
Independent verification and proof consumers retain their separate obligations.
The existing cache independently compresses admitted payloads with LZ4 when it
saves memory, otherwise retains decoded bytes. Common pressure/intent admission
remains authoritative; no private writer cache was added.

Build and regression qualification are delegated to GPT 5.6 Luna xhigh.
Logs: `.artifacts/tmp/slice-cache-*.log`. No deployment is included here.

## Additional live spike report during qualification

The screenshot around 15:15 reported DATA 47 MB/s and Metadata 17 MB/s. Subsequent
sampling from 15:16:42 for 39.020 seconds still found RPM 0.7.4-16 / PID 268873; the
pending slice/cache and online-GC/checkpoint fixes were not deployed. Average
DATA reads were 0.789 MB/s / 33.01 IOPS and Metadata 0.148 MB/s / 28.37 IOPS. Point samples
captured five Tokio-worker DATA Container preads and nine GC-worker Exact-file
preads. Scrub remained complete; advanced-reduction query/base-read counters zero.
The reported peak was not captured and cannot be retrospectively attributed
from these samples. Journal reports a 553244608-byte recovery checkpoint for
Commit 2678 at 15:15:59; this is another possible contributor, not measured proof
of the screenshot's read attribution. Artifacts:
`.artifacts/tmp/data-read-spike-1515/{sample.json,events.log}`.

## Verified correction results

The converted partial-overwrite regression forces Independent intent around the
checkpoint so writer-admitted bytes cannot hide reads: planner_read_exact_at=0,
DATA operations=[] and checkpoint_rechunk_bytes=1, versus 3 old-record reads and
262144 rechunked bytes before the correction. Writer-cache regressions verify
logical reuse, no Location proof, rejected mismatched hashes and Scan/Independent
bypass. Store suite 147 passed / 13 ignored; write-through 33 passed / 1 ignored; Manifest
tree 12 passed; Metadata publication fault 1 passed. Release build and production
Clippy succeeded. Results are saved in the slice-cache log files cited above.

Additional slice durability qualification passed: frozen replacement across
240 injected fault cases, mixed-growth checkpoint faults, and an unfaulted
frozen replacement followed by crash/recovery. Probe and fault replay now share
Independent intent throughout each test so cache warming cannot shift the
injected operation positions. Strict committed-layout assertions remain intact.
Logs: `.artifacts/tmp/slice-cache-slice-faults-{replace,growth,nofault}.log`.

## Captured large DATA spike

The extended 110-second recording finally captured the burst, at 15:24:39–46:
DATA reads peaked at 94.708 MB/s. In the same intervals repeated pread samples
on Tokio thread 281064 targeted
`data/.recovery-checkpoint.0000000000000aca.fdrc.building`.
The GC worker simultaneously read Exact `.fdx` files on Metadata. The journal
confirmed successful checkpoint 2762 at 15:24:56, file length 601106624 bytes.
There were 63 point observations of temporary checkpoint reads overall (not 63
completed calls). This identifies new Recovery Checkpoint publication readback
as the concrete DATA burst path; it is separate from ordinary Container
rechunking. `RecoveryCheckpointRepository::publish_source` calls `audit_named`
and `verify_publication_graph` on the just-written temporary file before sync
and rename. Skipping an unchanged Commit does not remove this first-publication
readback for a newer Commit. These observations are from RPM 0.7.4-16.
Evidence: `.artifacts/tmp/data-read-current/long-sample.json` and the repository
journal. No change to checkpoint publication readback was made in this task.

## Follow-up: remove new checkpoint image readback

The subsequent correction removes `audit_named` and `verify_publication_graph`
from the newly written temporary-file path. The source traversal has already
validated the exact pinned graph; copying checks each object identity and emits
checksums, body hash and lengths incrementally. The publication summary now
receives the required-Chunk count from that traversal. Explicit full publication
also avoids verifying those DATA dependencies a second time against the copy.

Sync and no-replace publication ordering is unchanged. Existing-file retries
still validate stored bytes, and a no-replace collision additionally requires
the complete descriptor to match. Small selector reads remain. Recovery, scrub
and GC graph reads are not removed by this correction. Writer memory remains
bounded; no checkpoint-sized RAM copy or private cache is introduced.

GPT 5.6 Luna xhigh completed qualification: the new multi-megabyte DATA-backed
regression records both whole-file and range reads under Independent intent,
and rejects reads of either `.fdrc` or `.fdrc.building`. It confirms one unique
required Chunk, one explicit DATA-verifier invocation, and complete recovery
after crashing the checkpoint store. The focused test passed; the complete
checkpoint fault suite passed 15 tests; the Store library passed 147 tests with
13 ignored. Release `fastdup-durable-fuse` build and production Clippy with
`-D warnings` succeeded. Logs: `.artifacts/checkpoint-readback-*.log`.
No VM deployment is part of this follow-up.
