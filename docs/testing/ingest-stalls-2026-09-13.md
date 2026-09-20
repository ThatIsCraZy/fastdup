# Ingest throughput stalls on 2026-09-13

The long zero-throughput intervals are caused by the repository closing global
mutation admission after a checkpoint exceeds five seconds. It then completes
that checkpoint and catches up the next Active epoch before reopening admission.
The live journal, completed-write counters and throughput samples reproduce this
stop/drain/reopen cycle. This is a diagnosis, with no runtime change or deployment.

Observed on the isolated test appliance, running `fastdup-0.7.4-13.el10.x86_64`, repository
PID 180746, started at 03:19:22 CEST. The source workspace also contains concurrent
module extraction work; function names below identify the relevant seams without
depending on changing line numbers.

## Live evidence

A read-only 40-second sample captured the existing Veeam SMB workload at
03:29:46–03:30:25 CEST. It sampled agent telemetry once per second and thread CPU
time/state every 250 ms, plus TCP and disk counters. It did not restart services,
generate an additional workload or discard caches.

| Admission closure | Reopened | Approximate duration | Observed effect |
| --- | --- | ---: | --- |
| 03:29:53 | 03:30:05 | 12 s | Write throughput reaches zero by 03:29:56; the completed-write count remains 85,951 through 03:30:06. |
| 03:30:10 | 03:30:24 | 14 s | Write throughput reaches zero by 03:30:13; the completed-write count remains 89,223 through 03:30:24. |

The admission gate was closed for approximately 26 of 40 sampled seconds (65%).
The journal reports seconds; telemetry represents a preceding sampling interval,
so shutdown/restart boundaries are not subsecond measurements. Already admitted
work can still finish after closure. Following the first reopening, throughput
reaches 837 MB/s at 03:30:07; the overall sample peak is 871 MB/s.

During the zero-throughput intervals, the sampled SMB connections also stop
receiving substantial data. Traffic resumes immediately after admission reopens.
This is consistent with server-side backpressure propagating to the sender;
these samples do not establish the client's precise SMB credit/request state.

Metadata disk utilization averaged 9.2% (maximum 36.9%); DATA averaged 4.7%
(maximum 20.6%). This does not indicate device-throughput saturation. Low device
utilization alone cannot exclude latency on an individual required operation.
The preceding 03:28:56 cache status had about 17.36 GB available memory and zero
swap use; the sampled slowdown does not coincide with an exhausted RAM budget.

## Why the phase breakdown is misleading

| Checkpoint generation | Total | Manifest planning | Index publication | Metadata commit | Outside recorded top-level phases |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1118 | 16.001 s | 0.971 s | 0.105 s | 0.828 s | 14.094 s |
| 1120 | 16.536 s | 0.756 s | 0.268 s | 0.339 s | 15.171 s |

The residual subtracts Freeze, Manifest planning, Index publication and Metadata
commit from Total. CDC, hashing, Exact lookup and encoding are nested inside
Manifest planning and must not be subtracted again. Thus about 88% and 92% of
these two long checkpoints are outside the separately recorded phases.

`DurableNamespace::checkpoint_profiled` does not separately time the checkpoint
lock, proof freeze and cut capture before the recorded Freeze phase, or the
following work before Manifest planning:

- `WriteThroughIngest::wait_for_commit_cut`: queued Ingest and detached publication
  retirement through the cut;
- `WriteThroughIngest::flush_stable_for_commit_cut`: acquisition and draining of
  live lanes, bounded publication admission and completion waits;
- attaching the resulting recipes to Namespace and constructing the writer.

The live-lane drain receives no Frozen-cut argument. It visits all current lanes,
uses each lane's latest mutation sequence and captures a finite retirement target
at that point. Consequently it can include newer Active work; this is a concrete
coupling to investigate, not a measurement proving that it consumes all residual
time. Current telemetry cannot split these waits and work precisely.

## CPU and publication evidence

| Thread | CPU seconds during the 40-second sample | CPU seconds while admission was closed |
| --- | ---: | ---: |
| Exact publisher | 31.66 | 20.71 |
| Online GC | 23.44 | 13.73 |
| Busiest Ingest worker over the whole sample | 2.04 | 0.02 |

These are CPU seconds per thread, not host CPU percentages. The Exact publisher
used roughly 79% of one core; the VM exposes twelve CPUs. Most Ingest/publication
workers were sleeping on futexes throughout the middle of the first pause.
This explains how the pipeline can stall with substantial spare aggregate CPU.
The sample does not contain userspace stacks, so it cannot identify the exact
mutex or queue a sleeping worker is waiting on.

`ExactPublicationQueue` serializes publications on one permanent thread and uses
a bounded blocking send. Each publication calls
`ExactIndexRunRepository::append_level_zero_from`, which may synchronously
compact levels and activate a new Run Set before consuming the next command.
Queue saturation can therefore propagate to detached publication retirement and
then to a checkpoint. The busy publisher is the leading measured candidate for
the backlog. Establishing which operation dominates it requires stack sampling
or dedicated queue/activation/compaction timing. Online GC is concurrent CPU work;
its CPU usage alone does not prove that it causes the admission closures.

## Follow-up and regression signal

Instrument the missing checkpoint phases and Exact queue wait separately, then
exercise a Frozen checkpoint while newer writes and a deliberately slow Exact
publisher continue. The regression should require finite progress of the Frozen
prefix without waiting for unrelated later work, while preserving crash recovery
and the existing DATA/Index/Commit ordering. Inspect publication batching and
synchronous compaction with that evidence. Raising the five-second timeout alone
would leave the backlog in place and weaken the existing durability guard.

Artifacts are workspace-local under
`.artifacts/tmp/ingest-stalls-20260913/`: `repository.log`, `inspect.json`,
`sample.py`, `live.json`, `checkpoints.json`, `correlation.json`, and
`summary-threads.json`. Raw observations are kept out of source control.

`python3 .artifacts/tmp/ingest-stalls-20260913/check_stalls.py` replays this capture.
It exits 1 when an active high-throughput workload has a zero-write interval of
at least five seconds overlapping a recorded admission closure. This capture is
expected to fail. It is an observational regression signal, not a replacement
for a controlled concurrency test or an end-to-end remeasurement after a fix.
