# Checkpoint stalls: live and historical telemetry

The observations added after `ingest-stalls-2026-09-13.md` expose unfinished
checkpoint and Exact publication waits. A long-running operation becomes visible
before it returns, including when no checkpoint has completed since mount.

The UI entry is **Ursachen & Details → Checkpoint-Phasen**. It shows admission
state at the sample time, closure reason, current/total/longest closed duration,
active pipeline operations and their continuous busy duration. The disclosure
**Pipeline-Zeiten seit Mount** shows finished-operation counts, summed durations,
means and maxima. The same values survive the existing historical telemetry
storage; historical views do not advance elapsed time using the browser clock.

## Interpretation

| Observation | Meaning |
| --- | --- |
| Checkpoint lock, proof freeze, cut capture | Work before the Namespace Freeze timer starts. |
| Ingest wait / publication wait | Completion through the Frozen cut's Ingest and publication fences. |
| Lane acquisition / stable extraction | Registry/lane acquisition and processing of complete staged chunks. |
| Publication admission / retirement | Waiting to enqueue a partial Container and waiting for its publication/externalization to finish. |
| Recipe attachment / writer setup | Preparing the checkpoint before Manifest planning. |
| Exact queue admission | Time spent in the bounded channel send, including blocking when full. |
| Exact before processing | Pending commands, including producers still waiting for channel capacity. Excludes the command currently processing. |
| Exact processing / flush | Publisher command processing and callers waiting for a flush fence. Flush includes its send and queue wait. |
| Exact generation lock / predecessor / validation | Serialized writer admission, predecessor selection and transition validation. |
| Exact Run publication / compaction / activation | Building and writing the new Run, each synchronous compaction, and activating/installing the resulting Run Set. |

`active` counts unfinished operations. `busyMs` measures time since that
operation family most recently changed from zero active operations to nonzero;
with overlapping callers it is a continuous busy period, not an individual
request's latency. `completed`, `totalMs` and `maximumMs` cover finished attempts,
including error paths. Their timers retire on early return and unwind. A live
stall can therefore have a large `busyMs` while its completed maximum is still
small. Counters are local to one runtime; compare deltas only within a matching
`runtimeId`.

The queue has eight command slots. `exactQueueWait.active` can exceed eight
because it also includes producers blocked before admission. `exactEnqueue.active`
exposes those sends separately. These observations do not change queue bounds.

Exact storage timings are shared by clones of the same repository, including
Online GC's append operations. They are nested within publisher processing when
that publisher is the caller. Do not add nested or concurrent durations to obtain
wall time or infer host CPU utilization from them.

Admission durations include the currently closed interval. Repeated pause calls
do not count another closure or reset its start time; a failure may update its
reason. Recovery failure/integrity failure, dirty pressure, checkpoint timeout,
durability lag and orderly shutdown have distinct reasons. The counters describe
explicit admission closures, not ordinary short Namespace commit fences.

## Completed checkpoints and API

`telemetry.details.runtime.pipeline` contains `admission` and `operations`.
`telemetry.details.runtime.checkpoint.phases` additionally contains the missing
checkpoint phases and the parent `manifestPlan`. Its `unattributedMs` is Total
minus the non-overlapping top-level phases. The chart uses those top-level
phases and the residual; CDC, hashing, Exact lookup, encoding and Container
publication remain visible in the table as parts of Manifest planning.

Older samples without `pipeline` or `unattributedMs` remain readable and do not
acquire fabricated zero-valued measurements. Existing management polling and
historical retention are reused; no separate telemetry history queue is added.

The journal emits `checkpoint_wait_metrics` with the generation and the new
`*_wall_ns` fields next to the existing `checkpoint_metrics` line. Join these
records on generation when inspecting a completed checkpoint outside the UI.

## Validation and limits

The tests hold the actual checkpoint lock, saturate the real bounded Exact
command channel, and hold the Exact repository's generation publication lock.
They require observations while the work is blocked; checkpoint/repository
observation must return within the control sampler's 400-ms deadline. Additional
tests cover overlapping timers, repeated admission transitions, API/history
roundtrips, unfinished waits in the UI and non-overlapping chart phases.

Validation passed: 59 appliance library tests (two existing ignored), 19 runtime
binary tests, 37 POSIX tests (one existing ignored), two targeted store timing
tests, five detail-telemetry API tests and one byte-exact checkpoint/recovery
integration test. All 52 UI tests, TypeScript checking, the production UI build
and library/binary Clippy with warnings denied passed. The recovery test was
rerun successfully after removing obsolete local build products from a full
workspace filesystem.

The implementation uses fixed operation families and constant-size counter state.
It records no payloads, adds no disk reads, and never holds an observation lock
over queue waits or storage work. Short counter mutexes and clock reads add some
overhead per checkpoint phase/publication, not per DATA chunk lookup. These are
observations; storage policy, durability boundaries and persistent formats are
unchanged.

Workspace evidence: `.artifacts/tmp/pipeline-validation.log`,
`pipeline-control-tests.log`, `pipeline-posix-tests.log`,
`pipeline-recovery-test.log`, `pipeline-clippy.log`, `pipeline-ui-test.log`,
and `pipeline-ui-build.log`. The production UI build is under
`.artifacts/pipeline-telemetry-ui/`. These local changes need an updated runtime
and UI installation before the running test VM can emit/display them.
