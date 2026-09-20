# Live checkpoint diagnosis with publication timings

Read-only observations on the isolated test appliance, `fastdup-0.7.4-14.el10.x86_64`, repository
PID 187676, started 2026-09-13 04:04:40 CEST. The updated runtime exposes the new
pipeline telemetry. The existing Veeam SMB job supplied the workload; no service
restart, configuration change or additional write workload was performed.

## Confirmed bottleneck

The bounded Exact command queue stays full during the bursts. The sampled
`exactQueueWait.active` reaches eleven: eight queue slots plus three producers
still inside send. `exactEnqueue.active` directly confirms blocked senders.
Checkpoint waits for detached publication retirement then exceed the five-second
guard and cause the observed global admission closure.

| Window, CEST | Sample interval | Admission closed | Mean frontend write | Peak frontend write |
| --- | ---: | ---: | ---: | ---: |
| 04:09:44–04:10:23 | 39.02 s | 23.49 s / 60.2% | 252 MB/s | 801 MB/s |
| 04:11:02–04:11:42 | 39.02 s | 16.21 s / 41.5% | 364 MB/s | 789 MB/s |

Frontend rates represent the agent's preceding sample interval. Admission time
comes from monotonic cumulative counters and includes unfinished closures.

| Completed checkpoint | Total | Publication cut wait + retirement | Metadata commit | Unattributed |
| --- | ---: | ---: | ---: | ---: |
| 1171, 04:09:17 | 13.995 s | 11.909 s | 0.376 s | <1 ms |
| 1174, 04:09:35 | 13.258 s | 11.332 s | 0.447 s | <1 ms |
| 1180, 04:10:02 | 9.057 s | 7.169 s | 0.412 s | <1 ms |
| 1184, 04:10:20 | 10.771 s | 8.741 s | 0.543 s | <1 ms |

The new phases explain the previously missing checkpoint time. Ingest-worker
waits, lane acquisition, proof freeze and the checkpoint lock are much smaller
than publication completion waits in these examples.

## Exact publisher breakdown

The two windows completed 407 and 392 publisher commands, respectively. Their
completed processing time was 28.98 s and 32.94 s. These cumulative-duration
deltas include a command finishing after the window starts, and omit an
unfinished command at the end; they are not exact busy-time integrals.

| Nested storage phase | First window | Second window |
| --- | ---: | ---: |
| New Run preparation/publication | 17.91 s | 18.28 s |
| Synchronous compaction | 0.64 s | 0.66 s |
| Run Set activation | 2.69 s | 2.64 s |
| Transition validation | 1.37 s | 1.42 s |
| Generation publication lock | <1 ms | <1 ms |

Thus synchronous compaction is a minor component of this repeated slowdown.
There was a separate earlier 13.57-second generation-lock wait, but that counter
did not increase in either sampled window. It does not explain the repeating
pauses measured here. Publisher processing includes work outside these nested
storage timers, so their durations must not be presented as an exhaustive split.

The metadata device averaged 15–16.5% utilization and DATA 7.7–10.2%. The Exact
publisher consumed 24.66 and 28.82 CPU seconds per 40-second capture. Online GC
also remained active (37.47 and 35.75 CPU seconds); this is concurrent work, not
proof that it caused publication backpressure. A separate 30-second syscall
sample of the Exact publisher mostly found userspace execution, with some futex,
write, file sync and directory sync observations. Sampling is not a syscall count
or a userspace stack profile.

## Concrete avoidable work found in code

`ExactIndexRunRepository::append_level_zero_from` unconditionally calls
`discover_run_generation_high_water` for each new L0 publication. That function
calls `FsStorageIo::list_names` and parses every canonical Run name in the
metadata root, including inactive historical Runs. This work is inside the
`exactRunPublish` timer.

A single read-only directory observation after the sampling windows found
80,706 names: 43,676 `.fdx` Runs, 32,760 `.fdxset` objects and 4,270 other names.
Python's single `os.listdir` call alone took 30.92 ms. This is independent
corroboration of directory-walk cost, not a measurement of the Rust function or
its exact fraction of the 44–47 ms average new-Run phase. With roughly ten L0
publications per second, the code repeatedly enumerates on the order of 800,000
directory entries per second merely to choose the next generation.

The first implementation target is an owned generation allocator: establish its
high-water during open/recovery, reserve monotonically under the existing shared
publisher lock and retain consumed generations after failed writes. Recovery
must still account for orphaned Runs. Also inspect batching adjacent small
publications before choosing a larger queue: more queue slots alone cannot
remove repeated scans or increase the single publisher's service rate.

Artifacts: `.artifacts/tmp/pipeline-live-20260913-0409/` contains both 40-second
captures, the inspect response, journal, syscall observations, directory summary
and machine-readable `summary.json`. The raw observations stay out of source
control. This turn diagnoses the live workload; it does not change publication
or allocation behavior.
