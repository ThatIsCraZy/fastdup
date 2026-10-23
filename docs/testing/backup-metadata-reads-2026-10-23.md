# Metadata reads during Active Full backups, 2026-10-23

## Screenshot and its exact retained observation

The screenshot at 04:01:20 CEST shows runtime 3945699 on RPM 0.8.1-48:
894.5 Metadata read IOPS, 5.325 MB/s read, 428.2 write IOPS, 11.093 MB/s
written, 68.1% device utilization and mean queue 1.369. The matching raw
telemetry observation was recovered with a bounded, indexed read-only query
from the control-plane history database; all these values match the image.

This was the preceding Active Full session
`99001b6d-e6ca-409b-9b82-07f34a4bb7c9`, which ran from 03:54:00 to
04:01:47 and completed successfully. The next Active Full,
`95aa052f-60bc-4b44-9629-a5dfdc2836a2`, began at 04:01:59. Measurements of
that later session must not be presented as measurements of the screenshot.

At the screenshot timestamp, initial scrub was still running: 8,982 of 9,477
Containers were covered, with 495 remaining. Its counters separate 7,358
resumed Containers from 1,624 freshly verified Containers. GC was not the
reported activity. The recorded logical Metadata read rates were:

| Reason | Logical storage operations/s |
| --- | ---: |
| Similarity candidate lookup | 233.1 |
| Index publication | 106.1 |
| Index audit | 90.1 |
| Index compaction | 57.0 |
| Recovery scrub | 57.0 |
| Index envelope | 28.0 |
| Other | 7.0 |
| Manifest | 0.0 |

These are application read operations, not physical device IOPS. Storage-head
reads and publication verification are not all represented one-for-one in the
reason table, so it is not an exact allocation of the 894.5 device IOPS.
Over the surrounding 80-second history window, the mean device read rate was
250.2 IOPS and the highest retained one-second sample was 1,062.8 IOPS.
The screenshot captures simultaneous lookup, publication/audit and remaining
scrub work rather than a sustained 895-IOPS baseline.

## Current job: opening versus sustained writes

The new job was observed without changing its configuration or restarting the
runtime. During its opening phase, 9,580 Manifest reads returned 857.6 MiB in
50.1 seconds; frontend writes averaged only 0.65 MiB/s. Those file-opening
reads are separate from the screenshot's write-phase activity.

A subsequent contiguous 180.7-second write window measured:

| Counter | Observation |
| --- | ---: |
| Frontend write throughput | 807.6 MiB/s |
| Mutation admission closed | 0.0% of the window |
| Similarity lookup reads | 11,658, each 4 KiB; 64.5/s |
| Similarity audit reads | 736; 4.1/s |
| Read/write errors | 0 / 0 |
| Integrity failure | false |

The corresponding 182.3-second host window measured 87.7 Metadata read IOPS
at 0.488 MB/s, 44.5 write IOPS at 2.107 MB/s, 12.0% busy time, 3.89-ms mean
read latency and a mean queue of 0.425. DATA read IOPS averaged 32.5.
Initial scrub had completed all 9,477 Containers without error before this
window. Its counters did not advance during the measured writes.

A three-second `perf trace` sample confirmed small `pread64` requests on the
runtime. The sample is included in this observation; no controlled profiler
overhead comparison was made. A subsequent FD map is not reliable attribution
for descriptors reused by short-lived publication files, and repeated FD/range
pairs alone do not establish redundant reads of one immutable object.

The observed write phase has no sustained Metadata saturation or closed
admission. The already known opportunities to coalesce concurrent cold
Similarity misses and reduce publication/audit I/O need separate controlled
tests; no storage verification or cache policy was relaxed to lower the IOPS
number. Repeated Active Full jobs, cache warmth and overlapping startup scrub
prevent treating the two sessions as an A/B optimization result.

The observed session subsequently completed successfully in 7:46, with Veeam
reporting 766 MB/s, 307,513,786,368 bytes read and 142,961,977,857 bytes
transferred. The final job-state observation is recorded in
`.artifacts/deploy-history-20261023/veeam-predeploy.json`; the observer's
top-level session/task fields refer to its older fixed session, so only the
current `jobState` and its matching session ID support this result.

## Evidence

Artifacts are under `.artifacts/backup-metadata-20261023/`: `runtime.jsonl`,
`host.jsonl`, `analysis.json`, `runtime-writing.jsonl`, `host-writing.jsonl`,
`writing-analysis.json`, `screenshot-history.json`, `screenshot-analysis.json`,
`pread-trace.txt`, `fd-map.json`, and `veeam-start.json`.

The supplied historical-diagnostics change was subsequently deployed as
[RPM 49](historical-diagnostics-2026-10-23.md#appliance-deployment); these
measurements were collected on RPM 48 before that deployment.
