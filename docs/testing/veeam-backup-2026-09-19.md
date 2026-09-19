# Veeam backup over SMB — 19 September 2026

Veeam Backup & Replication writing a VMware backup job to a fastdup SMB share
on the test appliance. Host identity, share name, job name, VM names and the
initiating account are deliberately omitted; they belong to the test
environment, not to this repository.

Appliance package during the recorded session: `fastdup-0.8.0-1.el10.x86_64`
(built from the tree tagged `0.7.4-77` before the version bump). Repository
mounted, `fastdup-repository`, `fastdup-control` and `smb` active, one SMB
share, no Advanced Reduction.

## Result

Five consecutive backup job sessions ran between 20:13 and 21:48 CEST. Every
session reported `result: Success`. The session recorded in detail below is the
last one, 21:39:47 → 21:48:20.

| | |
| --- | ---: |
| Tasks in the session | 8 |
| Task results | 8 × Success |
| Reported bottleneck | Target, on all 8 tasks |
| Task duration | 7:38 – 7:45 |
| Per-task processing rate reported by Veeam | 84.6 – 94.4 MB/s |
| Processed | 620.0 GiB |
| Read from source | 284.5 GiB |
| Transferred to fastdup | 283.4 GiB |
| Task window | 465.8 s |
| Aggregate transfer rate | 653.3 MB/s |

Repository occupancy grew from 89.2 GB to 108.6 GB across the session series on
a 772.7 GB pool.

## SMB path

Counters from the Samba profile over the measured window:

| Operation | Count | Failed | Average |
| --- | ---: | ---: | ---: |
| write | 74,710 | 0 | 76.8 ms |
| flush | 768 | 0 | 7.2 ms |
| close | 803 | 0 | 0.15 ms |

Write latency distribution: 68,513 writes under 8 ms, 2,278 at or above 256 ms.
Average active write concurrency 12.3. Samba's own overhead was 24.6 µs per
write; the remainder is the asynchronous fastdup write path.

Frontend during the same window: 566 checkpoints totalling 290.6 s, 9 admission
closures totalling 37.5 s, FUSE waiting queue p95 of 8. Metadata tier 4.3 MB/s
read and 4.6 MB/s write at 38.6 % utilisation; DATA tier 12.7 MB/s write at
18.5 % utilisation. Host CPU 31.2 % busy, 11.1 % iowait. Network receive
652.8 MB/s.

No unit errors were logged on the appliance for the session.

## What this does and does not establish

It establishes that a Veeam VMware backup job completes successfully against a
fastdup SMB share, repeatedly, with the target reported as the bottleneck and
no failed SMB operation.

It does not establish a Veeam-certified or Veeam-qualified integration, a
supported configuration, or a performance SLA. The session used Full backups,
so it exercises the write path rather than Fast Clone; Fast Clone over SMB is
covered separately by the 9 September 2026 clone runs
([alignment](veeam-clone-alignment-2026-09-09.md),
[partial clone length](veeam-partial-clone-length-2026-09-09.md),
[consecutive clones](veeam-consecutive-clones-2026-09-09.md)).

Interval statistics show two regimes: 162 intervals with write-through queue
backpressure at 811.1 MB/s and 28.1 ms average write latency, and 70 intervals
without it at 298.7 MB/s and 328.9 ms average write latency. The slower
intervals are the ones with the long-tail writes. That asymmetry is open work,
not a resolved result.
