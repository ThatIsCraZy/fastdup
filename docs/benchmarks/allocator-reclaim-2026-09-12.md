# Returning allocator retention to RAM, 12 September 2026

## Diagnosis

Read-only inspection of the test VM's glibc 2.39 process (PID 1142, started
11 September, RPM 0.7.2-2) found a substantial gap between cache accounting and
anonymous RSS. An earlier task had measured a different process at roughly
18 GiB RSS; that historical peak was not reproduced by restarting or disrupting
the current backup.

A bounded `/proc/PID/mem` census read allocator headers only, without ptrace,
process suspension, inferior function calls or payload inspection. The 55-arena
ring contained 9,761,820,672 system bytes and 6,052,266,528 bytes on observed free
lists. Sixty unstable/deadline-limited bins were excluded. This is a concurrent,
approximate census, not a complete live-object heap profile. It establishes
allocator retention as a substantial contributor; it does not rule out other
retention or leaks. Separate nearby management samples reported about 2.6 GiB
of resident caches. Free allocator bytes and cache bytes must not be subtracted
as if sampled atomically or added to RSS: some free pages may already be absent.

Layouts and semantics were checked against the matching primary glibc source:
[malloc.c](https://github.com/bminor/glibc/blob/release/2.39/master/malloc/malloc.c)
and [arena.c](https://github.com/bminor/glibc/blob/release/2.39/master/malloc/arena.c).
No live VM memory configuration or service was changed for the diagnosis.

## A/B experiment

The retained harness [allocator-reclaim.c](allocator-reclaim.c) uses eight
threads, eight rounds, alternating 16-KiB/256-KiB buffers and small surviving
allocations. It recreates freed buffers held inside still-live arenas. All
paths perform the same allocations/writes/frees; mode 2 trims once after the
last round while surviving allocations remain, mode 0 never trims. Measure RSS
at that barrier, not after all anchors and worker arenas have been released.

```sh
mkdir -p /source/fastdup/.artifacts/tmp /source/fastdup/.artifacts/allocator-ab
TMPDIR=/source/fastdup/.artifacts/tmp cc -O2 -fno-builtin-malloc -fno-builtin-free -pthread \
  docs/benchmarks/allocator-reclaim.c -o /source/fastdup/.artifacts/allocator-ab/run
/source/fastdup/.artifacts/allocator-ab/run 0
/source/fastdup/.artifacts/allocator-ab/run 2
```

Three interleaved runs on the development host:

| Mode | Total wall ms | Final barrier RSS MiB | Trim ms |
|---|---|---|---|
| No trim | 412.291 / 443.005 / 423.888 | 1094.70 / 1094.82 / 1094.77 | 0 |
| One trim | 519.509 / 531.063 / 504.370 | 39.88 / 39.89 / 39.77 | 105.422 / 111.184 / 108.886 |

This demonstrates about 96% lower retained RSS with a measurable cost, not a
throughput improvement. Trimming every round was roughly 3.6–4 times slower.
Setting `MALLOC_MMAP_THRESHOLD_=131072` reduced retention too but increased
wall time from roughly 0.4 seconds to 3.8 seconds in this allocation-heavy
workload, so it was not added to the service as a universal solution.

## Production policy and bounds

One daemon-owned background worker samples glibc counters and anonymous RSS.
It requests `malloc_trim(0)` only if both free allocator blocks and anonymous
resident bytes above allocator-allocated bytes exceed the shared 8% RAM reserve.
This distinguishes actually resident slack from already-discarded free blocks
or live working memory. No cache entries or live payloads are discarded by this
worker. Cache admission still uses observed OS availability, never a promise
that all free allocator bytes are reclaimable.

The worker runs at most once per 30 seconds and backs off to at least 100 times
its last probe/trim cost. No allocator census or trim executes on the cache hit,
FUSE request, management inspection or checkpoint thread. The work still takes
glibc arena locks, so the cadence limits overhead rather than guaranteeing zero
latency impact. The unsafe surface consists only of GNU/Linux `mallinfo2()` and
`malloc_trim(0)` behind a safe ownership-scoped interface; both acquire libc's
internal locks and never invalidate live Rust allocations.

Telemetry exposes sampled allocator allocation/free/arena bytes, anonymous RSS,
trim count and last trim time separately from cache occupancy. The new worker
has not yet been deployed or measured during a full Veeam run. The A/B evidence
establishes reclamation of the reproduced retention pattern, not a universal
RSS bound or the absence of additional live-memory growth.
