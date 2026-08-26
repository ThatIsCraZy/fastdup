# Memory and swap containment

fastdup must apply backpressure before dirty DATA exhausts RAM, and the service
must run in a cgroup that cannot swap. Either rule without the other is
insufficient: the application budget bounds known live work, while the cgroup
is the final kernel-enforced containment boundary for allocator fragmentation,
libraries, and future regressions.

## Required production controls

The durable FUSE daemon currently closes mutation admission at 512 MiB of
unique, reachable active Dirty DATA (eight nominal 64-MiB Containers). Keep that
limit enabled. One process-wide `MemoryBudgetGovernor` samples host and current
cgroup memory at most every 250 ms and supplies the same fail-closed snapshot to
all rebuildable caches. The verified read cache is a separate discardable tier:
it is hard-capped at one eighth of effective RAM (and at most 8 GiB), while
reserving at least one quarter of effective RAM or 4 GiB for ingest, XFS, and
I/O queues. It purges and stops admitting payloads whenever the reserve is
unavailable or the fastdup cgroup has charged Swap. The persistent Exact Index
retains only verified hot 4-KiB pages in a separate direct-mapped cache. Its hard geometry is one 128th
of effective RAM, clamped to 1-256 MiB, while its resident target uses the same
live reserve and drops to zero on cgroup Swap. It is never a complete in-memory Chunk
map. Rebuildable per-Run blocked Bloom hints use a separate active-set budget of
one 32nd of effective RAM, clamped to 1 MiB-8 GiB and limited by headroom above
the shared reserve. Every Run-Set activation resamples headroom, and Swap
charged to the fastdup cgroup disables filters in the replacement; host Swap
belonging to other workloads does not. Filters of at least 2 MiB use their own
anonymous `MADV_HUGEPAGE` mapping; smaller filters remain normal heap
allocations. Absence always falls back to the verified Exact pages. The
data-tier `io_uring` adapter has an independent
256-MiB publication-buffer budget. Its normal owned path transfers a prepared
Container image once and charges exactly one image for the whole publication.
The same lease remains charged through writer-image verification and the three
bounded storage samples. Legacy borrowed `write_at` calls still require a completion-lifetime
copy and report those bytes separately. Neither tier counts as read-cache
capacity. This budget begins at publication admission, so the Reduction
pipeline must independently bound prepared-but-not-yet-submitted images. The
Ring path is required. Missing kernel support or ring setup failure aborts
daemon startup; there is no synchronous DATA fallback.

Writer images of at least 1 MiB are decoded asynchronously by a bounded queue
on the process Rayon pool; smaller Containers stay inline. Every queued job
continues holding the same publication-buffer lease, so queued verifier input
cannot escape the 256-MiB bound. The decoded `SealedContainer` result can
temporarily coexist with its writer image during verification and is not part
of `inflight_bytes`; process RSS/cgroup limits remain the authoritative guard
for that decoded representation. With nominal 64-MiB images, the input budget
admits at most four such jobs concurrently.

Run the daemon with the following service properties:

```ini
[Service]
Environment=MALLOC_MMAP_THRESHOLD_=131072
Environment=FASTDUP_REQUIRE_CGROUP_NO_SWAP=1
MemorySwapMax=0
```

`MemorySwapMax=0` is the hard no-swap rule on a cgroup-v2 systemd host. The
environment switch makes the daemon verify `memory.swap.max=0` and zero current
cgroup Swap before it opens Metadata or DATA; it never tries to mutate a shared
or incorrectly delegated cgroup itself. Configure `MemoryHigh` as an early
host-specific pressure signal and `MemoryMax` below the
RAM reserved for the OS, Samba, metadata services, and recovery tooling. Those
two values deliberately have no universal repository default: a 128-GiB and a
256-GiB appliance require different reservations. Hitting `MemoryMax` is a
containment failure, not normal flow control; the application must close write
admission first.

The glibc mmap threshold is evidence-based. In the Rocky ISO pressure test it
caused large temporary CDC/planning/codec allocations to be unmapped after a
checkpoint instead of remaining in allocator arenas. It reduced the comparable
30-second peak from 1,173,072 KiB to 766,356 KiB and finished the empty
checkpoint at 81 MiB RSS. An arena-count-only comparison retained about 620 MiB
after accepting somewhat more data, so arena count was not the primary control.
This environment setting must be present before the process starts; setting it
inside an already-running daemon is too late.

## Acceptance checks

For every sustained ingest test record all of the following:

- daemon `VmRSS`, `VmHWM`, and `VmSwap` from `/proc/PID/status`;
- daemon `Swap` and anonymous memory from `/proc/PID/smaps_rollup`;
- system `MemAvailable` and `SwapFree`;
- deltas of `pswpin` and `pswpout` from `/proc/vmstat`;
- cgroup `memory.current`, `memory.events`, and `memory.swap.current`; and
- `verified_read_cache` telemetry, especially payload `resident_bytes`,
  `target_bytes`, `pressure_rejections`, and fixed `metadata_bytes`; and
- `exact_index_page_cache` hits, misses, resident/target/capacity pages,
  evictions, pressure rejections, reserve, and Swap sample; and
- Container-descriptor cache hits, misses, admissions, evictions,
  pressure/allocation rejections, hard/target/resident entries,
  hard/target coverage, resident/fixed bytes, and its memory-pressure sample;
  this cache contains no Header/Footer pages or DATA payload; and
- `exact_run_membership` filters, allocated and huge-page-advised bytes, probes,
  definite absences, and lookups requiring Exact pages; and
- `memory_budget_governor` effective limit, available bytes, host and cgroup
  Swap separately, cgroup Swap limit/protection, sample count, and failures; and
- `data_io_uring` mode, `inflight_bytes`, `peak_inflight_bytes`, submitted and
  completed operation counts, root-sync caller/submission counts, owned
  publications started/completed, `borrowed_write_copy_bytes`, configured
  verifier workers, jobs started/completed/failed, and active/peak-active
  verifications.

Pass requires daemon and cgroup Swap to remain zero. Host `pswpout` may change
because of another cgroup and must be attributed before failing fastdup;
fastdup's `memory.swap.current` and process `VmSwap` must not increase. No
`oom`, `oom_kill`, or `max` event may occur. Historical host SwapUsed is not by
itself a fastdup failure; attribute it using the process and cgroup counters.
Ring `inflight_bytes` must return to zero after quiescence, its peak must not
exceed `max_inflight_bytes`, and completed operations must equal submitted
operations after a clean shutdown.
In active Ring mode, every successful normal Container publication must
increment both owned counters exactly once, and `borrowed_write_copy_bytes`
must remain zero. A nominal 64-MiB image must charge approximately one image,
never two; the hard peak remains at or below 256 MiB. Synchronous and fallback
modes report zero Ring-publication counters by construction.
After quiescence, active verifications must be zero, completed jobs must equal
started jobs, failures must be zero for a healthy workload, and peak active
must not exceed configured workers. Small-Container-only runs should report no
pooled jobs.

The 2026-08-16 bounded-dirty 50-ISO run met these memory checks with a 2.40-GiB
peak and more than 19 GiB of available-RAM margin. It did not meet throughput or
the ten-second commit SLO because growing files are still re-chunked from byte
zero at each pressure checkpoint. See the
[intensive FUSE benchmark](../benchmarks/io-intensive-fuse-600s.md).
