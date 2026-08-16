# Memory and swap containment

fastdup must apply backpressure before dirty DATA exhausts RAM, and the service
must run in a cgroup that cannot swap. Either rule without the other is
insufficient: the application budget bounds known live work, while the cgroup
is the final kernel-enforced containment boundary for allocator fragmentation,
libraries, and future regressions.

## Required production controls

The durable FUSE daemon currently closes mutation admission at 512 MiB of
unique, reachable active Dirty DATA (eight nominal 64-MiB Containers). Keep that
limit enabled. Run the daemon with the following service properties:

```ini
[Service]
Environment=MALLOC_MMAP_THRESHOLD_=131072
MemorySwapMax=0
```

`MemorySwapMax=0` is the hard no-swap rule on a cgroup-v2 systemd host. Configure
`MemoryHigh` as an early host-specific pressure signal and `MemoryMax` below the
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
- cgroup `memory.current`, `memory.events`, and `memory.swap.current`.

Pass requires daemon and cgroup swap to remain zero, `pswpout` not to increase,
and no `oom`, `oom_kill`, or `max` event. Historical host SwapUsed is not by
itself a fastdup failure; attribute it using the process and cgroup counters.

The 2026-08-16 bounded-dirty 50-ISO run met these memory checks with a 2.40-GiB
peak and more than 19 GiB of available-RAM margin. It did not meet throughput or
the ten-second commit SLO because growing files are still re-chunked from byte
zero at each pressure checkpoint. See the
[intensive FUSE benchmark](../benchmarks/io-intensive-fuse-600s.md).
