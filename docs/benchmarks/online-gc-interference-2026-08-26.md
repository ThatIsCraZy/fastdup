# Online-GC write-interference checkpoint, 2026-08-26

The Release FUSE appliance was mounted with Metadata and DATA on separate XFS
filesystems. The real `online_gc_interference` harness copied the existing
256-MiB Rocky prefix 20 times per phase and called `fsync` for every completed
file. The second phase requested one urgent Online-GC quantum per second through
the daemon-owned control socket.

| Measure | Baseline | Online GC |
| --- | ---: | ---: |
| completed-write p50 | 733.755 ms | 747.697 ms |
| completed-write p99/max | 815.988 ms | 810.013 ms |
| Online-GC requests | 0 | 13 successful, 0 failed |

The observed p99 regression was zero basis points on this host. This is a
checkpoint, not a portable performance guarantee. An intentionally abusive
unthrottled control run issued 106 urgent quanta during 20 writes and increased
p99 from 843.676 ms to 1,028.271 ms (+2,187 basis points); this demonstrates why
normal operation uses adaptive admission rather than treating repeated manual
requests as free.

After both phases removed all test files, the appliance was restarted and
online `gc-now` was repeated until `no_candidates`. Two bounded quanta removed
18 Containers / 245,862,400 bytes and 24 Containers / 3,444,736 bytes. Offline
Scrub then reported `containers=0`, `container_chunks=0`, and
`exact_active_locations_verified=0`.

Reproduction:

```text
cargo run --release -p fastdup-appliance --example online_gc_interference -- \
  --iterations 20 --gc-interval-ms 1000 \
  MOUNT METADATA_ROOT SOURCE
```

Every Cargo artifact and test repository used for this checkpoint remained
under `/source/fastdup/.artifacts/`.
