# Exact Run membership v1

The active Exact Index now rebuilds a cache-line-blocked Bloom hint for each
Run that fits the active-set memory budget. This benchmark checks whether the
hint removes enough verified Exact-page work to justify its RAM.

## Workload

- Rocky Linux 10.2 minimal ISO, SHA-256
  `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`;
- three serial writes of the same 2,072,444,928-byte file through Samba and the
  fastdup FUSE mount;
- fresh metadata and DATA repositories on separate XFS devices;
- all three files live for the repository measurement;
- 12-second post-write settle; and
- synchronous DATA publisher, zero host/cgroup/daemon Swap.

The pre-filter comparison is
`.artifacts/benchmarks/smb-single-stream-final.json`. Two filter-enabled runs
are `smb-single-stream-run-membership.json` and
`smb-single-stream-run-membership-repeat.json` in the same artifact directory.
Artifacts are deliberately outside source control.

## Results

| Metric | Pre-filter | Filter run 1 | Filter run 2 |
| --- | ---: | ---: | ---: |
| Aggregate SMB write MiB/s | 147.90 | 300.10 | 226.21 |
| Completed-file p99/max ms | 14,271.91 | 7,352.13 | 10,163.58 |
| Daemon CPU seconds | 80.68 | 39.10 | 52.80 |
| Peak RSS bytes | 2,482,638,848 | 2,486,640,640 | 2,326,065,152 |
| Exact-page hits | 4,905,230 | 1,233,326 | 1,223,847 |
| Exact-page misses | 14,292 | 2,833 | 2,675 |
| Membership probes | n/a | 702,723 | 702,102 |
| Definitely absent | n/a | 572,327 | 572,394 |
| Final active filter bytes | 0 | 41,088 | 41,216 |
| Data-reduction ratio | 2.840 | 2.904 | 2.889 |
| Peak daemon Swap bytes | 0 | 0 | 0 |

The two enabled runs rejected 81.44% and 81.53% of Run probes before any Exact
page lookup. Compared with the pre-filter run, total Exact-page accesses fell
74.87% and 75.07%, while misses fell 80.18% and 81.28%. The final active-set
filter footprint was only about 41 KiB.

End-to-end throughput improved by 103% and 53%, and daemon CPU fell by 52% and
35%. These wall-clock numbers are supporting evidence, not an isolated causal
estimate: faster processing changes checkpoint cuts and physical write timing,
and the two virtual disks share host resources. The stable evidence is the
roughly 75% reduction in instrumented Exact-page accesses across both runs,
with byte-exact completion, similar data reduction, bounded RAM, and zero Swap.

## Decision

Keep the blocked Bloom hints enabled. They reuse the existing reduction filter,
add no on-disk format, never authorize a Location, and provide a large measured
reduction in metadata lookup work for negligible RAM on this workload. Continue
reporting filter bytes and absent/maybe probes in every SMB benchmark so a
future workload with poor selectivity can be detected.
