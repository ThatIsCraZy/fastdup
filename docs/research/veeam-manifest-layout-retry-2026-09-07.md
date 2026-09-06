# Veeam checkpoint retry diagnosis — 2026-09-07

## Live evidence before any deployment

The test VM still ran fastdup 0.6.4-11, PID 44717, started 23:35:07 CEST with
zero service restarts. Checkpoint 339 completed at 00:02:17. At 00:02:23 the
five-second watchdog closed mutation admission. Attempts failed at 00:12:36
and 00:23:47 with `Metadata(InvalidObjectLength(20620288))`. The supervisor
retried the frozen cut without advancing the committed generation.

A later 15.010-second read-only sample measured 43.75 MB/s physical process
reads and zero physical writes, with about 276.6 MB/s userspace read bytes.
Active workers were reading DATA Container files; the management socket still
responded. RSS was approximately 14.9 million KiB and process Swap was zero.
Host Swap existed but was not Swap charged to the repository process. This was
an alive, write-blocked runtime doing repeated work, not a service crash or VPN
throughput measurement. No cache changes were installed during diagnosis.

## Reproducers and changes

A 322,191-extent append reproduced exactly
`ManifestTree(Metadata(InvalidObjectLength(20620288)))`:
322,191 * 64-byte entries + a 64-byte Manifest header. The tree writer first
constructed one temporary Manifest Leaf for the complete append before
partitioning it. That confused an in-memory logical layout with a physical
Metadata Object. Complete checkpoint layouts and splice/replacement validation
had equivalent whole-layout leaf construction.

`ManifestLayout` now validates extent semantics and a complete checked logical
partition independently of physical leaf size. The tree writer partitions
complete layouts and append/splice sequences into its existing bounded leaves.
Physical leaf encoding/decoding, Metadata Object limits, content IDs, successor
DATA dependency proofs, WAL visibility, recovery and scrub remain unchanged.
The exact reproducer now publishes and scrubs successfully. A complete-layout
fixture with the same extent count covers the corresponding new-file path.

A second fixture used a 262,144-extent predecessor, rewrote its header and
shrunk its length in the same cut. The previous planner fell back to complete
file reconstruction and issued 1,038 Metadata reads. Shrink with dirty ranges
now composes touched-path replacements and authenticated truncation. A cutoff
inside DATA re-encodes only the retained prefix; the temporary discarded suffix
is HOLE until the structural truncate. The fixture now needs fewer than 64
Metadata reads and recovers byte-exact header and EOF data. Fault injection
checks old versus complete new header/length around every Metadata operation,
including a cutoff inside DATA. Existing mixed-growth and DATA-fault suites
continue to cover successor dependency retention.

These reproducers establish the rejected-layout bug and the unnecessary full
read path. The VM's existing binary did not include a per-inode failure trace,
so the precise client syscall sequence which produced its 322,191 extents is
not established by the log alone.

## Concurrent GC catalog failure

The VM additionally logged `GcCandidateCatalog(Format(RowCountMismatch))` at
00:08:38. GC bootstrap counted one directory listing and generated rows from a
second listing. A deterministic test pauses catalog creation after the count,
publishes another Container, then resumes bootstrap; the old code produces the
same error. Both count and row stream now use one sorted published-name
snapshot. Concurrent publication stays unblocked and later Containers belong
to later hint refreshes. No DATA liveness or deletion authority changes.
The repro and all 56 end-to-end maintenance tests pass.

## Validation and artifacts

Artifacts are under `.artifacts/adaptive-cache-budget/`: live initial log,
15-second samples, active worker syscalls and file descriptors, red/green
Manifest and catalog logs, format layout tests, complete appliance/store/format
library results, Namespace/recovery/fault integration results, Clippy and RPM
build logs. Separate cache measurements and UI behavior are documented in
`adaptive-cache-budget-2026-09-07.md`.

The release is 0.6.4-12. Existing repository data is not reformatted by the
update. A stalled old process must be replaced to execute the fixes; restarting
selects the last completely committed generation and cannot retain an
uncommitted in-memory cut from the stopped process.
