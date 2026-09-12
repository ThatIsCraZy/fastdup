# Unified read cache and aligned storage qualification — 12 September 2026

Implementation of [ADR 0046](../adr/0046-bound-verified-read-cache-by-live-memory-headroom.md)
and [aligned storage v1](../specs/aligned-storage-v1.md). Pre-stable repositories
require rebuilding; the current Repository Format Epoch is two. The
[research note](../research/unified-read-cache-2026-09-12.md) records the
primary-source investigation and XFS unaligned-truncate observation.

## Implementation covered

One process-wide engine owns reusable DATA representations, historical proofs,
Container descriptors, Metadata objects, Manifest nodes, Exact/Similarity pages,
Similarity fences, Exact membership filters/page bounds, GC reverse-dependency
projections, encoded ranges, length-head views and retained file handles. Typed
namespaces share its directory, admission, replacement and byte ledger.
Production registers one `unifiedRead` memory-broker lease; `codecBuffers`
retains reusable allocation buffers with no object identity or reusable stored
content. Active roots, running proofs, dirty bytes and in-flight decoding remain
working state.

Online GC reuses warm content. Catalog scans read at most 4,096 rows per batch
and decline admission. Recovery, scrub and fresh physical publication/deletion
checks bypass every cache and shared in-flight result. Arbitrary logical I/O
becomes aligned Direct I/O, including Metadata, WAL, control records and small
publications. All regular FUSE handles use Direct I/O.

## Correctness and integration

- Six affected Rust crates completed **899 tests, zero failures, 28 ignored**
  across 103 test targets. Ignored tests are separate manual benchmarks and
  crash/soak scenarios. Log: `.artifacts/tmp/unified-qualified-tests.log`.
- Catalog Scan-intent behavior was also checked with all eight catalog
  integration tests and all 59 end-to-end maintenance tests. Log:
  `.artifacts/tmp/unified-gc-scan-tests.log`.
- The 14-test direct-publication suite rejects buffered
  configuration before root creation and measures resident pages for small
  and large native io_uring publications. Log:
  `.artifacts/tmp/unified-direct-publication-tests.log`.
- `cargo check --workspace --all-targets` passed, including control and benchmark
  crates. Production-library Clippy passed with warnings denied for store,
  io_uring, appliance and POSIX. Logs: `unified-workspace-check.log` and
  `unified-lib-clippy.log` in `.artifacts/tmp`.
- All 14 telemetry UI tests and TypeScript checking passed. The wider UI run
  passed 49 of 50 tests; the unrelated session-language test raced the DOM
  language effect. Its isolated App suite then passed all 25 tests. This does
  not claim a stable green result for the entire UI suite. Logs:
  `unified-ui-detail-tests.log`, `unified-ui-types.log`, `unified-ui-tests.log`
  and `unified-ui-app-tests.log` in `.artifacts/tmp`.
- An exploratory all-target Clippy run encountered existing test-only lints in
  `candidate_read_gate.rs`, `seqcdc.rs`, `similarity_simd.rs` and
  `persistent_reduction_gate_tests.rs`. The green Clippy claim is limited to
  production libraries.

Regression cases cover cross-class eviction, concurrent admission, shared
backing charged once, reader ownership surviving eviction, pressure, nested
Scan/Independent scopes, successful and failed shared misses, and Independent
reads refusing another reader's in-flight result. Compressed siblings reserve
their combined admission before insertion, preventing them from evicting one
another within the same read. The workload-change regression requires only its
first new read to reach DATA. A separate regression invalidates warm raw ranges
without changing any filesystem bytes or timestamps, then checks a failed
partial mutation. Mutable Reduction Heads always read current storage.

GC tests require warm graph reuse with no additional Metadata-object backend
reads, invalidate liveness on a new Commit, evict reverse-dependency projections
while a proof still holds them, and rebuild after eviction. Existing retirement
barriers, pin drain, generation binding and publication/unlink fault tests pass.

Physical envelope tests cover unaligned reads/writes, holes, short logical EOF,
shrinking/re-extension, payload extension before its length head, a torn alternate
head, two invalid heads, equal-sequence forks, oversized lengths, nonzero reserved
fields with a valid CRC, and sequence zero. A warm Manifest with both physical
head checksums damaged remains available from its verified cache view, while
scrub, publication readback and recovery reject the physical corruption.
Recovery returns `InvalidData` for the broken envelope; it does not reinterpret
it as an absent or legacy object. Separate logical-corruption tests retain the
existing valid-generation fallback behavior.

## Actual Linux page-cache observation

The automated direct-storage test writes 5,003 logical bytes, shortens to 73,
synchronizes, reads an unaligned 67-byte range, then requires `fincore` to report
**zero resident file-content bytes**. No eviction advice or cache dropping is
used. Native io_uring publication is exercised on real rings.

A live FUSE gate mounted the current daemon, wrote a deterministic 1,048,647-byte
payload and a small file, shortened the small file to 73 bytes, extended it to
129 with zero-filled bytes, and verified both through repeated read-only opens.
It stopped the daemon cleanly, restarted it on the same repository and required
byte-exact recovery of both files.

| Observation | Regular files checked | Resident file-content bytes |
| --- | ---: | ---: |
| First mounted process, after writes and reads | 13 | 0 |
| Restarted process, after recovery and reads | 31 | 0 |

The second observation includes native DATA Containers, Metadata objects, Exact
Runs/Run Sets, WALs, pool identity/lease/control records, a Recovery Checkpoint
and both FUSE files. Initial frontend reads can still use dirty working state;
the restart establishes durable publication and recovery through repository
readers. The command was `fincore --bytes --noheadings --output RES FILE` for each
regular file. It neither reads file contents nor drops cached pages.

Environment: Linux `6.12.0-211.50.1.el10_2.x86_64`, Rust `1.97.1`, util-linux
`2.40.2`, XFS with 4,096-byte blocks. The FUSE gate used a workspace-local
512-MiB XFS loop image with Direct I/O enabled on the loop device and 4,096-byte
sectors. Metadata and DATA used the existing lab isolation override. The main
workspace filesystem lacked the free space for the daemon's normal 10% physical
reserve; the isolated filesystem preserved that admission rule.

Generated evidence: `.artifacts/tmp/unified-fuse-gate.py`,
`.artifacts/tmp/unified-fuse-gate.log`, and daemon logs in
`.artifacts/tests/unified-fuse-1789229379143926785/`. Both daemons exited
successfully. FUSE/XFS mounts were detached, the loop device released and its
temporary image removed.

## Reproduction and limits

Create `.artifacts/target` and `.artifacts/tmp`. Every Cargo invocation uses:

```sh
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
cargo test -p fastdup-format -p fastdup-store -p fastdup-io-uring \
  -p fastdup-appliance -p fastdup-posix -p fastdup-testkit --tests --no-fail-fast
cargo check --workspace --all-targets
cargo clippy -p fastdup-store -p fastdup-io-uring \
  -p fastdup-appliance -p fastdup-posix --lib -- -D warnings
```

Tests require supported XFS geometry, `statx` Direct-I/O alignment reporting,
working io_uring and util-linux `fincore`. The separate live FUSE gate also
requires mount/loop privileges and `/dev/fuse`.

This qualifies file-content caching on the tested Linux/XFS stack. VFS/XFS
metadata caches and device/controller caches remain outside application cache
ownership. There is no claim of hardware power-loss qualification, a completed
random SIGKILL soak, SMB/Veeam throughput improvement, or a bound on all process
allocations. Reader-held views still consume working memory after eviction.

Generic files incur 8 KiB plus tail padding; extensions synchronize each
length-head update. GC replacement estimates include this overhead. The unified
DATA adapter retains its admitted compressed/decoded representation; the former
private hot-promotion/cold-demotion controller remains a historical test
comparator. Offline Similarity audits use up to 32 MiB of freshly decoded entries
for their own cross-reference pass and release it at the end; larger inputs keep
the bounded reread path. No new unsafe code was added.
