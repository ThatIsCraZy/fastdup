# ADR-to-code audit, 2026-08-27

This audit compares every accepted, proposed, or superseded record in
`docs/adr/` with the current workspace. “Current” means that the implemented
scope agrees with the decision; it does not claim that every production or
hardware evidence gate is closed. Historical ADR text is not silently rewritten.

## Result

- 84 uniquely numbered ADRs exist. ADR 0024's five independent capacity and
  placement decisions are split into ADRs 0080 through 0084.
- DATA-tier Recovery Checkpoints are now current across writer, recovery,
  scrub, GC, fault injection, and daemon scheduling.
- Pool/Appliance identity and fixed roles are current across startup, offline
  Scrub, durable format, corruption checks, and fault injection. The material
  implementation gaps remain hard quota isolation, commit-capacity admission,
  Small-File placement, and per-handle userspace prefetch. Physical-HDD evidence
  for restore coalescing and broader Advanced-Reduction gates remain open.
- The pre-production compatibility conflict found by this audit is closed:
  Commit epoch zero, absent-high-water migration, superseded object decoders,
  direct Similarity Runs, and oversized single-slot WAL inputs are rejected or
  removed. Current repositories are born directly in the current formats.

## Complete inventory

| ADR | State against code | Evidence / qualification |
| --- | --- | --- |
| 0001 | Current architecture | Redundancy is outside the repository format; no in-process RAID layer exists. Hardware qualification remains external. |
| 0002 | Current | Durable formats use explicit versions and field-wise encoders in `fastdup-format`; compatibility is still explicitly pre-release. |
| 0003 | Current | Publication and Commit paths preserve file and directory durability barriers rather than treating `fsync` as stronger hardware truth. |
| 0004 | Current | Normal Commit recovery and a separately scheduled 90-second DATA-tier disaster-recovery point are implemented; blocked HDD publication does not hold the Commit lock. |
| 0005 | Current | Commit generations preserve wholly committed ingest prefixes. |
| 0006 | Current | Namespace visibility is one immutable Root plus one Commit Record. |
| 0007 | Current | The live namespace retains acknowledged mutations ahead of the durability cut. |
| 0008 | Current, narrowed by 0060 | Container v2 is self-validating; structural BLAKE3 plus Record CRC and decoded Chunk BLAKE3 replaced a whole-image payload rehash. |
| 0009 | Current | independent and multi-Chunk Encoding Records are separate from logical Chunk identity. |
| 0010 | Current boundary | Persistent Prefix/Delta dependency depth is one. Depth two remains excluded. |
| 0011 | Current | Immutable Manifest leaves/inner nodes and opaque Root IDs are implemented. Small-file placement mentioned here remains open under ADR 0024. |
| 0012 | Current | One Namespace Root is selected per Commit. |
| 0013 | Current | path-local replacement, truncate, and splice bound random-update rechunking. |
| 0014 | Current abstraction; historical default | Region-local Chunking Profiles remain valid. FastCDC as the initial profile is superseded by SeqCDC in ADR 0054. |
| 0015 | Current | Exact remains nonauthoritative; selected Locations are verified and scans remain the correctness fallback. |
| 0016 | Partly superseded by 0077 | Compression Region and Placement Window bounds remain. Physical similarity reordering is no longer a production placement policy. |
| 0017 | Design open | Dictionary IDs and dependency rules exist only in the bounded experimental pipeline; no durable production Dictionary activation exists. |
| 0018 | Current | Similarity lookup is bounded/versioned and is candidate acceleration only. |
| 0019 | Current | Commit follows durable DATA and Metadata publication. |
| 0020 | Current implemented scope | Paired heads select current/previous self-contained checkpoints; writer, bounded reader, exact recovery, scrub, GC retention, daemon scheduling, and exhaustive publication/installation fault tests exist. Automatic `lost+found` discovery remains a separate optional design. |
| 0021 | Current | Exact Location transitions implement ACTIVE, RETIRING, and REMOVED RoW state. |
| 0022 | Current | ASSERT/VERIFY/AUDIT language and independent scrub boundaries are used consistently. |
| 0023 | Current | Exact and Similarity rebuild as immutable generations; after complete Metadata loss the selected DATA-tier checkpoint is installed and the applicable index generation is rebuilt before mount. |
| 0024 | Superseded by 0080–0084 | The former combined decision is retained only as a pointer to independently verifiable invariants. |
| 0025 | Current process | Fault matrices and benchmark corpora exist under testkit/docs; hardware gates remain explicit rather than claimed. |
| 0026 | Historical platform wording | Deep crate boundaries and field-wise formats remain. The “future SIMD/FUSE/io_uring” text is fulfilled by isolated SeqCDC, FUSE, mmap, ioprio, and io_uring modules; ADR 0058 supersedes the synchronous Stage-1 publisher. |
| 0027 | Current | POSIX edge semantics are centralized in `fastdup-posix`. |
| 0028 | Current deployment contract | XFS/stable-storage assumptions remain external qualification requirements. |
| 0029 | Partly superseded by 0074 | Object decode-version separation remains. Predecessor Policy Sets, read-only downgrade, and policy migration are excluded for the pre-production repository. |
| 0030 | Partly implemented | bounded record/Base amplification, verified caches, candidate fallback, and maintenance priority exist. Per-handle userspace prefetch and its metrics do not; kernel readahead under ADR 0073 is not the same mechanism. |
| 0031 | Current | RoW history is not exposed as snapshots. |
| 0032 | Historical milestone | POSIX/Exact MVP, advanced reduction/GC, and DATA-tier DR now exist. FastCDC is superseded by ADR 0054; Small-File production scope remains open. |
| 0033 | Current | model and FUSE share the Namespace dispatch seam. |
| 0034 | Current | Inode IDs are durably reserved before visibility. |
| 0035 | Implemented, evidence gates open | immutable sorted Runs, activation, partitioned compaction, filters, rebuild, bounded lookup, fallback, and fault tests exist. The ADR correctly remains `proposed` because corpus throughput and write-amplification gates are still open. |
| 0036 | Current core; stale details | Successor DATA proofs and path-local Manifest publication exist. FastCDC/LRU text is superseded by ADRs 0054/0051, and the “deferred” cross-process lease is implemented by ADR 0069. |
| 0037 | Current | structural recovery and current DATA graph proof remain separate. |
| 0038 | Superseded by 0072 | Paired-envelope proof remains diagnostic; it no longer initializes writable allocator state. |
| 0039 | Current | paired Commit slots are bounded from creation; recovery and scrub reject old single-slot inputs. |
| 0040 | Current core; historical names | streaming Container prepublication and Commit coalescing exist. FastCDC wording is SeqCDC now; production DATA publication is io_uring under ADR 0058. |
| 0041 | Current core; historical names | Active/Frozen overlap and bounded Ingest Lanes exist. FastCDC names should be read as SeqCDC under ADR 0054. |
| 0042 | Current v2 | authenticated allocation summaries drive truncate/splice; writer and reader accept only the current inner-node format. |
| 0043 | Current implemented scope | metadata-only range clone is implemented; sparse-source clone remains deliberately deferred. |
| 0044 | Current | paired Exact activation slots are bounded from creation; former single-WAL input is unsupported. |
| 0045 | Current | Exact compaction uses key-disjoint Run families and bounded partition targets. |
| 0046 | Current | verified DATA cache, Exact-page cache, descriptor cache, process-only Swap admission, huge-page-backed dense filters, and telemetry are implemented. Its long io_uring history is explicitly superseded by ADR 0058. |
| 0047 | Design open | bounded Dictionary training/family experiments exist, but durable activation and production write-through are still gated. |
| 0048 | Partly superseded by 0064 | pressure and replacement-before-delete rules remain; full scrub is no longer required before every online DATA-GC proof. |
| 0049 | Current | offline mixed-Container RoW compaction and transition publication exist. |
| 0050 | Current | reduction stages use bounded Rayon work and shared memory/CPU limits; no unbounded pipeline is introduced. |
| 0051 | Current | Historical Proof Cache uses bounded S3-FIFO rather than the LRU described in ADR 0036. |
| 0052 | Current, gate remains off | predictor implementation and telemetry exist; production Store entry points intentionally pass `IncompressibilityGatePolicy::Off` until evidence promotes it. |
| 0053 | Current | prehashed/owned buffers flow into publication; fallback copying and telemetry remain explicit. |
| 0054 | Current | SeqCDC-v1 is the active profile with scalar/SIMD differential coverage. |
| 0055 | Correctly superseded | ADR 0056 replaced exact whole-image publication proof. |
| 0056 | Correctly superseded | ADR 0059 replaced sampled writer rereads with writer-work trust until independent read. |
| 0057 | Current | two publication states may overlap for one active inode under bounded ownership. |
| 0058 | Current | production DATA storage requires the CQE-driven io_uring adapter; no setup fallback is admitted. |
| 0059 | Current | writer-owned prehashed evidence is trusted only inside publication; recovery/read/scrub independently verify. |
| 0060 | Current | Container v2 structural commitment excludes payload but binds every durable coordinate and CRC. |
| 0061 | Current | Similarity and GC catalog mmap readers hold immutable file leases; positional fallback and independent scrub remain. |
| 0062 | Current | one verified pool scan builds paired Exact/Similarity generations. |
| 0063 | Current | write-through pins one coherent Exact/Similarity pair and bounds Prefix Base trials. |
| 0064 | Current | catalog discovery is nonauthoritative; local proof binds Commit pair, root pins, dependency closure, retirement barrier, and restart finalization. |
| 0065 | Current | adaptive Online GC uses isolated idle I/O workers; offline full-speed requires lease/exclusivity. |
| 0066 | Current | Metadata GC marks Commit graphs plus live root pins under the publication barrier. |
| 0067 | Current | persistent Metadata marks are acceleration and are re-proved after start. |
| 0068 | Current | additive Metadata deltas and exact-mark fallbacks exist; unclassified publication is named directly and Metadata Mark readers accept only v2. |
| 0069 | Current | one kernel-backed Appliance Lease precedes repository mutation. |
| 0070 | Current | Recovery Latch and startup/shutdown ordering are implemented and tested. |
| 0071 | Current | every Commit is v2/epoch one; epoch zero and unknown epochs fail before mutation. |
| 0072 | Current | paired high-water slots initialize only in an empty DATA repository; missing state beside Containers fails closed. |
| 0073 | Current | read-only kernel caching, direct writable handles, explicit DATA invalidation, and adapter tests exist. |
| 0074 | Current | only the current Policy Set and current pre-production durable formats are accepted. |
| 0075 | Current | Prefix Base recovery uses bounded envelope, Recovery Index, selected-record reads, and pass-local Base caching without Container v3. |
| 0076 | Current | Similarity snapshots are partitioned by complete BucketKey ranges; singleton and multipart snapshots both use immutable family manifests. |
| 0077 | Current | Verified Read Plan groups shared Records, coalesces directly adjacent Records up to 1 MiB, prefers same-Container forward Locations, preserves logical order, verifies every Record independently, and leaves the single-extent hot path scalar. |
| 0078 | Current | Workspace and production builds target x86-64 only; scalar implementations remain differential or unsupported-host guards where required. |
| 0079 | Current | Active filesystem Exact Runs use fully audited leased mappings plus compact page-key bounds; candidate pages retain the memory-governed decoded cache, while adapters and offline scrub remain positional. |
| 0080 | Current | Checksummed current-only Pool records bind distinct Metadata/Data Pool IDs to one Appliance ID and fixed roles. Daemon startup and offline Scrub reject missing populated, corrupt, foreign, duplicate, swapped, and non-regular identities; exhaustive first-publication fault tests recover a valid pair. |
| 0081 | Design open | Hard filesystem/project-quota isolation for Metadata reserve, Small-File data, and cache is not yet enforced or qualification-tested. |
| 0082 | Design open | Pessimistic per-mutation Metadata/DATA capacity reservation before acknowledgement is not yet implemented. |
| 0083 | Current | Cached five-second physical tier sampling, ten-percent reserve reporting, Metadata gating, and validated reporting-only overrides are implemented outside the hot loop. |
| 0084 | Design open | Durable Small-File placement, the 8 MiB spill hysteresis, and separate quota behavior are not implemented. |

## Required follow-up

1. Implement and qualify ADR 0081's physical quota isolation and ADR 0082's
   bounded commit-capacity admission before Small-File placement under ADR 0084.
2. Treat speculative userspace read-ahead as an evidence-gated follow-up to
   ADR 0077. Do not claim ADR 0030 complete merely because the kernel page cache
   performs its own readahead.
