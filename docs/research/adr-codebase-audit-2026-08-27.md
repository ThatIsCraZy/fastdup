# ADR-to-code audit, 2026-08-27

This audit compares every accepted, proposed, or superseded record in
`docs/adr/` with the current workspace. “Current” means that the implemented
scope agrees with the decision; it does not claim that every production or
hardware evidence gate is closed. Historical ADR text is not silently rewritten.

## Result

- 77 uniquely numbered ADRs exist after renumbering the accidentally duplicated
  Similarity ADR from 0046 to 0076.
- 58 are current for their implemented scope.
- 8 are current designs with an explicit implementation or evidence gate still
  open.
- 7 contain historical wording already superseded by later ADRs and need only a
  current-state pointer, not a code change.
- 4 expose material documentation/code drift: DATA-tier Recovery Checkpoints
  and Small-File placement are not implemented; per-handle userspace prefetch is
  not implemented; and the repository still accepts legacy Format-Epoch and
  generation-allocation migration paths despite the pre-production
  no-migration decision.

The last item is the only correctness-policy conflict. It is not a live-data
compatibility requirement: `RepositoryFormatSupport::legacy_only`, Commit epoch
zero, the absent-high-water migration scan, legacy Metadata-GC classification,
and their tests are removable before production. This audit does not remove
that broad compatibility surface as a side effect of documentation review.

## Complete inventory

| ADR | State against code | Evidence / qualification |
| --- | --- | --- |
| 0001 | Current architecture | Redundancy is outside the repository format; no in-process RAID layer exists. Hardware qualification remains external. |
| 0002 | Current | Durable formats use explicit versions and field-wise encoders in `fastdup-format`; compatibility is still explicitly pre-release. |
| 0003 | Current | Publication and Commit paths preserve file and directory durability barriers rather than treating `fsync` as stronger hardware truth. |
| 0004 | Design open | Normal Commit recovery exists; the separate 90-second DATA-tier Recovery Checkpoint does not. Same gap as ADR 0020. |
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
| 0020 | Not implemented | No Recovery-Checkpoint writer, reader, recovery selector, or `lost+found` path exists in the crates. |
| 0021 | Current | Exact Location transitions implement ACTIVE, RETIRING, and REMOVED RoW state. |
| 0022 | Current | ASSERT/VERIFY/AUDIT language and independent scrub boundaries are used consistently. |
| 0023 | Partly implemented | Exact and Similarity rebuild as immutable generations; the disaster-recovery source checkpoint named by this ADR is absent. |
| 0024 | Partly implemented | `statfs` reserve sampling/override is implemented. Physical quota enforcement, Pool IDs, Small-File DATA placement, and spill hysteresis are not. |
| 0025 | Current process | Fault matrices and benchmark corpora exist under testkit/docs; hardware gates remain explicit rather than claimed. |
| 0026 | Historical platform wording | Deep crate boundaries and field-wise formats remain. The “future SIMD/FUSE/io_uring” text is fulfilled by isolated SeqCDC, FUSE, mmap, ioprio, and io_uring modules; ADR 0058 supersedes the synchronous Stage-1 publisher. |
| 0027 | Current | POSIX edge semantics are centralized in `fastdup-posix`. |
| 0028 | Current deployment contract | XFS/stable-storage assumptions remain external qualification requirements. |
| 0029 | Partly superseded by 0074 | Object decode-version separation remains. Predecessor Policy Sets, read-only downgrade, and policy migration are excluded for the pre-production repository. |
| 0030 | Partly implemented | bounded record/Base amplification, verified caches, candidate fallback, and maintenance priority exist. Per-handle userspace prefetch and its metrics do not; kernel readahead under ADR 0073 is not the same mechanism. |
| 0031 | Current | RoW history is not exposed as snapshots. |
| 0032 | Historical milestone | POSIX/Exact MVP and advanced reduction/GC now exist. FastCDC is superseded by ADR 0054; DATA-tier DR and Small-File production scope remain open. |
| 0033 | Current | model and FUSE share the Namespace dispatch seam. |
| 0034 | Current | Inode IDs are durably reserved before visibility. |
| 0035 | Implemented, evidence gates open | immutable sorted Runs, activation, partitioned compaction, filters, rebuild, bounded lookup, fallback, and fault tests exist. The ADR correctly remains `proposed` because corpus throughput and write-amplification gates are still open. |
| 0036 | Current core; stale details | Successor DATA proofs and path-local Manifest publication exist. FastCDC/LRU text is superseded by ADRs 0054/0051, and the “deferred” cross-process lease is implemented by ADR 0069. |
| 0037 | Current | structural recovery and current DATA graph proof remain separate. |
| 0038 | Legacy-only drift | Paired-envelope proof exists solely to migrate a repository without high-water slots. ADR 0074 plus the pre-production no-migration decision make this path obsolete. |
| 0039 | Current rotation; legacy drift | paired Commit slots and recovery are current. Single-file WAL migration language/code is obsolete; offline scrub is already implemented, not future. |
| 0040 | Current core; historical names | streaming Container prepublication and Commit coalescing exist. FastCDC wording is SeqCDC now; production DATA publication is io_uring under ADR 0058. |
| 0041 | Current core; historical names | Active/Frozen overlap and bounded Ingest Lanes exist. FastCDC names should be read as SeqCDC under ADR 0054. |
| 0042 | Current v2; compatibility question | authenticated allocation summaries drive truncate/splice. Readers still accept Manifest Inner v1, which should be removed if the no-prototype-data rule is applied to every object-local version. |
| 0043 | Current implemented scope | metadata-only range clone is implemented; sparse-source clone remains deliberately deferred. |
| 0044 | Current rotation; legacy drift | paired Exact activation slots are current. Former single-WAL migration code/text has no production data to serve. |
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
| 0068 | Current core; legacy clause obsolete | additive Metadata deltas and exact-mark fallbacks exist. `LegacyCommit` remains only because epoch-zero commits are still accepted. |
| 0069 | Current | one kernel-backed Appliance Lease precedes repository mutation. |
| 0070 | Current | Recovery Latch and startup/shutdown ordering are implemented and tested. |
| 0071 | Code/policy conflict | epoch-one writer/downgrade fence exists, but epoch-zero reader and upgrade path remain despite the later no-migration instruction. Keep the epoch field and unsupported-epoch rejection; remove epoch-zero compatibility. |
| 0072 | Code/policy conflict | paired high-water slots and 1,024-generation reservation are current. Absent-slot legacy scan/migration remains and should be replaced by first-repository initialization only. |
| 0073 | Current | read-only kernel caching, direct writable handles, explicit DATA invalidation, and adapter tests exist. |
| 0074 | Current policy, incompletely enforced | only the current Policy Set is accepted. The same no-prototype-data rule is not yet applied consistently to legacy Commit/high-water/activation/Manifest readers. |
| 0075 | Current | Prefix Base recovery uses bounded envelope, Recovery Index, selected-record reads, and pass-local Base caching without Container v3. |
| 0076 | Current | Similarity snapshots are partitioned by complete BucketKey ranges and use immutable family manifests. The old singleton migration clause is obsolete in a no-prototype-data repository. |
| 0077 | Current | Verified Read Plan groups one Record read/decode, prefers same-Container forward Locations, preserves logical order, and leaves the single-extent hot path scalar. |

## Required follow-up

1. Remove the epoch-zero and absent-high-water migration surface together, not
   piecemeal. Update writer, recovery, scrub, Metadata-GC classification, fault
   tests, and specs in one change. Also decide whether “no legacy data” covers
   Manifest Inner v1, Metadata Mark v1, Namespace Root v1, and singleton
   Similarity readers; the current code treats those object-local versions
   inconsistently.
2. Either implement DATA-tier Recovery Checkpoints as a complete writer,
   reader/recovery, scrub, and fault-injection slice, or mark ADRs 0004/0020 and
   the dependent part of 0023 as deferred. They must not read as delivered.
3. Split ADR 0024's delivered `statfs` decision from the still-open Pool-ID,
   physical-quota, and Small-File placement design before production planning.
4. Treat speculative userspace read-ahead as an evidence-gated follow-up to
   ADR 0077. Do not claim ADR 0030 complete merely because the kernel page cache
   performs its own readahead.

