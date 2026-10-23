# Online-GC Base lookup, 2026-10-23

## Observed work

Runtime 3945699 on RPM 48 showed mean host CPU 54.1%, peaking at 97.1%,
between 04:14 and 04:17 CEST. DATA averaged 1.92 MB/s reads and Metadata
0.006 MB/s. Over 179 seconds, the unified read cache recorded 215,919,539
hits and 86,298,955 misses. These are cache operations, not device IOPS.
Scrub was complete; reduction, read-cache compression and checkpoint counters
did not advance. The manually requested GC was still running when the later
RPM 49 update shut it down cooperatively. Its old process was unavailable
for a retrospective CPU profile.

The candidate proof pinned an Exact generation but used the index-free
`read_verified_image` reader. For each distinct dependent Base in a victim,
that reader searched published Containers in name order and verified their
Recovery Indexes until finding the Base. Cached encoded ranges avoid physical
reads but still incur lookup, copying and Recovery Index validation work.
The mechanism is reproduced below; the historical CPU percentage alone does
not establish how much CPU each function consumed.

## Change and trust boundary

Online candidate-proof workers now borrow the same pinned Exact generation
to locate independent Bases. A successful hint still requires a fresh Base
payload read, Record verification and Chunk identity check. Only ACTIVE,
dependency-free Locations qualify, with the existing two-attempt bound.
Index errors, absent keys and unusable candidates retain the verified
Container scan. Both paths share the existing decode-local Base memo, so
repeated references to one Base do not repeat index or payload work.

The victim image is still fully decoded and verified. Proof liveness, Base
pins, canonical accounting, replacement-before-retirement, cancellation and
unlink are unchanged. Offline/recovery readers retain their index-free path.
There is no new resident cache, format change or scheduling/worker-limit
change. The public proof methods require a thread-safe index backend because
scoped proof workers now borrow it.

## Regression and comparison

`online_gc_dependent_proof_does_not_scan_unrelated_containers_for_bases`
exercises the complete online proof, collection and final scrub. Its eight
distinct codec-5 Bases follow 64 unrelated Containers in canonical order.
Before the change it fails the read bound: 1,596 DATA reads and one namespace
enumeration. The first fixed run needed 28 reads and no enumeration; repeated
runs with resident descriptor hints needed 12. The asserted bound is 96,
independent of descriptor residency.

Five alternating before/after runs used the same deterministic MemoryStorageIo
fixture. The baseline is `e7772ea`, built in a separate worktree and target
directory; only the expected operation counts in its copied regression were
changed to allow the baseline to complete. Median proof time was 31.946 ms
before and 6.612 ms after. All baseline runs performed 1,596 reads and all
comparison runs performed 12. This small debug-build experiment measures
algorithmic work, not physical IOPS or a production wall-time guarantee.

Additional tests cover one indexed Base shared by 128 targets, byte equality
with the scan reader, a changed Base payload after a successful read, corrupt
Exact data with scan fallback, a missing Base after index damage, and absent,
wrong-location, RETIRING and REMOVED candidates.
Two dependent victims sharing a Base also produce identical proof accounting
with one or four configured readers, followed by successful collection and scrub.

Validation passed before packaging: 657 Store/Appliance tests and 81
maintenance/dependent-Base tests (738 total; 20 pre-existing ignored tests).
The subsequent review added five passing regressions (743 distinct tests in
total); the seven focused Store verifier tests and all 14 dependent-Base
integration tests passed again. Store library Clippy and
the focused dependent-Base integration target pass with warnings denied.
Formatting and whitespace checks pass. The full Store test build still emits
the pre-existing unused `progress` variable warning in the Exact-index tests.

Historical evidence is under `.artifacts/gc-cpu-20261023/`. Regression logs,
the alternating comparison and subsequent package/live validation are under
`.artifacts/gc-base-lookup-20261023/`.

## Review and remaining limits

Muse Wonder Full 1.0 at xhigh reviewed the implementation read-only and
confirmed the hint/authority boundary and scoped generation-pin lifetime.
Its captured diff preceded the final fault tests. In response, the final
suite additionally covers:

- A truncated Exact file after pinning: lookup returns an I/O error, but
  verified Container scanning still reconstructs the victim correctly.
- A corrupt first indexed Base copy with a healthy second independent copy.
- A sole corrupt Base: proof fails before any DATA retirement; an unrelated
  committed file remains readable.
- An activated index without the subsequently published Base: one namespace
  enumeration, successful collection and clean scrub.
- An index containing only a dependent Location for the requested Base.
- A cached, subsequently unlinked victim: the indexed verifier reports missing
  DATA. The old verifier does too under the actual GC `Scan` intent; its
  `ContainerImageCache::get` already rejects non-Demand reads. Thus the review's
  suggested cache-related production behavior delta does not occur.

Successful hint lookup can avoid an unrelated damaged Recovery Index that the
old namespace scan would have visited first. The GC still verifies its victim
and required Base bytes; global integrity checking remains the scrub's job.
Index faults are intentionally non-authoritative and fall back silently.
Dedicated hint-hit/fallback counters remain a telemetry follow-up. Existing
unified-cache and metadata-read counters cannot directly identify that ratio;
worker Exact reads currently have `indexLookup` attribution.

The decode-local Base memo and its memory accounting are unchanged. Decoded
Base bytes are not included in the physical victim-image admission budget;
this pre-existing accounting gap remains a follow-up. The private hint seam
still carries verified bytes as `Vec<u8>` from its sole gated producer;
carrying `VerifiedChunkPayload` through the seam is optional type hardening.

## RPM 50 deployment

The clean build at `cd30c755a6530a24e3db63d5c323bec1135f0375` produced
`fastdup-0.8.1-50.el10.x86_64.rpm`, SHA-256
`04b81a0e98b87501e70ae6d28a437644338e39734aaa5e4b066712038613f246`.
The UI was rebuilt before embedding it into Control Plane. Later changes
only add tests, correct a comment and record this evidence; they do not
change the installed runtime implementation.

The deployment helper upgraded VM `10.1.1.161` from RPM 49 to 50 between
04:49:22 and 04:50:34 CEST. Its terminal record is `success`:
`.artifacts/test-vm-update/20261023T024825Z-207e405362/job.json`.
The three FastDup backup jobs were stopped before the update and before the
manual GC. No new backup job was started for this validation.

Verified after deployment:

- Repository PID changed from `3978184` to `3998074`; installed and running
  Runtime, Agent and Control Plane hashes match the package. The installed
  maintenance executable also matches. Full hashes are in
  `package-provenance.json` and `processes-and-history-after-50.json`.
- Repository, Agent, Control Plane, Samba and Veeam container were active;
  the container's four Veeam services were active. Agent reported `online`,
  no runtime issue and current historical latency/workload telemetry.
- All five publicly served UI files, including `index.html`, match the
  freshly built files byte for byte. Evidence: `ui-served-after-50.json`.
- Startup scrub completed with 12,128 Containers: 12,124 certificates resumed
  and four Containers newly verified, with no error. No frontend read/write
  errors or integrity failure were reported.
- Host and Veeam container see the same FUSE mount; two VBM file-prefix
  hashes match. Twenty-four 64-KiB samples at the start, middle and end of
  eight existing VBK files were read successfully and retained as the
  pre-GC baseline in `backup-data-before-gc.json`.

## Live GC validation: interrupted observation

One online `gc-now` request started at 04:54:49 CEST after the startup scrub
completed. `gc-live.jsonl` contains observations through 04:54:58 only; no
terminal GC result has been received. At that point the runtime still reported
`running`, no frontend errors and no integrity failure. The first nine seconds
were Metadata reads without DATA reads, so they do not validate the optimized
dependent-Base proof or establish a production CPU improvement.

Subsequently both FastDup (`10.1.1.161`, SSH and HTTPS) and the separate Veeam
server (`10.1.10.110`, HTTPS) timed out from the build environment. Public Git
access still worked. This suggests a shared internal network/VPN interruption;
it is not evidence of a FastDup process crash. The already started GC was not
cancelled, restarted or duplicated. Evidence: `reachability-interruption.json`.

Pending once connectivity returns: inspect the existing GC result and service
health, recover any missing historical measurements, then repeat the 24 backup
samples and compare every size/offset/hash with the pre-GC baseline. Do not
start another GC until the first request's state is known. Live GC completion,
post-GC data equality and the production CPU effect remain unverified.
