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

Validation passed: 657 Store/Appliance tests and 81 maintenance/dependent-Base
tests (738 total; 20 pre-existing ignored tests). Store library Clippy and
the focused dependent-Base integration target pass with warnings denied.
Formatting and whitespace checks pass. The full Store test build still emits
the pre-existing unused `progress` variable warning in the Exact-index tests.

Historical evidence is under `.artifacts/gc-cpu-20261023/`. Regression logs,
the alternating comparison and subsequent package/live validation are under
`.artifacts/gc-base-lookup-20261023/`.
