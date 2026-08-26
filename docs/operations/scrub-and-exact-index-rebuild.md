# Offline scrub and Exact-Index rebuild

Status: implemented and deterministically fault-tested for the current
single-writer checkpoint formats.

Offline commands acquire the Metadata root's exclusive Appliance Lease before
opening either repository and fail if the writable daemon or another offline
process owns it. Stop and unmount `fastdup-durable-fuse`; `--offline` remains an
explicit acknowledgement that unrestricted maintenance scheduling is intended.

The writable daemon durably arms
`.fastdup-appliance.recovery-required` before ordinary repository access and
removes it only after a complete catch-up and clean unmount. If a crash or
failed shutdown leaves that latch armed, offline `scrub`, `scrub-gc`, and
`gc-now` may perform the required complete verification and clear it after
success. `rebuild-exact` and `metadata-gc` fail before mutation until that proof
exists. Unexpected latch bytes, symlinks, and other non-regular objects fail
closed.

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline scrub METADATA_ROOT CONTAINER_ROOT

cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline rebuild-exact METADATA_ROOT CONTAINER_ROOT

cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline scrub-gc METADATA_ROOT CONTAINER_ROOT

cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline gc-now METADATA_ROOT CONTAINER_ROOT

cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --offline metadata-gc METADATA_ROOT CONTAINER_ROOT

# Against the currently mounted writable appliance:
cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --online gc-now METADATA_ROOT
```

`scrub` is read-only with respect to repository generations and stored user
data; after a successful complete verification it may remove a pre-existing
Appliance Recovery Latch. It fails closed on a torn or invalid Commit-Log tail
and does not use the ordinary recovery fallback. It verifies:

- both bounded Commit-Log slots and their selected bridge topology;
- every generation retained by the selected bounded Log segment and every
  reachable Namespace Root, Manifest node, and subtree allocation summary;
- the complete union of DATA dependencies pinned by the current and immediately
  previous online generations; older Log records remain diagnostic transition
  history and never become implicit DATA snapshots;
- every canonically named published Container, including decoded records,
  Recovery Index, CRCs, Chunk tables, and BLAKE3 Chunk identities;
- both Exact-Index activation slots, the selected Run Set, every complete Run
  hash/page/order invariant, and cross-family Chunk-length identity; and
- every ACTIVE Exact-Index Location against its exact immutable Container
  record and decoded Chunk ID.

The command prints one `scrub_ok=true` line only after all checks pass. It
retains no complete Container payload set or Exact Chunk map, but directory
name discovery is currently a bounded-by-store-count `Vec`. Namespace graph
verification retains the unique dependency set of one generation at a time,
and DATA coverage for the two online generations is verified in one combined
Container pass rather than once per historical Commit.
The active-index Location pass intentionally performs random Container reads;
it is an offline integrity operation, not a mount-time path.

`scrub-gc` starts the Scrub on a dedicated asynchronous maintenance thread.
All adaptive maintenance threads run in Linux's work-conserving idle I/O class:
they receive device service only when the block scheduler has no higher-class
frontend request. This protection adds no observation, atomic operation, lock,
or branch to the write hot loop. Below 90% Data Pool occupancy the thread also
runs at Unix nice +10; at or above the inclusive 90% threshold it runs at normal
CPU priority while retaining idle I/O priority. A completed Scrub produces
an opaque plan bound to the exact current and previous online Commit Records.
GC CPU scheduling is promoted to normal priority when either pool occupancy is
at least 90% or immediately reclaimable Container bytes are more than 20% of all
Container file bytes. Exactly 20% remains a nice +10 background phase.

`gc-now` starts the same verified Scrub/GC protocol immediately with full CPU
and ordinary I/O scheduling priority. It performs no maintenance demotion and
is intended for an operator-selected maintenance window. Like every current
destructive command it requires `--offline`; do not use it while the appliance
is mounted. The command reports `mode=FullSpeed` at admission.

GC inventory classification reuses the payload-free live Chunk identities
collected while each Container is decoded. Mixed-victim selection therefore
does not perform a second complete Container-store pass. A deterministic
24-Container/24-DATA-generation tracer reduced full DATA reads from 324 to at
most 48; a 22-Container mixed-victim tracer reduced 66 reads to at most 44.
Both bounds are linear in Container count and are enforced by public
`StorageIo` operation-count regression tests.

GC removes completely unreachable Containers and may compact a set of at least
two partially live Containers when their unique uncovered live Chunks fit into
fewer replacement Containers. Replacement batches retain at most 48 MiB of
decoded payload and 32,768 Chunks; adaptive Compression Regions remain bounded
to 512 KiB. Existing fully live copies satisfy coverage without another write.
Mixed-Container reclaim pressure subtracts a conservative independent-RAW
physical upper bound from victim bytes before applying the strict 20% priority
threshold; compression can only make the eventual net gain larger.
Every replacement is published, reread, and verified before GC activates a
complete RoW Exact Index excluding all victims, revalidates that the online
generation pair did not change, rereads every victim identity, then unlinks the
canonical names and synchronizes the DATA directory. Interrupted work therefore
leaves old coverage, harmless verified duplicates, or the complete compacted
coverage; no visible Manifest loses coverage. Deterministic replacement IDs let
retry resume a partially written non-authoritative `.building` object. The
command prints `gc_ok=true` only after the directory sync.

The destructive phase still requires `--offline`. The asynchronous job shape
prevents the control plane from executing Scrub inline; the separately held
Appliance Lease prevents this CLI process from racing a writable mount.

The shared in-process store API now provides the safe online execution core for
a locally constructed `GcCandidateProof`. It publishes replacement `ACTIVE`
and victim `RETIRING` Locations in one Exact L0 activation, closes new admission
to the displaced Exact generation, drains existing reader/write-through/
reduction pins from every still-live predecessor generation, unlinks verified
victim identities, synchronizes DATA, and then appends `REMOVED`. The
scan-selection barrier rolls back if Exact activation fails and is released
after `REMOVED`. Before the writable FUSE mount admits frontend I/O, its Online
GC recovery finalizer derives every effective `RETIRING` Location, fully
verifies each still-present victim against that complete Location set, finishes
the DATA unlink/directory sync, and activates `REMOVED`. Already absent victims
are accepted so a crash after durable unlink but before Exact publication is
idempotently resumable. Failure aborts writable mount startup.

Candidate discovery and bounded execution are wired to the writable daemon's
adaptive scheduler. Continuing frontend io_uring submissions permit one small
background quantum every fifteen minutes. After thirty seconds without a new
frontend submission, the daemon may run one larger quantum per minute. At or
above 90% physical Data Pool occupancy, it may run an urgent larger quantum
every thirty seconds; pressure remains latched until occupancy reaches 85%.
All three remain in Linux idle I/O class. Operators may override the four
intervals, low/high basis-point watermarks, one `HH:MM-HH:MM` UTC window and its
interval, and the maximum relocation encoder workers through the corresponding
`FASTDUP_ONLINE_GC_*` environment variables exported by the appliance crate.
Background relocation remains single-worker; all destructive phases remain
serialized.

The startup overrides are:

- `FASTDUP_ONLINE_GC_ACTIVE_INTERVAL_SECONDS`
- `FASTDUP_ONLINE_GC_IDLE_AFTER_SECONDS`
- `FASTDUP_ONLINE_GC_IDLE_INTERVAL_SECONDS`
- `FASTDUP_ONLINE_GC_URGENT_INTERVAL_SECONDS`
- `FASTDUP_ONLINE_GC_PRESSURE_LOW_BASIS_POINTS`
- `FASTDUP_ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS`
- `FASTDUP_ONLINE_GC_DAILY_WINDOW_UTC`, for example `23:00-05:00`
- `FASTDUP_ONLINE_GC_WINDOW_INTERVAL_SECONDS`
- `FASTDUP_ONLINE_GC_MAX_RELOCATION_WORKERS`

The low watermark must be below the high watermark. All durations and the
worker count must be nonzero. Supplying a window interval without a daily
window is rejected.

`--online gc-now METADATA_ROOT` sends one request over the daemon-owned
mode-0600 Unix socket in the Metadata root. It starts an urgent quantum
immediately with normal CPU priority, waits for its result, and prints a single
`online_gc_ok=...` status line. The CLI never opens either storage repository.
It can therefore be used while mounted without creating a second owner. This
is intentionally different from `--offline gc-now`, which remains the only
ordinary-I/O full-speed mode.

Online DATA reads, relocation writes, verification, unlink, and directory sync
use a synchronous maintenance storage view on the idle-prioritized worker. It
shares RETIRING state and descriptor cache with the frontend Container
repository, while bypassing the frontend io_uring ring and inflight-byte
budget. Candidate bootstrap streams rows from bounded Header/Footer reads; it
does not read Container payload and does not retain a pool-sized catalog map.

If Current/Previous, Active/Frozen, and open-orphan liveness contain no DATA
Chunk, an urgent quantum retires its verified victims without publishing
replacement Containers. A large empty pool is drained across repeated bounded
quanta; a stable empty pool returns `outcome=no_candidates`. With protected
DATA present, a cached Reverse Dependency Generation binds the protected Commit
pair to the active Exact generation. It preserves live targets and their
effective ACTIVE Prefix Bases while leaving unrelated dead victim Chunks
behind. Truncated lookup, missing ACTIVE coverage, or either changed binding
aborts the proof.

The mode-0600 control socket remains a filesystem object in the Metadata root.
Bind, stale-owner probing, and client connect use a short directory-descriptor
alias internally, so long run-specific Metadata paths do not exceed Linux
`sockaddr_un.sun_path`.

Every admitted adaptive Online-GC quantum also runs Metadata GC in the same
idle-I/O maintenance class. The first or invalidated cycle exactly marks all
Namespace Roots and Manifest nodes reachable from the selected bounded
Commit-Log segment together with every process-local Metadata Root Pin held by
an installed/open Manifest reader or an unpublished successor proof. A
publication barrier prevents collection from observing a child-first Manifest
batch before its root becomes pinned. An unchanged process-local liveness epoch
reuses the clean mark-catalog result without another graph or directory scan.
Only canonical `.fdm` objects outside that union are candidates; every candidate
is fully identity-verified before the first unlink. The exact result is written
as a sorted immutable Metadata Mark Catalog, audited before no-replace
publication, and committed together with garbage and obsolete-catalog removal
by one final Metadata-root sync. The online status line reports removed
objects/bytes, exact-versus-catalog mode, retained objects, and catalog
generation separately from DATA GC.

The online status line exposes one unambiguous Metadata mark mode
(`reused`, `addition_delta`, or `exact_snapshot`) and, for an exact mark, the
fallback reason. It also reports total and per-phase wall time for recovery,
Metadata GC, candidate-catalog work, candidate proof, `RETIRING` activation,
generation-pin drain, victim verification, unlink, DATA-directory sync,
`REMOVED` activation, and the final catalog refresh. Physical work counters
cover catalog bytes examined/written, verified victim bytes read in proof and
relocation, replacement bytes written, bytes unlinked, shortlist/proof counts,
and candidates abandoned as unprofitable or stale. Reverse-dependency counters
report logical edges and required target/Base Chunks. Scheduler counters report
polls, interval-deferred polls, frontend activity changes, admissions by pace
and scheduled window, immediate operator requests, and the applied relocation
worker bound. These counters are maintained by the
maintenance runtime; the frontend I/O path does not update GC telemetry.

Proof-bearing commits that do not rotate the Commit WAL journal their newly
published Manifest nodes and Namespace Root in RAM. They perform no catalog
I/O. A later maintenance quantum persists those identities as an immutable
Addition run chained to the prior exact Snapshot or Addition and syncs the
Metadata directory without holding the Metadata publication barrier or Commit
lock over that I/O. Addition runs never authorize deletion. Unclassified
publication, an unpublished-pin drain, uncertain WAL durability, WAL rotation,
process restart, or the 32-run chain limit falls back to an exact mark and a new
Snapshot.

`--offline metadata-gc` runs this collector explicitly and follows it with a
complete scrub. `scrub-gc` and offline `gc-now` collect both DATA Containers and
Metadata Objects. The filesystem directory adapter streams names without a
pool-sized name vector. The exact mark set remains bounded by reachable Metadata
Objects. Continuously committed additions avoid complete graph and directory
scans between exact safety boundaries; process restart deliberately performs
one exact refresh.

`rebuild-exact` scans and fully decodes one Container at a time. Verified
Locations immediately become hidden immutable level-zero Runs. Four complete
same-level families are streamed into a higher-level family with one verified
4-KiB page per input family; no full index is held in RAM. Before activation,
a second bounded K-way pass proves cross-family Chunk-length and transition
invariants. The replacement Run Set becomes visible only at the final paired
Activation-Log file sync. The command then runs the complete scrub above.

An interrupted rebuild leaves the previously active index selected. Published
orphan Runs remain immutable and unselected; retry discovers their generation
high-water and uses fresh names. Rebuild includes all valid published Container
Locations, even if no current Manifest references them. This is safe because
the Exact Index is acceleration rather than liveness authority. The scrub-bound
GC path separately rebuilds an index excluding its fully unreachable deletion
candidates.

The public fault matrix injects failures before and after every metadata I/O
operation and proves crash recovery exposes either no first index/its prior
index or the complete new generation. Additional tests cover corruption of a
Container and active Run page, cross-Run Chunk-length conflict, orphan retry,
repeat rebuild generations, and multi-level compaction across 17 Containers.

The library also exposes a paired pool-index rebuild for advanced reduction.
It feeds Exact and Similarity builders from the same verified Container read,
audits both hidden outputs, activates Exact first, and publishes the Similarity
family manifest last. The family authenticates the active Exact Run Set ID;
paired recovery and offline audit do not select a different or unbound identity. Empty
pools publish a bound empty Similarity tombstone, and retry allocates after the
generation of any orphan Similarity partition. The metadata fault matrix proves
that crash recovery never exposes a Similarity family without its bound Exact
Run Set. A maintenance CLI command for this paired operation is still pending.

Remaining production gates are real block-device power-cut campaigns, measured
large-store scrub/rebuild/GC throughput, broad randomized process-kill
coverage, and a stable downgrade/format-epoch fence. Fake-clock tests now cover
stalled Metadata and DATA sync plus admission closure. Process-local Metadata
Root Pins disappear with their owning process;
the accepted restart boundary therefore performs one exact mark before catalog
reuse instead of attempting to reconstruct unpublished pins across processes.
