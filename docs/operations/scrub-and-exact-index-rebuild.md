# Offline scrub and Exact-Index rebuild

Status: implemented and deterministically fault-tested for the current
single-writer checkpoint formats.

Stop and unmount `fastdup-durable-fuse` before every command. The repository
does not yet have a durable Appliance Lease, so the tool requires an explicit
`--offline` acknowledgement but cannot independently prove that another
process is absent.

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

# Against the currently mounted writable appliance:
cargo run --release -p fastdup-appliance --bin fastdup-maintenance -- \
  --online gc-now METADATA_ROOT
```

`scrub` is read-only. It fails closed on a torn or invalid Commit-Log tail and
does not use the ordinary recovery fallback. It verifies:

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
prevents the control plane from executing Scrub inline, but it is not authority
for this separate CLI process to race a writable mount before the Appliance
Lease is implemented.

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
every thirty seconds. All three remain in Linux idle I/O class.

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

Remaining production gates are an Appliance Lease/multi-process exclusion,
real block-device power-cut campaigns, streaming directory enumeration,
measured large-store scrub/rebuild/GC throughput, configurable maintenance
windows, and Metadata-object collection.
