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
Below 90% Data Pool occupancy the thread runs at Unix nice +10; at or above the
inclusive 90% threshold it runs at normal priority. A completed Scrub produces
an opaque plan bound to the exact current and previous online Commit Records.
GC is promoted to normal priority when either pool occupancy is at least 90% or
immediately reclaimable Container bytes are more than 20% of all Container file
bytes. Exactly 20% remains a nice +10 background phase.

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
to race a writable mount before the Appliance Lease, RETIRING transitions, and
reader/writer pin drain are implemented.

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

Remaining production gates are an Appliance Lease/multi-process exclusion,
online RETIRING/relocation and pin drain, real block-device power-cut campaigns,
streaming directory enumeration, measured large-store scrub/rebuild/GC
throughput, and Metadata-object collection.
