use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use fastcdc::v2020::{Normalization, StreamCDC};
use fastdup_format::{
    ChunkId, CommitRecord, ContainerId, DurableInode, ExactIndexEntry, ExactIndexProfileId,
    ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, MAX_LOGICAL_CHUNK_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_posix::{
    CommitInode, CommitRange, CommittedFile, CommittedFileInstall, Namespace, NamespaceCommit,
    NamespaceConfig, PosixError,
};
use fastdup_store::{
    ActivatedExactIndex, AdaptiveContainerPublishMetrics, ContainerRepository,
    ExactIndexRunRepository, GenerationError, GenerationRepository, IndexedRequiredChunkVerifier,
    ManifestReadError, RequiredChunkVerifier, StorageIo, StoreError, VerifiedManifestFile,
};

use crate::{ManifestCommittedFile, MountError, namespace_from_verified_files_using};

const FIRST_REGULAR_INODE: u64 = 2;
const CONTAINER_PAYLOAD_TARGET_BYTES: usize = 32 * 1_024 * 1_024;
const COMPRESSION_REGION_TARGET_BYTES: usize = 512 * 1_024;
const CDC_MINIMUM_BYTES: usize = 16 * 1_024;
const CDC_TARGET_BYTES: usize = 64 * 1_024;
const CDC_MAXIMUM_BYTES: usize = 256 * 1_024;
const CDC_SEED_V1: u64 = 0;
const EXACT_INDEX_COMPACTION_FANIN: usize = 4;
const MAX_CHECKPOINT_WORKERS: usize =
    CONTAINER_PAYLOAD_TARGET_BYTES / COMPRESSION_REGION_TARGET_BYTES;

/// V1 scheduler high-water for active checkpointable DATA.
///
/// This is deliberately expressed from the durable format's 64-MiB maximum
/// Container size rather than the adaptive writer's current 32-MiB payload
/// target. Reaching it starts an early checkpoint and applies admission
/// backpressure until durable progress catches up.
pub const CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1: u64 = 8 * fastdup_format::MAX_CONTAINER_BYTES;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CheckpointPhaseMetrics {
    wall: Duration,
    process_cpu: Duration,
}

impl CheckpointPhaseMetrics {
    #[must_use]
    pub const fn wall(self) -> Duration {
        self.wall
    }

    #[must_use]
    pub const fn process_cpu(self) -> Duration {
        self.process_cpu
    }

    fn add(&mut self, wall: Duration, process_cpu: Duration) {
        self.wall = self
            .wall
            .checked_add(wall)
            .expect("ASSERT: checkpoint wall-clock accounting cannot overflow");
        self.process_cpu = self
            .process_cpu
            .checked_add(process_cpu)
            .expect("ASSERT: checkpoint CPU accounting cannot overflow");
    }
}

/// Per-checkpoint data-reduction and durability measurements.
///
/// Nested `manifest_plan` contains `FastCDC`, hash/FILL, Exact lookup, encoding,
/// and Container publication. The leaf phases may be summed; callers must not
/// add the parent to them. Process CPU includes all process threads active in
/// the phase, including compression workers and concurrent FUSE request work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CheckpointMetrics {
    total: CheckpointPhaseMetrics,
    freeze: CheckpointPhaseMetrics,
    manifest_plan: CheckpointPhaseMetrics,
    fastcdc: CheckpointPhaseMetrics,
    hash_and_fill: CheckpointPhaseMetrics,
    exact_lookup: CheckpointPhaseMetrics,
    compression_encode: CheckpointPhaseMetrics,
    container_publish: CheckpointPhaseMetrics,
    exact_index_publish: CheckpointPhaseMetrics,
    metadata_commit: CheckpointPhaseMetrics,
    logical_chunks: u64,
    logical_chunk_bytes: u64,
    fill_chunks: u64,
    fill_bytes: u64,
    exact_hit_chunks: u64,
    exact_hit_bytes: u64,
    new_chunks: u64,
    new_chunk_bytes: u64,
    container_file_bytes: u64,
    raw_records: u64,
    zstd_records: u64,
    containers: u64,
    peak_buffered_chunk_bytes: u64,
    peak_buffered_chunks: u64,
}

macro_rules! phase_getter {
    ($name:ident) => {
        #[must_use]
        pub const fn $name(self) -> CheckpointPhaseMetrics {
            self.$name
        }
    };
}

impl CheckpointMetrics {
    phase_getter!(total);
    phase_getter!(freeze);
    phase_getter!(manifest_plan);
    phase_getter!(fastcdc);
    phase_getter!(hash_and_fill);
    phase_getter!(exact_lookup);
    phase_getter!(compression_encode);
    phase_getter!(container_publish);
    phase_getter!(exact_index_publish);
    phase_getter!(metadata_commit);

    #[must_use]
    pub const fn logical_chunks(self) -> u64 {
        self.logical_chunks
    }

    #[must_use]
    pub const fn logical_chunk_bytes(self) -> u64 {
        self.logical_chunk_bytes
    }

    #[must_use]
    pub const fn fill_chunks(self) -> u64 {
        self.fill_chunks
    }

    #[must_use]
    pub const fn fill_bytes(self) -> u64 {
        self.fill_bytes
    }

    #[must_use]
    pub const fn exact_hit_chunks(self) -> u64 {
        self.exact_hit_chunks
    }

    #[must_use]
    pub const fn exact_hit_bytes(self) -> u64 {
        self.exact_hit_bytes
    }

    #[must_use]
    pub const fn new_chunks(self) -> u64 {
        self.new_chunks
    }

    #[must_use]
    pub const fn new_chunk_bytes(self) -> u64 {
        self.new_chunk_bytes
    }

    #[must_use]
    pub const fn container_file_bytes(self) -> u64 {
        self.container_file_bytes
    }

    #[must_use]
    pub const fn raw_records(self) -> u64 {
        self.raw_records
    }

    #[must_use]
    pub const fn zstd_records(self) -> u64 {
        self.zstd_records
    }

    #[must_use]
    pub const fn containers(self) -> u64 {
        self.containers
    }

    #[must_use]
    pub const fn peak_buffered_chunk_bytes(self) -> u64 {
        self.peak_buffered_chunk_bytes
    }

    #[must_use]
    pub const fn peak_buffered_chunks(self) -> u64 {
        self.peak_buffered_chunks
    }

    fn merge_reduction(&mut self, reduction: &CheckpointReductionMetrics) {
        self.fastcdc = reduction.fastcdc;
        self.hash_and_fill = reduction.hash_and_fill;
        self.exact_lookup = reduction.exact_lookup;
        self.compression_encode = reduction.compression_encode;
        self.container_publish = reduction.container_publish;
        self.logical_chunks = reduction.logical_chunks;
        self.logical_chunk_bytes = reduction.logical_chunk_bytes;
        self.fill_chunks = reduction.fill_chunks;
        self.fill_bytes = reduction.fill_bytes;
        self.exact_hit_chunks = reduction.exact_hit_chunks;
        self.exact_hit_bytes = reduction.exact_hit_bytes;
        self.new_chunks = reduction.new_chunks;
        self.new_chunk_bytes = reduction.new_chunk_bytes;
        self.container_file_bytes = reduction.container_file_bytes;
        self.raw_records = reduction.raw_records;
        self.zstd_records = reduction.zstd_records;
        self.containers = reduction.containers;
        self.peak_buffered_chunk_bytes = reduction.peak_buffered_chunk_bytes;
        self.peak_buffered_chunks = reduction.peak_buffered_chunks;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProfiledCheckpoint {
    record: CommitRecord,
    metrics: CheckpointMetrics,
}

impl ProfiledCheckpoint {
    #[must_use]
    pub const fn record(self) -> CommitRecord {
        self.record
    }

    #[must_use]
    pub const fn metrics(self) -> CheckpointMetrics {
        self.metrics
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointReductionMetrics {
    fastcdc: CheckpointPhaseMetrics,
    hash_and_fill: CheckpointPhaseMetrics,
    exact_lookup: CheckpointPhaseMetrics,
    compression_encode: CheckpointPhaseMetrics,
    container_publish: CheckpointPhaseMetrics,
    logical_chunks: u64,
    logical_chunk_bytes: u64,
    fill_chunks: u64,
    fill_bytes: u64,
    exact_hit_chunks: u64,
    exact_hit_bytes: u64,
    new_chunks: u64,
    new_chunk_bytes: u64,
    container_file_bytes: u64,
    raw_records: u64,
    zstd_records: u64,
    containers: u64,
    peak_buffered_chunk_bytes: u64,
    peak_buffered_chunks: u64,
}

#[derive(Clone, Copy)]
struct PhaseStarted {
    wall: Instant,
    process_cpu: rustix::time::Timespec,
}

impl PhaseStarted {
    fn now() -> Self {
        Self {
            wall: Instant::now(),
            process_cpu: rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime),
        }
    }

    fn finish_into(self, phase: &mut CheckpointPhaseMetrics) {
        let process_cpu = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
        let process_cpu = Duration::try_from(process_cpu - self.process_cpu)
            .expect("ASSERT: monotonic process CPU time must form a nonnegative Duration");
        phase.add(self.wall.elapsed(), process_cpu);
    }
}

/// Returns the immutable identity of the currently implemented durable
/// checkpoint writer policy.
///
/// The canonical bytes pin FastCDC-v1, region sizing, adaptive Zstd thresholds,
/// and automatic level-zero Exact-Index publication. Changing any decision
/// requires a new canonical policy string rather than silently reusing this ID.
///
/// # Panics
///
/// Panics only if BLAKE3 maps the fixed canonical policy bytes to the reserved
/// all-zero identity, an impossible production `ASSERT` for this pinned input.
#[must_use]
pub fn checkpoint_policy_set_v1() -> PolicySetId {
    PolicySetId::new(
        ChunkId::of(
            b"fastdup/checkpoint-policy-v1/FastCDC=16384:65536:262144:norm1:seed0:append-tail-anchor-v1/region=524288/Zstd=level3:min4096:min3pct/exact=l0-runs-v1:fanin4/proof=installed-successor-delta-v1",
        )
        .bytes(),
    )
    .expect("ASSERT: the checkpoint Policy Set hash is nonzero")
}

/// Writable namespace plus the durable generation machinery behind it.
///
/// The module owns the only checkpoint serialization lock. POSIX callers use
/// [`Self::namespace`] and do not need to know about manifests, containers, or
/// Commit Records.
#[derive(Debug)]
pub struct DurableNamespace<M, C> {
    namespace: Arc<Namespace>,
    generations: GenerationRepository<M>,
    containers: ContainerRepository<C>,
    checkpoint_lock: Mutex<()>,
    manifests: Mutex<Vec<(u64, ManifestLeaf)>>,
    next_container_generation: Mutex<u64>,
    manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
    checkpoint_workers: NonZeroUsize,
}

trait ManifestReaderPolicy<C>: fmt::Debug + Send + Sync {
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C>;
    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier>;
    fn exact_index_run_count(&self) -> usize;
    fn contains_verified(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> bool;
    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>);
    fn exact_index_degraded(&self) -> bool;
}

/// Composes one successor proof from the installed predecessor and a fully
/// verified changed-dependency set. Construction stays private because only
/// `DurableNamespace` owns the serialized installed-generation transition.
struct IncrementalGraphVerifier<'a> {
    complete: &'a dyn RequiredChunkVerifier,
    changed: &'a BTreeMap<ChunkId, u64>,
}

impl RequiredChunkVerifier for IncrementalGraphVerifier<'_> {
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        for (chunk_id, logical_length) in self.changed {
            assert_eq!(
                required.get(chunk_id),
                Some(logical_length),
                "ASSERT: incremental DATA proof must be a subset of the reread Manifest graph"
            );
        }
        self.complete.verify_required_chunks(self.changed)
    }
}

#[derive(Debug)]
struct ScanManifestReaders;

impl<C: StorageIo + 'static> ManifestReaderPolicy<C> for ScanManifestReaders {
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        file
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        Box::new(containers)
    }

    fn exact_index_run_count(&self) -> usize {
        0
    }

    fn contains_verified(
        &self,
        _containers: &ContainerRepository<C>,
        _chunk_id: ChunkId,
        _logical_length: u64,
    ) -> bool {
        false
    }

    fn publish_level_zero(&self, _entries: Vec<ExactIndexEntry>) {}

    fn exact_index_degraded(&self) -> bool {
        false
    }
}

struct IndexedManifestReaders<X> {
    repository: ExactIndexRunRepository<X>,
    profile: ExactIndexProfileId,
    active: RwLock<Option<Arc<ActivatedExactIndex<X>>>>,
    degraded: AtomicBool,
}

impl<X> fmt::Debug for IndexedManifestReaders<X> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedManifestReaders")
            .field("run_count", &self.run_count())
            .field("degraded", &self.degraded.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<X> IndexedManifestReaders<X> {
    fn run_count(&self) -> usize {
        self.active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .as_ref()
            .map_or(0, |active| active.run_count())
    }
}

impl<C, X> ManifestReaderPolicy<C> for IndexedManifestReaders<X>
where
    C: Clone + Send + Sync + StorageIo + 'static,
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        match self
            .active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .as_ref()
        {
            Some(active) => file.with_active_index(Arc::clone(active)),
            None => file,
        }
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        let active = self
            .active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .clone();
        match active {
            Some(index) => Box::new(IndexedRequiredChunkVerifier::new(containers, index)),
            None => Box::new(containers),
        }
    }

    fn exact_index_run_count(&self) -> usize {
        self.run_count()
    }

    fn contains_verified(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> bool {
        let active = self
            .active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .clone();
        let Some(active) = active else {
            return false;
        };
        match containers.find_verified_chunk_with_index(&active, chunk_id, logical_length) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => {
                self.degraded.store(true, Ordering::Release);
                false
            }
        }
    }

    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>) {
        if entries.is_empty() {
            return;
        }
        if self.try_publish_level_zero(entries).is_err() {
            self.degraded.store(true, Ordering::Release);
        }
    }

    fn exact_index_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }
}

impl<X> IndexedManifestReaders<X>
where
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn try_publish_level_zero(
        &self,
        entries: Vec<ExactIndexEntry>,
    ) -> Result<(), fastdup_store::ExactIndexStoreError> {
        let previous = self
            .active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .clone();
        let run_generation = previous
            .as_ref()
            .and_then(|active| {
                active
                    .run_set()
                    .runs()
                    .iter()
                    .map(|run| run.generation())
                    .max()
            })
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(fastdup_store::ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
        let run = ExactIndexRun::new(self.profile, run_generation, entries)?;
        let descriptor = self.repository.publish(&run)?;
        let mut run_refs = previous
            .as_ref()
            .map_or_else(Vec::new, |active| active.run_set().runs().to_vec());
        run_refs
            .try_reserve(1)
            .map_err(|_| fastdup_store::ExactIndexStoreError::OutOfMemory)?;
        run_refs.push(ExactIndexRunRef::new(0, descriptor)?);
        let mut newest_run_generation = run_generation;
        while let Some((source_level, inputs)) = select_compaction_inputs(&run_refs) {
            newest_run_generation = newest_run_generation
                .checked_add(1)
                .ok_or(fastdup_store::ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
            let target_level = source_level
                .checked_add(1)
                .ok_or(fastdup_store::ExactIndexStoreError::InvalidCompactionInput)?;
            let compacted = self.repository.compact(&inputs, newest_run_generation)?;
            run_refs.retain(|run| {
                !inputs
                    .iter()
                    .any(|input| input.generation() == run.generation())
            });
            run_refs.push(ExactIndexRunRef::new(target_level, compacted)?);
        }
        let run_set_generation = previous.as_ref().map_or(Ok(1), |active| {
            active
                .run_set()
                .generation()
                .checked_add(1)
                .ok_or(fastdup_store::ExactIndexStoreError::NonMonotonicRunSetGeneration)
        })?;
        let run_set = ExactIndexRunSet::new(self.profile, run_set_generation, run_refs)?;
        let active = Arc::new(self.repository.activate(&run_set)?);
        *self
            .active
            .write()
            .expect("ASSERT: active Exact Index writer lock poisoned") = Some(active);
        self.degraded.store(false, Ordering::Release);
        Ok(())
    }
}

fn select_compaction_inputs(runs: &[ExactIndexRunRef]) -> Option<(u16, Vec<ExactIndexRunRef>)> {
    let mut by_level = BTreeMap::<u16, Vec<ExactIndexRunRef>>::new();
    for run in runs.iter().copied() {
        by_level.entry(run.level()).or_default().push(run);
    }
    for (level, mut candidates) in by_level {
        if candidates.len() < EXACT_INDEX_COMPACTION_FANIN {
            continue;
        }
        candidates.sort_unstable_by_key(|run| run.generation());
        candidates.truncate(EXACT_INDEX_COMPACTION_FANIN);
        return Some((level, candidates));
    }
    None
}

impl<M, C> DurableNamespace<M, C>
where
    M: StorageIo,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    /// Recovers the newest generation, durably reserves a fresh Inode ID
    /// range, and only then enables mutation admission.
    ///
    /// A new repository first publishes its initial reservation generation.
    /// Reopening an existing repository deliberately skips every unused ID in
    /// the prior reservation before publishing a new range.
    ///
    /// # Errors
    ///
    /// Returns recovery, reservation, graph verification, durability, or
    /// namespace-construction failures. A zero or overflowing reservation span
    /// is rejected before mutation admission.
    pub fn open(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError> {
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            Arc::new(ScanManifestReaders),
        )
    }

    /// Opens a writable namespace and pins the newest valid Exact Index Run
    /// Set behind every committed Manifest reader.
    ///
    /// Missing or corrupt index acceleration falls back to verified Container
    /// scans and does not make Namespace DATA unavailable. The recovered Run
    /// Set is immutable and remains pinned for this appliance lifetime; newly
    /// committed locations remain readable through the same correctness
    /// fallback until a later checkpoint-index publisher activates them.
    ///
    /// # Errors
    ///
    /// Returns the same recovery, reservation, graph, durability, and
    /// namespace-construction failures as [`Self::open`]. Exact Index recovery
    /// failure is deliberately not a Namespace failure.
    pub fn open_with_index<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let recovered = indexes.recover_active();
        let initially_degraded = recovered.is_err();
        let active = recovered.ok().flatten().map(Arc::new);
        let profile = active
            .as_ref()
            .map_or_else(checkpoint_index_profile_v1, |index| {
                index.run_set().profile()
            });
        let manifest_readers: Arc<dyn ManifestReaderPolicy<C>> = Arc::new(IndexedManifestReaders {
            repository: indexes.clone(),
            profile,
            active: RwLock::new(active),
            degraded: AtomicBool::new(initially_degraded),
        });
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            manifest_readers,
        )
    }

    fn open_using(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
        manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
    ) -> Result<Self, DurableNamespaceError> {
        if inode_reservation_span == 0 {
            return Err(DurableNamespaceError::InvalidReservationSpan);
        }
        let graph_verifier = manifest_readers.graph_verifier(containers.clone());
        let recovered = generations
            .recover_latest_with_verified_files_using(&containers, graph_verifier.as_ref())?;
        let next_container_generation = discover_next_container_generation(&containers)?;
        let (root, next_inode, reservation_end, verified_files) = match recovered {
            None => {
                let reservation_end = FIRST_REGULAR_INODE
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new(
                    reservation_end,
                    FIRST_REGULAR_INODE,
                    0,
                    Vec::new(),
                    Vec::new(),
                )?;
                generations.commit_namespace(&root)?;
                (root, FIRST_REGULAR_INODE, reservation_end, Vec::new())
            }
            Some(recovered) => {
                let (recovered, _prior_files) = recovered.into_parts();
                let previous = recovered.namespace_root();
                let next_inode = recovered.inode_reservation_end_high_water();
                let reservation_end = next_inode
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new(
                    reservation_end,
                    next_inode,
                    previous.namespace_mutation_sequence(),
                    previous.inodes().to_vec(),
                    previous.entries().to_vec(),
                )?;
                let committed = generations.commit_namespace_with_verified_files_using(
                    &root,
                    &containers,
                    graph_verifier.as_ref(),
                )?;
                let (_record, verified_files) = committed.into_parts();
                (root, next_inode, reservation_end, verified_files)
            }
        };
        let manifests = load_manifest_cache(&root, &generations)?;
        let namespace = namespace_from_verified_files_using(
            config,
            &root,
            next_inode,
            reservation_end,
            verified_files,
            true,
            |file| manifest_readers.prepare(file),
        )?;
        let available_workers = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        let checkpoint_workers =
            NonZeroUsize::new(available_workers.get().min(MAX_CHECKPOINT_WORKERS))
                .expect("ASSERT: the checkpoint worker cap is nonzero");
        Ok(Self {
            namespace: Arc::new(namespace),
            generations,
            containers,
            checkpoint_lock: Mutex::new(()),
            manifests: Mutex::new(manifests),
            next_container_generation: Mutex::new(next_container_generation),
            manifest_readers,
            checkpoint_workers,
        })
    }

    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    #[must_use]
    pub fn namespace_arc(&self) -> Arc<Namespace> {
        Arc::clone(&self.namespace)
    }

    /// Returns the number of immutable Exact-Index Runs pinned for ordinary
    /// Manifest demand reads. Zero means this mount is using the verified
    /// Container-scan fallback.
    #[must_use]
    pub fn exact_index_run_count(&self) -> usize {
        self.manifest_readers.exact_index_run_count()
    }

    /// Reports that Exact-Index recovery, lookup, publication, or activation
    /// degraded while Namespace durability remained available.
    #[must_use]
    pub fn exact_index_degraded(&self) -> bool {
        self.manifest_readers.exact_index_degraded()
    }

    /// Returns the runtime worker cap for independent Compression Regions.
    #[must_use]
    pub const fn checkpoint_worker_limit(&self) -> NonZeroUsize {
        self.checkpoint_workers
    }

    /// Durably commits one complete prefix of accepted mutations.
    ///
    /// DATA containers are sealed and synchronized first, immutable manifests
    /// and the Namespace Root follow, and the Commit WAL is synchronized last.
    /// A failed call leaves the same frozen cut available for retry while later
    /// writes remain live in the next epoch.
    ///
    /// # Errors
    ///
    /// Returns frozen-view, format, container, metadata, graph, or durability
    /// failures. `Ok(None)` means no accepted mutation is waiting for a commit.
    ///
    /// # Panics
    ///
    /// Panics when a prior impossible invariant poisoned the single checkpoint
    /// lock or when a verified installed view disagrees with its own cut.
    pub fn checkpoint(&self) -> Result<Option<CommitRecord>, DurableNamespaceError> {
        self.checkpoint_profiled()
            .map(|profiled| profiled.map(ProfiledCheckpoint::record))
    }

    /// Commits the same durable prefix as [`Self::checkpoint`] and returns
    /// bounded phase/counter evidence for observability and benchmarks.
    ///
    /// Metrics are advisory and are returned only for a successful commit.
    /// They never participate in visibility, recovery, or integrity decisions.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::checkpoint`].
    ///
    /// # Panics
    ///
    /// Panics for the same impossible poisoned-lock or paired-proof failures as
    /// [`Self::checkpoint`], or if monotonic duration accounting moves backward.
    pub fn checkpoint_profiled(&self) -> Result<Option<ProfiledCheckpoint>, DurableNamespaceError> {
        let total_started = PhaseStarted::now();
        let _guard = self
            .checkpoint_lock
            .lock()
            .expect("ASSERT: durable namespace checkpoint lock poisoned");
        let freeze_started = PhaseStarted::now();
        let Some(commit) = self.namespace.begin_commit()? else {
            return Ok(None);
        };
        let mut metrics = CheckpointMetrics::default();
        freeze_started.finish_into(&mut metrics.freeze);
        let mut next_container_generation = self
            .next_container_generation
            .lock()
            .expect("ASSERT: Container generation allocator lock poisoned");
        let mut writer = AdaptiveCommitWriter::new(
            &self.containers,
            &mut next_container_generation,
            self.manifest_readers.as_ref(),
            self.checkpoint_workers,
        );
        let manifest_plan_started = PhaseStarted::now();
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let installed_manifests = self
            .manifests
            .lock()
            .expect("ASSERT: installed Manifest cache lock poisoned");
        for inode in commit.inodes() {
            let previous = installed_manifests
                .binary_search_by_key(&inode.inode().get(), |(inode, _)| *inode)
                .ok()
                .map(|index| &installed_manifests[index].1);
            manifests.push(plan_manifest(inode, previous, &mut writer)?);
        }
        let changed_dependencies =
            changed_manifest_dependencies(&installed_manifests, commit.inodes(), &manifests)?;
        drop(installed_manifests);
        let (level_zero_entries, reduction_metrics) = writer.finish()?;
        manifest_plan_started.finish_into(&mut metrics.manifest_plan);
        metrics.merge_reduction(&reduction_metrics);
        drop(next_container_generation);
        let exact_index_started = PhaseStarted::now();
        self.manifest_readers.publish_level_zero(level_zero_entries);
        exact_index_started.finish_into(&mut metrics.exact_index_publish);
        let metadata_started = PhaseStarted::now();
        let record = self.publish_generation(&commit, manifests, &changed_dependencies)?;
        metadata_started.finish_into(&mut metrics.metadata_commit);
        total_started.finish_into(&mut metrics.total);
        Ok(Some(ProfiledCheckpoint { record, metrics }))
    }

    fn publish_generation(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestLeaf>,
        changed_dependencies: &BTreeMap<ChunkId, u64>,
    ) -> Result<CommitRecord, DurableNamespaceError> {
        if manifests.len() != commit.inodes().len() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let mut durable_inodes = Vec::new();
        let mut installs = Vec::new();
        let mut next_manifests = Vec::new();
        durable_inodes
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        installs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        next_manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, manifest) in commit.inodes().iter().zip(manifests) {
            let manifest_root = self.generations.publish_manifest(&manifest)?;
            durable_inodes.push(DurableInode::new(
                inode.inode().get(),
                inode.mode(),
                inode.uid(),
                inode.gid(),
                inode.link_count(),
                inode.mutation_sequence(),
                inode.logical_size(),
                manifest_root,
            )?);
            next_manifests.push((inode.inode().get(), manifest));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(commit.entries().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for entry in commit.entries() {
            entries.push(NamespaceEntry::new(
                entry.parent().get(),
                entry.target().get(),
                entry.name().to_vec(),
            )?);
        }
        let root = NamespaceRoot::new(
            commit.inode_reservation_end(),
            commit.inode_allocation_cursor(),
            commit.namespace_mutation_sequence(),
            durable_inodes,
            entries,
        )?;
        let graph_verifier = self
            .manifest_readers
            .graph_verifier(self.containers.clone());
        let incremental_verifier = IncrementalGraphVerifier {
            complete: graph_verifier.as_ref(),
            changed: changed_dependencies,
        };
        let committed = self
            .generations
            .commit_namespace_with_verified_files_using(
                &root,
                &self.containers,
                &incremental_verifier,
            )?;
        let (record, verified_files) = committed.into_parts();
        assert_eq!(
            verified_files.len(),
            commit.inodes().len(),
            "ASSERT: committed DATA proof must cover every frozen inode"
        );
        for ((inode, verified), (_, planned_manifest)) in commit
            .inodes()
            .iter()
            .zip(verified_files)
            .zip(&next_manifests)
        {
            assert_eq!(
                verified.inode(),
                inode.inode().get(),
                "ASSERT: committed DATA proof order must match the Namespace Root"
            );
            assert_eq!(
                verified.manifest(),
                planned_manifest,
                "ASSERT: published Manifest reread must equal the planned Manifest"
            );
            let installed = Arc::new(ManifestCommittedFile::from_verified(
                self.manifest_readers.prepare(verified.into_file()),
            )) as Arc<dyn CommittedFile>;
            installs.push(CommittedFileInstall::new(
                inode.inode(),
                inode.mutation_sequence(),
                installed,
            ));
        }
        self.namespace.complete_commit(commit, installs)?;
        *self
            .manifests
            .lock()
            .expect("ASSERT: installed Manifest cache lock poisoned") = next_manifests;
        Ok(record)
    }
}

fn changed_manifest_dependencies(
    previous: &[(u64, ManifestLeaf)],
    inodes: &[CommitInode],
    proposed: &[ManifestLeaf],
) -> Result<BTreeMap<ChunkId, u64>, DurableNamespaceError> {
    assert_eq!(
        inodes.len(),
        proposed.len(),
        "ASSERT: every proposed Manifest must pair with one frozen inode"
    );
    let mut changed = BTreeMap::new();
    let mut previous_inode = 0_u64;
    for (inode, manifest) in inodes.iter().zip(proposed) {
        let inode_id = inode.inode().get();
        assert!(
            inode_id > previous_inode,
            "ASSERT: frozen commit inodes must remain strictly ordered"
        );
        previous_inode = inode_id;
        let installed = previous
            .binary_search_by_key(&inode_id, |(candidate, _)| *candidate)
            .ok()
            .map(|index| &previous[index].1);
        collect_changed_manifest_dependencies(installed, manifest, &mut changed)?;
    }
    Ok(changed)
}

fn collect_changed_manifest_dependencies(
    previous: Option<&ManifestLeaf>,
    proposed: &ManifestLeaf,
    changed: &mut BTreeMap<ChunkId, u64>,
) -> Result<(), DurableNamespaceError> {
    let proposed_extents = proposed.extents();
    let (prefix, suffix) = previous.map_or((0, 0), |previous| {
        let previous_extents = previous.extents();
        let prefix = previous_extents
            .iter()
            .zip(proposed_extents)
            .take_while(|(left, right)| left == right)
            .count();
        let remaining_previous = previous_extents.len().saturating_sub(prefix);
        let remaining_proposed = proposed_extents.len().saturating_sub(prefix);
        let suffix = previous_extents
            .iter()
            .rev()
            .take(remaining_previous)
            .zip(proposed_extents.iter().rev().take(remaining_proposed))
            .take_while(|(left, right)| left == right)
            .count();
        (prefix, suffix)
    });
    let changed_end = proposed_extents
        .len()
        .checked_sub(suffix)
        .expect("ASSERT: common Manifest suffix cannot exceed proposed extents");
    assert!(
        prefix <= changed_end,
        "ASSERT: common Manifest prefix and suffix cannot overlap"
    );
    for extent in &proposed_extents[prefix..changed_end] {
        let ManifestExtent::Data {
            logical_length,
            chunk_id,
        } = *extent
        else {
            continue;
        };
        if let Some(previous_length) = changed.insert(chunk_id, logical_length)
            && previous_length != logical_length
        {
            return Err(DurableNamespaceError::ChunkLengthConflict {
                chunk_id,
                first_length: previous_length,
                second_length: logical_length,
            });
        }
    }
    Ok(())
}

fn checkpoint_index_profile_v1() -> ExactIndexProfileId {
    ExactIndexProfileId::new(
        ChunkId::of(b"fastdup/FastCDC-v1/min=16384,target=65536,max=262144,norm=1,seed=0").bytes(),
    )
    .expect("ASSERT: the FastCDC-v1 Exact-Index profile hash is nonzero")
}

fn discover_next_container_generation<I: StorageIo>(
    containers: &ContainerRepository<I>,
) -> Result<u64, DurableNamespaceError> {
    containers
        .discover_container_generation_high_water()?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(DurableNamespaceError::ContainerGenerationExhausted)
}

fn load_manifest_cache<I: StorageIo>(
    root: &NamespaceRoot,
    generations: &GenerationRepository<I>,
) -> Result<Vec<(u64, ManifestLeaf)>, DurableNamespaceError> {
    let mut manifests = Vec::new();
    manifests
        .try_reserve_exact(root.inodes().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for inode in root.inodes() {
        let manifest = generations.read_manifest(inode.manifest_root())?;
        if manifest.file_length() != inode.logical_size() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if let Some((previous, _)) = manifests.last()
            && *previous >= inode.inode()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        manifests.push((inode.inode(), manifest));
    }
    Ok(manifests)
}

fn plan_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: Option<&ManifestLeaf>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    if logical_size == 0 {
        if inode.allocated_bytes() != 0 {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        return ManifestLeaf::new(0, Vec::new()).map_err(Into::into);
    }
    let changed = inode.changed_ranges()?;
    if let Some(previous) = previous
        && previous.file_length() <= logical_size
    {
        if changed.is_empty() && previous.file_length() == logical_size {
            verify_manifest_allocation(previous, inode)?;
            return Ok(previous.clone());
        }
        return plan_incremental_manifest(inode, previous, &changed, writer);
    }
    plan_full_manifest(inode, writer)
}

fn plan_full_manifest<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    stack.push((0_u64, logical_size));
    let mut extents = Vec::new();
    plan_manifest_ranges(inode, writer, &mut stack, &mut extents)?;
    let manifest = ManifestLeaf::new(logical_size, extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

#[derive(Clone, Copy, Debug)]
struct RewriteRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Copy)]
struct LocatedExtent<'a> {
    start: u64,
    end: u64,
    extent: &'a ManifestExtent,
}

fn plan_incremental_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: &ManifestLeaf,
    changed: &[CommitRange],
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    assert!(
        previous.file_length() <= inode.logical_size(),
        "ASSERT: incremental planning cannot preserve bytes beyond the new EOF"
    );
    assert!(
        !changed.is_empty() || previous.file_length() < inode.logical_size(),
        "ASSERT: incremental planning requires a changed range or an append"
    );
    let located = locate_extents(previous)?;
    let previous_length = previous.file_length();
    let logical_size = inode.logical_size();
    let mut rewrites = rewrite_ranges_before(changed, logical_size, previous_length)?;
    if previous_length < logical_size {
        // A completed FastCDC Chunk is a reset point. Replaying the final DATA
        // Chunk therefore reconstructs exactly the CDC state needed for an
        // appended suffix while bounding old-prefix work by the format maximum.
        // FILL and HOLE already force a region boundary and need no replay.
        let append_start = located.last().map_or(0, |last| {
            if matches!(last.extent, ManifestExtent::Data { .. }) {
                last.start
            } else {
                previous_length
            }
        });
        rewrites
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        rewrites.push(RewriteRange {
            start: append_start,
            end: logical_size,
        });
        rewrites.sort_unstable_by_key(|rewrite| rewrite.start);
    }
    expand_rewrites_over_data(&mut rewrites, &located)?;
    coalesce_rewrites(&mut rewrites);

    let capacity = previous
        .extents()
        .len()
        .checked_add(rewrites.len())
        .ok_or(DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    extents
        .try_reserve_exact(capacity)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut cursor = 0_u64;
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for rewrite in rewrites {
        if rewrite.start < cursor || rewrite.end > logical_size {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if rewrite.start > previous_length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        preserve_range(&located, cursor, rewrite.start, &mut extents)?;
        stack.push((rewrite.start, rewrite.end - rewrite.start));
        plan_manifest_ranges(inode, writer, &mut stack, &mut extents)?;
        assert!(
            stack.is_empty(),
            "ASSERT: range planner must consume its complete work stack"
        );
        cursor = rewrite.end;
    }
    if cursor < previous_length {
        preserve_range(&located, cursor, previous_length, &mut extents)?;
    } else if cursor != logical_size {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let manifest = ManifestLeaf::new(logical_size, extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

fn locate_extents(
    manifest: &ManifestLeaf,
) -> Result<Vec<LocatedExtent<'_>>, DurableNamespaceError> {
    let mut located = Vec::new();
    located
        .try_reserve_exact(manifest.extents().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut start = 0_u64;
    for extent in manifest.extents() {
        let end = start
            .checked_add(extent_length(extent))
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        located.push(LocatedExtent { start, end, extent });
        start = end;
    }
    if start != manifest.file_length() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(located)
}

fn rewrite_ranges_before(
    changed: &[CommitRange],
    logical_size: u64,
    end_limit: u64,
) -> Result<Vec<RewriteRange>, DurableNamespaceError> {
    if end_limit > logical_size {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let cell = MAX_LOGICAL_CHUNK_BYTES as u64;
    let mut rewrites = Vec::new();
    rewrites
        .try_reserve_exact(changed.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut previous_end = 0_u64;
    for range in changed {
        let raw_end = range
            .offset()
            .checked_add(range.length())
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        if range.length() == 0 || raw_end > logical_size || range.offset() < previous_end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let clipped_end = raw_end.min(end_limit);
        if range.offset() >= clipped_end {
            previous_end = raw_end;
            continue;
        }
        let start = range.offset() / cell * cell;
        let remainder = clipped_end % cell;
        let end = if remainder == 0 {
            clipped_end
        } else {
            clipped_end
                .checked_add(cell - remainder)
                .unwrap_or(end_limit)
        }
        .min(end_limit);
        if start >= end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        rewrites.push(RewriteRange { start, end });
        previous_end = raw_end;
    }
    coalesce_rewrites(&mut rewrites);
    Ok(rewrites)
}

fn expand_rewrites_over_data(
    rewrites: &mut [RewriteRange],
    located: &[LocatedExtent<'_>],
) -> Result<(), DurableNamespaceError> {
    for rewrite in rewrites {
        let first = located.partition_point(|extent| extent.end <= rewrite.start);
        for extent in &located[first..] {
            if extent.start >= rewrite.end {
                break;
            }
            if extent.end <= rewrite.start {
                continue;
            }
            if matches!(extent.extent, ManifestExtent::Data { .. }) {
                rewrite.start = rewrite.start.min(extent.start);
                rewrite.end = rewrite.end.max(extent.end);
            }
        }
        if rewrite.start >= rewrite.end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
    }
    Ok(())
}

fn coalesce_rewrites(rewrites: &mut Vec<RewriteRange>) {
    let mut output = 0_usize;
    for input in 0..rewrites.len() {
        let candidate = rewrites[input];
        if output > 0 && rewrites[output - 1].end >= candidate.start {
            rewrites[output - 1].end = rewrites[output - 1].end.max(candidate.end);
        } else {
            rewrites[output] = candidate;
            output += 1;
        }
    }
    rewrites.truncate(output);
}

fn preserve_range(
    located: &[LocatedExtent<'_>],
    start: u64,
    end: u64,
    output: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    if start == end {
        return Ok(());
    }
    if start > end {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let first = located.partition_point(|extent| extent.end <= start);
    let mut cursor = start;
    for located_extent in &located[first..] {
        if located_extent.start >= end {
            break;
        }
        let overlap_start = located_extent.start.max(start);
        let overlap_end = located_extent.end.min(end);
        if overlap_start != cursor || overlap_start >= overlap_end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let logical_length = overlap_end - overlap_start;
        let extent = match located_extent.extent {
            ManifestExtent::Data {
                logical_length: original_length,
                chunk_id,
            } => {
                if overlap_start != located_extent.start
                    || overlap_end != located_extent.end
                    || logical_length != *original_length
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                ManifestExtent::Data {
                    logical_length,
                    chunk_id: *chunk_id,
                }
            }
            ManifestExtent::Hole { .. } => ManifestExtent::Hole { logical_length },
            ManifestExtent::Fill { value, .. } => ManifestExtent::Fill {
                logical_length,
                value: *value,
            },
        };
        push_extent(output, extent)?;
        cursor = overlap_end;
    }
    if cursor != end {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

fn plan_manifest_ranges<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    stack: &mut Vec<(u64, u64)>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    while let Some((offset, length)) = stack.pop() {
        assert!(
            length > 0,
            "ASSERT: manifest planner range must be nonempty"
        );
        let allocated = inode.allocated_bytes_in_range(offset, length)?;
        if allocated > length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if allocated == 0 {
            push_extent(
                extents,
                ManifestExtent::Hole {
                    logical_length: length,
                },
            )?;
            continue;
        }
        if allocated == length {
            plan_allocated_range(inode, offset, length, writer, extents)?;
            continue;
        }

        let left_length = if allocated == length {
            (MAX_LOGICAL_CHUNK_BYTES as u64).min(length)
        } else {
            length / 2
        };
        if left_length == 0 || left_length == length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let right_offset = offset
            .checked_add(left_length)
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        stack.push((right_offset, length - left_length));
        stack.push((offset, left_length));
    }
    Ok(())
}

fn plan_allocated_range<C: StorageIo>(
    inode: &CommitInode,
    offset: u64,
    length: u64,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    assert!(length > 0, "ASSERT: a DATA range must be nonempty");
    assert_eq!(
        CDC_MAXIMUM_BYTES, MAX_LOGICAL_CHUNK_BYTES,
        "ASSERT: FastCDC-v1 maximum must equal the durable format bound"
    );
    let reader = CommitRangeReader {
        inode,
        start: offset,
        consumed: 0,
        length,
    };
    let mut chunks = StreamCDC::with_level_and_seed(
        reader,
        CDC_MINIMUM_BYTES,
        CDC_TARGET_BYTES,
        CDC_MAXIMUM_BYTES,
        Normalization::Level1,
        CDC_SEED_V1,
    );
    let mut expected_offset = 0_u64;
    loop {
        let cdc_started = PhaseStarted::now();
        let result = chunks.next();
        cdc_started.finish_into(&mut writer.metrics.fastcdc);
        let Some(result) = result else {
            break;
        };
        let chunk = result.map_err(io::Error::from)?;
        assert_eq!(
            chunk.offset, expected_offset,
            "ASSERT: FastCDC-v1 chunks must be contiguous"
        );
        assert_eq!(
            chunk.length,
            chunk.data.len(),
            "ASSERT: a FastCDC chunk length must equal its owned bytes"
        );
        assert!(
            chunk.length > 0 && chunk.length <= CDC_MAXIMUM_BYTES,
            "ASSERT: FastCDC-v1 returned an invalid logical Chunk length"
        );
        let logical_length =
            u64::try_from(chunk.length).expect("ASSERT: a bounded FastCDC chunk length fits u64");
        writer.metrics.logical_chunks = writer
            .metrics
            .logical_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint logical Chunk count cannot overflow u64");
        writer.metrics.logical_chunk_bytes = writer
            .metrics
            .logical_chunk_bytes
            .checked_add(logical_length)
            .expect("ASSERT: checkpoint logical Chunk bytes cannot overflow u64");
        expected_offset = expected_offset
            .checked_add(logical_length)
            .expect("ASSERT: a FastCDC DATA range cursor cannot overflow");
        let hash_started = PhaseStarted::now();
        let fill = chunk.data.iter().all(|byte| *byte == chunk.data[0]);
        let chunk_id = (!fill).then(|| ChunkId::of(&chunk.data));
        hash_started.finish_into(&mut writer.metrics.hash_and_fill);
        if fill {
            writer.metrics.fill_chunks = writer
                .metrics
                .fill_chunks
                .checked_add(1)
                .expect("ASSERT: checkpoint FILL Chunk count cannot overflow u64");
            writer.metrics.fill_bytes = writer
                .metrics
                .fill_bytes
                .checked_add(logical_length)
                .expect("ASSERT: checkpoint FILL bytes cannot overflow u64");
            push_extent(
                extents,
                ManifestExtent::Fill {
                    logical_length,
                    value: chunk.data[0],
                },
            )?;
        } else {
            let chunk_id = chunk_id.expect("ASSERT: non-FILL data must have one Chunk ID");
            writer.push(chunk_id, chunk.data)?;
            push_extent(
                extents,
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                },
            )?;
        }
    }
    if expected_offset != length {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

struct CommitRangeReader<'a> {
    inode: &'a CommitInode,
    start: u64,
    consumed: u64,
    length: u64,
}

impl Read for CommitRangeReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.consumed == self.length || output.is_empty() {
            return Ok(0);
        }
        let remaining = self
            .length
            .checked_sub(self.consumed)
            .expect("ASSERT: a range reader cursor cannot exceed its length");
        let requested = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(output.len())
            .min(CDC_MAXIMUM_BYTES);
        let requested_u32 =
            u32::try_from(requested).expect("ASSERT: FastCDC-v1 reads never exceed 256 KiB");
        let read_offset = self
            .start
            .checked_add(self.consumed)
            .ok_or_else(|| io::Error::other("commit range read offset overflow"))?;
        let bytes = self
            .inode
            .read_at(read_offset, requested_u32)
            .map_err(|error| io::Error::other(format!("commit range read failed: {error:?}")))?;
        if bytes.len() != requested {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "commit range returned fewer bytes than requested",
            ));
        }
        output[..requested].copy_from_slice(&bytes);
        self.consumed = self
            .consumed
            .checked_add(
                u64::try_from(requested).expect("ASSERT: a bounded range read length fits u64"),
            )
            .expect("ASSERT: a bounded range reader cursor cannot overflow");
        Ok(requested)
    }
}

fn verify_manifest_allocation(
    manifest: &ManifestLeaf,
    inode: &CommitInode,
) -> Result<(), DurableNamespaceError> {
    let planned_allocated = manifest.extents().iter().try_fold(0_u64, |total, extent| {
        let length = match extent {
            ManifestExtent::Data { logical_length, .. }
            | ManifestExtent::Fill { logical_length, .. } => *logical_length,
            ManifestExtent::Hole { .. } => 0,
        };
        total.checked_add(length)
    });
    if planned_allocated != Some(inode.allocated_bytes()) {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

fn push_extent(
    extents: &mut Vec<ManifestExtent>,
    extent: ManifestExtent,
) -> Result<(), DurableNamespaceError> {
    match (extents.last_mut(), &extent) {
        (
            Some(ManifestExtent::Hole { logical_length }),
            ManifestExtent::Hole {
                logical_length: added,
            },
        ) => {
            *logical_length = logical_length
                .checked_add(*added)
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            return Ok(());
        }
        (
            Some(ManifestExtent::Fill {
                logical_length,
                value,
            }),
            ManifestExtent::Fill {
                logical_length: added,
                value: added_value,
            },
        ) if value == added_value => {
            *logical_length = logical_length
                .checked_add(*added)
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            return Ok(());
        }
        _ => {}
    }
    extents
        .try_reserve(1)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    extents.push(extent);
    Ok(())
}

struct AdaptiveCommitWriter<'a, C> {
    containers: &'a ContainerRepository<C>,
    next_generation: &'a mut u64,
    index: &'a dyn ManifestReaderPolicy<C>,
    seen: BTreeMap<ChunkId, u64>,
    chunks: Vec<Vec<u8>>,
    level_zero_entries: Vec<ExactIndexEntry>,
    payload_bytes: usize,
    workers: NonZeroUsize,
    metrics: CheckpointReductionMetrics,
}

impl<'a, C: StorageIo> AdaptiveCommitWriter<'a, C> {
    fn new(
        containers: &'a ContainerRepository<C>,
        next_generation: &'a mut u64,
        index: &'a dyn ManifestReaderPolicy<C>,
        workers: NonZeroUsize,
    ) -> Self {
        Self {
            containers,
            next_generation,
            index,
            seen: BTreeMap::new(),
            chunks: Vec::new(),
            level_zero_entries: Vec::new(),
            payload_bytes: 0,
            workers,
            metrics: CheckpointReductionMetrics::default(),
        }
    }

    fn push(&mut self, chunk_id: ChunkId, bytes: Vec<u8>) -> Result<(), DurableNamespaceError> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
        let exact_started = PhaseStarted::now();
        if let Some(previous) = self.seen.insert(chunk_id, length) {
            if previous != length {
                return Err(DurableNamespaceError::ChunkLengthConflict {
                    chunk_id,
                    first_length: previous,
                    second_length: length,
                });
            }
            self.record_exact_hit(length);
            exact_started.finish_into(&mut self.metrics.exact_lookup);
            return Ok(());
        }
        if self
            .index
            .contains_verified(self.containers, chunk_id, length)
        {
            self.record_exact_hit(length);
            exact_started.finish_into(&mut self.metrics.exact_lookup);
            return Ok(());
        }
        exact_started.finish_into(&mut self.metrics.exact_lookup);
        self.metrics.new_chunks = self
            .metrics
            .new_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint new Chunk count cannot overflow u64");
        self.metrics.new_chunk_bytes = self
            .metrics
            .new_chunk_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint new Chunk bytes cannot overflow u64");
        let next_payload = self
            .payload_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        if !self.chunks.is_empty() && next_payload > CONTAINER_PAYLOAD_TARGET_BYTES {
            self.flush()?;
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        self.chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        self.chunks.push(bytes);
        self.metrics.peak_buffered_chunk_bytes = self.metrics.peak_buffered_chunk_bytes.max(
            u64::try_from(self.payload_bytes)
                .expect("ASSERT: a bounded checkpoint Container payload byte count fits u64"),
        );
        self.metrics.peak_buffered_chunks = self.metrics.peak_buffered_chunks.max(
            u64::try_from(self.chunks.len())
                .expect("ASSERT: a bounded checkpoint Chunk count fits u64"),
        );
        Ok(())
    }

    fn record_exact_hit(&mut self, length: u64) {
        self.metrics.exact_hit_chunks = self
            .metrics
            .exact_hit_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint Exact Hit count cannot overflow u64");
        self.metrics.exact_hit_bytes = self
            .metrics
            .exact_hit_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint Exact Hit bytes cannot overflow u64");
    }

    fn finish(
        mut self,
    ) -> Result<(Vec<ExactIndexEntry>, CheckpointReductionMetrics), DurableNamespaceError> {
        self.flush()?;
        Ok((self.level_zero_entries, self.metrics))
    }

    fn flush(&mut self) -> Result<(), DurableNamespaceError> {
        if self.chunks.is_empty() {
            return Ok(());
        }
        let id = random_container_id()?;
        let mut regions = Vec::<Vec<&[u8]>>::new();
        let mut region_bytes = 0_usize;
        for chunk in &self.chunks {
            let next_region_bytes = region_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            if !regions.last().is_none_or(Vec::is_empty)
                && next_region_bytes > COMPRESSION_REGION_TARGET_BYTES
            {
                region_bytes = 0;
            }
            if region_bytes == 0 {
                regions
                    .try_reserve(1)
                    .map_err(|_| DurableNamespaceError::OutOfMemory)?;
                regions.push(Vec::new());
            }
            let region = regions
                .last_mut()
                .expect("ASSERT: a zero region cursor must create one region");
            region
                .try_reserve(1)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            region.push(chunk.as_slice());
            region_bytes = region_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            assert!(
                region_bytes <= COMPRESSION_REGION_TARGET_BYTES,
                "ASSERT: no logical Chunk may exceed a Compression Region"
            );
        }
        let region_refs = regions.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let generation = *self.next_generation;
        *self.next_generation = generation
            .checked_add(1)
            .ok_or(DurableNamespaceError::ContainerGenerationExhausted)?;
        let (verified, publish_metrics) =
            self.containers.publish_adaptive_regions_parallel_profiled(
                id,
                generation,
                &region_refs,
                self.workers,
            )?;
        self.record_container_metrics(publish_metrics);
        self.level_zero_entries
            .try_reserve(verified.locations().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        self.level_zero_entries
            .extend(verified.locations().iter().copied().map(|location| {
                ExactIndexEntry::from_verified(location).expect(
                "ASSERT: fully verified Container evidence must form a valid Exact-Index Location",
            )
            }));
        self.chunks.clear();
        self.payload_bytes = 0;
        Ok(())
    }

    fn record_container_metrics(&mut self, published: AdaptiveContainerPublishMetrics) {
        self.metrics
            .compression_encode
            .add(published.encode_wall(), published.encode_process_cpu());
        self.metrics
            .container_publish
            .add(published.publish_wall(), published.publish_process_cpu());
        self.metrics.container_file_bytes = self
            .metrics
            .container_file_bytes
            .checked_add(published.file_bytes())
            .expect("ASSERT: checkpoint Container file bytes cannot overflow u64");
        self.metrics.raw_records = self
            .metrics
            .raw_records
            .checked_add(
                u64::try_from(published.raw_records())
                    .expect("ASSERT: bounded RAW Record count fits u64"),
            )
            .expect("ASSERT: checkpoint RAW Record count cannot overflow u64");
        self.metrics.zstd_records = self
            .metrics
            .zstd_records
            .checked_add(
                u64::try_from(published.zstd_records())
                    .expect("ASSERT: bounded Zstd Record count fits u64"),
            )
            .expect("ASSERT: checkpoint Zstd Record count cannot overflow u64");
        self.metrics.containers = self
            .metrics
            .containers
            .checked_add(1)
            .expect("ASSERT: checkpoint Container count cannot overflow u64");
        assert_eq!(
            published.logical_bytes(),
            self.chunks.iter().fold(0_u64, |total, chunk| {
                total
                    .checked_add(
                        u64::try_from(chunk.len()).expect("ASSERT: bounded Chunk length fits u64"),
                    )
                    .expect("ASSERT: bounded Container logical bytes cannot overflow")
            }),
            "ASSERT: profiled Container logical bytes must equal buffered Chunks"
        );
    }
}

fn random_container_id() -> Result<ContainerId, DurableNamespaceError> {
    let mut random = File::open("/dev/urandom")?;
    loop {
        let mut bytes = [0_u8; 16];
        random.read_exact(&mut bytes)?;
        if bytes != [0; 16] {
            return ContainerId::new(bytes).map_err(|_| DurableNamespaceError::FrozenViewMismatch);
        }
    }
}

#[derive(Debug)]
pub enum DurableNamespaceError {
    Io(io::Error),
    Posix(PosixError),
    Metadata(MetadataFormatError),
    Store(StoreError),
    Generation(GenerationError),
    Manifest(ManifestReadError),
    Mount(MountError),
    InvalidReservationSpan,
    InodeReservationExhausted,
    ContainerGenerationExhausted,
    OutOfMemory,
    FrozenViewMismatch,
    ChunkLengthConflict {
        chunk_id: ChunkId,
        first_length: u64,
        second_length: u64,
    },
}

impl fmt::Display for DurableNamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DurableNamespaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Generation(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::Mount(error) => Some(error),
            Self::Posix(_)
            | Self::InvalidReservationSpan
            | Self::InodeReservationExhausted
            | Self::ContainerGenerationExhausted
            | Self::OutOfMemory
            | Self::FrozenViewMismatch
            | Self::ChunkLengthConflict { .. } => None,
        }
    }
}

impl From<io::Error> for DurableNamespaceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<PosixError> for DurableNamespaceError {
    fn from(error: PosixError) -> Self {
        Self::Posix(error)
    }
}

impl From<MetadataFormatError> for DurableNamespaceError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
}

impl From<StoreError> for DurableNamespaceError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<GenerationError> for DurableNamespaceError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<ManifestReadError> for DurableNamespaceError {
    fn from(error: ManifestReadError) -> Self {
        Self::Manifest(error)
    }
}

impl From<MountError> for DurableNamespaceError {
    fn from(error: MountError) -> Self {
        Self::Mount(error)
    }
}
