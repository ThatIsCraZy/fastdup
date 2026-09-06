use fastdup_store::{WorkerPermitLease, WorkerPermits};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak, mpsc};
use std::time::{Duration, Instant};

use fastdup_copy_metrics::{CopyClass, record_copy};
use fastdup_format::{
    ChunkId, CommitRecord, ContainerId, DurableInode, DurableRootMetadata, DurableTimes,
    DurableTimestamp, DurableXattr, ExactIndexEntry, ExactIndexProfileId,
    IncompressibilityGateMetrics, MAX_LOGICAL_CHUNK_BYTES, ManifestExtent, ManifestLayout,
    MetadataFormatError, MetadataObjectId, NamespaceEntry, NamespaceRoot, PolicySetId,
    PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion,
};
use fastdup_posix::{
    CommitInode, CommitRange, CommittedFile, CommittedFileInstall, ExternalizedExtent, InodeId,
    MutationObserver, MutationPayload, Namespace, NamespaceCommit, NamespaceConfig, PosixError,
    PreparedCommitExtent, PreparedDataRecipe,
};
use fastdup_store::{
    AdaptiveContainerPublishMetrics, CONTAINER_GENERATION_RESERVATION_SPAN_V1,
    ContainerDescriptorCacheStatus, ContainerGenerationAllocator, ContainerPlacement,
    ContainerRepository, ExactIndexPageCacheStatus, ExactIndexRunRepository,
    ExactRunMembershipStatus, GenerationError, GenerationRepository, IndexedRequiredChunkVerifier,
    ManifestReadError, ManifestSuccessorProof, ManifestTreeSummary, PersistentChunkPlan,
    PersistentReductionIndex, PersistentReductionStatus, RequiredChunkVerifier, SeqCdcConfig,
    SimilarityIndexPageCacheStatus, SimilarityIndexRepository, StorageIo, StoreError,
    SuccessorPredecessor, VerifiedCommittedFile, VerifiedManifestFile, VerifiedReadCache,
    VerifiedReadCacheError, VerifiedReadCacheStatus, seqcdc_cut, seqcdc_cut_scalar,
    seqcdc_cut_segmented, seqcdc_cut_segmented_scalar,
};
use hashbrown::HashTable;
use rayon::prelude::*;

use crate::historical_proof_cache::{HistoricalProofAdmission, HistoricalProofCache};
use crate::proof_cache_trace::ProofCacheTraceRecorder;
use crate::{
    HistoricalProofCacheStatus, ManifestCommittedFile, MountError, ProofCacheEvent,
    ProofCacheReplayError, ProofCacheTrace, ProofKey, namespace_from_verified_files_using,
};

const FIRST_REGULAR_INODE: u64 = 2;
/// Production Inode IDs durably reserved per writable appliance start.
///
/// A 2^32 range keeps allocation entirely in-memory for any practical process
/// lifetime while a restart can still skip its unused suffix without reuse.
pub const INODE_RESERVATION_SPAN_V1: u64 = 1_u64 << 32;
const CONTAINER_PAYLOAD_TARGET_BYTES: usize = 32 * 1_024 * 1_024;
const CONTAINER_PAYLOAD_FLUSH_BYTES: usize = CONTAINER_PAYLOAD_TARGET_BYTES - CDC_MAXIMUM_BYTES;
const COMPRESSION_REGION_TARGET_BYTES: usize = 512 * 1_024;
const CDC_MINIMUM_BYTES: usize = 16 * 1_024;
const CDC_MAXIMUM_BYTES: usize = 256 * 1_024;
const SEQCDC_CONFIG_V1: SeqCdcConfig = SeqCdcConfig {
    sequence_length: 6,
    skip_trigger: 50,
    skip_bytes: 1_024,
    minimum_bytes: CDC_MINIMUM_BYTES,
    maximum_bytes: CDC_MAXIMUM_BYTES,
};
const MAX_CHUNK_FRAGMENTS_V1: usize = 1_024;
type RetainedManifestRanges = BTreeMap<u64, BTreeMap<MetadataObjectId, Vec<Range<u64>>>>;
type AdaptiveCommitFinish = (
    Vec<ExactIndexEntry>,
    CheckpointReductionMetrics,
    RetainedManifestRanges,
);
const EXACT_PUBLICATION_QUEUE_BATCHES: usize = 8;
const MAX_RECENT_EXACT_LOCATIONS: usize = 8_192;
// Shared admission cap for cached Active and Frozen dependency proofs.
// Externalized DATA and short boundary Chunks are not bounded by resident bytes.
const MAX_ONLINE_DEPENDENCY_PROOFS_V1: usize = 65_536;
const WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1: usize = 400 * 1_024 * 1_024;
const WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1: usize = 32 * 1_024 * 1_024;
const MULTI_STREAM_QUEUE_BUDGET_BYTES_V1: usize = 16 * 1_024 * 1_024;
const DETACHED_CONTAINER_BUDGET_BYTES_V1: usize = 2 * CONTAINER_PAYLOAD_TARGET_BYTES;
const SINGLE_STREAM_PUBLICATION_WINDOW_V1: usize = 2;
const WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1: usize = 1_024 * 1_024;
// Owned request fragments remain separate allocations. The Ingest Ring groups
// only their immutable views, so sealing a batch never copies payload bytes.
const SINGLE_STREAM_INGEST_BATCH_BYTES_V1: usize = 4 * 1_024 * 1_024;
const SINGLE_STREAM_INGEST_RING_SLOTS_V1: usize = 8;
const INGEST_BATCH_MAXIMUM_AGE_V1: Duration = Duration::from_millis(10);
const MAX_ACTIVE_INGEST_LANES_V1: usize = (WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1
    - WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1
    - DETACHED_CONTAINER_BUDGET_BYTES_V1)
    / (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES)
    - 1;
const _: () = assert!(
    WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1
        >= WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1
            + DETACHED_CONTAINER_BUDGET_BYTES_V1
            + 2 * (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES)
);

/// V1 scheduler high-water for active checkpointable DATA.
///
/// This is deliberately expressed from the durable format's 64-MiB maximum
/// Container size rather than the adaptive writer's current 32-MiB payload
/// target. Reaching it starts an early checkpoint and applies admission
/// backpressure until durable progress catches up.
pub const CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1: u64 = 8 * fastdup_format::MAX_CONTAINER_BYTES;

/// Cumulative scheduler evidence for one write-through CPU phase.
///
/// Runnable wall time starts after permit acquisition and ends after the CPU
/// work. `permit_wait_ns` includes uncontended lock acquisition, while
/// `permit_blocked_phases` counts phases that actually waited on the permit
/// condition variable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuPhaseStatus {
    phases: u64,
    active: u64,
    maximum_active: u64,
    runnable_wall_ns: u64,
    permit_blocked_phases: u64,
    permit_wait_ns: u64,
    maximum_permit_wait_ns: u64,
    requested_workers: u64,
    granted_workers: u64,
    partial_grants: u64,
}

impl CpuPhaseStatus {
    #[must_use]
    pub const fn phases(self) -> u64 {
        self.phases
    }

    #[must_use]
    pub const fn active(self) -> u64 {
        self.active
    }

    #[must_use]
    pub const fn maximum_active(self) -> u64 {
        self.maximum_active
    }

    #[must_use]
    pub const fn runnable_wall_ns(self) -> u64 {
        self.runnable_wall_ns
    }

    #[must_use]
    pub const fn permit_blocked_phases(self) -> u64 {
        self.permit_blocked_phases
    }

    #[must_use]
    pub const fn permit_wait_ns(self) -> u64 {
        self.permit_wait_ns
    }

    #[must_use]
    pub const fn maximum_permit_wait_ns(self) -> u64 {
        self.maximum_permit_wait_ns
    }

    #[must_use]
    pub const fn requested_workers(self) -> u64 {
        self.requested_workers
    }

    #[must_use]
    pub const fn granted_workers(self) -> u64 {
        self.granted_workers
    }

    #[must_use]
    pub const fn partial_grants(self) -> u64 {
        self.partial_grants
    }
}

/// Bounded process-local state of the pre-commit reduction pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteThroughStatus {
    buffered_bytes: u64,
    queued_bytes: u64,
    active_lanes: u64,
    sealed_uncommitted_containers: u64,
    oldest_sealed_age: Option<Duration>,
    hash_batches: u64,
    maximum_hash_workers: u64,
    ingest_batches: u64,
    ingest_fragments: u64,
    maximum_ingest_batch_bytes: u64,
    minimum_ingest_batch_target_bytes: u64,
    maximum_ingest_batch_target_bytes: u64,
    maximum_ingest_ring_slots: u64,
    ingest_ring_wait_ns: u64,
    hash_cpu: CpuPhaseStatus,
    encode_cpu: CpuPhaseStatus,
    planning_cpu: CpuPhaseStatus,
    materialization_wall_ns: u64,
    advanced_reduction: PersistentReductionStatus,
    degraded: bool,
}

impl WriteThroughStatus {
    #[must_use]
    pub const fn buffered_bytes(self) -> u64 {
        self.buffered_bytes
    }

    #[must_use]
    pub const fn queued_bytes(self) -> u64 {
        self.queued_bytes
    }

    #[must_use]
    pub const fn active_lanes(self) -> u64 {
        self.active_lanes
    }

    #[must_use]
    pub const fn sealed_uncommitted_containers(self) -> u64 {
        self.sealed_uncommitted_containers
    }

    #[must_use]
    pub const fn oldest_sealed_age(self) -> Option<Duration> {
        self.oldest_sealed_age
    }

    #[must_use]
    pub const fn hash_batches(self) -> u64 {
        self.hash_batches
    }

    #[must_use]
    pub const fn maximum_hash_workers(self) -> u64 {
        self.maximum_hash_workers
    }

    #[must_use]
    pub const fn ingest_batches(self) -> u64 {
        self.ingest_batches
    }

    #[must_use]
    pub const fn ingest_fragments(self) -> u64 {
        self.ingest_fragments
    }

    #[must_use]
    pub const fn maximum_ingest_batch_bytes(self) -> u64 {
        self.maximum_ingest_batch_bytes
    }

    #[must_use]
    pub const fn minimum_ingest_batch_target_bytes(self) -> u64 {
        self.minimum_ingest_batch_target_bytes
    }

    #[must_use]
    pub const fn maximum_ingest_batch_target_bytes(self) -> u64 {
        self.maximum_ingest_batch_target_bytes
    }

    #[must_use]
    pub const fn maximum_ingest_ring_slots(self) -> u64 {
        self.maximum_ingest_ring_slots
    }

    #[must_use]
    pub const fn ingest_ring_wait_ns(self) -> u64 {
        self.ingest_ring_wait_ns
    }

    #[must_use]
    pub const fn hash_cpu(self) -> CpuPhaseStatus {
        self.hash_cpu
    }

    #[must_use]
    pub const fn encode_cpu(self) -> CpuPhaseStatus {
        self.encode_cpu
    }

    /// Bounded Advanced planning wall time, including candidate Base reads.
    #[must_use]
    pub const fn planning_cpu(self) -> CpuPhaseStatus {
        self.planning_cpu
    }

    /// Summed preparation wall time, including CPU admission waits.
    #[must_use]
    pub const fn materialization_wall_ns(self) -> u64 {
        self.materialization_wall_ns
    }

    #[must_use]
    pub const fn advanced_reduction(self) -> PersistentReductionStatus {
        self.advanced_reduction
    }

    #[must_use]
    pub const fn degraded(self) -> bool {
        self.degraded
    }
}

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
/// Nested `manifest_plan` contains CDC, hash/FILL, Exact lookup, encoding,
/// and Container publication. The leaf phases may be summed; callers must not
/// add the parent to them. Process CPU includes all process threads active in
/// the phase, including compression workers and concurrent FUSE request work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CheckpointMetrics {
    total: CheckpointPhaseMetrics,
    freeze: CheckpointPhaseMetrics,
    manifest_plan: CheckpointPhaseMetrics,
    cdc: CheckpointPhaseMetrics,
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
    incompressibility_gate: IncompressibilityGateMetrics,
    containers: u64,
    peak_buffered_chunk_bytes: u64,
    peak_buffered_chunks: u64,
    recipe_reuse_chunks: u64,
    recipe_reuse_bytes: u64,
    checkpoint_rechunk_bytes: u64,
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
    phase_getter!(cdc);
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
    pub const fn incompressibility_gate(self) -> IncompressibilityGateMetrics {
        self.incompressibility_gate
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

    #[must_use]
    pub const fn recipe_reuse_chunks(self) -> u64 {
        self.recipe_reuse_chunks
    }

    #[must_use]
    pub const fn recipe_reuse_bytes(self) -> u64 {
        self.recipe_reuse_bytes
    }

    #[must_use]
    pub const fn checkpoint_rechunk_bytes(self) -> u64 {
        self.checkpoint_rechunk_bytes
    }

    fn merge_reduction(&mut self, reduction: &CheckpointReductionMetrics) {
        self.cdc = reduction.cdc;
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
        self.incompressibility_gate = reduction.incompressibility_gate;
        self.containers = reduction.containers;
        self.peak_buffered_chunk_bytes = reduction.peak_buffered_chunk_bytes;
        self.peak_buffered_chunks = reduction.peak_buffered_chunks;
        self.recipe_reuse_chunks = reduction.recipe_reuse_chunks;
        self.recipe_reuse_bytes = reduction.recipe_reuse_bytes;
        self.checkpoint_rechunk_bytes = reduction.checkpoint_rechunk_bytes;
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
    cdc: CheckpointPhaseMetrics,
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
    incompressibility_gate: IncompressibilityGateMetrics,
    containers: u64,
    peak_buffered_chunk_bytes: u64,
    peak_buffered_chunks: u64,
    recipe_reuse_chunks: u64,
    recipe_reuse_bytes: u64,
    checkpoint_rechunk_bytes: u64,
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
/// The canonical bytes pin SeqCDC-v1, region sizing, adaptive Zstd thresholds,
/// Exact publication, the optional coherent Similarity profile, bounded
/// candidate/trial counts, and Depth-1 dependent-codec admission. Every new
/// repository uses this Policy Set from its first Commit. Disabling dependent
/// selection or lacking a usable Similarity snapshot selects the independent
/// RAW/Zstd fallback without changing the Policy Set.
/// The legacy `paired-exact-v1`/`contiguous-only` spelling remains byte-stable
/// for existing repositories: online hint refresh and one-time materialization
/// change candidate admission, not any permitted durable record or codec.
///
/// # Panics
///
/// Panics only if BLAKE3 maps the fixed canonical policy bytes to the reserved
/// all-zero identity, an impossible production `ASSERT` for this pinned input.
#[must_use]
pub fn checkpoint_policy_set() -> PolicySetId {
    PolicySetId::new(
        ChunkId::of(
            b"fastdup/checkpoint-policy-v3/SeqCDC=increasing:seq6:skip-trigger50:skip1024:min16384:max262144:append-tail-anchor-v1/region=524288/Zstd=level3:min4096:min3pct/exact=l0-runs-v2:fanin4:partition262144/proof=installed-successor-delta-v1/similarity=fingerprint-v1:bucket-v1:candidates16:trials4:paired-exact-v1/dependent=codec3-zstd-prefix+codec4-sparse-xor:depth1:min4096:min5pct:contiguous-only",
        )
        .bytes(),
    )
    .expect("ASSERT: the current checkpoint Policy Set hash is nonzero")
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
    installed_predecessor: Mutex<SuccessorPredecessor>,
    manifests: Mutex<Vec<InstalledManifest>>,
    container_generations: ContainerGenerationAllocator<C>,
    manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
    checkpoint_workers: NonZeroUsize,
    write_through: Arc<WriteThroughIngest<C>>,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
}

#[derive(Clone, Copy, Debug)]
struct GenerationProof {
    entry: ExactIndexEntry,
    admission: HistoricalProofAdmission,
}

/// Full identities live once in the arena; table buckets carry only ordinals.
/// Entries never move independently or disappear while the table is queryable.
#[derive(Debug, Default)]
struct GenerationProofMap {
    index: HashTable<u32>,
    entries: Vec<GenerationProof>,
}

impl GenerationProofMap {
    fn key(proof: &GenerationProof) -> (ChunkId, u32) {
        (proof.entry.chunk_id(), proof.entry.logical_length())
    }

    fn hash(key: (ChunkId, u32)) -> u64 {
        let bytes = key.0.bytes();
        u64::from_le_bytes(bytes[..8].try_into().expect("fixed Chunk ID prefix"))
            ^ u64::from_le_bytes(bytes[24..].try_into().expect("fixed Chunk ID suffix"))
                .rotate_left(23)
            ^ u64::from(key.1).wrapping_mul(0x9E37_79B1_85EB_CA87)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn get(&self, key: &(ChunkId, u32)) -> Option<&GenerationProof> {
        let &ordinal = self.index.find(Self::hash(*key), |&ordinal| {
            Self::key(&self.entries[ordinal as usize]) == *key
        })?;
        Some(&self.entries[ordinal as usize])
    }

    fn get_mut(&mut self, key: &(ChunkId, u32)) -> Option<&mut GenerationProof> {
        let &ordinal = self.index.find(Self::hash(*key), |&ordinal| {
            Self::key(&self.entries[ordinal as usize]) == *key
        })?;
        Some(&mut self.entries[ordinal as usize])
    }

    fn insert(
        &mut self,
        key: (ChunkId, u32),
        mut proof: GenerationProof,
    ) -> Option<GenerationProof> {
        assert_eq!(
            key,
            Self::key(&proof),
            "ASSERT: arena key matches proof identity"
        );
        if let Some(previous) = self.get_mut(&key) {
            if previous.admission == HistoricalProofAdmission::ExactReuse {
                proof.admission = previous.admission;
            }
            return Some(std::mem::replace(previous, proof));
        }
        assert!(
            self.len() < MAX_ONLINE_DEPENDENCY_PROOFS_V1,
            "ASSERT: proof arena is bounded"
        );
        let ordinal = u32::try_from(self.len()).expect("ASSERT: bounded proof ordinal fits u32");
        self.entries.push(proof);
        self.index
            .insert_unique(Self::hash(key), ordinal, |&ordinal| {
                Self::hash(Self::key(&self.entries[ordinal as usize]))
            });
        None
    }

    fn into_sorted_values(self) -> impl Iterator<Item = GenerationProof> {
        let Self { index, mut entries } = self;
        drop(index);
        // Historical admission order must remain identical to the old BTree.
        entries.sort_unstable_by_key(Self::key);
        entries.into_iter()
    }

    fn allocated_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<GenerationProof>()
            + self.index.allocation_size()
    }
}

/// Bounded, non-evictable proof ownership for the two live commit generations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GenerationProofSetStatus {
    active_proofs: usize,
    frozen_proofs: usize,
    accounted_bytes: usize,
}

impl GenerationProofSetStatus {
    #[must_use]
    pub const fn active_proofs(self) -> usize {
        self.active_proofs
    }

    #[must_use]
    pub const fn frozen_proofs(self) -> usize {
        self.frozen_proofs
    }

    #[must_use]
    pub const fn accounted_bytes(self) -> usize {
        self.accounted_bytes
    }
}

#[derive(Debug, Default)]
struct GenerationProofState {
    active: GenerationProofMap,
    frozen: Option<GenerationProofMap>,
    publishing: BTreeSet<(ChunkId, u32)>,
}

impl GenerationProofState {
    /// Keep admitted proofs pinned until commit completion. At capacity, a new
    /// dependency remains uncached and the successor verifier must verify it
    /// through storage. Resident Dirty DATA does not bound externalized Chunks.
    fn remember(&mut self, key: (ChunkId, u32), proof: GenerationProof, frozen: bool) -> bool {
        let count = self.active.len() + self.frozen.as_ref().map_or(0, GenerationProofMap::len);
        let target = if frozen {
            self.frozen
                .as_mut()
                .expect("ASSERT: commit-only proof requires one Frozen Generation")
        } else {
            &mut self.active
        };
        if count >= MAX_ONLINE_DEPENDENCY_PROOFS_V1 && target.get(&key).is_none() {
            return false;
        }
        target.insert(key, proof);
        true
    }
}

#[derive(Debug)]
struct OnlineDependencyProofs {
    generation: Mutex<GenerationProofState>,
    publication_completed: Condvar,
    historical: HistoricalProofCache,
    trace: ProofCacheTraceRecorder,
}

#[derive(Clone, Copy)]
enum OnlineProofAdmission {
    Published,
    ExactReuse,
    Touch,
}

impl OnlineDependencyProofs {
    fn new() -> Result<Self, DurableNamespaceError> {
        Ok(Self {
            generation: Mutex::new(GenerationProofState::default()),
            publication_completed: Condvar::new(),
            historical: HistoricalProofCache::new_system()
                .map_err(|_| DurableNamespaceError::OutOfMemory)?,
            trace: ProofCacheTraceRecorder::default(),
        })
    }

    fn remember_active(&self, entry: ExactIndexEntry, admission: OnlineProofAdmission) {
        self.remember_generation(entry, admission, false);
    }

    fn remember_frozen(&self, entry: ExactIndexEntry, admission: OnlineProofAdmission) {
        self.remember_generation(entry, admission, true);
    }

    fn remember_generation(
        &self,
        entry: ExactIndexEntry,
        admission: OnlineProofAdmission,
        frozen: bool,
    ) {
        assert_eq!(
            entry.transition(),
            fastdup_format::ExactLocationTransition::Active,
            "ASSERT: only an ACTIVE verified Location can prove an online dependency"
        );
        let key = (entry.chunk_id(), entry.logical_length());
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let historical_admission = match admission {
            OnlineProofAdmission::Published => HistoricalProofAdmission::Published,
            OnlineProofAdmission::ExactReuse | OnlineProofAdmission::Touch => {
                HistoricalProofAdmission::ExactReuse
            }
        };
        if !state.remember(
            key,
            GenerationProof {
                entry,
                admission: historical_admission,
            },
            frozen,
        ) {
            return;
        }
        drop(state);
        let key = ProofKey::new(entry.chunk_id(), entry.logical_length());
        let verify_bytes = entry.location().record_length();
        match admission {
            OnlineProofAdmission::Published => self
                .trace
                .record(ProofCacheEvent::admit_published(key, verify_bytes)),
            OnlineProofAdmission::ExactReuse | OnlineProofAdmission::Touch => self
                .trace
                .record(ProofCacheEvent::admit_exact_reuse(key, verify_bytes)),
        }
    }

    fn freeze_for_commit(&self) -> bool {
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        if state.frozen.is_some() {
            return false;
        }
        let frozen = std::mem::take(&mut state.active);
        state.frozen = Some(frozen);
        true
    }

    fn cancel_new_freeze(&self, newly_frozen: bool) {
        if !newly_frozen {
            return;
        }
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let frozen = state
            .frozen
            .take()
            .expect("ASSERT: a new proof freeze must still own Frozen state");
        for proof in frozen.into_sorted_values() {
            let key = GenerationProofMap::key(&proof);
            if state.active.get(&key).is_none() {
                state.active.insert(key, proof);
            }
        }
        assert!(
            state.active.len() <= MAX_ONLINE_DEPENDENCY_PROOFS_V1,
            "ASSERT: canceled proof freeze exceeded the combined Generation Proof budget"
        );
    }

    fn complete_frozen(&self) {
        let frozen = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned")
            .frozen
            .take()
            .expect("ASSERT: a successful commit owns one Frozen Generation Proof Set");
        for proof in frozen.into_sorted_values() {
            self.historical.admit(proof.entry, proof.admission);
        }
    }

    fn generation_entry(&self, key: (ChunkId, u32)) -> Option<ExactIndexEntry> {
        let state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        state
            .active
            .get(&key)
            .or_else(|| state.frozen.as_ref().and_then(|frozen| frozen.get(&key)))
            .map(|proof| proof.entry)
    }

    fn unproven(&self, required: &BTreeMap<ChunkId, u64>) -> BTreeMap<ChunkId, u64> {
        let mut unproven = BTreeMap::new();
        let mut history_hits = Vec::new();
        for (chunk_id, logical_length) in required {
            let Ok(index_length) = u32::try_from(*logical_length) else {
                unproven.insert(*chunk_id, *logical_length);
                continue;
            };
            let key = (*chunk_id, index_length);
            if let Some(entry) = self.generation_entry(key) {
                assert_entry_matches(entry, *chunk_id, index_length);
                continue;
            }
            if let Some(entry) = self.historical.get(*chunk_id, *logical_length) {
                assert_entry_matches(entry, *chunk_id, index_length);
                history_hits.push(entry);
            } else {
                unproven.insert(*chunk_id, *logical_length);
            }
        }
        for entry in history_hits {
            self.remember_frozen(entry, OnlineProofAdmission::Touch);
        }
        for (chunk_id, logical_length) in required {
            if let Ok(logical_length) = u32::try_from(*logical_length) {
                self.trace.record(ProofCacheEvent::lookup(ProofKey::new(
                    *chunk_id,
                    logical_length,
                )));
            }
        }
        unproven
    }

    fn verified_entry(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        let logical_length = u32::try_from(logical_length).ok()?;
        let entry = self
            .generation_entry((chunk_id, logical_length))
            .or_else(|| self.historical.get(chunk_id, u64::from(logical_length)));
        self.trace.record(ProofCacheEvent::lookup(ProofKey::new(
            chunk_id,
            logical_length,
        )));
        entry
    }

    /// Finds and promotes a proof for ingest under one Generation lock. A
    /// Frozen/history hits are promoted when capacity permits; otherwise the
    /// successor verifier independently verifies uncached dependencies.
    fn verified_entry_for_active(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<ExactIndexEntry> {
        let logical_length = u32::try_from(logical_length).ok()?;
        let key = (chunk_id, logical_length);
        let generation_entry = {
            let mut state = self
                .generation
                .lock()
                .expect("ASSERT: Generation Proof Set lock poisoned");
            if let Some(proof) = state.active.get_mut(&key) {
                proof.admission = HistoricalProofAdmission::ExactReuse;
                Some(proof.entry)
            } else {
                let entry = state
                    .frozen
                    .as_ref()
                    .and_then(|proofs| proofs.get(&key))
                    .map(|proof| proof.entry);
                if let Some(entry) = entry {
                    state.remember(
                        key,
                        GenerationProof {
                            entry,
                            admission: HistoricalProofAdmission::ExactReuse,
                        },
                        false,
                    );
                }
                entry
            }
        };
        let entry =
            generation_entry.or_else(|| self.historical.get(chunk_id, u64::from(logical_length)));
        self.trace.record(ProofCacheEvent::lookup(ProofKey::new(
            chunk_id,
            logical_length,
        )));
        if let Some(entry) = entry {
            assert_entry_matches(entry, chunk_id, logical_length);
            if generation_entry.is_some() {
                self.trace.record(ProofCacheEvent::admit_exact_reuse(
                    ProofKey::new(chunk_id, logical_length),
                    entry.location().record_length(),
                ));
            } else {
                self.remember_active(entry, OnlineProofAdmission::Touch);
            }
        }
        entry
    }

    /// Claims one missing Chunk for publication or waits for the current
    /// in-process publisher to install its proof.
    ///
    /// Callers claim keys in ascending `(ChunkId, logical_length)` order. That
    /// ordering prevents two partially overlapping Container batches from
    /// waiting on each other while retaining disjoint claims.
    fn claim_publication(&self, chunk_id: ChunkId, logical_length: u32) -> PublicationClaim {
        let key = (chunk_id, logical_length);
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        loop {
            if let Some(proof) = state.active.get(&key) {
                assert_entry_matches(proof.entry, chunk_id, logical_length);
                return PublicationClaim::Existing(proof.entry);
            }
            if let Some(entry) = state
                .frozen
                .as_ref()
                .and_then(|frozen| frozen.get(&key))
                .map(|proof| proof.entry)
            {
                assert_entry_matches(entry, chunk_id, logical_length);
                drop(state);
                self.remember_active(entry, OnlineProofAdmission::Touch);
                return PublicationClaim::Existing(entry);
            }
            if state.publishing.insert(key) {
                return PublicationClaim::Acquired;
            }
            state = self
                .publication_completed
                .wait(state)
                .expect("ASSERT: Generation Proof Set lock poisoned while awaiting publication");
        }
    }

    fn finish_publications(&self, entries: &[ExactIndexEntry], claimed: &[(ChunkId, u32)]) {
        assert_eq!(
            entries.len(),
            claimed.len(),
            "ASSERT: every publication claim must produce one verified Location"
        );
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        for (entry, key) in entries.iter().zip(claimed) {
            assert_eq!(
                entry.transition(),
                fastdup_format::ExactLocationTransition::Active,
                "ASSERT: only an ACTIVE verified Location can prove an online dependency"
            );
            assert_entry_matches(*entry, key.0, key.1);
            assert!(
                state.publishing.contains(key),
                "ASSERT: completed publication must own its Chunk claim"
            );
            assert!(
                state.active.get(key).is_none(),
                "ASSERT: an owned publication claim cannot already have an active proof"
            );
            state.remember(
                *key,
                GenerationProof {
                    entry: *entry,
                    admission: HistoricalProofAdmission::Published,
                },
                false,
            );
        }
        for key in claimed {
            assert!(
                state.publishing.remove(key),
                "ASSERT: completed publication must release its Chunk claim"
            );
        }
        let frozen_proofs = state.frozen.as_ref().map_or(0, GenerationProofMap::len);
        assert!(
            state
                .active
                .len()
                .checked_add(frozen_proofs)
                .is_some_and(|total| total <= MAX_ONLINE_DEPENDENCY_PROOFS_V1),
            "ASSERT: completed publication exceeded the combined Generation Proof budget"
        );
        drop(state);
        self.publication_completed.notify_all();
        for entry in entries {
            self.trace.record(ProofCacheEvent::admit_published(
                ProofKey::new(entry.chunk_id(), entry.logical_length()),
                entry.location().record_length(),
            ));
        }
    }

    fn abandon_publications(&self, claimed: &[(ChunkId, u32)]) {
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        for key in claimed {
            assert!(
                state.publishing.remove(key),
                "ASSERT: failed publication must own its Chunk claim"
            );
        }
        drop(state);
        self.publication_completed.notify_all();
    }

    fn historical_status(&self) -> HistoricalProofCacheStatus {
        self.historical.status()
    }

    fn generation_status(&self) -> GenerationProofSetStatus {
        let state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let active_proofs = state.active.len();
        let frozen_proofs = state.frozen.as_ref().map_or(0, GenerationProofMap::len);
        let accounted_bytes = state
            .active
            .allocated_bytes()
            .checked_add(
                state
                    .frozen
                    .as_ref()
                    .map_or(0, GenerationProofMap::allocated_bytes),
            )
            .expect("ASSERT: bounded Generation Proof allocation accounting cannot overflow");
        GenerationProofSetStatus {
            active_proofs,
            frozen_proofs,
            accounted_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum PublicationClaim {
    Existing(ExactIndexEntry),
    Acquired,
}

struct PublicationClaims<'a> {
    proofs: &'a OnlineDependencyProofs,
    keys: Vec<(ChunkId, u32)>,
    finished: bool,
}

impl<'a> PublicationClaims<'a> {
    fn new(
        proofs: &'a OnlineDependencyProofs,
        capacity: usize,
    ) -> Result<Self, DurableNamespaceError> {
        let mut keys = Vec::new();
        keys.try_reserve(capacity)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        Ok(Self {
            proofs,
            keys,
            finished: false,
        })
    }

    fn claim(&mut self, chunk_id: ChunkId, logical_length: u32) -> PublicationClaim {
        let claim = self.proofs.claim_publication(chunk_id, logical_length);
        if matches!(claim, PublicationClaim::Acquired) {
            self.keys.push((chunk_id, logical_length));
        }
        claim
    }

    fn finish(mut self, entries: &mut [ExactIndexEntry]) {
        // Reduction planning groups ordinary, independent, and dependent
        // records. The resulting writer Locations therefore follow encoding
        // group order, while claims follow Chunk-ID order. Restore the claim
        // order before pairing each verified Location with its key.
        entries.sort_unstable_by_key(|entry| (entry.chunk_id(), entry.logical_length()));
        self.proofs.finish_publications(entries, &self.keys);
        self.finished = true;
    }
}

impl Drop for PublicationClaims<'_> {
    fn drop(&mut self) {
        if !self.finished && !self.keys.is_empty() {
            self.proofs.abandon_publications(&self.keys);
        }
    }
}

fn assert_entry_matches(entry: ExactIndexEntry, chunk_id: ChunkId, logical_length: u32) {
    assert_eq!(
        entry.chunk_id(),
        chunk_id,
        "ASSERT: Generation Proof key matches its verified Location"
    );
    assert_eq!(
        entry.logical_length(),
        logical_length,
        "ASSERT: Generation Proof length matches its verified Location"
    );
}

struct OnlineSuccessorVerifier {
    proofs: Arc<OnlineDependencyProofs>,
    fallback: Box<dyn RequiredChunkVerifier>,
}

impl RequiredChunkVerifier for OnlineSuccessorVerifier {
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        self.fallback
            .verify_required_chunks(&self.proofs.unproven(required))
    }
}

#[derive(Clone, Copy, Debug)]
struct InstalledManifest {
    inode: u64,
    root: MetadataObjectId,
    logical_size: u64,
    allocated_bytes: u64,
    summary: ManifestTreeSummary,
}

struct VerifiedLocationFile {
    source: Arc<dyn CommittedFile>,
    source_offset: u64,
    entry: ExactIndexEntry,
}

impl fmt::Debug for VerifiedLocationFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedLocationFile")
            .field("chunk_id", &self.entry.chunk_id())
            .field("logical_length", &self.entry.logical_length())
            .field("location", &self.entry.location())
            .finish_non_exhaustive()
    }
}

impl CommittedFile for VerifiedLocationFile {
    fn logical_size(&self) -> u64 {
        u64::from(self.entry.logical_length())
    }

    fn allocated_bytes(&self) -> u64 {
        self.logical_size()
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let logical_size = self.logical_size();
        let start = offset.min(logical_size);
        let end = offset.saturating_add(length).min(logical_size);
        Ok(end - start)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.read_shared_at(offset, length).map(Vec::from)
    }

    fn read_shared_at(&self, offset: u64, length: u32) -> Result<bytes::Bytes, PosixError> {
        let start = offset.min(self.logical_size());
        let length = u32::try_from(u64::from(length).min(self.logical_size() - start))
            .map_err(|_| PosixError::Io)?;
        self.source
            .read_shared_at(self.source_offset + start, length)
    }

    fn shared_read_source(&self) -> Option<(Arc<dyn CommittedFile>, u64)> {
        Some((Arc::clone(&self.source), self.source_offset))
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        Ok(candidate.len()
            == usize::try_from(self.entry.logical_length())
                .expect("ASSERT: Exact Index length fits usize")
            && ChunkId::of(candidate) == self.entry.chunk_id())
    }

    fn matches_complete_segments(&self, segments: &[&[u8]]) -> Result<bool, PosixError> {
        let mut length = 0_usize;
        let mut hasher = blake3::Hasher::new();
        for segment in segments {
            length = length
                .checked_add(segment.len())
                .ok_or(PosixError::FileTooLarge)?;
            hasher.update(segment);
        }
        Ok(length
            == usize::try_from(self.entry.logical_length())
                .expect("ASSERT: Exact Index length fits usize")
            && ChunkId::from_bytes(*hasher.finalize().as_bytes()) == self.entry.chunk_id())
    }

    fn prepared_data_recipe(&self) -> Option<PreparedDataRecipe> {
        Some(PreparedDataRecipe::Chunk {
            chunk_id: self.entry.chunk_id().bytes(),
        })
    }
}

#[derive(Debug)]
struct FillCommittedFile {
    value: u8,
    length: u64,
}

impl CommittedFile for FillCommittedFile {
    fn logical_size(&self) -> u64 {
        self.length
    }

    fn allocated_bytes(&self) -> u64 {
        self.length
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let start = offset.min(self.length);
        let end = offset.saturating_add(length).min(self.length);
        Ok(end - start)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        let start = offset.min(self.length);
        let end = offset.saturating_add(u64::from(length)).min(self.length);
        let output_length = usize::try_from(end - start).map_err(|_| PosixError::FileTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_length)
            .map_err(|_| PosixError::OutOfMemory)?;
        output.resize(output_length, self.value);
        Ok(output)
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        Ok(
            u64::try_from(candidate.len()).expect("ASSERT: usize fits u64") == self.length
                && candidate.iter().all(|byte| *byte == self.value),
        )
    }

    fn matches_complete_segments(&self, segments: &[&[u8]]) -> Result<bool, PosixError> {
        let mut length = 0_u64;
        for segment in segments {
            length = length
                .checked_add(u64::try_from(segment.len()).expect("ASSERT: usize fits u64"))
                .ok_or(PosixError::FileTooLarge)?;
            if !segment.iter().all(|byte| *byte == self.value) {
                return Ok(false);
            }
        }
        Ok(length == self.length)
    }

    fn prepared_data_recipe(&self) -> Option<PreparedDataRecipe> {
        Some(PreparedDataRecipe::Fill { value: self.value })
    }
}

#[derive(Debug)]
struct PendingWriteThroughChunk {
    offset: u64,
    chunk_id: ChunkId,
    bytes: ChunkFragments,
    placement: ContainerPlacement,
}

struct CompressionRegionPlan<'a> {
    chunks: Vec<&'a PendingWriteThroughChunk>,
    decoded_length: usize,
    materialized: bool,
}

struct MaterializedCompressionRegion {
    decoded: Vec<u8>,
    chunks: Vec<(ChunkId, Range<usize>)>,
}

#[derive(Clone, Copy)]
enum CompressionRegionOrder {
    Borrowed(usize),
    Materialized(usize),
}

struct PreparedCompressionRegions<'a> {
    borrowed: Vec<Vec<PrehashedChunk<'a>>>,
    materialized: Vec<MaterializedCompressionRegion>,
    order: Vec<CompressionRegionOrder>,
}

#[derive(Debug)]
enum ChunkParts {
    Single(MutationPayload),
    Fragmented(Vec<MutationPayload>),
}

impl ChunkParts {
    fn as_slice(&self) -> &[MutationPayload] {
        match self {
            Self::Single(part) => std::slice::from_ref(part),
            Self::Fragmented(parts) => parts,
        }
    }
}

#[derive(Debug)]
struct ChunkFragments {
    parts: ChunkParts,
    length: usize,
    through_sequence: u64,
}

impl ChunkFragments {
    #[cfg(test)]
    fn new(parts: Vec<MutationPayload>, length: usize) -> Self {
        Self::new_through(parts, length, 0)
    }

    #[cfg(test)]
    fn new_through(mut parts: Vec<MutationPayload>, length: usize, through_sequence: u64) -> Self {
        let parts = if parts.len() == 1 {
            ChunkParts::Single(parts.pop().expect("ASSERT: one fixture fragment"))
        } else {
            ChunkParts::Fragmented(parts)
        };
        Self::from_parts(parts, length, through_sequence)
    }

    fn from_parts(parts: ChunkParts, length: usize, through_sequence: u64) -> Self {
        assert!(length != 0, "ASSERT: a SeqCDC Chunk is nonempty");
        let actual = parts.as_slice().iter().fold(0_usize, |total, part| {
            assert!(!part.is_empty(), "ASSERT: Chunk fragments are nonempty");
            total
                .checked_add(part.len())
                .expect("ASSERT: bounded Chunk fragment sum cannot overflow")
        });
        assert_eq!(actual, length, "ASSERT: Chunk fragment length is exact");
        Self {
            parts,
            length,
            through_sequence,
        }
    }

    const fn len(&self) -> usize {
        self.length
    }

    const fn is_empty(&self) -> bool {
        self.length == 0
    }

    const fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    fn first_byte(&self) -> u8 {
        self.parts.as_slice()[0].as_bytes()[0]
    }

    fn is_fill(&self) -> bool {
        let first = self.first_byte();
        let repeated = [first; 32];
        self.parts.as_slice().iter().all(|part| {
            let bytes = part.as_bytes();
            // Keep immediate rejection cheap; fixed-size comparisons let the
            // compiler vectorize long FILL scans without alignment or Unsafe.
            let head = bytes.len().min(8);
            if !bytes[..head].iter().all(|&byte| byte == first) {
                return false;
            }
            let mut blocks = bytes[head..].chunks_exact(repeated.len());
            blocks.all(|block| block == repeated)
                && blocks.remainder().iter().all(|&byte| byte == first)
        })
    }

    fn chunk_id(&self) -> ChunkId {
        let mut hasher = blake3::Hasher::new();
        for part in self.parts.as_slice() {
            hasher.update(part.as_bytes());
        }
        ChunkId::from_bytes(*hasher.finalize().as_bytes())
    }

    #[cfg(test)]
    fn materialize_new_chunk(&self) -> Result<Cow<'_, [u8]>, DurableNamespaceError> {
        if self.parts.as_slice().len() == 1 {
            return Ok(Cow::Borrowed(self.parts.as_slice()[0].as_bytes()));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.length)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for part in self.parts.as_slice() {
            bytes.extend_from_slice(part.as_bytes());
        }
        assert_eq!(
            bytes.len(),
            self.length,
            "ASSERT: new-Chunk coalescing preserves its exact length"
        );
        record_copy(CopyClass::ChunkFragmentCoalescing, bytes.len());
        Ok(Cow::Owned(bytes))
    }

    fn append_to_compression_region(&self, decoded: &mut Vec<u8>) {
        let start = decoded.len();
        for part in self.parts.as_slice() {
            decoded.extend_from_slice(part.as_bytes());
        }
        assert_eq!(
            decoded.len() - start,
            self.length,
            "ASSERT: Compression Region materialization preserves the Chunk length"
        );
        record_copy(CopyClass::CompressionRegionMaterialization, self.length);
    }

    fn contiguous_bytes(&self) -> Option<&[u8]> {
        (self.parts.as_slice().len() == 1).then(|| self.parts.as_slice()[0].as_bytes())
    }

    #[cfg(test)]
    fn materialize_fixture(&self) -> Vec<u8> {
        self.parts
            .as_slice()
            .iter()
            .flat_map(|part| part.as_bytes().iter().copied())
            .collect()
    }
}

fn prepare_compression_regions<'a>(
    new_chunks: &[&'a PendingWriteThroughChunk],
    workers: NonZeroUsize,
    admission: &WorkerPermits,
) -> Result<PreparedCompressionRegions<'a>, DurableNamespaceError> {
    let mut plans = Vec::<CompressionRegionPlan<'_>>::new();
    for chunk in new_chunks {
        let materialized = chunk.bytes.contiguous_bytes().is_none();
        let needs_region = plans.last().is_none_or(|region| {
            region.chunks.last().is_some_and(|previous| {
                previous.offset + previous.bytes.len() as u64 != chunk.offset
            }) || region
                .decoded_length
                .checked_add(chunk.bytes.len())
                .is_none_or(|length| length > COMPRESSION_REGION_TARGET_BYTES)
        });
        if needs_region {
            plans
                .try_reserve(1)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            plans.push(CompressionRegionPlan {
                chunks: Vec::new(),
                decoded_length: 0,
                materialized,
            });
        }
        let region = plans
            .last_mut()
            .expect("ASSERT: a new Chunk owns one Compression Region");
        region
            .chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        region.materialized |= materialized;
        region.chunks.push(chunk);
        region.decoded_length = region
            .decoded_length
            .checked_add(chunk.bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
    }

    let mut prepared = PreparedCompressionRegions {
        borrowed: Vec::new(),
        materialized: Vec::new(),
        order: Vec::new(),
    };
    prepared
        .order
        .try_reserve_exact(plans.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut materializing = Vec::new();
    for plan in plans {
        if !plan.materialized {
            let mut chunks = Vec::new();
            chunks
                .try_reserve_exact(plan.chunks.len())
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            for chunk in plan.chunks {
                chunks.push(PrehashedChunk::new(
                    chunk.chunk_id,
                    chunk
                        .bytes
                        .contiguous_bytes()
                        .expect("ASSERT: a borrowed region contains contiguous Chunks"),
                ));
            }
            let ordinal = prepared.borrowed.len();
            prepared.borrowed.push(chunks);
            prepared
                .order
                .push(CompressionRegionOrder::Borrowed(ordinal));
            continue;
        }
        let ordinal = materializing.len();
        materializing.push(plan);
        prepared
            .order
            .push(CompressionRegionOrder::Materialized(ordinal));
    }
    let materializing_bytes = materializing.iter().try_fold(0_usize, |sum, plan| {
        sum.checked_add(plan.decoded_length)
            .ok_or(DurableNamespaceError::OutOfMemory)
    })?;
    // Copies need substantially more bytes per worker than fingerprint/codec
    // jobs. Small batches still own one permit; large batches keep parallelism.
    let copy_workers = NonZeroUsize::new(
        workers
            .get()
            .min((materializing_bytes / (512 * 1024)).max(1)),
    )
    .expect("ASSERT: materialization requests at least one worker");
    prepared.materialized = admission
        .map(materializing, copy_workers, materialize_compression_region)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(prepared)
}

fn materialize_compression_region(
    plan: CompressionRegionPlan<'_>,
) -> Result<MaterializedCompressionRegion, DurableNamespaceError> {
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(plan.decoded_length)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(plan.chunks.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for chunk in plan.chunks {
        let start = decoded.len();
        chunk.bytes.append_to_compression_region(&mut decoded);
        chunks.push((chunk.chunk_id, start..decoded.len()));
    }
    assert_eq!(
        decoded.len(),
        plan.decoded_length,
        "ASSERT: one Compression Region is materialized exactly once"
    );
    Ok(MaterializedCompressionRegion { decoded, chunks })
}

/// Retains prepared region owners and splits only at a selected codec boundary.
/// `NoCandidate` uses identical grouping regardless of receive fragmentation.
fn ordinary_region_slices<'a>(
    regions: &[PrehashedAdaptiveRegion<'a>],
    ordinary: &[bool],
) -> Result<Vec<PrehashedAdaptiveRegion<'a>>, DurableNamespaceError> {
    let mut result = Vec::new();
    let mut ordinal = 0;
    for region in regions {
        let (chunks, decoded) = match *region {
            PrehashedAdaptiveRegion::Borrowed(chunks) => (chunks, None),
            PrehashedAdaptiveRegion::Contiguous(region) => {
                (region.chunks(), Some(region.decoded()))
            }
        };
        let mut position = 0;
        let mut byte_offset = 0;
        while position < chunks.len() {
            let start = position;
            let start_byte = byte_offset;
            let include = ordinary[ordinal + position];
            while position < chunks.len() && ordinary[ordinal + position] == include {
                byte_offset += chunks[position].bytes().len();
                position += 1;
            }
            if include {
                let chunks = &chunks[start..position];
                result.push(match decoded {
                    Some(decoded) => PrehashedAdaptiveRegion::Contiguous(
                        PrehashedContiguousRegion::new(chunks, &decoded[start_byte..byte_offset])
                            .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?,
                    ),
                    None => PrehashedAdaptiveRegion::Borrowed(chunks),
                });
            }
        }
        ordinal += chunks.len();
    }
    assert_eq!(
        ordinal,
        ordinary.len(),
        "ASSERT: one encoding plan per prepared target"
    );
    Ok(result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StableExtraction {
    FillContainer,
    DrainStable,
}

#[derive(Debug)]
struct IngestSegment {
    payload: MutationPayload,
    mutation_sequence: u64,
}

#[derive(Debug, Default)]
struct SegmentedIngestTail {
    segments: VecDeque<IngestSegment>,
    length: usize,
    #[cfg(test)]
    materialized_bytes: usize,
}

impl SegmentedIngestTail {
    fn len(&self) -> usize {
        self.length
    }

    fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn clear(&mut self) {
        self.segments.clear();
        self.length = 0;
        #[cfg(test)]
        {
            self.materialized_bytes = 0;
        }
    }

    fn push(&mut self, payload: MutationPayload, mutation_sequence: u64) {
        assert!(
            !payload.is_empty(),
            "ASSERT: an Ingest Tail segment is nonempty"
        );
        self.length = self
            .length
            .checked_add(payload.len())
            .expect("ASSERT: bounded Ingest Tail length cannot overflow");
        self.segments.push_back(IngestSegment {
            payload,
            mutation_sequence,
        });
    }

    fn front_bytes(&self) -> &[u8] {
        self.segments
            .front()
            .map_or(&[], |segment| segment.payload.as_bytes())
    }

    #[cfg(test)]
    fn take_prefix(&mut self, length: usize) -> Result<MutationPayload, DurableNamespaceError> {
        let fragments = self.take_prefix_fragments(length)?;
        let bytes = fragments.materialize_new_chunk()?.into_owned();
        self.materialized_bytes = self
            .materialized_bytes
            .checked_add(length)
            .expect("ASSERT: bounded materialized-byte counter cannot overflow");
        self.assert_valid();
        Ok(MutationPayload::from_owned_bytes(bytes))
    }

    fn take_prefix_fragments(
        &mut self,
        length: usize,
    ) -> Result<ChunkFragments, DurableNamespaceError> {
        assert!(length != 0, "ASSERT: consumed Ingest prefix is nonempty");
        if length > self.length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if self.front_bytes().len() >= length {
            let (part, sequence) = self.consume_front(length);
            return Ok(ChunkFragments::from_parts(
                ChunkParts::Single(part),
                length,
                sequence,
            ));
        }

        // Preflight only the consumed prefix. All fallible reservations precede
        // mutation, including the pathological fragment-coalescing fallback.
        let mut covered = 0_usize;
        let mut count = 0_usize;
        for segment in &self.segments {
            covered = covered
                .checked_add(segment.payload.len())
                .expect("ASSERT: bounded Ingest prefix length cannot overflow");
            count += 1;
            if covered >= length {
                break;
            }
        }
        assert!(
            covered >= length,
            "ASSERT: accounted Tail covers its consumed prefix"
        );
        let mut parts = Vec::new();
        let mut compact = Vec::new();
        let coalesce = count > MAX_CHUNK_FRAGMENTS_V1;
        if coalesce {
            compact
                .try_reserve_exact(length)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        } else {
            parts
                .try_reserve_exact(count)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        }
        let mut through_sequence = 0;
        let mut remaining = length;
        while remaining != 0 {
            let consumed = remaining.min(self.front_bytes().len());
            let (part, sequence) = self.consume_front(consumed);
            through_sequence = through_sequence.max(sequence);
            if coalesce {
                compact.extend_from_slice(part.as_bytes());
            } else {
                parts.push(part);
            }
            remaining -= consumed;
        }
        let parts = if coalesce {
            assert_eq!(
                compact.len(),
                length,
                "ASSERT: compaction preserves the complete Chunk"
            );
            record_copy(CopyClass::ChunkFragmentCoalescing, compact.len());
            ChunkParts::Single(MutationPayload::from_owned_bytes(compact))
        } else {
            ChunkParts::Fragmented(parts)
        };
        Ok(ChunkFragments::from_parts(parts, length, through_sequence))
    }

    fn consume_front(&mut self, length: usize) -> (MutationPayload, u64) {
        let front = self
            .segments
            .front_mut()
            .expect("ASSERT: accounted Ingest Tail owns a front segment");
        assert!(
            length != 0 && length <= front.payload.len(),
            "ASSERT: consumed prefix lies in front segment"
        );
        let sequence = front.mutation_sequence;
        let payload = if length == front.payload.len() {
            self.segments
                .pop_front()
                .expect("ASSERT: front was established")
                .payload
        } else {
            front
                .payload
                .checked_split_to(length)
                .expect("ASSERT: consumed fragment lies inside front segment")
        };
        self.length = self
            .length
            .checked_sub(length)
            .expect("ASSERT: Tail byte accounting covers the consumed prefix");
        assert_eq!(
            self.segments.is_empty(),
            self.is_empty(),
            "ASSERT: Tail emptiness matches its byte count"
        );
        (payload, sequence)
    }

    #[cfg(test)]
    fn materialized_bytes(&self) -> usize {
        self.materialized_bytes
    }

    // Independently audit the remaining Tail at a stable-batch boundary, rather
    // than rescanning the entire retained suffix after each individual Chunk.
    fn assert_valid(&self) {
        let actual = self.segments.iter().fold(0_usize, |total, segment| {
            assert!(
                !segment.payload.is_empty(),
                "ASSERT: Ingest Tail cannot retain empty segments"
            );
            total
                .checked_add(segment.payload.len())
                .expect("ASSERT: bounded Ingest Tail segment sum cannot overflow")
        });
        assert_eq!(
            actual, self.length,
            "ASSERT: cached Ingest Tail length must match its segments"
        );
        assert_eq!(
            self.segments.is_empty(),
            self.is_empty(),
            "ASSERT: Ingest Tail emptiness must match its byte count"
        );
    }
}

fn seqcdc_force_scalar() -> bool {
    static FORCE_SCALAR: OnceLock<bool> = OnceLock::new();
    *FORCE_SCALAR.get_or_init(|| {
        std::env::var("FASTDUP_SEQCDC_FORCE_SCALAR").is_ok_and(|value| value == "1")
    })
}

fn segmented_seqcdc_cut(tail: &SegmentedIngestTail) -> usize {
    assert!(
        tail.len() > CDC_MAXIMUM_BYTES,
        "ASSERT: stable SeqCDC scan owns more than one maximum Chunk"
    );
    let segments = || {
        tail.segments
            .iter()
            .map(|segment| segment.payload.as_bytes())
    };
    if seqcdc_force_scalar() {
        seqcdc_cut_segmented_scalar(segments(), tail.len(), SEQCDC_CONFIG_V1)
    } else {
        seqcdc_cut_segmented(segments(), tail.len(), SEQCDC_CONFIG_V1)
    }
}

fn take_next_stable_seqcdc_chunk(
    tail: &mut SegmentedIngestTail,
) -> Result<Option<ChunkFragments>, DurableNamespaceError> {
    if tail.len() <= CDC_MAXIMUM_BYTES {
        return Ok(None);
    }
    let stable_before = tail.len() - CDC_MAXIMUM_BYTES;
    let length = if tail.front_bytes().len() >= CDC_MAXIMUM_BYTES {
        if seqcdc_force_scalar() {
            seqcdc_cut_scalar(tail.front_bytes(), SEQCDC_CONFIG_V1)
        } else {
            seqcdc_cut(tail.front_bytes(), SEQCDC_CONFIG_V1)
        }
    } else {
        segmented_seqcdc_cut(tail)
    };
    if length > stable_before {
        return Ok(None);
    }
    tail.take_prefix_fragments(length).map(Some)
}

#[derive(Debug)]
struct StableChunk {
    offset: u64,
    bytes: ChunkFragments,
}

fn take_stable_chunk_batch(
    state: &mut WriteThroughStream,
    maximum_bytes: usize,
) -> Result<Vec<StableChunk>, DurableNamespaceError> {
    assert!(maximum_bytes != 0, "ASSERT: stable batch budget is nonzero");
    let mut batch = Vec::new();
    let mut batch_bytes = 0_usize;
    while let Some(bytes) = take_next_stable_seqcdc_chunk(&mut state.tail)? {
        let offset = state.tail_offset;
        state.tail_offset = state
            .tail_offset
            .checked_add(
                u64::try_from(bytes.len()).expect("ASSERT: bounded SeqCDC Chunk length fits u64"),
            )
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        batch_bytes = batch_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        batch
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        batch.push(StableChunk { offset, bytes });
        if batch_bytes >= maximum_bytes {
            break;
        }
    }
    state.tail.assert_valid();
    Ok(batch)
}

/// A serial FILL pass carries its result into hashing without repeating FILL classification.
fn classify_stable_chunk_batch(
    batch: &[StableChunk],
    budget: NonZeroUsize,
    permits: &WorkerPermits,
    telemetry: &CpuPhaseTelemetry,
) -> Result<(Vec<Option<ChunkId>>, usize), DurableNamespaceError> {
    assert!(
        !batch.is_empty(),
        "ASSERT: classification batch is nonempty"
    );
    let mut lease = permits.acquire(NonZeroUsize::MIN);
    telemetry.record_permit(&lease);
    let mut phase = telemetry.begin();
    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(batch.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut classified = Vec::new();
    classified
        .try_reserve_exact(batch.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    classified.resize(batch.len(), None);
    let mut hash_bytes = 0_usize;
    for (ordinal, chunk) in batch.iter().enumerate() {
        assert!(!chunk.bytes.is_empty(), "ASSERT: SeqCDC Chunk is nonempty");
        if !chunk.bytes.is_fill() {
            ordinals.push(ordinal);
            hash_bytes = hash_bytes
                .checked_add(chunk.bytes.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
        }
    }
    let desired = stable_hash_workers(hash_bytes, ordinals.len(), budget);
    if desired.get() > 1 {
        // Never wait for another admission while holding the serial-pass permit.
        drop(phase);
        drop(lease);
        lease = permits.acquire(desired);
        telemetry.record_permit(&lease);
        phase = telemetry.begin();
    }
    let workers = lease.workers().get();
    if workers == 1 {
        for ordinal in ordinals {
            classified[ordinal] = Some(batch[ordinal].bytes.chunk_id());
        }
    } else {
        let next = AtomicUsize::new(0);
        let results = (0..workers)
            .into_par_iter()
            .map(|worker| {
                struct Retire<'a, 'b>(&'a WorkerPermitLease<'b>, usize);
                impl Drop for Retire<'_, '_> {
                    fn drop(&mut self) {
                        if self.1 != 0 {
                            self.0.retire_worker();
                        }
                    }
                }
                let _worker = Retire(&lease, worker);
                let mut completed = Vec::new();
                loop {
                    let start = next.fetch_add(4, Ordering::Relaxed);
                    if start >= ordinals.len() {
                        break;
                    }
                    for &ordinal in &ordinals[start..(start + 4).min(ordinals.len())] {
                        completed.push((ordinal, batch[ordinal].bytes.chunk_id()));
                    }
                }
                completed
            })
            .collect::<Vec<_>>();
        let mut completed_count = 0;
        for (ordinal, id) in results.into_iter().flatten() {
            classified[ordinal] = Some(id);
            completed_count += 1;
        }
        assert_eq!(
            completed_count,
            ordinals.len(),
            "ASSERT: every non-FILL Chunk has one hash owner"
        );
    }
    drop(phase);
    drop(lease);
    Ok((classified, workers))
}

fn stable_hash_workers(bytes: usize, chunks: usize, budget: NonZeroUsize) -> NonZeroUsize {
    // Measured crossover: 1 MiB -> 2 workers, 4 MiB -> 4; large batches may
    // use the entire pool. Balance coordination growing with worker count
    // against hash time decreasing with it, rather than imposing a CPU cap.
    let by_bytes = bytes.div_ceil(256 * 1024).isqrt().max(1);
    NonZeroUsize::new(budget.get().min(chunks.div_ceil(4).max(1)).min(by_bytes))
        .expect("ASSERT: hash classification always has one worker")
}

/// Append-only staging until the whole owner is detached or discarded.
/// Each append proves its new range and preserves the cached byte sum.
#[derive(Debug, Default)]
struct PendingWriteThrough {
    chunks: Vec<PendingWriteThroughChunk>,
    bytes: usize,
}

impl PendingWriteThrough {
    fn validate_append(previous_end: Option<u64>, chunk: &PendingWriteThroughChunk) -> u64 {
        assert!(
            !chunk.bytes.is_empty() && chunk.bytes.len() <= CDC_MAXIMUM_BYTES,
            "ASSERT: pending write-through Chunk violates SeqCDC length bounds"
        );
        assert!(
            previous_end.is_none_or(|end| end <= chunk.offset),
            "ASSERT: pending write-through Chunks must be ordered and disjoint"
        );
        chunk
            .offset
            .checked_add(u64::try_from(chunk.bytes.len()).expect("bounded Chunk fits u64"))
            .expect("ASSERT: pending write-through Chunk range cannot overflow")
    }

    fn push(&mut self, chunk: PendingWriteThroughChunk) -> Result<(), DurableNamespaceError> {
        let previous_end = self.chunks.last().map(|previous| {
            previous.offset + u64::try_from(previous.bytes.len()).expect("bounded Chunk fits u64")
        });
        Self::validate_append(previous_end, &chunk);
        let bytes = self
            .bytes
            .checked_add(chunk.bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        assert!(
            bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: pending write-through payload exceeds its Container bound"
        );
        self.chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        self.chunks.push(chunk);
        self.bytes = bytes;
        Ok(())
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.bytes = 0;
    }

    fn assert_bounded(&self) {
        assert_eq!(
            self.chunks.is_empty(),
            self.bytes == 0,
            "ASSERT: pending write-through count and bytes must agree"
        );
        assert!(
            self.bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: pending write-through payload exceeds its Container bound"
        );
    }
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct WriteThroughStream {
    inode: Option<InodeId>,
    placement: Option<ContainerPlacement>,
    last_mutation_sequence: Option<u64>,
    next_offset: u64,
    tail_offset: u64,
    tail: SegmentedIngestTail,
    pending: PendingWriteThrough,
}

#[derive(Debug, Default)]
struct WriteThroughRegistry {
    lanes: BTreeMap<InodeId, WriteThroughLane>,
    overflow: Arc<Mutex<WriteThroughStream>>,
    sealed: VecDeque<Instant>,
    degraded: bool,
    next_touch: u64,
}

struct WriteThroughStatusSnapshot {
    lanes: Vec<Arc<Mutex<WriteThroughStream>>>,
    overflow: Arc<Mutex<WriteThroughStream>>,
    sealed_uncommitted_containers: usize,
    oldest_sealed_age: Option<Duration>,
    degraded: bool,
}

#[derive(Debug)]
struct WriteThroughLane {
    stream: Arc<Mutex<WriteThroughStream>>,
    last_touch: u64,
}

impl WriteThroughRegistry {
    fn status_snapshot(&self) -> WriteThroughStatusSnapshot {
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(self.lanes.len())
            .expect("ASSERT: bounded status Lane snapshot allocation succeeds");
        lanes.extend(self.lanes.values().map(|lane| Arc::clone(&lane.stream)));
        WriteThroughStatusSnapshot {
            lanes,
            overflow: Arc::clone(&self.overflow),
            sealed_uncommitted_containers: self.sealed.len(),
            oldest_sealed_age: self.sealed.front().map(Instant::elapsed),
            degraded: self.degraded,
        }
    }

    fn acquire_lane(&mut self, inode: InodeId) -> Arc<Mutex<WriteThroughStream>> {
        let touch = self.next_touch;
        self.next_touch = self
            .next_touch
            .checked_add(1)
            .expect("ASSERT: Ingest Lane touch sequence cannot overflow");
        if let Some(lane) = self.lanes.get_mut(&inode) {
            lane.last_touch = touch;
            return Arc::clone(&lane.stream);
        }
        if self
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned")
            .inode
            == Some(inode)
        {
            return Arc::clone(&self.overflow);
        }
        if self.lanes.len() >= MAX_ACTIVE_INGEST_LANES_V1 {
            let eviction_grace = u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2)
                .expect("ASSERT: bounded Ingest Lane grace fits u64");
            let evicted = self
                .lanes
                .iter()
                .filter(|(_, lane)| Arc::strong_count(&lane.stream) == 1)
                .filter(|(_, lane)| touch.saturating_sub(lane.last_touch) > eviction_grace)
                .min_by_key(|(candidate_inode, lane)| (lane.last_touch, **candidate_inode))
                .map(|(candidate_inode, _)| *candidate_inode);
            if let Some(evicted) = evicted {
                let removed = self.lanes.remove(&evicted);
                assert!(removed.is_some(), "ASSERT: selected Ingest Lane vanished");
            } else {
                return Arc::clone(&self.overflow);
            }
        }
        let lane = Arc::new(Mutex::new(WriteThroughStream::default()));
        assert!(
            self.lanes
                .insert(
                    inode,
                    WriteThroughLane {
                        stream: Arc::clone(&lane),
                        last_touch: touch,
                    },
                )
                .is_none(),
            "ASSERT: a new Ingest Lane cannot replace an existing inode lane"
        );
        assert!(
            self.lanes.len() <= MAX_ACTIVE_INGEST_LANES_V1,
            "ASSERT: registered Ingest Lanes exceed the process memory budget"
        );
        lane
    }
}

struct WriteThroughIngest<C> {
    containers: ContainerRepository<C>,
    container_generations: ContainerGenerationAllocator<C>,
    index: Arc<dyn ManifestReaderPolicy<C>>,
    worker_budget: NonZeroUsize,
    worker_permits: Arc<WorkerPermits>,
    active_writers: AtomicUsize,
    hash_batches: AtomicUsize,
    maximum_hash_workers: AtomicUsize,
    hash_cpu: CpuPhaseTelemetry,
    encode_cpu: CpuPhaseTelemetry,
    planning_cpu: CpuPhaseTelemetry,
    materialization_wall_ns: AtomicU64,
    registry: Mutex<WriteThroughRegistry>,
    queue: Arc<IngestQueue>,
    publication_queue: Arc<PublicationQueue>,
    namespace: OnceLock<Weak<Namespace>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
}

#[derive(Debug)]
enum IngestJobKind {
    WriteFragment(IngestWriteFragment),
    WriteBatch { fragments: Vec<IngestWriteFragment> },
    Truncate,
}

#[derive(Debug)]
struct IngestWriteFragment {
    offset: u64,
    bytes: MutationPayload,
    mutation_sequence: u64,
    placement: ContainerPlacement,
}

#[derive(Debug)]
struct IngestJob {
    inode: InodeId,
    mutation_sequence: u64,
    kind: IngestJobKind,
}

impl IngestJob {
    fn buffered_bytes(&self) -> usize {
        match &self.kind {
            IngestJobKind::WriteFragment(fragment) => fragment.bytes.len(),
            IngestJobKind::WriteBatch { fragments } => {
                fragments.iter().fold(0_usize, |total, fragment| {
                    total
                        .checked_add(fragment.bytes.len())
                        .expect("ASSERT: bounded Ingest Batch bytes cannot overflow")
                })
            }
            IngestJobKind::Truncate => 0,
        }
    }
}

#[derive(Debug)]
struct OpenIngestBatch {
    opened_at: Instant,
    fragments: Vec<IngestWriteFragment>,
    buffered_bytes: usize,
    last_mutation_sequence: u64,
}

impl OpenIngestBatch {
    fn into_job(self, inode: InodeId) -> IngestJob {
        assert!(
            !self.fragments.is_empty() && self.buffered_bytes != 0,
            "ASSERT: only a nonempty Ingest Batch may be sealed"
        );
        IngestJob {
            inode,
            mutation_sequence: self.last_mutation_sequence,
            kind: IngestJobKind::WriteBatch {
                fragments: self.fragments,
            },
        }
    }
}

#[derive(Debug, Default)]
struct InodeJobQueue {
    pending: VecDeque<IngestJob>,
    open: Option<OpenIngestBatch>,
    in_flight: bool,
    last_enqueued_sequence: u64,
    completed_sequence: u64,
}

#[derive(Debug, Default)]
struct IngestQueueState {
    inodes: BTreeMap<InodeId, InodeJobQueue>,
    writable_handles: BTreeMap<InodeId, usize>,
    ready: VecDeque<InodeId>,
    buffered_bytes: usize,
    ingest_batches: u64,
    ingest_fragments: u64,
    maximum_ingest_batch_bytes: usize,
    minimum_ingest_batch_target_bytes: usize,
    maximum_ingest_batch_target_bytes: usize,
    maximum_ingest_ring_slots: usize,
    ingest_ring_wait_ns: u64,
    shutdown: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct IngestQueueStatus {
    buffered_bytes: usize,
    ingest_batches: u64,
    ingest_fragments: u64,
    maximum_ingest_batch_bytes: usize,
    minimum_ingest_batch_target_bytes: usize,
    maximum_ingest_batch_target_bytes: usize,
    maximum_ingest_ring_slots: usize,
    ingest_ring_wait_ns: u64,
}

#[derive(Debug)]
struct IngestQueue {
    #[cfg(test)]
    before_fragment_wait: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    state: Mutex<IngestQueueState>,
    work_available: Condvar,
    space_available: Condvar,
    completed: Condvar,
}

#[derive(Debug)]
struct DetachedContainerWork {
    inode: InodeId,
    first_chunk_sequence: u64,
    completion: Option<std::sync::mpsc::SyncSender<Result<(), DurableNamespaceError>>>,
    through_sequence: u64,
    publication_ordinal: u64,
    chunks: Vec<PendingWriteThroughChunk>,
    payload_bytes: usize,
}

impl DetachedContainerWork {
    fn new(
        inode: InodeId,
        through_sequence: u64,
        chunks: Vec<PendingWriteThroughChunk>,
        payload_bytes: usize,
    ) -> Self {
        let (actual, first_chunk_sequence) = validate_pending_chunks(&chunks);
        assert_eq!(
            actual, payload_bytes,
            "ASSERT: pending write-through byte accounting must be exact"
        );
        assert!(
            !chunks.is_empty() && payload_bytes != 0,
            "ASSERT: detached Container work must contain payload"
        );
        assert!(
            payload_bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: detached Container exceeds its pre-format payload bound"
        );
        Self {
            inode,
            through_sequence,
            publication_ordinal: 0,
            first_chunk_sequence: first_chunk_sequence
                .expect("ASSERT: detached work contains Chunks"),
            completion: None,
            chunks,
            payload_bytes,
        }
    }
}

#[derive(Debug, Default)]
struct InodePublicationQueue {
    pending: VecDeque<DetachedContainerWork>,
    in_flight: BTreeMap<u64, u64>,
    next_publication_ordinal: u64,
    next_retirement_ordinal: u64,
    last_enqueued_sequence: u64,
    ready: bool,
}

#[derive(Debug, Default)]
struct PublicationQueueState {
    inodes: BTreeMap<InodeId, InodePublicationQueue>,
    ready: VecDeque<InodeId>,
    buffered_bytes: usize,
    shutdown: bool,
}

#[derive(Debug)]
struct PublicationQueue {
    state: Mutex<PublicationQueueState>,
    work_available: Condvar,
    space_available: Condvar,
    completed: Condvar,
}

impl PublicationQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(PublicationQueueState::default()),
            work_available: Condvar::new(),
            space_available: Condvar::new(),
            completed: Condvar::new(),
        }
    }

    fn enqueue(&self, mut work: DetachedContainerWork) {
        let inode = work.inode;
        let work_bytes = work.payload_bytes;
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        while state
            .buffered_bytes
            .checked_add(work_bytes)
            .is_none_or(|total| total > DETACHED_CONTAINER_BUDGET_BYTES_V1)
        {
            state = self.space_available.wait(state).expect(
                "ASSERT: detached publication queue lock poisoned while applying backpressure",
            );
        }
        assert!(
            !state.shutdown,
            "ASSERT: cannot enqueue detached Container work after scheduler shutdown"
        );
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(work_bytes)
            .expect("ASSERT: detached publication bytes cannot overflow");
        let inode_queue = state.inodes.entry(inode).or_default();
        assert!(
            work.through_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: detached per-inode publication sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = work.through_sequence;
        work.publication_ordinal = inode_queue.next_publication_ordinal;
        inode_queue.next_publication_ordinal = inode_queue
            .next_publication_ordinal
            .checked_add(1)
            .expect("ASSERT: detached publication ordinal cannot overflow");
        inode_queue.pending.push_back(work);
        if schedule_publication_inodes(&mut state) {
            self.work_available.notify_one();
        }
    }

    fn next_work(&self) -> Option<DetachedContainerWork> {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        loop {
            if let Some(inode) = state.ready.pop_front() {
                let per_inode_limit = publication_window(&state);
                let inode_queue = state
                    .inodes
                    .get_mut(&inode)
                    .expect("ASSERT: ready publication inode must own a queue");
                inode_queue.ready = false;
                if inode_queue.pending.is_empty() || inode_queue.in_flight.len() >= per_inode_limit
                {
                    continue;
                }
                let work = inode_queue
                    .pending
                    .pop_front()
                    .expect("ASSERT: ready publication inode must own pending work");
                assert!(
                    inode_queue
                        .in_flight
                        .insert(work.publication_ordinal, work.first_chunk_sequence)
                        .is_none(),
                    "ASSERT: detached publication ordinal is unique per inode"
                );
                if schedule_publication_inodes(&mut state) {
                    self.work_available.notify_one();
                }
                return Some(work);
            }
            if state.shutdown {
                return None;
            }
            state = self
                .work_available
                .wait(state)
                .expect("ASSERT: detached publication queue lock poisoned while waiting for work");
        }
    }

    fn wait_for_retirement_turn(&self, work: &DetachedContainerWork) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        loop {
            let inode_queue = state
                .inodes
                .get(&work.inode)
                .expect("ASSERT: retiring publication inode retains queue state");
            if inode_queue.next_retirement_ordinal == work.publication_ordinal {
                return;
            }
            state = self.completed.wait(state).expect(
                "ASSERT: detached publication queue lock poisoned while ordering completion",
            );
        }
    }

    fn finish(&self, work: &DetachedContainerWork) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        state.buffered_bytes = state
            .buffered_bytes
            .checked_sub(work.payload_bytes)
            .expect("ASSERT: completed detached bytes must have been admitted");
        let inode_queue = state
            .inodes
            .get_mut(&work.inode)
            .expect("ASSERT: completed publication inode must retain queue state");
        assert_eq!(
            inode_queue.in_flight.remove(&work.publication_ordinal),
            Some(work.first_chunk_sequence),
            "ASSERT: completed publication must match the active inode sequence"
        );
        assert_eq!(
            inode_queue.next_retirement_ordinal, work.publication_ordinal,
            "ASSERT: detached publications retire in per-inode order"
        );
        inode_queue.next_retirement_ordinal = inode_queue
            .next_retirement_ordinal
            .checked_add(1)
            .expect("ASSERT: detached retirement ordinal cannot overflow");
        if schedule_publication_inodes(&mut state) {
            self.work_available.notify_one();
        }
        self.space_available.notify_all();
        self.completed.notify_all();
    }

    fn wait_through(&self, inode: InodeId, through_sequence: u64) {
        let target = {
            let state = self
                .state
                .lock()
                .expect("ASSERT: publication queue lock poisoned");
            state
                .inodes
                .get(&inode)
                .and_then(|queue| {
                    // A later-ending batch may contain complete pre-cut Chunks.
                    // Snapshot the required ordinal once: later arrivals cannot
                    // extend a Sync/Release/checkpoint fence indefinitely.
                    queue
                        .in_flight
                        .iter()
                        .filter_map(|(&ordinal, &first)| {
                            (first <= through_sequence).then_some(ordinal)
                        })
                        .chain(queue.pending.iter().filter_map(|work| {
                            (work.first_chunk_sequence <= through_sequence)
                                .then_some(work.publication_ordinal)
                        }))
                        .max()
                })
                .map_or(0, |ordinal| ordinal + 1)
        };
        self.wait_for_retirement(inode, target);
    }

    fn retirement_target(&self, inode: InodeId) -> u64 {
        self.state
            .lock()
            .expect("ASSERT: publication queue lock poisoned")
            .inodes
            .get(&inode)
            .map_or(0, |queue| queue.next_publication_ordinal)
    }

    fn wait_for_retirement(&self, inode: InodeId, target: u64) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: publication queue lock poisoned");
        while state
            .inodes
            .get(&inode)
            .is_some_and(|queue| queue.next_retirement_ordinal < target)
        {
            state = self
                .completed
                .wait(state)
                .expect("ASSERT: publication queue lock poisoned while waiting for retirement");
        }
    }

    fn buffered_bytes(&self) -> usize {
        self.state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned")
            .buffered_bytes
    }

    fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned during shutdown");
        state.shutdown = true;
        self.work_available.notify_all();
        self.space_available.notify_all();
        self.completed.notify_all();
    }
}

fn publication_window(state: &PublicationQueueState) -> usize {
    let active_inodes = state
        .inodes
        .values()
        .filter(|queue| !queue.pending.is_empty() || !queue.in_flight.is_empty())
        .count();
    if active_inodes <= 1 {
        SINGLE_STREAM_PUBLICATION_WINDOW_V1
    } else {
        1
    }
}

fn schedule_publication_inodes(state: &mut PublicationQueueState) -> bool {
    let per_inode_limit = publication_window(state);
    let mut scheduled = false;
    let inodes = state.inodes.keys().copied().collect::<Vec<_>>();
    for inode in inodes {
        let queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: enumerated publication inode remains present");
        if !queue.ready && !queue.pending.is_empty() && queue.in_flight.len() < per_inode_limit {
            queue.ready = true;
            state.ready.push_back(inode);
            scheduled = true;
        }
    }
    scheduled
}

impl IngestQueue {
    fn opened_write_handle(&self, inode: InodeId) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        let handles = state.writable_handles.entry(inode).or_default();
        *handles = handles
            .checked_add(1)
            .expect("ASSERT: writable handle count cannot overflow");
        if state.writable_handles.len() >= 2 {
            let open_inodes = state
                .inodes
                .iter()
                .filter_map(|(open_inode, queue)| queue.open.is_some().then_some(*open_inode))
                .collect::<Vec<_>>();
            for open_inode in open_inodes {
                seal_open_ingest_batch(&mut state, open_inode);
            }
            self.work_available.notify_all();
        }
    }

    fn released_write_handle(&self, inode: InodeId) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        let handles = state
            .writable_handles
            .get_mut(&inode)
            .expect("ASSERT: released writable handle must be tracked");
        *handles = handles
            .checked_sub(1)
            .expect("ASSERT: writable handle count cannot underflow");
        if *handles == 0 {
            state.writable_handles.remove(&inode);
        }
    }

    fn new() -> Self {
        Self {
            #[cfg(test)]
            before_fragment_wait: Mutex::new(None),
            state: Mutex::new(IngestQueueState::default()),
            work_available: Condvar::new(),
            space_available: Condvar::new(),
            completed: Condvar::new(),
        }
    }

    fn enqueue_write_fragment(&self, inode: InodeId, fragment: IngestWriteFragment) {
        let fragment_bytes = fragment.bytes.len();
        assert!(
            fragment_bytes <= WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1,
            "ASSERT: one queued ingest fragment exceeds its byte bound"
        );
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.inodes.entry(inode).or_default();
        if active_ingest_inodes_with_candidate(&state, inode) >= 2 {
            self.enqueue_unbatched_fragment(state, inode, fragment);
            return;
        }
        state = self.wait_for_fragment_admission(state, inode, &fragment);
        assert!(
            !state.shutdown,
            "ASSERT: cannot enqueue after scheduler shutdown"
        );
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(fragment_bytes)
            .expect("ASSERT: bounded ingest queue bytes cannot overflow");
        let batch_target =
            ingest_batch_target_bytes(active_ingest_inodes_with_candidate(&state, inode));
        let inode_queue = state.inodes.entry(inode).or_default();
        inode_queue.last_enqueued_sequence = fragment.mutation_sequence;
        let open = inode_queue.open.get_or_insert_with(|| OpenIngestBatch {
            opened_at: Instant::now(),
            fragments: Vec::with_capacity(4),
            buffered_bytes: 0,
            last_mutation_sequence: fragment.mutation_sequence,
        });
        open.buffered_bytes = open
            .buffered_bytes
            .checked_add(fragment_bytes)
            .expect("ASSERT: bounded open Ingest Batch bytes cannot overflow");
        open.last_mutation_sequence = fragment.mutation_sequence;
        open.fragments.push(fragment);
        if open.buffered_bytes >= batch_target {
            seal_open_ingest_batch(&mut state, inode);
        }
        self.work_available.notify_one();
    }

    fn enqueue_unbatched_fragment(
        &self,
        mut state: std::sync::MutexGuard<'_, IngestQueueState>,
        inode: InodeId,
        fragment: IngestWriteFragment,
    ) {
        // A mode change may leave a partial single-stream batch for this
        // inode. Queue it before admitting any newer unbatched fragment,
        // including when admission must release the lock for backpressure.
        if seal_open_ingest_batch(&mut state, inode) {
            self.work_available.notify_all();
        }
        let wait_started = Instant::now();
        let mut waited = false;
        while state
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .is_none_or(|total| total > MULTI_STREAM_QUEUE_BUDGET_BYTES_V1)
        {
            waited = true;
            state = self
                .space_available
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while applying backpressure");
        }
        if waited {
            state.ingest_ring_wait_ns = state.ingest_ring_wait_ns.saturating_add(
                u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
        }
        record_ingest_batch_target(&mut state, WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1);
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .expect("ASSERT: bounded ingest queue bytes cannot overflow");
        let fragment_bytes = fragment.bytes.len();
        let mutation_sequence = fragment.mutation_sequence;
        let inode_queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: admitted inode retains queue state");
        assert!(
            mutation_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: per-inode ingest admission sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = mutation_sequence;
        let schedule = !inode_queue.in_flight && inode_queue.pending.is_empty();
        inode_queue.pending.push_back(IngestJob {
            inode,
            mutation_sequence,
            kind: IngestJobKind::WriteFragment(fragment),
        });
        let queue_slots = ingest_ring_slots(inode_queue);
        state.ingest_batches = state.ingest_batches.saturating_add(1);
        state.ingest_fragments = state.ingest_fragments.saturating_add(1);
        state.maximum_ingest_batch_bytes = state.maximum_ingest_batch_bytes.max(fragment_bytes);
        state.maximum_ingest_ring_slots = state.maximum_ingest_ring_slots.max(queue_slots);
        if schedule {
            state.ready.push_back(inode);
        }
        self.work_available.notify_one();
    }

    fn wait_for_fragment_admission<'a>(
        &self,
        mut state: std::sync::MutexGuard<'a, IngestQueueState>,
        inode: InodeId,
        fragment: &IngestWriteFragment,
    ) -> std::sync::MutexGuard<'a, IngestQueueState> {
        let wait_started = Instant::now();
        let mut waited = false;
        loop {
            let active_inodes = active_ingest_inodes_with_candidate(&state, inode);
            let batch_target = ingest_batch_target_bytes(active_inodes);
            record_ingest_batch_target(&mut state, batch_target);
            if seal_open_batches_at_or_above(&mut state, batch_target) {
                self.work_available.notify_all();
            }
            let inode_queue = state
                .inodes
                .get(&inode)
                .expect("ASSERT: admitted inode retains queue state");
            assert!(
                fragment.mutation_sequence >= inode_queue.last_enqueued_sequence,
                "ASSERT: per-inode ingest admission sequence cannot move backwards"
            );
            if !ingest_fragment_extends_batch(inode_queue, fragment, batch_target) {
                seal_open_ingest_batch(&mut state, inode);
                self.work_available.notify_one();
                continue;
            }
            let ring_slots = SINGLE_STREAM_INGEST_RING_SLOTS_V1;
            let has_slot =
                inode_queue.open.is_some() || ingest_ring_slots(inode_queue) < ring_slots;
            let has_bytes = state
                .buffered_bytes
                .checked_add(fragment.bytes.len())
                .is_some_and(|total| total <= WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1);
            if has_slot && has_bytes {
                if waited {
                    state.ingest_ring_wait_ns = state.ingest_ring_wait_ns.saturating_add(
                        u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    );
                }
                return state;
            }
            waited = true;
            #[cfg(test)]
            if let Some(sender) = self.before_fragment_wait.lock().unwrap().take() {
                let _ = sender.send(());
            }
            state = self
                .space_available
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while applying backpressure");
        }
    }

    fn enqueue(&self, job: IngestJob) {
        let inode = job.inode;
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.inodes.entry(inode).or_default();
        seal_open_ingest_batch(&mut state, inode);
        let inode_queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: admitted inode retains queue state");
        assert!(
            job.mutation_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: per-inode ingest admission sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = job.mutation_sequence;
        let schedule = !inode_queue.in_flight && inode_queue.pending.is_empty();
        inode_queue.pending.push_back(job);
        if schedule {
            state.ready.push_back(inode);
        }
        self.work_available.notify_one();
    }

    fn next_job(&self) -> Option<IngestJob> {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        loop {
            seal_expired_ingest_batches(&mut state, Instant::now());
            if let Some(inode) = state.ready.pop_front() {
                let inode_queue = state
                    .inodes
                    .get_mut(&inode)
                    .expect("ASSERT: ready inode must own a queue");
                assert!(
                    !inode_queue.in_flight,
                    "ASSERT: one inode cannot have two active ingest jobs"
                );
                let job = inode_queue
                    .pending
                    .pop_front()
                    .expect("ASSERT: ready inode must own pending work");
                inode_queue.in_flight = true;
                return Some(job);
            }
            if state.shutdown {
                return None;
            }
            if let Some(wait) = next_ingest_batch_expiry(&state, Instant::now()) {
                let (next, _) = self
                    .work_available
                    .wait_timeout(state, wait)
                    .expect("ASSERT: ingest queue lock poisoned while waiting for batch age");
                state = next;
            } else {
                state = self
                    .work_available
                    .wait(state)
                    .expect("ASSERT: ingest queue lock poisoned while waiting for work");
            }
        }
    }

    fn finish(&self, job: &IngestJob) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.buffered_bytes = state
            .buffered_bytes
            .checked_sub(job.buffered_bytes())
            .expect("ASSERT: completed ingest bytes must have been admitted");
        let inode_queue = state
            .inodes
            .get_mut(&job.inode)
            .expect("ASSERT: completed inode must retain queue state");
        assert!(
            inode_queue.in_flight,
            "ASSERT: completed ingest job must have been active"
        );
        inode_queue.in_flight = false;
        assert!(
            job.mutation_sequence >= inode_queue.completed_sequence,
            "ASSERT: completed inode sequence cannot move backwards"
        );
        inode_queue.completed_sequence = job.mutation_sequence;
        if !inode_queue.pending.is_empty() {
            state.ready.push_back(job.inode);
            self.work_available.notify_one();
        }
        self.space_available.notify_all();
        self.completed.notify_all();
    }

    fn wait_through(&self, inode: InodeId, mutation_sequence: u64) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        if seal_open_ingest_batch(&mut state, inode) {
            self.work_available.notify_one();
        }
        // Inode versions include Metadata-only mutations that intentionally
        // have no ingest job. Flush waits through the newest DATA job at or
        // before the requested version instead of waiting for every sequence
        // number to appear in this queue.
        let Some(target_sequence) = state
            .inodes
            .get(&inode)
            .map(|queue| queue.last_enqueued_sequence.min(mutation_sequence))
        else {
            return;
        };
        loop {
            let complete = state
                .inodes
                .get(&inode)
                .is_none_or(|queue| queue.completed_sequence >= target_sequence);
            if complete {
                return;
            }
            state = self
                .completed
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while waiting for sequence fence");
        }
    }

    fn status(&self) -> IngestQueueStatus {
        let state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        IngestQueueStatus {
            buffered_bytes: state.buffered_bytes,
            ingest_batches: state.ingest_batches,
            ingest_fragments: state.ingest_fragments,
            maximum_ingest_batch_bytes: state.maximum_ingest_batch_bytes,
            minimum_ingest_batch_target_bytes: state.minimum_ingest_batch_target_bytes,
            maximum_ingest_batch_target_bytes: state.maximum_ingest_batch_target_bytes,
            maximum_ingest_ring_slots: state.maximum_ingest_ring_slots,
            ingest_ring_wait_ns: state.ingest_ring_wait_ns,
        }
    }

    fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned during shutdown");
        let open_inodes = state
            .inodes
            .iter()
            .filter_map(|(inode, queue)| queue.open.as_ref().map(|_| *inode))
            .collect::<Vec<_>>();
        for inode in open_inodes {
            seal_open_ingest_batch(&mut state, inode);
        }
        state.shutdown = true;
        self.work_available.notify_all();
        self.space_available.notify_all();
        self.completed.notify_all();
    }
}

fn ingest_ring_slots(queue: &InodeJobQueue) -> usize {
    queue.pending.len() + usize::from(queue.in_flight) + usize::from(queue.open.is_some())
}

fn active_ingest_inodes_with_candidate(state: &IngestQueueState, candidate: InodeId) -> usize {
    if !state.writable_handles.is_empty() {
        return state.writable_handles.len()
            + usize::from(!state.writable_handles.contains_key(&candidate));
    }
    let active = state
        .inodes
        .iter()
        .filter(|(_, queue)| ingest_ring_slots(queue) != 0)
        .count();
    let candidate_active = state
        .inodes
        .get(&candidate)
        .is_some_and(|queue| ingest_ring_slots(queue) != 0);
    active + usize::from(!candidate_active)
}

fn ingest_batch_target_bytes(active_inodes: usize) -> usize {
    match active_inodes {
        0..=1 => SINGLE_STREAM_INGEST_BATCH_BYTES_V1,
        _ => WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1,
    }
}

fn record_ingest_batch_target(state: &mut IngestQueueState, target: usize) {
    if state.minimum_ingest_batch_target_bytes == 0 {
        state.minimum_ingest_batch_target_bytes = target;
    } else {
        state.minimum_ingest_batch_target_bytes =
            state.minimum_ingest_batch_target_bytes.min(target);
    }
    state.maximum_ingest_batch_target_bytes = state.maximum_ingest_batch_target_bytes.max(target);
}

fn ingest_fragment_extends_batch(
    queue: &InodeJobQueue,
    fragment: &IngestWriteFragment,
    target: usize,
) -> bool {
    queue.open.as_ref().is_none_or(|open| {
        let contiguous = open.fragments.last().is_none_or(|last| {
            last.offset
                .checked_add(
                    u64::try_from(last.bytes.len()).expect("ASSERT: fragment length fits u64"),
                )
                .is_some_and(|next| next == fragment.offset)
                && fragment.mutation_sequence >= last.mutation_sequence
                && fragment.placement == last.placement
        });
        let fits = open
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .is_some_and(|bytes| bytes <= target);
        contiguous && fits
    })
}

fn seal_open_batches_at_or_above(state: &mut IngestQueueState, target: usize) -> bool {
    let ready = state
        .inodes
        .iter()
        .filter_map(|(inode, queue)| {
            queue
                .open
                .as_ref()
                .is_some_and(|open| open.buffered_bytes >= target)
                .then_some(*inode)
        })
        .collect::<Vec<_>>();
    let mut sealed = false;
    for inode in ready {
        sealed |= seal_open_ingest_batch(state, inode);
    }
    sealed
}

fn seal_open_ingest_batch(state: &mut IngestQueueState, inode: InodeId) -> bool {
    let Some(queue) = state.inodes.get_mut(&inode) else {
        return false;
    };
    let Some(open) = queue.open.take() else {
        return false;
    };
    let fragment_count = open.fragments.len();
    let batch_bytes = open.buffered_bytes;
    let schedule = !queue.in_flight && queue.pending.is_empty();
    queue.pending.push_back(open.into_job(inode));
    let ring_slots = ingest_ring_slots(queue);
    state.ingest_batches = state.ingest_batches.saturating_add(1);
    state.ingest_fragments = state
        .ingest_fragments
        .saturating_add(u64::try_from(fragment_count).unwrap_or(u64::MAX));
    state.maximum_ingest_batch_bytes = state.maximum_ingest_batch_bytes.max(batch_bytes);
    state.maximum_ingest_ring_slots = state.maximum_ingest_ring_slots.max(ring_slots);
    if schedule {
        state.ready.push_back(inode);
    }
    true
}

fn seal_expired_ingest_batches(state: &mut IngestQueueState, now: Instant) -> bool {
    let expired = state
        .inodes
        .iter()
        .filter_map(|(inode, queue)| {
            queue.open.as_ref().and_then(|open| {
                (now.saturating_duration_since(open.opened_at) >= INGEST_BATCH_MAXIMUM_AGE_V1)
                    .then_some(*inode)
            })
        })
        .collect::<Vec<_>>();
    let mut sealed = false;
    for inode in expired {
        sealed |= seal_open_ingest_batch(state, inode);
    }
    sealed
}

fn next_ingest_batch_expiry(state: &IngestQueueState, now: Instant) -> Option<Duration> {
    state
        .inodes
        .values()
        .filter_map(|queue| queue.open.as_ref())
        .map(|open| {
            INGEST_BATCH_MAXIMUM_AGE_V1
                .saturating_sub(now.saturating_duration_since(open.opened_at))
        })
        .min()
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct CpuPhaseTelemetry {
    phases: AtomicU64,
    active: AtomicU64,
    maximum_active: AtomicU64,
    runnable_wall_ns: AtomicU64,
    permit_blocked_phases: AtomicU64,
    permit_wait_ns: AtomicU64,
    maximum_permit_wait_ns: AtomicU64,
    requested_workers: AtomicU64,
    granted_workers: AtomicU64,
    partial_grants: AtomicU64,
}

impl CpuPhaseTelemetry {
    fn begin(&self) -> CpuPhaseGuard<'_> {
        atomic_saturating_add(&self.phases, 1);
        let active = self
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: active CPU-phase telemetry cannot overflow")
            .checked_add(1)
            .expect("ASSERT: active CPU-phase telemetry cannot overflow");
        self.maximum_active.fetch_max(active, Ordering::Relaxed);
        CpuPhaseGuard {
            telemetry: self,
            started: Instant::now(),
        }
    }

    fn record_permit(&self, lease: &WorkerPermitLease<'_>) {
        let requested = u64::try_from(lease.requested_workers().get())
            .expect("ASSERT: worker count fits telemetry");
        let granted =
            u64::try_from(lease.workers().get()).expect("ASSERT: worker count fits telemetry");
        atomic_saturating_add(&self.requested_workers, requested);
        atomic_saturating_add(&self.granted_workers, granted);
        atomic_saturating_add(&self.permit_wait_ns, lease.wait_ns());
        self.maximum_permit_wait_ns
            .fetch_max(lease.wait_ns(), Ordering::Relaxed);
        if lease.blocked() {
            atomic_saturating_add(&self.permit_blocked_phases, 1);
        }
        if granted < requested {
            atomic_saturating_add(&self.partial_grants, 1);
        }
    }

    fn status(&self) -> CpuPhaseStatus {
        CpuPhaseStatus {
            phases: self.phases.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            maximum_active: self.maximum_active.load(Ordering::Relaxed),
            runnable_wall_ns: self.runnable_wall_ns.load(Ordering::Relaxed),
            permit_blocked_phases: self.permit_blocked_phases.load(Ordering::Relaxed),
            permit_wait_ns: self.permit_wait_ns.load(Ordering::Relaxed),
            maximum_permit_wait_ns: self.maximum_permit_wait_ns.load(Ordering::Relaxed),
            requested_workers: self.requested_workers.load(Ordering::Relaxed),
            granted_workers: self.granted_workers.load(Ordering::Relaxed),
            partial_grants: self.partial_grants.load(Ordering::Relaxed),
        }
    }
}

struct CpuPhaseGuard<'a> {
    telemetry: &'a CpuPhaseTelemetry,
    started: Instant,
}

impl Drop for CpuPhaseGuard<'_> {
    fn drop(&mut self) {
        atomic_saturating_add(
            &self.telemetry.runnable_wall_ns,
            duration_ns_saturating(self.started.elapsed()),
        );
        let previous = self.telemetry.active.fetch_sub(1, Ordering::Relaxed);
        assert!(
            previous != 0,
            "ASSERT: active CPU-phase telemetry underflow"
        );
    }
}

fn atomic_saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn duration_ns_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

struct ActiveWriteThrough<'a> {
    active_writers: &'a AtomicUsize,
}

impl<'a> ActiveWriteThrough<'a> {
    fn enter(active_writers: &'a AtomicUsize) -> Self {
        active_writers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: active write-through job count cannot overflow");
        Self { active_writers }
    }
}

impl Drop for ActiveWriteThrough<'_> {
    fn drop(&mut self) {
        let previous = self.active_writers.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous != 0,
            "ASSERT: write-through job retirement requires one active job"
        );
    }
}

fn validate_pending_chunks(chunks: &[PendingWriteThroughChunk]) -> (usize, Option<u64>) {
    let mut summed_bytes = 0_usize;
    let mut previous_end = None;
    let mut first_sequence: Option<u64> = None;
    for pending in chunks {
        previous_end = Some(PendingWriteThrough::validate_append(previous_end, pending));
        summed_bytes = summed_bytes
            .checked_add(pending.bytes.len())
            .expect("ASSERT: bounded pending write-through bytes cannot overflow");
        let sequence = pending.bytes.through_sequence();
        first_sequence = Some(first_sequence.map_or(sequence, |previous| previous.min(sequence)));
    }
    (summed_bytes, first_sequence)
}

fn assert_pending_write_through_state(state: &WriteThroughStream) {
    state.pending.assert_bounded();
}

fn assert_bounded_write_through_lane(state: &WriteThroughStream) {
    assert_pending_write_through_state(state);
    let buffered = state
        .tail
        .len()
        .checked_add(state.pending.bytes)
        .expect("ASSERT: bounded Ingest Lane bytes cannot overflow");
    assert!(
        buffered <= CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
        "ASSERT: one Ingest Lane exceeded one Container plus CDC suffix: tail={} pending={} buffered={} bound={}",
        state.tail.len(),
        state.pending.bytes,
        buffered,
        CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
    );
}

impl<C> fmt::Debug for WriteThroughIngest<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .status_snapshot();
        let buffered_bytes = snapshot.lanes.iter().fold(0_usize, |total, lane| {
            let lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending.bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = snapshot
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let ingest = self.queue.status();
        let buffered_bytes = buffered_bytes
            .checked_add(overflow.tail.len())
            .and_then(|sum| sum.checked_add(overflow.pending.bytes))
            .and_then(|sum| sum.checked_add(ingest.buffered_bytes))
            .and_then(|sum| sum.checked_add(self.publication_queue.buffered_bytes()))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        assert!(
            buffered_bytes <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        formatter
            .debug_struct("WriteThroughIngest")
            .field("active_lanes", &snapshot.lanes.len())
            .field("buffered_bytes", &buffered_bytes)
            .field(
                "sealed_uncommitted",
                &snapshot.sealed_uncommitted_containers,
            )
            .field(
                "active_writers",
                &self.active_writers.load(Ordering::Relaxed),
            )
            .field("degraded", &snapshot.degraded)
            .finish_non_exhaustive()
    }
}

impl<C> WriteThroughIngest<C>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    fn start_workers(self: &Arc<Self>, namespace: &Arc<Namespace>) {
        self.namespace
            .set(Arc::downgrade(namespace))
            .expect("ASSERT: write-through Namespace is attached exactly once");
        let worker_count = self
            .worker_budget
            .get()
            .min(MAX_ACTIVE_INGEST_LANES_V1.saturating_add(1));
        let mut workers = self
            .workers
            .lock()
            .expect("ASSERT: ingest worker handle lock poisoned");
        assert!(
            workers.is_empty(),
            "ASSERT: ingest workers start exactly once"
        );
        let publication_worker_count = worker_count.min(2);
        workers
            .try_reserve_exact(worker_count.saturating_add(publication_worker_count))
            .expect("ASSERT: bounded ingest worker handle allocation succeeds");
        for ordinal in 0..worker_count {
            let owner = Arc::downgrade(self);
            let queue = Arc::clone(&self.queue);
            let worker = std::thread::Builder::new()
                .name(format!("fastdup-ingest-{ordinal}"))
                .spawn(move || {
                    while let Some(job) = queue.next_job() {
                        if let Some(owner) = owner.upgrade() {
                            owner.process_job(&job);
                        }
                        queue.finish(&job);
                    }
                })
                .expect("ASSERT: bounded permanent ingest worker creation succeeds");
            workers.push(worker);
        }
        for ordinal in 0..publication_worker_count {
            let owner = Arc::downgrade(self);
            let queue = Arc::clone(&self.publication_queue);
            let worker = std::thread::Builder::new()
                .name(format!("fastdup-publish-{ordinal}"))
                .spawn(move || {
                    while let Some(work) = queue.next_work() {
                        let result = if let Some(owner) = owner.upgrade() {
                            let result = owner.publish_detached_container(&work);
                            queue.wait_for_retirement_turn(&work);
                            owner.retire_detached_container(&work, result)
                        } else {
                            queue.wait_for_retirement_turn(&work);
                            Err(DurableNamespaceError::FrozenViewMismatch)
                        };
                        queue.finish(&work);
                        if let Some(completion) = &work.completion {
                            // One bounded reply; a canceled checkpoint may have
                            // dropped its receiver, but retirement still finishes.
                            let _ = completion.send(result);
                        }
                    }
                })
                .expect("ASSERT: bounded permanent publication worker creation succeeds");
            workers.push(worker);
        }
    }

    fn enqueue_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        placement: ContainerPlacement,
        bytes: &MutationPayload,
    ) {
        let mut consumed = 0_usize;
        while consumed < bytes.len() {
            let chunk_end = consumed
                .saturating_add(WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1)
                .min(bytes.len());
            let job_offset = offset
                .checked_add(u64::try_from(consumed).expect("ASSERT: queued offset fits u64"))
                .expect("ASSERT: accepted write range was already checked");
            let chunk = bytes
                .checked_slice(consumed, chunk_end)
                .expect("ASSERT: queued write slice lies inside accepted payload");
            consumed = chunk_end;
            self.queue.enqueue_write_fragment(
                inode,
                IngestWriteFragment {
                    offset: job_offset,
                    bytes: chunk,
                    mutation_sequence,
                    placement,
                },
            );
        }
    }

    fn process_job(&self, job: &IngestJob) {
        let result = match &job.kind {
            IngestJobKind::WriteFragment(fragment) => {
                self.stage_write_batch(job.inode, std::slice::from_ref(fragment))
            }
            IngestJobKind::WriteBatch { fragments } => self.stage_write_batch(job.inode, fragments),
            IngestJobKind::Truncate => {
                self.reset_lane(job.inode);
                Ok(Vec::new())
            }
        };
        match result {
            Ok(externalized) => {
                if !externalized.is_empty()
                    && let Some(namespace) = self.namespace.get().and_then(Weak::upgrade)
                {
                    namespace.externalize_verified_extents(externalized);
                }
            }
            Err(error) => self.degrade_job(job, &error),
        }
    }

    fn publish_detached_container(
        &self,
        work: &DetachedContainerWork,
    ) -> Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError> {
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        self.publish_chunks(&work.chunks, work.inode, work.through_sequence)
    }

    fn retire_detached_container(
        &self,
        work: &DetachedContainerWork,
        result: Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError>,
    ) -> Result<(), DurableNamespaceError> {
        match result {
            Ok((externalized, sealed)) => {
                if let Some(namespace) = self.namespace.get().and_then(Weak::upgrade) {
                    namespace.externalize_verified_extents(externalized);
                }
                if sealed {
                    let mut registry = self
                        .registry
                        .lock()
                        .expect("ASSERT: write-through registry lock poisoned");
                    registry.sealed.push_back(Instant::now());
                }
                Ok(())
            }
            Err(error) => {
                // Partial drains return the error to their checkpoint, which
                // retains the Frozen view for retry. Only unobserved background
                // failures use ordinary lane degradation.
                if work.completion.is_none() {
                    self.degrade_inode(work.inode, work.through_sequence, &error);
                }
                Err(error)
            }
        }
    }

    fn degrade_job(&self, job: &IngestJob, error: &DurableNamespaceError) {
        let (offset, length) = match &job.kind {
            IngestJobKind::WriteFragment(fragment) => (fragment.offset, fragment.bytes.len()),
            IngestJobKind::WriteBatch { fragments } => (
                fragments.first().map_or(0, |fragment| fragment.offset),
                job.buffered_bytes(),
            ),
            IngestJobKind::Truncate => (0, 0),
        };
        eprintln!(
            "write-through staging degraded; resident fallback retained: inode={} offset={offset} length={length} sequence={} error={error:?}",
            job.inode.get(),
            job.mutation_sequence,
        );
        self.mark_degraded();
        self.reset_lane(job.inode);
    }

    fn degrade_inode(&self, inode: InodeId, mutation_sequence: u64, error: &DurableNamespaceError) {
        eprintln!(
            "detached Container publication degraded; resident fallback retained: inode={} sequence={mutation_sequence} error={error:?}",
            inode.get(),
        );
        // The failed work was already detached. Later lane contents remain
        // valid and resident; the publisher must not invalidate them or wait
        // on a lane whose producer may need this publication to retire.
        self.mark_degraded();
    }

    fn mark_degraded(&self) {
        self.registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .degraded = true;
    }

    fn reset_lane(&self, inode: InodeId) {
        let lane = {
            let registry = self
                .registry
                .lock()
                .expect("ASSERT: write-through registry lock poisoned");
            registry.lanes.get(&inode).map_or_else(
                || Arc::clone(&registry.overflow),
                |lane| Arc::clone(&lane.stream),
            )
        };
        // A checkpoint may have captured this Arc before the truncate barrier.
        // Reset in place: replacing it would let that checkpoint drain stale
        // work after a new lane has published later mutation sequences.
        // Drop the Registry before waiting for a lane/publication backpressure.
        let mut lane = lane
            .lock()
            .expect("ASSERT: write-through lane lock poisoned");
        if lane.inode == Some(inode) {
            *lane = WriteThroughStream::default();
        }
    }

    fn status(&self) -> WriteThroughStatus {
        // Never wait for a Lane while holding the Registry. A Lane may apply
        // publication-queue backpressure while the completing publisher needs
        // the Registry to retire its work and release that queue space.
        let snapshot = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .status_snapshot();
        let buffered = snapshot.lanes.iter().fold(0_usize, |total, lane| {
            let lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending.bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = snapshot
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let buffered = buffered
            .checked_add(overflow.tail.len())
            .and_then(|sum| sum.checked_add(overflow.pending.bytes))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        let ingest = self.queue.status();
        let queued_bytes = ingest
            .buffered_bytes
            .checked_add(self.publication_queue.buffered_bytes())
            .expect("ASSERT: bounded scheduler queue bytes cannot overflow");
        let buffered = buffered
            .checked_add(queued_bytes)
            .expect("ASSERT: bounded write-through plus queue bytes cannot overflow");
        assert!(
            buffered <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        WriteThroughStatus {
            buffered_bytes: u64::try_from(buffered).expect("ASSERT: process buffers fit in u64"),
            queued_bytes: u64::try_from(queued_bytes)
                .expect("ASSERT: bounded queued bytes fit u64"),
            active_lanes: u64::try_from(snapshot.lanes.len())
                .expect("ASSERT: bounded Ingest Lane count fits u64"),
            sealed_uncommitted_containers: u64::try_from(snapshot.sealed_uncommitted_containers)
                .expect("ASSERT: process Container count fits in u64"),
            oldest_sealed_age: snapshot.oldest_sealed_age,
            hash_batches: u64::try_from(self.hash_batches.load(Ordering::Relaxed))
                .expect("ASSERT: process hash-batch count fits u64"),
            maximum_hash_workers: u64::try_from(self.maximum_hash_workers.load(Ordering::Relaxed))
                .expect("ASSERT: process hash-worker count fits u64"),
            ingest_batches: ingest.ingest_batches,
            ingest_fragments: ingest.ingest_fragments,
            maximum_ingest_batch_bytes: u64::try_from(ingest.maximum_ingest_batch_bytes)
                .expect("ASSERT: bounded Ingest Batch bytes fit u64"),
            minimum_ingest_batch_target_bytes: u64::try_from(
                ingest.minimum_ingest_batch_target_bytes,
            )
            .expect("ASSERT: bounded Ingest Batch target fits u64"),
            maximum_ingest_batch_target_bytes: u64::try_from(
                ingest.maximum_ingest_batch_target_bytes,
            )
            .expect("ASSERT: bounded Ingest Batch target fits u64"),
            maximum_ingest_ring_slots: u64::try_from(ingest.maximum_ingest_ring_slots)
                .expect("ASSERT: bounded Ingest Ring slots fit u64"),
            ingest_ring_wait_ns: ingest.ingest_ring_wait_ns,
            hash_cpu: self.hash_cpu.status(),
            encode_cpu: self.encode_cpu.status(),
            planning_cpu: self.planning_cpu.status(),
            materialization_wall_ns: self.materialization_wall_ns.load(Ordering::Relaxed),
            advanced_reduction: self.index.advanced_reduction_status(),
            degraded: snapshot.degraded,
        }
    }

    fn capture_cut(&self) -> usize {
        self.registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .sealed
            .len()
    }

    fn wait_for_commit_cut(&self, commit: &NamespaceCommit) {
        for inode in commit.inodes() {
            self.queue
                .wait_through(inode.inode(), inode.mutation_sequence());
            self.publication_queue
                .wait_through(inode.inode(), inode.mutation_sequence());
        }
    }

    fn complete_cut(&self, sealed_at_cut: usize) {
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        assert!(
            sealed_at_cut <= registry.sealed.len(),
            "ASSERT: completed cut cannot retire future Containers"
        );
        registry.sealed.drain(..sealed_at_cut);
        // Preserve the incomplete SeqCDC suffix across generations. It is the
        // exact content anchor required for a later append to make the same
        // cuts as the checkpoint planner. Chunks the checkpoint published from
        // this suffix are filtered through the newly active Exact Index before
        // the write-through path publishes its next Container.
    }

    fn stage_write_batch(
        &self,
        inode: InodeId,
        fragments: &[IngestWriteFragment],
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        assert!(
            !fragments.is_empty(),
            "ASSERT: an Ingest Batch contains at least one fragment"
        );
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        let lane = self.lane_for(inode);
        let mut lane = lane
            .lock()
            .expect("ASSERT: write-through lane lock poisoned");
        let mut externalized = Vec::new();
        for fragment in fragments {
            let discontinuous = lane.inode != Some(inode)
                || lane.placement != Some(fragment.placement)
                || lane.next_offset != fragment.offset
                || lane
                    .last_mutation_sequence
                    .is_some_and(|previous| fragment.mutation_sequence <= previous);
            if discontinuous {
                externalized.extend(self.drain_before_lane_reset(&mut lane)?);
                lane.inode = Some(inode);
                lane.placement = Some(fragment.placement);
                lane.tail_offset = fragment.offset;
                lane.tail.clear();
                lane.pending.clear();
            }
            assert_bounded_write_through_lane(&lane);
            lane.last_mutation_sequence = Some(fragment.mutation_sequence);
            lane.next_offset = fragment
                .offset
                .checked_add(u64::try_from(fragment.bytes.len()).expect("ASSERT: usize fits u64"))
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            lane.tail
                .push(fragment.bytes.clone(), fragment.mutation_sequence);
            loop {
                externalized.extend(self.extract_stable_chunks(
                    &mut lane,
                    inode,
                    fragment.mutation_sequence,
                    StableExtraction::FillContainer,
                )?);
                if lane.pending.bytes < CONTAINER_PAYLOAD_FLUSH_BYTES {
                    break;
                }
                assert!(
                    lane.pending.bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
                    "ASSERT: write-through payload exceeded its pre-format Container bound"
                );
                let pending = std::mem::take(&mut lane.pending);
                let work = DetachedContainerWork::new(
                    inode,
                    fragment.mutation_sequence,
                    pending.chunks,
                    pending.bytes,
                );
                assert_pending_write_through_state(&lane);
                self.publication_queue.enqueue(work);
            }
            assert_bounded_write_through_lane(&lane);
        }
        lane.tail.assert_valid();
        drop(lane);
        Ok(externalized)
    }

    fn drain_before_lane_reset(
        &self,
        lane: &mut WriteThroughStream,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let (Some(inode), Some(through_sequence)) = (lane.inode, lane.last_mutation_sequence)
        else {
            return Ok(Vec::new());
        };
        // A discontinuity ends CDC continuity, not the validity of the entire
        // buffered prefix. Preserve complete Chunks under their original
        // sequences; Namespace still rejects any subsequently overwritten range.
        // The ordinary bounded publication queue owns pending payload before
        // the lane starts the next range. No storage wait is added to admission.
        let mut externalized = Vec::new();
        loop {
            let previous_tail = lane.tail.len();
            externalized.extend(self.extract_stable_chunks(
                lane,
                inode,
                through_sequence,
                StableExtraction::DrainStable,
            )?);
            let had_pending = !lane.pending.chunks.is_empty();
            if had_pending {
                let pending = std::mem::take(&mut lane.pending);
                self.publication_queue.enqueue(DetachedContainerWork::new(
                    inode,
                    through_sequence,
                    pending.chunks,
                    pending.bytes,
                ));
            }
            if lane.tail.len() == previous_tail || !had_pending {
                break;
            }
        }
        assert!(
            lane.pending.chunks.is_empty() && lane.pending.bytes == 0,
            "ASSERT: a lane reset preserves every complete staged Chunk"
        );
        assert!(
            lane.tail.len() <= CDC_MAXIMUM_BYTES * 2,
            "ASSERT: a lane reset discards only the bounded incomplete CDC suffix"
        );
        Ok(externalized)
    }

    fn flush_stable_for_commit_cut(
        &self,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        let lanes = {
            let registry = self
                .registry
                .lock()
                .expect("ASSERT: write-through registry lock poisoned");
            let mut lanes = Vec::new();
            lanes
                .try_reserve_exact(registry.lanes.len().saturating_add(1))
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            lanes.extend(registry.lanes.values().map(|lane| Arc::clone(&lane.stream)));
            lanes.push(Arc::clone(&registry.overflow));
            lanes
        };
        let mut externalized = Vec::new();
        for lane in lanes {
            let mut lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            let (Some(inode), Some(through_sequence)) = (lane.inode, lane.last_mutation_sequence)
            else {
                assert_pending_write_through_state(&lane);
                continue;
            };
            let mut completions = Vec::new();
            loop {
                let previous_tail = lane.tail.len();
                let extracted = self.extract_stable_chunks(
                    &mut lane,
                    inode,
                    through_sequence,
                    StableExtraction::DrainStable,
                )?;
                externalized
                    .try_reserve(extracted.len())
                    .map_err(|_| DurableNamespaceError::OutOfMemory)?;
                externalized.extend(extracted);
                let had_pending = !lane.pending.chunks.is_empty();
                if had_pending {
                    completions
                        .try_reserve(1)
                        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
                    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                    let pending = std::mem::take(&mut lane.pending);
                    let mut work = DetachedContainerWork::new(
                        inode,
                        through_sequence,
                        pending.chunks,
                        pending.bytes,
                    );
                    work.completion = Some(sender);
                    // The same 64-MiB queue charges full and partial Containers.
                    // Only bounded queue backpressure may retain this lane lock.
                    self.publication_queue.enqueue(work);
                    completions.push(receiver);
                }
                if lane.tail.len() == previous_tail || !had_pending {
                    break;
                }
            }
            assert!(
                lane.pending.chunks.is_empty() && lane.pending.bytes == 0,
                "ASSERT: commit-cut drain detaches every complete staged Chunk"
            );
            assert!(
                lane.tail.len() <= CDC_MAXIMUM_BYTES * 2,
                "ASSERT: commit-cut drain retains only a boundary Chunk and CDC suffix"
            );
            assert_bounded_write_through_lane(&lane);
            // Pin the finite publication prefix while this lane is stable.
            let target = self.publication_queue.retirement_target(inode);
            drop(lane);
            self.publication_queue.wait_for_retirement(inode, target);
            for completion in completions {
                completion
                    .recv()
                    .map_err(|_| DurableNamespaceError::FrozenViewMismatch)??;
            }
        }
        Ok(externalized)
    }

    fn lane_for(&self, inode: InodeId) -> Arc<Mutex<WriteThroughStream>> {
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        registry.acquire_lane(inode)
    }

    #[allow(clippy::too_many_lines)]
    fn extract_stable_chunks(
        &self,
        state: &mut WriteThroughStream,
        inode: InodeId,
        through_sequence: u64,
        extraction: StableExtraction,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        assert_pending_write_through_state(state);
        let stable_before = state.tail.len().saturating_sub(CDC_MAXIMUM_BYTES);
        let stable_required = CONTAINER_PAYLOAD_FLUSH_BYTES
            .checked_sub(state.pending.bytes)
            .expect("ASSERT: pending bytes remain below the flush threshold");
        if extraction == StableExtraction::FillContainer && stable_before < stable_required {
            return Ok(Vec::new());
        }
        let mut externalized = Vec::new();
        while state.pending.bytes < CONTAINER_PAYLOAD_FLUSH_BYTES {
            let maximum_batch_bytes = CONTAINER_PAYLOAD_FLUSH_BYTES
                .checked_sub(state.pending.bytes)
                .expect("ASSERT: pending bytes remain below the flush threshold");
            let batch = take_stable_chunk_batch(state, maximum_batch_bytes)?;
            if batch.is_empty() {
                break;
            }
            let (chunk_ids, workers) = classify_stable_chunk_batch(
                &batch,
                self.worker_budget,
                &self.worker_permits,
                &self.hash_cpu,
            )?;
            self.hash_batches
                .fetch_add(1, Ordering::Relaxed)
                .checked_add(1)
                .expect("ASSERT: hash-batch telemetry cannot overflow");
            self.maximum_hash_workers
                .fetch_max(workers, Ordering::Relaxed);
            assert_eq!(
                batch.len(),
                chunk_ids.len(),
                "ASSERT: every stable Chunk has one classification"
            );
            for (chunk, chunk_id) in batch.into_iter().zip(chunk_ids) {
                let chunk_through_sequence = chunk.bytes.through_sequence();
                assert!(
                    chunk_through_sequence <= through_sequence,
                    "ASSERT: one stable Chunk cannot depend on a future mutation"
                );
                let logical_length = u64::try_from(chunk.bytes.len())
                    .expect("ASSERT: bounded SeqCDC Chunk length fits u64");
                let Some(chunk_id) = chunk_id else {
                    externalized.push(ExternalizedExtent::new(
                        inode,
                        chunk.offset,
                        chunk_through_sequence,
                        Arc::new(FillCommittedFile {
                            value: chunk.bytes.first_byte(),
                            length: logical_length,
                        }),
                    )?);
                    continue;
                };
                if let Some(entry) = self
                    .online_dependency_proofs
                    .verified_entry_for_active(chunk_id, logical_length)
                {
                    externalized.push(self.externalized_proven_location(
                        inode,
                        chunk.offset,
                        chunk_through_sequence,
                        entry,
                    )?);
                    continue;
                }
                if let Some(entry) =
                    self.index
                        .verified_location(&self.containers, chunk_id, logical_length)
                {
                    externalized.push(self.externalized_location(
                        inode,
                        chunk.offset,
                        chunk_through_sequence,
                        entry,
                        OnlineProofAdmission::ExactReuse,
                    )?);
                    continue;
                }
                state.pending.push(PendingWriteThroughChunk {
                    offset: chunk.offset,
                    chunk_id,
                    bytes: chunk.bytes,
                    placement: state
                        .placement
                        .expect("ASSERT: active Write-Through stream has a placement"),
                })?;
            }
        }
        assert_pending_write_through_state(state);
        Ok(externalized)
    }

    fn publish_chunks(
        &self,
        chunks: &[PendingWriteThroughChunk],
        inode: InodeId,
        through_sequence: u64,
    ) -> Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError> {
        assert!(
            !chunks.is_empty(),
            "ASSERT: Container publication requires at least one pending Chunk"
        );
        let mut locations = Vec::<ExactIndexEntry>::new();
        let mut candidates = Vec::<(ChunkId, u32, &PendingWriteThroughChunk)>::new();
        candidates
            .try_reserve_exact(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut new_chunks = Vec::new();
        new_chunks
            .try_reserve(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for chunk in chunks {
            let chunk_id = chunk.chunk_id;
            let logical_length = u32::try_from(chunk.bytes.len())
                .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
            candidates.push((chunk_id, logical_length, chunk));
        }
        candidates.sort_unstable_by_key(|(chunk_id, _, _)| *chunk_id);
        let mut unique_candidates = Vec::new();
        unique_candidates
            .try_reserve_exact(candidates.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (chunk_id, logical_length, chunk) in candidates {
            if let Some((previous_id, previous_length, _)) = unique_candidates.last().copied()
                && previous_id == chunk_id
            {
                if previous_length != logical_length {
                    return Err(DurableNamespaceError::ChunkLengthConflict {
                        chunk_id,
                        first_length: u64::from(previous_length),
                        second_length: u64::from(logical_length),
                    });
                }
                continue;
            }
            unique_candidates.push((chunk_id, logical_length, chunk));
        }
        let mut claims =
            PublicationClaims::new(&self.online_dependency_proofs, unique_candidates.len())?;
        for (chunk_id, logical_length, chunk) in unique_candidates {
            match claims.claim(chunk_id, logical_length) {
                PublicationClaim::Existing(entry) => {
                    locations.push(entry);
                }
                PublicationClaim::Acquired => new_chunks.push(chunk),
            }
        }
        // Claims are acquired in key order; physical output follows file order.
        new_chunks.sort_unstable_by_key(|chunk| chunk.offset);
        let advanced = self.index.advanced_reduction_available()
            && self
                .namespace
                .get()
                .and_then(Weak::upgrade)
                .is_some_and(|namespace| namespace.advanced_reduction_enabled(inode));
        let publication_guard = advanced
            .then(|| self.containers.try_pin_reduction_publication())
            .flatten();
        let advanced = publication_guard.is_some();
        let (mut entries, similarity_entries) = if new_chunks.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            self.publish_new_chunks(&new_chunks, advanced)?
        };
        locations.extend(entries.iter().copied());
        locations.sort_unstable_by_key(ExactIndexEntry::chunk_id);
        assert!(
            locations
                .windows(2)
                .all(|pair| pair[0].chunk_id() < pair[1].chunk_id()),
            "ASSERT: one unique candidate Chunk has exactly one publication result"
        );
        claims.finish(&mut entries);
        let sealed = !entries.is_empty();
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.bytes.through_sequence() <= through_sequence),
            "ASSERT: detached work sequence covers every published Chunk"
        );
        let externalized = self.externalize_chunks(chunks, inode, &locations)?;
        self.index
            .publish_reduction_batch(entries, similarity_entries, publication_guard);
        Ok((externalized, sealed))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "single owner for materialized input, admitted plans and ordered publication"
    )]
    fn publish_new_chunks(
        &self,
        new_chunks: &[&PendingWriteThroughChunk],
        advanced: bool,
    ) -> Result<
        (
            Vec<ExactIndexEntry>,
            Vec<fastdup_format::SimilarityIndexEntry>,
        ),
        DurableNamespaceError,
    > {
        assert!(
            !new_chunks.is_empty(),
            "ASSERT: a new-Chunk Container must contain at least one Chunk"
        );
        assert!(
            new_chunks
                .iter()
                .all(|chunk| chunk.placement == new_chunks[0].placement),
            "ASSERT: one Container cannot cross physical placement tiers"
        );
        let desired_workers = self.worker_budget;
        let materialization_started = Instant::now();
        let prepared_regions =
            prepare_compression_regions(new_chunks, desired_workers, &self.worker_permits);
        self.materialization_wall_ns.fetch_add(
            u64::try_from(materialization_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let prepared_regions = prepared_regions?;
        let materialized_chunks = prepared_regions
            .materialized
            .iter()
            .map(|region| {
                region
                    .chunks
                    .iter()
                    .map(|(chunk_id, range)| {
                        PrehashedChunk::new(*chunk_id, &region.decoded[range.clone()])
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let contiguous_regions = materialized_chunks
            .iter()
            .zip(&prepared_regions.materialized)
            .map(|(chunks, region)| {
                PrehashedContiguousRegion::new(chunks, &region.decoded)
                    .expect("ASSERT: constructed Chunk views exactly partition their region")
            })
            .collect::<Vec<_>>();
        let regions = prepared_regions
            .order
            .iter()
            .map(|region| match *region {
                CompressionRegionOrder::Borrowed(ordinal) => {
                    PrehashedAdaptiveRegion::Borrowed(&prepared_regions.borrowed[ordinal])
                }
                CompressionRegionOrder::Materialized(ordinal) => {
                    PrehashedAdaptiveRegion::Contiguous(contiguous_regions[ordinal])
                }
            })
            .collect::<Vec<_>>();
        let generation = self.container_generations.reserve_generation()?;
        let mut independent = Vec::new();
        let mut dependents = Vec::new();
        let mut similarity_entries = Vec::new();
        let ordinary_regions = if advanced {
            let targets = regions
                .iter()
                .flat_map(|region| match region {
                    PrehashedAdaptiveRegion::Borrowed(chunks) => chunks.iter().copied(),
                    PrehashedAdaptiveRegion::Contiguous(region) => region.chunks().iter().copied(),
                })
                .collect::<Vec<_>>();
            let mut ordinary = Vec::with_capacity(targets.len());
            let phase = self.planning_cpu.begin();
            let plans = self.index.plan_similarity_batch(
                &self.containers,
                &targets,
                desired_workers,
                &self.worker_permits,
            );
            drop(phase);
            for (plan, hint) in plans {
                if let Some(hint) = hint {
                    similarity_entries.push(hint);
                }
                ordinary.push(matches!(plan, PersistentChunkPlan::NoCandidates));
                match plan {
                    PersistentChunkPlan::NoCandidates => {}
                    PersistentChunkPlan::Independent(record) => independent.push(record),
                    PersistentChunkPlan::Dependent(record) => dependents.push(record),
                }
            }
            Cow::Owned(ordinary_region_slices(&regions, &ordinary)?)
        } else {
            Cow::Borrowed(regions.as_slice())
        };
        let chunk_order = regions
            .iter()
            .flat_map(|region| match region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => chunks.iter(),
                PrehashedAdaptiveRegion::Contiguous(region) => region.chunks().iter(),
            })
            .map(|target| target.chunk_id())
            .collect::<Vec<_>>();
        let desired_workers =
            NonZeroUsize::new(desired_workers.get().min(ordinary_regions.len().max(1)))
                .expect("ASSERT: serial assembly needs one worker");
        let worker_lease = self.worker_permits.acquire(desired_workers);
        let workers = worker_lease.workers();
        self.encode_cpu.record_permit(&worker_lease);
        let cpu_phase = self.encode_cpu.begin();
        assert!(
            workers.get() <= self.worker_budget.get(),
            "ASSERT: one encode job cannot exceed the write-through worker budget"
        );
        let prepared =
            ContainerRepository::<C>::prepare_mixed_prehashed_reduction_with_worker_retirement(
                random_container_id()?,
                generation,
                &ordinary_regions,
                independent,
                dependents,
                workers,
                Some(&chunk_order),
                &|worker| {
                    if worker != 0 {
                        worker_lease.retire_worker();
                    }
                },
            )?;
        drop(cpu_phase);
        drop(worker_lease);
        let (verified, _) = self
            .containers
            .publish_prepared_adaptive_profiled_with_placement(prepared, new_chunks[0].placement)?;
        let entries = verified
            .locations()
            .iter()
            .copied()
            .map(|location| {
                ExactIndexEntry::from_verified(location)
                    .expect("ASSERT: verified write-through Location forms an Exact Index entry")
            })
            .collect();
        Ok((entries, similarity_entries))
    }

    fn externalize_chunks(
        &self,
        chunks: &[PendingWriteThroughChunk],
        inode: InodeId,
        locations: &[ExactIndexEntry],
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let mut externalized = Vec::new();
        externalized
            .try_reserve(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut entries = Vec::with_capacity(chunks.len());
        for pending in chunks {
            let chunk_id = pending.chunk_id;
            let entry = locations
                .binary_search_by_key(&chunk_id, ExactIndexEntry::chunk_id)
                .ok()
                .and_then(|ordinal| locations.get(ordinal))
                .copied()
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            let expected_length = u32::try_from(pending.bytes.len())
                .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
            if entry.logical_length() != expected_length {
                return Err(DurableNamespaceError::ChunkLengthConflict {
                    chunk_id,
                    first_length: u64::from(entry.logical_length()),
                    second_length: u64::from(expected_length),
                });
            }
            entries.push(entry);
        }
        // One shared virtual file per bounded publication lets live reads
        // batch adjacent chunk views, including before Exact activation.
        let source: Arc<dyn CommittedFile> = Arc::new(crate::ManifestCommittedFile::from_verified(
            self.index
                .prepare(VerifiedManifestFile::from_published_locations(
                    &entries,
                    self.containers.clone(),
                )?),
        ));
        let mut source_offset = 0;
        for (pending, entry) in chunks.iter().zip(entries) {
            externalized.push(ExternalizedExtent::new(
                inode,
                pending.offset,
                pending.bytes.through_sequence(),
                Arc::new(VerifiedLocationFile {
                    source: Arc::clone(&source),
                    source_offset,
                    entry,
                }),
            )?);
            source_offset += u64::from(entry.logical_length());
        }
        Ok(externalized)
    }

    fn externalized_location(
        &self,
        inode: InodeId,
        offset: u64,
        through_sequence: u64,
        entry: ExactIndexEntry,
        admission: OnlineProofAdmission,
    ) -> Result<ExternalizedExtent, DurableNamespaceError> {
        self.online_dependency_proofs
            .remember_active(entry, admission);
        self.externalized_proven_location(inode, offset, through_sequence, entry)
    }

    fn externalized_proven_location(
        &self,
        inode: InodeId,
        offset: u64,
        through_sequence: u64,
        entry: ExactIndexEntry,
    ) -> Result<ExternalizedExtent, DurableNamespaceError> {
        ExternalizedExtent::new(
            inode,
            offset,
            through_sequence,
            Arc::new(VerifiedLocationFile {
                source: Arc::new(crate::ManifestCommittedFile::from_verified(
                    self.index
                        .prepare(VerifiedManifestFile::from_published_locations(
                            &[entry],
                            self.containers.clone(),
                        )?),
                )),
                source_offset: 0,
                entry,
            }),
        )
        .map_err(Into::into)
    }
}

impl<C> MutationObserver for WriteThroughIngest<C>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    fn opened_write_handle(&self, inode: InodeId) {
        self.queue.opened_write_handle(inode);
    }

    fn released_write_handle(&self, inode: InodeId) {
        self.queue.released_write_handle(inode);
    }

    fn accepted_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        small_file: bool,
        bytes: MutationPayload,
    ) -> Vec<ExternalizedExtent> {
        self.enqueue_write(
            inode,
            offset,
            mutation_sequence,
            if small_file {
                ContainerPlacement::SmallFile
            } else {
                ContainerPlacement::Data
            },
            &bytes,
        );
        Vec::new()
    }

    fn accepted_truncate(&self, inode: InodeId, mutation_sequence: u64, _length: u64) {
        self.queue.enqueue(IngestJob {
            inode,
            mutation_sequence,
            kind: IngestJobKind::Truncate,
        });
    }

    fn wait_through(&self, inode: InodeId, mutation_sequence: u64) {
        self.queue.wait_through(inode, mutation_sequence);
        self.publication_queue
            .wait_through(inode, mutation_sequence);
        self.index.flush_level_zero();
    }
}

impl<C> Drop for WriteThroughIngest<C> {
    fn drop(&mut self) {
        self.queue.shutdown();
        self.publication_queue.shutdown();
        let current = std::thread::current().id();
        let workers = self
            .workers
            .get_mut()
            .expect("ASSERT: ingest worker handle lock poisoned during shutdown");
        for worker in workers.drain(..) {
            if worker.thread().id() != current {
                worker
                    .join()
                    .expect("ASSERT: permanent ingest worker must not panic");
            }
        }
    }
}

fn install_write_through<C>(
    namespace: &Arc<Namespace>,
    containers: ContainerRepository<C>,
    container_generations: ContainerGenerationAllocator<C>,
    index: Arc<dyn ManifestReaderPolicy<C>>,
    worker_budget: NonZeroUsize,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
) -> Arc<WriteThroughIngest<C>>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let worker_permits = Arc::new(WorkerPermits::new(worker_budget));
    containers.install_cpu_admission(Arc::clone(&worker_permits));
    let write_through = Arc::new(WriteThroughIngest {
        containers,
        container_generations,
        index,
        worker_budget,
        worker_permits,
        active_writers: AtomicUsize::new(0),
        hash_batches: AtomicUsize::new(0),
        maximum_hash_workers: AtomicUsize::new(0),
        hash_cpu: CpuPhaseTelemetry::default(),
        encode_cpu: CpuPhaseTelemetry::default(),
        planning_cpu: CpuPhaseTelemetry::default(),
        materialization_wall_ns: AtomicU64::new(0),
        registry: Mutex::new(WriteThroughRegistry::default()),
        queue: Arc::new(IngestQueue::new()),
        publication_queue: Arc::new(PublicationQueue::new()),
        namespace: OnceLock::new(),
        workers: Mutex::new(Vec::new()),
        online_dependency_proofs,
    });
    write_through.start_workers(namespace);
    namespace.install_mutation_observer(write_through.clone());
    write_through
}

trait ManifestReaderPolicy<C>: fmt::Debug + Send + Sync {
    fn advanced_reduction_available(&self) -> bool {
        false
    }
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C>;
    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier>;
    fn exact_index_run_count(&self) -> usize;
    fn verified_location(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<ExactIndexEntry>;
    fn plan_similarity_chunk(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    );
    fn plan_similarity_batch(
        &self,
        containers: &ContainerRepository<C>,
        targets: &[PrehashedChunk<'_>],
        _workers: NonZeroUsize,
        _admission: &WorkerPermits,
    ) -> Vec<(
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    )> {
        targets
            .iter()
            .map(|target| self.plan_similarity_chunk(containers, target.chunk_id(), target.bytes()))
            .collect()
    }
    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>);
    fn publish_reduction_batch(
        &self,
        entries: Vec<ExactIndexEntry>,
        _similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        _guard: Option<fastdup_store::ReductionPublicationGuard>,
    ) {
        self.publish_level_zero(entries);
    }
    fn flush_level_zero(&self);
    fn exact_index_degraded(&self) -> bool;
    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus;
    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus;
    fn read_cache_status(&self) -> VerifiedReadCacheStatus;
    fn advanced_reduction_status(&self) -> PersistentReductionStatus;
    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus;
}

#[derive(Debug)]
struct ScanManifestReaders {
    read_cache: Arc<VerifiedReadCache>,
}

impl<C: StorageIo + 'static> ManifestReaderPolicy<C> for ScanManifestReaders {
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        file.with_verified_read_cache(Arc::clone(&self.read_cache))
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        Box::new(containers)
    }

    fn exact_index_run_count(&self) -> usize {
        0
    }

    fn verified_location(
        &self,
        _containers: &ContainerRepository<C>,
        _chunk_id: ChunkId,
        _logical_length: u64,
    ) -> Option<ExactIndexEntry> {
        None
    }

    fn plan_similarity_chunk(
        &self,
        _containers: &ContainerRepository<C>,
        _target_id: ChunkId,
        _target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    ) {
        (PersistentChunkPlan::NoCandidates, None)
    }

    fn publish_level_zero(&self, _entries: Vec<ExactIndexEntry>) {}

    fn flush_level_zero(&self) {}

    fn exact_index_degraded(&self) -> bool {
        false
    }

    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        ExactIndexPageCacheStatus::default()
    }

    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        ExactRunMembershipStatus::default()
    }

    fn read_cache_status(&self) -> VerifiedReadCacheStatus {
        self.read_cache.status()
    }

    fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        PersistentReductionStatus::default()
    }

    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        SimilarityIndexPageCacheStatus::default()
    }
}

struct IndexedManifestReaders<X> {
    core: Arc<ExactPublisherCore<X>>,
    publisher: ExactPublicationQueue,
    read_cache: Arc<VerifiedReadCache>,
    reduction: Option<Arc<PersistentReductionIndex<X>>>,
}

struct ExactPublisherCore<X> {
    repository: ExactIndexRunRepository<X>,
    profile: ExactIndexProfileId,
    degraded: AtomicBool,
    recent: RwLock<BTreeMap<(ChunkId, u32), ExactIndexEntry>>,
    similarity: Option<SimilarityPublicationQueue<X>>,
    failed_reduction_guard: Mutex<Option<fastdup_store::ReductionPublicationGuard>>,
}

enum ExactPublicationCommand {
    Publish(
        Vec<ExactIndexEntry>,
        Vec<fastdup_format::SimilarityIndexEntry>,
        Option<fastdup_store::ReductionPublicationGuard>,
    ),
    Flush(mpsc::SyncSender<()>),
    Shutdown,
}

struct ExactPublicationQueue {
    sender: mpsc::SyncSender<ExactPublicationCommand>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

type SimilarityBatch = Vec<fastdup_format::SimilarityIndexEntry>;

struct SimilarityPublicationQueue<X> {
    sender: mpsc::SyncSender<Option<SimilarityBatch>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    repository: Arc<fastdup_store::OnlineSimilarityRepository<X>>,
}

impl<X: Clone + Send + Sync + StorageIo + 'static> SimilarityPublicationQueue<X> {
    fn start(repository: Arc<fastdup_store::OnlineSimilarityRepository<X>>) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Option<SimilarityBatch>>(2);
        let worker_repository = Arc::clone(&repository);
        let worker = std::thread::Builder::new()
            .name("fastdup-similarity-publisher".to_owned())
            .spawn(move || {
                while let Ok(Some(entries)) = receiver.recv() {
                    if let Err(error) = worker_repository.append_current(&entries) {
                        eprintln!("online Similarity publication degraded: {error}");
                    }
                }
            })?;
        Ok(Self {
            sender,
            worker: Mutex::new(Some(worker)),
            repository,
        })
    }

    fn publish(&self, mut entries: Vec<fastdup_format::SimilarityIndexEntry>) {
        if entries.is_empty() {
            return;
        }
        let limit = fastdup_store::ONLINE_SIMILARITY_BATCH_ENTRIES;
        if entries.len() > limit {
            self.repository.skip(entries.len() - limit);
            entries.truncate(limit);
        }
        let count = entries.len();
        if self.sender.try_send(Some(entries)).is_err() {
            self.repository.skip(count);
        }
    }
}

impl<X> Drop for SimilarityPublicationQueue<X> {
    fn drop(&mut self) {
        let _ = self.sender.send(None);
        if let Some(worker) = self.worker.lock().expect("Similarity worker lock").take() {
            let _ = worker.join();
        }
    }
}

impl<X: Clone + StorageIo> fmt::Debug for IndexedManifestReaders<X> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedManifestReaders")
            .field("run_count", &self.run_count())
            .field("degraded", &self.core.degraded.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<X: Clone + StorageIo> IndexedManifestReaders<X> {
    fn run_count(&self) -> usize {
        self.core
            .repository
            .pin_active_generation()
            .as_deref()
            .map_or(0, fastdup_store::ActivatedExactIndex::run_count)
    }
}

impl ExactPublicationQueue {
    fn start<X>(core: Arc<ExactPublisherCore<X>>) -> io::Result<Self>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(EXACT_PUBLICATION_QUEUE_BATCHES);
        let worker = std::thread::Builder::new()
            .name("fastdup-exact-publisher".to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        ExactPublicationCommand::Publish(entries, similarities, guard) => {
                            if core
                                .try_publish_level_zero(entries.clone(), similarities)
                                .is_err()
                            {
                                core.degraded.store(true, Ordering::Release);
                                // One retained admission is enough to prevent
                                // GC unlink after an unindexed dependent write.
                                if let Some(guard) = guard {
                                    core.failed_reduction_guard
                                        .lock()
                                        .expect("failed reduction guard lock")
                                        .get_or_insert(guard);
                                }
                            }
                            core.forget_recent(&entries);
                        }
                        ExactPublicationCommand::Flush(reply) => {
                            let _ = reply.send(());
                        }
                        ExactPublicationCommand::Shutdown => break,
                    }
                }
            })?;
        Ok(Self {
            sender,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn publish(
        &self,
        entries: Vec<ExactIndexEntry>,
        similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        guard: Option<fastdup_store::ReductionPublicationGuard>,
    ) {
        self.sender
            .send(ExactPublicationCommand::Publish(
                entries,
                similarities,
                guard,
            ))
            .expect("ASSERT: permanent Exact publisher remains alive while mounted");
    }

    fn flush(&self) {
        let (reply, receive) = mpsc::sync_channel(1);
        self.sender
            .send(ExactPublicationCommand::Flush(reply))
            .expect("ASSERT: permanent Exact publisher remains alive while mounted");
        receive
            .recv()
            .expect("ASSERT: permanent Exact publisher acknowledges every fence");
    }
}

impl Drop for ExactPublicationQueue {
    fn drop(&mut self) {
        let _ = self.sender.send(ExactPublicationCommand::Shutdown);
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("ASSERT: Exact publisher handle lock poisoned during shutdown")
            .take()
        {
            worker
                .join()
                .expect("ASSERT: permanent Exact publisher must not panic");
        }
    }
}

impl<X> ExactPublisherCore<X>
where
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn remember_recent(&self, entries: &[ExactIndexEntry]) {
        let mut recent = self
            .recent
            .write()
            .expect("ASSERT: recent Exact Location lock poisoned");
        for entry in entries {
            recent.insert((entry.chunk_id(), entry.logical_length()), *entry);
        }
        while recent.len() > MAX_RECENT_EXACT_LOCATIONS {
            let oldest_key = *recent
                .keys()
                .next()
                .expect("ASSERT: an oversized recent Exact map is nonempty");
            recent.remove(&oldest_key);
        }
        assert!(
            recent.len() <= MAX_RECENT_EXACT_LOCATIONS,
            "ASSERT: recent Exact Location overlay exceeds its entry bound"
        );
    }

    fn recent_location(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        let length = u32::try_from(logical_length).ok()?;
        self.recent
            .read()
            .expect("ASSERT: recent Exact Location lock poisoned")
            .get(&(chunk_id, length))
            .copied()
    }

    fn forget_recent(&self, entries: &[ExactIndexEntry]) {
        let mut recent = self
            .recent
            .write()
            .expect("ASSERT: recent Exact Location lock poisoned");
        for entry in entries {
            let key = (entry.chunk_id(), entry.logical_length());
            if recent.get(&key) == Some(entry) {
                recent.remove(&key);
            }
        }
    }
}

impl<C, X> ManifestReaderPolicy<C> for IndexedManifestReaders<X>
where
    C: Clone + Send + Sync + StorageIo + 'static,
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn advanced_reduction_available(&self) -> bool {
        self.reduction.is_some()
            && self
                .core
                .failed_reduction_guard
                .lock()
                .expect("failed reduction guard lock")
                .is_none()
    }
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        let file = file.with_index_repository(&self.core.repository);
        file.with_verified_read_cache(Arc::clone(&self.read_cache))
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        let active = self.core.repository.pin_active_generation();
        match active {
            Some(index) => Box::new(IndexedRequiredChunkVerifier::new(containers, index)),
            None => Box::new(containers),
        }
    }

    fn exact_index_run_count(&self) -> usize {
        self.run_count()
    }

    fn verified_location(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<ExactIndexEntry> {
        let active = self.core.repository.pin_active_generation();
        if let Some(recent) = self.core.recent_location(chunk_id, logical_length)
            && active
                .as_deref()
                .is_none_or(|index| index.permits_active_overlay(recent).unwrap_or(false))
            && containers.read_verified_location(recent).is_ok()
        {
            return Some(recent);
        }
        let active = active?;
        if let Ok(location) =
            containers.find_verified_location_with_index(&active, chunk_id, logical_length)
        {
            location
        } else {
            self.core.degraded.store(true, Ordering::Release);
            None
        }
    }

    fn plan_similarity_chunk(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    ) {
        self.reduction
            .as_ref()
            .and_then(|reduction| {
                reduction
                    .plan_chunk_for_publication_cached(
                        containers,
                        target_id,
                        target,
                        Some(&self.read_cache),
                    )
                    .ok()
            })
            .unwrap_or((PersistentChunkPlan::NoCandidates, None))
    }

    fn plan_similarity_batch(
        &self,
        containers: &ContainerRepository<C>,
        targets: &[PrehashedChunk<'_>],
        workers: NonZeroUsize,
        admission: &WorkerPermits,
    ) -> Vec<(
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    )> {
        self.reduction.as_ref().map_or_else(
            || {
                targets
                    .iter()
                    .map(|_| (PersistentChunkPlan::NoCandidates, None))
                    .collect()
            },
            |reduction| {
                reduction.plan_batch_for_publication_cached(
                    containers,
                    targets,
                    Some(&self.read_cache),
                    workers,
                    admission,
                )
            },
        )
    }

    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>) {
        if entries.is_empty() {
            return;
        }
        self.core.remember_recent(&entries);
        self.publisher.publish(entries, Vec::new(), None);
    }

    fn publish_reduction_batch(
        &self,
        entries: Vec<ExactIndexEntry>,
        similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        guard: Option<fastdup_store::ReductionPublicationGuard>,
    ) {
        if entries.is_empty() {
            return;
        }
        self.core.remember_recent(&entries);
        self.publisher.publish(entries, similarities, guard);
    }

    fn flush_level_zero(&self) {
        self.publisher.flush();
    }

    fn exact_index_degraded(&self) -> bool {
        self.core.degraded.load(Ordering::Acquire)
    }

    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        self.core.repository.page_cache_status()
    }

    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        self.core
            .repository
            .pin_active_generation()
            .as_deref()
            .map_or_else(ExactRunMembershipStatus::default, |active| {
                active.membership_status()
            })
    }

    fn read_cache_status(&self) -> VerifiedReadCacheStatus {
        self.read_cache.status()
    }

    fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        self.reduction.as_deref().map_or_else(
            PersistentReductionStatus::default,
            PersistentReductionIndex::status,
        )
    }

    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        self.reduction.as_deref().map_or_else(
            SimilarityIndexPageCacheStatus::default,
            PersistentReductionIndex::similarity_page_cache_status,
        )
    }
}

impl<X> ExactPublisherCore<X>
where
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn try_publish_level_zero(
        &self,
        entries: Vec<ExactIndexEntry>,
        similarities: Vec<fastdup_format::SimilarityIndexEntry>,
    ) -> Result<(), fastdup_store::ExactIndexStoreError> {
        self.repository.append_level_zero(self.profile, entries)?;
        if let Some(queue) = &self.similarity {
            queue.publish(similarities);
        }
        self.degraded.store(false, Ordering::Release);
        Ok(())
    }
}

impl<M, C> DurableNamespace<M, C>
where
    M: Clone + Send + Sync + StorageIo + 'static,
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
        let read_cache = Arc::new(VerifiedReadCache::new_system()?);
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            Arc::new(ScanManifestReaders { read_cache }),
            false,
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
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            None,
            inode_reservation_span,
            false,
        )
    }

    /// Opens a writable namespace with one immutable pool-wide
    /// Exact/Similarity pair pinned for bounded write-through Prefix trials.
    ///
    /// A missing, stale, or corrupt Similarity snapshot disables advanced
    /// reduction without affecting Exact reuse or data availability.
    ///
    /// # Errors
    ///
    /// Returns the same recovery, reservation, graph, durability, and
    /// namespace-construction failures as [`Self::open`]. Exact or Similarity
    /// Index recovery failure only disables the affected acceleration path.
    pub fn open_with_reduction_indexes<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: &SimilarityIndexRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            Some(similarities),
            inode_reservation_span,
            false,
        )
    }

    /// Mounts after structural validation; demand reads and new DATA still use
    /// full verification. The owning daemon must scrub in the background and
    /// keep online deletion disabled until that pass succeeds.
    ///
    /// # Errors
    /// Returns structural recovery, reservation, or Namespace setup failures.
    pub fn open_with_structural_recovery<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: &SimilarityIndexRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            Some(similarities),
            inode_reservation_span,
            true,
        )
    }

    fn open_with_optional_reduction_index<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: Option<&SimilarityIndexRepository<X>>,
        inode_reservation_span: u64,
        structural_recovery: bool,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let read_cache = Arc::new(VerifiedReadCache::new_system()?);
        let recovered = indexes.recover_active_generation().and_then(|active| {
            if let Some(index) = &active {
                let retiring = indexes.retiring_containers(index)?;
                containers.install_retiring_selection_barrier(&retiring);
            }
            Ok(active)
        });
        let initially_degraded = recovered.is_err();
        let active = recovered.ok().flatten();
        let online =
            similarities.and_then(
                |repository| match fastdup_store::OnlineSimilarityRepository::open(
                    repository.clone(),
                    indexes,
                ) {
                    Ok(online) => Some(Arc::new(online)),
                    Err(error) => {
                        eprintln!("online Similarity recovery disabled: {error}");
                        None
                    }
                },
            );
        let reduction = online
            .as_ref()
            .map(|online| Arc::new(PersistentReductionIndex::online(Arc::clone(online))));
        let similarity = online.map(SimilarityPublicationQueue::start).transpose()?;
        let profile = active
            .as_ref()
            .map_or_else(checkpoint_exact_index_profile_v1, |index| {
                index.run_set().profile()
            });
        let core = Arc::new(ExactPublisherCore {
            repository: indexes.clone(),
            profile,
            degraded: AtomicBool::new(initially_degraded),
            recent: RwLock::new(BTreeMap::new()),
            similarity,
            failed_reduction_guard: Mutex::new(None),
        });
        let publisher = ExactPublicationQueue::start(Arc::clone(&core))?;
        let manifest_readers: Arc<dyn ManifestReaderPolicy<C>> = Arc::new(IndexedManifestReaders {
            core,
            publisher,
            read_cache,
            reduction,
        });
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            manifest_readers,
            structural_recovery,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep recovery, inode reservation and mutation admission in lifecycle order"
    )]
    fn open_using(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
        manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
        structural_recovery: bool,
    ) -> Result<Self, DurableNamespaceError> {
        if inode_reservation_span == 0 {
            return Err(DurableNamespaceError::InvalidReservationSpan);
        }
        let graph_verifier = manifest_readers.graph_verifier(containers.clone());
        let proof_started = Instant::now();
        let phase = if structural_recovery {
            "namespace_structure"
        } else {
            "namespace_data_proof"
        };
        eprintln!("recovery_phase={phase} state=started");
        let recovered = if structural_recovery {
            generations.recover_latest_with_structural_files(&containers)?
        } else {
            generations
                .recover_latest_with_verified_files_using(&containers, graph_verifier.as_ref())?
        };
        eprintln!(
            "recovery_phase={phase} state=complete elapsed_ms={}",
            proof_started.elapsed().as_millis()
        );
        let reservation_started = Instant::now();
        let (root, next_inode, reservation_end, installed_record, verified_files) = match recovered
        {
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
                let installed_record = generations.commit_namespace(&root)?;
                (
                    root,
                    FIRST_REGULAR_INODE,
                    reservation_end,
                    installed_record,
                    Vec::new(),
                )
            }
            Some(recovered) => {
                let (recovered, prior_files) = recovered.into_parts();
                let previous = recovered.namespace_root();
                let next_inode = recovered.inode_reservation_end_high_water();
                let reservation_end = next_inode
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new_with_root_metadata(
                    reservation_end,
                    next_inode,
                    previous.namespace_mutation_sequence(),
                    previous.root_metadata().clone(),
                    previous.inodes().to_vec(),
                    previous.entries().to_vec(),
                )?;
                let committed = if recovered.rejected_newer_generations() == 0 {
                    // Recovery established the selected graph under the startup policy.
                    // Reserving IDs preserves every inode/Manifest binding; the
                    // ordinary successor fence must still match the WAL head.
                    let predecessor =
                        SuccessorPredecessor::from_committed_record(recovered.record());
                    let proofs: Vec<_> = prior_files
                        .iter()
                        .map(|file| {
                            generations.reuse_manifest_successor(
                                predecessor,
                                file.manifest_summary().expect(
                                    "ASSERT: recovered files retain verified Manifest roots",
                                ),
                            )
                        })
                        .collect();
                    generations.commit_namespace_with_successor_proofs_using(
                        &root,
                        &containers,
                        predecessor,
                        &proofs,
                        graph_verifier.as_ref(),
                    )?
                } else {
                    // A fallback graph is not the current WAL head. Preserve
                    // the existing complete verification/transition path.
                    generations.commit_namespace_with_verified_files_using(
                        &root,
                        &containers,
                        graph_verifier.as_ref(),
                    )?
                };
                let (installed_record, verified_files) = committed.into_parts();
                (
                    root,
                    next_inode,
                    reservation_end,
                    installed_record,
                    verified_files,
                )
            }
        };
        eprintln!(
            "recovery_phase=inode_reservation state=complete elapsed_ms={}",
            reservation_started.elapsed().as_millis()
        );
        let container_generations =
            containers.open_generation_allocator(CONTAINER_GENERATION_RESERVATION_SPAN_V1)?;
        let manifests = load_manifest_cache(&root, &verified_files)?;
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
        let checkpoint_workers = available_workers;
        let namespace = Arc::new(namespace);
        let online_dependency_proofs = Arc::new(OnlineDependencyProofs::new()?);
        let write_through = install_write_through(
            &namespace,
            containers.clone(),
            container_generations.clone(),
            Arc::clone(&manifest_readers),
            checkpoint_workers,
            Arc::clone(&online_dependency_proofs),
        );
        Ok(Self {
            namespace,
            generations,
            containers,
            checkpoint_lock: Mutex::new(()),
            installed_predecessor: Mutex::new(SuccessorPredecessor::from_committed_record(
                installed_record,
            )),
            manifests: Mutex::new(manifests),
            container_generations,
            manifest_readers,
            checkpoint_workers,
            write_through,
            online_dependency_proofs,
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

    /// Returns bounded, pressure-aware Exact-Index hot-page cache evidence.
    #[must_use]
    pub fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        self.manifest_readers.exact_index_page_cache_status()
    }

    /// Returns active immutable-Run membership-filter memory and probe evidence.
    #[must_use]
    pub fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        self.manifest_readers.exact_run_membership_status()
    }

    /// Returns pressure-aware Similarity hot-page cache evidence.
    #[must_use]
    pub fn similarity_index_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        self.manifest_readers.similarity_page_cache_status()
    }

    /// Returns reduction counters without traversing ingest buffers.
    #[must_use]
    pub fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        self.manifest_readers.advanced_reduction_status()
    }

    /// Returns bounded shared read-cache memory and hit/miss evidence.
    ///
    /// A zero target means memory or Swap pressure disabled admission and
    /// purged all cached payloads. Fixed set metadata is reported separately
    /// from resident payload bytes.
    #[must_use]
    pub fn verified_read_cache_status(&self) -> VerifiedReadCacheStatus {
        self.manifest_readers.read_cache_status()
    }

    /// Returns process-local verified Container-envelope cache telemetry.
    #[must_use]
    pub fn container_descriptor_cache_status(&self) -> ContainerDescriptorCacheStatus {
        self.containers.descriptor_cache_status()
    }

    /// Returns pressure, admission, and hit evidence for historical S3-FIFO.
    ///
    /// Active and Frozen Generation proofs are pinned separately and therefore
    /// do not contribute to this rebuildable cache's entry count.
    #[must_use]
    pub fn historical_proof_cache_status(&self) -> HistoricalProofCacheStatus {
        self.online_dependency_proofs.historical_status()
    }

    /// Returns memory accounting for the non-evictable Active and Frozen proof sets.
    #[must_use]
    pub fn generation_proof_set_status(&self) -> GenerationProofSetStatus {
        self.online_dependency_proofs.generation_status()
    }

    /// Returns the runtime worker cap for independent Compression Regions.
    #[must_use]
    pub const fn checkpoint_worker_limit(&self) -> NonZeroUsize {
        self.checkpoint_workers
    }

    /// Returns bounded live state of the pre-commit SeqCDC/Container pipeline.
    #[must_use]
    pub fn write_through_status(&self) -> WriteThroughStatus {
        self.write_through.status()
    }

    /// Starts a bounded, payload-free trace of real online proof-cache events.
    ///
    /// Trace capture is benchmark instrumentation. It never changes cache
    /// authority, admission, eviction, durability, or recovery behavior.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive bounds and a second concurrent capture.
    pub fn start_online_proof_trace(&self, max_events: usize) -> Result<(), ProofCacheReplayError> {
        self.online_dependency_proofs.trace.start(max_events)
    }

    /// Finishes the active online proof-cache trace.
    ///
    /// # Errors
    ///
    /// Returns an error if capture was not active or exceeded its declared
    /// event bound. An overflow never truncates a trace silently.
    pub fn finish_online_proof_trace(&self) -> Result<ProofCacheTrace, ProofCacheReplayError> {
        self.online_dependency_proofs.trace.finish()
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
        // Freeze proofs before the namespace takes its mutation cut. Anything
        // accepted concurrently afterward stays in the next Active set. A
        // late proof for the selected cut may remain Active for one extra
        // generation, which is conservative and never demotes it too early.
        let newly_frozen_proofs = self.online_dependency_proofs.freeze_for_commit();
        let sealed_at_cut = self.write_through.capture_cut();
        let freeze_started = PhaseStarted::now();
        let commit = match self.namespace.begin_commit() {
            Ok(Some(commit)) => commit,
            Ok(None) => {
                // With no dirty generation, every Container trigger captured
                // above is stale acceleration evidence rather than uncommitted
                // data. Undo only the freeze created by this call.
                self.write_through.complete_cut(sealed_at_cut);
                self.online_dependency_proofs
                    .cancel_new_freeze(newly_frozen_proofs);
                return Ok(None);
            }
            Err(error) => {
                self.online_dependency_proofs
                    .cancel_new_freeze(newly_frozen_proofs);
                return Err(error.into());
            }
        };
        let mut metrics = CheckpointMetrics::default();
        freeze_started.finish_into(&mut metrics.freeze);
        self.write_through.wait_for_commit_cut(&commit);
        let stable = self.write_through.flush_stable_for_commit_cut()?;
        self.namespace.externalize_verified_extents(stable);
        let mut writer = AdaptiveCommitWriter::new(
            &self.containers,
            &self.container_generations,
            self.manifest_readers.as_ref(),
            self.checkpoint_workers,
            Arc::clone(&self.online_dependency_proofs),
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
            writer.begin_inode(
                inode.inode(),
                if commit.prefers_small_file_tier(inode) {
                    ContainerPlacement::SmallFile
                } else {
                    ContainerPlacement::Data
                },
                self.manifest_readers.advanced_reduction_available()
                    && self.namespace.advanced_reduction_enabled(inode.inode()),
            )?;
            let previous = installed_manifests
                .binary_search_by_key(&inode.inode().get(), |manifest| manifest.inode)
                .ok()
                .map(|index| installed_manifests[index]);
            manifests.push(plan_checkpoint_manifest(
                inode,
                previous,
                &self.generations,
                &mut writer,
            )?);
        }
        drop(installed_manifests);
        let (level_zero_entries, reduction_metrics, retained_ranges) = writer.finish()?;
        manifest_plan_started.finish_into(&mut metrics.manifest_plan);
        metrics.merge_reduction(&reduction_metrics);
        let exact_index_started = PhaseStarted::now();
        self.manifest_readers.publish_level_zero(level_zero_entries);
        // A checkpoint may have produced DATA outside the write-through
        // observer (for example, a partial lane drained at the commit cut).
        // Its Exact locations must be part of the durable activation history
        // before the Namespace generation can become visible. Ordinary ingest
        // keeps publication asynchronous until this commit/Sync fence.
        self.manifest_readers.flush_level_zero();
        exact_index_started.finish_into(&mut metrics.exact_index_publish);
        let metadata_started = PhaseStarted::now();
        let record = self.publish_generation(&commit, manifests, &retained_ranges)?;
        self.write_through.complete_cut(sealed_at_cut);
        self.online_dependency_proofs.complete_frozen();
        metadata_started.finish_into(&mut metrics.metadata_commit);
        total_started.finish_into(&mut metrics.total);
        Ok(Some(ProfiledCheckpoint { record, metrics }))
    }

    #[allow(clippy::too_many_lines)]
    fn publish_manifest_plans(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
        predecessor: SuccessorPredecessor,
        retained_ranges: &RetainedManifestRanges,
    ) -> Result<(Vec<DurableInode>, Vec<ManifestSuccessorProof>), DurableNamespaceError> {
        if manifests.len() != commit.inodes().len() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let mut durable_inodes = Vec::new();
        let mut successor_proofs = Vec::new();
        durable_inodes
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        successor_proofs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, publication) in commit.inodes().iter().zip(manifests) {
            let (manifest_root, mut proof) = match &publication {
                ManifestPublication::Reuse { summary } => {
                    let proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *summary);
                    (summary.root(), proof)
                }
                ManifestPublication::Append { previous, extents } => {
                    let proof =
                        self.generations
                            .stage_manifest_append(predecessor, *previous, extents)?;
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Replace {
                    previous,
                    replacements,
                    appended,
                } => {
                    let mut proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *previous);
                    for replacement in replacements {
                        proof = self.generations.stage_manifest_replacement_successor(
                            proof,
                            replacement.replaced.clone(),
                            &replacement.extents,
                        )?;
                    }
                    if !appended.is_empty() {
                        proof = self
                            .generations
                            .stage_manifest_append_successor(proof, appended)?;
                    }
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Truncate {
                    previous,
                    replacements,
                    logical_size,
                    allocated_bytes,
                } => {
                    let mut proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *previous);
                    for replacement in replacements {
                        proof = self.generations.stage_manifest_replacement_successor(
                            proof,
                            replacement.replaced.clone(),
                            &replacement.extents,
                        )?;
                    }
                    proof = self
                        .generations
                        .stage_manifest_truncate_successor(proof, *logical_size)?;
                    if proof.summary().allocated_bytes() != *allocated_bytes {
                        return Err(DurableNamespaceError::FrozenViewMismatch);
                    }
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Complete { manifest } => {
                    let proof = self
                        .generations
                        .stage_manifest_layout_successor(predecessor, manifest)?;
                    (proof.summary().root(), proof)
                }
            };
            if let Some(by_root) = retained_ranges.get(&inode.inode().get()) {
                for (source_root, ranges) in by_root {
                    for source_range in coalesced_ranges(ranges)? {
                        proof = self
                            .generations
                            .retain_predecessor_manifest_range_successor(
                                proof,
                                *source_root,
                                source_range,
                            )?;
                    }
                }
            }
            successor_proofs.push(proof);
            durable_inodes.push(
                DurableInode::new_with_metadata(
                    inode.inode().get(),
                    inode.mode(),
                    inode.uid(),
                    inode.gid(),
                    inode.link_count(),
                    inode.mutation_sequence(),
                    inode.logical_size(),
                    manifest_root,
                    inode.metadata().file_flags(),
                    durable_xattrs(inode.metadata())?,
                )?
                .with_times(durable_times(inode.times())),
            );
        }
        Ok((durable_inodes, successor_proofs))
    }

    fn publish_generation(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
        retained_ranges: &RetainedManifestRanges,
    ) -> Result<CommitRecord, DurableNamespaceError> {
        let predecessor = *self
            .installed_predecessor
            .lock()
            .expect("ASSERT: installed predecessor lock poisoned");
        let (durable_inodes, successor_proofs) =
            self.publish_manifest_plans(commit, manifests, predecessor, retained_ranges)?;
        let mut installs = Vec::new();
        installs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let root = namespace_root_for_commit(commit, durable_inodes)?;
        let fallback = self
            .manifest_readers
            .graph_verifier(self.containers.clone());
        let graph_verifier = OnlineSuccessorVerifier {
            proofs: Arc::clone(&self.online_dependency_proofs),
            fallback,
        };
        let committed = self
            .generations
            .commit_namespace_with_successor_proofs_using(
                &root,
                &self.containers,
                predecessor,
                &successor_proofs,
                &graph_verifier,
            )?;
        let (record, verified_files) = committed.into_parts();
        assert_eq!(
            verified_files.len(),
            commit.inodes().len(),
            "ASSERT: committed DATA proof must cover every frozen inode"
        );
        let mut next_manifests = Vec::new();
        next_manifests
            .try_reserve_exact(verified_files.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, verified) in commit.inodes().iter().zip(verified_files) {
            assert_eq!(
                verified.inode(),
                inode.inode().get(),
                "ASSERT: committed DATA proof order must match the Namespace Root"
            );
            assert_eq!(
                verified.logical_size(),
                inode.logical_size(),
                "ASSERT: published Manifest reread length must equal the planned Manifest"
            );
            assert_eq!(
                verified.manifest_root(),
                root.inodes()
                    .binary_search_by_key(&inode.inode().get(), DurableInode::inode)
                    .ok()
                    .and_then(|ordinal| root.inodes()[ordinal].file_manifest_root()),
                "ASSERT: committed DATA proof must retain the published Manifest Root"
            );
            assert_eq!(
                verified.allocated_bytes(),
                inode.allocated_bytes(),
                "ASSERT: committed Manifest allocation must match the Frozen inode"
            );
            let summary = verified
                .manifest_summary()
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            next_manifests.push(InstalledManifest {
                inode: inode.inode().get(),
                root: summary.root(),
                logical_size: summary.logical_size(),
                allocated_bytes: summary.allocated_bytes(),
                summary,
            });
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
        *self
            .installed_predecessor
            .lock()
            .expect("ASSERT: installed predecessor lock poisoned") =
            SuccessorPredecessor::from_committed_record(record);
        Ok(record)
    }
}

fn namespace_root_for_commit(
    commit: &NamespaceCommit,
    mut durable_inodes: Vec<DurableInode>,
) -> Result<NamespaceRoot, DurableNamespaceError> {
    durable_inodes
        .try_reserve_exact(commit.directories().len() + commit.symlinks().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for directory in commit.directories() {
        durable_inodes.push(
            DurableInode::new_directory_with_metadata(
                directory.inode().get(),
                directory.mode(),
                directory.uid(),
                directory.gid(),
                directory.link_count(),
                directory.mutation_sequence(),
                directory.metadata().file_flags(),
                durable_xattrs(directory.metadata())?,
            )?
            .with_times(durable_times(directory.times())),
        );
    }
    for symlink in commit.symlinks() {
        durable_inodes.push(
            DurableInode::new_symlink(
                symlink.inode().get(),
                symlink.uid(),
                symlink.gid(),
                symlink.link_count(),
                symlink.mutation_sequence(),
                symlink.target().to_vec(),
            )?
            .with_times(durable_times(symlink.times())),
        );
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
    NamespaceRoot::new_with_root_metadata(
        commit.inode_reservation_end(),
        commit.inode_allocation_cursor(),
        commit.namespace_mutation_sequence(),
        DurableRootMetadata::new(
            commit.root().mode(),
            commit.root().uid(),
            commit.root().gid(),
            commit.root().metadata().file_flags(),
            durable_xattrs(commit.root().metadata())?,
        )?
        .with_times(durable_times(commit.root().times())),
        durable_inodes,
        entries,
    )
    .map_err(Into::into)
}

fn durable_times(times: fastdup_posix::PosixTimes) -> DurableTimes {
    fn timestamp(value: fastdup_posix::PosixTimestamp) -> DurableTimestamp {
        DurableTimestamp {
            seconds: value.seconds,
            nanoseconds: value.nanoseconds,
        }
    }
    DurableTimes {
        atime: timestamp(times.atime),
        mtime: timestamp(times.mtime),
        ctime: timestamp(times.ctime),
    }
}

fn durable_xattrs(
    metadata: &fastdup_posix::InodeMetadata,
) -> Result<Vec<DurableXattr>, DurableNamespaceError> {
    let attributes = metadata.xattrs();
    let mut durable = Vec::new();
    durable
        .try_reserve_exact(attributes.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for xattr in attributes {
        durable.push(DurableXattr::new(
            xattr.name().to_vec(),
            xattr.value().to_vec(),
        )?);
    }
    Ok(durable)
}

enum ManifestPublication {
    Reuse {
        summary: ManifestTreeSummary,
    },
    Append {
        previous: ManifestTreeSummary,
        extents: Vec<ManifestExtent>,
    },
    Replace {
        previous: ManifestTreeSummary,
        replacements: Vec<ManifestReplacement>,
        appended: Vec<ManifestExtent>,
    },
    Truncate {
        previous: ManifestTreeSummary,
        replacements: Vec<ManifestReplacement>,
        logical_size: u64,
        allocated_bytes: u64,
    },
    Complete {
        manifest: ManifestLayout,
    },
}

struct ManifestReplacement {
    replaced: Range<u64>,
    extents: Vec<ManifestExtent>,
}

fn plan_checkpoint_manifest<M: StorageIo, C: StorageIo>(
    inode: &CommitInode,
    previous: Option<InstalledManifest>,
    generations: &GenerationRepository<M>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let changed = inode.changed_ranges()?;
    if let Some(previous) = previous
        && previous.logical_size == logical_size
    {
        if changed.is_empty() {
            if previous.allocated_bytes != inode.allocated_bytes() {
                return Err(DurableNamespaceError::FrozenViewMismatch);
            }
            return Ok(ManifestPublication::Reuse {
                summary: previous.summary,
            });
        }
        return plan_path_local_manifest(inode, previous, &changed, generations, writer);
    }

    if let Some(previous) = previous
        && previous.logical_size < logical_size
        && changed
            .iter()
            .all(|range| range.offset() >= previous.logical_size)
    {
        return plan_append_manifest(inode, previous, writer);
    }

    if let Some(previous) = previous {
        return plan_path_local_manifest(inode, previous, &changed, generations, writer);
    }
    let manifest = plan_full_manifest(inode, writer)?;
    Ok(ManifestPublication::Complete { manifest })
}

fn plan_append_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: InstalledManifest,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let append_length = inode
        .logical_size()
        .checked_sub(previous.logical_size)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    assert!(append_length > 0, "ASSERT: append plan must grow the file");
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    plan_manifest_range_with_prepared(
        inode,
        previous.logical_size,
        append_length,
        writer,
        &mut stack,
        &mut extents,
    )?;
    let appended_allocated = extents.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(DurableNamespaceError::ArithmeticOverflow)
        }
    })?;
    if previous
        .allocated_bytes
        .checked_add(appended_allocated)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?
        != inode.allocated_bytes()
    {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(ManifestPublication::Append {
        previous: previous.summary,
        extents,
    })
}

fn plan_path_local_manifest<M: StorageIo, C: StorageIo>(
    inode: &CommitInode,
    previous: InstalledManifest,
    changed: &[CommitRange],
    generations: &GenerationRepository<M>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let shrinking = logical_size < previous.logical_size;
    let rewrites = manifest_rewrites(logical_size, previous, changed, generations)?;
    let mut replacements = Vec::new();
    replacements
        .try_reserve_exact(rewrites.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut allocated_bytes = previous.allocated_bytes;
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for rewrite in rewrites {
        let previous_extents = generations.read_manifest_range(
            previous.root,
            previous.logical_size,
            rewrite.start..rewrite.end,
        )?;
        let removed = allocated_bytes_in_range(&previous_extents, rewrite.start..rewrite.end)?;
        let mut extents = Vec::new();
        plan_manifest_range_with_prepared(
            inode,
            rewrite.start,
            rewrite.end.min(logical_size) - rewrite.start,
            writer,
            &mut stack,
            &mut extents,
        )?;
        assert!(
            stack.is_empty(),
            "ASSERT: path-local range planner must consume its complete work stack"
        );
        if rewrite.end > logical_size {
            extents.push(ManifestExtent::Hole {
                logical_length: rewrite.end - logical_size,
            });
        }
        ManifestLayout::validate(rewrite.end - rewrite.start, &extents)?;
        let added = manifest_extent_allocation(&extents)?;
        allocated_bytes = allocated_bytes
            .checked_sub(removed)
            .and_then(|remaining| remaining.checked_add(added))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        replacements.push(ManifestReplacement {
            replaced: rewrite.start..rewrite.end,
            extents,
        });
    }
    let mut appended = Vec::new();
    if inode.logical_size() > previous.logical_size {
        plan_manifest_range_with_prepared(
            inode,
            previous.logical_size,
            inode.logical_size() - previous.logical_size,
            writer,
            &mut stack,
            &mut appended,
        )?;
        allocated_bytes = allocated_bytes
            .checked_add(manifest_extent_allocation(&appended)?)
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    }
    if shrinking {
        return Ok(ManifestPublication::Truncate {
            previous: previous.summary,
            replacements,
            logical_size,
            allocated_bytes: inode.allocated_bytes(),
        });
    }
    if allocated_bytes != inode.allocated_bytes() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(ManifestPublication::Replace {
        previous: previous.summary,
        replacements,
        appended,
    })
}

fn manifest_rewrites<M: StorageIo>(
    logical_size: u64,
    previous: InstalledManifest,
    changed: &[CommitRange],
    generations: &GenerationRepository<M>,
) -> Result<Vec<RewriteRange>, DurableNamespaceError> {
    let mut rewrites = rewrite_ranges_before(
        changed,
        logical_size,
        previous.logical_size.min(logical_size),
    )?;
    if logical_size < previous.logical_size && logical_size > 0 {
        let boundary = generations.read_manifest_range(
            previous.root,
            previous.logical_size,
            logical_size - 1..logical_size,
        )?;
        let extent = boundary
            .first()
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        let end = extent
            .logical_offset()
            .checked_add(extent_length(extent.extent()))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        if matches!(extent.extent(), ManifestExtent::Data { .. }) && end > logical_size {
            rewrites.push(RewriteRange {
                start: extent.logical_offset(),
                end,
            });
            rewrites.sort_unstable_by_key(|range| range.start);
        }
    }
    for rewrite in &mut rewrites {
        let touched = generations.read_manifest_range(
            previous.root,
            previous.logical_size,
            rewrite.start..rewrite.end,
        )?;
        for extent in touched {
            if !matches!(extent.extent(), ManifestExtent::Data { .. }) {
                continue;
            }
            let extent_end = extent
                .logical_offset()
                .checked_add(extent_length(extent.extent()))
                .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
            rewrite.start = rewrite.start.min(extent.logical_offset());
            rewrite.end = rewrite.end.max(extent_end);
        }
    }
    coalesce_rewrites(&mut rewrites);

    Ok(rewrites)
}

fn manifest_extent_allocation(extents: &[ManifestExtent]) -> Result<u64, DurableNamespaceError> {
    extents.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(DurableNamespaceError::ArithmeticOverflow)
        }
    })
}

fn allocated_bytes_in_range(
    extents: &[fastdup_store::ManifestRangeExtent],
    range: Range<u64>,
) -> Result<u64, DurableNamespaceError> {
    let mut allocated = 0_u64;
    for extent in extents {
        if matches!(extent.extent(), ManifestExtent::Hole { .. }) {
            continue;
        }
        let extent_end = extent
            .logical_offset()
            .checked_add(extent_length(extent.extent()))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        let start = extent.logical_offset().max(range.start);
        let end = extent_end.min(range.end);
        if start < end {
            allocated = allocated
                .checked_add(end - start)
                .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        }
    }
    Ok(allocated)
}

/// Exact-Index profile paired with the durable checkpoint's SeqCDC-v1 rules.
///
/// # Panics
///
/// Panics only if BLAKE3 maps the fixed canonical profile bytes to the
/// reserved all-zero identity, an impossible production `ASSERT` for this
/// pinned input.
#[must_use]
pub fn checkpoint_exact_index_profile_v1() -> ExactIndexProfileId {
    ExactIndexProfileId::new(
        ChunkId::of(
            b"fastdup/SeqCDC-v1/mode=increasing,sequence=6,skip-trigger=50,skip=1024,min=16384,max=262144",
        )
        .bytes(),
    )
    .expect("ASSERT: the SeqCDC-v1 Exact-Index profile hash is nonzero")
}

fn load_manifest_cache<I: StorageIo>(
    root: &NamespaceRoot,
    files: &[VerifiedCommittedFile<I>],
) -> Result<Vec<InstalledManifest>, DurableNamespaceError> {
    if root.file_inode_count() != files.len() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let mut manifests: Vec<InstalledManifest> = Vec::new();
    manifests
        .try_reserve_exact(root.file_inode_count())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for (inode, file) in root.file_inodes().zip(files) {
        if file.inode() != inode.inode()
            || file.manifest_root() != Some(inode.manifest_root())
            || file.logical_size() != inode.logical_size()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if let Some(previous) = manifests.last()
            && previous.inode >= inode.inode()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let summary = file
            .manifest_summary()
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        manifests.push(InstalledManifest {
            inode: inode.inode(),
            root: summary.root(),
            logical_size: summary.logical_size(),
            allocated_bytes: summary.allocated_bytes(),
            summary,
        });
    }
    Ok(manifests)
}

fn plan_full_manifest<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestLayout, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    if logical_size != 0 {
        plan_manifest_range_with_prepared(
            inode,
            0,
            logical_size,
            writer,
            &mut stack,
            &mut extents,
        )?;
    }
    let manifest = ManifestLayout::new(logical_size, extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

#[derive(Clone, Copy, Debug)]
struct RewriteRange {
    start: u64,
    end: u64,
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

fn coalesced_ranges(ranges: &[Range<u64>]) -> Result<Vec<Range<u64>>, DurableNamespaceError> {
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(ranges.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    sorted.extend_from_slice(ranges);
    sorted.sort_unstable_by_key(|range| range.start);
    let mut output = Vec::<Range<u64>>::new();
    output
        .try_reserve_exact(sorted.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for range in sorted {
        if range.start >= range.end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        match output.last_mut() {
            Some(previous) if range.start <= previous.end => {
                previous.end = previous.end.max(range.end);
            }
            _ => output.push(range),
        }
    }
    Ok(output)
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::DataSlice { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

fn plan_manifest_range_with_prepared<C: StorageIo>(
    inode: &CommitInode,
    offset: u64,
    length: u64,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    stack: &mut Vec<(u64, u64)>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    assert!(length > 0, "ASSERT: manifest range must be nonempty");
    assert!(
        stack.is_empty(),
        "ASSERT: prepared range planning requires an empty work stack"
    );
    let end = offset
        .checked_add(length)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    let prepared = inode.prepared_extents_in_range(offset, length)?;
    let mut cursor = offset;
    for extent in prepared {
        let prepared_end = extent
            .offset()
            .checked_add(extent.length())
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        if extent.offset() < cursor || prepared_end > end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if cursor < extent.offset() {
            stack.push((cursor, extent.offset() - cursor));
            plan_manifest_ranges(inode, writer, stack, extents)?;
            assert!(
                stack.is_empty(),
                "ASSERT: range planner must consume a prepared-recipe gap"
            );
        }
        let manifest_extent = match extent.recipe() {
            PreparedDataRecipe::Chunk { chunk_id } => {
                if extent.length()
                    > u64::try_from(MAX_LOGICAL_CHUNK_BYTES)
                        .expect("ASSERT: maximum logical Chunk bytes fit u64")
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                let chunk_id = ChunkId::from_bytes(chunk_id);
                writer.record_prepared_chunk(chunk_id, extent.length(), extent)?;
                ManifestExtent::Data {
                    logical_length: extent.length(),
                    chunk_id,
                }
            }
            PreparedDataRecipe::ChunkSlice {
                chunk_id,
                chunk_length,
                chunk_offset,
            } => {
                let slice_end = u64::from(chunk_offset)
                    .checked_add(extent.length())
                    .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
                if chunk_length == 0
                    || usize::try_from(chunk_length).is_err()
                    || usize::try_from(chunk_length)
                        .is_ok_and(|length| length > MAX_LOGICAL_CHUNK_BYTES)
                    || slice_end > u64::from(chunk_length)
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                let chunk_id = ChunkId::from_bytes(chunk_id);
                writer.record_prepared_chunk(chunk_id, u64::from(chunk_length), extent)?;
                ManifestExtent::DataSlice {
                    logical_length: extent.length(),
                    chunk_id,
                    chunk_length,
                    chunk_offset,
                }
            }
            PreparedDataRecipe::Fill { value } => {
                writer.record_prepared_fill(extent.length());
                ManifestExtent::Fill {
                    logical_length: extent.length(),
                    value,
                }
            }
        };
        push_extent(extents, manifest_extent)?;
        cursor = prepared_end;
    }
    if cursor < end {
        stack.push((cursor, end - cursor));
        plan_manifest_ranges(inode, writer, stack, extents)?;
    }
    assert!(
        stack.is_empty(),
        "ASSERT: prepared range planner must consume its complete work stack"
    );
    Ok(())
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
        "ASSERT: SeqCDC-v1 maximum must equal the durable format bound"
    );
    writer.metrics.checkpoint_rechunk_bytes = writer
        .metrics
        .checkpoint_rechunk_bytes
        .checked_add(length)
        .expect("ASSERT: checkpoint rechunk bytes cannot overflow u64");
    let reader = CommitRangeReader {
        inode,
        start: offset,
        consumed: 0,
        length,
    };
    let mut chunks = SeqCdcStream::new(reader)?;
    let mut expected_offset = 0_u64;
    loop {
        let cdc_started = PhaseStarted::now();
        let chunk = chunks.next_chunk()?;
        cdc_started.finish_into(&mut writer.metrics.cdc);
        let Some(chunk) = chunk else {
            break;
        };
        assert_eq!(
            expected_offset,
            chunks.consumed_bytes() - u64::try_from(chunk.len()).expect("bounded Chunk length"),
            "ASSERT: SeqCDC-v1 chunks must be contiguous"
        );
        assert!(
            !chunk.is_empty() && chunk.len() <= CDC_MAXIMUM_BYTES,
            "ASSERT: SeqCDC-v1 returned an invalid logical Chunk length"
        );
        let logical_length =
            u64::try_from(chunk.len()).expect("ASSERT: a bounded SeqCDC Chunk length fits u64");
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
            .expect("ASSERT: a SeqCDC DATA range cursor cannot overflow");
        let hash_started = PhaseStarted::now();
        let fill = chunk.iter().all(|byte| *byte == chunk[0]);
        let chunk_id = (!fill).then(|| ChunkId::of(&chunk));
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
                    value: chunk[0],
                },
            )?;
        } else {
            let chunk_id = chunk_id.expect("ASSERT: non-FILL data must have one Chunk ID");
            writer.push(chunk_id, chunk)?;
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

struct SeqCdcStream<R> {
    reader: R,
    buffer: Vec<u8>,
    start: usize,
    eof: bool,
    consumed_bytes: u64,
}

impl<R: Read> SeqCdcStream<R> {
    fn new(reader: R) -> Result<Self, DurableNamespaceError> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(2 * CDC_MAXIMUM_BYTES)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        Ok(Self {
            reader,
            buffer,
            start: 0,
            eof: false,
            consumed_bytes: 0,
        })
    }

    fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, DurableNamespaceError> {
        if self.start >= CDC_MAXIMUM_BYTES {
            self.buffer.copy_within(self.start.., 0);
            self.buffer.truncate(self.buffer.len() - self.start);
            self.start = 0;
        }
        while self.buffer.len() - self.start < CDC_MAXIMUM_BYTES && !self.eof {
            let available = self.buffer.len() - self.start;
            let requested = CDC_MAXIMUM_BYTES - available;
            let old_length = self.buffer.len();
            let new_length = old_length
                .checked_add(requested)
                .expect("ASSERT: bounded SeqCDC stream buffer cannot overflow");
            assert!(
                new_length <= self.buffer.capacity(),
                "ASSERT: SeqCDC stream buffer exceeds its fixed reservation"
            );
            self.buffer.resize(new_length, 0);
            let read = self.reader.read(&mut self.buffer[old_length..])?;
            self.buffer.truncate(old_length + read);
            self.eof = read == 0;
        }
        let remaining = &self.buffer[self.start..];
        if remaining.is_empty() {
            return Ok(None);
        }
        let length = if seqcdc_force_scalar() {
            seqcdc_cut_scalar(remaining, SEQCDC_CONFIG_V1)
        } else {
            seqcdc_cut(remaining, SEQCDC_CONFIG_V1)
        };
        assert!(
            length != 0 && length <= remaining.len() && length <= CDC_MAXIMUM_BYTES,
            "ASSERT: SeqCDC selected an invalid stream Chunk length"
        );
        let mut chunk = Vec::new();
        chunk
            .try_reserve_exact(length)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        chunk.extend_from_slice(&remaining[..length]);
        self.start += length;
        self.consumed_bytes = self
            .consumed_bytes
            .checked_add(u64::try_from(length).expect("ASSERT: bounded Chunk length fits u64"))
            .expect("ASSERT: SeqCDC stream position cannot overflow");
        Ok(Some(chunk))
    }

    const fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }
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
            u32::try_from(requested).expect("ASSERT: SeqCDC-v1 reads never exceed 256 KiB");
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
    manifest: &ManifestLayout,
    inode: &CommitInode,
) -> Result<(), DurableNamespaceError> {
    let planned_allocated = manifest.extents().iter().try_fold(0_u64, |total, extent| {
        let length = match extent {
            ManifestExtent::Data { logical_length, .. }
            | ManifestExtent::DataSlice { logical_length, .. }
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
    container_generations: &'a ContainerGenerationAllocator<C>,
    index: &'a dyn ManifestReaderPolicy<C>,
    seen: BTreeMap<ChunkId, u64>,
    chunks: Vec<Vec<u8>>,
    chunk_ids: Vec<ChunkId>,
    advanced: bool,
    level_zero_entries: Vec<ExactIndexEntry>,
    payload_bytes: usize,
    workers: NonZeroUsize,
    metrics: CheckpointReductionMetrics,
    current_inode: Option<u64>,
    placement: ContainerPlacement,
    retained_ranges: RetainedManifestRanges,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
}

impl<'a, C: StorageIo> AdaptiveCommitWriter<'a, C> {
    fn new(
        containers: &'a ContainerRepository<C>,
        container_generations: &'a ContainerGenerationAllocator<C>,
        index: &'a dyn ManifestReaderPolicy<C>,
        workers: NonZeroUsize,
        online_dependency_proofs: Arc<OnlineDependencyProofs>,
    ) -> Self {
        Self {
            containers,
            container_generations,
            index,
            seen: BTreeMap::new(),
            chunks: Vec::new(),
            chunk_ids: Vec::new(),
            advanced: false,
            level_zero_entries: Vec::new(),
            payload_bytes: 0,
            workers,
            metrics: CheckpointReductionMetrics::default(),
            current_inode: None,
            placement: ContainerPlacement::Data,
            retained_ranges: BTreeMap::new(),
            online_dependency_proofs,
        }
    }

    fn begin_inode(
        &mut self,
        inode: InodeId,
        placement: ContainerPlacement,
        advanced: bool,
    ) -> Result<(), DurableNamespaceError> {
        if self.placement != placement || self.advanced != advanced {
            self.flush()?;
            self.placement = placement;
            self.advanced = advanced;
        }
        self.current_inode = Some(inode.get());
        Ok(())
    }

    fn record_prepared_chunk(
        &mut self,
        chunk_id: ChunkId,
        length: u64,
        prepared: PreparedCommitExtent,
    ) -> Result<(), DurableNamespaceError> {
        assert!(length > 0, "ASSERT: a prepared Chunk is nonempty");
        if let Some(previous) = self.seen.insert(chunk_id, length)
            && previous != length
        {
            return Err(DurableNamespaceError::ChunkLengthConflict {
                chunk_id,
                first_length: previous,
                second_length: length,
            });
        }
        match (
            prepared.retained_manifest_root(),
            prepared.retained_source_offset(),
        ) {
            (Some(root), Some(source_offset)) => {
                let root =
                    MetadataObjectId::new(root).ok_or(DurableNamespaceError::FrozenViewMismatch)?;
                let end = source_offset
                    .checked_add(prepared.length())
                    .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
                let inode = self
                    .current_inode
                    .expect("ASSERT: prepared Chunk planning requires an active inode");
                self.retained_ranges
                    .entry(inode)
                    .or_default()
                    .entry(root)
                    .or_default()
                    .push(source_offset..end);
            }
            (None, None) => {}
            _ => return Err(DurableNamespaceError::FrozenViewMismatch),
        }
        self.record_prepared_recipe(length);
        Ok(())
    }

    fn record_prepared_fill(&mut self, length: u64) {
        assert!(length > 0, "ASSERT: a prepared FILL is nonempty");
        self.metrics.fill_chunks = self
            .metrics
            .fill_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint FILL Chunk count cannot overflow u64");
        self.metrics.fill_bytes = self
            .metrics
            .fill_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint FILL bytes cannot overflow u64");
        self.record_prepared_recipe(length);
    }

    fn record_prepared_recipe(&mut self, length: u64) {
        self.metrics.logical_chunks = self
            .metrics
            .logical_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint logical Chunk count cannot overflow u64");
        self.metrics.logical_chunk_bytes = self
            .metrics
            .logical_chunk_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint logical Chunk bytes cannot overflow u64");
        self.metrics.recipe_reuse_chunks = self
            .metrics
            .recipe_reuse_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint recipe-reuse count cannot overflow u64");
        self.metrics.recipe_reuse_bytes = self
            .metrics
            .recipe_reuse_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint recipe-reuse bytes cannot overflow u64");
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
        if let Some(entry) = self
            .online_dependency_proofs
            .verified_entry(chunk_id, length)
        {
            self.online_dependency_proofs
                .remember_frozen(entry, OnlineProofAdmission::Touch);
            self.record_exact_hit(length);
            exact_started.finish_into(&mut self.metrics.exact_lookup);
            return Ok(());
        }
        if let Some(entry) = self
            .index
            .verified_location(self.containers, chunk_id, length)
        {
            self.online_dependency_proofs
                .remember_frozen(entry, OnlineProofAdmission::ExactReuse);
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
        self.chunk_ids.push(chunk_id);
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

    fn finish(mut self) -> Result<AdaptiveCommitFinish, DurableNamespaceError> {
        self.flush()?;
        Ok((self.level_zero_entries, self.metrics, self.retained_ranges))
    }

    fn flush(&mut self) -> Result<(), DurableNamespaceError> {
        if self.chunks.is_empty() {
            return Ok(());
        }
        let id = random_container_id()?;
        let publication_guard = (self.advanced && self.index.advanced_reduction_available())
            .then(|| self.containers.try_pin_reduction_publication())
            .flatten();
        let advanced = publication_guard.is_some();
        let mut regions = Vec::<Vec<PrehashedChunk<'_>>>::new();
        let mut independent = Vec::new();
        let mut dependents = Vec::new();
        let mut similarities = Vec::new();
        let mut region_bytes = 0_usize;
        for (chunk, &chunk_id) in self.chunks.iter().zip(&self.chunk_ids) {
            if advanced {
                let (plan, hint) =
                    self.index
                        .plan_similarity_chunk(self.containers, chunk_id, chunk);
                if let Some(entry) = hint {
                    similarities.push(entry);
                }
                match plan {
                    PersistentChunkPlan::NoCandidates => (),
                    PersistentChunkPlan::Independent(record) => {
                        independent.push(record);
                        region_bytes = 0;
                        continue;
                    }
                    PersistentChunkPlan::Dependent(record) => {
                        dependents.push(record);
                        region_bytes = 0;
                        continue;
                    }
                }
            }
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
            region.push(PrehashedChunk::new(chunk_id, chunk));
            region_bytes = region_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            assert!(
                region_bytes <= COMPRESSION_REGION_TARGET_BYTES,
                "ASSERT: no logical Chunk may exceed a Compression Region"
            );
        }
        let region_refs = regions
            .iter()
            .map(|r| PrehashedAdaptiveRegion::Borrowed(r))
            .collect::<Vec<_>>();
        let generation = self.container_generations.reserve_generation()?;
        let prepared = ContainerRepository::<C>::prepare_mixed_prehashed_reduction_parallel(
            id,
            generation,
            &region_refs,
            independent,
            dependents,
            self.workers,
            Some(&self.chunk_ids),
        )?;
        let (verified, publish_metrics) = self
            .containers
            .publish_prepared_adaptive_profiled_with_placement(prepared, self.placement)?;
        self.record_container_metrics(publish_metrics);
        let mut published_entries = Vec::new();
        published_entries
            .try_reserve(verified.locations().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for location in verified.locations().iter().copied() {
            let entry = ExactIndexEntry::from_verified(location).expect(
                "ASSERT: fully verified Container evidence must form a valid Exact-Index Location",
            );
            self.online_dependency_proofs
                .remember_frozen(entry, OnlineProofAdmission::Published);
            published_entries.push(entry);
        }
        self.index
            .publish_reduction_batch(published_entries, similarities, publication_guard);
        self.chunks.clear();
        self.chunk_ids.clear();
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
        self.metrics
            .incompressibility_gate
            .checked_merge(published.incompressibility_gate())
            .expect("ASSERT: bounded gate metrics cannot overflow within one checkpoint");
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
    ReadCache(VerifiedReadCacheError),
    Mount(MountError),
    InvalidReservationSpan,
    InodeReservationExhausted,
    ContainerGenerationExhausted,
    ArithmeticOverflow,
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
            Self::ReadCache(error) => Some(error),
            Self::Mount(error) => Some(error),
            Self::Posix(_)
            | Self::InvalidReservationSpan
            | Self::InodeReservationExhausted
            | Self::ContainerGenerationExhausted
            | Self::ArithmeticOverflow
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

impl From<VerifiedReadCacheError> for DurableNamespaceError {
    fn from(error: VerifiedReadCacheError) -> Self {
        Self::ReadCache(error)
    }
}

impl From<MountError> for DurableNamespaceError {
    fn from(error: MountError) -> Self {
        Self::Mount(error)
    }
}

#[cfg(test)]
#[path = "checkpoint/lane_lifetime_tests.rs"]
mod lane_lifetime_tests;

#[cfg(test)]
mod tests {

    #[test]
    fn block_fill_scan_matches_scalar_across_fragment_and_lane_edges() {
        for length in 1..=512 {
            for value in [0, 31, 255] {
                for split in [1, 31, 32, 33, length] {
                    let mut bytes = vec![value; length];
                    for bad in [None, Some(0), Some(length / 2), Some(length - 1)] {
                        if let Some(index) = bad {
                            bytes[index] ^= 1;
                        }
                        let parts = bytes
                            .chunks(split)
                            .map(|part| MutationPayload::from_owned_bytes(part.to_vec()))
                            .collect();
                        let input = ChunkFragments::new(parts, length);
                        assert_eq!(input.is_fill(), bytes.iter().all(|byte| *byte == bytes[0]));
                        if let Some(index) = bad {
                            bytes[index] ^= 1;
                        }
                    }
                }
            }
        }
    }

    fn publication_fixture(first_sequence: u64, through_sequence: u64) -> DetachedContainerWork {
        let bytes = vec![17; 16_384];
        DetachedContainerWork::new(
            InodeId::new(2).unwrap(),
            through_sequence,
            vec![PendingWriteThroughChunk {
                offset: 0,
                chunk_id: ChunkId::of(&bytes),
                bytes: ChunkFragments::new_through(
                    vec![MutationPayload::from_owned_bytes(bytes)],
                    16_384,
                    first_sequence,
                ),
                placement: ContainerPlacement::Data,
            }],
            16_384,
        )
    }

    #[test]
    fn publication_fence_waits_for_pre_cut_chunks_in_a_later_ending_batch() {
        let queue = Arc::new(PublicationQueue::new());
        let inode = InodeId::new(2).unwrap();
        queue.enqueue(publication_fixture(7, 36));
        let work = queue.next_work().unwrap();
        let waiter = Arc::clone(&queue);
        let (sender, receiver) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            waiter.wait_through(inode, 13);
            sender.send(()).unwrap();
        });
        let escaped = receiver.recv_timeout(Duration::from_millis(100)).is_ok();
        queue.finish(&work);
        thread.join().unwrap();
        assert!(
            !escaped,
            "the pre-cut Chunk recipe is not attached before retirement"
        );
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn fixed_publication_fence_does_not_wait_for_later_arrivals() {
        let queue = PublicationQueue::new();
        let inode = InodeId::new(2).unwrap();
        queue.enqueue(publication_fixture(7, 36));
        let first = queue.next_work().unwrap();
        let target = queue.retirement_target(inode);
        queue.enqueue(publication_fixture(8, 37));
        let later = queue.next_work().unwrap();
        queue.finish(&first);
        queue.wait_for_retirement(inode, target);
        assert_eq!(queue.buffered_bytes(), 16_384);
        queue.finish(&later);
    }

    fn budget_entry(ordinal: usize) -> ExactIndexEntry {
        let location =
            ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
        ExactIndexEntry::active(ChunkId::of(&ordinal.to_le_bytes()), 8, location).unwrap()
    }

    #[test]
    fn proof_budget_overflow_keeps_existing_proofs_and_verifies_uncached_dependencies() {
        struct RejectMissing;
        impl RequiredChunkVerifier for RejectMissing {
            fn verify_required_chunks(
                &self,
                required: &BTreeMap<ChunkId, u64>,
            ) -> Result<(), StoreError> {
                assert_eq!(required.len(), 1);
                let (&chunk_id, &logical_length) = required.first_key_value().unwrap();
                Err(StoreError::MissingVerifiedChunk {
                    chunk_id,
                    logical_length,
                })
            }
        }
        // This test requires a historical hit; supply deterministic capacity
        // instead of competing with concurrently running system-cache tests.
        let snapshot = fastdup_store::MemoryPressureSnapshot::new(1 << 30, 1 << 30, 0);
        let mut owned = OnlineDependencyProofs::new().unwrap();
        owned.historical = HistoricalProofCache::new_with_snapshot(
            crate::historical_proof_cache::HistoricalProofCacheConfig::conservative(snapshot),
            snapshot,
        )
        .unwrap();
        let proofs = Arc::new(owned);
        for ordinal in 0..MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2 {
            proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
        }
        assert!(proofs.freeze_for_commit());
        for ordinal in MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2..MAX_ONLINE_DEPENDENCY_PROOFS_V1 {
            proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
        }
        let overflow = budget_entry(MAX_ONLINE_DEPENDENCY_PROOFS_V1);
        proofs.remember_frozen(overflow, OnlineProofAdmission::ExactReuse);
        proofs.remember_active(overflow, OnlineProofAdmission::Published);
        assert_eq!(
            proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1
        );
        assert_eq!(proofs.verified_entry(overflow.chunk_id(), 8), None);
        let resident = budget_entry(0);
        assert_eq!(
            proofs.verified_entry_for_active(resident.chunk_id(), 8),
            Some(resident)
        );
        assert!(matches!(
            proofs.claim_publication(resident.chunk_id(), 8),
            PublicationClaim::Existing(_)
        ));
        assert_eq!(
            proofs.generation_status().active_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2
        );

        let verifier = OnlineSuccessorVerifier {
            proofs: Arc::clone(&proofs),
            fallback: Box::new(RejectMissing),
        };
        let required = BTreeMap::from([(resident.chunk_id(), 8), (overflow.chunk_id(), 8)]);
        assert!(
            matches!(verifier.verify_required_chunks(&required), Err(StoreError::MissingVerifiedChunk { chunk_id, .. }) if chunk_id == overflow.chunk_id())
        );
        // Exercise the real Container verifier, including a corrupt object,
        // while the dependency cannot be admitted into either proof set.
        let io = MemoryStorageIo::new();
        let containers = ContainerRepository::new(io.clone());
        let real_verifier = OnlineSuccessorVerifier {
            proofs: Arc::clone(&proofs),
            fallback: Box::new(containers.clone()),
        };
        assert!(real_verifier.verify_required_chunks(&required).is_err());
        let payload = MAX_ONLINE_DEPENDENCY_PROOFS_V1.to_le_bytes();
        containers
            .publish_raw(ContainerId::new([31; 16]).unwrap(), 1, &[&payload])
            .unwrap();
        real_verifier.verify_required_chunks(&required).unwrap();
        assert_eq!(
            proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1
        );
        let name = io.list_names().unwrap().pop().unwrap();
        io.write_at(&name, 0, b"BAD!").unwrap();
        assert!(real_verifier.verify_required_chunks(&required).is_err());

        // Even historical hits must remain safe when promotion has no capacity.
        proofs
            .historical
            .admit(overflow, HistoricalProofAdmission::ExactReuse);
        assert!(proofs.unproven(&required).is_empty());
        assert_eq!(
            proofs.verified_entry_for_active(overflow.chunk_id(), 8),
            Some(overflow)
        );
        assert_eq!(
            proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1
        );
        proofs.complete_frozen();
        proofs.remember_active(overflow, OnlineProofAdmission::Published);
        assert_eq!(
            proofs.generation_status().active_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2 + 1
        );
        assert!(proofs.freeze_for_commit());
        proofs.cancel_new_freeze(true);
        assert_eq!(
            proofs.verified_entry(overflow.chunk_id(), 8),
            Some(overflow)
        );
    }

    #[test]
    fn proof_budget_overflow_releases_completed_publication_claims() {
        let proofs = OnlineDependencyProofs::new().unwrap();
        for ordinal in 0..MAX_ONLINE_DEPENDENCY_PROOFS_V1 {
            proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
        }
        assert!(proofs.freeze_for_commit());
        let entry = budget_entry(MAX_ONLINE_DEPENDENCY_PROOFS_V1);
        let key = (entry.chunk_id(), entry.logical_length());
        assert!(matches!(
            proofs.claim_publication(key.0, key.1),
            PublicationClaim::Acquired
        ));
        proofs.finish_publications(&[entry], &[key]);
        assert!(proofs.generation.lock().unwrap().publishing.is_empty());
        assert_eq!(proofs.generation_status().active_proofs(), 0);
        assert_eq!(
            proofs.generation_status().frozen_proofs(),
            MAX_ONLINE_DEPENDENCY_PROOFS_V1
        );
        assert!(matches!(
            proofs.claim_publication(key.0, key.1),
            PublicationClaim::Acquired
        ));
        proofs.abandon_publications(&[key]);
        proofs.complete_frozen();
        proofs.remember_active(entry, OnlineProofAdmission::Published);
        assert_eq!(proofs.verified_entry(key.0, u64::from(key.1)), Some(entry));
    }

    #[test]
    fn active_proof_lookup_promotes_frozen_and_preserves_reuse_admission() {
        let proofs = OnlineDependencyProofs::new().unwrap();
        let location =
            ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
        let id = ChunkId::of(b"proof");
        let entry = ExactIndexEntry::active(id, 5, location).unwrap();
        proofs.remember_active(entry, OnlineProofAdmission::Published);
        assert_eq!(proofs.verified_entry_for_active(id, 5), Some(entry));
        assert!(proofs.freeze_for_commit());
        assert_eq!(proofs.verified_entry_for_active(id, 5), Some(entry));
        proofs.remember_active(entry, OnlineProofAdmission::Published);
        let state = proofs.generation.lock().unwrap();
        assert_eq!(state.active.len(), 1);
        assert_eq!(state.frozen.as_ref().unwrap().len(), 1);
        assert_eq!(
            state.active.get(&(id, 5)).unwrap().admission,
            HistoricalProofAdmission::ExactReuse
        );
        drop(state);
        assert_eq!(proofs.verified_entry_for_active(id, 6), None);
    }

    #[test]
    fn generation_arena_checks_full_keys_and_survives_cancelled_freeze() {
        let proofs = OnlineDependencyProofs::new().unwrap();
        let location =
            ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
        let mut first = [3_u8; 32];
        first[8] = 7;
        let mut second = first;
        second[8] = 8;
        let a = ExactIndexEntry::active(ChunkId::from_bytes(first), 32, location).unwrap();
        let b = ExactIndexEntry::active(ChunkId::from_bytes(second), 32, location).unwrap();
        assert_eq!(
            GenerationProofMap::hash((a.chunk_id(), 32)),
            GenerationProofMap::hash((b.chunk_id(), 32))
        );
        proofs.remember_active(a, OnlineProofAdmission::Published);
        proofs.remember_active(b, OnlineProofAdmission::Published);
        assert!(proofs.freeze_for_commit());
        assert_eq!(proofs.verified_entry_for_active(b.chunk_id(), 32), Some(b));
        proofs.cancel_new_freeze(true);
        let state = proofs.generation.lock().unwrap();
        assert!(state.frozen.is_none());
        assert_eq!(state.active.len(), 2);
        assert_eq!(state.active.get(&(a.chunk_id(), 32)).unwrap().entry, a);
        assert_eq!(
            state.active.get(&(b.chunk_id(), 32)).unwrap().admission,
            HistoricalProofAdmission::ExactReuse
        );
        assert!(state.active.get(&(a.chunk_id(), 33)).is_none());
        drop(state);
        assert!(proofs.freeze_for_commit());
        let frozen = proofs.generation.lock().unwrap().frozen.take().unwrap();
        let ordered: Vec<_> = frozen
            .into_sorted_values()
            .map(|p| p.entry.chunk_id())
            .collect();
        assert_eq!(ordered, vec![a.chunk_id(), b.chunk_id()]);
        assert_eq!(proofs.generation_status().accounted_bytes(), 0);
    }

    #[test]
    fn hash_granularity_preserves_fragmented_mixed_batches_and_partial_permits() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(10)
            .build()
            .unwrap();
        pool.install(|| {
            let permits = WorkerPermits::new(NonZeroUsize::new(10).unwrap());
            let mut batch = Vec::new();
            for ordinal in 0..128 {
                let mut bytes = vec![19; CDC_MAXIMUM_BYTES];
                if ordinal % 3 != 0 {
                    let middle = bytes.len() / 2;
                    bytes[middle] = 27;
                }
                let parts = bytes
                    .chunks(17003)
                    .map(|part| MutationPayload::from_owned_bytes(part.to_vec()))
                    .collect();
                batch.push(StableChunk {
                    offset: (ordinal * CDC_MAXIMUM_BYTES) as u64,
                    bytes: ChunkFragments::new(parts, bytes.len()),
                });
            }
            let expected: Vec<_> = batch
                .iter()
                .map(|c| (!c.bytes.is_fill()).then(|| c.bytes.chunk_id()))
                .collect();
            let held = permits.acquire(NonZeroUsize::new(8).unwrap());
            let (actual, workers) = classify_stable_chunk_batch(
                &batch,
                NonZeroUsize::new(10).unwrap(),
                &permits,
                &CpuPhaseTelemetry::default(),
            )
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(workers, 2);
            assert_eq!(permits.available(), 2);
            drop(held);
            let fill: Vec<_> = batch.into_iter().step_by(3).collect();
            let (actual, workers) = classify_stable_chunk_batch(
                &fill,
                NonZeroUsize::new(10).unwrap(),
                &permits,
                &CpuPhaseTelemetry::default(),
            )
            .unwrap();
            assert!(actual.iter().all(Option::is_none));
            assert_eq!(workers, 1);
            assert_eq!(permits.available(), 10);
            assert_eq!(
                stable_hash_workers(32 * 1024 * 1024, 128, NonZeroUsize::new(10).unwrap()).get(),
                10
            );
        });
    }

    #[test]
    #[should_panic(expected = "ordered and disjoint")]
    fn pending_append_rejects_overlap_before_staging() {
        let mut pending = PendingWriteThrough::default();
        for offset in [10, 12] {
            pending
                .push(PendingWriteThroughChunk {
                    offset,
                    chunk_id: ChunkId::of(b"abc"),
                    bytes: ChunkFragments::new(
                        vec![MutationPayload::from_owned_bytes(b"abc".to_vec())],
                        3,
                    ),
                    placement: ContainerPlacement::Data,
                })
                .unwrap();
        }
    }

    #[test]
    #[should_panic(expected = "ordered and disjoint")]
    fn detach_independently_rejects_reordered_chunks() {
        let chunks = [10, 5]
            .into_iter()
            .map(|offset| PendingWriteThroughChunk {
                offset,
                chunk_id: ChunkId::of(b"abc"),
                bytes: ChunkFragments::new(
                    vec![MutationPayload::from_owned_bytes(b"abc".to_vec())],
                    3,
                ),
                placement: ContainerPlacement::Data,
            })
            .collect();
        DetachedContainerWork::new(InodeId::new(2).unwrap(), 1, chunks, 6);
    }
    use super::*;
    use fastdup_format::ExactIndexLocation;
    use fastdup_testkit::MemoryStorageIo;

    #[test]
    fn failed_exact_publication_retains_one_gc_guard_until_owner_teardown() {
        let containers = ContainerRepository::new(MemoryStorageIo::new());
        let core = Arc::new(ExactPublisherCore {
            repository: ExactIndexRunRepository::new(MemoryStorageIo::with_fail_before(0)),
            profile: checkpoint_exact_index_profile_v1(),
            degraded: AtomicBool::new(false),
            recent: RwLock::new(BTreeMap::new()),
            similarity: None,
            failed_reduction_guard: Mutex::new(None),
        });
        let queue = ExactPublicationQueue::start(Arc::clone(&core)).unwrap();
        let location =
            ExactIndexLocation::raw(ContainerId::new([7; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
        let entry = ExactIndexEntry::active(ChunkId::of(b"target"), 6, location).unwrap();
        queue.publish(
            vec![entry],
            Vec::new(),
            containers.try_pin_reduction_publication(),
        );
        queue.flush();
        assert!(core.degraded.load(Ordering::Acquire));
        assert!(core.failed_reduction_guard.lock().unwrap().is_some());
        // A later successful Exact write must not release the earlier failed
        // target's protection. One retained guard remains a bounded safe stop.
        queue.publish(
            vec![entry],
            Vec::new(),
            containers.try_pin_reduction_publication(),
        );
        queue.flush();
        assert!(!core.degraded.load(Ordering::Acquire));
        assert!(core.failed_reduction_guard.lock().unwrap().is_some());
    }

    #[test]
    fn completing_one_publication_batch_is_atomic_with_generation_freeze() {
        const ENTRY_COUNT: usize = 16_384;

        let proofs = Arc::new(OnlineDependencyProofs::new().expect("allocate proof sets"));
        let container_id = ContainerId::new([0xA7; 16]).expect("fixture ID is nonzero");
        let mut entries = Vec::with_capacity(ENTRY_COUNT);
        let mut claimed = Vec::with_capacity(ENTRY_COUNT);
        for ordinal in 0..ENTRY_COUNT {
            let mut chunk_bytes = [0_u8; 32];
            chunk_bytes[..8].copy_from_slice(
                &u64::try_from(ordinal + 1)
                    .expect("fixture ordinal fits u64")
                    .to_le_bytes(),
            );
            let chunk_id = ChunkId::from_bytes(chunk_bytes);
            let logical_length = 1_u32;
            let record_offset =
                4_096_u64 + u64::try_from(ordinal).expect("fixture ordinal fits u64") * 256;
            let location = ExactIndexLocation::raw(
                container_id,
                1,
                record_offset,
                256,
                u32::try_from(ordinal).expect("fixture ordinal fits u32"),
            )
            .expect("construct fixture RAW location");
            let entry = ExactIndexEntry::active(chunk_id, logical_length, location)
                .expect("construct fixture Exact entry");
            assert!(matches!(
                proofs.claim_publication(chunk_id, logical_length),
                PublicationClaim::Acquired
            ));
            entries.push(entry);
            claimed.push((chunk_id, logical_length));
        }

        let publisher_proofs = Arc::clone(&proofs);
        let publisher = std::thread::spawn(move || {
            publisher_proofs.finish_publications(&entries, &claimed);
        });

        let mut observed_partial_batch = false;
        loop {
            let state = proofs
                .generation
                .lock()
                .expect("fixture Generation Proof Set lock remains healthy");
            if state.active.len() != 0 && !state.publishing.is_empty() {
                observed_partial_batch = true;
                drop(state);
                assert!(proofs.freeze_for_commit());
                break;
            }
            if state.publishing.is_empty() {
                break;
            }
            drop(state);
            std::thread::yield_now();
        }

        assert!(
            publisher.join().is_ok(),
            "publication completion must not race"
        );
        assert!(
            !observed_partial_batch,
            "a Generation freeze observed only part of one published Container"
        );
    }

    #[test]
    fn publication_claims_match_grouped_encoder_locations_by_chunk_key() {
        let proofs = OnlineDependencyProofs::new().expect("allocate proof sets");
        let container_id = ContainerId::new([0xB8; 16]).expect("fixture ID is nonzero");
        let low_id = ChunkId::from_bytes([0x11; 32]);
        let high_id = ChunkId::from_bytes([0xEE; 32]);
        let logical_length = 1_u32;
        let low_location = ExactIndexLocation::raw(container_id, 1, 4_096, 256, 0)
            .expect("construct low fixture Location");
        let high_location = ExactIndexLocation::raw(container_id, 1, 8_192, 256, 1)
            .expect("construct high fixture Location");
        let low_entry = ExactIndexEntry::active(low_id, logical_length, low_location)
            .expect("construct low fixture Exact entry");
        let high_entry = ExactIndexEntry::active(high_id, logical_length, high_location)
            .expect("construct high fixture Exact entry");

        let mut claims = PublicationClaims::new(&proofs, 2).expect("allocate claims");
        assert!(matches!(
            claims.claim(low_id, logical_length),
            PublicationClaim::Acquired
        ));
        assert!(matches!(
            claims.claim(high_id, logical_length),
            PublicationClaim::Acquired
        ));

        // Advanced reduction groups ordinary, independent, and dependent
        // records before encoding, so writer Locations need not retain the
        // Chunk-ID order in which publication claims were acquired.
        let mut entries = [high_entry, low_entry];
        claims.finish(&mut entries);

        assert_eq!(
            proofs.verified_entry(low_id, u64::from(logical_length)),
            Some(low_entry)
        );
        assert_eq!(
            proofs.verified_entry(high_id, u64::from(logical_length)),
            Some(high_entry)
        );
    }

    #[test]
    fn exact_externalization_consumes_existing_verification_without_second_container_read() {
        let storage = MemoryStorageIo::new();
        let containers = ContainerRepository::new(storage.clone());
        let container_id = ContainerId::new([0xD4; 16]).expect("fixture ID is nonzero");
        let payload = b"one verified exact Chunk must not be physically verified twice";
        containers
            .publish_raw(container_id, 23, &[payload])
            .expect("publish fixture Container");
        let sealed = containers
            .read(container_id)
            .expect("read rebuild evidence for fixture entry");
        let entry = ExactIndexEntry::from_verified_raw(sealed.raw_locations()[0])
            .expect("construct fixture Exact entry");
        let verified = containers
            .read_verified_location(entry)
            .expect("perform the one physical candidate verification");
        let after_verification = storage.operation_count();
        let external = VerifiedLocationFile {
            source: Arc::new(crate::ManifestCommittedFile::from_verified(
                VerifiedManifestFile::from_published_locations(&[entry], containers).unwrap(),
            )),
            source_offset: 0,
            entry,
        };

        assert!(
            external
                .matches_complete_bytes(&verified)
                .expect("match already verified bytes"),
            "the externalization proof must match the physically verified Chunk"
        );
        assert_eq!(
            storage.operation_count(),
            after_verification,
            "frontend externalization must not repeat Container envelope or Record I/O"
        );
    }

    #[test]
    fn live_prefix_location_reads_before_target_index_activation() {
        let storage = MemoryStorageIo::new();
        let containers = ContainerRepository::new(storage.clone());
        let base = (0_u32..16384)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect::<Vec<_>>();
        let mut target = base.clone();
        target[17] ^= 0x5b;
        let base_id = ContainerId::new([0xe1; 16]).unwrap();
        containers.publish_raw(base_id, 1, &[&base]).unwrap();
        let base_entry =
            ExactIndexEntry::from_verified(containers.read(base_id).unwrap().locations()[0])
                .unwrap();
        let indexes = ExactIndexRunRepository::new(storage.clone());
        indexes
            .append_level_zero(
                ExactIndexProfileId::new([0xe3; 32]).unwrap(),
                vec![base_entry],
            )
            .unwrap();
        let active = indexes.pin_active_generation().unwrap();
        let publication = containers
            .publish_zstd_prefix_pairs_verified(
                ContainerId::new([0xe2; 16]).unwrap(),
                2,
                &[(&base, &target)],
            )
            .unwrap();
        let entry = ExactIndexEntry::from_verified(publication.locations()[0]).unwrap();
        let before = storage.operation_count();
        let reader = VerifiedManifestFile::from_published_locations(&[entry], containers)
            .unwrap()
            .with_active_index(&active);
        let external = VerifiedLocationFile {
            source: Arc::new(ManifestCommittedFile::from_verified(reader)),
            source_offset: 0,
            entry,
        };
        assert_eq!(
            storage.operation_count(),
            before,
            "live recipe construction cannot reread DATA"
        );
        assert_eq!(external.read_at(0, 16384).unwrap(), target);
        assert_eq!(
            external.read_shared_at(9, 53).unwrap().as_ref(),
            &target[9..62]
        );
    }

    #[test]
    fn segmented_ingest_tail_materializes_only_the_selected_prefix() {
        let mut tail = SegmentedIngestTail::default();
        for (sequence, bytes) in [b"abc".as_slice(), b"defg".as_slice(), b"hijkl".as_slice()]
            .into_iter()
            .enumerate()
        {
            tail.push(
                MutationPayload::try_copy_from_slice(bytes).expect("allocate fixture segment"),
                u64::try_from(sequence + 1).expect("fixture sequence fits u64"),
            );
        }

        let consumed = tail.take_prefix(5).expect("consume compact prefix");
        assert_eq!(consumed.as_bytes(), b"abcde");
        assert_eq!(tail.len(), 7);
        assert_eq!(tail.materialized_bytes(), 5);
        let remaining = tail
            .take_prefix(7)
            .expect("consume remaining fixture bytes");
        assert_eq!(remaining.as_bytes(), b"fghijkl");
        assert_eq!(tail.materialized_bytes(), 12);
    }

    #[test]
    fn inline_and_fragmented_prefixes_keep_owners_sequences_and_error_state() {
        let first = MutationPayload::try_copy_from_slice(b"abcdef").unwrap();
        let first_address = first.as_bytes().as_ptr();
        let mut tail = SegmentedIngestTail::default();
        tail.push(first, 9);
        tail.push(MutationPayload::try_copy_from_slice(b"ghij").unwrap(), 3);
        assert!(tail.take_prefix_fragments(11).is_err());
        assert_eq!(tail.len(), 10);
        tail.assert_valid();
        let prefix = tail.take_prefix_fragments(2).unwrap();
        assert!(matches!(prefix.parts, ChunkParts::Single(_)));
        assert_eq!(prefix.contiguous_bytes().unwrap().as_ptr(), first_address);
        assert_eq!(prefix.materialize_fixture(), b"ab");
        assert_eq!(prefix.through_sequence(), 9);
        let spanning = tail.take_prefix_fragments(5).unwrap();
        assert!(matches!(spanning.parts, ChunkParts::Fragmented(_)));
        assert_eq!(spanning.materialize_fixture(), b"cdefg");
        assert_eq!(spanning.through_sequence(), 9);
        let remaining = tail.take_prefix_fragments(3).unwrap();
        assert_eq!(remaining.materialize_fixture(), b"hij");
        assert_eq!(remaining.through_sequence(), 3);
        assert!(tail.is_empty());
        tail.assert_valid();
        drop(tail);
        assert_eq!(prefix.materialize_fixture(), b"ab");
        assert_eq!(spanning.materialize_fixture(), b"cdefg");
    }

    #[test]
    #[should_panic(expected = "cached Ingest Tail length must match its segments")]
    fn tail_boundary_audit_rejects_corrupted_cached_length() {
        let mut tail = SegmentedIngestTail::default();
        tail.push(MutationPayload::try_copy_from_slice(b"data").unwrap(), 1);
        tail.length += 1;
        tail.assert_valid();
    }

    #[test]
    fn pathological_tiny_writes_compact_before_chunk_fragment_metadata_can_grow_unbounded() {
        let mut tail = SegmentedIngestTail::default();
        let length = MAX_CHUNK_FRAGMENTS_V1 + 1;
        for ordinal in 0..length {
            tail.push(
                MutationPayload::from_owned_bytes(vec![
                    u8::try_from(ordinal % 251).expect("fixture byte fits u8"),
                ]),
                u64::try_from(ordinal + 1).expect("fixture sequence fits u64"),
            );
        }

        let chunk = tail
            .take_prefix_fragments(length)
            .expect("bounded fragment compaction succeeds");

        assert_eq!(chunk.parts.as_slice().len(), 1);
        assert_eq!(chunk.len(), length);
        assert_eq!(chunk.materialize_fixture().len(), length);
        assert_eq!(chunk.through_sequence(), u64::try_from(length).unwrap());
        assert_eq!(
            chunk.materialize_fixture(),
            (0..length)
                .map(|ordinal| u8::try_from(ordinal % 251).unwrap())
                .collect::<Vec<_>>()
        );
        assert!(tail.is_empty());
        tail.assert_valid();
    }

    #[test]
    fn fragmented_chunks_materialize_directly_into_one_compression_region() {
        let first = ChunkFragments::new(
            vec![
                MutationPayload::try_copy_from_slice(b"fragmented ").expect("allocate fixture"),
                MutationPayload::try_copy_from_slice(b"first chunk").expect("allocate fixture"),
            ],
            22,
        );
        let second = ChunkFragments::new(
            vec![
                MutationPayload::try_copy_from_slice(b" and ").expect("allocate fixture"),
                MutationPayload::try_copy_from_slice(b"second").expect("allocate fixture"),
            ],
            11,
        );
        let chunks = [
            PendingWriteThroughChunk {
                offset: 0,
                chunk_id: first.chunk_id(),
                bytes: first,
                placement: ContainerPlacement::Data,
            },
            PendingWriteThroughChunk {
                offset: 22,
                chunk_id: second.chunk_id(),
                bytes: second,
                placement: ContainerPlacement::Data,
            },
        ];
        let references = chunks.iter().collect::<Vec<_>>();

        let regions = prepare_compression_regions(
            &references,
            NonZeroUsize::new(4).unwrap(),
            &WorkerPermits::new(NonZeroUsize::new(4).unwrap()),
        )
        .expect("prepare fixture regions");

        assert!(regions.borrowed.is_empty());
        assert_eq!(regions.materialized.len(), 1);
        assert_eq!(
            regions.materialized[0].decoded,
            b"fragmented first chunk and second"
        );
        assert_eq!(regions.materialized[0].chunks[0].1, 0..22);
        assert_eq!(regions.materialized[0].chunks[1].1, 22..33);
    }

    fn materialization_fixture(count: u8) -> Vec<PendingWriteThroughChunk> {
        (0..count)
            .map(|n| {
                let bytes = vec![n; 128 * 1024];
                let parts = if n < 4 {
                    vec![MutationPayload::try_copy_from_slice(&bytes).unwrap()]
                } else {
                    vec![
                        MutationPayload::try_copy_from_slice(&bytes[..17]).unwrap(),
                        MutationPayload::try_copy_from_slice(&bytes[17..]).unwrap(),
                    ]
                };
                PendingWriteThroughChunk {
                    offset: u64::from(n) * 128 * 1024,
                    chunk_id: ChunkId::of(&bytes),
                    bytes: ChunkFragments::new(parts, 128 * 1024),
                    placement: ContainerPlacement::Data,
                }
            })
            .collect()
    }

    #[test]
    fn parallel_materialization_preserves_regions_views_and_byte_order() {
        let chunks = materialization_fixture(32);
        let references = chunks.iter().collect::<Vec<_>>();
        let admission = WorkerPermits::new(NonZeroUsize::new(4).unwrap());
        let serial =
            prepare_compression_regions(&references, NonZeroUsize::MIN, &admission).unwrap();
        let parallel =
            prepare_compression_regions(&references, NonZeroUsize::new(4).unwrap(), &admission)
                .unwrap();
        assert_eq!(serial.borrowed.len(), 1);
        assert_eq!(serial.materialized.len(), 7);
        assert_eq!(serial.order.len(), parallel.order.len());
        for (left, right) in serial.order.iter().zip(&parallel.order) {
            assert_eq!(std::mem::discriminant(left), std::mem::discriminant(right));
        }
        for (left, right) in serial.materialized.iter().zip(&parallel.materialized) {
            assert_eq!(left.decoded, right.decoded);
            assert_eq!(left.chunks, right.chunks);
        }
        assert_eq!(admission.available(), 4);
    }

    #[test]
    #[ignore = "manual release-mode Compression Region materialization A/B"]
    fn parallel_materialization_microbenchmark() {
        let chunks = materialization_fixture(255);
        let references = chunks.iter().collect::<Vec<_>>();
        let admission = WorkerPermits::new(NonZeroUsize::new(8).unwrap());
        let mut samples = [Vec::new(), Vec::new()];
        for round in 0..11 {
            for side in 0..2 {
                let side = (side + round) % 2;
                let workers = NonZeroUsize::new(if side == 0 { 1 } else { 8 }).unwrap();
                let start = Instant::now();
                std::hint::black_box(
                    prepare_compression_regions(&references, workers, &admission).unwrap(),
                );
                samples[side].push(start.elapsed());
            }
        }
        for samples in &mut samples {
            samples.sort_unstable();
        }
        println!(
            "region_materialization serial_ms={:.3} parallel_ms={:.3} speedup={:.3}",
            samples[0][5].as_secs_f64() * 1000.0,
            samples[1][5].as_secs_f64() * 1000.0,
            samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
        );
    }

    #[test]
    fn independent_ingest_lane_state_starts_on_cache_lines() {
        assert_eq!(std::mem::align_of::<WriteThroughStream>(), 64);
    }

    #[test]
    fn segmented_seqcdc_matches_contiguous_v1_boundaries() {
        let mut state = 0x5eed_cafe_1234_5678_u64;
        let mut source = vec![0_u8; 4 * 1_024 * 1_024 + 91_337];
        for byte in &mut source {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let mut expected = Vec::new();
        let mut expected_offset = 0_usize;
        while source.len() - expected_offset > CDC_MAXIMUM_BYTES {
            let length = seqcdc_cut(&source[expected_offset..], SEQCDC_CONFIG_V1);
            if length > source.len() - expected_offset - CDC_MAXIMUM_BYTES {
                break;
            }
            expected.push(source[expected_offset..expected_offset + length].to_vec());
            expected_offset += length;
        }

        let mut tail = SegmentedIngestTail::default();
        let segment_sizes = [1, 4_095, 131_071, 17, 1_048_576, 65_537];
        let mut cursor = 0_usize;
        let mut ordinal = 0_usize;
        while cursor < source.len() {
            let end = cursor
                .saturating_add(segment_sizes[ordinal % segment_sizes.len()])
                .min(source.len());
            tail.push(
                MutationPayload::try_copy_from_slice(&source[cursor..end])
                    .expect("allocate segmented fixture"),
                u64::try_from(ordinal + 1).expect("fixture sequence fits u64"),
            );
            cursor = end;
            ordinal += 1;
        }
        let mut observed = Vec::new();
        while let Some(chunk) =
            take_next_stable_seqcdc_chunk(&mut tail).expect("chunk segmented fixture")
        {
            observed.push(chunk.materialize_fixture());
        }

        assert_eq!(observed, expected);
        assert_eq!(tail.materialized_bytes(), 0);
        assert_eq!(
            tail.len(),
            source.len() - expected.iter().map(Vec::len).sum::<usize>()
        );

        let mut fragmented = SegmentedIngestTail::default();
        for (sequence, bytes) in source.chunks(4_096).enumerate() {
            fragmented.push(
                MutationPayload::try_copy_from_slice(bytes).expect("allocate fragmented fixture"),
                u64::try_from(sequence + 1).expect("fixture sequence fits u64"),
            );
        }
        let mut selected_bytes = 0_usize;
        while let Some(chunk) =
            take_next_stable_seqcdc_chunk(&mut fragmented).expect("chunk fragmented fixture")
        {
            selected_bytes = selected_bytes
                .checked_add(chunk.len())
                .expect("fixture byte count cannot overflow");
        }
        assert_ne!(
            selected_bytes, 0,
            "fragmented fixture must expose stable Chunks"
        );
        assert_eq!(
            fragmented.materialized_bytes(),
            0,
            "SeqCDC extraction and hashing retain request fragments without copying"
        );
    }

    #[test]
    fn streaming_seqcdc_matches_contiguous_v1_boundaries() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut source = vec![0_u8; 4 * 1_024 * 1_024 + 73_019];
        for byte in &mut source {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }

        let mut stream = SeqCdcStream::new(std::io::Cursor::new(source.as_slice()))
            .expect("allocate SeqCDC stream fixture");
        let mut offset = 0_usize;
        while let Some(observed) = stream.next_chunk().expect("scan SeqCDC stream fixture") {
            let expected_length = seqcdc_cut(&source[offset..], SEQCDC_CONFIG_V1);
            assert_eq!(observed, source[offset..offset + expected_length]);
            offset += expected_length;
        }
        assert_eq!(offset, source.len());
        assert_eq!(stream.consumed_bytes(), source.len() as u64);
    }

    #[test]
    fn full_registry_never_evicts_an_in_flight_ingest_lane() {
        let mut registry = WriteThroughRegistry::default();
        let mut held = Vec::new();
        for raw_inode in
            2..u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 + 2).expect("fixture lane bound fits u64")
        {
            let inode = InodeId::new(raw_inode).expect("fixture inode is nonzero");
            held.push(registry.acquire_lane(inode));
        }
        assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);

        let overflow_inode =
            u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 + 2).expect("fixture overflow inode fits u64");
        let overflow = registry
            .acquire_lane(InodeId::new(overflow_inode).expect("fixture overflow inode is nonzero"));
        assert!(Arc::ptr_eq(&overflow, &registry.overflow));
        assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);

        for raw_inode in (overflow_inode + 1)
            ..(overflow_inode
                + u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2 + 2)
                    .expect("fixture grace fits u64"))
        {
            let candidate =
                registry.acquire_lane(InodeId::new(raw_inode).expect("fixture inode is nonzero"));
            assert!(Arc::ptr_eq(&candidate, &registry.overflow));
        }
        drop(held.remove(0));
        let replacement = registry.acquire_lane(
            InodeId::new(
                overflow_inode
                    + u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2 + 2)
                        .expect("fixture grace fits u64"),
            )
            .expect("fixture replacement inode is nonzero"),
        );
        assert!(!Arc::ptr_eq(&replacement, &registry.overflow));
        assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);
        assert!(
            !registry
                .lanes
                .contains_key(&InodeId::new(2).expect("fixture inode is nonzero"))
        );
    }

    #[test]
    fn cpu_admission_uses_available_workers_during_ingest_io() {
        let budget = NonZeroUsize::new(10).unwrap();
        let permits = WorkerPermits::new(budget);
        let active = AtomicUsize::new(0);
        let _io_job = ActiveWriteThrough::enter(&active);
        let cpu = permits.acquire(budget);
        assert_eq!(cpu.workers(), budget);
        assert_eq!(permits.available(), 0);
        drop(cpu);
        assert_eq!(permits.available(), 10);
    }

    #[test]
    fn ingest_mode_transition_drains_older_batch_before_unbatched_write() {
        let queue = IngestQueue::new();
        let a = InodeId::new(2).unwrap();
        let b = InodeId::new(3).unwrap();
        let fragment = |sequence, offset| IngestWriteFragment {
            offset,
            bytes: MutationPayload::from_owned_bytes(vec![u8::try_from(sequence).unwrap(); 4096]),
            mutation_sequence: sequence,
            placement: ContainerPlacement::Data,
        };
        // One inode leaves a partial batch. A second inode changes admission
        // mode before that batch has been consumed.
        queue.enqueue_write_fragment(a, fragment(1, 0));
        queue.enqueue_write_fragment(b, fragment(1, 0));
        queue.enqueue_write_fragment(a, fragment(2, 4096));
        queue.shutdown();
        let mut sequences = Vec::new();
        while let Some(job) = queue.next_job() {
            if job.inode == a {
                sequences.push(job.mutation_sequence);
            }
            queue.finish(&job);
        }
        assert_eq!(sequences, vec![1, 2]);
        assert_eq!(queue.status().buffered_bytes, 0);
        queue.wait_through(a, 2);
    }

    #[test]
    fn ingest_mode_transition_during_backpressure_preserves_handle_order() {
        let queue = Arc::new(IngestQueue::new());
        let a = InodeId::new(2).unwrap();
        let b = InodeId::new(3).unwrap();
        queue.opened_write_handle(a);
        let payload = MutationPayload::from_owned_bytes(vec![7; 1024 * 1024]);
        for sequence in 1..=32 {
            queue.enqueue_write_fragment(
                a,
                IngestWriteFragment {
                    offset: (sequence - 1) * 1024 * 1024,
                    bytes: payload.clone(),
                    mutation_sequence: sequence,
                    placement: ContainerPlacement::Data,
                },
            );
        }
        let (entered, waiting) = mpsc::channel();
        *queue.before_fragment_wait.lock().unwrap() = Some(entered);
        let writer_queue = Arc::clone(&queue);
        let writer = std::thread::spawn(move || {
            writer_queue.enqueue_write_fragment(
                a,
                IngestWriteFragment {
                    offset: 32 * 1024 * 1024,
                    bytes: MutationPayload::from_owned_bytes(vec![8; 4096]),
                    mutation_sequence: 33,
                    placement: ContainerPlacement::Data,
                },
            );
        });
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        // The waiting writer selected batching with one writable inode. A
        // second handle changes the mode while its admission lock is released.
        queue.opened_write_handle(b);
        let first = queue.next_job().unwrap();
        queue.finish(&first);
        writer.join().unwrap();
        // Hold the batch's age below the expiry threshold deterministically.
        queue
            .state
            .lock()
            .unwrap()
            .inodes
            .get_mut(&a)
            .unwrap()
            .open
            .as_mut()
            .unwrap()
            .opened_at = Instant::now() + Duration::from_mins(1);
        for _ in 0..4 {
            let job = queue.next_job().unwrap();
            queue.finish(&job);
        }
        queue.enqueue_write_fragment(
            a,
            IngestWriteFragment {
                offset: 32 * 1024 * 1024 + 4096,
                bytes: MutationPayload::from_owned_bytes(vec![9; 4096]),
                mutation_sequence: 34,
                placement: ContainerPlacement::Data,
            },
        );
        queue.shutdown();
        let mut sequences = Vec::new();
        while let Some(job) = queue.next_job() {
            sequences.push(job.mutation_sequence);
            queue.finish(&job);
        }
        assert_eq!(sequences, vec![24, 28, 32, 33, 34]);
        queue.wait_through(a, 34);
        assert_eq!(queue.status().buffered_bytes, 0);
        queue.released_write_handle(a);
        queue.released_write_handle(b);
    }

    #[test]
    #[ignore = "manual optimized queue admission A/B benchmark"]
    fn ingest_queue_admission_benchmark() {
        let payload = MutationPayload::from_owned_bytes(vec![7; 1024 * 1024]);
        for mode in ["single", "multi"] {
            for round in 0..7 {
                let queue = IngestQueue::new();
                let a = InodeId::new(2).unwrap();
                let b = InodeId::new(3).unwrap();
                queue.opened_write_handle(a);
                if mode == "multi" {
                    queue.opened_write_handle(b);
                }
                let started = Instant::now();
                let jobs = 200_000_u64;
                let per_job = if mode == "single" { 4 } else { 1 };
                for job in 0..jobs {
                    let inode = if mode == "multi" && job % 2 == 1 {
                        b
                    } else {
                        a
                    };
                    for part in 0..per_job {
                        let sequence = job * per_job + part + 1;
                        queue.enqueue_write_fragment(
                            inode,
                            IngestWriteFragment {
                                offset: sequence * 1024 * 1024,
                                bytes: std::hint::black_box(payload.clone()),
                                mutation_sequence: sequence,
                                placement: ContainerPlacement::Data,
                            },
                        );
                    }
                    let work = queue.next_job().unwrap();
                    queue.finish(&work);
                }
                println!(
                    "queue_bench mode={mode} round={round} fragments={} elapsed_ns={}",
                    jobs * per_job,
                    started.elapsed().as_nanos()
                );
                assert_eq!(queue.status().buffered_bytes, 0);
            }
        }
    }

    #[test]
    fn ingest_queue_preserves_per_inode_sequence_order() {
        let queue = IngestQueue::new();
        let inode = InodeId::new(2).expect("fixture inode is nonzero");
        queue.enqueue_write_fragment(
            inode,
            IngestWriteFragment {
                offset: 0,
                bytes: MutationPayload::try_copy_from_slice(&[1])
                    .expect("allocate fixture payload"),
                mutation_sequence: 7,
                placement: ContainerPlacement::Data,
            },
        );
        queue.enqueue(IngestJob {
            inode,
            mutation_sequence: 8,
            kind: IngestJobKind::Truncate,
        });

        let first = queue.next_job().expect("first queued job exists");
        assert_eq!(first.mutation_sequence, 7);
        queue.finish(&first);
        let second = queue.next_job().expect("second queued job exists");
        assert_eq!(second.mutation_sequence, 8);
        queue.finish(&second);
        queue.wait_through(inode, 8);
    }

    #[test]
    fn ingest_queue_wait_ignores_metadata_only_sequence_gaps() {
        let queue = Arc::new(IngestQueue::new());
        let inode = InodeId::new(2).expect("fixture inode is nonzero");
        queue.enqueue_write_fragment(
            inode,
            IngestWriteFragment {
                offset: 0,
                bytes: MutationPayload::try_copy_from_slice(&[1])
                    .expect("allocate fixture payload"),
                mutation_sequence: 1,
                placement: ContainerPlacement::Data,
            },
        );
        let write = queue.next_job().expect("queued write exists");
        queue.finish(&write);

        let waiter = Arc::clone(&queue);
        let (completed, receive) = mpsc::channel();
        std::thread::spawn(move || {
            waiter.wait_through(inode, 2);
            completed.send(()).expect("test receiver remains live");
        });
        receive
            .recv_timeout(Duration::from_millis(100))
            .expect("Metadata-only sequence gaps have no missing ingest work");
    }

    #[test]
    #[should_panic(expected = "per-inode ingest admission sequence cannot move backwards")]
    fn ingest_queue_asserts_on_decreasing_inode_sequence() {
        let queue = IngestQueue::new();
        let inode = InodeId::new(2).expect("fixture inode is nonzero");
        for mutation_sequence in [8, 7] {
            queue.enqueue(IngestJob {
                inode,
                mutation_sequence,
                kind: IngestJobKind::Truncate,
            });
        }
    }

    #[test]
    fn encode_worker_permits_cannot_overbook_the_write_through_budget() {
        let budget = NonZeroUsize::new(10).expect("fixture budget is nonzero");
        let permits = WorkerPermits::new(budget);
        let telemetry = CpuPhaseTelemetry::default();
        let first = permits.acquire(NonZeroUsize::new(6).expect("fixture share is nonzero"));
        telemetry.record_permit(&first);
        let first_phase = telemetry.begin();
        let second = permits.acquire(NonZeroUsize::new(10).expect("fixture share is nonzero"));
        telemetry.record_permit(&second);
        let second_phase = telemetry.begin();
        assert_eq!(first.workers().get(), 6);
        assert_eq!(second.workers().get(), 4);
        assert_eq!(first.requested_workers().get(), 6);
        assert_eq!(second.requested_workers().get(), 10);
        assert!(!first.blocked());
        assert!(!second.blocked());
        let active = telemetry.status();
        assert_eq!(active.phases(), 2);
        assert_eq!(active.active(), 2);
        assert_eq!(active.maximum_active(), 2);
        assert_eq!(active.requested_workers(), 16);
        assert_eq!(active.granted_workers(), 10);
        assert_eq!(active.partial_grants(), 1);
        assert_eq!(active.permit_blocked_phases(), 0);
        assert_eq!(permits.available(), 0);
        drop(second_phase);
        drop(first_phase);
        drop(second);
        drop(first);
        let completed = telemetry.status();
        assert_eq!(completed.active(), 0);
        assert!(completed.runnable_wall_ns() > 0);
        assert!(completed.maximum_permit_wait_ns() <= completed.permit_wait_ns());
        assert_eq!(permits.available(), budget.get());
    }

    #[test]
    #[should_panic(expected = "requested encode workers exceed the write-through worker budget")]
    fn encode_worker_permits_assert_on_an_impossible_request() {
        let permits = WorkerPermits::new(NonZeroUsize::new(10).expect("fixture is nonzero"));
        let _lease = permits.acquire(NonZeroUsize::new(11).expect("fixture is nonzero"));
    }

    #[test]
    #[should_panic(expected = "pending write-through byte accounting must be exact")]
    fn pending_write_through_accounting_asserts_at_the_planner_boundary() {
        let chunks = vec![PendingWriteThroughChunk {
            offset: 0,
            chunk_id: ChunkId::of(&[1, 2, 3]),
            bytes: ChunkFragments::new(vec![MutationPayload::from_owned_bytes(vec![1, 2, 3])], 3),
            placement: ContainerPlacement::Data,
        }];
        DetachedContainerWork::new(InodeId::new(2).unwrap(), 1, chunks, 2);
    }

    #[test]
    #[should_panic(expected = "one Ingest Lane exceeded one Container plus CDC suffix")]
    fn stable_lane_asserts_on_an_impossible_buffer_overshoot() {
        let mut tail = SegmentedIngestTail::default();
        tail.push(
            MutationPayload::try_copy_from_slice(&vec![
                0_u8;
                CONTAINER_PAYLOAD_TARGET_BYTES
                    + CDC_MAXIMUM_BYTES
                    + 1
            ])
            .expect("allocate oversized fixture payload"),
            1,
        );
        let state = WriteThroughStream {
            tail,
            ..WriteThroughStream::default()
        };
        assert_bounded_write_through_lane(&state);
    }
}
