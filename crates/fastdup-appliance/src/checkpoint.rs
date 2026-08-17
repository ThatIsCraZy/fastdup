use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use fastcdc::v2020::{FastCDC, Normalization, StreamCDC};
use fastdup_format::{
    ChunkId, CommitRecord, ContainerId, DurableInode, ExactIndexEntry, ExactIndexProfileId,
    ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, MAX_LOGICAL_CHUNK_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, MetadataObjectId, NamespaceEntry, NamespaceRoot,
    PolicySetId,
};
use fastdup_posix::{
    CommitInode, CommitRange, CommittedFile, CommittedFileInstall, ExternalizedExtent, InodeId,
    MutationObserver, Namespace, NamespaceCommit, NamespaceConfig, PosixError,
};
use fastdup_store::{
    ActivatedExactIndex, AdaptiveContainerPublishMetrics, ContainerRepository,
    ExactIndexRunRepository, GenerationError, GenerationRepository, IndexedRequiredChunkVerifier,
    ManifestReadError, RequiredChunkVerifier, StorageIo, StoreError, VerifiedCommittedFile,
    VerifiedManifestFile,
};

use crate::{ManifestCommittedFile, MountError, namespace_from_verified_files_using};

const FIRST_REGULAR_INODE: u64 = 2;
const CONTAINER_PAYLOAD_TARGET_BYTES: usize = 32 * 1_024 * 1_024;
const CONTAINER_PAYLOAD_FLUSH_BYTES: usize = CONTAINER_PAYLOAD_TARGET_BYTES - CDC_MAXIMUM_BYTES;
const COMPRESSION_REGION_TARGET_BYTES: usize = 512 * 1_024;
const CDC_MINIMUM_BYTES: usize = 16 * 1_024;
const CDC_TARGET_BYTES: usize = 64 * 1_024;
const CDC_MAXIMUM_BYTES: usize = 256 * 1_024;
const CDC_SEED_V1: u64 = 0;
const EXACT_INDEX_COMPACTION_FANIN: usize = 4;
const MAX_CHECKPOINT_WORKERS: usize =
    CONTAINER_PAYLOAD_TARGET_BYTES / COMPRESSION_REGION_TARGET_BYTES;
const WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1: usize = 384 * 1_024 * 1_024;
const MAX_ACTIVE_INGEST_LANES_V1: usize =
    WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1 / (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES) - 1;
const _: () = assert!(
    WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1
        >= 2 * (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES)
);

/// V1 scheduler high-water for active checkpointable DATA.
///
/// This is deliberately expressed from the durable format's 64-MiB maximum
/// Container size rather than the adaptive writer's current 32-MiB payload
/// target. Reaching it starts an early checkpoint and applies admission
/// backpressure until durable progress catches up.
pub const CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1: u64 = 8 * fastdup_format::MAX_CONTAINER_BYTES;

/// Bounded process-local state of the pre-commit reduction pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteThroughStatus {
    buffered_bytes: u64,
    active_lanes: u64,
    sealed_uncommitted_containers: u64,
    oldest_sealed_age: Option<Duration>,
    degraded: bool,
}

impl WriteThroughStatus {
    #[must_use]
    pub const fn buffered_bytes(self) -> u64 {
        self.buffered_bytes
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
    manifests: Mutex<Vec<InstalledManifest>>,
    next_container_generation: Arc<Mutex<u64>>,
    manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
    checkpoint_workers: NonZeroUsize,
    write_through: Arc<WriteThroughIngest<C>>,
}

#[derive(Clone, Copy, Debug)]
struct InstalledManifest {
    inode: u64,
    root: MetadataObjectId,
    logical_size: u64,
    allocated_bytes: u64,
}

struct VerifiedLocationFile<C> {
    containers: ContainerRepository<C>,
    entry: ExactIndexEntry,
}

impl<C> fmt::Debug for VerifiedLocationFile<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedLocationFile")
            .field("chunk_id", &self.entry.chunk_id())
            .field("logical_length", &self.entry.logical_length())
            .field("location", &self.entry.location())
            .finish_non_exhaustive()
    }
}

impl<C> CommittedFile for VerifiedLocationFile<C>
where
    C: Send + Sync + StorageIo,
{
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
        let logical_size = self.logical_size();
        let start = offset.min(logical_size);
        let end = offset.saturating_add(u64::from(length)).min(logical_size);
        if start == end {
            return Ok(Vec::new());
        }
        let bytes = self
            .containers
            .read_verified_location(self.entry)
            .map_err(|_| PosixError::Io)?;
        if bytes.len()
            != usize::try_from(logical_size).expect("ASSERT: Exact Index length fits usize")
        {
            return Err(PosixError::Io);
        }
        let slice_start = usize::try_from(start).expect("ASSERT: Chunk offset fits usize");
        let slice_end = usize::try_from(end).expect("ASSERT: Chunk end fits usize");
        let mut output = Vec::new();
        output
            .try_reserve_exact(slice_end - slice_start)
            .map_err(|_| PosixError::OutOfMemory)?;
        output.extend_from_slice(&bytes[slice_start..slice_end]);
        Ok(output)
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        Ok(candidate.len()
            == usize::try_from(self.entry.logical_length())
                .expect("ASSERT: Exact Index length fits usize")
            && ChunkId::of(candidate) == self.entry.chunk_id())
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
}

#[derive(Debug)]
struct PendingWriteThroughChunk {
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct WriteThroughStream {
    inode: Option<InodeId>,
    last_mutation_sequence: Option<u64>,
    next_offset: u64,
    tail_offset: u64,
    tail: Vec<u8>,
    pending_chunks: Vec<PendingWriteThroughChunk>,
    pending_bytes: usize,
}

#[derive(Debug, Default)]
struct WriteThroughRegistry {
    lanes: BTreeMap<InodeId, WriteThroughLane>,
    overflow: Arc<Mutex<WriteThroughStream>>,
    sealed: VecDeque<Instant>,
    degraded: bool,
    next_touch: u64,
}

#[derive(Debug)]
struct WriteThroughLane {
    stream: Arc<Mutex<WriteThroughStream>>,
    last_touch: u64,
}

impl WriteThroughRegistry {
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
    next_generation: Arc<Mutex<u64>>,
    index: Arc<dyn ManifestReaderPolicy<C>>,
    worker_budget: NonZeroUsize,
    worker_permits: WorkerPermits,
    active_writers: AtomicUsize,
    registry: Mutex<WriteThroughRegistry>,
}

struct WorkerPermits {
    total: NonZeroUsize,
    available: Mutex<usize>,
    changed: Condvar,
}

impl WorkerPermits {
    fn new(total: NonZeroUsize) -> Self {
        Self {
            total,
            available: Mutex::new(total.get()),
            changed: Condvar::new(),
        }
    }

    fn acquire(&self, desired: NonZeroUsize) -> WorkerPermitLease<'_> {
        assert!(
            desired.get() <= self.total.get(),
            "ASSERT: requested encode workers exceed the write-through worker budget"
        );
        let mut available = self
            .available
            .lock()
            .expect("ASSERT: encode worker permit lock poisoned");
        while *available == 0 {
            available = self
                .changed
                .wait(available)
                .expect("ASSERT: encode worker permit lock poisoned while waiting");
        }
        assert!(
            *available <= self.total.get(),
            "ASSERT: available encode workers exceed the write-through worker budget"
        );
        let acquired = desired.get().min(*available);
        assert!(acquired != 0, "ASSERT: a granted worker lease is nonempty");
        *available -= acquired;
        WorkerPermitLease {
            pool: self,
            acquired: NonZeroUsize::new(acquired)
                .expect("ASSERT: a granted worker lease is nonempty"),
        }
    }
}

struct WorkerPermitLease<'a> {
    pool: &'a WorkerPermits,
    acquired: NonZeroUsize,
}

impl WorkerPermitLease<'_> {
    const fn workers(&self) -> NonZeroUsize {
        self.acquired
    }
}

impl Drop for WorkerPermitLease<'_> {
    fn drop(&mut self) {
        let mut available = self
            .pool
            .available
            .lock()
            .expect("ASSERT: encode worker permit lock poisoned during retirement");
        *available = available
            .checked_add(self.acquired.get())
            .expect("ASSERT: encode worker permit accounting cannot overflow");
        assert!(
            *available <= self.pool.total.get(),
            "ASSERT: encode worker retirement exceeded the write-through worker budget"
        );
        self.pool.changed.notify_all();
    }
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

fn workers_per_ingest_job(worker_budget: NonZeroUsize, active_jobs: usize) -> NonZeroUsize {
    assert!(
        active_jobs != 0,
        "ASSERT: worker sharing requires one active write-through job"
    );
    let jobs = active_jobs;
    NonZeroUsize::new((worker_budget.get() / jobs).max(1))
        .expect("ASSERT: clamped worker share is nonzero")
}

fn assert_pending_write_through_state(state: &WriteThroughStream) {
    let mut summed_bytes = 0_usize;
    let mut previous_end = None;
    for pending in &state.pending_chunks {
        assert!(
            !pending.bytes.is_empty() && pending.bytes.len() <= CDC_MAXIMUM_BYTES,
            "ASSERT: pending write-through Chunk violates FastCDC length bounds"
        );
        let end = pending
            .offset
            .checked_add(
                u64::try_from(pending.bytes.len()).expect("ASSERT: pending Chunk length fits u64"),
            )
            .expect("ASSERT: pending write-through Chunk range cannot overflow");
        assert!(
            previous_end.is_none_or(|previous| previous <= pending.offset),
            "ASSERT: pending write-through Chunks must be ordered and disjoint"
        );
        previous_end = Some(end);
        summed_bytes = summed_bytes
            .checked_add(pending.bytes.len())
            .expect("ASSERT: bounded pending write-through bytes cannot overflow");
    }
    assert_eq!(
        summed_bytes, state.pending_bytes,
        "ASSERT: pending write-through byte accounting must be exact"
    );
    assert_eq!(
        state.pending_chunks.is_empty(),
        state.pending_bytes == 0,
        "ASSERT: pending write-through count and bytes must agree"
    );
    assert!(
        state.pending_bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
        "ASSERT: pending write-through payload exceeds its Container bound"
    );
}

fn assert_bounded_write_through_lane(state: &WriteThroughStream) {
    assert_pending_write_through_state(state);
    let buffered = state
        .tail
        .len()
        .checked_add(state.pending_bytes)
        .expect("ASSERT: bounded Ingest Lane bytes cannot overflow");
    assert!(
        buffered <= CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
        "ASSERT: one Ingest Lane exceeded one Container plus CDC suffix: tail={} pending={} buffered={} bound={}",
        state.tail.len(),
        state.pending_bytes,
        buffered,
        CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
    );
}

impl<C> fmt::Debug for WriteThroughIngest<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        let buffered_bytes = registry.lanes.values().fold(0_usize, |total, lane| {
            let lane = lane
                .stream
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending_bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = registry
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let buffered_bytes = buffered_bytes
            .checked_add(overflow.tail.len())
            .and_then(|sum| sum.checked_add(overflow.pending_bytes))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        assert!(
            buffered_bytes <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        formatter
            .debug_struct("WriteThroughIngest")
            .field("active_lanes", &registry.lanes.len())
            .field("buffered_bytes", &buffered_bytes)
            .field("sealed_uncommitted", &registry.sealed.len())
            .field(
                "active_writers",
                &self.active_writers.load(Ordering::Relaxed),
            )
            .field("degraded", &registry.degraded)
            .finish_non_exhaustive()
    }
}

impl<C> WriteThroughIngest<C>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    fn status(&self) -> WriteThroughStatus {
        let registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        let buffered = registry.lanes.values().fold(0_usize, |total, lane| {
            let lane = lane
                .stream
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending_bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = registry
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let buffered = buffered
            .checked_add(overflow.tail.len())
            .and_then(|sum| sum.checked_add(overflow.pending_bytes))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        assert!(
            buffered <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        WriteThroughStatus {
            buffered_bytes: u64::try_from(buffered).expect("ASSERT: process buffers fit in u64"),
            active_lanes: u64::try_from(registry.lanes.len())
                .expect("ASSERT: bounded Ingest Lane count fits u64"),
            sealed_uncommitted_containers: u64::try_from(registry.sealed.len())
                .expect("ASSERT: process Container count fits in u64"),
            oldest_sealed_age: registry.sealed.front().map(Instant::elapsed),
            degraded: registry.degraded,
        }
    }

    fn capture_cut(&self) -> usize {
        self.registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .sealed
            .len()
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
        // Preserve the incomplete FastCDC suffix across generations. It is the
        // exact content anchor required for a later append to make the same
        // cuts as the checkpoint planner. Chunks the checkpoint published from
        // this suffix are filtered through the newly active Exact Index before
        // the write-through path publishes its next Container.
    }

    fn stage_write(
        &self,
        inode: InodeId,
        offset: u64,
        through_sequence: u64,
        bytes: &[u8],
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        let lane = self.lane_for(inode);
        let mut lane = lane
            .lock()
            .expect("ASSERT: write-through lane lock poisoned");
        let discontinuous = lane.inode != Some(inode)
            || lane.next_offset != offset
            || lane
                .last_mutation_sequence
                .is_some_and(|previous| through_sequence <= previous);
        if discontinuous {
            lane.inode = Some(inode);
            lane.tail_offset = offset;
            lane.tail.clear();
            lane.pending_chunks.clear();
            lane.pending_bytes = 0;
        }
        assert_bounded_write_through_lane(&lane);
        lane.last_mutation_sequence = Some(through_sequence);
        lane.next_offset = offset
            .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"))
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        lane.tail
            .try_reserve(bytes.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        lane.tail.extend_from_slice(bytes);
        let mut externalized = Vec::new();
        let mut sealed = 0_usize;
        loop {
            externalized.extend(self.extract_stable_chunks(&mut lane, inode, through_sequence)?);
            if lane.pending_bytes < CONTAINER_PAYLOAD_FLUSH_BYTES {
                break;
            }
            assert!(
                lane.pending_bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
                "ASSERT: write-through payload exceeded its pre-format Container bound"
            );
            externalized.extend(self.publish_pending(&mut lane, inode, through_sequence)?);
            sealed = sealed
                .checked_add(1)
                .expect("ASSERT: one bounded write cannot seal usize::MAX Containers");
        }
        assert_bounded_write_through_lane(&lane);
        drop(lane);
        if sealed != 0 {
            let mut registry = self
                .registry
                .lock()
                .expect("ASSERT: write-through registry lock poisoned");
            registry
                .sealed
                .try_reserve(sealed)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            for _ in 0..sealed {
                registry.sealed.push_back(Instant::now());
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

    fn extract_stable_chunks(
        &self,
        state: &mut WriteThroughStream,
        inode: InodeId,
        through_sequence: u64,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        assert_pending_write_through_state(state);
        let stable_before = state.tail.len().saturating_sub(CDC_MAXIMUM_BYTES);
        let stable_required = CONTAINER_PAYLOAD_FLUSH_BYTES
            .checked_sub(state.pending_bytes)
            .expect("ASSERT: pending bytes remain below the flush threshold");
        if stable_before < stable_required {
            return Ok(Vec::new());
        }
        let ranges = FastCDC::with_level_and_seed(
            &state.tail,
            CDC_MINIMUM_BYTES,
            CDC_TARGET_BYTES,
            CDC_MAXIMUM_BYTES,
            Normalization::Level1,
            CDC_SEED_V1,
        )
        .take_while(|chunk| chunk.offset + chunk.length <= stable_before)
        .map(|chunk| (chunk.offset, chunk.length))
        .collect::<Vec<_>>();
        let Some(_) = ranges.last() else {
            return Ok(Vec::new());
        };
        state
            .pending_chunks
            .try_reserve(ranges.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut externalized = Vec::new();
        externalized
            .try_reserve(ranges.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut consumed = 0_usize;
        for (chunk_offset, chunk_length) in ranges {
            let chunk_end = chunk_offset
                .checked_add(chunk_length)
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            let chunk = &state.tail[chunk_offset..chunk_end];
            let logical_offset = state
                .tail_offset
                .checked_add(
                    u64::try_from(chunk_offset).expect("ASSERT: bounded Chunk offset fits u64"),
                )
                .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
            if chunk.iter().all(|byte| *byte == chunk[0]) {
                externalized.push(ExternalizedExtent::new(
                    inode,
                    logical_offset,
                    through_sequence,
                    Arc::new(FillCommittedFile {
                        value: chunk[0],
                        length: u64::try_from(chunk.len())
                            .expect("ASSERT: bounded FastCDC Chunk length fits u64"),
                    }),
                )?);
                consumed = chunk_end;
                continue;
            }
            let chunk_id = ChunkId::of(chunk);
            let logical_length =
                u64::try_from(chunk.len()).expect("ASSERT: bounded FastCDC Chunk length fits u64");
            if let Some(entry) =
                self.index
                    .verified_location(&self.containers, chunk_id, logical_length)
            {
                externalized.push(self.externalized_location(
                    inode,
                    logical_offset,
                    through_sequence,
                    entry,
                )?);
                consumed = chunk_end;
                continue;
            }
            state.pending_bytes = state
                .pending_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            state.pending_chunks.push(PendingWriteThroughChunk {
                offset: logical_offset,
                bytes: chunk.to_vec(),
            });
            consumed = chunk_end;
            if state.pending_bytes >= CONTAINER_PAYLOAD_FLUSH_BYTES {
                break;
            }
        }
        assert!(
            consumed != 0,
            "ASSERT: one stable FastCDC Chunk was consumed"
        );
        state.tail.drain(..consumed);
        state.tail_offset = state
            .tail_offset
            .checked_add(u64::try_from(consumed).expect("ASSERT: bounded drain fits u64"))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        assert_pending_write_through_state(state);
        Ok(externalized)
    }

    fn publish_pending(
        &self,
        state: &mut WriteThroughStream,
        inode: InodeId,
        through_sequence: u64,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        assert_pending_write_through_state(state);
        if state.pending_chunks.is_empty() {
            state.pending_bytes = 0;
            return Ok(Vec::new());
        }
        let mut regions = Vec::<Vec<&[u8]>>::new();
        let mut region_bytes = 0_usize;
        for chunk in &state.pending_chunks {
            if region_bytes != 0
                && region_bytes
                    .checked_add(chunk.bytes.len())
                    .ok_or(DurableNamespaceError::OutOfMemory)?
                    > COMPRESSION_REGION_TARGET_BYTES
            {
                region_bytes = 0;
            }
            if region_bytes == 0 {
                regions.push(Vec::new());
            }
            regions
                .last_mut()
                .expect("ASSERT: active region exists")
                .push(chunk.bytes.as_slice());
            region_bytes = region_bytes
                .checked_add(chunk.bytes.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
        }
        let region_refs = regions.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let generation = reserve_container_generation(&self.next_generation)?;
        let active_writers = self.active_writers.load(Ordering::Acquire);
        let desired_workers = workers_per_ingest_job(self.worker_budget, active_writers);
        let worker_lease = self.worker_permits.acquire(desired_workers);
        let workers = worker_lease.workers();
        assert!(
            workers.get() <= self.worker_budget.get(),
            "ASSERT: one encode job cannot exceed the write-through worker budget"
        );
        let (verified, _) = self.containers.publish_adaptive_regions_parallel_profiled(
            random_container_id()?,
            generation,
            &region_refs,
            workers,
        )?;
        drop(worker_lease);
        let entries: Vec<ExactIndexEntry> = verified
            .locations()
            .iter()
            .copied()
            .map(|location| {
                ExactIndexEntry::from_verified(location)
                    .expect("ASSERT: verified write-through Location forms an Exact Index entry")
            })
            .collect();
        let mut locations = BTreeMap::new();
        for entry in &entries {
            if let Some(previous) = locations.insert(entry.chunk_id(), *entry) {
                assert_eq!(
                    previous.logical_length(),
                    entry.logical_length(),
                    "ASSERT: one Chunk ID cannot identify two logical lengths"
                );
            }
        }
        let mut externalized = Vec::new();
        externalized
            .try_reserve(state.pending_chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for pending in &state.pending_chunks {
            let chunk_id = ChunkId::of(&pending.bytes);
            let entry = locations
                .get(&chunk_id)
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
            externalized.push(self.externalized_location(
                inode,
                pending.offset,
                through_sequence,
                entry,
            )?);
        }
        self.index.publish_level_zero(entries);
        state.pending_chunks.clear();
        state.pending_bytes = 0;
        assert_pending_write_through_state(state);
        Ok(externalized)
    }

    fn externalized_location(
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
                containers: self.containers.clone(),
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
    fn accepted_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        bytes: &[u8],
    ) -> Vec<ExternalizedExtent> {
        let error = match self.stage_write(inode, offset, mutation_sequence, bytes) {
            Ok(externalized) => return externalized,
            Err(error) => error,
        };
        eprintln!(
            "write-through staging degraded; resident fallback retained: inode={} offset={offset} length={} sequence={mutation_sequence} error={error:?}",
            inode.get(),
            bytes.len(),
        );
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        registry.degraded = true;
        registry.lanes.remove(&inode);
        let mut overflow = registry
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        if overflow.inode == Some(inode) {
            *overflow = WriteThroughStream::default();
        }
        Vec::new()
    }

    fn accepted_truncate(&self, inode: InodeId, _mutation_sequence: u64, _length: u64) {
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        registry.lanes.remove(&inode);
        let mut overflow = registry
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        if overflow.inode == Some(inode) {
            *overflow = WriteThroughStream::default();
        }
    }
}

trait ManifestReaderPolicy<C>: fmt::Debug + Send + Sync {
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C>;
    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier>;
    fn exact_index_run_count(&self) -> usize;
    fn verified_location(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<ExactIndexEntry>;
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

    fn verified_location(
        &self,
        _containers: &ContainerRepository<C>,
        _chunk_id: ChunkId,
        _logical_length: u64,
    ) -> Option<ExactIndexEntry> {
        None
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
    publish_lock: Mutex<()>,
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

    fn verified_location(
        &self,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<ExactIndexEntry> {
        let active = self
            .active
            .read()
            .expect("ASSERT: active Exact Index reader lock poisoned")
            .clone();
        let active = active?;
        if let Ok(location) =
            containers.find_verified_location_with_index(&active, chunk_id, logical_length)
        {
            location
        } else {
            self.degraded.store(true, Ordering::Release);
            None
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
        let _publisher = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index generation publisher lock poisoned");
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
            publish_lock: Mutex::new(()),
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
        let next_container_generation =
            Arc::new(Mutex::new(discover_next_container_generation(&containers)?));
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
        let checkpoint_workers =
            NonZeroUsize::new(available_workers.get().min(MAX_CHECKPOINT_WORKERS))
                .expect("ASSERT: the checkpoint worker cap is nonzero");
        let write_through = Arc::new(WriteThroughIngest {
            containers: containers.clone(),
            next_generation: Arc::clone(&next_container_generation),
            index: Arc::clone(&manifest_readers),
            worker_budget: checkpoint_workers,
            worker_permits: WorkerPermits::new(checkpoint_workers),
            active_writers: AtomicUsize::new(0),
            registry: Mutex::new(WriteThroughRegistry::default()),
        });
        let namespace = Arc::new(namespace);
        namespace.install_mutation_observer(write_through.clone());
        Ok(Self {
            namespace,
            generations,
            containers,
            checkpoint_lock: Mutex::new(()),
            manifests: Mutex::new(manifests),
            next_container_generation,
            manifest_readers,
            checkpoint_workers,
            write_through,
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

    /// Returns bounded live state of the pre-commit FastCDC/Container pipeline.
    #[must_use]
    pub fn write_through_status(&self) -> WriteThroughStatus {
        self.write_through.status()
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
        let sealed_at_cut = self.write_through.capture_cut();
        let freeze_started = PhaseStarted::now();
        let Some(commit) = self.namespace.begin_commit()? else {
            return Ok(None);
        };
        let mut metrics = CheckpointMetrics::default();
        freeze_started.finish_into(&mut metrics.freeze);
        let mut writer = AdaptiveCommitWriter::new(
            &self.containers,
            &self.next_container_generation,
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
        let changed_dependencies = collect_planned_dependencies(&manifests)?;
        drop(installed_manifests);
        let (level_zero_entries, reduction_metrics) = writer.finish()?;
        manifest_plan_started.finish_into(&mut metrics.manifest_plan);
        metrics.merge_reduction(&reduction_metrics);
        let exact_index_started = PhaseStarted::now();
        self.manifest_readers.publish_level_zero(level_zero_entries);
        exact_index_started.finish_into(&mut metrics.exact_index_publish);
        let metadata_started = PhaseStarted::now();
        let record = self.publish_generation(&commit, manifests, &changed_dependencies)?;
        self.write_through.complete_cut(sealed_at_cut);
        metadata_started.finish_into(&mut metrics.metadata_commit);
        total_started.finish_into(&mut metrics.total);
        Ok(Some(ProfiledCheckpoint { record, metrics }))
    }

    fn publish_manifest_plans(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
    ) -> Result<(Vec<DurableInode>, Vec<InstalledManifest>), DurableNamespaceError> {
        if manifests.len() != commit.inodes().len() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let mut durable_inodes = Vec::new();
        let mut next_manifests = Vec::new();
        durable_inodes
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        next_manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, publication) in commit.inodes().iter().zip(manifests) {
            let manifest_root = match &publication {
                ManifestPublication::Reuse { root, .. } => *root,
                ManifestPublication::Replace {
                    previous_root,
                    logical_size,
                    replacements,
                    ..
                } => {
                    let mut root = *previous_root;
                    for replacement in replacements {
                        root = self.generations.publish_manifest_replacement(
                            root,
                            *logical_size,
                            replacement.replaced.clone(),
                            &replacement.extents,
                        )?;
                    }
                    root
                }
                ManifestPublication::Complete { manifest, .. } => {
                    self.generations.publish_manifest(manifest)?
                }
            };
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
            next_manifests.push(InstalledManifest {
                inode: inode.inode().get(),
                root: manifest_root,
                logical_size: inode.logical_size(),
                allocated_bytes: inode.allocated_bytes(),
            });
        }
        Ok((durable_inodes, next_manifests))
    }

    fn publish_generation(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
        changed_dependencies: &BTreeMap<ChunkId, u64>,
    ) -> Result<CommitRecord, DurableNamespaceError> {
        let (durable_inodes, next_manifests) = self.publish_manifest_plans(commit, manifests)?;
        let mut installs = Vec::new();
        installs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
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
        for (ordinal, ((inode, verified), planned_manifest)) in commit
            .inodes()
            .iter()
            .zip(verified_files)
            .zip(&next_manifests)
            .enumerate()
        {
            assert_eq!(
                verified.inode(),
                inode.inode().get(),
                "ASSERT: committed DATA proof order must match the Namespace Root"
            );
            assert_eq!(
                verified.logical_size(),
                planned_manifest.logical_size,
                "ASSERT: published Manifest reread length must equal the planned Manifest"
            );
            assert_eq!(
                verified.manifest_root(),
                Some(root.inodes()[ordinal].manifest_root()),
                "ASSERT: committed DATA proof must retain the published Manifest Root"
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

enum ManifestPublication {
    Reuse {
        root: MetadataObjectId,
    },
    Replace {
        previous_root: MetadataObjectId,
        logical_size: u64,
        replacements: Vec<ManifestReplacement>,
        dependencies: BTreeMap<ChunkId, u64>,
    },
    Complete {
        manifest: ManifestLeaf,
        dependencies: BTreeMap<ChunkId, u64>,
    },
}

struct ManifestReplacement {
    replaced: Range<u64>,
    extents: Vec<ManifestExtent>,
}

fn collect_planned_dependencies(
    plans: &[ManifestPublication],
) -> Result<BTreeMap<ChunkId, u64>, DurableNamespaceError> {
    let mut all = BTreeMap::new();
    for dependencies in plans.iter().filter_map(|plan| match plan {
        ManifestPublication::Reuse { .. } => None,
        ManifestPublication::Replace { dependencies, .. }
        | ManifestPublication::Complete { dependencies, .. } => Some(dependencies),
    }) {
        for (chunk_id, logical_length) in dependencies {
            insert_chunk_dependency(&mut all, *chunk_id, *logical_length)?;
        }
    }
    Ok(all)
}

fn insert_chunk_dependency(
    dependencies: &mut BTreeMap<ChunkId, u64>,
    chunk_id: ChunkId,
    logical_length: u64,
) -> Result<(), DurableNamespaceError> {
    if let Some(previous_length) = dependencies.insert(chunk_id, logical_length)
        && previous_length != logical_length
    {
        return Err(DurableNamespaceError::ChunkLengthConflict {
            chunk_id,
            first_length: previous_length,
            second_length: logical_length,
        });
    }
    Ok(())
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
        insert_chunk_dependency(changed, chunk_id, logical_length)?;
    }
    Ok(())
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
                root: previous.root,
            });
        }
        return plan_path_local_manifest(inode, previous, &changed, generations, writer);
    }

    let previous_manifest = match previous {
        Some(previous) if previous.logical_size <= logical_size => {
            Some(generations.read_manifest(previous.root)?)
        }
        Some(_) | None => None,
    };
    let manifest = plan_manifest(inode, previous_manifest.as_ref(), writer)?;
    let mut dependencies = BTreeMap::new();
    collect_changed_manifest_dependencies(
        previous_manifest.as_ref(),
        &manifest,
        &mut dependencies,
    )?;
    Ok(ManifestPublication::Complete {
        manifest,
        dependencies,
    })
}

fn plan_path_local_manifest<M: StorageIo, C: StorageIo>(
    inode: &CommitInode,
    previous: InstalledManifest,
    changed: &[CommitRange],
    generations: &GenerationRepository<M>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    assert_eq!(
        previous.logical_size,
        inode.logical_size(),
        "ASSERT: path-local replacement preserves the complete logical length"
    );
    let mut rewrites = rewrite_ranges_before(changed, inode.logical_size(), inode.logical_size())?;
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

    let mut replacements = Vec::new();
    replacements
        .try_reserve_exact(rewrites.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut dependencies = BTreeMap::new();
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
        stack.push((rewrite.start, rewrite.end - rewrite.start));
        plan_manifest_ranges(inode, writer, &mut stack, &mut extents)?;
        assert!(
            stack.is_empty(),
            "ASSERT: path-local range planner must consume its complete work stack"
        );
        let mut verified_extents = Vec::new();
        verified_extents
            .try_reserve_exact(extents.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        verified_extents.extend_from_slice(&extents);
        ManifestLeaf::new(rewrite.end - rewrite.start, verified_extents)?;
        let added = extents.iter().try_fold(0_u64, |total, extent| {
            if matches!(extent, ManifestExtent::Hole { .. }) {
                Ok(total)
            } else {
                total
                    .checked_add(extent_length(extent))
                    .ok_or(DurableNamespaceError::ArithmeticOverflow)
            }
        })?;
        allocated_bytes = allocated_bytes
            .checked_sub(removed)
            .and_then(|remaining| remaining.checked_add(added))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        collect_extent_dependencies(&extents, &mut dependencies)?;
        replacements.push(ManifestReplacement {
            replaced: rewrite.start..rewrite.end,
            extents,
        });
    }
    if allocated_bytes != inode.allocated_bytes() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(ManifestPublication::Replace {
        previous_root: previous.root,
        logical_size: previous.logical_size,
        replacements,
        dependencies,
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

fn collect_extent_dependencies(
    extents: &[ManifestExtent],
    dependencies: &mut BTreeMap<ChunkId, u64>,
) -> Result<(), DurableNamespaceError> {
    for extent in extents {
        if let ManifestExtent::Data {
            logical_length,
            chunk_id,
        } = *extent
        {
            insert_chunk_dependency(dependencies, chunk_id, logical_length)?;
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
    files: &[VerifiedCommittedFile<I>],
) -> Result<Vec<InstalledManifest>, DurableNamespaceError> {
    if root.inodes().len() != files.len() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let mut manifests: Vec<InstalledManifest> = Vec::new();
    manifests
        .try_reserve_exact(root.inodes().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for (inode, file) in root.inodes().iter().zip(files) {
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
        manifests.push(InstalledManifest {
            inode: inode.inode(),
            root: inode.manifest_root(),
            logical_size: inode.logical_size(),
            allocated_bytes: file.allocated_bytes(),
        });
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
    next_generation: &'a Mutex<u64>,
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
        next_generation: &'a Mutex<u64>,
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
            .verified_location(self.containers, chunk_id, length)
            .is_some()
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
        let generation = reserve_container_generation(self.next_generation)?;
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

fn reserve_container_generation(allocator: &Mutex<u64>) -> Result<u64, DurableNamespaceError> {
    let mut next = allocator
        .lock()
        .expect("ASSERT: Container generation allocator lock poisoned");
    let generation = *next;
    *next = generation
        .checked_add(1)
        .ok_or(DurableNamespaceError::ContainerGenerationExhausted)?;
    Ok(generation)
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

impl From<MountError> for DurableNamespaceError {
    fn from(error: MountError) -> Self {
        Self::Mount(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn active_ingest_jobs_share_one_global_worker_budget() {
        let budget = NonZeroUsize::new(10).expect("fixture budget is nonzero");
        assert_eq!(workers_per_ingest_job(budget, 1).get(), 10);
        assert_eq!(workers_per_ingest_job(budget, 2).get(), 5);
        assert_eq!(workers_per_ingest_job(budget, 10).get(), 1);
        assert_eq!(workers_per_ingest_job(budget, 64).get(), 1);
        for active in 1..=10 {
            assert!(workers_per_ingest_job(budget, active).get() * active <= budget.get());
        }
    }

    #[test]
    fn encode_worker_permits_cannot_overbook_the_write_through_budget() {
        let budget = NonZeroUsize::new(10).expect("fixture budget is nonzero");
        let permits = WorkerPermits::new(budget);
        let first = permits.acquire(NonZeroUsize::new(6).expect("fixture share is nonzero"));
        let second = permits.acquire(NonZeroUsize::new(10).expect("fixture share is nonzero"));
        assert_eq!(first.workers().get(), 6);
        assert_eq!(second.workers().get(), 4);
        assert_eq!(
            *permits
                .available
                .lock()
                .expect("fixture permit lock is not poisoned"),
            0
        );
        drop(first);
        drop(second);
        assert_eq!(
            *permits
                .available
                .lock()
                .expect("fixture permit lock is not poisoned"),
            budget.get()
        );
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
        let state = WriteThroughStream {
            pending_chunks: vec![PendingWriteThroughChunk {
                offset: 0,
                bytes: vec![1, 2, 3],
            }],
            pending_bytes: 2,
            ..WriteThroughStream::default()
        };
        assert_pending_write_through_state(&state);
    }

    #[test]
    #[should_panic(expected = "one Ingest Lane exceeded one Container plus CDC suffix")]
    fn stable_lane_asserts_on_an_impossible_buffer_overshoot() {
        let state = WriteThroughStream {
            tail: vec![0_u8; CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES + 1],
            ..WriteThroughStream::default()
        };
        assert_bounded_write_through_lane(&state);
    }
}
