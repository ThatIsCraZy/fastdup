//! Public checkpoint telemetry and its private accounting helpers.

use std::time::{Duration, Instant};

use fastdup_format::{CommitRecord, IncompressibilityGateMetrics};
use fastdup_store::PersistentReductionStatus;

pub const CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1: u64 = 8 * fastdup_format::MAX_CONTAINER_BYTES;

/// Cumulative scheduler evidence for one write-through CPU phase.
///
/// Runnable wall time starts after permit acquisition and ends after the CPU
/// work. `permit_wait_ns` includes uncontended lock acquisition, while
/// `permit_blocked_phases` counts phases that actually waited on the permit
/// condition variable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuPhaseStatus {
    pub(super) phases: u64,
    pub(super) active: u64,
    pub(super) maximum_active: u64,
    pub(super) runnable_wall_ns: u64,
    pub(super) permit_blocked_phases: u64,
    pub(super) permit_wait_ns: u64,
    pub(super) maximum_permit_wait_ns: u64,
    pub(super) requested_workers: u64,
    pub(super) granted_workers: u64,
    pub(super) partial_grants: u64,
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
    pub(super) buffered_bytes: u64,
    pub(super) queued_bytes: u64,
    pub(super) active_lanes: u64,
    pub(super) sealed_uncommitted_containers: u64,
    pub(super) oldest_sealed_age: Option<Duration>,
    pub(super) hash_batches: u64,
    pub(super) maximum_hash_workers: u64,
    pub(super) ingest_batches: u64,
    pub(super) ingest_fragments: u64,
    pub(super) maximum_ingest_batch_bytes: u64,
    pub(super) minimum_ingest_batch_target_bytes: u64,
    pub(super) maximum_ingest_batch_target_bytes: u64,
    pub(super) maximum_ingest_ring_slots: u64,
    pub(super) ingest_ring_wait_ns: u64,
    pub(super) hash_cpu: CpuPhaseStatus,
    pub(super) encode_cpu: CpuPhaseStatus,
    pub(super) planning_cpu: CpuPhaseStatus,
    pub(super) materialization_wall_ns: u64,
    pub(super) advanced_reduction: PersistentReductionStatus,
    pub(super) degraded: bool,
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
    pub(super) wall: Duration,
    pub(super) process_cpu: Duration,
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

    pub(super) fn add(&mut self, wall: Duration, process_cpu: Duration) {
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
    pub(super) total: CheckpointPhaseMetrics,
    pub(super) checkpoint_lock: CheckpointPhaseMetrics,
    pub(super) proof_freeze: CheckpointPhaseMetrics,
    pub(super) cut_capture: CheckpointPhaseMetrics,
    pub(super) ingest_wait: CheckpointPhaseMetrics,
    pub(super) publication_wait: CheckpointPhaseMetrics,
    pub(super) lane_lock: CheckpointPhaseMetrics,
    pub(super) stable_extract: CheckpointPhaseMetrics,
    pub(super) publication_enqueue: CheckpointPhaseMetrics,
    pub(super) publication_retire: CheckpointPhaseMetrics,
    pub(super) recipe_attach: CheckpointPhaseMetrics,
    pub(super) writer_setup: CheckpointPhaseMetrics,

    pub(super) freeze: CheckpointPhaseMetrics,
    pub(super) manifest_plan: CheckpointPhaseMetrics,
    pub(super) cdc: CheckpointPhaseMetrics,
    pub(super) hash_and_fill: CheckpointPhaseMetrics,
    pub(super) exact_lookup: CheckpointPhaseMetrics,
    pub(super) compression_encode: CheckpointPhaseMetrics,
    pub(super) container_publish: CheckpointPhaseMetrics,
    pub(super) exact_index_publish: CheckpointPhaseMetrics,
    pub(super) metadata_commit: CheckpointPhaseMetrics,
    pub(super) logical_chunks: u64,
    pub(super) logical_chunk_bytes: u64,
    pub(super) fill_chunks: u64,
    pub(super) fill_bytes: u64,
    pub(super) exact_hit_chunks: u64,
    pub(super) exact_hit_bytes: u64,
    pub(super) new_chunks: u64,
    pub(super) new_chunk_bytes: u64,
    pub(super) container_file_bytes: u64,
    pub(super) raw_records: u64,
    pub(super) zstd_records: u64,
    pub(super) incompressibility_gate: IncompressibilityGateMetrics,
    pub(super) containers: u64,
    pub(super) peak_buffered_chunk_bytes: u64,
    pub(super) peak_buffered_chunks: u64,
    pub(super) recipe_reuse_chunks: u64,
    pub(super) recipe_reuse_bytes: u64,
    pub(super) checkpoint_rechunk_bytes: u64,
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
    phase_getter!(checkpoint_lock);
    phase_getter!(proof_freeze);
    phase_getter!(cut_capture);
    phase_getter!(ingest_wait);
    phase_getter!(publication_wait);
    phase_getter!(lane_lock);
    phase_getter!(stable_extract);
    phase_getter!(publication_enqueue);
    phase_getter!(publication_retire);
    phase_getter!(recipe_attach);
    phase_getter!(writer_setup);

    phase_getter!(freeze);
    phase_getter!(manifest_plan);
    phase_getter!(cdc);
    phase_getter!(hash_and_fill);
    phase_getter!(exact_lookup);
    phase_getter!(compression_encode);
    phase_getter!(container_publish);
    phase_getter!(exact_index_publish);
    phase_getter!(metadata_commit);

    /// Small bookkeeping gaps outside the non-overlapping top-level phases.
    #[must_use]
    pub fn unattributed(self) -> Duration {
        self.total.wall.saturating_sub(
            [
                self.checkpoint_lock.wall,
                self.proof_freeze.wall,
                self.cut_capture.wall,
                self.ingest_wait.wall,
                self.publication_wait.wall,
                self.lane_lock.wall,
                self.stable_extract.wall,
                self.publication_enqueue.wall,
                self.publication_retire.wall,
                self.recipe_attach.wall,
                self.writer_setup.wall,
                self.freeze.wall,
                self.manifest_plan.wall,
                self.exact_index_publish.wall,
                self.metadata_commit.wall,
            ]
            .into_iter()
            .sum(),
        )
    }

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

    pub(super) fn merge_reduction(&mut self, reduction: &CheckpointReductionMetrics) {
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
    pub(super) record: CommitRecord,
    pub(super) metrics: CheckpointMetrics,
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
pub(super) struct CheckpointReductionMetrics {
    pub(super) cdc: CheckpointPhaseMetrics,
    pub(super) hash_and_fill: CheckpointPhaseMetrics,
    pub(super) exact_lookup: CheckpointPhaseMetrics,
    pub(super) compression_encode: CheckpointPhaseMetrics,
    pub(super) container_publish: CheckpointPhaseMetrics,
    pub(super) logical_chunks: u64,
    pub(super) logical_chunk_bytes: u64,
    pub(super) fill_chunks: u64,
    pub(super) fill_bytes: u64,
    pub(super) exact_hit_chunks: u64,
    pub(super) exact_hit_bytes: u64,
    pub(super) new_chunks: u64,
    pub(super) new_chunk_bytes: u64,
    pub(super) container_file_bytes: u64,
    pub(super) raw_records: u64,
    pub(super) zstd_records: u64,
    pub(super) incompressibility_gate: IncompressibilityGateMetrics,
    pub(super) containers: u64,
    pub(super) peak_buffered_chunk_bytes: u64,
    pub(super) peak_buffered_chunks: u64,
    pub(super) recipe_reuse_chunks: u64,
    pub(super) recipe_reuse_bytes: u64,
    pub(super) checkpoint_rechunk_bytes: u64,
}

#[derive(Clone, Copy)]
pub(super) struct PhaseStarted {
    wall: Instant,
    process_cpu: rustix::time::Timespec,
}

impl PhaseStarted {
    pub(super) fn now() -> Self {
        Self {
            wall: Instant::now(),
            process_cpu: rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime),
        }
    }

    pub(super) fn finish_into(self, phase: &mut CheckpointPhaseMetrics) {
        let process_cpu = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
        let process_cpu = Duration::try_from(process_cpu - self.process_cpu)
            .expect("ASSERT: monotonic process CPU time must form a nonnegative Duration");
        phase.add(self.wall.elapsed(), process_cpu);
    }
}

/// Fixed-size live observations; none of these locks protects pipeline work.
#[derive(Debug, Default)]
pub(super) struct CheckpointTimings {
    operations: [fastdup_store::OperationTiming; 16],
}

#[derive(Clone, Copy)]
pub(super) enum CheckpointStage {
    CheckpointLock,
    ProofFreeze,
    CutCapture,
    IngestWait,
    PublicationWait,
    LaneLock,
    StableExtract,
    PublicationEnqueue,
    PublicationRetire,
    RecipeAttach,
    WriterSetup,
    Freeze,
    ManifestPlan,
    IndexPublish,
    MetadataCommit,
    Total,
}

pub(super) struct ObservedPhase {
    started: PhaseStarted,
    _live: fastdup_store::OperationTimer,
}

impl ObservedPhase {
    pub(super) fn finish_into(self, metrics: &mut CheckpointPhaseMetrics) {
        self.started.finish_into(metrics);
    }
}

impl CheckpointTimings {
    pub(super) fn begin(&self, stage: CheckpointStage) -> ObservedPhase {
        ObservedPhase {
            started: PhaseStarted::now(),
            _live: self.operations[stage as usize].begin(),
        }
    }

    pub(super) fn snapshots(&self) -> Vec<fastdup_store::OperationTimingSnapshot> {
        const IDS: [&str; 16] = [
            "checkpointCheckpointLock",
            "checkpointProofFreeze",
            "checkpointCutCapture",
            "checkpointIngestWait",
            "checkpointPublicationWait",
            "checkpointLaneLock",
            "checkpointStableExtract",
            "checkpointPublicationEnqueue",
            "checkpointPublicationRetire",
            "checkpointRecipeAttach",
            "checkpointWriterSetup",
            "checkpointFreeze",
            "checkpointManifestPlan",
            "checkpointIndexPublish",
            "checkpointMetadataCommit",
            "checkpointTotal",
        ];
        self.operations
            .iter()
            .zip(IDS)
            .map(|(timing, id)| timing.snapshot(id))
            .collect()
    }
}
