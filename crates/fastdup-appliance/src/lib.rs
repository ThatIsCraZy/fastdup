#![forbid(unsafe_code)]

//! Durable repository-to-POSIX mount orchestration.

mod appliance_lease;
mod appliance_recovery_latch;
mod checkpoint;
mod checkpoint_trigger;
mod commit_capacity;
mod historical_proof_cache;
mod manifest_file;
mod mount;
mod namespace_restore;
mod online_gc;
mod pool_binding;
mod pool_isolation;
mod proof_cache_trace;
mod small_file_tier;
mod statfs;

pub use appliance_lease::{APPLIANCE_LEASE_FILE_NAME, ApplianceLease, ApplianceLeaseOwner};
pub use appliance_recovery_latch::{
    APPLIANCE_RECOVERY_LATCH_FILE_NAME, ApplianceRecoveryLatch, ApplianceRecoveryState,
};
pub use pool_binding::{AppliancePoolBinding, AppliancePoolBindingError, POOL_IDENTITY_FILE_NAME};
pub use pool_isolation::{
    POOL_ISOLATION_POLICY_ENV, PhysicalPoolIsolation, PoolIsolationError, PoolIsolationObservation,
    PoolIsolationPolicy, PoolIsolationPolicyError,
};

pub use checkpoint::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointMetrics, CheckpointPhaseMetrics, CpuPhaseStatus,
    DurableNamespace, DurableNamespaceError, GenerationProofSetStatus, INODE_RESERVATION_SPAN_V1,
    ProfiledCheckpoint, WriteThroughStatus, checkpoint_exact_index_profile_v1,
    checkpoint_policy_set,
};
pub use checkpoint_trigger::{
    CONTAINER_COMMIT_COALESCE, CheckpointAction, CheckpointPressure, CheckpointProgressAction,
    CheckpointTrigger, DurabilityObservation, DurabilitySupervisor, MUTATION_ADMISSION_GUARD,
    MUTATION_COMMIT_TARGET, SEALED_CONTAINER_COMMIT_LIMIT, checkpoint_action,
};
pub use commit_capacity::{
    COMMIT_METADATA_FLOOR_BYTES_V1, CommitCapacityConfigurationError, CommitCapacityGovernor,
    CommitCapacitySnapshot, CommitCapacityStatus,
};
pub use historical_proof_cache::HistoricalProofCacheStatus;
pub use online_gc::{
    DailyGcWindow, ONLINE_GC_ACTIVE_INTERVAL_SECONDS_ENV, ONLINE_GC_CONTROL_REQUEST,
    ONLINE_GC_CONTROL_SOCKET_NAME, ONLINE_GC_DAILY_WINDOW_UTC_ENV,
    ONLINE_GC_IDLE_AFTER_SECONDS_ENV, ONLINE_GC_IDLE_INTERVAL_SECONDS_ENV,
    ONLINE_GC_MAX_RELOCATION_WORKERS_ENV, ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS_ENV,
    ONLINE_GC_PRESSURE_LOW_BASIS_POINTS_ENV, ONLINE_GC_URGENT_INTERVAL_SECONDS_ENV,
    ONLINE_GC_WINDOW_INTERVAL_SECONDS_ENV, OnlineGcPolicy, OnlineGcPolicyConfigurationError,
    OnlineGcPolicyError, OnlineGcScheduler, OnlineGcSchedulerStatus, bind_online_gc_control_socket,
    online_gc_control_path, remove_stale_online_gc_socket, request_online_gc_now,
};
pub use proof_cache_trace::{
    ProofCacheEvent, ProofCachePolicy, ProofCacheReplayError, ProofCacheReplayReport,
    ProofCacheTrace, ProofKey, replay_proof_cache_trace,
};
pub use small_file_tier::{
    SMALL_FILE_PROJECT_ID_ENV, SMALL_FILE_QUOTA_BYTES_ENV, SmallFileTierIsolation,
    SmallFileTierIsolationError, small_file_container_root,
};
pub use statfs::{
    STATFS_RESERVE_BASIS_POINTS, StatFsOverride, StatFsOverrideError, TieredStatFsSource,
};

pub use mount::{MountError, recover_mount, recover_mount_with_index};

pub(crate) use manifest_file::ManifestCommittedFile;
pub(crate) use namespace_restore::namespace_from_verified_files_using;
