#![forbid(unsafe_code)]
#![allow(
    clippy::cast_precision_loss,
    clippy::missing_errors_doc,
    clippy::struct_excessive_bools
)]

//! Local management Control Plane for one `FastDup` Appliance.
//!
//! Repository formats and Pool identities remain authoritative. This crate
//! retains only operator configuration, rebuildable observations, and jobs.
//!
//! The root re-exports keep the flat public API used by both binaries stable;
//! type definitions live in the domain module that owns each vocabulary.

mod appliance;
mod auth;
mod cache_window;
mod control;
mod detail_telemetry;
mod firewall;
mod inventory;
mod jobs;
mod pfx;
mod protocol;
mod repository;
mod runtime_health;
mod samba;
mod samba_users;
mod shares;
mod store;
mod telemetry;
mod tls;

pub use appliance::ApplianceSnapshot;
pub use auth::{AuthError, AuthenticatedSession, LoginResult, SessionManager, WebUser};
pub use control::{AgentControl, AgentRuntime, ApplianceControl, InMemoryControl};
pub use detail_telemetry::{
    AdmissionTelemetry, AllocatorMemoryTelemetry, CacheBudgetTelemetry, CachePoolTelemetry,
    CacheTelemetry, CacheWindowCounters, CacheWindowTelemetry, CheckpointPhase,
    CheckpointTelemetry, CodecBufferTelemetry, DetailTelemetry, ExactCacheTelemetry,
    ExactMembershipTelemetry, ExactWarmTelemetry, FrontendLatency, GcPhaseDurations, GcTelemetry,
    IoUringTelemetry, MetadataGcTelemetry, MetadataReadRow, MetadataReadTelemetry,
    OperationLatency, PipelineOperation, PipelineTelemetry, ReadCacheCompression,
    ReductionTelemetry, RuntimeDetails, ScrubTelemetry,
};
pub use inventory::{BackingDisk, BlockInventory, BlockTarget, InventoryError};
pub use jobs::{AuditEvent, JobState, JobStatus};
pub use pfx::decode_pfx;
pub use protocol::{
    AGENT_PROTOCOL_VERSION, AgentOperation, AgentRequest, AgentResponse, AgentResult,
    CONTROL_SOCKET_PATH, Command, ControlEvent, ControlProblem,
};
pub use repository::{AdvancedReduction, RepositoryBinding, RepositorySettings, RepositoryState};
pub use samba::{SambaConfig, SambaError};
pub use samba_users::SambaUserRequest;
pub use shares::{CapacityUnit, LogicalQuota, ShareSettings, SmbEncryption};
pub use store::{ControlStore, StoreError, TelemetryStore};
pub use telemetry::{
    DiskTelemetry, IngestReductionTelemetry, RuntimeIssue, SeriesPoint, SmallFileQuotaStatus,
    StorageUsageTelemetry, SystemSampler, TelemetrySnapshot, dedup_rate,
};
pub use tls::{TlsIdentity, TlsIdentityError};

#[must_use]
pub fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}
