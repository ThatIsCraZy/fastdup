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

mod auth;
mod control;
mod inventory;
mod samba;
mod store;
mod telemetry;
mod tls;

pub use auth::{AuthError, AuthenticatedSession, LoginResult, SessionManager};
pub use control::{AgentControl, AgentRuntime, ApplianceControl, InMemoryControl};
pub use inventory::{BlockInventory, InventoryError};
pub use samba::{SambaConfig, SambaError};
pub use store::{ControlStore, StoreError, TelemetryStore};
pub use telemetry::{SystemSampler, dedup_rate};
pub use tls::{TlsIdentity, TlsIdentityError};

use serde::{Deserialize, Serialize};

pub const AGENT_PROTOCOL_VERSION: u16 = 1;
pub const CONTROL_SOCKET_PATH: &str = "/run/fastdup/agent.sock";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryState {
    Uninitialized,
    Provisioning,
    Unmounted,
    Mounting,
    Recovering,
    Online,
    Unmounting,
    Scrubbing,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetRole {
    Metadata,
    Data,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackingDisk {
    pub stable_id: String,
    pub model: String,
    pub serial: String,
    pub hba_port: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockTarget {
    pub stable_id: String,
    pub path: String,
    pub kernel_name: String,
    pub model: String,
    pub serial: String,
    pub wwn: String,
    pub target_type: String,
    pub capacity_bytes: u64,
    pub hba_port: String,
    pub filesystem: Option<String>,
    pub eligible: bool,
    pub eligibility_reason: Option<String>,
    pub backing_disks: Vec<BackingDisk>,
    pub inventory_revision: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvancedReduction {
    Off,
    PrefixV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySettings {
    pub revision: u64,
    pub auto_mount: bool,
    pub advanced_reduction: AdvancedReduction,
    pub online_gc_enabled: bool,
    pub maintenance_window_utc: Option<String>,
    pub pressure_low_basis_points: u16,
    pub pressure_high_basis_points: u16,
}

impl Default for RepositorySettings {
    fn default() -> Self {
        Self {
            revision: 1,
            auto_mount: true,
            advanced_reduction: AdvancedReduction::Off,
            online_gc_enabled: true,
            maintenance_window_utc: None,
            pressure_low_basis_points: 8_500,
            pressure_high_basis_points: 9_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SmbEncryption {
    Desired,
    Required,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CapacityUnit {
    Gb,
    Tb,
    Pb,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalQuota {
    pub value: u16,
    pub unit: CapacityUnit,
}

impl LogicalQuota {
    /// Converts the exact decimal UI unit to bytes without crossing a
    /// JavaScript number boundary on the public interface.
    #[must_use]
    pub const fn bytes(self) -> Option<u64> {
        let multiplier = match self.unit {
            CapacityUnit::Gb => 1_000_000_000,
            CapacityUnit::Tb => 1_000_000_000_000,
            CapacityUnit::Pb => 1_000_000_000_000_000,
        };
        (self.value as u64).checked_mul(multiplier)
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.value >= 1 && self.value <= 999 && self.bytes().is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShareSettings {
    pub id: String,
    pub revision: u64,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub hidden: bool,
    pub read_only: bool,
    pub guest_access: bool,
    pub encryption: SmbEncryption,
    pub access_based_enumeration: bool,
    pub allowed_users: Vec<String>,
    pub allowed_groups: Vec<String>,
    #[serde(
        default,
        alias = "presentedCapacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub logical_quota: Option<LogicalQuota>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskTelemetry {
    pub id: String,
    pub role: String,
    pub model: String,
    pub kind: String,
    pub capacity_bytes: u64,
    pub hba_port: String,
    pub outstanding_io: u64,
    pub read_mbps: f64,
    pub write_mbps: f64,
    pub read_iops: f64,
    pub write_iops: f64,
    pub utilization: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesPoint {
    pub time: String,
    pub read: f64,
    pub write: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetrySnapshot {
    pub sequence: u64,
    pub observed_at: String,
    pub repository_state: RepositoryState,
    pub frontend_read_mbps: f64,
    pub frontend_write_mbps: f64,
    pub dedup_rate: f64,
    pub reduction_ratio: f64,
    pub cpu_percent: f64,
    pub ram_percent: f64,
    pub data_used_bytes: u64,
    pub data_capacity_bytes: u64,
    pub last_checkpoint_seconds: u64,
    pub disks: Vec<DiskTelemetry>,
    pub series: Vec<SeriesPoint>,
}

impl Default for TelemetrySnapshot {
    fn default() -> Self {
        Self {
            sequence: 0,
            observed_at: unix_seconds().to_string(),
            repository_state: RepositoryState::Uninitialized,
            frontend_read_mbps: 0.0,
            frontend_write_mbps: 0.0,
            dedup_rate: 0.0,
            reduction_ratio: 0.0,
            cpu_percent: 0.0,
            ram_percent: 0.0,
            data_used_bytes: 0,
            data_capacity_bytes: 0,
            last_checkpoint_seconds: 0,
            disks: Vec::new(),
            series: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub id: String,
    pub kind: String,
    pub state: JobState,
    pub progress_basis_points: u16,
    pub message: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplianceSnapshot {
    pub telemetry: TelemetrySnapshot,
    pub targets: Vec<BlockTarget>,
    pub settings: RepositorySettings,
    pub shares: Vec<ShareSettings>,
    pub jobs: Vec<JobStatus>,
    pub certificate_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryBinding {
    pub metadata_target: String,
    pub data_target: String,
    pub metadata_uuid: String,
    pub data_uuid: String,
    pub metadata_kernel_name: String,
    pub data_kernel_name: String,
    pub state: RepositoryState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command {
    Provision {
        metadata_target: String,
        data_target: String,
        inventory_revision: String,
        confirmed: bool,
    },
    Adopt {
        metadata_target: String,
        data_target: String,
        inventory_revision: String,
    },
    Mount,
    Unmount,
    OfflineScrub,
    UpdateSettings {
        expected_revision: u64,
        settings: RepositorySettings,
    },
    UpsertShare {
        expected_revision: Option<u64>,
        share: ShareSettings,
    },
    DeleteShare {
        id: String,
        expected_revision: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlEvent {
    Snapshot { snapshot: TelemetrySnapshot },
    Job { job: JobStatus },
    Alert { code: String, message: String },
    Audit { action: String, outcome: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRequest {
    pub version: u16,
    pub request_id: String,
    pub operation: AgentOperation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentOperation {
    Inspect,
    Submit {
        command: Command,
        idempotency_key: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResponse {
    pub version: u16,
    pub request_id: String,
    pub result: Result<AgentResult, ControlProblem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentResult {
    Snapshot { snapshot: ApplianceSnapshot },
    Job { job: JobStatus },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, thiserror::Error)]
#[error("{message}")]
#[serde(rename_all = "camelCase")]
pub struct ControlProblem {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ControlProblem {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
}

#[must_use]
pub fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}
