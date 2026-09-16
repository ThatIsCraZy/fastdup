//! Snapshot DTOs persisted into the telemetry database and streamed live.
//! The serde shape is the durable telemetry format; field-wise attributes carry
//! the reader/scrub compatibility rules for legacy samples.

use serde::{Deserialize, Serialize};

use crate::{DetailTelemetry, RepositoryState, unix_seconds};

/// A live observation, never a persisted mount instruction or storage authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeIssue {
    Unavailable,
    WriteBlocked,
    IntegrityFailed,
    ProcessExited,
}

impl RuntimeIssue {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::Unavailable => "Repository-Mount fehlt oder der Runtime-Prozess ist beendet.",
            Self::ProcessExited => {
                "Repository-Runtime ist abgestürzt. Details stehen unter Ereignisse."
            }
            Self::WriteBlocked => {
                "Repository wartet auf dauerhaften Fortschritt. Neue Schreibzugriffe sind pausiert."
            }
            Self::IntegrityFailed => {
                "Repository hat einen Integritätsfehler erkannt. Schreibzugriffe sind gesperrt."
            }
        }
    }
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
    /// Requests in flight at the sample instant (Linux stat field 9).
    pub outstanding_io: u64,
    /// Time-weighted mean queue depth over the same interval as throughput.
    /// Missing until two valid samples are available or after counter reset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub average_outstanding_io: Option<f64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_issue: Option<RuntimeIssue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub small_file_quota: Option<SmallFileQuotaStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_usage: Option<Box<StorageUsageTelemetry>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<DetailTelemetry>>,
    pub sequence: u64,
    pub observed_at: String,
    pub repository_state: RepositoryState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_generation: Option<u64>,
    pub frontend_read_mbps: f64,
    pub frontend_write_mbps: f64,
    pub dedup_rate: f64,
    pub reduction_ratio: Option<f64>,
    pub cpu_percent: f64,
    pub ram_percent: f64,
    pub data_used_bytes: u64,
    pub data_capacity_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_seconds: Option<u64>,
    pub disks: Vec<DiskTelemetry>,
    pub series: Vec<SeriesPoint>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageUsageTelemetry {
    pub logical_allocated_bytes: Option<u64>,
    pub logical_observed_at: Option<u64>,
    pub metadata_used_bytes: Option<u64>,
    pub metadata_capacity_bytes: Option<u64>,
    pub data_used_bytes: Option<u64>,
    pub data_capacity_bytes: Option<u64>,
}

impl StorageUsageTelemetry {
    /// Current allocated logical bytes per occupied byte across both pools.
    /// Includes filesystem overhead and storage awaiting GC; never ingest counters.
    #[must_use]
    pub fn reduction_ratio(&self) -> Option<f64> {
        let logical = self.logical_allocated_bytes?;
        let physical = self
            .metadata_used_bytes?
            .checked_add(self.data_used_bytes?)?;
        (physical != 0).then(|| logical as f64 / physical as f64)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SmallFileQuotaStatus {
    pub requested_bytes: u64,
    pub effective_bytes: u64,
}

impl Default for TelemetrySnapshot {
    fn default() -> Self {
        Self {
            runtime_issue: None,
            small_file_quota: None,
            storage_usage: None,
            details: None,
            sequence: 0,
            observed_at: unix_seconds().to_string(),
            repository_state: RepositoryState::Uninitialized,
            commit_generation: None,
            frontend_read_mbps: 0.0,
            frontend_write_mbps: 0.0,
            dedup_rate: 0.0,
            reduction_ratio: None,
            cpu_percent: 0.0,
            ram_percent: 0.0,
            data_used_bytes: 0,
            data_capacity_bytes: 0,
            last_checkpoint_seconds: None,
            disks: Vec::new(),
            series: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_telemetry_does_not_claim_repository_durability() {
        let snapshot = TelemetrySnapshot {
            sequence: 216,
            ..TelemetrySnapshot::default()
        };
        let value = serde_json::to_value(snapshot).expect("serialize telemetry snapshot");

        assert_eq!(
            value.get("sequence").and_then(serde_json::Value::as_u64),
            Some(216)
        );
        assert!(value.get("commitGeneration").is_none());
        assert!(value.get("lastCheckpointSeconds").is_none());
    }
}
