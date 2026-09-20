//! Telemetry observation: the snapshot DTOs and the `/proc`-based sampler that
//! fills them. Telemetry is rebuildable observation, never storage authority.

mod sampler;
mod snapshot;

pub use sampler::{SystemSampler, dedup_rate};
pub use snapshot::{
    DiskTelemetry, IngestReductionTelemetry, RuntimeIssue, SeriesPoint, SmallFileQuotaStatus,
    StorageUsageTelemetry, TelemetrySnapshot,
};

pub(crate) use sampler::filesystem_usage;
