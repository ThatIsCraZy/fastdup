//! Telemetry observation: the snapshot DTOs and the `/proc`-based sampler that
//! fills them. Telemetry is rebuildable observation, never storage authority.

pub(crate) mod history;
pub(crate) mod latency;
mod sampler;
mod snapshot;
pub(crate) mod workloads;
pub use latency::{LatencyHistogram, LatencyWindow};
pub use workloads::{WorkloadConnection, WorkloadContext, WorkloadShare};

pub use sampler::{SystemSampler, dedup_rate};
pub use snapshot::{
    DiskTelemetry, IngestReductionTelemetry, RuntimeIssue, SeriesPoint, SmallFileQuotaStatus,
    StorageUsageTelemetry, TelemetrySnapshot,
};

pub use history::{HistorySummary, MetricRange};
pub(crate) use sampler::filesystem_usage;
