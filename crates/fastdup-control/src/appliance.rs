//! The aggregate observation assembled fresh on every agent `Inspect`.

use serde::{Deserialize, Serialize};

use crate::{
    BlockTarget, JobStatus, RepositoryBinding, RepositorySettings, ShareSettings, TelemetrySnapshot,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplianceSnapshot {
    pub telemetry: TelemetrySnapshot,
    pub targets: Vec<BlockTarget>,
    pub repository: Option<RepositoryBinding>,
    pub settings: RepositorySettings,
    pub shares: Vec<ShareSettings>,
    #[serde(default)]
    pub veeam: Option<crate::VeeamSettings>,
    #[serde(default)]
    pub veeam_active: bool,
    pub jobs: Vec<JobStatus>,
    pub certificate_fingerprint: String,
}
