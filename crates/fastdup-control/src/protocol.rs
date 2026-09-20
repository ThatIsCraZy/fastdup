//! Versioned typed command and reply seam between the unprivileged web
//! process and the root-owned agent, per the accepted Control Plane ADR.

use serde::{Deserialize, Serialize};

use crate::{
    ApplianceSnapshot, JobStatus, RepositorySettings, SambaUserRequest, ShareSettings,
    TelemetrySnapshot,
};

pub const AGENT_PROTOCOL_VERSION: u16 = 1;
pub const CONTROL_SOCKET_PATH: &str = "/run/fastdup/agent.sock";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command {
    ConfigureVeeam {
        settings: crate::VeeamSettings,
        #[serde(default)]
        bootstrap_password: Option<crate::BootstrapPassword>,
    },
    StartVeeam,
    StopVeeam,
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
    GcNow,
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
    SambaUsers,
    CreateSambaUser {
        request: SambaUserRequest,
    },
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
    SambaUsers { users: Vec<String> },
    SambaUserCreated,
    Snapshot { snapshot: Box<ApplianceSnapshot> },
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
