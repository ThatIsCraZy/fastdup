//! Repository lifecycle state and the durable operator configuration that
//! projects into the writer environment. Configuration is control-plane
//! intent only; it never carries storage authority.

use serde::{Deserialize, Serialize};

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvancedReduction {
    Off,
    DependentV1,
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
    #[serde(default = "default_small_file_extensions")]
    pub small_file_extensions: Vec<String>,
}

fn default_small_file_extensions() -> Vec<String> {
    fastdup_posix::DEFAULT_SMALL_FILE_EXTENSIONS
        .map(str::to_owned)
        .to_vec()
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
            small_file_extensions: default_small_file_extensions(),
        }
    }
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

#[cfg(test)]
mod repository_settings_tests {
    use super::*;

    #[test]
    fn legacy_settings_receive_the_v1_small_file_defaults() {
        let settings: RepositorySettings = serde_json::from_str(
            r#"{
                "revision": 7,
                "autoMount": true,
                "advancedReduction": "off",
                "onlineGcEnabled": true,
                "maintenanceWindowUtc": null,
                "pressureLowBasisPoints": 8500,
                "pressureHighBasisPoints": 9000
            }"#,
        )
        .expect("deserialize legacy settings");
        assert_eq!(settings.small_file_extensions, [".json", ".xml"]);
    }
}
