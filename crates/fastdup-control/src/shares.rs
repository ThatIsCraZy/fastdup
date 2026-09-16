//! Managed Share policy: SMB exposure rules plus the logical quota bound
//! admitted later at the Namespace seam, never here.

use serde::{Deserialize, Serialize};

use crate::AdvancedReduction;

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
    /// Absent legacy values inherit the repository default. Explicit values
    /// govern only new writer work beneath this Share root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advanced_reduction: Option<AdvancedReduction>,
    #[serde(
        default,
        alias = "presentedCapacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub logical_quota: Option<LogicalQuota>,
}
