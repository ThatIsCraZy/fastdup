//! Typed, additive management telemetry. Missing fields mean unavailable, not zero.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailTelemetry {
    pub latency: Option<FrontendLatency>,
    pub runtime: Option<RuntimeDetails>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendLatency {
    pub read: OperationLatency,
    pub write: OperationLatency,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationLatency {
    pub operations: u64,
    pub errors: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDetails {
    pub cache_budget: Option<CacheBudgetTelemetry>,
    pub scrub: Option<ScrubTelemetry>,
    pub runtime_id: String,
    pub io_uring: IoUringTelemetry,
    pub caches: Vec<CacheTelemetry>,
    pub reduction: ReductionTelemetry,
    pub checkpoint: Option<CheckpointTelemetry>,
    pub gc: Option<GcTelemetry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheBudgetTelemetry {
    pub maximum_memory_used_basis_points: u64,
    pub effective_limit_bytes: u64,
    pub available_bytes: u64,
    pub budget_bytes: u64,
    pub pools: Vec<CachePoolTelemetry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CachePoolTelemetry {
    pub id: String,
    pub fallback_tier: String,
    pub resident_bytes: u64,
    pub target_bytes: u64,
    pub leased_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IoUringTelemetry {
    pub ring_entries: u32,
    pub inflight_bytes: u64,
    pub max_inflight_bytes: u64,
    pub peak_inflight_bytes: u64,
    pub submitted: u64,
    pub completed: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheTelemetry {
    pub id: String,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub resident_bytes: Option<u64>,
    pub resident_pages: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReductionTelemetry {
    pub enabled: bool,
    pub queries: u64,
    pub candidates: u64,
    pub accepted_prefixes: u64,
    pub accepted_sparse_xor: u64,
    pub saved_payload_bytes: u64,
    pub fallbacks: u64,
    pub errors: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointTelemetry {
    pub completed_at: u64,
    pub generation: u64,
    pub total_ms: f64,
    pub phases: Vec<CheckpointPhase>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointPhase {
    pub id: String,
    pub wall_ms: f64,
    pub cpu_ms: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcTelemetry {
    pub state: String,
    pub observed_at: u64,
    pub total_ms: Option<f64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub unlinked_bytes: Option<u64>,
    pub candidates: Option<u64>,
    pub victims: Option<u64>,
    pub aborted_candidates: Option<u64>,
}

pub(crate) fn parse_details(frontend: &serde_json::Value) -> DetailTelemetry {
    let operation = |name: &str| -> Option<OperationLatency> {
        let value = |suffix: &str| {
            frontend
                .get(format!("{name}_{suffix}"))
                .and_then(serde_json::Value::as_u64)
        };
        Some(OperationLatency {
            operations: value("operations")?,
            errors: value("errors")?,
            p50_micros: value("latency_micros_p50")?,
            p95_micros: value("latency_micros_p95")?,
            p99_micros: value("latency_micros_p99")?,
        })
    };
    DetailTelemetry {
        latency: operation("read")
            .zip(operation("write"))
            .map(|(read, write)| FrontendLatency { read, write }),
        runtime: frontend
            .get("details")
            .and_then(|value| serde_json::from_value(value.clone()).ok()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_runtime_is_unavailable_and_zero_samples_remain_explicit() {
        assert_eq!(
            parse_details(&serde_json::json!({})),
            DetailTelemetry::default()
        );
        let mut frontend = serde_json::Map::new();
        for name in ["read", "write"] {
            for suffix in [
                "operations",
                "errors",
                "latency_micros_p50",
                "latency_micros_p95",
                "latency_micros_p99",
            ] {
                frontend.insert(format!("{name}_{suffix}"), serde_json::json!(0));
            }
        }
        frontend.insert("read_operations".into(), serde_json::json!(100));
        frontend.insert("read_latency_micros_p99".into(), serde_json::json!(250));
        let details = parse_details(&frontend.into());
        let latency = details.latency.unwrap();
        assert_eq!(latency.read.p99_micros, 250);
        assert_eq!(latency.write.operations, 0);
        assert!(details.runtime.is_none());
    }
    #[test]
    fn shared_cache_budget_survives_runtime_parse_and_history_serialization() {
        let mut frontend = serde_json::json!({"details":{
            "scrub":{"state":"failed","totalContainers":12,"verifiedContainers":3,"verifiedBytes":1000,"readBytes":1100,"currentContainer":"abc","error":"checksum mismatch"},
            "runtimeId":"test", "ioUring":{"ringEntries":64,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[], "reduction":{"enabled":true,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0},
            "cacheBudget":{"maximumMemoryUsedBasisPoints":9200,"effectiveLimitBytes":1000,"availableBytes":80,"budgetBytes":900,"pools":[
                {"id":"verifiedRead","fallbackTier":"data","residentBytes":600,"targetBytes":400,"leasedBytes":600,"hits":9,"misses":1,"evictions":2}
            ]}
        }});
        let details = parse_details(&frontend);
        let budget = details
            .runtime
            .as_ref()
            .unwrap()
            .cache_budget
            .as_ref()
            .unwrap();
        assert_eq!(budget.maximum_memory_used_basis_points, 9200);
        assert_eq!(budget.pools[0].leased_bytes, 600);
        assert_eq!(budget.pools[0].target_bytes, 400);
        assert_eq!(
            details
                .runtime
                .as_ref()
                .unwrap()
                .scrub
                .as_ref()
                .unwrap()
                .state,
            "failed"
        );
        let saved = serde_json::to_value(&details).unwrap();
        assert_eq!(saved["runtime"]["scrub"], frontend["details"]["scrub"]);
        assert_eq!(
            saved["runtime"]["cacheBudget"],
            frontend["details"]["cacheBudget"]
        );
        frontend["details"]
            .as_object_mut()
            .unwrap()
            .remove("cacheBudget");
        let legacy = parse_details(&frontend).runtime.unwrap();
        assert!(
            legacy.cache_budget.is_none(),
            "old runtime samples remain readable"
        );
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScrubTelemetry {
    pub state: String,
    pub total_containers: u64,
    pub verified_containers: u64,
    pub verified_bytes: u64,
    pub read_bytes: u64,
    pub current_container: Option<String>,
    pub error: Option<String>,
}
