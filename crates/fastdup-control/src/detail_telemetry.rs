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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<PipelineTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_reads: Option<MetadataReadTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec_buffers: Option<CodecBufferTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_memory: Option<AllocatorMemoryTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_cache_compression: Option<ReadCacheCompression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_window: Option<CacheWindowTelemetry>,
    pub cache_budget: Option<CacheBudgetTelemetry>,
    pub scrub: Option<ScrubTelemetry>,
    pub runtime_id: String,
    pub io_uring: IoUringTelemetry,
    pub caches: Vec<CacheTelemetry>,
    pub reduction: ReductionTelemetry,
    pub checkpoint: Option<CheckpointTelemetry>,
    pub gc: Option<GcTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_cache: Option<ExactCacheTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_membership: Option<ExactMembershipTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_warm: Option<ExactWarmTelemetry>,
}

/// Index pages held against eviction so a lookup stays a RAM access.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExactCacheTelemetry {
    pub protected_limit_bytes: u64,
    pub protected_resident_bytes: u64,
}

/// Membership filters answer "definitely absent" without reading an index page.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExactMembershipTelemetry {
    pub leased_runs: u64,
    pub filters: u64,
    pub constructed_filters: u64,
    pub missing_filters: u64,
    pub page_bounds_runs: u64,
    pub missing_page_bounds: u64,
    pub page_bounds_bytes: u64,
    pub probes: u64,
    pub definitely_absent: u64,
    pub requires_exact_lookup: u64,
}

/// Outcome of the most recent index warm-up cycle.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExactWarmTelemetry {
    pub state: String,
}

/// Reusable work buffers; active owners can also belong to content caches.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodecBufferTelemetry {
    pub retained_bytes: u64,
    pub active_bytes: u64,
    pub peak_active_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Backend API work; bytes and calls are not physical disk bytes or IOPS.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataReadTelemetry {
    pub interval_seconds: f64,
    pub rows: Vec<MetadataReadRow>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataReadRow {
    pub reason: String,
    pub object: String,
    pub mode: String,
    pub operations: u64,
    pub requested_bytes: u64,
    pub returned_bytes: u64,
    pub errors: u64,
    pub elapsed_micros: u64,
    pub max_micros: u64,
    pub in_flight: u64,
    pub operations_per_second: f64,
    pub requested_mbps: f64,
}

/// Background allocator sample; free blocks may already be absent from RSS.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocatorMemoryTelemetry {
    pub arena_bytes: u64,
    pub allocated_bytes: u64,
    pub free_bytes: u64,
    pub anonymous_resident_bytes: u64,
    pub trim_attempts: u64,
    pub last_trim_micros: u64,
}

/// Gauges at the sample time and codec counters since this runtime started.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadCacheCompression {
    pub decoded_resident_bytes: u64,
    pub compressed_resident_bytes: u64,
    pub compressed_logical_bytes: u64,
    pub attempts: u64,
    pub admissions: u64,
    pub compression_nanos: u64,
    pub hits: u64,
    pub decompressions: u64,
    pub decompression_nanos: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub failures: u64,
    pub bypasses: u64,
    pub working_bytes: u64,
    pub peak_working_bytes: u64,
    pub max_working_bytes: u64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped_cold_candidates: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exploration_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_base_reads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm_base_reuses: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successful_base_trials: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unattributed_ms: Option<f64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phases_ms: Option<GcPhaseDurations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_gc: Option<MetadataGcTelemetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_examined_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_write_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_proof_read_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse_dependency_edges: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse_dependency_required_chunks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_queue_retained: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_queue_scanned_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_pending_updates: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_retirement_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_runs_retired: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_run_sets_retired: Option<u64>,
}

/// Wall time per collection phase of the last cycle; phases do not overlap.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcPhaseDurations {
    pub recovery: f64,
    pub metadata_gc: f64,
    pub candidate_catalog: f64,
    pub candidate_proof: f64,
    pub relocation: f64,
    pub retiring_activation: f64,
    pub pin_drain: f64,
    pub victim_verify: f64,
    pub unlink: f64,
    pub data_sync: f64,
    pub removed_activation: f64,
    pub post_collection_catalog: f64,
}

/// Namespace and manifest collection that runs inside the same GC cycle.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataGcTelemetry {
    pub mark_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_reason: Option<String>,
    pub wall_ms: f64,
    pub barrier_wait_ms: f64,
    pub object_graph_read_bytes: u64,
    pub candidate_read_bytes: u64,
    pub catalog_read_bytes: u64,
    pub catalog_write_bytes: u64,
    pub unlinked_bytes: u64,
    pub root_syncs: u64,
    pub catalog_chain_runs: u64,
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
    fn pipeline_waits_and_open_admission_history_survive_roundtrip() {
        let mut frontend = serde_json::json!({"details": {
            "runtimeId":"test", "ioUring":{"ringEntries":64,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[], "reduction":{"enabled":true,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0}
        }});
        assert!(parse_details(&frontend).runtime.unwrap().pipeline.is_none());
        frontend["details"]["pipeline"] = serde_json::json!({
            "admission":{"open":false,"reason":"checkpointTimeout","closures":2,"closedMs":26000.0,"currentClosedMs":12000.0,"maximumClosedMs":14000.0},
            "operations":[{"id":"exactEnqueue","active":3,"completed":9,"totalMs":2100.0,"maximumMs":2000.0,"busyMs":8000.0}]
        });
        frontend["details"]["checkpoint"] = serde_json::json!({"completedAt":100,"generation":8,"totalMs":16000,"unattributedMs":10,
            "phases":[{"id":"publicationWait","wallMs":14000,"cpuMs":1000}]});
        let saved = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(
            saved["runtime"]["pipeline"],
            frontend["details"]["pipeline"]
        );
        let restored: DetailTelemetry = serde_json::from_value(saved).unwrap();
        let runtime = restored.runtime.unwrap();
        assert_eq!(runtime.checkpoint.unwrap().unattributed_ms, Some(10.0));
        assert_eq!(runtime.pipeline.unwrap().operations[0].active, 3);
        frontend["details"]["pipeline"]["admission"]["open"] = serde_json::json!(true);
        frontend["details"]["pipeline"]["admission"]["reason"] = serde_json::Value::Null;
        frontend["details"]["pipeline"]["admission"]["currentClosedMs"] = serde_json::json!(0);
        let current = parse_details(&frontend).runtime.unwrap().pipeline.unwrap();
        assert!(current.admission.open);
        assert_eq!(current.admission.closed_ms, 26000.0);
    }
    #[test]
    fn metadata_read_attribution_survives_api_history_roundtrip() {
        let mut frontend = serde_json::json!({"details": {
            "runtimeId":"test", "ioUring":{"ringEntries":64,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[], "reduction":{"enabled":true,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0}
        }});
        assert!(
            parse_details(&frontend)
                .runtime
                .unwrap()
                .metadata_reads
                .is_none()
        );
        frontend["details"]["metadataReads"] = serde_json::json!({"intervalSeconds":2.0,"rows":[{
            "reason":"indexAudit","object":"exactIndex","mode":"mmap",
            "operations":2,"requestedBytes":8192,"returnedBytes":8192,"errors":0,
            "elapsedMicros":0,"maxMicros":0,"inFlight":0,"operationsPerSecond":1.0,"requestedMbps":0.004096
        }]});
        let saved = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(
            saved["runtime"]["metadataReads"],
            frontend["details"]["metadataReads"]
        );
        let restored: DetailTelemetry = serde_json::from_value(saved).unwrap();
        assert_eq!(
            restored.runtime.unwrap().metadata_reads.unwrap().rows[0].mode,
            "mmap"
        );
    }

    #[test]
    fn codec_buffers_survive_api_history_and_missing_legacy_samples() {
        let mut frontend = serde_json::json!({"details": {
            "runtimeId":"test", "ioUring":{"ringEntries":64,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[], "reduction":{"enabled":true,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0}
        }});
        assert!(
            parse_details(&frontend)
                .runtime
                .unwrap()
                .codec_buffers
                .is_none()
        );
        frontend["details"]["codecBuffers"] = serde_json::json!({
            "retainedBytes":262144,"activeBytes":65536,"peakActiveBytes":524288,
            "hits":1999,"misses":1,"evictions":0
        });
        let saved = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(
            saved["runtime"]["codecBuffers"],
            frontend["details"]["codecBuffers"]
        );
        let restored: DetailTelemetry = serde_json::from_value(saved).unwrap();
        assert_eq!(restored.runtime.unwrap().codec_buffers.unwrap().hits, 1999);
    }

    #[test]
    fn collection_phases_and_membership_evidence_survive_the_history_roundtrip() {
        let mut frontend = serde_json::json!({"details": {
            "runtimeId":"test", "ioUring":{"ringEntries":64,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[], "reduction":{"enabled":true,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0},
            "gc":{"state":"collected","observedAt":100,"totalMs":12.0}
        }});
        let legacy = parse_details(&frontend).runtime.unwrap();
        assert!(
            legacy.exact_membership.is_none(),
            "old runtimes stay readable"
        );
        assert!(legacy.gc.unwrap().phases_ms.is_none());

        frontend["details"]["gc"]["phasesMs"] = serde_json::json!({
            "recovery":1.0,"metadataGc":2.0,"candidateCatalog":3.0,"candidateProof":4.0,
            "relocation":5.0,"retiringActivation":6.0,"pinDrain":7.0,"victimVerify":8.0,
            "unlink":9.0,"dataSync":10.0,"removedActivation":11.0,"postCollectionCatalog":12.0});
        frontend["details"]["gc"]["metadataGc"] = serde_json::json!({
            "markMode":"incremental","exactReason":"runRetirement","wallMs":13.0,"barrierWaitMs":14.0,
            "objectGraphReadBytes":15,"candidateReadBytes":16,"catalogReadBytes":17,
            "catalogWriteBytes":18,"unlinkedBytes":19,"rootSyncs":20,"catalogChainRuns":21});
        for (key, value) in [
            ("catalogExaminedBytes", 22),
            ("catalogWriteBytes", 23),
            ("candidateProofReadBytes", 24),
            ("reverseDependencyEdges", 25),
            ("reverseDependencyRequiredChunks", 26),
            ("candidateQueueRetained", 27),
            ("candidateQueueScannedRows", 28),
            ("catalogPendingUpdates", 29),
            ("exactRunsRetired", 30),
            ("exactRunSetsRetired", 31),
        ] {
            frontend["details"]["gc"][key] = serde_json::json!(value);
        }
        frontend["details"]["gc"]["exactRetirementMs"] = serde_json::json!(32.0);
        frontend["details"]["exactCache"] =
            serde_json::json!({"protectedLimitBytes":100,"protectedResidentBytes":40});
        frontend["details"]["exactMembership"] = serde_json::json!({
            "leasedRuns":8,"filters":7,"constructedFilters":2,"missingFilters":1,
            "pageBoundsRuns":7,"missingPageBounds":1,"pageBoundsBytes":4096,
            "probes":1000,"definitelyAbsent":940,"requiresExactLookup":60});
        frontend["details"]["exactWarm"] = serde_json::json!({"state":"warmed"});

        let saved = serde_json::to_value(parse_details(&frontend)).unwrap();
        for key in ["exactCache", "exactMembership", "exactWarm"] {
            assert_eq!(saved["runtime"][key], frontend["details"][key], "{key}");
        }
        // The older GcTelemetry options still serialize as explicit nulls, so
        // compare the added blocks rather than the whole record.
        for key in [
            "phasesMs",
            "metadataGc",
            "exactRetirementMs",
            "catalogExaminedBytes",
        ] {
            assert_eq!(
                saved["runtime"]["gc"][key], frontend["details"]["gc"][key],
                "gc.{key}"
            );
        }
        let restored: DetailTelemetry = serde_json::from_value(saved).unwrap();
        let runtime = restored.runtime.unwrap();
        let gc = runtime.gc.unwrap();
        assert!((gc.phases_ms.unwrap().post_collection_catalog - 12.0).abs() < 1e-9);
        assert_eq!(gc.metadata_gc.unwrap().mark_mode, "incremental");
        assert_eq!(gc.exact_run_sets_retired, Some(31));
        assert_eq!(runtime.exact_membership.unwrap().definitely_absent, 940);
        assert_eq!(runtime.exact_cache.unwrap().protected_resident_bytes, 40);
        assert_eq!(runtime.exact_warm.unwrap().state, "warmed");
    }

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
        assert!(
            details
                .runtime
                .as_ref()
                .unwrap()
                .read_cache_compression
                .is_none()
        );
        frontend["details"]["readCacheCompression"] = serde_json::json!({
            "decodedResidentBytes":1000,"compressedResidentBytes":2000,"compressedLogicalBytes":8000,
            "attempts":10,"admissions":8,"compressionNanos":1000000,"hits":20,"decompressions":10,
            "decompressionNanos":500000,"promotions":2,"demotions":1,"failures":0,"bypasses":2,
            "workingBytes":0,"peakWorkingBytes":100000,"maxWorkingBytes":200000
        });
        assert!(details.runtime.as_ref().unwrap().allocator_memory.is_none());
        frontend["details"]["allocatorMemory"] = serde_json::json!({
            "arenaBytes":900,"allocatedBytes":300,"freeBytes":600,
            "anonymousResidentBytes":400,"trimAttempts":2,"lastTrimMicros":1000
        });
        let roundtrip = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(
            roundtrip["runtime"]["allocatorMemory"],
            frontend["details"]["allocatorMemory"]
        );
        assert_eq!(
            roundtrip["runtime"]["readCacheCompression"],
            frontend["details"]["readCacheCompression"]
        );
        let saved = serde_json::to_value(&details).unwrap();
        assert_eq!(saved["runtime"]["scrub"], frontend["details"]["scrub"]);
        frontend["details"]["scrub"]["resumedContainers"] = serde_json::json!(2);
        frontend["details"]["scrub"]["newlyVerifiedContainers"] = serde_json::json!(1);
        frontend["details"]["scrub"]["remainingContainers"] = serde_json::json!(9);
        let resumed = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(resumed["runtime"]["scrub"], frontend["details"]["scrub"]);
        assert_eq!(
            saved["runtime"]["cacheBudget"],
            frontend["details"]["cacheBudget"]
        );
        assert!(
            saved["runtime"]["reduction"]
                .get("backendBaseReads")
                .is_none()
        );
        for (key, value) in [
            ("backendBaseReads", 140),
            ("skippedColdCandidates", 931),
            ("explorationReads", 32),
            ("warmBaseReuses", 710),
            ("successfulBaseTrials", 411),
        ] {
            frontend["details"]["reduction"][key] = serde_json::json!(value);
        }
        let with_gate = serde_json::to_value(parse_details(&frontend)).unwrap();
        assert_eq!(
            with_gate["runtime"]["reduction"],
            frontend["details"]["reduction"]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed_containers: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newly_verified_containers: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_containers: Option<u64>,
    pub verified_bytes: u64,
    pub read_bytes: u64,
    pub current_container: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheWindowTelemetry {
    pub seconds: u64,
    pub pools: Vec<CacheWindowCounters>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheWindowCounters {
    pub id: String,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Bounded runtime observations, stored unchanged with each historical sample.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineTelemetry {
    pub operations: Vec<PipelineOperation>,
    pub admission: AdmissionTelemetry,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineOperation {
    pub id: String,
    pub active: u64,
    pub completed: u64,
    pub total_ms: f64,
    pub maximum_ms: f64,
    pub busy_ms: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionTelemetry {
    pub open: bool,
    pub reason: Option<String>,
    pub closures: u64,
    pub closed_ms: f64,
    pub current_closed_ms: f64,
    pub maximum_closed_ms: f64,
}
