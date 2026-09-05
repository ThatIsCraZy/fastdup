//! Bounded, read-only management observations. No storage decisions depend on these.
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    FsAppliance, OnlineGcCycleOutcome, OnlineGcCycleReport, ProfiledCheckpoint, TelemetryStorageIo,
};
use serde_json::{Value, json};

static CHECKPOINT: Mutex<Option<Value>> = Mutex::new(None);
static GC: Mutex<Option<Value>> = Mutex::new(None);

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn record_checkpoint(profiled: &ProfiledCheckpoint) {
    let metrics = profiled.metrics();
    let phases: Vec<_> = [
        ("freeze", metrics.freeze()),
        ("cdc", metrics.cdc()),
        ("hashFill", metrics.hash_and_fill()),
        ("exactLookup", metrics.exact_lookup()),
        ("encode", metrics.compression_encode()),
        ("containerPublish", metrics.container_publish()),
        ("indexPublish", metrics.exact_index_publish()),
        ("metadataCommit", metrics.metadata_commit()),
    ]
    .into_iter()
    .map(|(id, phase)| {
        json!({
            "id": id, "wallMs": phase.wall().as_secs_f64() * 1000.0,
            "cpuMs": phase.process_cpu().as_secs_f64() * 1000.0,
        })
    })
    .collect();
    if let Ok(mut last) = CHECKPOINT.lock() {
        *last = Some(json!({
            "completedAt": unix_seconds(), "generation": profiled.record().generation(),
            "totalMs": metrics.total().wall().as_secs_f64() * 1000.0,
            "phases": phases,
        }));
    }
}

pub fn gc_started() {
    if let Ok(mut last) = GC.lock() {
        *last = Some(json!({"state":"running", "observedAt":unix_seconds()}));
    }
}

pub fn gc_finished(result: &Result<OnlineGcCycleReport, String>) {
    let value = match result {
        Ok(report) => {
            let m = report.metrics();
            let state = match report.outcome() {
                OnlineGcCycleOutcome::NoCandidates => "noCandidates",
                OnlineGcCycleOutcome::NoProfitableCandidates => "noProfitableCandidates",
                OnlineGcCycleOutcome::CatalogRebuilt => "catalogRebuilt",
                OnlineGcCycleOutcome::Collected(_) => "collected",
            };
            json!({"state":state, "observedAt":unix_seconds(),
                "totalMs":m.total_wall().as_secs_f64() * 1000.0,
                "readBytes":m.relocation_read_bytes(), "writeBytes":m.relocation_write_bytes(),
                "unlinkedBytes":m.unlinked_bytes(), "candidates":m.shortlisted_candidates(),
                "victims":m.proved_victims(), "abortedCandidates":m.aborted_candidates()})
        }
        Err(_) => json!({"state":"failed", "observedAt":unix_seconds()}),
    };
    if let Ok(mut last) = GC.lock() {
        *last = Some(value);
    }
}

pub fn snapshot(appliance: &FsAppliance, storage: &TelemetryStorageIo) -> Value {
    let io = storage.inner.status();
    let read = appliance.verified_read_cache_status();
    let exact = appliance.exact_index_page_cache_status();
    let similarity = appliance.similarity_index_page_cache_status();
    let descriptors = appliance.container_descriptor_cache_status();
    let reduction = appliance.write_through_status().advanced_reduction();
    json!({
        "runtimeId": format!("{}", std::process::id()),
        "ioUring": {"ringEntries":io.ring_entries(), "inflightBytes":io.inflight_bytes(),
            "maxInflightBytes":io.max_inflight_bytes(), "peakInflightBytes":io.peak_inflight_bytes(),
            "submitted":io.submitted_operations(), "completed":io.completed_operations()},
        "caches": [
            {"id":"verifiedRead", "hits":read.hits(), "misses":read.misses(), "evictions":read.evictions(), "residentBytes":read.resident_bytes()},
            {"id":"exactIndex", "hits":exact.hits(), "misses":exact.misses(), "evictions":exact.evictions(), "residentPages":exact.resident_pages()},
            {"id":"similarityIndex", "hits":similarity.hits(), "misses":similarity.misses(), "evictions":similarity.evictions(), "residentPages":similarity.resident_pages()},
            {"id":"containerDescriptors", "hits":descriptors.hits(), "misses":descriptors.misses(), "evictions":descriptors.evictions(), "residentBytes":descriptors.resident_bytes()}
        ],
        "reduction": {"enabled":reduction.enabled(), "queries":reduction.queries(), "candidates":reduction.candidates(),
            "acceptedPrefixes":reduction.accepted_prefixes(), "acceptedSparseXor":reduction.accepted_sparse_xor(),
            "savedPayloadBytes":reduction.saved_payload_bytes(), "fallbacks":reduction.independent_fallbacks(), "errors":reduction.errors()},
        "checkpoint":CHECKPOINT.lock().ok().and_then(|last| last.clone()),
        "gc":GC.lock().ok().and_then(|last| last.clone()),
    })
}
