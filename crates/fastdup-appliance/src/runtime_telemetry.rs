//! Bounded, read-only management observations. No storage decisions depend on these.
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    FsAppliance, OnlineGcCycleOutcome, OnlineGcCycleReport, ProfiledCheckpoint, TelemetryStorageIo,
};
use serde_json::{Value, json};

static CHECKPOINT: Mutex<Option<Value>> = Mutex::new(None);
static SCRUB: Mutex<Option<Value>> = Mutex::new(None);

pub fn record_scrub(value: Value) {
    if let Ok(mut status) = SCRUB.lock() {
        *status = Some(value);
    }
}

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
    let reduction = appliance.advanced_reduction_status();
    let history = appliance.historical_proof_cache_status();
    let budget = fastdup_store::cache_budget_status();
    let pools: Vec<_> = budget
        .pools
        .iter()
        .map(|pool| {
            json!({"id":pool.name, "fallbackTier":match pool.fallback {
            fastdup_store::CacheFallback::Data => "data",
            fastdup_store::CacheFallback::Metadata => "metadata",
        }, "residentBytes":pool.resident_bytes, "targetBytes":pool.target_bytes,
            "leasedBytes":pool.leased_bytes, "hits":pool.hits, "misses":pool.misses,
            "evictions":pool.evictions})
        })
        .collect();
    json!({
        "cacheBudget": {"maximumMemoryUsedBasisPoints":9200,
            "effectiveLimitBytes":budget.effective_limit_bytes,
            "availableBytes":budget.available_bytes,
            "budgetBytes":budget.budget_bytes, "pools":pools},
        "readCacheCompression": {
            "decodedResidentBytes":read.resident_bytes().saturating_sub(read.compressed_resident_bytes()),
            "compressedResidentBytes":read.compressed_resident_bytes(),
            "compressedLogicalBytes":read.compressed_logical_bytes(),
            "attempts":read.compression_attempts(), "admissions":read.compressed_admissions(),
            "compressionNanos":read.compression_nanos(), "hits":read.compressed_hits(),
            "decompressions":read.decompressions(), "decompressionNanos":read.decompression_nanos(),
            "promotions":read.promotions(), "demotions":read.demotions(),
            "failures":read.compression_failures(), "bypasses":read.compression_bypasses(),
            "workingBytes":read.codec_working_bytes(), "peakWorkingBytes":read.codec_peak_working_bytes(),
            "maxWorkingBytes":read.codec_max_working_bytes()
        },
        "scrub": SCRUB.lock().ok().and_then(|status| status.clone()),
        "runtimeId": format!("{}", std::process::id()),
        "ioUring": {"ringEntries":io.ring_entries(), "inflightBytes":io.inflight_bytes(),
            "maxInflightBytes":io.max_inflight_bytes(), "peakInflightBytes":io.peak_inflight_bytes(),
            "submitted":io.submitted_operations(), "completed":io.completed_operations()},
        "caches": [
            {"id":"verifiedRead", "hits":read.hits(), "misses":read.misses(), "evictions":read.evictions(), "residentBytes":read.resident_bytes()},
            {"id":"exactIndex", "hits":exact.hits(), "misses":exact.misses(), "evictions":exact.evictions(), "residentPages":exact.resident_pages()},
            {"id":"similarityIndex", "hits":similarity.hits(), "misses":similarity.misses(), "evictions":similarity.evictions(), "residentPages":similarity.resident_pages()},
            {"id":"containerDescriptors", "hits":descriptors.hits(), "misses":descriptors.misses(), "evictions":descriptors.evictions(), "residentBytes":descriptors.resident_bytes()},
            {"id":"historicalProofs", "hits":history.hits(), "misses":history.misses(), "evictions":history.evictions(), "residentBytes":history.resident_bytes()}
        ],
        "reduction": {"skippedColdCandidates":reduction.skipped_cold_candidates(), "explorationReads":reduction.exploration_reads(), "backendBaseReads":reduction.backend_base_reads(), "warmBaseReuses":reduction.warm_base_reuses(), "successfulBaseTrials":reduction.successful_base_trials(), "enabled":reduction.enabled(), "queries":reduction.queries(), "candidates":reduction.candidates(),
            "acceptedPrefixes":reduction.accepted_prefixes(), "acceptedSparseXor":reduction.accepted_sparse_xor(),
            "savedPayloadBytes":reduction.saved_payload_bytes(), "fallbacks":reduction.independent_fallbacks(), "errors":reduction.errors()},
        "checkpoint":CHECKPOINT.lock().ok().and_then(|last| last.clone()),
        "gc":GC.lock().ok().and_then(|last| last.clone()),
    })
}
