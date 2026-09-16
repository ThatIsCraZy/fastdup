//! Bounded, read-only management observations. No storage decisions depend on these.
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    FsAppliance, OnlineGcCycleOutcome, OnlineGcCycleReport, ProfiledCheckpoint, TelemetryStorageIo,
};
use serde_json::{Value, json};

static CHECKPOINT: Mutex<Option<Value>> = Mutex::new(None);
static SCRUB: Mutex<Option<Value>> = Mutex::new(None);
static EXACT_HIT_BYTES: AtomicU64 = AtomicU64::new(0);
static NEW_CHUNK_BYTES: AtomicU64 = AtomicU64::new(0);
static LOGICAL_CHUNK_BYTES: AtomicU64 = AtomicU64::new(0);
static PHYSICAL_CONTAINER_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IngestCounters {
    pub exact_hit: u64,
    pub new_chunk: u64,
    pub logical_chunk: u64,
    pub physical_container: u64,
}

pub fn ingest_counters() -> IngestCounters {
    IngestCounters {
        exact_hit: EXACT_HIT_BYTES.load(Ordering::Relaxed),
        new_chunk: NEW_CHUNK_BYTES.load(Ordering::Relaxed),
        logical_chunk: LOGICAL_CHUNK_BYTES.load(Ordering::Relaxed),
        physical_container: PHYSICAL_CONTAINER_BYTES.load(Ordering::Relaxed),
    }
}

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
    EXACT_HIT_BYTES.fetch_add(metrics.exact_hit_bytes(), Ordering::Relaxed);
    NEW_CHUNK_BYTES.fetch_add(metrics.new_chunk_bytes(), Ordering::Relaxed);
    LOGICAL_CHUNK_BYTES.fetch_add(metrics.logical_chunk_bytes(), Ordering::Relaxed);
    PHYSICAL_CONTAINER_BYTES.fetch_add(metrics.container_file_bytes(), Ordering::Relaxed);
    let phases: Vec<_> = [
        ("checkpointLock", metrics.checkpoint_lock()),
        ("proofFreeze", metrics.proof_freeze()),
        ("cutCapture", metrics.cut_capture()),
        ("freeze", metrics.freeze()),
        ("ingestWait", metrics.ingest_wait()),
        ("publicationWait", metrics.publication_wait()),
        ("laneLock", metrics.lane_lock()),
        ("stableExtract", metrics.stable_extract()),
        ("publicationEnqueue", metrics.publication_enqueue()),
        ("publicationRetire", metrics.publication_retire()),
        ("recipeAttach", metrics.recipe_attach()),
        ("writerSetup", metrics.writer_setup()),
        ("manifestPlan", metrics.manifest_plan()),
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
            "unattributedMs": metrics.unattributed().as_secs_f64() * 1000.0,
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
                OnlineGcCycleOutcome::MetadataOnly => "metadataOnly",
                OnlineGcCycleOutcome::DataOnly => "dataOnly",
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

pub fn gc_cancelled() {
    if let Ok(mut last) = GC.lock() {
        *last = Some(json!({"state":"cancelled", "observedAt":unix_seconds()}));
    }
}

static EXACT_WARM: Mutex<Option<Value>> = Mutex::new(None);

pub fn record_exact_warm(value: Value) {
    if let Ok(mut last) = EXACT_WARM.lock() {
        *last = Some(value);
    }
}

#[allow(clippy::too_many_lines, reason = "management snapshot projection")]
pub fn snapshot(appliance: &FsAppliance, storage: &TelemetryStorageIo) -> Value {
    let admission = appliance.namespace().admission_status();
    let pipeline: Vec<_> = appliance
        .pipeline_timings()
        .into_iter()
        .map(|phase| {
            json!({
                "id":phase.id, "active":phase.active, "completed":phase.completed,
                "totalMs":phase.total.as_secs_f64() * 1000.0,
                "maximumMs":phase.maximum.as_secs_f64() * 1000.0,
                "busyMs":phase.busy.as_secs_f64() * 1000.0,
            })
        })
        .collect();
    let io = storage.inner.status();
    let read = appliance.verified_read_cache_status();
    let exact = appliance.exact_index_page_cache_status();
    let membership = appliance.exact_run_membership_status();
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
            fastdup_store::CacheFallback::Memory => "memory",
        }, "residentBytes":pool.resident_bytes, "targetBytes":pool.target_bytes,
            "leasedBytes":pool.leased_bytes, "hits":pool.hits, "misses":pool.misses,
            "evictions":pool.evictions})
        })
        .collect();
    let metadata_reads = fastdup_store::metadata_read_status();
    let metadata_rows: Vec<_> = metadata_reads.rows.iter().map(|row| json!({
        "reason":row.reason,"object":row.object,"mode":row.mode,
        "operations":row.operations,"requestedBytes":row.requested_bytes,"returnedBytes":row.returned_bytes,
        "errors":row.errors,"elapsedMicros":row.elapsed_micros,"maxMicros":row.max_micros,
        "inFlight":row.in_flight,"operationsPerSecond":row.operations_per_second,"requestedMbps":row.requested_mbps
    })).collect();
    let buffers = read.buffer_pool();
    let allocator = fastdup_store::allocator_memory_status().map(|status| {
        json!({
            "arenaBytes":status.arena_bytes, "allocatedBytes":status.allocated_bytes,
            "freeBytes":status.free_bytes, "anonymousResidentBytes":status.anonymous_resident_bytes,
            "trimAttempts":status.trim_attempts, "lastTrimMicros":status.last_trim_micros,
        })
    });
    json!({
        "metadataReads": {"intervalSeconds":metadata_reads.interval_seconds,"rows":metadata_rows},
        "allocatorMemory": allocator,
        "cacheBudget": {"maximumMemoryUsedBasisPoints":9200,
            "effectiveLimitBytes":budget.effective_limit_bytes,
            "availableBytes":budget.available_bytes,
            "budgetBytes":budget.budget_bytes, "pools":pools},
        "codecBuffers": {"retainedBytes": buffers.retained_bytes, "activeBytes": buffers.active_bytes,
            "peakActiveBytes": buffers.peak_active_bytes, "hits": buffers.hits,
            "misses": buffers.misses, "evictions": buffers.evictions},
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
        "exactCache": {"protectedLimitBytes":exact.protected_limit_bytes(),
            "protectedResidentBytes":exact.protected_resident_bytes()},
        "exactMembership": {"leasedRuns":membership.leased_run_count(),
            "filters":membership.filter_count(),"constructedFilters":membership.constructed_filter_count(),
            "missingFilters":membership.missing_filter_count(),
            "pageBoundsRuns":membership.leased_run_count_with_bounds(),
            "missingPageBounds":membership.missing_page_bounds_count(),
            "pageBoundsBytes":membership.leased_page_bounds_bytes(),
            "probes":membership.probes(),"definitelyAbsent":membership.definitely_absent(),
            "requiresExactLookup":membership.requires_exact_lookup()},
        "exactWarm": EXACT_WARM.lock().ok().and_then(|status| status.clone()),
        "runtimeId": format!("{}", std::process::id()),
        "ioUring": {"ringEntries":io.ring_entries(), "inflightBytes":io.inflight_bytes(),
            "maxInflightBytes":io.max_inflight_bytes(), "peakInflightBytes":io.peak_inflight_bytes(),
            "submitted":io.submitted_operations(), "completed":io.completed_operations()},
        "caches": [
            {"id":"locationProofs", "hits":read.location_proofs().hits, "misses":read.location_proofs().misses, "evictions":read.location_proofs().evictions, "residentBytes":read.location_proofs().resident_bytes},
            {"id":"verifiedRead", "hits":read.hits(), "misses":read.misses(), "evictions":read.evictions(), "residentBytes":read.resident_bytes()},
            {"id":"exactIndex", "hits":exact.hits(), "misses":exact.misses(), "evictions":exact.evictions(), "residentPages":exact.resident_pages(), "protectedLimitBytes":exact.protected_limit_bytes(), "protectedResidentBytes":exact.protected_resident_bytes()},
            {"id":"similarityIndex", "hits":similarity.hits(), "misses":similarity.misses(), "evictions":similarity.evictions(), "residentPages":similarity.resident_pages()},
            {"id":"containerDescriptors", "hits":descriptors.hits(), "misses":descriptors.misses(), "evictions":descriptors.evictions(), "residentBytes":descriptors.resident_bytes()},
            {"id":"historicalProofs", "hits":history.hits(), "misses":history.misses(), "evictions":history.evictions(), "residentBytes":history.resident_bytes()}
        ],
        "reduction": {"skippedColdCandidates":reduction.skipped_cold_candidates(), "explorationReads":reduction.exploration_reads(), "backendBaseReads":reduction.backend_base_reads(), "warmBaseReuses":reduction.warm_base_reuses(), "successfulBaseTrials":reduction.successful_base_trials(), "enabled":reduction.enabled(), "queries":reduction.queries(), "candidates":reduction.candidates(),
            "acceptedPrefixes":reduction.accepted_prefixes(), "acceptedSparseXor":reduction.accepted_sparse_xor(),
            "savedPayloadBytes":reduction.saved_payload_bytes(), "fallbacks":reduction.independent_fallbacks(), "errors":reduction.errors()},
        "pipeline": {"operations":pipeline, "admission": {
            "open":admission.open, "reason":admission.reason.map(fastdup_posix::AdmissionPauseReason::name),
            "closures":admission.closures, "closedMs":admission.closed.as_secs_f64() * 1000.0,
            "currentClosedMs":admission.current_closed.as_secs_f64() * 1000.0,
            "maximumClosedMs":admission.maximum_closed.as_secs_f64() * 1000.0,
        }},
        "checkpoint":CHECKPOINT.lock().ok().and_then(|last| last.clone()),
        "gc":GC.lock().ok().and_then(|last| last.clone()),
    })
}
