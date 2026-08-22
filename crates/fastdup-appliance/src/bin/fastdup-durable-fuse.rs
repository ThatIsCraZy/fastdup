use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fastdup_appliance::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointAction, CheckpointPressure, DurableNamespace,
    MUTATION_COMMIT_TARGET, ProfiledCheckpoint, checkpoint_action, checkpoint_policy_set_v1,
};
use fastdup_format::{HEADER_BYTES, SealedContainer};
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_posix::{FuseFilesystem, NamespaceConfig, volatile_mount_options};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
    OwnedContainerPublication, StorageIo, StoreError,
};
use fuse3::raw::Session;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, sleep};

const SCHEDULER_RESOLUTION: Duration = Duration::from_millis(50);
const CHECKPOINT_WARNING: Duration = Duration::from_secs(5);
const INODE_RESERVATION_SPAN: u64 = 4_096;

type FsAppliance = DurableNamespace<FsStorageIo, TelemetryStorageIo>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1).map(PathBuf::from);
    let mount_path = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    let metadata_root = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    let container_root = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    if arguments.next().is_some() {
        return Err("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT".into());
    }
    if !mount_path.is_dir() {
        return Err(format!("mount path is not a directory: {}", mount_path.display()).into());
    }
    if metadata_root == container_root {
        return Err("metadata and container roots must be distinct".into());
    }

    let policy = checkpoint_policy_set_v1();
    let io_telemetry_enabled = std::env::var_os("FASTDUP_IO_TELEMETRY").is_some();
    let data_storage = open_data_storage(&container_root, io_telemetry_enabled)?;
    let appliance = Arc::new(DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(FsStorageIo::open(&metadata_root)?, policy),
        ContainerRepository::new(data_storage.clone()),
        &ExactIndexRunRepository::new(FsStorageIo::open(&metadata_root)?),
        INODE_RESERVATION_SPAN,
    )?);
    let filesystem = FuseFilesystem::new(appliance.namespace_arc());
    let session = Session::new(volatile_mount_options());
    let mount = session.mount(filesystem, &mount_path).await?;
    eprintln!(
        "fastdup durable checkpoint mount at {}; metadata+exact-index={}, containers={}, exact-index-runs={}, checkpoint-workers={}, dirty-checkpoint-bytes={}, exact-index-degraded={}",
        mount_path.display(),
        metadata_root.display(),
        container_root.display(),
        appliance.exact_index_run_count(),
        appliance.checkpoint_worker_limit(),
        CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1,
        appliance.exact_index_degraded(),
    );
    emit_io_telemetry_state(io_telemetry_enabled);
    emit_io_uring_state(&data_storage);
    emit_verified_read_cache(&appliance);

    let mut ticks = interval(SCHEDULER_RESOLUTION);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await;
    let mut oldest_dirty = None::<Instant>;
    let mut last_checkpoint_attempt = Instant::now();
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            _ = ticks.tick() => {
                let action = heartbeat_action(
                    observe_checkpoint_action(&appliance, &mut oldest_dirty),
                    last_checkpoint_attempt,
                );
                if !matches!(action, CheckpointAction::Wait(_)) {
                    if matches!(action, CheckpointAction::PauseAndCommit(_)) {
                        appliance.namespace().pause_mutation_admission();
                    }
                    let checkpoint_started = Instant::now();
                    if let Err(error) = checkpoint_cycle(Arc::clone(&appliance)).await {
                        appliance.namespace().pause_mutation_admission();
                        eprintln!(
                            "CRITICAL: durable progress failed; mutation admission remains closed: {error}"
                        );
                    }
                    oldest_dirty = (appliance.namespace().checkpointable_dirty_payload_bytes() != 0)
                        .then_some(checkpoint_started);
                    last_checkpoint_attempt = Instant::now();
                }
            }
            dirty_bytes = appliance
                .namespace()
                .wait_for_checkpointable_dirty_payload(CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1) => {
                appliance.namespace().pause_mutation_admission();
                emit_checkpoint_pressure(&appliance, dirty_bytes, false);
                if let Err(error) = checkpoint_cycle(Arc::clone(&appliance)).await {
                    eprintln!(
                        "CRITICAL: pressure checkpoint failed; mutation admission remains closed: {error}"
                    );
                }
                last_checkpoint_attempt = Instant::now();
            }
        }
    }

    appliance.namespace().pause_mutation_admission();
    if let Err(error) = catch_up(Arc::clone(&appliance)).await {
        eprintln!("CRITICAL: final checkpoint failed during shutdown: {error}");
    }
    mount.unmount().await?;
    emit_verified_read_cache(&appliance);
    data_storage.emit();
    emit_io_uring_state(&data_storage);
    Ok(())
}

fn open_data_storage(root: &std::path::Path, telemetry: bool) -> io::Result<TelemetryStorageIo> {
    let config = IoUringStorageConfig::default();
    let storage = match std::env::var("FASTDUP_IO_URING").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("off") => {
            IoUringStorageIo::open_synchronous(root, config)?
        }
        Ok("try") => IoUringStorageIo::open_or_fallback(root, config)?,
        Ok("required") => IoUringStorageIo::open_required(root, config)?,
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FASTDUP_IO_URING must be off, try, or required",
            ));
        }
    };
    Ok(TelemetryStorageIo::new(storage, telemetry))
}

fn emit_io_telemetry_state(enabled: bool) {
    if enabled {
        eprintln!("data-tier StorageIo telemetry is enabled for this mount");
    }
}

fn emit_io_uring_state(storage: &TelemetryStorageIo) {
    let status = storage.inner.status();
    eprintln!(
        concat!(
            "data_io_uring mode={:?} ring_entries={} max_inflight_bytes={} ",
            "inflight_bytes={} peak_inflight_bytes={} submitted_operations={} ",
            "completed_operations={} root_sync_callers={} root_sync_submissions={} ",
            "owned_publications_started={} owned_publications_completed={} ",
            "borrowed_write_copy_bytes={} ",
            "verifier_workers={} verification_jobs_started={} ",
            "verification_jobs_completed={} verification_jobs_failed={} ",
            "parallel_hash_verifications={} ",
            "active_verifications={} peak_active_verifications={} ",
            "fallback_reason={:?}"
        ),
        status.mode(),
        status.ring_entries(),
        status.max_inflight_bytes(),
        status.inflight_bytes(),
        status.peak_inflight_bytes(),
        status.submitted_operations(),
        status.completed_operations(),
        status.root_sync_callers(),
        status.root_sync_submissions(),
        status.owned_publications_started(),
        status.owned_publications_completed(),
        status.borrowed_write_copy_bytes(),
        status.verifier_workers(),
        status.verification_jobs_started(),
        status.verification_jobs_completed(),
        status.verification_jobs_failed(),
        status.parallel_hash_verifications(),
        status.active_verifications(),
        status.peak_active_verifications(),
        status.fallback_reason(),
    );
}

fn emit_write_through_cpu_state(appliance: &FsAppliance) {
    let status = appliance.write_through_status();
    eprintln!(
        "write_through_cpu hash_batches={} maximum_hash_workers={}",
        status.hash_batches(),
        status.maximum_hash_workers(),
    );
}

fn emit_checkpoint_pressure(appliance: &FsAppliance, dirty_bytes: u64, running: bool) {
    let staged = appliance.write_through_status();
    let context = if running {
        " while a checkpoint was running"
    } else {
        ""
    };
    eprintln!(
        "checkpoint pressure reached {dirty_bytes} active dirty bytes{context}; mutation admission is closed; active_lanes={} buffered_bytes={} sealed={} degraded={}",
        staged.active_lanes(),
        staged.buffered_bytes(),
        staged.sealed_uncommitted_containers(),
        staged.degraded(),
    );
}

fn heartbeat_action(action: CheckpointAction, last_attempt: Instant) -> CheckpointAction {
    if matches!(action, CheckpointAction::Wait(_))
        && last_attempt.elapsed() >= MUTATION_COMMIT_TARGET
    {
        CheckpointAction::Commit(fastdup_appliance::CheckpointTrigger::MutationAge)
    } else {
        action
    }
}

fn observe_checkpoint_action(
    appliance: &FsAppliance,
    oldest_dirty: &mut Option<Instant>,
) -> CheckpointAction {
    let now = Instant::now();
    if appliance.namespace().checkpointable_dirty_payload_bytes() == 0 {
        *oldest_dirty = None;
    } else {
        oldest_dirty.get_or_insert(now);
    }
    let staged = appliance.write_through_status();
    checkpoint_action(CheckpointPressure {
        oldest_mutation_age: oldest_dirty.map(|started| now.duration_since(started)),
        oldest_sealed_container_age: staged.oldest_sealed_age(),
        sealed_uncommitted_containers: staged.sealed_uncommitted_containers(),
    })
}

async fn checkpoint_cycle(appliance: Arc<FsAppliance>) -> Result<(), String> {
    let already_paused = !appliance.namespace().mutation_admission_open();
    let worker_appliance = Arc::clone(&appliance);
    let mut worker = tokio::task::spawn_blocking(move || worker_appliance.checkpoint_profiled());
    let result = if already_paused {
        await_worker(worker).await?
    } else {
        tokio::select! {
            result = &mut worker => map_worker_result(result)?,
            () = sleep(CHECKPOINT_WARNING) => {
                appliance.namespace().pause_mutation_admission();
                eprintln!(
                    "CRITICAL: checkpoint exceeded five seconds; mutation admission is closed"
                );
                await_worker(worker).await?
            }
            dirty_bytes = appliance
                .namespace()
                .wait_for_checkpointable_dirty_payload(CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1) => {
                appliance.namespace().pause_mutation_admission();
                emit_checkpoint_pressure(&appliance, dirty_bytes, true);
                await_worker(worker).await?
            }
        }
    };
    if let Some(profiled) = result {
        emit_checkpoint_metrics(&profiled);
    }
    emit_verified_read_cache(&appliance);
    if !appliance.namespace().mutation_admission_open() {
        catch_up(Arc::clone(&appliance)).await?;
        appliance.namespace().resume_mutation_admission();
        eprintln!("durable progress caught up; mutation admission reopened");
    }
    Ok(())
}

async fn catch_up(appliance: Arc<FsAppliance>) -> Result<(), String> {
    loop {
        let worker_appliance = Arc::clone(&appliance);
        let worker = tokio::task::spawn_blocking(move || worker_appliance.checkpoint_profiled());
        match await_worker(worker).await? {
            Some(profiled) => {
                emit_checkpoint_metrics(&profiled);
                emit_verified_read_cache(&appliance);
            }
            None => return Ok(()),
        }
    }
}

fn emit_verified_read_cache(appliance: &FsAppliance) {
    emit_write_through_cpu_state(appliance);
    let membership = appliance.exact_run_membership_status();
    eprintln!(
        concat!(
            "exact_run_membership filters={} allocated_bytes={} probes={} ",
            "definitely_absent={} requires_exact_lookup={}"
        ),
        membership.filter_count(),
        membership.allocated_bytes(),
        membership.probes(),
        membership.definitely_absent(),
        membership.requires_exact_lookup(),
    );
    let exact = appliance.exact_index_page_cache_status();
    eprintln!(
        concat!(
            "exact_index_page_cache hits={} misses={} hit_rate_basis_points={} ",
            "resident_pages={} target_pages={} capacity_pages={} evictions={} ",
            "pressure_rejections={} reserve_bytes={} effective_limit_bytes={} ",
            "available_bytes={} swap_used_bytes={}"
        ),
        exact.hits(),
        exact.misses(),
        exact.hit_rate_basis_points(),
        exact.resident_pages(),
        exact.target_pages(),
        exact.capacity_pages(),
        exact.evictions(),
        exact.pressure_rejections(),
        exact.reserve_bytes(),
        exact.effective_limit_bytes(),
        exact.available_bytes(),
        exact.swap_used_bytes(),
    );
    let descriptors = appliance.container_descriptor_cache_status();
    eprintln!(
        concat!(
            "container_descriptor_cache hits={} misses={} hit_rate_basis_points={} ",
            "admissions={} evictions={} pressure_rejections={} allocation_rejections={} ",
            "capacity={} target_entries={} entries={} resident_bytes={} metadata_bytes={} ",
            "hard_coverage_bytes={} target_coverage_bytes={} effective_limit_bytes={} ",
            "available_bytes={} swap_used_bytes={}"
        ),
        descriptors.hits(),
        descriptors.misses(),
        descriptors.hit_rate_basis_points(),
        descriptors.admissions(),
        descriptors.evictions(),
        descriptors.pressure_rejections(),
        descriptors.allocation_rejections(),
        descriptors.capacity(),
        descriptors.target_entries(),
        descriptors.entry_count(),
        descriptors.resident_bytes(),
        descriptors.metadata_bytes(),
        descriptors.hard_coverage_bytes(),
        descriptors.target_coverage_bytes(),
        descriptors.effective_limit_bytes(),
        descriptors.available_bytes(),
        descriptors.swap_used_bytes(),
    );
    emit_dependency_proof_caches(appliance);
    let cache = appliance.verified_read_cache_status();
    eprintln!(
        concat!(
            "verified_read_cache hits={} misses={} admissions={} evictions={} ",
            "pressure_rejections={} oversized_rejections={} entries={} resident_bytes={} ",
            "target_bytes={} metadata_bytes={} hard_limit_bytes={} reserve_bytes={} ",
            "effective_limit_bytes={} available_bytes={} swap_used_bytes={}"
        ),
        cache.hits(),
        cache.misses(),
        cache.admissions(),
        cache.evictions(),
        cache.pressure_rejections(),
        cache.oversized_rejections(),
        cache.entry_count(),
        cache.resident_bytes(),
        cache.target_bytes(),
        cache.metadata_bytes(),
        cache.hard_limit_bytes(),
        cache.reserve_bytes(),
        cache.effective_limit_bytes(),
        cache.available_bytes(),
        cache.swap_used_bytes(),
    );
}

fn emit_dependency_proof_caches(appliance: &FsAppliance) {
    let proofs = appliance.historical_proof_cache_status();
    eprintln!(
        concat!(
            "historical_proof_cache policy=s3-fifo hits={} misses={} ",
            "hit_rate_basis_points={} admissions={} admission_rejections={} ",
            "allocation_rejections={} evictions={} ghost_hits={} entries={} ",
            "target_entries={} resident_bytes={} metadata_bytes={} hard_limit_bytes={} ",
            "reserve_bytes={} maximum_eviction_steps={} effective_limit_bytes={} ",
            "available_bytes={} swap_used_bytes={}"
        ),
        proofs.hits(),
        proofs.misses(),
        proofs.hit_rate_basis_points(),
        proofs.admissions(),
        proofs.admission_rejections(),
        proofs.allocation_rejections(),
        proofs.evictions(),
        proofs.ghost_hits(),
        proofs.entry_count(),
        proofs.target_entries(),
        proofs.resident_bytes(),
        proofs.metadata_bytes(),
        proofs.hard_limit_bytes(),
        proofs.reserve_bytes(),
        proofs.maximum_eviction_steps(),
        proofs.effective_limit_bytes(),
        proofs.available_bytes(),
        proofs.swap_used_bytes(),
    );
    let generation_proofs = appliance.generation_proof_set_status();
    eprintln!(
        "generation_proof_set active={} frozen={} accounted_bytes={}",
        generation_proofs.active_proofs(),
        generation_proofs.frozen_proofs(),
        generation_proofs.accounted_bytes(),
    );
}

async fn await_worker(
    worker: JoinHandle<
        Result<Option<ProfiledCheckpoint>, fastdup_appliance::DurableNamespaceError>,
    >,
) -> Result<Option<ProfiledCheckpoint>, String> {
    map_worker_result(worker.await)
}

fn map_worker_result(
    result: Result<
        Result<Option<ProfiledCheckpoint>, fastdup_appliance::DurableNamespaceError>,
        tokio::task::JoinError,
    >,
) -> Result<Option<ProfiledCheckpoint>, String> {
    result
        .map_err(|error| format!("checkpoint worker failed: {error}"))?
        .map_err(|error| format!("checkpoint failed: {error}"))
}

fn emit_checkpoint_metrics(profiled: &ProfiledCheckpoint) {
    let metrics = profiled.metrics();
    eprintln!(
        concat!(
            "checkpoint_metrics generation={} ",
            "total_wall_ns={} total_cpu_ns={} freeze_wall_ns={} freeze_cpu_ns={} ",
            "plan_wall_ns={} plan_cpu_ns={} cdc_wall_ns={} cdc_cpu_ns={} ",
            "hash_fill_wall_ns={} hash_fill_cpu_ns={} exact_wall_ns={} exact_cpu_ns={} ",
            "encode_wall_ns={} encode_cpu_ns={} container_publish_wall_ns={} ",
            "container_publish_cpu_ns={} index_wall_ns={} index_cpu_ns={} ",
            "metadata_wall_ns={} metadata_cpu_ns={} logical_chunks={} logical_bytes={} ",
            "fill_chunks={} fill_bytes={} exact_hit_chunks={} exact_hit_bytes={} ",
            "new_chunks={} new_bytes={} container_file_bytes={} raw_records={} ",
            "zstd_records={} containers={} peak_buffered_bytes={} peak_buffered_chunks={} ",
            "recipe_reuse_chunks={} recipe_reuse_bytes={} checkpoint_rechunk_bytes={}"
        ),
        profiled.record().generation(),
        metrics.total().wall().as_nanos(),
        metrics.total().process_cpu().as_nanos(),
        metrics.freeze().wall().as_nanos(),
        metrics.freeze().process_cpu().as_nanos(),
        metrics.manifest_plan().wall().as_nanos(),
        metrics.manifest_plan().process_cpu().as_nanos(),
        metrics.fastcdc().wall().as_nanos(),
        metrics.fastcdc().process_cpu().as_nanos(),
        metrics.hash_and_fill().wall().as_nanos(),
        metrics.hash_and_fill().process_cpu().as_nanos(),
        metrics.exact_lookup().wall().as_nanos(),
        metrics.exact_lookup().process_cpu().as_nanos(),
        metrics.compression_encode().wall().as_nanos(),
        metrics.compression_encode().process_cpu().as_nanos(),
        metrics.container_publish().wall().as_nanos(),
        metrics.container_publish().process_cpu().as_nanos(),
        metrics.exact_index_publish().wall().as_nanos(),
        metrics.exact_index_publish().process_cpu().as_nanos(),
        metrics.metadata_commit().wall().as_nanos(),
        metrics.metadata_commit().process_cpu().as_nanos(),
        metrics.logical_chunks(),
        metrics.logical_chunk_bytes(),
        metrics.fill_chunks(),
        metrics.fill_bytes(),
        metrics.exact_hit_chunks(),
        metrics.exact_hit_bytes(),
        metrics.new_chunks(),
        metrics.new_chunk_bytes(),
        metrics.container_file_bytes(),
        metrics.raw_records(),
        metrics.zstd_records(),
        metrics.containers(),
        metrics.peak_buffered_chunk_bytes(),
        metrics.peak_buffered_chunks(),
        metrics.recipe_reuse_chunks(),
        metrics.recipe_reuse_bytes(),
        metrics.checkpoint_rechunk_bytes(),
    );
}

#[derive(Clone, Debug)]
struct TelemetryStorageIo {
    inner: IoUringStorageIo,
    enabled: bool,
    telemetry: Arc<DataIoTelemetry>,
}

impl TelemetryStorageIo {
    fn new(inner: IoUringStorageIo, enabled: bool) -> Self {
        Self {
            inner,
            enabled,
            telemetry: Arc::new(DataIoTelemetry::default()),
        }
    }

    fn emit(&self) {
        if !self.enabled {
            return;
        }
        eprintln!(
            concat!(
                "data_io_metrics whole_reads={} whole_read_bytes={} range_reads={} ",
                "range_read_bytes={} random_range_reads={} writes={} write_bytes={} ",
                "nonsequential_writes={}"
            ),
            self.telemetry.whole_reads.load(Ordering::Relaxed),
            self.telemetry.whole_read_bytes.load(Ordering::Relaxed),
            self.telemetry.range_reads.load(Ordering::Relaxed),
            self.telemetry.range_read_bytes.load(Ordering::Relaxed),
            self.telemetry.random_range_reads.load(Ordering::Relaxed),
            self.telemetry.writes.load(Ordering::Relaxed),
            self.telemetry.write_bytes.load(Ordering::Relaxed),
            self.telemetry.nonsequential_writes.load(Ordering::Relaxed),
        );
    }
}

#[derive(Debug, Default)]
struct DataIoTelemetry {
    whole_reads: AtomicU64,
    whole_read_bytes: AtomicU64,
    range_reads: AtomicU64,
    range_read_bytes: AtomicU64,
    random_range_reads: AtomicU64,
    writes: AtomicU64,
    write_bytes: AtomicU64,
    nonsequential_writes: AtomicU64,
    last_read_end: Mutex<BTreeMap<String, u64>>,
    last_write_end: Mutex<BTreeMap<String, u64>>,
}

impl DataIoTelemetry {
    fn classify_range_read(&self, name: &str, offset: u64, length: usize) {
        let mut ends = self
            .last_read_end
            .lock()
            .expect("ASSERT: data-tier read telemetry lock poisoned");
        let random = ends.get(name).map_or(offset != 0, |end| *end != offset);
        let length = u64::try_from(length).expect("ASSERT: range-read length fits u64");
        let end = offset.saturating_add(length);
        ends.insert(name.to_owned(), end);
        if random {
            self.random_range_reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn classify_write(&self, name: &str, offset: u64, length: usize) {
        let mut ends = self
            .last_write_end
            .lock()
            .expect("ASSERT: data-tier write telemetry lock poisoned");
        let nonsequential = ends.get(name).map_or(offset != 0, |end| *end != offset);
        let length = u64::try_from(length).expect("ASSERT: write length fits u64");
        let end = offset.saturating_add(length);
        ends.insert(name.to_owned(), end);
        if nonsequential {
            self.nonsequential_writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_owned_publication(&self, sealed_bytes: usize) {
        let sealed_bytes =
            u64::try_from(sealed_bytes).expect("ASSERT: format-v1 Container length fits u64");
        let durable_write_bytes = sealed_bytes
            .checked_add(
                u64::try_from(HEADER_BYTES).expect("ASSERT: format Header length fits u64"),
            )
            .expect("ASSERT: bounded Container publication bytes cannot overflow");
        self.whole_reads.fetch_add(1, Ordering::Relaxed);
        self.whole_read_bytes
            .fetch_add(sealed_bytes, Ordering::Relaxed);
        self.writes.fetch_add(3, Ordering::Relaxed);
        self.write_bytes
            .fetch_add(durable_write_bytes, Ordering::Relaxed);
        self.nonsequential_writes.fetch_add(1, Ordering::Relaxed);
    }
}

impl StorageIo for TelemetryStorageIo {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.inner.create_new(name)?;
        if self.enabled {
            self.telemetry
                .last_write_end
                .lock()
                .expect("ASSERT: data-tier write telemetry lock poisoned")
                .insert(name.to_owned(), 0);
        }
        Ok(())
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.inner.exists(name)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_at(name, offset, bytes)?;
        if !self.enabled {
            return Ok(());
        }
        self.telemetry.classify_write(name, offset, bytes.len());
        self.telemetry.writes.fetch_add(1, Ordering::Relaxed);
        self.telemetry.write_bytes.fetch_add(
            u64::try_from(bytes.len()).expect("ASSERT: write length fits u64"),
            Ordering::Relaxed,
        );
        Ok(())
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read(name)?;
        if !self.enabled {
            return Ok(bytes);
        }
        self.telemetry.whole_reads.fetch_add(1, Ordering::Relaxed);
        self.telemetry.whole_read_bytes.fetch_add(
            u64::try_from(bytes.len()).expect("ASSERT: whole-object read length fits u64"),
            Ordering::Relaxed,
        );
        Ok(bytes)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.inner.object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_exact_at(name, offset, length)?;
        if !self.enabled {
            return Ok(bytes);
        }
        self.telemetry.classify_range_read(name, offset, length);
        self.telemetry.range_reads.fetch_add(1, Ordering::Relaxed);
        self.telemetry.range_read_bytes.fetch_add(
            u64::try_from(length).expect("ASSERT: range-read length fits u64"),
            Ordering::Relaxed,
        );
        Ok(bytes)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.inner.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.inner.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.inner.sync_file(name)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.inner.publish_noreplace(temporary_name, published_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.inner.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.inner.sync_root()
    }

    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<SealedContainer, StoreError> {
        let sealed_bytes = publication.sealed_len();
        let verified = self.inner.publish_owned_container(publication)?;
        if self.enabled {
            self.telemetry.record_owned_publication(sealed_bytes);
        }
        Ok(verified)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use fastdup_format::ContainerId;

    use super::*;

    #[test]
    fn telemetry_adapter_preserves_owned_container_publication() {
        let root =
            std::env::temp_dir().join(format!("fastdup-telemetry-owned-{}", std::process::id()));
        std::fs::create_dir(&root).expect("create unique test root");
        let inner = IoUringStorageIo::open_required(&root, IoUringStorageConfig::default())
            .expect("test requires the production io_uring backend");
        let storage = TelemetryStorageIo::new(inner, true);
        let repository = ContainerRepository::new(storage.clone());
        let chunk = b"telemetry-owned-publication".repeat(8_192);
        let region = [chunk.as_slice()];
        let regions = [region.as_slice()];
        let prepared =
            ContainerRepository::<TelemetryStorageIo>::prepare_adaptive_regions_parallel(
                ContainerId::new([0xA7; 16]).expect("fixture Container ID is nonzero"),
                1,
                &regions,
                NonZeroUsize::MIN,
            )
            .expect("prepare fixture Container");

        let (_, metrics) = repository
            .publish_prepared_adaptive_profiled(prepared)
            .expect("publish fixture Container through telemetry adapter");

        let status = storage.inner.status();
        assert_eq!(status.owned_publications_started(), 1);
        assert_eq!(status.owned_publications_completed(), 1);
        assert_eq!(status.borrowed_write_copy_bytes(), 0);
        assert_eq!(storage.telemetry.whole_reads.load(Ordering::Relaxed), 1);
        assert_eq!(
            storage.telemetry.whole_read_bytes.load(Ordering::Relaxed),
            metrics.file_bytes()
        );
        assert_eq!(storage.telemetry.writes.load(Ordering::Relaxed), 3);
        assert_eq!(
            storage.telemetry.write_bytes.load(Ordering::Relaxed),
            metrics.file_bytes()
                + u64::try_from(HEADER_BYTES).expect("format Header length fits u64")
        );
        assert_eq!(
            storage
                .telemetry
                .nonsequential_writes
                .load(Ordering::Relaxed),
            1
        );

        drop(repository);
        drop(storage);
        std::fs::remove_dir_all(root).expect("remove test root");
    }
}
