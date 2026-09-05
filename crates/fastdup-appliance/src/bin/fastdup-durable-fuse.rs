use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fastdup_appliance::OnlineGcSchedulerStatus;
use fastdup_appliance::{
    ApplianceLease, ApplianceLeaseOwner, AppliancePoolBinding, ApplianceRecoveryLatch,
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointAction, CheckpointProgressAction,
    DurabilityObservation, DurabilitySupervisor, DurableNamespace, INODE_RESERVATION_SPAN_V1,
    ONLINE_GC_CONTROL_REQUEST, OnlineGcPolicy, OnlineGcScheduler, PhysicalPoolIsolation,
    PoolIsolationPolicy, ProfiledCheckpoint, SMALL_FILE_PROJECT_ID_ENV, SMALL_FILE_QUOTA_BYTES_ENV,
    STATFS_RESERVE_BASIS_POINTS, SmallFileTierIsolation, StatFsOverride, TieredStatFsSource,
    bind_online_gc_control_socket, checkpoint_exact_index_profile_v1, checkpoint_policy_set,
    online_gc_control_path,
};
use fastdup_copy_metrics::copy_telemetry;
use fastdup_format::{HEADER_BYTES, VerifiedContainerPublication};
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_posix::{
    FrontendTelemetry, FuseFilesystem, InodeId, LogicalQuotaRule, Namespace, NamespaceConfig,
    volatile_mount_options,
};
use fastdup_store::{
    ContainerRepository, DataPoolUsage, ExactIndexRunRepository, FsStorageIo,
    GcCandidateCatalogRepository, GenerationRepository, IndexedRequiredChunkVerifier,
    MaintenanceRepository, OnlineGcCycleOutcome, OnlineGcCycleReport, OnlineGcRecoveryReport,
    OnlineGcRunMode, OwnedContainerPublication, RecoveryCheckpointRepository,
    SimilarityIndexRepository, StorageIo, StoreError, TieredStorageIo, publication_sample_ranges,
    system_memory_budget_governor,
};

mod common;
#[path = "../runtime_telemetry.rs"]
mod runtime_telemetry;

use common::metadata_gc_status_fields;
use fuse3::raw::Session;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};

use serde::{Deserialize, Serialize};

const SCHEDULER_RESOLUTION: Duration = Duration::from_millis(50);
const CHECKPOINT_WARNING: Duration = Duration::from_secs(5);
const ONLINE_GC_SCHEDULER_RESOLUTION: Duration = Duration::from_secs(5);
const RECOVERY_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(90);
const MANAGEMENT_PROTOCOL_VERSION: u16 = 1;
const MANAGEMENT_SOCKET_NAME: &str = ".fastdup-management.sock";
static TELEMETRY_EXACT_HIT_BYTES: AtomicU64 = AtomicU64::new(0);
static TELEMETRY_NEW_CHUNK_BYTES: AtomicU64 = AtomicU64::new(0);
static TELEMETRY_LOGICAL_CHUNK_BYTES: AtomicU64 = AtomicU64::new(0);
static TELEMETRY_PHYSICAL_CONTAINER_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdvancedReductionPolicy {
    Off,
    DependentV1,
}

struct StartupPolicies {
    statfs_override: Option<StatFsOverride>,
    online_gc: OnlineGcPolicy,
    advanced_reduction: AdvancedReductionPolicy,
    pool_isolation: PoolIsolationPolicy,
    small_file_policy_revision: String,
    small_file_extensions: Vec<String>,
}

type FrontendContainerStorage = TieredStorageIo<TelemetryStorageIo, FsStorageIo>;
type MaintenanceContainerStorage = TieredStorageIo<FsStorageIo, FsStorageIo>;
type FsAppliance = DurableNamespace<FsStorageIo, FrontendContainerStorage>;
type FsOnlineMaintenance =
    MaintenanceRepository<FsStorageIo, MaintenanceContainerStorage, FsStorageIo>;
type FsGcCatalog = GcCandidateCatalogRepository<FsStorageIo>;

struct RecoveredAppliance {
    appliance: FsAppliance,
    online_gc_recovery: OnlineGcRecoveryReport,
    online_maintenance: FsOnlineMaintenance,
    gc_catalog: FsGcCatalog,
    recovery_generations: GenerationRepository<FsStorageIo>,
    recovery_containers: ContainerRepository<MaintenanceContainerStorage>,
    recovery_indexes: ExactIndexRunRepository<FsStorageIo>,
    recovery_checkpoints: RecoveryCheckpointRepository<FsStorageIo>,
}

struct OnlineGcControlRequest {
    response: oneshot::Sender<String>,
}

struct OnlineGcSocketGuard {
    path: PathBuf,
}

struct OnlineGcRuntimeHandle {
    requests: mpsc::Sender<OnlineGcControlRequest>,
    configuration: watch::Sender<OnlineGcRuntimeConfiguration>,
    shutdown: watch::Sender<bool>,
    worker: JoinHandle<Result<(), String>>,
}

#[derive(Clone, Copy, Debug)]
struct OnlineGcRuntimeConfiguration {
    enabled: bool,
    policy: OnlineGcPolicy,
}

#[derive(Debug, Deserialize)]
struct ManagementRequest {
    version: u16,
    operation: ManagementOperation,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ManagementOperation {
    Inspect,
    UpdateOnlineGc {
        enabled: bool,
        pressure_low_basis_points: u16,
        pressure_high_basis_points: u16,
    },
    UpdatePresentedCapacities {
        revision: String,
        rules: Vec<ManagementPresentedCapacityRule>,
        #[serde(default)]
        reduction_rules: Option<Vec<ManagementReductionRule>>,
    },
    UpdateAdvancedReductionDefault {
        enabled: bool,
    },
    UpdateSmallFileExtensions {
        revision: String,
        extensions: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct ManagementPresentedCapacityRule {
    inode: u64,
    capacity_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct ShareCapacityManifest {
    version: u16,
    revision: String,
    rules: Vec<ManagementPresentedCapacityRule>,
    #[serde(default)]
    reduction_rules: Vec<ManagementReductionRule>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct ManagementReductionRule {
    inode: u64,
    enabled: bool,
}

fn apply_reduction_rules(
    namespace: &Namespace,
    rules: Vec<ManagementReductionRule>,
) -> Result<(), String> {
    let rules = rules
        .into_iter()
        .map(|r| {
            InodeId::new(r.inode)
                .map(|inode| (inode, r.enabled))
                .ok_or_else(|| "Share inode must be nonzero".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    namespace
        .replace_share_reduction(namespace.advanced_reduction_default(), rules)
        .map_err(|e| format!("{e:?}"))
}

trait PresentedCapacityControl: Send + Sync {
    fn replace(&self, revision: String, rules: Vec<(u64, u64)>) -> io::Result<()>;
    fn revision(&self) -> io::Result<String>;
}

#[derive(Clone, Debug)]
struct RuntimePresentedCapacityControl {
    statfs: TieredStatFsSource,
    namespace: Arc<Namespace>,
}

impl PresentedCapacityControl for RuntimePresentedCapacityControl {
    fn replace(&self, revision: String, rules: Vec<(u64, u64)>) -> io::Result<()> {
        let logical_rules = rules
            .iter()
            .map(|&(inode, capacity_bytes)| {
                let inode = InodeId::new(inode).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "quota inode must be nonzero")
                })?;
                LogicalQuotaRule::new(inode, capacity_bytes).map_err(|error| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("{error:?}"))
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        self.namespace
            .replace_logical_quotas(revision.clone(), logical_rules)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("{error:?}")))?;
        self.statfs.replace_presented_capacities(revision, rules)
    }

    fn revision(&self) -> io::Result<String> {
        let logical = self.namespace.logical_quota_revision();
        let presented = self.statfs.presented_capacity_revision()?;
        if logical != presented {
            return Err(io::Error::other(
                "logical quota and statfs presentation revisions differ",
            ));
        }
        Ok(logical)
    }
}

#[derive(Debug, Serialize)]
struct ManagementResponse {
    version: u16,
    ok: bool,
    error: Option<String>,
    frontend: Option<ManagementFrontendTelemetry>,
    presented_capacity_revision: Option<String>,
    small_file_policy: Option<ManagementSmallFilePolicy>,
}

#[derive(Debug, Serialize)]
struct ManagementSmallFilePolicy {
    revision: String,
    extensions: Vec<String>,
}

impl From<fastdup_posix::SmallFilePolicySnapshot> for ManagementSmallFilePolicy {
    fn from(snapshot: fastdup_posix::SmallFilePolicySnapshot) -> Self {
        Self {
            revision: snapshot.revision,
            extensions: snapshot.extensions,
        }
    }
}

#[derive(Debug, Serialize)]
struct ManagementFrontendTelemetry {
    read_bytes: u64,
    write_bytes: u64,
    read_operations: u64,
    write_operations: u64,
    read_errors: u64,
    write_errors: u64,
    read_latency_micros_p50: u64,
    read_latency_micros_p95: u64,
    read_latency_micros_p99: u64,
    write_latency_micros_p50: u64,
    write_latency_micros_p95: u64,
    write_latency_micros_p99: u64,
    exact_hit_bytes: u64,
    new_chunk_bytes: u64,
    logical_chunk_bytes: u64,
    physical_container_bytes: u64,
    details: Option<serde_json::Value>,
}

struct RecoveryCheckpointRuntimeHandle {
    shutdown: watch::Sender<bool>,
    worker: JoinHandle<Result<(), String>>,
}

impl OnlineGcRuntimeHandle {
    async fn stop(self) -> Result<(), String> {
        self.shutdown
            .send(true)
            .map_err(|_| "Online-GC runtime stopped before shutdown".to_owned())?;
        self.worker
            .await
            .map_err(|error| format!("Online-GC runtime join failed: {error}"))?
    }
}

impl RecoveryCheckpointRuntimeHandle {
    async fn stop(self) -> Result<(), String> {
        self.shutdown
            .send(true)
            .map_err(|_| "Recovery-Checkpoint runtime stopped before shutdown".to_owned())?;
        self.worker
            .await
            .map_err(|error| format!("Recovery-Checkpoint runtime join failed: {error}"))?
    }
}

impl Drop for OnlineGcSocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mount_path, metadata_root, container_root) = parse_mount_arguments()?;
    let policies = validated_startup_policies()?;
    let statfs_override = policies.statfs_override;
    let online_gc_policy = policies.online_gc;
    let advanced_reduction = policies.advanced_reduction;
    let pool_isolation_policy = policies.pool_isolation;
    let small_file_policy_revision = policies.small_file_policy_revision;
    let small_file_extensions = policies.small_file_extensions;
    std::fs::create_dir_all(&metadata_root)?;
    let _appliance_lease =
        ApplianceLease::acquire(&metadata_root, ApplianceLeaseOwner::WritableDaemon)?;
    let recovery_latch = arm_recovery_latch(&metadata_root)?;
    let metadata_pool = FsStorageIo::open(&metadata_root)?;
    let data_pool = FsStorageIo::open(&container_root)?;
    if metadata_pool.root() == data_pool.root() {
        return Err("metadata and DATA roots must resolve to distinct directories".into());
    }
    let isolation = PhysicalPoolIsolation::audit(
        &PhysicalPoolIsolation::observe_paths(metadata_pool.root(), data_pool.root())?,
        pool_isolation_policy,
    )?;
    if isolation == PhysicalPoolIsolation::LabBypass {
        eprintln!(
            "WARNING: physical pool isolation bypassed for LAB; this configuration is not production-safe"
        );
    }
    AppliancePoolBinding::initialize_or_open_filesystem(&metadata_pool, &data_pool)?;
    let small_file_isolation =
        SmallFileTierIsolation::prepare(&metadata_root, pool_isolation_policy)?;
    emit_small_file_tier(&small_file_isolation);
    let small_file_root = small_file_isolation.root().to_path_buf();
    let capacity_source = TieredStatFsSource::open_with_small_file_tier(
        &container_root,
        &metadata_root,
        &small_file_root,
        small_file_isolation.hard_limit_bytes(),
        statfs_override,
    )?;
    let (control_listener, _control_guard) = bind_online_gc_control(&metadata_root)?;
    let (management_listener, _management_guard) = bind_management_control(&metadata_root)?;

    let io_telemetry_enabled = std::env::var_os("FASTDUP_IO_TELEMETRY").is_some();
    let data_storage = open_data_storage(&container_root, io_telemetry_enabled)?;
    let small_file_storage = FsStorageIo::open(&small_file_root)?;
    let recovered = recover_appliance(
        &metadata_root,
        &data_storage,
        &small_file_storage,
        advanced_reduction,
    )?;
    emit_online_gc_recovery(recovered.online_gc_recovery);
    let appliance = Arc::new(recovered.appliance);
    let namespace = appliance.namespace_arc();
    namespace
        .set_advanced_reduction_default(advanced_reduction == AdvancedReductionPolicy::DependentV1);
    namespace.replace_small_file_extensions(small_file_policy_revision, small_file_extensions)?;
    capacity_source.attach_logical_quota_namespace(&namespace)?;
    let presented_capacity_control = RuntimePresentedCapacityControl {
        statfs: capacity_source.clone(),
        namespace: Arc::clone(&namespace),
    };
    if let Some(manifest) = load_share_capacity_manifest()? {
        apply_reduction_rules(&namespace, manifest.reduction_rules)?;
        presented_capacity_control.replace(
            manifest.revision,
            manifest
                .rules
                .into_iter()
                .map(|rule| (rule.inode, rule.capacity_bytes))
                .collect(),
        )?;
    }
    let filesystem = configured_filesystem(&appliance, capacity_source.clone());
    let frontend_telemetry = filesystem.frontend_telemetry();
    let session = Session::new(volatile_mount_options());
    let mount = session.mount(filesystem, &mount_path).await?;
    let gc_runtime = start_online_gc_runtime(
        recovered.online_maintenance,
        recovered.gc_catalog,
        data_storage.clone(),
        container_root.clone(),
        online_gc_policy,
        online_gc_enabled(),
    );
    let recovery_checkpoint_runtime = start_recovery_checkpoint_runtime(
        recovered.recovery_generations,
        recovered.recovery_containers,
        recovered.recovery_indexes,
        recovered.recovery_checkpoints,
    );
    emit_mount_state(
        &appliance,
        &mount_path,
        &metadata_root,
        &container_root,
        &data_storage,
        io_telemetry_enabled,
        statfs_override,
        advanced_reduction,
    );

    let mut ticks = interval(SCHEDULER_RESOLUTION);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await;
    let supervisor_epoch = Instant::now();
    let mut durability = DurabilitySupervisor::new(Duration::ZERO);
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            _ = ticks.tick() => {
                let action = observe_durability(
                    &appliance,
                    &mut durability,
                    supervisor_epoch.elapsed(),
                );
                if !matches!(action, CheckpointAction::Wait(_)) {
                    if matches!(action, CheckpointAction::PauseAndCommit(_)) {
                        appliance.namespace().pause_mutation_admission();
                    }
                    let checkpoint_started = supervisor_epoch.elapsed();
                    if let Err(error) = checkpoint_cycle(Arc::clone(&appliance)).await {
                        appliance.namespace().pause_mutation_admission();
                        eprintln!(
                            "CRITICAL: durable progress failed; mutation admission remains closed: {error}"
                        );
                    }
                    record_checkpoint_attempt(&appliance, &mut durability, checkpoint_started);
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
                record_checkpoint_attempt(
                    &appliance,
                    &mut durability,
                    supervisor_epoch.elapsed(),
                );
            }
            accepted = control_listener.accept() => {
                let (stream, _) = accepted?;
                let requests = gc_runtime.requests.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_online_gc_control(stream, requests).await {
                        eprintln!("online_gc_control_error={error}");
                    }
                });
            }
            accepted = management_listener.accept() => {
                let (stream, _) = accepted?;
                let telemetry = Arc::clone(&frontend_telemetry);
                let configuration = gc_runtime.configuration.clone();
                let capacity_control = presented_capacity_control.clone();
                let namespace = Arc::clone(&namespace);
                let inspected_appliance = Arc::clone(&appliance);
                let inspected_storage = data_storage.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_management_control(stream, telemetry, configuration, capacity_control, namespace, inspected_appliance, inspected_storage).await {
                        eprintln!("management_control_error={error}");
                    }
                });
            }
        }
    }

    let clean_catch_up = stop_background_and_catch_up(
        Arc::clone(&appliance),
        gc_runtime,
        recovery_checkpoint_runtime,
    )
    .await?;
    mount.unmount().await?;
    if clean_catch_up {
        recovery_latch.mark_clean()?;
    }
    emit_verified_read_cache(&appliance);
    data_storage.emit();
    emit_io_uring_state(&data_storage);
    Ok(())
}

fn arm_recovery_latch(
    metadata_root: &std::path::Path,
) -> io::Result<ApplianceRecoveryLatch<FsStorageIo>> {
    let latch = ApplianceRecoveryLatch::arm_filesystem(FsStorageIo::open(metadata_root)?)?;
    if latch.prior_recovery_required() {
        eprintln!(
            "appliance_recovery_required=true action=verify_generation_before_mutation_admission"
        );
    }
    Ok(latch)
}

async fn stop_background_and_catch_up(
    appliance: Arc<FsAppliance>,
    gc_runtime: OnlineGcRuntimeHandle,
    recovery_checkpoint_runtime: RecoveryCheckpointRuntimeHandle,
) -> Result<bool, String> {
    appliance.namespace().pause_mutation_admission();
    gc_runtime.stop().await?;
    match catch_up(appliance).await {
        Ok(()) => {
            recovery_checkpoint_runtime.stop().await?;
            Ok(true)
        }
        Err(error) => {
            eprintln!("CRITICAL: final checkpoint failed during shutdown: {error}");
            recovery_checkpoint_runtime.stop().await?;
            Ok(false)
        }
    }
}

fn parse_mount_arguments() -> Result<(PathBuf, PathBuf, PathBuf), Box<dyn std::error::Error>> {
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
    Ok((mount_path, metadata_root, container_root))
}

fn validate_memory_budget_policy() -> Result<(), Box<dyn std::error::Error>> {
    const REQUIRE_NO_SWAP: &str = "FASTDUP_REQUIRE_CGROUP_NO_SWAP";
    let required = match std::env::var(REQUIRE_NO_SWAP) {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Ok(value) => {
            return Err(format!("{REQUIRE_NO_SWAP} must be 0 or 1, got {value:?}").into());
        }
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => return Err(error.into()),
    };
    let governor = system_memory_budget_governor();
    if required {
        governor.require_no_swap()?;
    } else if governor
        .snapshot()
        .is_ok_and(|snapshot| !snapshot.swap_protection_enforced())
    {
        eprintln!(
            "WARNING: fastdup cgroup can swap; production requires MemorySwapMax=0 and FASTDUP_REQUIRE_CGROUP_NO_SWAP=1"
        );
    }
    Ok(())
}

fn validated_startup_policies() -> Result<StartupPolicies, Box<dyn std::error::Error>> {
    let statfs_override = statfs_override_from_environment()?;
    let online_gc_policy = OnlineGcPolicy::from_environment()?;
    let advanced_reduction = advanced_reduction_policy_from_environment()?;
    let pool_isolation = PoolIsolationPolicy::from_environment()?;
    let (small_file_policy_revision, small_file_extensions) = small_file_policy_from_environment()?;
    validate_memory_budget_policy()?;
    Ok(StartupPolicies {
        statfs_override,
        online_gc: online_gc_policy,
        advanced_reduction,
        pool_isolation,
        small_file_policy_revision,
        small_file_extensions,
    })
}

fn small_file_policy_from_environment() -> Result<(String, Vec<String>), Box<dyn std::error::Error>>
{
    const REVISION: &str = "FASTDUP_SMALL_FILE_POLICY_REVISION";
    const EXTENSIONS: &str = "FASTDUP_SMALL_FILE_EXTENSIONS";
    let revision = match std::env::var(REVISION) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "default-v1".to_owned(),
        Err(error) => return Err(error.into()),
    };
    let extensions = match std::env::var(EXTENSIONS) {
        Ok(value) if value.is_empty() => Vec::new(),
        Ok(value) => value.split(',').map(str::to_owned).collect(),
        Err(std::env::VarError::NotPresent) => fastdup_posix::DEFAULT_SMALL_FILE_EXTENSIONS
            .map(str::to_owned)
            .to_vec(),
        Err(error) => return Err(error.into()),
    };
    let canonical = fastdup_posix::validate_small_file_extensions(&extensions)?;
    if revision.is_empty() || revision.len() > fastdup_posix::MAX_SMALL_FILE_POLICY_REVISION_BYTES {
        return Err(format!("{REVISION} is invalid").into());
    }
    Ok((revision, canonical))
}

fn advanced_reduction_policy_from_environment()
-> Result<AdvancedReductionPolicy, Box<dyn std::error::Error>> {
    const NAME: &str = "FASTDUP_ADVANCED_REDUCTION";
    match std::env::var(NAME) {
        Ok(value) if value == "off" => Ok(AdvancedReductionPolicy::Off),
        Ok(value) if value == "dependent-v1" => Ok(AdvancedReductionPolicy::DependentV1),
        Ok(value) => Err(format!("{NAME} must be off or dependent-v1, got {value:?}").into()),
        Err(std::env::VarError::NotPresent) => Ok(AdvancedReductionPolicy::Off),
        Err(error) => Err(error.into()),
    }
}

fn recover_appliance(
    metadata_root: &std::path::Path,
    data_storage: &TelemetryStorageIo,
    small_file_storage: &FsStorageIo,
    advanced_reduction: AdvancedReductionPolicy,
) -> Result<RecoveredAppliance, Box<dyn std::error::Error>> {
    let metadata_storage = FsStorageIo::open(metadata_root)?;
    let generations = GenerationRepository::new(metadata_storage.clone(), checkpoint_policy_set());
    let containers = ContainerRepository::new(TieredStorageIo::new(
        data_storage.clone(),
        small_file_storage.clone(),
    ));
    let indexes = ExactIndexRunRepository::new(metadata_storage.clone());
    let maintenance_containers = containers.with_maintenance_storage(TieredStorageIo::new(
        FsStorageIo::open(data_storage.inner.root())?,
        small_file_storage.clone(),
    ));
    let recovery_checkpoints =
        RecoveryCheckpointRepository::new(FsStorageIo::open(data_storage.inner.root())?);
    let restored_from_data_tier = if generations.has_committed_generation()? {
        None
    } else {
        let recovered =
            recovery_checkpoints.recover_latest(&generations, &maintenance_containers)?;
        if recovered.is_none() && containers.audit_generation_high_water(None)?.is_some() {
            return Err(
                "Metadata tier is empty but initialized DATA has no complete Recovery Checkpoint"
                    .into(),
            );
        }
        recovered
    };
    if let Some(recovered) = &restored_from_data_tier {
        eprintln!(
            "data_tier_recovery_checkpoint_ok=true generation={} metadata_objects_restored=true",
            recovered.record().generation()
        );
    }
    let online_maintenance = MaintenanceRepository::new(
        generations.clone(),
        maintenance_containers.clone(),
        indexes.clone(),
        checkpoint_exact_index_profile_v1(),
    );
    let online_gc_recovery = online_maintenance.finalize_recovered_online_gc()?;
    let gc_catalog = GcCandidateCatalogRepository::new(FsStorageIo::open(metadata_root)?);
    let similarities = Some(SimilarityIndexRepository::new(metadata_storage));
    if restored_from_data_tier.is_some() {
        if advanced_reduction == AdvancedReductionPolicy::DependentV1 {
            let similarities = similarities.as_ref().expect("Similarity repository exists");
            let rebuilt = online_maintenance.rebuild_pool_indexes(similarities)?;
            eprintln!(
                "data_tier_recovery_indexes_ok=true exact_generation={} similarity_generation={}",
                rebuilt.exact().activation_generation(),
                rebuilt.similarity_generation(),
            );
        } else {
            let rebuilt = online_maintenance.rebuild_exact_index()?;
            eprintln!(
                "data_tier_recovery_indexes_ok=true exact_generation={}",
                rebuilt.activation_generation(),
            );
        }
    }
    let appliance = DurableNamespace::open_with_reduction_indexes(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        &indexes,
        similarities
            .as_ref()
            .expect("ASSERT: Prefix policy constructs Similarity repository"),
        INODE_RESERVATION_SPAN_V1,
    )?;
    appliance
        .namespace_arc()
        .set_advanced_reduction_default(advanced_reduction == AdvancedReductionPolicy::DependentV1);
    Ok(RecoveredAppliance {
        appliance,
        online_gc_recovery,
        online_maintenance,
        gc_catalog,
        recovery_generations: generations,
        recovery_containers: maintenance_containers,
        recovery_indexes: indexes,
        recovery_checkpoints,
    })
}

fn start_recovery_checkpoint_runtime(
    generations: GenerationRepository<FsStorageIo>,
    containers: ContainerRepository<MaintenanceContainerStorage>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    checkpoints: RecoveryCheckpointRepository<FsStorageIo>,
) -> RecoveryCheckpointRuntimeHandle {
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let worker = tokio::spawn(async move {
        let mut ticks = interval(RECOVERY_CHECKPOINT_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticks.tick().await;
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = ticks.tick() => {
                    publish_recovery_checkpoint_background(
                        generations.clone(),
                        containers.clone(),
                        indexes.clone(),
                        checkpoints.clone(),
                    ).await;
                }
            }
        }
        publish_recovery_checkpoint_once(generations, containers, indexes, checkpoints)
            .await
            .map(drop)
    });
    RecoveryCheckpointRuntimeHandle { shutdown, worker }
}

async fn publish_recovery_checkpoint_background(
    generations: GenerationRepository<FsStorageIo>,
    containers: ContainerRepository<MaintenanceContainerStorage>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    checkpoints: RecoveryCheckpointRepository<FsStorageIo>,
) {
    match publish_recovery_checkpoint_once(generations, containers, indexes, checkpoints).await {
        Ok(Some(summary)) => eprintln!(
            "data_tier_recovery_checkpoint_ok=true generation={} metadata_objects={} metadata_bytes={} required_chunks={} file_bytes={}",
            summary.generation(),
            summary.metadata_object_count(),
            summary.metadata_payload_bytes(),
            summary.required_chunk_count(),
            summary.file_length(),
        ),
        Ok(None) => {}
        Err(error) => eprintln!("data_tier_recovery_checkpoint_error={error}"),
    }
}

async fn publish_recovery_checkpoint_once(
    generations: GenerationRepository<FsStorageIo>,
    containers: ContainerRepository<MaintenanceContainerStorage>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    checkpoints: RecoveryCheckpointRepository<FsStorageIo>,
) -> Result<Option<fastdup_store::RecoveryCheckpointSummary>, String> {
    tokio::task::spawn_blocking(move || {
        if let Some(index) = indexes.pin_active_generation() {
            let verifier = IndexedRequiredChunkVerifier::new(containers, index);
            checkpoints.publish(&generations, &verifier)
        } else {
            checkpoints.publish(&generations, &containers)
        }
    })
    .await
    .map_err(|error| format!("Recovery-Checkpoint worker join failed: {error}"))?
    .map_err(|error| error.to_string())
}

fn emit_online_gc_recovery(report: OnlineGcRecoveryReport) {
    if report.retiring_containers() == 0 {
        return;
    }
    eprintln!(
        "online_gc_recovery_ok=true retiring_containers={} containers_removed={} containers_already_absent={} bytes_removed={} retiring_locations_finalized={} activation_generation={}",
        report.retiring_containers(),
        report.containers_removed(),
        report.containers_already_absent(),
        report.bytes_removed(),
        report.retiring_locations_finalized(),
        report
            .activation_generation()
            .expect("ASSERT: nonempty recovery publishes one Exact activation"),
    );
}

fn bind_online_gc_control(
    metadata_root: &std::path::Path,
) -> io::Result<(UnixListener, OnlineGcSocketGuard)> {
    let path = online_gc_control_path(metadata_root);
    let listener = bind_online_gc_control_socket(metadata_root)?;
    let guard = OnlineGcSocketGuard { path: path.clone() };
    listener.set_nonblocking(true)?;
    let listener = UnixListener::from_std(listener)?;
    Ok((listener, guard))
}

fn bind_management_control(
    metadata_root: &std::path::Path,
) -> io::Result<(UnixListener, OnlineGcSocketGuard)> {
    use std::os::unix::fs::PermissionsExt as _;

    let path = metadata_root.join(MANAGEMENT_SOCKET_NAME);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = std::os::unix::net::UnixListener::bind(&path)?;
    // Only the root agent may mutate live filesystem policy. The unprivileged
    // HTTPS process reaches this seam exclusively through the typed agent.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    Ok((
        UnixListener::from_std(listener)?,
        OnlineGcSocketGuard { path },
    ))
}

async fn handle_management_control(
    mut stream: UnixStream,
    telemetry: Arc<FrontendTelemetry>,
    configuration: watch::Sender<OnlineGcRuntimeConfiguration>,
    capacity_source: RuntimePresentedCapacityControl,
    namespace: Arc<Namespace>,
    appliance: Arc<FsAppliance>,
    storage: TelemetryStorageIo,
) -> Result<(), String> {
    let mut request = Vec::new();
    timeout(
        Duration::from_secs(5),
        (&mut stream).take(1_048_577).read_to_end(&mut request),
    )
    .await
    .map_err(|_| "management request timed out".to_owned())?
    .map_err(|error| format!("management request read failed: {error}"))?;
    let mut response = match serde_json::from_slice::<ManagementRequest>(&request) {
        Ok(request) if request.version == MANAGEMENT_PROTOCOL_VERSION => {
            apply_management_operation(
                request.operation,
                &telemetry,
                &configuration,
                &capacity_source,
                &namespace,
            )
        }
        Ok(_) => ManagementResponse {
            version: MANAGEMENT_PROTOCOL_VERSION,
            ok: false,
            error: Some("unsupported_version".to_owned()),
            frontend: None,
            presented_capacity_revision: None,
            small_file_policy: None,
        },
        Err(_) => ManagementResponse {
            version: MANAGEMENT_PROTOCOL_VERSION,
            ok: false,
            error: Some("invalid_request".to_owned()),
            frontend: None,
            presented_capacity_revision: None,
            small_file_policy: None,
        },
    };
    if let Some(frontend) = response.frontend.as_mut() {
        frontend.details = tokio::task::spawn_blocking(move || runtime_telemetry::snapshot(&appliance, &storage)).await.ok();
    }
    let mut encoded = serde_json::to_vec(&response)
        .map_err(|error| format!("management response encode failed: {error}"))?;
    encoded.push(b'\n');
    stream
        .write_all(&encoded)
        .await
        .map_err(|error| format!("management response write failed: {error}"))
}

#[allow(clippy::too_many_lines, reason = "typed management operation dispatch")]
fn apply_management_operation(
    operation: ManagementOperation,
    telemetry: &FrontendTelemetry,
    configuration: &watch::Sender<OnlineGcRuntimeConfiguration>,
    capacity_source: &dyn PresentedCapacityControl,
    namespace: &Namespace,
) -> ManagementResponse {
    match operation {
        ManagementOperation::Inspect => {
            let snapshot = telemetry.snapshot();
            ManagementResponse {
                version: MANAGEMENT_PROTOCOL_VERSION,
                ok: true,
                error: None,
                frontend: Some(ManagementFrontendTelemetry {
                    details: None,
                    read_bytes: snapshot.read_bytes,
                    write_bytes: snapshot.write_bytes,
                    read_operations: snapshot.read_operations,
                    write_operations: snapshot.write_operations,
                    read_errors: snapshot.read_errors,
                    write_errors: snapshot.write_errors,
                    read_latency_micros_p50: snapshot.read_latency_micros_p50,
                    read_latency_micros_p95: snapshot.read_latency_micros_p95,
                    read_latency_micros_p99: snapshot.read_latency_micros_p99,
                    write_latency_micros_p50: snapshot.write_latency_micros_p50,
                    write_latency_micros_p95: snapshot.write_latency_micros_p95,
                    write_latency_micros_p99: snapshot.write_latency_micros_p99,
                    exact_hit_bytes: TELEMETRY_EXACT_HIT_BYTES.load(Ordering::Relaxed),
                    new_chunk_bytes: TELEMETRY_NEW_CHUNK_BYTES.load(Ordering::Relaxed),
                    logical_chunk_bytes: TELEMETRY_LOGICAL_CHUNK_BYTES.load(Ordering::Relaxed),
                    physical_container_bytes: TELEMETRY_PHYSICAL_CONTAINER_BYTES
                        .load(Ordering::Relaxed),
                }),
                presented_capacity_revision: capacity_source.revision().ok(),
                small_file_policy: Some(namespace.small_file_policy().into()),
            }
        }
        ManagementOperation::UpdateOnlineGc {
            enabled,
            pressure_low_basis_points,
            pressure_high_basis_points,
        } => {
            let current = *configuration.borrow();
            let policy = current
                .policy
                .with_pressure_watermarks(pressure_low_basis_points, pressure_high_basis_points);
            match policy {
                Ok(policy)
                    if configuration
                        .send(OnlineGcRuntimeConfiguration { enabled, policy })
                        .is_ok() =>
                {
                    ManagementResponse {
                        version: MANAGEMENT_PROTOCOL_VERSION,
                        ok: true,
                        error: None,
                        frontend: None,
                        presented_capacity_revision: None,
                        small_file_policy: None,
                    }
                }
                Ok(_) => ManagementResponse {
                    version: MANAGEMENT_PROTOCOL_VERSION,
                    ok: false,
                    error: Some("online_gc_runtime_unavailable".to_owned()),
                    frontend: None,
                    presented_capacity_revision: None,
                    small_file_policy: None,
                },
                Err(error) => ManagementResponse {
                    version: MANAGEMENT_PROTOCOL_VERSION,
                    ok: false,
                    error: Some(error.to_string()),
                    frontend: None,
                    presented_capacity_revision: None,
                    small_file_policy: None,
                },
            }
        }
        ManagementOperation::UpdatePresentedCapacities {
            revision,
            rules,
            reduction_rules,
        } => {
            let mut response = update_presented_capacities(capacity_source, revision, rules);
            if response.ok
                && let Some(rules) = reduction_rules
                && let Err(error) = apply_reduction_rules(namespace, rules)
            {
                response.ok = false;
                response.error = Some(error);
            }
            response
        }
        ManagementOperation::UpdateAdvancedReductionDefault { enabled } => {
            namespace.set_advanced_reduction_default(enabled);
            ManagementResponse {
                version: MANAGEMENT_PROTOCOL_VERSION,
                ok: true,
                error: None,
                frontend: None,
                presented_capacity_revision: None,
                small_file_policy: None,
            }
        }
        ManagementOperation::UpdateSmallFileExtensions {
            revision,
            extensions,
        } => match namespace.replace_small_file_extensions(revision, extensions) {
            Ok(snapshot) => ManagementResponse {
                version: MANAGEMENT_PROTOCOL_VERSION,
                ok: true,
                error: None,
                frontend: None,
                presented_capacity_revision: None,
                small_file_policy: Some(snapshot.into()),
            },
            Err(error) => ManagementResponse {
                version: MANAGEMENT_PROTOCOL_VERSION,
                ok: false,
                error: Some(error.to_string()),
                frontend: None,
                presented_capacity_revision: None,
                small_file_policy: None,
            },
        },
    }
}

fn update_presented_capacities(
    capacity_source: &dyn PresentedCapacityControl,
    revision: String,
    rules: Vec<ManagementPresentedCapacityRule>,
) -> ManagementResponse {
    if rules.len() > 4_096 {
        return ManagementResponse {
            version: MANAGEMENT_PROTOCOL_VERSION,
            ok: false,
            error: Some("too_many_presented_capacity_rules".to_owned()),
            frontend: None,
            presented_capacity_revision: None,
            small_file_policy: None,
        };
    }
    match capacity_source.replace(
        revision.clone(),
        rules
            .into_iter()
            .map(|rule| (rule.inode, rule.capacity_bytes))
            .collect(),
    ) {
        Ok(()) => ManagementResponse {
            version: MANAGEMENT_PROTOCOL_VERSION,
            ok: true,
            error: None,
            frontend: None,
            presented_capacity_revision: Some(revision),
            small_file_policy: None,
        },
        Err(error) => ManagementResponse {
            version: MANAGEMENT_PROTOCOL_VERSION,
            ok: false,
            error: Some(error.to_string()),
            frontend: None,
            presented_capacity_revision: None,
            small_file_policy: None,
        },
    }
}

fn online_gc_enabled() -> bool {
    std::env::var("FASTDUP_ONLINE_GC_ENABLED").map_or(true, |value| value != "0")
}

async fn handle_online_gc_control(
    mut stream: UnixStream,
    requests: mpsc::Sender<OnlineGcControlRequest>,
) -> Result<(), String> {
    let mut request = Vec::new();
    timeout(
        Duration::from_secs(5),
        (&mut stream).take(65).read_to_end(&mut request),
    )
    .await
    .map_err(|_| "control request timed out".to_owned())?
    .map_err(|error| format!("control request read failed: {error}"))?;
    if request.is_empty() {
        return Ok(());
    }
    if request.as_slice() != ONLINE_GC_CONTROL_REQUEST {
        stream
            .write_all(b"online_gc_ok=false error=invalid_request\n")
            .await
            .map_err(|error| format!("control rejection write failed: {error}"))?;
        return Ok(());
    }
    let (response_tx, response_rx) = oneshot::channel();
    if let Err(error) = requests.try_send(OnlineGcControlRequest {
        response: response_tx,
    }) {
        let status = match error {
            mpsc::error::TrySendError::Full(_) => "busy",
            mpsc::error::TrySendError::Closed(_) => "unavailable",
        };
        stream
            .write_all(format!("online_gc_ok=false error={status}\n").as_bytes())
            .await
            .map_err(|error| format!("control busy response failed: {error}"))?;
        return Ok(());
    }
    let response = response_rx
        .await
        .map_err(|_| "Online-GC runtime dropped its response".to_owned())?;
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|error| format!("control response write failed: {error}"))?;
    Ok(())
}

fn start_online_gc_runtime(
    maintenance: FsOnlineMaintenance,
    catalog: FsGcCatalog,
    frontend_storage: TelemetryStorageIo,
    container_root: PathBuf,
    policy: OnlineGcPolicy,
    enabled: bool,
) -> OnlineGcRuntimeHandle {
    let (requests, control) = mpsc::channel(1);
    let (configuration, configuration_rx) =
        watch::channel(OnlineGcRuntimeConfiguration { enabled, policy });
    let (shutdown, shutdown_rx) = watch::channel(false);
    let worker = tokio::spawn(run_online_gc_runtime(
        maintenance,
        catalog,
        frontend_storage,
        container_root,
        policy,
        enabled,
        control,
        configuration_rx,
        shutdown_rx,
    ));
    OnlineGcRuntimeHandle {
        requests,
        configuration,
        shutdown,
        worker,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_online_gc_runtime(
    maintenance: FsOnlineMaintenance,
    catalog: FsGcCatalog,
    frontend_storage: TelemetryStorageIo,
    container_root: PathBuf,
    policy: OnlineGcPolicy,
    mut enabled: bool,
    mut control: mpsc::Receiver<OnlineGcControlRequest>,
    mut configuration: watch::Receiver<OnlineGcRuntimeConfiguration>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let now = Instant::now();
    let mut scheduler = OnlineGcScheduler::with_policy(
        now,
        frontend_storage.inner.status().submitted_operations(),
        policy,
    );
    let mut ticks = interval(ONLINE_GC_SCHEDULER_RESOLUTION);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await;
    loop {
        tokio::select! {
            changed = configuration.changed() => {
                changed.map_err(|_| "Online-GC configuration channel closed".to_owned())?;
                let replacement = *configuration.borrow_and_update();
                enabled = replacement.enabled;
                scheduler = OnlineGcScheduler::with_policy(
                    Instant::now(),
                    frontend_storage.inner.status().submitted_operations(),
                    replacement.policy,
                );
            }
            changed = shutdown.changed() => {
                changed.map_err(|_| "Online-GC shutdown channel closed".to_owned())?;
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            request = control.recv() => {
                let Some(request) = request else {
                    return Ok(());
                };
                scheduler.record_immediate_start(Instant::now());
                let relocation_workers = scheduler.relocation_workers(OnlineGcRunMode::Urgent);
                let response = run_online_gc_quantum(
                    maintenance.clone(),
                    catalog.clone(),
                    data_pool_usage(&container_root).map_err(|error| error.to_string()),
                    OnlineGcRunMode::Urgent,
                    relocation_workers,
                    "control",
                    scheduler.status(),
                ).await;
                let _ = request.response.send(response);
            }
            _ = ticks.tick() => {
                if !enabled {
                    continue;
                }
                let usage = match data_pool_usage(&container_root) {
                    Ok(usage) => usage,
                    Err(error) => {
                        eprintln!("online_gc_scheduler_error={error}");
                        continue;
                    }
                };
                let operations = frontend_storage.inner.status().submitted_operations();
                if let Some(mode) = scheduler.poll(Instant::now(), operations, usage) {
                    let relocation_workers = scheduler.relocation_workers(mode);
                    let status = run_online_gc_quantum(
                        maintenance.clone(),
                        catalog.clone(),
                        Ok(usage),
                        mode,
                        relocation_workers,
                        "scheduler",
                        scheduler.status(),
                    ).await;
                    eprint!("{status}");
                }
            }
        }
    }
}

async fn run_online_gc_quantum(
    maintenance: FsOnlineMaintenance,
    catalog: FsGcCatalog,
    usage: Result<DataPoolUsage, String>,
    mode: OnlineGcRunMode,
    relocation_workers: std::num::NonZeroUsize,
    source: &'static str,
    scheduler: OnlineGcSchedulerStatus,
) -> String {
    runtime_telemetry::gc_started();
    let result = match usage {
        Ok(usage) => tokio::task::spawn_blocking(move || {
            maintenance.run_adaptive_online_gc_cycle_with_workers(
                &catalog,
                usage,
                mode,
                relocation_workers,
            )
        })
        .await
        .map_err(|error| format!("worker_join_failed:{error}"))
        .and_then(|result| result.map_err(|error| format!("{error}"))),
        Err(error) => Err(error),
    };
    runtime_telemetry::gc_finished(&result);
    match result {
        Ok(report) => online_gc_status_line(source, mode, relocation_workers, &report, scheduler),
        Err(error) => format!(
            "online_gc_ok=false source={source} mode={mode:?} relocation_workers={} error={}\n",
            relocation_workers,
            error.replace(['\n', '\r'], " ")
        ),
    }
}

#[allow(clippy::too_many_lines)]
fn online_gc_status_line(
    source: &str,
    mode: OnlineGcRunMode,
    relocation_workers: std::num::NonZeroUsize,
    report: &OnlineGcCycleReport,
    scheduler: OnlineGcSchedulerStatus,
) -> String {
    let catalog_generation = report.catalog().generation();
    let metadata = report.metadata_gc();
    let metrics = report.metrics();
    let metadata_status = format!(" {}", metadata_gc_status_fields(metadata, "metadata_"));
    let work_status = format!(
        concat!(
            " total_wall_us={} recovery_wall_us={} candidate_catalog_wall_us={} ",
            "metadata_gc_wall_us={} candidate_proof_wall_us={} relocation_wall_us={} ",
            "retiring_activation_wall_us={} pin_drain_wall_us={} victim_verify_wall_us={} ",
            "unlink_wall_us={} data_sync_wall_us={} removed_activation_wall_us={} ",
            "post_catalog_wall_us={} ",
            "catalog_examined_bytes={} catalog_write_bytes={} candidate_proof_read_bytes={} ",
            "relocation_read_bytes={} relocation_write_bytes={} unlinked_bytes={} ",
            "shortlisted_candidates={} proved_victims={} aborted_candidates={} ",
            "reverse_dependency_edges={} reverse_dependency_required_chunks={} ",
            "scheduler_polls={} scheduler_deferred_polls={} scheduler_frontend_activity_changes={} ",
            "scheduler_background_admissions={} scheduler_idle_admissions={} ",
            "scheduler_urgent_admissions={} scheduler_scheduled_admissions={} ",
            "scheduler_immediate_requests={} relocation_workers={}"
        ),
        metrics.total_wall().as_micros(),
        metrics.recovery_wall().as_micros(),
        metrics.candidate_catalog_wall().as_micros(),
        metrics.metadata_gc_wall().as_micros(),
        metrics.candidate_proof_wall().as_micros(),
        metrics.relocation_wall().as_micros(),
        metrics.retiring_activation_wall().as_micros(),
        metrics.pin_drain_wall().as_micros(),
        metrics.victim_verify_wall().as_micros(),
        metrics.unlink_wall().as_micros(),
        metrics.data_sync_wall().as_micros(),
        metrics.removed_activation_wall().as_micros(),
        metrics.post_collection_catalog_wall().as_micros(),
        metrics.catalog_examined_bytes(),
        metrics.catalog_write_bytes(),
        metrics.candidate_proof_read_bytes(),
        metrics.relocation_read_bytes(),
        metrics.relocation_write_bytes(),
        metrics.unlinked_bytes(),
        metrics.shortlisted_candidates(),
        metrics.proved_victims(),
        metrics.aborted_candidates(),
        metrics.reverse_dependency_edges(),
        metrics.reverse_dependency_required_chunks(),
        scheduler.polls(),
        scheduler.deferred_polls(),
        scheduler.frontend_activity_changes(),
        scheduler.background_admissions(),
        scheduler.idle_admissions(),
        scheduler.urgent_admissions(),
        scheduler.scheduled_admissions(),
        scheduler.immediate_requests(),
        relocation_workers,
    );
    match report.outcome() {
        OnlineGcCycleOutcome::NoCandidates => format!(
            "online_gc_ok=true source={source} mode={mode:?} outcome=no_candidates catalog_generation={catalog_generation}{work_status}{metadata_status}\n"
        ),
        OnlineGcCycleOutcome::NoProfitableCandidates => format!(
            "online_gc_ok=true source={source} mode={mode:?} outcome=no_profitable_candidates catalog_generation={catalog_generation}{work_status}{metadata_status}\n"
        ),
        OnlineGcCycleOutcome::CatalogRebuilt => format!(
            "online_gc_ok=true source={source} mode={mode:?} outcome=catalog_rebuilt catalog_generation={catalog_generation}{work_status}{metadata_status}\n"
        ),
        OnlineGcCycleOutcome::Collected(gc) => format!(
            "online_gc_ok=true source={source} mode={mode:?} outcome=collected catalog_generation={catalog_generation} containers_removed={} bytes_removed={} replacement_containers={} replacement_bytes={} chunks_relocated={} bytes_reclaimed={}{work_status}{metadata_status}\n",
            gc.containers_removed(),
            gc.bytes_removed(),
            gc.replacement_containers(),
            gc.replacement_bytes(),
            gc.chunks_relocated(),
            gc.bytes_reclaimed(),
        ),
    }
}

fn data_pool_usage(path: &std::path::Path) -> io::Result<DataPoolUsage> {
    let statistics = rustix::fs::statvfs(path)?;
    let fragment_bytes = statistics.f_frsize.max(1);
    let capacity = statistics
        .f_blocks
        .checked_mul(fragment_bytes)
        .ok_or_else(|| io::Error::other("data-pool capacity overflows u64"))?;
    let available = statistics
        .f_bavail
        .checked_mul(fragment_bytes)
        .ok_or_else(|| io::Error::other("data-pool availability overflows u64"))?;
    let used = capacity
        .checked_sub(available)
        .ok_or_else(|| io::Error::other("data-pool availability exceeds capacity"))?;
    DataPoolUsage::new(used, capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[allow(clippy::too_many_arguments)]
fn emit_mount_state(
    appliance: &FsAppliance,
    mount_path: &std::path::Path,
    metadata_root: &std::path::Path,
    container_root: &std::path::Path,
    data_storage: &TelemetryStorageIo,
    io_telemetry_enabled: bool,
    statfs_override: Option<StatFsOverride>,
    advanced_reduction: AdvancedReductionPolicy,
) {
    let reduction = appliance.write_through_status().advanced_reduction();
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
    eprintln!(
        "advanced_reduction_configured={advanced_reduction:?} active={}",
        reduction.enabled(),
    );
    emit_io_telemetry_state(io_telemetry_enabled);
    emit_statfs_state(statfs_override);
    emit_io_uring_state(data_storage);
    emit_verified_read_cache(appliance);
}

fn configured_filesystem(appliance: &FsAppliance, source: TieredStatFsSource) -> FuseFilesystem {
    appliance
        .namespace()
        .install_commit_capacity_admission(source.commit_capacity_governor());
    FuseFilesystem::new(appliance.namespace_arc()).with_statfs_source(Arc::new(source))
}

fn statfs_override_from_environment() -> Result<Option<StatFsOverride>, Box<dyn std::error::Error>>
{
    const CAPACITY: &str = "FASTDUP_STATFS_FAKE_CAPACITY_BYTES";
    const AVAILABLE: &str = "FASTDUP_STATFS_FAKE_AVAILABLE_BYTES";
    let capacity = std::env::var_os(CAPACITY);
    let available = std::env::var_os(AVAILABLE);
    statfs_override_from_values(capacity.as_deref(), available.as_deref())
}

fn load_share_capacity_manifest()
-> Result<Option<ShareCapacityManifest>, Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("FASTDUP_SHARE_CAPACITY_MANIFEST") else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > 1_048_576 {
        return Err("Share capacity manifest exceeds one MiB".into());
    }
    let manifest: ShareCapacityManifest = serde_json::from_slice(&std::fs::read(&path)?)?;
    if manifest.version != MANAGEMENT_PROTOCOL_VERSION || manifest.rules.len() > 4_096 {
        return Err("Share capacity manifest version or rule count is invalid".into());
    }
    Ok(Some(manifest))
}

fn statfs_override_from_values(
    capacity: Option<&std::ffi::OsStr>,
    available: Option<&std::ffi::OsStr>,
) -> Result<Option<StatFsOverride>, Box<dyn std::error::Error>> {
    const CAPACITY: &str = "FASTDUP_STATFS_FAKE_CAPACITY_BYTES";
    const AVAILABLE: &str = "FASTDUP_STATFS_FAKE_AVAILABLE_BYTES";
    match (capacity, available) {
        (None, None) => Ok(None),
        (Some(capacity), Some(available)) => {
            let capacity = parse_environment_bytes(CAPACITY, capacity)?;
            let available = parse_environment_bytes(AVAILABLE, available)?;
            Ok(Some(StatFsOverride::new(capacity, available)?))
        }
        _ => Err(format!("{CAPACITY} and {AVAILABLE} must be set together").into()),
    }
}

fn parse_environment_bytes(
    name: &str,
    value: &std::ffi::OsStr,
) -> Result<u64, Box<dyn std::error::Error>> {
    let value = value
        .to_str()
        .ok_or_else(|| format!("{name} must contain decimal ASCII bytes"))?;
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}").into())
}

fn emit_statfs_state(capacity_override: Option<StatFsOverride>) {
    if let Some(capacity_override) = capacity_override {
        eprintln!(
            "statfs_capacity mode=fake capacity_bytes={} available_bytes={}",
            capacity_override.capacity_bytes(),
            capacity_override.available_bytes(),
        );
    } else {
        eprintln!(
            "statfs_capacity mode=physical reserve_basis_points={STATFS_RESERVE_BASIS_POINTS}"
        );
    }
}

fn open_data_storage(root: &std::path::Path, telemetry: bool) -> io::Result<TelemetryStorageIo> {
    let storage = IoUringStorageIo::open(root, IoUringStorageConfig::default())?;
    Ok(TelemetryStorageIo::new(storage, telemetry))
}

fn emit_small_file_tier(isolation: &SmallFileTierIsolation) {
    eprintln!(
        "small_file_tier enabled=true enforced={} project_id={} hard_limit_bytes={} root={} quota_env={} project_env={}",
        isolation.enforced(),
        isolation.project_id(),
        isolation.hard_limit_bytes(),
        isolation.root().display(),
        SMALL_FILE_QUOTA_BYTES_ENV,
        SMALL_FILE_PROJECT_ID_ENV,
    );
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
            "data_io_uring ring_entries={} max_inflight_bytes={} ",
            "inflight_bytes={} peak_inflight_bytes={} submitted_operations={} ",
            "completed_operations={} root_sync_callers={} root_sync_submissions={} ",
            "owned_publications_started={} owned_publications_completed={} ",
            "borrowed_write_copy_bytes={}"
        ),
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
    );
    let copies = copy_telemetry();
    eprintln!(
        concat!(
            "copy_bytes checksum_scratch_bytes={} publication_verify_materialization_bytes={} ",
            "fuse_request_adaptation_bytes={} container_assembly_bytes={} ",
            "chunk_fragment_coalescing_bytes={} compression_region_materialization_bytes={} ",
            "compression_region_concatenation_bytes={}"
        ),
        copies.checksum_scratch_bytes,
        copies.publication_verify_materialization_bytes,
        copies.fuse_request_adaptation_bytes,
        copies.container_assembly_bytes,
        copies.chunk_fragment_coalescing_bytes,
        copies.compression_region_materialization_bytes,
        copies.compression_region_concatenation_bytes,
    );
}

fn emit_write_through_cpu_state(appliance: &FsAppliance) {
    let status = appliance.write_through_status();
    eprintln!(
        "write_through_cpu hash_batches={} maximum_hash_workers={}",
        status.hash_batches(),
        status.maximum_hash_workers(),
    );
    eprintln!(
        concat!(
            "write_through_ingest_ring batches={} fragments={} maximum_batch_bytes={} ",
            "minimum_batch_target_bytes={} maximum_batch_target_bytes={} ",
            "maximum_slots={} full_wait_ns={}"
        ),
        status.ingest_batches(),
        status.ingest_fragments(),
        status.maximum_ingest_batch_bytes(),
        status.minimum_ingest_batch_target_bytes(),
        status.maximum_ingest_batch_target_bytes(),
        status.maximum_ingest_ring_slots(),
        status.ingest_ring_wait_ns(),
    );
    emit_cpu_phase_state("write_through_hash_cpu", status.hash_cpu());
    emit_cpu_phase_state("write_through_encode_cpu", status.encode_cpu());
    emit_cpu_phase_state("write_through_planning", status.planning_cpu());
    eprintln!(
        "write_through_materialization wall_ns={}",
        status.materialization_wall_ns()
    );
    let reduction = status.advanced_reduction();
    eprintln!(
        "advanced_reduction_timing fingerprint_ns={} candidate_lookup_ns={} base_read_ns={} codec_trial_ns={}",
        reduction.fingerprint_ns(),
        reduction.candidate_lookup_ns(),
        reduction.base_read_ns(),
        reduction.codec_trial_ns()
    );
    let online = reduction.online();
    eprintln!(
        "online_similarity families={} batches={} compactions={} skipped_entries={} errors={}",
        online.active_families,
        online.published_batches,
        online.compactions,
        online.skipped_entries,
        online.errors
    );
    eprintln!(
        concat!(
            "advanced_reduction enabled={} queries={} candidates={} base_reads={} ",
            "base_read_bytes={} sparse_xor_trials={} prefix_trials={} ",
            "accepted_sparse_xor={} accepted_prefixes={} ",
            "independent_fallbacks={} no_candidate_fallbacks={} ",
            "saved_payload_bytes={} errors={}"
        ),
        reduction.enabled(),
        reduction.queries(),
        reduction.candidates(),
        reduction.base_reads(),
        reduction.base_read_bytes(),
        reduction.sparse_xor_trials(),
        reduction.prefix_trials(),
        reduction.accepted_sparse_xor(),
        reduction.accepted_prefixes(),
        reduction.independent_fallbacks(),
        reduction.no_candidate_fallbacks(),
        reduction.saved_payload_bytes(),
        reduction.errors(),
    );
}

fn emit_cpu_phase_state(label: &str, status: fastdup_appliance::CpuPhaseStatus) {
    eprintln!(
        concat!(
            "{} phases={} active={} maximum_active={} runnable_wall_ns={} ",
            "permit_blocked_phases={} permit_wait_ns={} maximum_permit_wait_ns={} ",
            "requested_workers={} granted_workers={} partial_grants={}"
        ),
        label,
        status.phases(),
        status.active(),
        status.maximum_active(),
        status.runnable_wall_ns(),
        status.permit_blocked_phases(),
        status.permit_wait_ns(),
        status.maximum_permit_wait_ns(),
        status.requested_workers(),
        status.granted_workers(),
        status.partial_grants(),
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

fn observe_durability(
    appliance: &FsAppliance,
    supervisor: &mut DurabilitySupervisor,
    now: Duration,
) -> CheckpointAction {
    let staged = appliance.write_through_status();
    supervisor.observe(
        now,
        DurabilityObservation {
            has_checkpointable_dirty_payload: appliance
                .namespace()
                .checkpointable_dirty_payload_bytes()
                != 0,
            oldest_sealed_container_age: staged.oldest_sealed_age(),
            sealed_uncommitted_containers: staged.sealed_uncommitted_containers(),
        },
    )
}

fn record_checkpoint_attempt(
    appliance: &FsAppliance,
    supervisor: &mut DurabilitySupervisor,
    started: Duration,
) {
    supervisor.record_checkpoint_attempt(
        started,
        appliance.namespace().checkpointable_dirty_payload_bytes() != 0,
    );
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
                if DurabilitySupervisor::checkpoint_progress(CHECKPOINT_WARNING)
                    == CheckpointProgressAction::CloseAdmission
                {
                    appliance.namespace().pause_mutation_admission();
                    eprintln!(
                        "CRITICAL: checkpoint exceeded five seconds; mutation admission is closed"
                    );
                }
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
    emit_memory_budget_governor();
    let membership = appliance.exact_run_membership_status();
    eprintln!(
        concat!(
            "exact_run_membership mapped_runs={} positional_runs={} mapped_page_bounds_bytes={} ",
            "filters={} allocated_bytes={} ",
            "huge_page_advised_filters={} huge_page_advised_bytes={} probes={} ",
            "definitely_absent={} requires_exact_lookup={}"
        ),
        membership.mapped_run_count(),
        membership.positional_run_count(),
        membership.mapped_page_bounds_bytes(),
        membership.filter_count(),
        membership.allocated_bytes(),
        membership.huge_page_advised_filter_count(),
        membership.huge_page_advised_bytes(),
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
    emit_similarity_index_page_cache(appliance);
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

fn emit_similarity_index_page_cache(appliance: &FsAppliance) {
    let similarity = appliance.similarity_index_page_cache_status();
    eprintln!(
        concat!(
            "similarity_index_page_cache hits={} misses={} hit_rate_basis_points={} ",
            "resident_pages={} target_pages={} capacity_pages={} evictions={} ",
            "pressure_rejections={} reserve_bytes={} effective_limit_bytes={} ",
            "available_bytes={} swap_used_bytes={}"
        ),
        similarity.hits(),
        similarity.misses(),
        similarity.hit_rate_basis_points(),
        similarity.resident_pages(),
        similarity.target_pages(),
        similarity.capacity_pages(),
        similarity.evictions(),
        similarity.pressure_rejections(),
        similarity.reserve_bytes(),
        similarity.effective_limit_bytes(),
        similarity.available_bytes(),
        similarity.swap_used_bytes(),
    );
}

fn emit_memory_budget_governor() {
    let status = system_memory_budget_governor().status();
    if let Some(snapshot) = status.snapshot() {
        let swap_limit = snapshot
            .cgroup_swap_limit_bytes()
            .map_or_else(|| "max".to_owned(), |limit| limit.to_string());
        eprintln!(
            concat!(
                "memory_budget_governor samples={} sample_failures={} ",
                "effective_limit_bytes={} available_bytes={} process_swap_used_bytes={} ",
                "host_swap_used_bytes={} ",
                "cgroup_swap_used_bytes={} cgroup_swap_limit_bytes={} swap_protected={}"
            ),
            status.samples(),
            status.sample_failures(),
            snapshot.effective_limit_bytes(),
            snapshot.available_bytes(),
            snapshot.process_swap_used_bytes(),
            snapshot.host_swap_used_bytes(),
            snapshot.cgroup_swap_used_bytes(),
            swap_limit,
            snapshot.swap_protection_enforced(),
        );
    } else {
        eprintln!(
            "memory_budget_governor samples={} sample_failures={} admission=closed",
            status.samples(),
            status.sample_failures(),
        );
    }
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
    runtime_telemetry::record_checkpoint(profiled);
    let metrics = profiled.metrics();
    TELEMETRY_EXACT_HIT_BYTES.fetch_add(metrics.exact_hit_bytes(), Ordering::Relaxed);
    TELEMETRY_NEW_CHUNK_BYTES.fetch_add(metrics.new_chunk_bytes(), Ordering::Relaxed);
    TELEMETRY_LOGICAL_CHUNK_BYTES.fetch_add(metrics.logical_chunk_bytes(), Ordering::Relaxed);
    TELEMETRY_PHYSICAL_CONTAINER_BYTES.fetch_add(metrics.container_file_bytes(), Ordering::Relaxed);
    let gate = metrics.incompressibility_gate();
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
            "recipe_reuse_chunks={} recipe_reuse_bytes={} checkpoint_rechunk_bytes={} ",
            "gate_policy=off gate_disabled={} gate_eligible={} gate_size_bypass={} ",
            "gate_lz4_allowed={} ",
            "gate_lz4_rejected={} ",
            "gate_zstd1_allowed={} gate_zstd1_rejected={} gate_target_trials={} ",
            "gate_target_accepted={} gate_target_rejected={} gate_raw={} gate_scratch_hwm={}"
        ),
        profiled.record().generation(),
        metrics.total().wall().as_nanos(),
        metrics.total().process_cpu().as_nanos(),
        metrics.freeze().wall().as_nanos(),
        metrics.freeze().process_cpu().as_nanos(),
        metrics.manifest_plan().wall().as_nanos(),
        metrics.manifest_plan().process_cpu().as_nanos(),
        metrics.cdc().wall().as_nanos(),
        metrics.cdc().process_cpu().as_nanos(),
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
        gate.disabled_regions(),
        gate.eligible_regions(),
        gate.size_bypassed_regions(),
        gate.lz4_allowed_regions(),
        gate.lz4_rejected_regions(),
        gate.zstd1_allowed_regions(),
        gate.zstd1_rejected_regions(),
        gate.target_zstd_trials(),
        gate.target_zstd_accepted(),
        gate.target_zstd_rejected(),
        gate.raw_regions_after_gate(),
        gate.scratch_high_water_bytes(),
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

    fn record_owned_publication(&self, temporary_name: &str, sealed_bytes: usize) {
        let ranges = publication_sample_ranges(sealed_bytes)
            .expect("ASSERT: owned publication has valid format-v1 sample ranges");
        let sealed_bytes =
            u64::try_from(sealed_bytes).expect("ASSERT: format-v1 Container length fits u64");
        let durable_write_bytes = sealed_bytes
            .checked_add(
                u64::try_from(HEADER_BYTES).expect("ASSERT: format Header length fits u64"),
            )
            .expect("ASSERT: bounded Container publication bytes cannot overflow");
        for range in ranges {
            self.classify_range_read(temporary_name, range.offset(), range.length());
            self.range_reads.fetch_add(1, Ordering::Relaxed);
            self.range_read_bytes.fetch_add(
                u64::try_from(range.length()).expect("ASSERT: sample length fits u64"),
                Ordering::Relaxed,
            );
        }
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
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let sealed_bytes = publication.sealed_len();
        let temporary_name = publication.temporary_name().to_owned();
        let verified = self.inner.publish_owned_container(publication)?;
        if self.enabled {
            self.telemetry
                .record_owned_publication(&temporary_name, sealed_bytes);
        }
        Ok(verified)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::num::NonZeroUsize;
    use std::os::unix::fs::PermissionsExt;

    use fastdup_appliance::request_online_gc_now;
    use fastdup_format::ContainerId;

    use super::*;

    #[test]
    fn persisted_share_reduction_and_hot_default_are_backward_compatible() {
        let namespace = Namespace::new_volatile(NamespaceConfig::default());
        namespace.set_advanced_reduction_default(false);
        let legacy: ShareCapacityManifest =
            serde_json::from_str(r#"{"version":1,"revision":"old","rules":[]}"#).unwrap();
        assert!(legacy.reduction_rules.is_empty());
        let selected: ShareCapacityManifest = serde_json::from_str(
            r#"{"version":1,"revision":"new","rules":[],"reduction_rules":[{"inode":1,"enabled":true}]}"#,
        ).unwrap();
        apply_reduction_rules(&namespace, selected.reduction_rules).unwrap();
        assert!(namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE));
        let (configuration, _rx) = watch::channel(OnlineGcRuntimeConfiguration {
            enabled: false,
            policy: OnlineGcPolicy::default(),
        });
        let response = apply_management_operation(
            ManagementOperation::UpdateAdvancedReductionDefault { enabled: false },
            &FrontendTelemetry::default(),
            &configuration,
            &TestPresentedCapacityControl::default(),
            &namespace,
        );
        assert!(response.ok);
        assert!(
            namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE),
            "an explicit Share override survives a default update"
        );
        apply_reduction_rules(&namespace, legacy.reduction_rules).unwrap();
        assert!(!namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE));
        assert!(
            apply_reduction_rules(
                &namespace,
                vec![ManagementReductionRule {
                    inode: 0,
                    enabled: true
                }]
            )
            .is_err()
        );
        assert!(!namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE));
    }

    #[test]
    fn management_protocol_exposes_frontend_counters_and_hot_gc_policy() {
        let telemetry = FrontendTelemetry::default();
        let initial = OnlineGcRuntimeConfiguration {
            enabled: true,
            policy: OnlineGcPolicy::default(),
        };
        let (configuration, _configuration_rx) = watch::channel(initial);
        let capacity_source = TestPresentedCapacityControl::default();
        let namespace = Namespace::new_volatile(NamespaceConfig::default());

        let inspected = apply_management_operation(
            ManagementOperation::Inspect,
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(inspected.ok);
        assert!(inspected.frontend.is_some());

        let updated = apply_management_operation(
            ManagementOperation::UpdateOnlineGc {
                enabled: false,
                pressure_low_basis_points: 8_100,
                pressure_high_basis_points: 8_800,
            },
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(updated.ok);
        assert!(!configuration.borrow().enabled);

        let quota = apply_management_operation(
            ManagementOperation::UpdatePresentedCapacities {
                reduction_rules: None,
                revision: "shares-r1".to_owned(),
                rules: vec![ManagementPresentedCapacityRule {
                    inode: 42,
                    capacity_bytes: 25_000_000_000_000,
                }],
            },
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(quota.ok);
        assert_eq!(
            capacity_source.revision().expect("capacity revision"),
            "shares-r1"
        );

        let suffixes = apply_management_operation(
            ManagementOperation::UpdateSmallFileExtensions {
                revision: "settings-2".to_owned(),
                extensions: vec![".VMDK".to_owned()],
            },
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(suffixes.ok);
        assert_eq!(namespace.small_file_policy().extensions, [".vmdk"]);

        let rejected = apply_management_operation(
            ManagementOperation::UpdateSmallFileExtensions {
                revision: "settings-3".to_owned(),
                extensions: vec!["vmdk".to_owned()],
            },
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(!rejected.ok);
        assert_eq!(namespace.small_file_policy().extensions, [".vmdk"]);
    }

    #[tokio::test]
    async fn management_socket_is_root_only() {
        let root = std::env::temp_dir().join(format!(
            "fastdup-management-permissions-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create management fixture root");
        let (_listener, guard) = bind_management_control(&root).expect("bind management socket");
        let mode = std::fs::metadata(root.join(MANAGEMENT_SOCKET_NAME))
            .expect("management socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        drop(guard);
        std::fs::remove_dir(root).expect("remove management fixture root");
    }

    #[derive(Debug, Default)]
    struct TestPresentedCapacityControl {
        revision: Mutex<String>,
    }

    impl PresentedCapacityControl for TestPresentedCapacityControl {
        fn replace(&self, revision: String, _rules: Vec<(u64, u64)>) -> io::Result<()> {
            *self.revision.lock().expect("test capacity lock") = revision;
            Ok(())
        }

        fn revision(&self) -> io::Result<String> {
            Ok(self.revision.lock().expect("test capacity lock").clone())
        }
    }

    #[test]
    fn statfs_override_requires_a_complete_bounded_decimal_pair() {
        assert_eq!(
            statfs_override_from_values(None, None).expect("no override"),
            None
        );
        assert!(statfs_override_from_values(Some(OsStr::new("1")), None).is_err());
        assert!(statfs_override_from_values(Some(OsStr::new("x")), Some(OsStr::new("1"))).is_err());
        assert!(
            statfs_override_from_values(Some(OsStr::new("9")), Some(OsStr::new("10"))).is_err()
        );
        assert_eq!(
            statfs_override_from_values(Some(OsStr::new("1000")), Some(OsStr::new("750")))
                .expect("valid override"),
            Some(StatFsOverride::new(1_000, 750).expect("valid fixture"))
        );
    }

    #[test]
    fn production_data_storage_requires_io_uring() {
        let root =
            std::env::temp_dir().join(format!("fastdup-default-io-uring-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create unique test root");

        let storage = open_data_storage(&root, false).expect("open production data storage");

        assert!(storage.inner.status().ring_entries() > 0);
        drop(storage);
        std::fs::remove_dir_all(root).expect("remove test root");
    }

    #[test]
    fn telemetry_adapter_records_sampled_owned_container_publication() {
        let root =
            std::env::temp_dir().join(format!("fastdup-telemetry-owned-{}", std::process::id()));
        std::fs::create_dir(&root).expect("create unique test root");
        let inner = IoUringStorageIo::open(&root, IoUringStorageConfig::default())
            .expect("open the production io_uring backend");
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
        assert_eq!(storage.telemetry.whole_reads.load(Ordering::Relaxed), 0);
        assert_eq!(
            storage.telemetry.whole_read_bytes.load(Ordering::Relaxed),
            0
        );
        assert_eq!(storage.telemetry.range_reads.load(Ordering::Relaxed), 3);
        assert_eq!(
            storage.telemetry.range_read_bytes.load(Ordering::Relaxed),
            u64::try_from(HEADER_BYTES * 3).expect("sample bytes fit u64")
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

    #[tokio::test]
    async fn local_control_socket_admits_one_bounded_gc_now_request_from_a_long_metadata_path() {
        let owner = PathBuf::from(
            std::env::var_os("TMPDIR").expect("workspace test TMPDIR must be configured"),
        )
        .join(format!("fastdup-online-gc-control-{}", std::process::id()));
        let root = owner.join("metadata-path-".repeat(7));
        std::fs::create_dir_all(&root).expect("create unique long control root");
        assert!(
            online_gc_control_path(&root)
                .as_os_str()
                .as_encoded_bytes()
                .len()
                > 108,
            "fixture must exceed Linux sockaddr_un.sun_path"
        );
        let (listener, guard) = bind_online_gc_control(&root).expect("bind control socket");
        let mode = std::fs::metadata(online_gc_control_path(&root))
            .expect("read socket metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let Err(second_owner) = bind_online_gc_control(&root) else {
            panic!("ASSERT: a live control owner cannot be replaced");
        };
        assert_eq!(second_owner.kind(), io::ErrorKind::AddrInUse);
        assert!(
            online_gc_control_path(&root).exists(),
            "a rejected second owner must not unlink the live socket"
        );
        let (requests, mut received) = mpsc::channel(1);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("accept control client");
                handle_online_gc_control(stream, requests.clone())
                    .await
                    .expect("serve owner probe or control request");
            }
        });
        let responder = tokio::spawn(async move {
            let request = received.recv().await.expect("receive GC request");
            request
                .response
                .send("online_gc_ok=true outcome=no_candidates\n".to_owned())
                .expect("return GC response");
        });
        let client_root = root.clone();
        let response = tokio::task::spawn_blocking(move || request_online_gc_now(&client_root))
            .await
            .expect("join control client")
            .expect("control request succeeds");
        assert_eq!(response, "online_gc_ok=true outcome=no_candidates\n");
        server.await.expect("join control server");
        responder.await.expect("join control responder");
        drop(guard);
        std::fs::remove_dir_all(owner).expect("remove control root");
    }
}
