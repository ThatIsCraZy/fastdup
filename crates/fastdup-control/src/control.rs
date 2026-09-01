use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::net::Shutdown;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use fastdup_appliance::AppliancePoolBinding;
use fastdup_store::FsStorageIo;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

use crate::{
    AGENT_PROTOCOL_VERSION, AdvancedReduction, AgentOperation, AgentRequest, AgentResponse,
    AgentResult, ApplianceSnapshot, BlockInventory, Command, ControlEvent, ControlProblem,
    ControlStore, JobState, JobStatus, RepositoryBinding, RepositoryState, SambaConfig,
    ShareSettings, SystemSampler, TelemetrySnapshot, TelemetryStore, unix_seconds,
};

const METADATA_ROOT: &str = "/var/lib/fastdup/repository/metadata";
const DATA_ROOT: &str = "/var/lib/fastdup/repository/data";
const POSIX_MOUNT: &str = "/srv/fastdup/repository";
const RUNTIME_ENV: &str = "/etc/fastdup/repository.env";
const SHARE_CAPACITY_MANIFEST: &str = "/etc/fastdup/share-capacities.json";
const REPOSITORY_UNIT: &str = "fastdup-repository.service";
const SCRUB_UNIT: &str = "fastdup-maintenance@scrub.service";
const MANAGEMENT_SOCKET: &str = "/var/lib/fastdup/repository/metadata/.fastdup-management.sock";
const SHARE_SYNC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct RuntimeFrontendCounters {
    read_bytes: u64,
    write_bytes: u64,
    exact_hit_bytes: u64,
    new_chunk_bytes: u64,
    logical_chunk_bytes: u64,
    physical_container_bytes: u64,
    presented_capacity_revision: String,
}

#[async_trait]
pub trait ApplianceControl: Send + Sync {
    async fn inspect(&self) -> Result<ApplianceSnapshot, ControlProblem>;
    async fn submit(
        &self,
        command: Command,
        idempotency_key: String,
    ) -> Result<JobStatus, ControlProblem>;
    fn subscribe(&self) -> broadcast::Receiver<ControlEvent>;
}

#[derive(Debug)]
pub struct AgentControl {
    socket_path: PathBuf,
    events: broadcast::Sender<ControlEvent>,
}

impl AgentControl {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Arc<Self> {
        let (events, _) = broadcast::channel(256);
        let control = Arc::new(Self {
            socket_path: socket_path.into(),
            events,
        });
        let polling = Arc::clone(&control);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut known_jobs = BTreeMap::<String, (JobState, i64)>::new();
            loop {
                interval.tick().await;
                if let Ok(snapshot) = polling.inspect_remote().await {
                    for job in &snapshot.jobs {
                        let revision = (job.state, job.updated_at);
                        if known_jobs.get(&job.id) != Some(&revision) {
                            known_jobs.insert(job.id.clone(), revision);
                            let _ = polling.events.send(ControlEvent::Job { job: job.clone() });
                        }
                    }
                    let _ = polling.events.send(ControlEvent::Snapshot {
                        snapshot: snapshot.telemetry,
                    });
                }
            }
        });
        control
    }

    async fn request(&self, operation: AgentOperation) -> Result<AgentResult, ControlProblem> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|error| ControlProblem::new("agent_unavailable", error.to_string()))?;
        let request = AgentRequest {
            version: AGENT_PROTOCOL_VERSION,
            request_id: Uuid::new_v4().to_string(),
            operation,
        };
        let mut body = serde_json::to_vec(&request)
            .map_err(|error| ControlProblem::new("protocol_encode", error.to_string()))?;
        body.push(b'\n');
        stream
            .write_all(&body)
            .await
            .map_err(|error| ControlProblem::new("agent_write", error.to_string()))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| ControlProblem::new("agent_read", error.to_string()))?;
        let response: AgentResponse = serde_json::from_str(&response)
            .map_err(|error| ControlProblem::new("protocol_decode", error.to_string()))?;
        if response.version != AGENT_PROTOCOL_VERSION || response.request_id != request.request_id {
            return Err(ControlProblem::new(
                "protocol_mismatch",
                "Agent response does not match request",
            ));
        }
        response.result
    }

    async fn inspect_remote(&self) -> Result<ApplianceSnapshot, ControlProblem> {
        match self.request(AgentOperation::Inspect).await? {
            AgentResult::Snapshot { snapshot } => Ok(snapshot),
            AgentResult::Job { .. } => Err(ControlProblem::new(
                "protocol_mismatch",
                "Expected snapshot",
            )),
        }
    }
}

#[async_trait]
impl ApplianceControl for AgentControl {
    async fn inspect(&self) -> Result<ApplianceSnapshot, ControlProblem> {
        self.inspect_remote().await
    }

    async fn submit(
        &self,
        command: Command,
        idempotency_key: String,
    ) -> Result<JobStatus, ControlProblem> {
        match self
            .request(AgentOperation::Submit {
                command,
                idempotency_key,
            })
            .await?
        {
            AgentResult::Job { job } => Ok(job),
            AgentResult::Snapshot { .. } => {
                Err(ControlProblem::new("protocol_mismatch", "Expected job"))
            }
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<ControlEvent> {
        self.events.subscribe()
    }
}

#[derive(Debug)]
pub struct InMemoryControl {
    snapshot: RwLock<ApplianceSnapshot>,
    events: broadcast::Sender<ControlEvent>,
}

impl InMemoryControl {
    #[must_use]
    pub fn new(snapshot: ApplianceSnapshot) -> Arc<Self> {
        let (events, _) = broadcast::channel(64);
        Arc::new(Self {
            snapshot: RwLock::new(snapshot),
            events,
        })
    }
}

#[async_trait]
impl ApplianceControl for InMemoryControl {
    async fn inspect(&self) -> Result<ApplianceSnapshot, ControlProblem> {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| ControlProblem::new("state_poisoned", "In-memory state lock is poisoned"))
    }

    async fn submit(
        &self,
        command: Command,
        _idempotency_key: String,
    ) -> Result<JobStatus, ControlProblem> {
        let now = unix_seconds();
        let job = JobStatus {
            id: Uuid::new_v4().to_string(),
            kind: command_name(&command).to_owned(),
            state: JobState::Succeeded,
            progress_basis_points: 10_000,
            message: "Abgeschlossen".to_owned(),
            created_at: now,
            updated_at: now,
        };
        let _ = self.events.send(ControlEvent::Job { job: job.clone() });
        Ok(job)
    }

    fn subscribe(&self) -> broadcast::Receiver<ControlEvent> {
        self.events.subscribe()
    }
}

#[derive(Debug)]
pub struct AgentRuntime {
    store: ControlStore,
    telemetry_store: TelemetryStore,
    inventory: BlockInventory,
    samba: SambaConfig,
    fingerprint: String,
    latest: RwLock<TelemetrySnapshot>,
    sampler: Mutex<SystemSampler>,
    events: broadcast::Sender<ControlEvent>,
    shutdown: watch::Sender<bool>,
}

impl AgentRuntime {
    #[must_use]
    pub fn new(
        store: ControlStore,
        telemetry_store: TelemetryStore,
        samba: SambaConfig,
        fingerprint: String,
    ) -> Arc<Self> {
        let (events, _) = broadcast::channel(512);
        let (shutdown, _) = watch::channel(false);
        Arc::new(Self {
            store,
            telemetry_store,
            inventory: BlockInventory,
            samba,
            fingerprint,
            latest: RwLock::new(TelemetrySnapshot::default()),
            sampler: Mutex::new(SystemSampler::default()),
            events,
            shutdown,
        })
    }

    pub fn start_sampler(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => runtime.sample_once(),
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        });
    }

    fn sample_once(&self) {
        let binding = self.store.repository_binding().ok().flatten();
        let state = binding
            .as_ref()
            .map_or(RepositoryState::Uninitialized, |binding| {
                binding.state.clone()
            });
        let frontend = read_frontend_counters();
        let snapshot = {
            let Ok(mut sampler) = self.sampler.lock() else {
                return;
            };
            sampler.configure_repository(
                state,
                binding
                    .as_ref()
                    .map(|binding| binding.metadata_kernel_name.clone()),
                binding
                    .as_ref()
                    .map(|binding| binding.data_kernel_name.clone()),
                binding.as_ref().map(|_| PathBuf::from(DATA_ROOT)),
            );
            if let Some(frontend) = &frontend {
                sampler.update_frontend_counters(frontend.read_bytes, frontend.write_bytes);
                sampler.update_reduction(
                    frontend.exact_hit_bytes,
                    frontend.new_chunk_bytes,
                    frontend.logical_chunk_bytes,
                    frontend.physical_container_bytes,
                );
            }
            sampler.sample()
        };
        if let Some(frontend) = &frontend
            && let Ok(shares) = self.store.shares()
            && frontend.presented_capacity_revision != share_capacity_revision(&shares)
        {
            let _ = sync_share_capacities(&shares);
        }
        if let Ok(mut latest) = self.latest.write() {
            latest.clone_from(&snapshot);
        }
        let _ = self.telemetry_store.insert(unix_seconds(), &snapshot);
        if snapshot.sequence % 3_600 == 0 {
            let _ = self.telemetry_store.retain_and_roll_up(unix_seconds());
        }
        let _ = self.events.send(ControlEvent::Snapshot { snapshot });
    }

    pub fn inspect(&self) -> Result<ApplianceSnapshot, ControlProblem> {
        let telemetry = self
            .latest
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| ControlProblem::new("state_poisoned", "Telemetry state is unavailable"))?;
        Ok(ApplianceSnapshot {
            telemetry,
            targets: self
                .inventory
                .discover()
                .map_err(problem("inventory_failed"))?,
            settings: self.store.settings().map_err(problem("settings_failed"))?,
            shares: self.store.shares().map_err(problem("shares_failed"))?,
            jobs: self.store.recent_jobs(20).map_err(problem("jobs_failed"))?,
            certificate_fingerprint: self.fingerprint.clone(),
        })
    }

    pub fn submit(
        self: &Arc<Self>,
        command: Command,
        key: &str,
    ) -> Result<JobStatus, ControlProblem> {
        if let Some(existing) = self
            .store
            .job_for_idempotency(key)
            .map_err(problem("job_failed"))?
        {
            return Ok(existing);
        }
        if self
            .store
            .recent_jobs(1)
            .map_err(problem("job_failed"))?
            .first()
            .is_some_and(|job| matches!(job.state, JobState::Queued | JobState::Running))
        {
            return Err(ControlProblem::new(
                "operation_in_progress",
                "Eine Appliance-Aktion läuft bereits",
            ));
        }
        let now = unix_seconds();
        let job = JobStatus {
            id: Uuid::new_v4().to_string(),
            kind: command_name(&command).to_owned(),
            state: JobState::Queued,
            progress_basis_points: 0,
            message: "Wartet auf Ausführung".to_owned(),
            created_at: now,
            updated_at: now,
        };
        self.store
            .insert_job(key, &job)
            .map_err(problem("job_failed"))?;
        let runtime = Arc::clone(self);
        let job_for_task = job.clone();
        tokio::task::spawn_blocking(move || runtime.execute_job(job_for_task, command));
        Ok(job)
    }

    fn execute_job(&self, mut job: JobStatus, command: Command) {
        update_job(
            self,
            &mut job,
            JobState::Running,
            500,
            "Aktion wird ausgeführt",
        );
        let result = self.execute_command(command);
        match result {
            Ok(message) => update_job(self, &mut job, JobState::Succeeded, 10_000, &message),
            Err(error) => {
                let progress = job.progress_basis_points;
                update_job(self, &mut job, JobState::Failed, progress, &error.message);
                let _ = self.events.send(ControlEvent::Alert {
                    code: error.code,
                    message: error.message,
                });
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute_command(&self, command: Command) -> Result<String, ControlProblem> {
        match command {
            Command::Provision {
                metadata_target,
                data_target,
                inventory_revision,
                confirmed,
            } => {
                if !confirmed {
                    return Err(ControlProblem::new(
                        "confirmation_required",
                        "Die Datenlöschung wurde nicht bestätigt",
                    ));
                }
                self.provision(&metadata_target, &data_target, &inventory_revision)?;
                Ok("Repository wurde provisioniert und gestartet".to_owned())
            }
            Command::Adopt {
                metadata_target,
                data_target,
                inventory_revision,
            } => {
                self.adopt(&metadata_target, &data_target, &inventory_revision)?;
                Ok("Vorhandenes Repository wurde übernommen".to_owned())
            }
            Command::Mount => {
                self.set_state(RepositoryState::Mounting)?;
                self.start_repository()?;
                self.set_state(RepositoryState::Online)?;
                Ok("Repository ist online".to_owned())
            }
            Command::Unmount => {
                self.set_state(RepositoryState::Unmounting)?;
                systemctl("stop", REPOSITORY_UNIT)?;
                self.set_state(RepositoryState::Unmounted)?;
                Ok("Repository wurde sauber ausgehängt".to_owned())
            }
            Command::OfflineScrub => self.offline_scrub(),
            Command::UpdateSettings {
                expected_revision,
                settings,
            } => {
                validate_settings(&settings)?;
                let current = self.store.settings().map_err(problem("settings_failed"))?;
                if current.revision != expected_revision {
                    return Err(ControlProblem::new(
                        "settings_conflict",
                        "Die Einstellungen wurden zwischenzeitlich geändert",
                    ));
                }
                let requires_remount = current.advanced_reduction != settings.advanced_reduction;
                let was_online = self
                    .store
                    .repository_binding()
                    .ok()
                    .flatten()
                    .is_some_and(|binding| binding.state == RepositoryState::Online);
                write_runtime_environment(&settings)?;
                if requires_remount {
                    let activation = (|| {
                        if was_online {
                            systemctl("stop", REPOSITORY_UNIT)?;
                        }
                        if settings.advanced_reduction == AdvancedReduction::PrefixV1 {
                            maintenance("rebuild-pool-indexes")?;
                        }
                        if was_online {
                            self.start_repository()?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = activation {
                        let _ = write_runtime_environment(&current);
                        if was_online {
                            let _ = self.start_repository();
                        }
                        return Err(error);
                    }
                } else if was_online && let Err(error) = send_online_gc_configuration(&settings) {
                    let _ = write_runtime_environment(&current);
                    let _ = send_online_gc_configuration(&current);
                    return Err(error);
                }
                if let Err(error) = self.store.update_settings(expected_revision, settings) {
                    let _ = write_runtime_environment(&current);
                    if was_online {
                        if requires_remount {
                            let _ = systemctl("stop", REPOSITORY_UNIT);
                            let _ = self.start_repository();
                        } else {
                            let _ = send_online_gc_configuration(&current);
                        }
                    }
                    return Err(ControlProblem::new("settings_conflict", error.to_string()));
                }
                Ok("Einstellungen sind aktiv".to_owned())
            }
            Command::UpsertShare {
                expected_revision,
                share,
            } => {
                let current = self.store.shares().map_err(problem("shares_failed"))?;
                let mut candidate = current.clone();
                if let Some(existing) = candidate
                    .iter_mut()
                    .find(|existing| existing.id == share.id)
                {
                    existing.clone_from(&share);
                } else {
                    candidate.push(share.clone());
                }
                if let Err(error) = self.activate_share_configuration(&candidate) {
                    let _ = self.activate_share_configuration(&current);
                    return Err(error);
                }
                if let Err(error) = self.store.upsert_share(expected_revision, share) {
                    let _ = self.activate_share_configuration(&current);
                    return Err(ControlProblem::new("share_conflict", error.to_string()));
                }
                Ok("SMB-Freigabe ist aktiv".to_owned())
            }
            Command::DeleteShare {
                id,
                expected_revision,
            } => {
                let current = self.store.shares().map_err(problem("shares_failed"))?;
                let mut candidate = current.clone();
                let name = current
                    .iter()
                    .find(|share| share.id == id)
                    .map(|share| share.name.clone());
                candidate.retain(|share| share.id != id);
                if let Err(error) = self.activate_share_configuration(&candidate) {
                    let _ = self.activate_share_configuration(&current);
                    return Err(error);
                }
                if let Err(error) = self.store.delete_share(&id, expected_revision) {
                    let _ = self.activate_share_configuration(&current);
                    return Err(ControlProblem::new("share_conflict", error.to_string()));
                }
                if let Some(name) = name {
                    let _ = SambaConfig::close_share(&name);
                }
                Ok("SMB-Freigabe wurde entfernt; Daten blieben erhalten".to_owned())
            }
        }
    }

    fn start_repository(&self) -> Result<(), ControlProblem> {
        systemctl("start", REPOSITORY_UNIT)?;
        if dry_run() {
            return Ok(());
        }
        let shares = self.store.shares().map_err(problem("shares_failed"))?;
        let deadline = std::time::Instant::now() + SHARE_SYNC_TIMEOUT;
        loop {
            match sync_share_capacities(&shares) {
                Ok(()) => return Ok(()),
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(error) => {
                    let _ = systemctl("stop", REPOSITORY_UNIT);
                    return Err(error);
                }
            }
        }
    }

    fn activate_share_configuration(&self, shares: &[ShareSettings]) -> Result<(), ControlProblem> {
        // Rendering validates every identifier before it becomes a directory
        // name, a management-socket policy, or Samba configuration.
        SambaConfig::render(shares).map_err(problem("samba_invalid"))?;
        let online = self
            .store
            .repository_binding()
            .map_err(problem("binding_failed"))?
            .is_some_and(|binding| binding.state == RepositoryState::Online);
        if online && !dry_run() {
            sync_share_capacities(shares)?;
        }
        apply_samba(&self.samba, shares)
    }

    fn resolve_pair(
        &self,
        metadata_id: &str,
        data_id: &str,
        revision: &str,
    ) -> Result<(crate::BlockTarget, crate::BlockTarget), ControlProblem> {
        let targets = self
            .inventory
            .discover()
            .map_err(problem("inventory_failed"))?;
        if targets
            .first()
            .is_none_or(|target| target.inventory_revision != revision)
        {
            return Err(ControlProblem::new(
                "inventory_changed",
                "Die Laufwerksliste hat sich geändert; bitte neu auswählen",
            ));
        }
        let metadata = targets
            .iter()
            .find(|target| target.stable_id == metadata_id)
            .cloned()
            .ok_or_else(|| {
                ControlProblem::new("target_missing", "Metadata-Target ist nicht mehr vorhanden")
            })?;
        let data = targets
            .iter()
            .find(|target| target.stable_id == data_id)
            .cloned()
            .ok_or_else(|| {
                ControlProblem::new("target_missing", "DATA-Target ist nicht mehr vorhanden")
            })?;
        if metadata.stable_id == data.stable_id || !metadata.eligible || !data.eligible {
            return Err(ControlProblem::new(
                "target_ineligible",
                "Targets sind nicht getrennt und frei verwendbar",
            ));
        }
        let metadata_backing = metadata
            .backing_disks
            .iter()
            .map(|disk| &disk.stable_id)
            .collect::<BTreeSet<_>>();
        if data
            .backing_disks
            .iter()
            .any(|disk| metadata_backing.contains(&disk.stable_id))
        {
            return Err(ControlProblem::new(
                "shared_backing_disk",
                "Metadata und DATA teilen ein physisches Backing Device",
            ));
        }
        Ok((metadata, data))
    }

    fn provision(
        &self,
        metadata_id: &str,
        data_id: &str,
        revision: &str,
    ) -> Result<(), ControlProblem> {
        let (metadata, data) = self.resolve_pair(metadata_id, data_id, revision)?;
        if dry_run() {
            let binding = RepositoryBinding {
                metadata_target: metadata.stable_id,
                data_target: data.stable_id,
                metadata_uuid: "dry-run-metadata".to_owned(),
                data_uuid: "dry-run-data".to_owned(),
                metadata_kernel_name: metadata.kernel_name,
                data_kernel_name: data.kernel_name,
                state: RepositoryState::Online,
            };
            return self
                .store
                .set_repository_binding(&binding)
                .map_err(problem("binding_failed"));
        }
        self.store
            .audit(
                "admin",
                "provision",
                "started",
                "destructive target initialization",
            )
            .map_err(problem("audit_failed"))?;
        self.store
            .begin_provisioning(&metadata.stable_id, &data.stable_id)
            .map_err(problem("provisioning_incomplete"))?;
        let metadata_partition = format_target(&metadata.path, "FASTDUP_META")?;
        self.store
            .advance_provisioning("metadata_formatted")
            .map_err(problem("provisioning_journal"))?;
        let data_partition = format_target(&data.path, "FASTDUP_DATA")?;
        self.store
            .advance_provisioning("data_formatted")
            .map_err(problem("provisioning_journal"))?;
        let metadata_uuid = block_uuid(&metadata_partition)?;
        let data_uuid = block_uuid(&data_partition)?;
        mount_filesystem(&metadata_uuid, Path::new(METADATA_ROOT))?;
        mount_filesystem(&data_uuid, Path::new(DATA_ROOT))?;
        self.store
            .advance_provisioning("filesystems_mounted")
            .map_err(problem("provisioning_journal"))?;
        std::fs::create_dir_all(POSIX_MOUNT).map_err(problem("mount_directory"))?;
        let metadata_storage =
            FsStorageIo::open(METADATA_ROOT).map_err(problem("metadata_open"))?;
        let data_storage = FsStorageIo::open(DATA_ROOT).map_err(problem("data_open"))?;
        AppliancePoolBinding::initialize_or_open_filesystem(&metadata_storage, &data_storage)
            .map_err(problem("pool_identity"))?;
        self.store
            .advance_provisioning("pool_identity_initialized")
            .map_err(problem("provisioning_journal"))?;
        let binding = RepositoryBinding {
            metadata_target: metadata.stable_id,
            data_target: data.stable_id,
            metadata_uuid,
            data_uuid,
            metadata_kernel_name: metadata.kernel_name,
            data_kernel_name: data.kernel_name,
            state: RepositoryState::Unmounted,
        };
        self.store
            .set_repository_binding(&binding)
            .map_err(problem("binding_failed"))?;
        write_runtime_environment(&self.store.settings().map_err(problem("settings_failed"))?)?;
        self.start_repository()?;
        self.set_state(RepositoryState::Online)?;
        self.store
            .finish_provisioning()
            .map_err(problem("provisioning_journal"))
    }

    fn adopt(
        &self,
        metadata_id: &str,
        data_id: &str,
        revision: &str,
    ) -> Result<(), ControlProblem> {
        let (metadata, data) = self.resolve_pair(metadata_id, data_id, revision)?;
        if metadata.filesystem.as_deref() != Some("xfs")
            || data.filesystem.as_deref() != Some("xfs")
        {
            return Err(ControlProblem::new(
                "unsupported_filesystem",
                "Beide vorhandenen Pools müssen XFS verwenden",
            ));
        }
        let metadata_uuid = block_uuid(Path::new(&metadata.path))?;
        let data_uuid = block_uuid(Path::new(&data.path))?;
        if !dry_run() {
            mount_filesystem(&metadata_uuid, Path::new(METADATA_ROOT))?;
            mount_filesystem(&data_uuid, Path::new(DATA_ROOT))?;
            let metadata_storage =
                FsStorageIo::open(METADATA_ROOT).map_err(problem("metadata_open"))?;
            let data_storage = FsStorageIo::open(DATA_ROOT).map_err(problem("data_open"))?;
            AppliancePoolBinding::audit_filesystem(&metadata_storage, &data_storage)
                .map_err(problem("pool_identity"))?;
        }
        self.store
            .set_repository_binding(&RepositoryBinding {
                metadata_target: metadata.stable_id,
                data_target: data.stable_id,
                metadata_uuid,
                data_uuid,
                metadata_kernel_name: metadata.kernel_name,
                data_kernel_name: data.kernel_name,
                state: RepositoryState::Unmounted,
            })
            .map_err(problem("binding_failed"))?;
        Ok(())
    }

    fn offline_scrub(&self) -> Result<String, ControlProblem> {
        let was_online = self
            .store
            .repository_binding()
            .map_err(problem("binding_failed"))?
            .is_some_and(|binding| binding.state == RepositoryState::Online);
        if was_online {
            systemctl("stop", REPOSITORY_UNIT)?;
        }
        self.set_state(RepositoryState::Scrubbing)?;
        if let Err(error) = maintenance("scrub") {
            self.set_state(RepositoryState::Error)?;
            return Err(error);
        }
        if was_online {
            self.start_repository()?;
            self.set_state(RepositoryState::Online)?;
        } else {
            self.set_state(RepositoryState::Unmounted)?;
        }
        Ok("Offline-Scrub erfolgreich abgeschlossen".to_owned())
    }

    fn set_state(&self, state: RepositoryState) -> Result<(), ControlProblem> {
        let mut binding = self
            .store
            .repository_binding()
            .map_err(problem("binding_failed"))?
            .ok_or_else(|| {
                ControlProblem::new(
                    "repository_uninitialized",
                    "Repository ist nicht initialisiert",
                )
            })?;
        binding.state = state;
        self.store
            .set_repository_binding(&binding)
            .map_err(problem("binding_failed"))
    }

    pub fn handle_request(self: &Arc<Self>, request: AgentRequest) -> AgentResponse {
        let request_id = request.request_id.clone();
        let result = if request.version == AGENT_PROTOCOL_VERSION {
            match request.operation {
                AgentOperation::Inspect => self
                    .inspect()
                    .map(|snapshot| AgentResult::Snapshot { snapshot }),
                AgentOperation::Submit {
                    command,
                    idempotency_key,
                } => self
                    .submit(command, &idempotency_key)
                    .map(|job| AgentResult::Job { job }),
            }
        } else {
            Err(ControlProblem::new(
                "protocol_version",
                "Unsupported agent protocol version",
            ))
        };
        AgentResponse {
            version: AGENT_PROTOCOL_VERSION,
            request_id,
            result,
        }
    }
}

impl Drop for AgentRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

fn update_job(
    runtime: &AgentRuntime,
    job: &mut JobStatus,
    state: JobState,
    progress: u16,
    message: &str,
) {
    job.state = state;
    job.progress_basis_points = progress;
    message.clone_into(&mut job.message);
    job.updated_at = unix_seconds();
    let _ = runtime.store.update_job(job);
    let _ = runtime.events.send(ControlEvent::Job { job: job.clone() });
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Provision { .. } => "provision",
        Command::Adopt { .. } => "adopt",
        Command::Mount => "mount",
        Command::Unmount => "unmount",
        Command::OfflineScrub => "offline_scrub",
        Command::UpdateSettings { .. } => "update_settings",
        Command::UpsertShare { .. } => "upsert_share",
        Command::DeleteShare { .. } => "delete_share",
    }
}

fn validate_settings(settings: &crate::RepositorySettings) -> Result<(), ControlProblem> {
    if settings.pressure_low_basis_points >= settings.pressure_high_basis_points
        || settings.pressure_high_basis_points > 10_000
    {
        return Err(ControlProblem::new(
            "invalid_gc_pressure",
            "GC Low Watermark muss unter High Watermark liegen",
        ));
    }
    if settings
        .maintenance_window_utc
        .as_ref()
        .is_some_and(|window| window.len() != 11 || window.as_bytes().get(5) != Some(&b'-'))
    {
        return Err(ControlProblem::new(
            "invalid_gc_window",
            "Wartungsfenster muss HH:MM-HH:MM verwenden",
        ));
    }
    Ok(())
}

fn apply_samba(samba: &SambaConfig, shares: &[ShareSettings]) -> Result<(), ControlProblem> {
    if dry_run() {
        SambaConfig::render(shares)
            .map(|_| ())
            .map_err(problem("samba_invalid"))
    } else {
        samba.apply(shares).map_err(problem("samba_apply"))
    }
}

fn dry_run() -> bool {
    std::env::var_os("FASTDUP_AGENT_DRY_RUN").is_some()
}

fn systemctl(action: &str, unit: &str) -> Result<(), ControlProblem> {
    if dry_run() {
        return Ok(());
    }
    run_process("systemctl", &[action, unit]).map(|_| ())
}

fn maintenance(operation: &str) -> Result<(), ControlProblem> {
    let unit = maintenance_unit(operation)?;
    if dry_run() {
        return Ok(());
    }
    // Offline repository work belongs to the storage slice.  Running it as an
    // agent child would charge it to the management plane's 1 GiB and one-CPU
    // budget and turn a UI containment policy into a scrub limit.
    systemctl("start", unit)
}

fn maintenance_unit(operation: &str) -> Result<&'static str, ControlProblem> {
    match operation {
        "scrub" => Ok(SCRUB_UNIT),
        "rebuild-pool-indexes" => Ok("fastdup-maintenance@rebuild-pool-indexes.service"),
        _ => Err(ControlProblem::new(
            "unsupported_maintenance",
            "Nicht unterstützte Offline-Wartung",
        )),
    }
}

fn format_target(target: &str, label: &str) -> Result<PathBuf, ControlProblem> {
    run_process("wipefs", &["--all", "--force", target])?;
    let mut child = ProcessCommand::new("sfdisk")
        .args(["--wipe", "always", target])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(problem("sfdisk_start"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| ControlProblem::new("sfdisk_stdin", "sfdisk stdin unavailable"))?
        .write_all(b"label: gpt\n,;\n")
        .map_err(problem("sfdisk_write"))?;
    let output = child.wait_with_output().map_err(problem("sfdisk_wait"))?;
    if !output.status.success() {
        return Err(ControlProblem::new(
            "sfdisk_failed",
            String::from_utf8_lossy(&output.stderr).trim(),
        ));
    }
    run_process("udevadm", &["settle"])?;
    let listing = run_process("lsblk", &["-nrpo", "PATH,TYPE", target])?;
    let partition = listing
        .lines()
        .filter_map(|line| line.split_once(' '))
        .find_map(|(path, kind)| (kind.trim() == "part").then(|| PathBuf::from(path)))
        .ok_or_else(|| {
            ControlProblem::new("partition_missing", "Neue Partition wurde nicht erkannt")
        })?;
    let path = partition.to_string_lossy().into_owned();
    run_process("mkfs.xfs", &["-f", "-L", label, &path])?;
    Ok(partition)
}

fn block_uuid(path: &Path) -> Result<String, ControlProblem> {
    let path = path.to_string_lossy();
    let value = run_process("blkid", &["-s", "UUID", "-o", "value", &path])?;
    let value = value.trim();
    if value.is_empty() {
        Err(ControlProblem::new(
            "uuid_missing",
            "Blockgerät besitzt keine UUID",
        ))
    } else {
        Ok(value.to_owned())
    }
}

fn mount_filesystem(uuid: &str, target: &Path) -> Result<(), ControlProblem> {
    std::fs::create_dir_all(target).map_err(problem("mount_directory"))?;
    let target = target.to_string_lossy();
    run_process(
        "mount",
        &[
            "-t",
            "xfs",
            "-o",
            "noatime,nodev,nosuid",
            &format!("UUID={uuid}"),
            &target,
        ],
    )?;
    Ok(())
}

fn write_runtime_environment(settings: &crate::RepositorySettings) -> Result<(), ControlProblem> {
    if dry_run() {
        return Ok(());
    }
    let path = Path::new(RUNTIME_ENV);
    let parent = path
        .parent()
        .ok_or_else(|| ControlProblem::new("runtime_path", "Runtime environment has no parent"))?;
    std::fs::create_dir_all(parent).map_err(problem("runtime_directory"))?;
    let stage = path.with_extension("staged");
    let reduction = match settings.advanced_reduction {
        AdvancedReduction::Off => "off",
        AdvancedReduction::PrefixV1 => "prefix-v1",
    };
    let mut file = std::fs::File::create(&stage).map_err(problem("runtime_write"))?;
    writeln!(file, "FASTDUP_ADVANCED_REDUCTION={reduction}").map_err(problem("runtime_write"))?;
    writeln!(
        file,
        "FASTDUP_ONLINE_GC_ENABLED={}",
        u8::from(settings.online_gc_enabled)
    )
    .map_err(problem("runtime_write"))?;
    writeln!(
        file,
        "FASTDUP_ONLINE_GC_PRESSURE_LOW_BASIS_POINTS={}",
        settings.pressure_low_basis_points
    )
    .map_err(problem("runtime_write"))?;
    writeln!(
        file,
        "FASTDUP_ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS={}",
        settings.pressure_high_basis_points
    )
    .map_err(problem("runtime_write"))?;
    if let Some(window) = &settings.maintenance_window_utc {
        writeln!(file, "FASTDUP_ONLINE_GC_DAILY_WINDOW_UTC={window}")
            .map_err(problem("runtime_write"))?;
    }
    file.sync_all().map_err(problem("runtime_sync"))?;
    std::fs::rename(stage, path).map_err(problem("runtime_publish"))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(problem("runtime_sync"))
}

fn read_frontend_counters() -> Option<RuntimeFrontendCounters> {
    let mut stream = StdUnixStream::connect(MANAGEMENT_SOCKET).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(400)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(400)))
        .ok()?;
    stream
        .write_all(br#"{"version":1,"operation":{"kind":"inspect"}}"#)
        .ok()?;
    stream.shutdown(Shutdown::Write).ok()?;
    let mut response = Vec::new();
    stream.take(16 * 1_024).read_to_end(&mut response).ok()?;
    let response: serde_json::Value = serde_json::from_slice(&response).ok()?;
    if response.get("ok")?.as_bool()? {
        Some(RuntimeFrontendCounters {
            read_bytes: response.pointer("/frontend/read_bytes")?.as_u64()?,
            write_bytes: response.pointer("/frontend/write_bytes")?.as_u64()?,
            exact_hit_bytes: response.pointer("/frontend/exact_hit_bytes")?.as_u64()?,
            new_chunk_bytes: response.pointer("/frontend/new_chunk_bytes")?.as_u64()?,
            logical_chunk_bytes: response
                .pointer("/frontend/logical_chunk_bytes")?
                .as_u64()?,
            physical_container_bytes: response
                .pointer("/frontend/physical_container_bytes")?
                .as_u64()?,
            presented_capacity_revision: response
                .get("presented_capacity_revision")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    } else {
        None
    }
}

fn share_capacity_revision(shares: &[ShareSettings]) -> String {
    let mut shares = shares.iter().collect::<Vec<_>>();
    shares.sort_by(|left, right| left.id.cmp(&right.id));
    let mut digest = Sha256::new();
    digest.update(b"fastdup-presented-capacity-v1\0");
    for share in shares {
        digest.update(share.id.as_bytes());
        digest.update([0]);
        if let Some(capacity) = share.logical_quota {
            digest.update(capacity.bytes().unwrap_or_default().to_le_bytes());
        } else {
            digest.update(0_u64.to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

fn sync_share_capacities(shares: &[ShareSettings]) -> Result<(), ControlProblem> {
    if !repository_mount_is_active() {
        return Err(ControlProblem::new(
            "repository_mount_unavailable",
            "Repository-Mount ist für die Share-Aktivierung nicht verfügbar",
        ));
    }
    let mut rules = Vec::new();
    for share in shares {
        let path = SambaConfig::share_path(share);
        std::fs::create_dir_all(&path).map_err(problem("share_directory"))?;
        if let Some(capacity) = share.logical_quota {
            let capacity_bytes = capacity.bytes().ok_or_else(|| {
                ControlProblem::new(
                    "share_capacity_invalid",
                    "Share-Kapazität überschreitet den unterstützten Bereich",
                )
            })?;
            let inode = std::fs::metadata(&path)
                .map_err(problem("share_directory"))?
                .ino();
            rules.push(serde_json::json!({
                "inode": inode,
                "capacity_bytes": capacity_bytes,
            }));
        }
    }
    let revision = share_capacity_revision(shares);
    send_management_operation(&serde_json::json!({
        "kind": "update_presented_capacities",
        "revision": revision,
        "rules": rules,
    }))?;
    persist_share_capacity_manifest(&revision, &rules)?;
    Ok(())
}

fn persist_share_capacity_manifest(
    revision: &str,
    rules: &[serde_json::Value],
) -> Result<(), ControlProblem> {
    if dry_run() {
        return Ok(());
    }
    let path = Path::new(SHARE_CAPACITY_MANIFEST);
    let parent = path.parent().ok_or_else(|| {
        ControlProblem::new(
            "share_capacity_manifest_path",
            "Share-Kapazitätsmanifest besitzt kein Elternverzeichnis",
        )
    })?;
    std::fs::create_dir_all(parent).map_err(problem("share_capacity_manifest_directory"))?;
    let stage = path.with_extension("staged");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&stage)
        .map_err(problem("share_capacity_manifest_write"))?;
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({
            "version": 1,
            "revision": revision,
            "rules": rules,
        }),
    )
    .map_err(problem("share_capacity_manifest_write"))?;
    file.write_all(b"\n")
        .map_err(problem("share_capacity_manifest_write"))?;
    file.sync_all()
        .map_err(problem("share_capacity_manifest_sync"))?;
    std::fs::rename(&stage, path).map_err(problem("share_capacity_manifest_publish"))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(problem("share_capacity_manifest_sync"))
}

fn repository_mount_is_active() -> bool {
    let mount = std::fs::metadata(POSIX_MOUNT);
    let parent = Path::new(POSIX_MOUNT).parent().map(std::fs::metadata);
    matches!((mount, parent), (Ok(mount), Some(Ok(parent))) if mount.dev() != parent.dev())
}

fn send_management_operation(
    operation: &serde_json::Value,
) -> Result<serde_json::Value, ControlProblem> {
    let mut stream =
        StdUnixStream::connect(MANAGEMENT_SOCKET).map_err(problem("management_socket"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(problem("management_socket"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(problem("management_socket"))?;
    let request = serde_json::json!({ "version": 1, "operation": operation });
    serde_json::to_writer(&mut stream, &request).map_err(problem("management_encode"))?;
    stream
        .shutdown(Shutdown::Write)
        .map_err(problem("management_socket"))?;
    let response: serde_json::Value =
        serde_json::from_reader(stream).map_err(problem("management_decode"))?;
    if response.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        Ok(response)
    } else {
        Err(ControlProblem::new(
            "management_rejected",
            response
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("FastDup runtime rejected the setting"),
        ))
    }
}

fn send_online_gc_configuration(
    settings: &crate::RepositorySettings,
) -> Result<(), ControlProblem> {
    if dry_run() {
        return Ok(());
    }
    send_management_operation(&serde_json::json!({
        "kind": "update_online_gc",
        "enabled": settings.online_gc_enabled,
        "pressure_low_basis_points": settings.pressure_low_basis_points,
        "pressure_high_basis_points": settings.pressure_high_basis_points,
    }))
    .map(|_| ())
}

fn run_process(program: &str, arguments: &[&str]) -> Result<String, ControlProblem> {
    let output = ProcessCommand::new(program)
        .args(arguments)
        .output()
        .map_err(problem("command_start"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(ControlProblem::new(
            "command_failed",
            format!(
                "{program} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ))
    }
}

fn problem<E: std::fmt::Display>(code: &'static str) -> impl FnOnce(E) -> ControlProblem {
    move |error| ControlProblem::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_pressure_is_fail_closed() {
        let settings = crate::RepositorySettings {
            pressure_low_basis_points: 9_500,
            pressure_high_basis_points: 9_000,
            ..crate::RepositorySettings::default()
        };
        assert!(validate_settings(&settings).is_err());
    }

    #[test]
    fn offline_maintenance_is_a_fixed_storage_job() {
        assert_eq!(maintenance_unit("scrub").unwrap(), SCRUB_UNIT);
        assert_eq!(
            maintenance_unit("rebuild-pool-indexes").unwrap(),
            "fastdup-maintenance@rebuild-pool-indexes.service"
        );
        assert!(maintenance_unit("arbitrary-command").is_err());
    }

    #[test]
    fn share_capacity_revision_is_order_independent_and_capacity_sensitive() {
        let mut first = test_share("one");
        let second = test_share("two");
        let initial = share_capacity_revision(&[first.clone(), second.clone()]);
        assert_eq!(
            initial,
            share_capacity_revision(&[second.clone(), first.clone()])
        );
        first.logical_quota = Some(crate::LogicalQuota {
            value: 10,
            unit: crate::CapacityUnit::Tb,
        });
        assert_ne!(initial, share_capacity_revision(&[first, second]));
    }

    fn test_share(id: &str) -> ShareSettings {
        ShareSettings {
            id: id.to_owned(),
            revision: 1,
            name: id.to_owned(),
            description: String::new(),
            enabled: true,
            hidden: false,
            read_only: false,
            guest_access: false,
            encryption: crate::SmbEncryption::Desired,
            access_based_enumeration: true,
            allowed_users: Vec::new(),
            allowed_groups: Vec::new(),
            logical_quota: None,
        }
    }
}
