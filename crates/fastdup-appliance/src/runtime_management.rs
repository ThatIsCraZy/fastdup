//! Serve the typed management protocol independently of durability waits.
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fastdup_appliance::TieredStatFsSource;
use fastdup_posix::{FrontendTelemetry, InodeId, LogicalQuotaRule, Namespace};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

use super::{FsAppliance, OnlineGcRuntimeConfiguration, TelemetryStorageIo, runtime_telemetry};

const MAX_REQUESTS: usize = 16;
const PROTOCOL_VERSION: u16 = 1;
const SOCKET_NAME: &str = ".fastdup-management.sock";
const MAX_REQUEST_BYTES: u64 = 1_048_576;
const MAX_CAPACITY_RULES: usize = 4_096;

pub(super) struct ManagementListener {
    listener: UnixListener,
    guard: SocketGuard,
}

struct SocketGuard {
    path: std::path::PathBuf,
}

#[derive(Clone, Debug)]
pub(super) struct PresentedCapacityControl {
    statfs: TieredStatFsSource,
    namespace: Arc<Namespace>,
}

trait CapacityControl: Send + Sync {
    fn replace(&self, revision: String, rules: Vec<(u64, u64)>) -> io::Result<()>;
    fn revision(&self) -> io::Result<String>;
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
        rules: Vec<PresentedCapacityRule>,
        #[serde(default)]
        reduction_rules: Option<Vec<ReductionRule>>,
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
struct PresentedCapacityRule {
    inode: u64,
    capacity_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct ShareCapacityManifest {
    version: u16,
    revision: String,
    rules: Vec<PresentedCapacityRule>,
    #[serde(default)]
    reduction_rules: Vec<ReductionRule>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct ReductionRule {
    inode: u64,
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct ManagementResponse {
    version: u16,
    ok: bool,
    error: Option<String>,
    frontend: Option<FrontendResponse>,
    presented_capacity_revision: Option<String>,
    small_file_policy: Option<SmallFilePolicyResponse>,
}

#[derive(Debug, Serialize)]
struct SmallFilePolicyResponse {
    revision: String,
    extensions: Vec<String>,
}

impl From<fastdup_posix::SmallFilePolicySnapshot> for SmallFilePolicyResponse {
    fn from(snapshot: fastdup_posix::SmallFilePolicySnapshot) -> Self {
        Self {
            revision: snapshot.revision,
            extensions: snapshot.extensions,
        }
    }
}

#[derive(Debug, Serialize)]
struct FrontendResponse {
    mutation_admission_open: bool,
    integrity_failed: bool,
    logical_allocated_bytes: Option<u64>,
    logical_allocated_observed_at: Option<u64>,
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

impl ManagementListener {
    pub(super) fn bind(metadata_root: &Path) -> io::Result<Self> {
        use std::os::unix::fs::PermissionsExt as _;

        let path = metadata_root.join(SOCKET_NAME);
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
        Ok(Self {
            listener: UnixListener::from_std(listener)?,
            guard: SocketGuard { path },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn start(
        self,
        telemetry: Arc<FrontendTelemetry>,
        configuration: watch::Sender<OnlineGcRuntimeConfiguration>,
        capacity_control: PresentedCapacityControl,
        namespace: Arc<Namespace>,
        appliance: Arc<FsAppliance>,
        storage: TelemetryStorageIo,
    ) -> ManagementServer {
        ManagementServer::start_with_handler(self.listener, Some(self.guard), move |stream| {
            let telemetry = Arc::clone(&telemetry);
            let configuration = configuration.clone();
            let capacity_control = capacity_control.clone();
            let namespace = Arc::clone(&namespace);
            let appliance = Arc::clone(&appliance);
            let storage = storage.clone();
            async move {
                if let Err(error) = handle_request(
                    stream,
                    telemetry,
                    configuration,
                    capacity_control,
                    namespace,
                    appliance,
                    storage,
                )
                .await
                {
                    eprintln!("management_control_error={error}");
                }
            }
        })
    }
}

impl PresentedCapacityControl {
    pub(super) fn configure(
        statfs: TieredStatFsSource,
        namespace: Arc<Namespace>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        statfs.attach_logical_quota_namespace(&namespace)?;
        let control = Self { statfs, namespace };
        if let Some((path, manifest)) = load_share_capacity_manifest()? {
            // The manifest names Share inodes. A pool that was re-provisioned
            // keeps the manifest but not those inodes, and applying it then
            // fails the mount with a bare `NoEntry`. Fail closed, but say what
            // has to be corrected: skipping the rules would silently drop a
            // quota the operator configured.
            let describe = |error: String| {
                format!(
                    "Share capacity manifest {} does not match this Namespace ({error}).                      It names Share inodes from an earlier pool; remove or regenerate it.",
                    path.display()
                )
            };
            apply_reduction_rules(&control.namespace, manifest.reduction_rules)
                .map_err(describe)?;
            control
                .replace(
                    manifest.revision,
                    manifest
                        .rules
                        .into_iter()
                        .map(|rule| (rule.inode, rule.capacity_bytes))
                        .collect(),
                )
                .map_err(|error| describe(error.to_string()))?;
        }
        Ok(control)
    }
}

impl CapacityControl for PresentedCapacityControl {
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

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct ManagementServer {
    task: JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _socket_guard: Option<SocketGuard>,
}

impl ManagementServer {
    fn start_with_handler<F, R>(
        listener: UnixListener,
        socket_guard: Option<SocketGuard>,
        mut handle: F,
    ) -> Self
    where
        F: FnMut(UnixStream) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        let (shutdown, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept(), if requests.len() < MAX_REQUESTS => {
                        match accepted {
                            Ok((stream, _)) => { requests.spawn(handle(stream)); }
                            Err(error) => {
                                eprintln!("management_accept_error={error}");
                                break;
                            }
                        }
                    }
                    completed = requests.join_next(), if !requests.is_empty() => {
                        if let Some(Err(error)) = completed {
                            eprintln!("management_task_error={error}");
                        }
                    }
                }
            }
            requests.shutdown().await;
        });
        Self {
            task,
            shutdown: Some(shutdown),
            _socket_guard: socket_guard,
        }
    }

    pub async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = (&mut self.task).await;
    }
}

async fn handle_request(
    mut stream: UnixStream,
    telemetry: Arc<FrontendTelemetry>,
    configuration: watch::Sender<OnlineGcRuntimeConfiguration>,
    capacity_source: PresentedCapacityControl,
    namespace: Arc<Namespace>,
    appliance: Arc<FsAppliance>,
    storage: TelemetryStorageIo,
) -> Result<(), String> {
    let mut request = Vec::new();
    timeout(
        Duration::from_secs(5),
        (&mut stream)
            .take(MAX_REQUEST_BYTES + 1)
            .read_to_end(&mut request),
    )
    .await
    .map_err(|_| "management request timed out".to_owned())?
    .map_err(|error| format!("management request read failed: {error}"))?;
    let mut response = match serde_json::from_slice::<ManagementRequest>(&request) {
        Ok(request) if request.version == PROTOCOL_VERSION => apply_operation(
            request.operation,
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        ),
        Ok(_) => error_response("unsupported_version"),
        Err(_) => error_response("invalid_request"),
    };
    if let Some(frontend) = response.frontend.as_mut() {
        frontend.details =
            tokio::task::spawn_blocking(move || runtime_telemetry::snapshot(&appliance, &storage))
                .await
                .ok();
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
fn apply_operation(
    operation: ManagementOperation,
    telemetry: &FrontendTelemetry,
    configuration: &watch::Sender<OnlineGcRuntimeConfiguration>,
    capacity_source: &dyn CapacityControl,
    namespace: &Namespace,
) -> ManagementResponse {
    match operation {
        ManagementOperation::Inspect => {
            let snapshot = telemetry.snapshot();
            let logical_usage = namespace.sample_logical_usage();
            let ingest = runtime_telemetry::ingest_counters();
            ManagementResponse {
                version: PROTOCOL_VERSION,
                ok: true,
                error: None,
                frontend: Some(FrontendResponse {
                    mutation_admission_open: namespace.mutation_admission_open(),
                    integrity_failed: namespace.integrity_failed(),
                    logical_allocated_bytes: logical_usage.map(|value| value.0),
                    logical_allocated_observed_at: logical_usage.map(|value| value.1),
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
                    exact_hit_bytes: ingest.exact_hit,
                    new_chunk_bytes: ingest.new_chunk,
                    logical_chunk_bytes: ingest.logical_chunk,
                    physical_container_bytes: ingest.physical_container,
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
            match current
                .policy
                .with_pressure_watermarks(pressure_low_basis_points, pressure_high_basis_points)
            {
                Ok(policy)
                    if configuration
                        .send(OnlineGcRuntimeConfiguration { enabled, policy })
                        .is_ok() =>
                {
                    ok_response()
                }
                Ok(_) => error_response("online_gc_runtime_unavailable"),
                Err(error) => error_response(error.to_string()),
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
            ok_response()
        }
        ManagementOperation::UpdateSmallFileExtensions {
            revision,
            extensions,
        } => match namespace.replace_small_file_extensions(revision, extensions) {
            Ok(snapshot) => ManagementResponse {
                small_file_policy: Some(snapshot.into()),
                ..ok_response()
            },
            Err(error) => error_response(error.to_string()),
        },
    }
}

fn update_presented_capacities(
    capacity_source: &dyn CapacityControl,
    revision: String,
    rules: Vec<PresentedCapacityRule>,
) -> ManagementResponse {
    if rules.len() > MAX_CAPACITY_RULES {
        return error_response("too_many_presented_capacity_rules");
    }
    match capacity_source.replace(
        revision.clone(),
        rules
            .into_iter()
            .map(|rule| (rule.inode, rule.capacity_bytes))
            .collect(),
    ) {
        Ok(()) => ManagementResponse {
            presented_capacity_revision: Some(revision),
            ..ok_response()
        },
        Err(error) => error_response(error.to_string()),
    }
}

fn apply_reduction_rules(namespace: &Namespace, rules: Vec<ReductionRule>) -> Result<(), String> {
    let rules = rules
        .into_iter()
        .map(|rule| {
            InodeId::new(rule.inode)
                .map(|inode| (inode, rule.enabled))
                .ok_or_else(|| "Share inode must be nonzero".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    namespace
        .replace_share_reduction(namespace.advanced_reduction_default(), rules)
        .map_err(|error| format!("{error:?}"))
}

fn load_share_capacity_manifest()
-> Result<Option<(std::path::PathBuf, ShareCapacityManifest)>, Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("FASTDUP_SHARE_CAPACITY_MANIFEST") else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(path);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > MAX_REQUEST_BYTES {
        return Err("Share capacity manifest exceeds one MiB".into());
    }
    let manifest: ShareCapacityManifest = serde_json::from_slice(&std::fs::read(&path)?)?;
    if manifest.version != PROTOCOL_VERSION || manifest.rules.len() > MAX_CAPACITY_RULES {
        return Err("Share capacity manifest version or rule count is invalid".into());
    }
    Ok(Some((path, manifest)))
}

fn ok_response() -> ManagementResponse {
    ManagementResponse {
        version: PROTOCOL_VERSION,
        ok: true,
        error: None,
        frontend: None,
        presented_capacity_revision: None,
        small_file_policy: None,
    }
}

fn error_response(error: impl Into<String>) -> ManagementResponse {
    ManagementResponse {
        version: PROTOCOL_VERSION,
        ok: false,
        error: Some(error.into()),
        frontend: None,
        presented_capacity_revision: None,
        small_file_policy: None,
    }
}

impl Drop for ManagementServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_appliance::OnlineGcPolicy;
    use fastdup_posix::NamespaceConfig;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct CanceledRequest(tokio::sync::mpsc::UnboundedSender<()>);

    impl Drop for CanceledRequest {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[tokio::test]
    async fn requests_are_bounded_and_shutdown_retires_them() {
        let path =
            std::env::temp_dir().join(format!("fastdup-mgmt-bound-{}.sock", std::process::id()));
        let listener = UnixListener::bind(&path).unwrap();
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (canceled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        let server = ManagementServer::start_with_handler(listener, None, move |stream| {
            let started = started.clone();
            let guard = CanceledRequest(canceled.clone());
            async move {
                let _stream = stream;
                let _guard = guard;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            }
        });
        let mut clients = Vec::new();
        for _ in 0..=MAX_REQUESTS {
            clients.push(UnixStream::connect(&path).await.unwrap());
        }
        for _ in 0..MAX_REQUESTS {
            tokio::time::timeout(Duration::from_secs(1), starts.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), starts.recv())
                .await
                .is_err()
        );
        server.stop().await;
        for _ in 0..MAX_REQUESTS {
            cancellations
                .try_recv()
                .expect("shutdown must retire every accepted request");
        }
        assert!(cancellations.try_recv().is_err());
        drop(clients);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn inspection_accepts_while_supervisor_and_another_client_are_waiting() {
        let path = std::env::temp_dir().join(format!(
            "fastdup-management-isolation-{}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).unwrap();
        let server =
            ManagementServer::start_with_handler(listener, None, |mut stream| async move {
                let mut request = Vec::new();
                stream.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, b"inspect");
                stream.write_all(b"runtime-counters").await.unwrap();
            });
        // A client that has not finished its request cannot block other clients.
        let slow = UnixStream::connect(&path).await.unwrap();
        // The supervisor is awaiting a checkpoint. The separate listener must
        // serve the inspection that allows that simulated checkpoint to finish.
        let (completed, checkpoint) = tokio::sync::oneshot::channel();
        let client_path = path.clone();
        let client = tokio::spawn(async move {
            let mut stream = UnixStream::connect(client_path).await.unwrap();
            stream.write_all(b"inspect").await.unwrap();
            stream.shutdown().await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"runtime-counters");
            completed.send(()).unwrap();
        });
        let result = tokio::time::timeout(Duration::from_millis(400), checkpoint).await;
        server.stop().await;
        drop(slow);
        std::fs::remove_file(path).unwrap();
        result
            .expect("inspection must meet the sampler deadline during checkpoint wait")
            .unwrap();
        client.await.unwrap();
    }

    #[test]
    fn persisted_share_reduction_and_hot_default_are_backward_compatible() {
        let namespace = Namespace::new_volatile(NamespaceConfig::default());
        namespace.set_advanced_reduction_default(false);
        let legacy: ShareCapacityManifest =
            serde_json::from_str(r#"{"version":1,"revision":"old","rules":[]}"#).unwrap();
        assert!(legacy.reduction_rules.is_empty());
        let selected: ShareCapacityManifest = serde_json::from_str(
            r#"{"version":1,"revision":"new","rules":[],"reduction_rules":[{"inode":1,"enabled":true}]}"#,
        )
        .unwrap();
        apply_reduction_rules(&namespace, selected.reduction_rules).unwrap();
        assert!(namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE));
        let (configuration, _rx) = watch::channel(OnlineGcRuntimeConfiguration {
            enabled: false,
            policy: OnlineGcPolicy::default(),
        });
        let response = apply_operation(
            ManagementOperation::UpdateAdvancedReductionDefault { enabled: false },
            &FrontendTelemetry::default(),
            &configuration,
            &TestCapacityControl::default(),
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
                vec![ReductionRule {
                    inode: 0,
                    enabled: true,
                }]
            )
            .is_err()
        );
        assert!(!namespace.advanced_reduction_enabled(fastdup_posix::ROOT_INODE));
    }

    #[test]
    fn protocol_exposes_frontend_counters_and_hot_gc_policy() {
        let telemetry = FrontendTelemetry::default();
        let initial = OnlineGcRuntimeConfiguration {
            enabled: true,
            policy: OnlineGcPolicy::default(),
        };
        let (configuration, _configuration_rx) = watch::channel(initial);
        let capacity_source = TestCapacityControl::default();
        let namespace = Namespace::new_volatile(NamespaceConfig::default());

        let inspected = apply_operation(
            ManagementOperation::Inspect,
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(inspected.ok);
        assert!(inspected.frontend.is_some());
        assert!(inspected.frontend.as_ref().unwrap().mutation_admission_open);

        namespace.pause_mutation_admission_for(fastdup_posix::AdmissionPauseReason::Shutdown);
        let paused = apply_operation(
            ManagementOperation::Inspect,
            &telemetry,
            &configuration,
            &capacity_source,
            &namespace,
        );
        assert!(!paused.frontend.as_ref().unwrap().mutation_admission_open);
        assert!(!paused.frontend.as_ref().unwrap().integrity_failed);
        namespace.resume_mutation_admission();

        let updated = apply_operation(
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

        let quota = apply_operation(
            ManagementOperation::UpdatePresentedCapacities {
                reduction_rules: None,
                revision: "shares-r1".to_owned(),
                rules: vec![PresentedCapacityRule {
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
        assert_eq!(capacity_source.revision().unwrap(), "shares-r1");

        let suffixes = apply_operation(
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

        let rejected = apply_operation(
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
    async fn socket_is_root_only() {
        let root = std::env::temp_dir().join(format!(
            "fastdup-management-permissions-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create management fixture root");
        let listener = ManagementListener::bind(&root).expect("bind management socket");
        let mode = std::fs::metadata(root.join(SOCKET_NAME))
            .expect("management socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        drop(listener);
        std::fs::remove_dir(root).expect("remove management fixture root");
    }

    #[derive(Debug, Default)]
    struct TestCapacityControl {
        revision: Mutex<String>,
    }

    impl CapacityControl for TestCapacityControl {
        fn replace(&self, revision: String, _rules: Vec<(u64, u64)>) -> io::Result<()> {
            *self.revision.lock().expect("test capacity lock") = revision;
            Ok(())
        }

        fn revision(&self) -> io::Result<String> {
            Ok(self.revision.lock().expect("test capacity lock").clone())
        }
    }
}
