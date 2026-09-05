use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fastdup_control::{
    AgentRequest, AgentResponse, AgentRuntime, ControlProblem, ControlStore, SambaConfig,
    TelemetryStore, TlsIdentity,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};

const MAX_REQUEST_BYTES: u64 = 1_048_576;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let state_directory = PathBuf::from(
        std::env::var_os("FASTDUP_CONTROL_STATE_DIR")
            .unwrap_or_else(|| "/var/lib/fastdup/control".into()),
    );
    let socket_path = PathBuf::from(
        std::env::var_os("FASTDUP_AGENT_SOCKET")
            .unwrap_or_else(|| fastdup_control::CONTROL_SOCKET_PATH.into()),
    );
    let store = ControlStore::open(&state_directory.join("control.db"))?;
    let telemetry = TelemetryStore::open(&state_directory.join("telemetry.db"))?;
    let identity = TlsIdentity::load_or_generate(&state_directory.join("tls"), &[hostname()])?;
    let runtime = AgentRuntime::new(
        store,
        telemetry,
        SambaConfig::new("/etc/samba/fastdup-shares.conf"),
        identity.fingerprint,
    );
    runtime.start_sampler();
    let listener = bind_socket(&socket_path)?;
    runtime.reconcile_startup();
    let control_user = std::env::var("FASTDUP_CONTROL_UID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let control_group = rustix::process::getegid().as_raw();
    tracing::info!(path = %socket_path.display(), "fastdup agent ready");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let runtime = Arc::clone(&runtime);
                tokio::spawn(async move {
                    if let Err(error) =
                        handle_client(stream, runtime, control_user, control_group).await
                    {
                        tracing::warn!(%error, "agent request rejected");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
        }
    }
    drop(listener);
    let _ = std::fs::remove_file(socket_path);
    Ok(())
}

fn bind_socket(path: &Path) -> Result<UnixListener, std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o750))?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => return Err(std::io::Error::other("agent socket path is not a socket")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
    Ok(listener)
}

async fn handle_client(
    stream: UnixStream,
    runtime: Arc<AgentRuntime>,
    control_user: u32,
    control_group: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let credentials = stream.peer_cred()?;
    if !peer_allowed(
        credentials.uid(),
        credentials.gid(),
        control_user,
        control_group,
    ) {
        return Err("Unix peer is not the configured Control Plane user".into());
    }
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader)
        .take(MAX_REQUEST_BYTES)
        .read_line(&mut line)
        .await?;
    let response = match serde_json::from_str::<AgentRequest>(&line) {
        Ok(request) => tokio::task::spawn_blocking(move || runtime.handle_request(request)).await?,
        Err(error) => AgentResponse {
            version: fastdup_control::AGENT_PROTOCOL_VERSION,
            request_id: "invalid".to_owned(),
            result: Err(ControlProblem::new("invalid_request", error.to_string())),
        },
    };
    let mut encoded = serde_json::to_vec(&response)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    Ok(())
}

fn peer_allowed(
    caller_user: u32,
    caller_group: u32,
    control_user: u32,
    control_group: u32,
) -> bool {
    caller_user == 0 || caller_user == control_user || caller_group == control_group
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname").map_or_else(
        |_| "fastdup-appliance".to_owned(),
        |value| value.trim().to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::peer_allowed;

    #[test]
    fn agent_accepts_only_root_or_the_configured_control_identity() {
        assert!(peer_allowed(0, 0, 991, 990));
        assert!(peer_allowed(991, 1_000, 991, 990));
        assert!(peer_allowed(1_000, 990, 991, 990));
        assert!(!peer_allowed(1_000, 1_000, 991, 990));
    }
}
