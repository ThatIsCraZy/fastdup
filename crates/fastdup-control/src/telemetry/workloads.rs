//! Sampled connections are workload context, never per-client I/O accounting.
//! Samba's documented JSON schema: <https://www.samba.org/samba/docs/current/man-html/smbstatus.1.html>
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadContext {
    pub observed_at: i64,
    pub smb_status: String,
    pub truncated: bool,
    pub repository: Option<String>,
    pub shares: Vec<WorkloadShare>,
    pub connections: Vec<WorkloadConnection>,
    /// The repository service does not currently expose backup job/session IDs.
    /// An SMB session ID is explicitly not a backup session ID.
    pub backup_sessions: Option<Vec<String>>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadShare {
    pub id: String,
    pub name: String,
    pub path: String,
    pub protocol: String,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadConnection {
    pub share: String,
    pub client: String,
    pub smb_session_id: Option<String>,
    pub connected_at: Option<String>,
}

pub(crate) fn parse_connections(body: &[u8]) -> Option<(Vec<WorkloadConnection>, bool)> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let connections = value.get("tcons")?.as_object()?;
    let mut result = Vec::new();
    for connection in connections.values().take(256) {
        let string = |key| {
            connection
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        result.push(WorkloadConnection {
            share: string("service")?,
            client: string("machine")?,
            smb_session_id: string("session_id"),
            connected_at: string("connected_at"),
        });
    }
    Some((result, connections.len() > 256))
}

pub(crate) async fn sample(store: &crate::ControlStore) -> WorkloadContext {
    let mut context = WorkloadContext {
        observed_at: crate::unix_seconds(),
        smb_status: "unavailable".into(),
        truncated: false,
        repository: store
            .repository_binding()
            .ok()
            .flatten()
            .map(|r| r.data_uuid),
        shares: store
            .shares()
            .unwrap_or_default()
            .iter()
            .filter(|s| s.enabled)
            .map(|s| WorkloadShare {
                id: s.id.clone(),
                name: s.name.clone(),
                path: crate::SambaConfig::share_path(s).display().to_string(),
                protocol: "smb".into(),
            })
            .collect(),
        connections: Vec::new(),
        backup_sessions: None,
    };
    if store.veeam().ok().flatten().is_some() {
        context.shares.push(WorkloadShare {
            id: "veeam-native".into(),
            name: "Veeam".into(),
            path: crate::veeam::DIRECTORY.into(),
            protocol: "native".into(),
        });
    }
    let read = async {
        let mut child = tokio::process::Command::new("smbstatus")
            .args(["--json", "--shares"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let mut body = Vec::new();
        child
            .stdout
            .take()?
            .take(1_048_577)
            .read_to_end(&mut body)
            .await
            .ok()?;
        if body.len() > 1_048_576 {
            return None;
        }
        if !child.wait().await.ok()?.success() {
            return None;
        }
        parse_connections(&body)
    };
    if let Ok(Some((connections, truncated))) =
        tokio::time::timeout(std::time::Duration::from_secs(2), read).await
    {
        context.smb_status = "available".into();
        context.connections = connections;
        context.truncated = truncated;
    }
    context
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn connections_keep_client_share_and_smb_session_separate() {
        let body = br#"{"tcons":{"1":{"service":"backup","machine":"10.0.0.8","session_id":"42","connected_at":"2026-10-23T01:00:00Z"}}}"#;
        let (connections, truncated) = parse_connections(body).unwrap();
        assert!(!truncated);
        assert_eq!(connections[0].share, "backup");
        assert_eq!(connections[0].client, "10.0.0.8");
        assert_eq!(connections[0].smb_session_id.as_deref(), Some("42"));
        assert!(parse_connections(br"{}").is_none());
        assert_eq!(parse_connections(br#"{"tcons":{}}"#), Some((vec![], false)));
        assert!(parse_connections(br#"{"tcons":{"1":{"service":"backup"}}}"#).is_none());
    }
    #[test]
    fn sampled_connections_are_bounded_and_truncation_is_explicit() {
        let tcons: serde_json::Map<_, _> = (0..300)
            .map(|i| {
                (
                    i.to_string(),
                    serde_json::json!({"service":"s","machine":"c"}),
                )
            })
            .collect();
        let (connections, truncated) =
            parse_connections(&serde_json::to_vec(&serde_json::json!({"tcons":tcons})).unwrap())
                .unwrap();
        assert_eq!(connections.len(), 256);
        assert!(truncated);
    }
}
