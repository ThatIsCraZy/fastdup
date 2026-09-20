//! One managed Veeam service container and its exclusive Namespace root.
//! Linux repository frontend with scoped Fast Clone and immutable-flag adapters.
use std::net::Ipv4Addr;
use std::os::unix::fs::{PermissionsExt as _, chown};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{AdvancedReduction, ControlProblem, LogicalQuota};

pub const DIRECTORY: &str = "/srv/fastdup/repository/veeam";
pub const IMMUTABILITY_STATE_DIRECTORY: &str = "/srv/fastdup/repository/.fastdup-veeam-immurepo";
pub const UNIT: &str = "fastdup-veeam.service";
/// Container root in the fixed, private nspawn user namespace.
pub const CONTAINER_ROOT_UID: u32 = 524_288;
/// The guest transport account (uid 1000) in the reserved user namespace.
pub const CONTAINER_UID: u32 = 525_288;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImmutabilityIdentity {
    pub authority_uid: u32,
    pub authority_gid: u32,
    pub writer_uid: u32,
    pub writer_gid: u32,
    pub handoff_uid: u32,
    pub handoff_gid: u32,
}

/// Resolves the effective host IDs of Veeam's running transport service.
///
/// The vendor may allocate a different guest service group on a new install
/// or upgrade. Reading the service's actual process also accounts for systemd
/// overrides and the container's active user-namespace mapping. Any ambiguous,
/// host-root, or unmapped identity fails closed.
#[must_use]
pub fn immutability_identity() -> Option<ImmutabilityIdentity> {
    use crate::control::run_process;

    let leader = run_process(
        "machinectl",
        &["show", "--property=Leader", "--value", "fastdup-veeam"],
    )
    .ok()?
    .trim()
    .parse::<u32>()
    .ok()?;
    if leader <= 1 {
        return None;
    }
    let (authority_uid, authority_gid) =
        service_host_identity(leader, "veeamimmurepo.service", "veeamimmureposvc")?;
    let (writer_uid, writer_gid) =
        service_host_identity(leader, "veeamtransport.service", "veeamtransport")?;
    let uid_map = std::fs::read_to_string(format!("/proc/{leader}/uid_map")).ok()?;
    let gid_map = std::fs::read_to_string(format!("/proc/{leader}/gid_map")).ok()?;
    let handoff_uid = map_guest_id(0, &uid_map)?;
    let handoff_gid = map_guest_id(0, &gid_map)?;
    if authority_uid != handoff_uid
        || authority_uid == 0
        || authority_gid == 0
        || writer_uid == 0
        || writer_gid == 0
        || handoff_uid == 0
        || handoff_gid == 0
        || !host_id_is_shifted_and_mapped(authority_gid, &gid_map)
        || !host_id_is_shifted_and_mapped(writer_uid, &uid_map)
        || !host_id_is_shifted_and_mapped(writer_gid, &gid_map)
    {
        return None;
    }
    Some(ImmutabilityIdentity {
        authority_uid,
        authority_gid,
        writer_uid,
        writer_gid,
        handoff_uid,
        handoff_gid,
    })
}

fn service_host_identity(leader: u32, unit: &str, executable: &str) -> Option<(u32, u32)> {
    let namespace_pid = crate::control::run_process(
        "systemctl",
        &[
            "--machine=fastdup-veeam",
            "show",
            "--property=MainPID",
            "--value",
            unit,
        ],
    )
    .ok()?
    .trim()
    .parse::<u32>()
    .ok()?;
    let process_root = format!("/proc/{leader}/root/proc/{namespace_pid}");
    let observed_executable = std::fs::read_link(format!("{process_root}/exe")).ok()?;
    if observed_executable
        .file_name()
        .and_then(|name| name.to_str())
        != Some(executable)
    {
        return None;
    }
    let status = std::fs::read_to_string(format!("{process_root}/status")).ok()?;
    Some((
        effective_process_id(&status, "Uid:")?,
        effective_process_id(&status, "Gid:")?,
    ))
}

fn effective_process_id(status: &str, field: &str) -> Option<u32> {
    status
        .lines()
        .find(|line| line.starts_with(field))?
        .split_ascii_whitespace()
        .nth(2)?
        .parse()
        .ok()
}

fn map_guest_id(id: u32, map: &str) -> Option<u32> {
    map.lines().find_map(|line| {
        let mut fields = line.split_ascii_whitespace();
        let guest_start = fields.next().and_then(|value| value.parse::<u64>().ok());
        let host_start = fields.next().and_then(|value| value.parse::<u64>().ok());
        let length = fields.next().and_then(|value| value.parse::<u64>().ok());
        let (guest, host, length) = match (guest_start, host_start, length) {
            (Some(guest), Some(host), Some(length)) if host > 0 => (guest, host, length),
            _ => return None,
        };
        let offset = u64::from(id).checked_sub(guest)?;
        if offset >= length {
            return None;
        }
        u32::try_from(host.checked_add(offset)?).ok()
    })
}

fn host_id_is_shifted_and_mapped(id: u32, map: &str) -> bool {
    map.lines().any(|line| {
        let mut fields = line.split_ascii_whitespace();
        let _guest_start = fields.next();
        let host_start = fields.next().and_then(|value| value.parse::<u64>().ok());
        let length = fields.next().and_then(|value| value.parse::<u64>().ok());
        matches!((host_start, length), (Some(host), Some(len)) if host > 0 && u64::from(id) >= host && u64::from(id) < host.saturating_add(len))
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VeeamSettings {
    pub revision: u64,
    pub interface: String,
    pub address: Ipv4Addr,
    pub prefix: u8,
    pub gateway: Ipv4Addr,
    pub dns: Ipv4Addr,
    pub ssh_public_key: String,
    pub ssh_enabled: bool,
    #[serde(default)]
    pub hardened_immutability: bool,
    pub advanced_reduction: AdvancedReduction,
    pub logical_quota: Option<LogicalQuota>,
}

impl VeeamSettings {
    pub fn validate(&self) -> Result<(), ControlProblem> {
        let valid_ip = |ip: Ipv4Addr| {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && !ip.is_link_local()
        };
        if self.interface.is_empty()
            || self.interface.len() > 12
            || !self
                .interface
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            || !(1..=30).contains(&self.prefix)
            || !valid_ip(self.address)
            || !valid_ip(self.gateway)
            || !valid_ip(self.dns)
        {
            return Err(invalid("Ungültige IPv4-Netzwerkkonfiguration"));
        }
        let mask = u32::MAX << (32 - self.prefix);
        let address = u32::from(self.address);
        let gateway = u32::from(self.gateway);
        if address & mask != gateway & mask
            || address == gateway
            || address & !mask == 0
            || address & !mask == !mask
            || gateway & !mask == 0
            || gateway & !mask == !mask
        {
            return Err(invalid(
                "Adresse und Gateway müssen verschiedene Host-Adressen im selben Netz sein",
            ));
        }
        let key: Vec<_> = self.ssh_public_key.split_whitespace().collect();
        if !self.ssh_public_key.is_empty()
            && (self.ssh_public_key.len() > 8192
                || self.ssh_public_key.contains(['\n', '\r'])
                || key.len() < 2
                || !matches!(key[0], "ssh-ed25519" | "ssh-rsa")
                || key[1].len() < 32
                || !key[1]
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=')))
        {
            return Err(invalid(
                "Ein einzelner OpenSSH-Public-Key ohne Optionen ist erforderlich",
            ));
        }
        if self.logical_quota.is_some_and(|quota| !quota.is_valid()) {
            return Err(invalid(
                "Quota muss zwischen 1 und 999 GB, TB oder PB liegen",
            ));
        }
        Ok(())
    }
}

/// Transient install credential. Debug and audit output must never expose it.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct BootstrapPassword(pub String);

impl std::fmt::Debug for BootstrapPassword {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BootstrapPassword([redacted])")
    }
}

impl BootstrapPassword {
    pub fn validate(&self) -> Result<(), ControlProblem> {
        if !(16..=128).contains(&self.0.len())
            || !self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b':')
        {
            return Err(invalid(
                "Das temporäre Passwort benötigt 16–128 druckbare ASCII-Zeichen ohne Doppelpunkt",
            ));
        }
        Ok(())
    }

    fn install(&self) -> Result<(), ControlProblem> {
        use std::io::Write as _;
        use std::process::{Command, Stdio};
        self.validate()?;
        let mut child = Command::new("systemd-run")
            .args([
                "--quiet",
                "--wait",
                "--pipe",
                "--machine=fastdup-veeam",
                "/usr/sbin/chpasswd",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| invalid("Temporäres Passwort konnte nicht gesetzt werden"))?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| invalid("Passwortkanal ist nicht verfügbar"))?;
        let sent = writeln!(input, "veeam:{}", self.0);
        drop(input);
        let status = child.wait();
        if sent.is_err() || !status.is_ok_and(|status| status.success()) {
            return Err(invalid("Temporäres Passwort konnte nicht gesetzt werden"));
        }
        Ok(())
    }
}

pub fn provision(
    settings: &VeeamSettings,
    password: Option<&BootstrapPassword>,
) -> Result<(), ControlProblem> {
    use crate::control::run_process;
    use std::os::unix::fs::OpenOptionsExt as _;
    let problem =
        |error: std::io::Error| ControlProblem::new("veeam_configuration", error.to_string());
    // Persist desired state before provisioning. A failed/interrupted install
    // retains its reservation and policy and can be retried from the WebUI.
    let file = Path::new("/etc/fastdup/veeam.json");
    let stage = file.with_extension("json.staged");
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&stage)
        .map_err(problem)?;
    serde_json::to_writer(&mut output, settings)
        .map_err(|error| ControlProblem::new("veeam_configuration", error.to_string()))?;
    output.sync_all().map_err(problem)?;
    std::fs::rename(&stage, file).map_err(problem)?;
    std::fs::File::open("/etc/fastdup")
        .and_then(|directory| directory.sync_all())
        .map_err(problem)?;
    run_process("systemctl", &["start", "fastdup-veeam-provision.service"])?;
    run_process("systemctl", &["enable", "--now", crate::veeam::UNIT])?;
    if let Some(password) = password {
        password.install()?;
    }
    let mut readiness = vec![
        "fastdup-network.service",
        "fastdup-repository-guard.service",
    ];
    if settings.ssh_enabled {
        readiness.push("sshd.service");
    }
    for unit in readiness {
        run_process(
            "systemctl",
            &["--machine=fastdup-veeam", "is-active", "--quiet", unit],
        )
        .map_err(|_| {
            ControlProblem::new(
                "veeam_not_ready",
                format!("Container läuft, aber {unit} ist nicht bereit"),
            )
        })?;
    }
    Ok(())
}

fn invalid(message: &str) -> ControlProblem {
    ControlProblem::new("veeam_invalid", message)
}

pub fn prepare_directory() -> Result<(), std::io::Error> {
    for (name, owner) in [
        (DIRECTORY, CONTAINER_UID),
        (IMMUTABILITY_STATE_DIRECTORY, CONTAINER_ROOT_UID),
    ] {
        let path = Path::new(name);
        // Never follow an existing symlink into a share or out of the repository.
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                return Err(std::io::Error::other(
                    "Veeam roots must be real directories",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(path)?;
            }
            Err(error) => return Err(error),
        }
        chown(path, Some(owner), Some(owner))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> VeeamSettings {
        serde_json::from_value(serde_json::json!({
            "revision": 0, "interface": "eth0", "address": "192.0.2.50",
            "prefix":24, "gateway":"192.0.2.1", "dns":"192.0.2.53",
            "sshPublicKey":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExamplePublicKey00000000000",
            "sshEnabled":true, "hardenedImmutability":false,
            "advancedReduction":"off", "logicalQuota":null
        }))
        .unwrap()
    }

    #[test]
    fn effective_identity_requires_a_shifted_mapping() {
        let status = "Name:\tveeamtransport\nUid:\t525288\t525288\t525288\t525288\nGid:\t524699\t524699\t524699\t524699\n";
        assert_eq!(effective_process_id(status, "Uid:"), Some(525_288));
        assert_eq!(effective_process_id(status, "Gid:"), Some(524_699));
        assert_eq!(map_guest_id(0, "0 524288 65536\n"), Some(524_288));
        assert_eq!(map_guest_id(1_000, "0 524288 65536\n"), Some(525_288));
        assert_eq!(map_guest_id(411, "0 524288 65536\n"), Some(524_699));
        assert_eq!(map_guest_id(1_000, "0 0 4294967295\n"), None);
        assert_eq!(map_guest_id(70_000, "0 524288 65536\n"), None);
        assert!(host_id_is_shifted_and_mapped(525_288, "0 524288 65536\n"));
        assert!(!host_id_is_shifted_and_mapped(1_000, "0 0 4294967295\n"));
    }
    #[test]
    fn bootstrap_password_is_transient_and_debug_redacted() {
        let password = BootstrapPassword("temporary-only-0123456789".into());
        assert!(password.validate().is_ok());
        assert!(!format!("{password:?}").contains(&password.0));
        assert!(BootstrapPassword("short".into()).validate().is_err());
        assert!(
            BootstrapPassword("new-user:injected-password".into())
                .validate()
                .is_err()
        );
        assert!(
            BootstrapPassword("line-one\nline-two-more".into())
                .validate()
                .is_err()
        );
        assert!(
            !serde_json::to_string(&settings())
                .unwrap()
                .contains("password")
        );
    }

    #[test]
    fn desired_state_survives_restart_and_rejects_stale_revision() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sqlite");
        let store = crate::ControlStore::open(&path).unwrap();
        let config = settings();
        store.save_veeam(&config).unwrap();
        assert!(store.save_veeam(&config).is_err());
        drop(store);
        let store = crate::ControlStore::open(&path).unwrap();
        let mut restored = store.veeam().unwrap().unwrap();
        assert_eq!(restored.revision, 1);
        restored.logical_quota = Some(crate::LogicalQuota {
            value: 100,
            unit: crate::CapacityUnit::Gb,
        });
        store.save_veeam(&restored).unwrap();
        assert_eq!(
            store.veeam().unwrap().unwrap().logical_quota,
            restored.logical_quota
        );
    }

    #[test]
    fn rejects_injection_and_invalid_networks() {
        assert!(settings().validate().is_ok());
        let mut s = settings();
        s.interface = "ens33\nExecStart=bad".into();
        assert!(s.validate().is_err());
        let mut s = settings();
        s.gateway = "198.51.100.1".parse().unwrap();
        assert!(s.validate().is_err());
        let mut s = settings();
        s.address = "192.0.2.0".parse().unwrap();
        assert!(s.validate().is_err());
        let mut s = settings();
        s.ssh_public_key.push_str("\nssh-rsa bad");
        assert!(s.validate().is_err());
    }
}
