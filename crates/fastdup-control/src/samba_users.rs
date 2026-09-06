//! SMB credentials live only in Samba's passdb. The web process never runs
//! account tools, and passwords never enter Jobs, audit text, argv, or files.
use crate::ControlProblem;
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::process::{Command, Stdio};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SambaUserRequest {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for SambaUserRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SambaUserRequest")
            .field("username", &self.username)
            .field("password", &"<REDACTED>")
            .finish()
    }
}

impl SambaUserRequest {
    pub fn validate(&self) -> Result<(), ControlProblem> {
        let name = self.username.as_bytes();
        if name.is_empty()
            || name.len() > 32
            || !name[0].is_ascii_lowercase()
            || !name
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
        {
            return Err(ControlProblem::new(
                "samba_user_invalid",
                "SMB-Benutzername: 1–32 Kleinbuchstaben, Ziffern, _ oder -, beginnend mit einem Buchstaben",
            ));
        }
        if self.password.chars().count() < 12
            || self.password.len() > 256
            || self.password.contains(['\n', '\r', '\0'])
        {
            return Err(ControlProblem::new(
                "samba_password_invalid",
                "SMB-Passwort: mindestens zwölf Zeichen, maximal 256 Bytes, keine Zeilenumbrüche",
            ));
        }
        Ok(())
    }
}

fn failed() -> ControlProblem {
    ControlProblem::new(
        "samba_account_failed",
        "SMB-Konto konnte nicht angelegt werden",
    )
}

pub fn list() -> Result<Vec<String>, ControlProblem> {
    let output = Command::new("/usr/bin/pdbedit")
        .arg("-L")
        .output()
        .map_err(|_| failed())?;
    if !output.status.success() {
        return Err(failed());
    }
    let mut users: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once(':').map(|(name, _)| name.to_owned()))
        .collect();
    users.sort();
    Ok(users)
}

pub fn create(request: &SambaUserRequest) -> Result<(), ControlProblem> {
    request.validate()?;
    // Refuse all existing Unix identities, especially root/system accounts.
    // Account creation is serialized by the agent; useradd also checks NSS.
    let lookup = Command::new("/usr/bin/getent")
        .args(["passwd", &request.username])
        .output()
        .map_err(|_| failed())?;
    if lookup.status.success() || list()?.contains(&request.username) {
        return Err(ControlProblem::new(
            "samba_user_exists",
            "Benutzername ist bereits vorhanden; bestehende Konten werden nicht verändert",
        ));
    }
    if lookup.status.code() != Some(2) {
        return Err(failed());
    }
    let added = Command::new("/usr/sbin/useradd")
        .args([
            "--no-create-home",
            "--shell",
            "/sbin/nologin",
            "--comment",
            "FastDup SMB account",
            "--",
            &request.username,
        ])
        .output()
        .map_err(|_| failed())?;
    if !added.status.success() {
        return Err(failed());
    }
    let result = (|| {
        let mut child = Command::new("/usr/bin/smbpasswd")
            .args(["-s", "-a", &request.username])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| failed())?;
        let input = child.stdin.take().ok_or_else(failed)?;
        let mut input = input;
        let written = writeln!(input, "{}\n{}", request.password, request.password);
        drop(input);
        let status = child.wait().map_err(|_| failed())?;
        written.map_err(|_| failed())?;
        if status.success() {
            Ok(())
        } else {
            Err(failed())
        }
    })();
    if result.is_err() {
        // Only the account just created by this call is eligible for rollback.
        let _ = Command::new("/usr/bin/smbpasswd")
            .args(["-x", &request.username])
            .output();
        let _ = Command::new("/usr/sbin/userdel")
            .args(["--", &request.username])
            .output();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_option_and_stdin_injection_and_redacts_debug() {
        for username in ["-root", "a/b", "a\nb", "Root", ""] {
            assert!(
                SambaUserRequest {
                    username: username.into(),
                    password: "long-valid-password".into()
                }
                .validate()
                .is_err()
            );
        }
        let mut request = SambaUserRequest {
            username: "backup".into(),
            password: "long-valid-password".into(),
        };
        assert!(request.validate().is_ok());
        assert!(!format!("{request:?}").contains(&request.password));
        request.password = "long-password\ninjected".into();
        assert!(request.validate().is_err());
    }
}
