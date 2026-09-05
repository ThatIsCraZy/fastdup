//! Additive SMB ingress reconciliation. Never removes operator-owned rules.
use std::collections::BTreeSet;
use std::process::Command;

use crate::ControlProblem;

pub(crate) fn ensure_smb(share_count: usize) -> Result<(), ControlProblem> {
    reconcile(share_count, |args| {
        let output = Command::new("firewall-cmd")
            .args(args)
            .output()
            .map_err(|error| ControlProblem::new("firewall_failed", error.to_string()))?;
        if !output.status.success() {
            return Err(ControlProblem::new(
                "firewall_failed",
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    })
}

fn reconcile(
    share_count: usize,
    mut run: impl FnMut(&[&str]) -> Result<String, ControlProblem>,
) -> Result<(), ControlProblem> {
    if share_count == 0 {
        return Ok(());
    }
    let active = run(&["--get-active-zones"])?;
    let mut zones: BTreeSet<String> = active
        .lines()
        .filter(|line| !line.starts_with(char::is_whitespace))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .collect();
    zones.insert(run(&["--get-default-zone"])?.trim().to_owned());
    for zone in zones {
        if zone.is_empty() {
            continue;
        }
        let zone = format!("--zone={zone}");
        // Idempotent additions preserve other firewall configuration; no reload.
        run(&[&zone, "--add-port=445/tcp"])?;
        run(&["--permanent", &zone, "--add-port=445/tcp"])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_share_opens_only_tcp_445_in_active_and_default_zones() {
        let mut calls = Vec::new();
        reconcile(1, |args| {
            calls.push(args.join(" "));
            Ok(match args {
                ["--get-active-zones"] => {
                    "public (default)\n  interfaces: eth0\ninternal\n  interfaces: eth1\n"
                }
                ["--get-default-zone"] => "public\n",
                _ => "success",
            }
            .to_owned())
        })
        .unwrap();
        assert_eq!(calls.len(), 6);
        for zone in ["public", "internal"] {
            assert!(calls.contains(&format!("--zone={zone} --add-port=445/tcp")));
            assert!(calls.contains(&format!("--permanent --zone={zone} --add-port=445/tcp")));
        }
        reconcile(0, |_| panic!("no share must not change firewall")).unwrap();
    }
    #[test]
    fn firewall_failures_are_reported() {
        assert!(
            reconcile(1, |_| Err(ControlProblem::new(
                "firewall_failed",
                "unavailable"
            )))
            .is_err()
        );
    }
}
