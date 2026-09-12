//! Mount/process evidence is independent of optional Runtime telemetry.
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::{RepositoryState, RuntimeIssue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessState {
    Running,
    Stopped,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimeHealth {
    pub mounted: Option<bool>,
    pub process: ProcessState,
}

impl RuntimeHealth {
    pub fn issue(
        self,
        state: &RepositoryState,
        integrity_failed: Option<bool>,
        previous: Option<RuntimeIssue>,
    ) -> Option<RuntimeIssue> {
        if !matches!(
            state,
            RepositoryState::Online
                | RepositoryState::Error
                | RepositoryState::Mounting
                | RepositoryState::Recovering
        ) {
            return None;
        }
        if self.process == ProcessState::Failed {
            return Some(RuntimeIssue::ProcessExited);
        }
        if matches!(
            state,
            RepositoryState::Mounting | RepositoryState::Recovering
        ) {
            return None;
        }
        if integrity_failed == Some(true) {
            return Some(RuntimeIssue::IntegrityFailed);
        }
        if self.mounted == Some(false) || self.process == ProcessState::Stopped {
            return Some(RuntimeIssue::Unavailable);
        }
        // A missed metrics sample must neither create nor clear a confirmed failure.
        // During auto-restart an old FUSE entry can briefly outlive its owner.
        if matches!(
            previous,
            Some(RuntimeIssue::IntegrityFailed | RuntimeIssue::ProcessExited)
        ) && integrity_failed.is_none()
        {
            return previous;
        }
        if self.mounted.is_none() || self.process == ProcessState::Unknown {
            return previous;
        }
        None
    }

    pub fn read(mount: &str, unit: &str) -> Self {
        // Reading mountinfo never issues a potentially blocked FUSE getattr.
        let mounted = std::fs::read_to_string("/proc/self/mountinfo")
            .ok()
            .map(|text| mount_present(&text, mount));
        Self {
            mounted,
            process: read_process(unit),
        }
    }
}

fn mount_present(mountinfo: &str, mount: &str) -> bool {
    mountinfo.lines().any(|line| {
        let Some((fields, filesystem)) = line.split_once(" - ") else {
            return false;
        };
        fields.split_whitespace().nth(4) == Some(mount)
            && filesystem
                .split_whitespace()
                .next()
                .is_some_and(|fs| fs == "fuse" || fs.starts_with("fuse."))
    })
}

fn parse_process(properties: &str) -> ProcessState {
    let property = |name: &str| properties.lines().find_map(|line| line.strip_prefix(name));
    let active = property("ActiveState=");
    let result = property("Result=");
    // Result catches SIGABRT/asserts and OOM even during systemd's restart delay.
    if active == Some("failed")
        || result.is_some_and(|value| value != "success" && !value.is_empty())
    {
        return ProcessState::Failed;
    }
    match active {
        Some("active")
            if property("MainPID=")
                .and_then(|pid| pid.parse::<u32>().ok())
                .is_some_and(|pid| pid != 0) =>
        {
            ProcessState::Running
        }
        Some("inactive" | "deactivating") => ProcessState::Stopped,
        _ => ProcessState::Unknown,
    }
}

fn read_process(unit: &str) -> ProcessState {
    let Ok(mut child) = Command::new("systemctl")
        .args(["show", unit, "--property=ActiveState,MainPID,Result"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return ProcessState::Unknown;
    };
    let deadline = Instant::now() + Duration::from_millis(400);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map_or(ProcessState::Unknown, |output| {
                        parse_process(&String::from_utf8_lossy(&output.stdout))
                    });
            }
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return ProcessState::Unknown;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mount_and_process_survive_missing_metrics_but_not_a_crash() {
        let mounted = RuntimeHealth {
            mounted: Some(true),
            process: ProcessState::Running,
        };
        assert_eq!(mounted.issue(&RepositoryState::Online, None, None), None);
        assert_eq!(
            mounted.issue(&RepositoryState::Online, Some(false), None),
            None
        );
        assert_eq!(
            mounted.issue(&RepositoryState::Online, Some(true), None),
            Some(RuntimeIssue::IntegrityFailed)
        );
        assert_eq!(
            mounted.issue(
                &RepositoryState::Online,
                None,
                Some(RuntimeIssue::IntegrityFailed)
            ),
            Some(RuntimeIssue::IntegrityFailed)
        );
        assert_eq!(
            mounted.issue(
                &RepositoryState::Online,
                Some(false),
                Some(RuntimeIssue::IntegrityFailed)
            ),
            None
        );
        assert_eq!(
            mounted.issue(
                &RepositoryState::Online,
                None,
                Some(RuntimeIssue::ProcessExited)
            ),
            Some(RuntimeIssue::ProcessExited)
        );
        assert_eq!(
            mounted.issue(
                &RepositoryState::Online,
                Some(false),
                Some(RuntimeIssue::ProcessExited)
            ),
            None
        );
        let crashed = RuntimeHealth {
            process: ProcessState::Failed,
            ..mounted
        };
        assert_eq!(
            crashed.issue(&RepositoryState::Online, None, None),
            Some(RuntimeIssue::ProcessExited)
        );
        assert_eq!(
            crashed.issue(&RepositoryState::Mounting, None, None),
            Some(RuntimeIssue::ProcessExited)
        );
        assert_eq!(crashed.issue(&RepositoryState::Unmounted, None, None), None);
        let absent = RuntimeHealth {
            mounted: Some(false),
            ..mounted
        };
        assert_eq!(
            absent.issue(&RepositoryState::Online, None, None),
            Some(RuntimeIssue::Unavailable)
        );
        assert_eq!(absent.issue(&RepositoryState::Mounting, None, None), None);
        let unknown = RuntimeHealth {
            mounted: None,
            process: ProcessState::Unknown,
        };
        assert_eq!(unknown.issue(&RepositoryState::Online, None, None), None);
        assert_eq!(
            unknown.issue(
                &RepositoryState::Online,
                None,
                Some(RuntimeIssue::ProcessExited)
            ),
            Some(RuntimeIssue::ProcessExited)
        );
    }
    #[test]
    fn service_failure_survives_auto_restart_delay_and_resets_on_new_process() {
        assert_eq!(
            parse_process("ActiveState=activating\nMainPID=0\nResult=core-dump\n"),
            ProcessState::Failed
        );
        assert_eq!(
            parse_process("ActiveState=failed\nMainPID=0\nResult=oom-kill\n"),
            ProcessState::Failed
        );
        assert_eq!(
            parse_process("ActiveState=active\nMainPID=123\nResult=success\n"),
            ProcessState::Running
        );
        assert_eq!(
            parse_process("ActiveState=inactive\nMainPID=0\nResult=success\n"),
            ProcessState::Stopped
        );
        assert_eq!(
            parse_process("ActiveState=activating\nMainPID=0\nResult=success\n"),
            ProcessState::Unknown
        );
        assert_eq!(parse_process(""), ProcessState::Unknown);
    }
    #[test]
    fn mount_table_requires_the_exact_fuse_mount() {
        let line = "82 23 0:77 / /srv/fastdup/repository rw - fuse.fastdup fastdup rw\n";
        assert!(mount_present(line, "/srv/fastdup/repository"));
        assert!(!mount_present(line, "/srv/fastdup"));
        assert!(!mount_present(
            &line.replace("fuse.fastdup", "xfs"),
            "/srv/fastdup/repository"
        ));
        assert!(!mount_present("", "/srv/fastdup/repository"));
    }
}
