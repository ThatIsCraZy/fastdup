//! Coordinates the SMB frontend with the lifecycle of the repository mount.

use std::path::Path;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

const SMB_UNIT: &str = "smb.service";
const SYSTEMCTL: &str = "/usr/bin/systemctl";
const TIMEOUT: &str = "/usr/bin/timeout";
const FUSERMOUNT: &str = "/usr/bin/fusermount3";
const FINDMNT: &str = "/usr/bin/findmnt";
const JOB_TIMEOUT: &str = "30s";
const KILL_AFTER_TIMEOUT: &str = "5s";
const UNMOUNT_ATTEMPTS: usize = 3;
const START_ATTEMPTS: usize = 3;
const START_RETRY: Duration = Duration::from_secs(1);
const FORCE_ATTEMPTS: usize = 12;
const FORCE_POLL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendStartOutcome {
    Disabled,
    AlreadyActive,
    Started,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendStopOutcome {
    AlreadyInactive,
    Stopped,
    ForceStopped,
    Failed,
}

impl BackendStartOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::AlreadyActive => "already-active",
            Self::Started => "started",
            Self::Failed => "failed",
        }
    }
}

impl BackendStopOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyInactive => "already-inactive",
            Self::Stopped => "stopped",
            Self::ForceStopped => "force-stopped",
            Self::Failed => "failed",
        }
    }
}

pub async fn force_unmount(path: &Path) -> bool {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || force_unmount_blocking(&path))
        .await
        .unwrap_or(false)
}

pub async fn start_after_mount() -> BackendStartOutcome {
    tokio::task::spawn_blocking(start_after_mount_blocking)
        .await
        .unwrap_or(BackendStartOutcome::Failed)
}

pub async fn stop_for_unmount() -> BackendStopOutcome {
    tokio::task::spawn_blocking(stop_for_unmount_blocking)
        .await
        .unwrap_or(BackendStopOutcome::Failed)
}

fn start_after_mount_blocking() -> BackendStartOutcome {
    start_with(&run_system_command)
}

fn stop_for_unmount_blocking() -> BackendStopOutcome {
    stop_with(&run_system_command)
}

fn start_with(runner: &dyn Fn(&[&str]) -> bool) -> BackendStartOutcome {
    if !runner(&[SYSTEMCTL, "is-enabled", "--quiet", SMB_UNIT]) {
        return BackendStartOutcome::Disabled;
    }
    if runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
        return BackendStartOutcome::AlreadyActive;
    }
    for attempt in 0..START_ATTEMPTS {
        let _ = runner(&bounded(SYSTEMCTL, &["start", SMB_UNIT]));
        if runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
            return BackendStartOutcome::Started;
        }
        if attempt + 1 < START_ATTEMPTS {
            sleep(START_RETRY);
        }
    }
    BackendStartOutcome::Failed
}

fn stop_with(runner: &dyn Fn(&[&str]) -> bool) -> BackendStopOutcome {
    if !runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
        return BackendStopOutcome::AlreadyInactive;
    }
    let _ = runner(&bounded(SYSTEMCTL, &["stop", SMB_UNIT]));
    if !runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
        return BackendStopOutcome::Stopped;
    }
    if force_inactive(runner) {
        return BackendStopOutcome::ForceStopped;
    }
    BackendStopOutcome::Failed
}

fn force_inactive(runner: &dyn Fn(&[&str]) -> bool) -> bool {
    for attempt in 0..FORCE_ATTEMPTS {
        if !runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
            return true;
        }
        let _ = runner(&bounded(SYSTEMCTL, &["kill", "--signal=SIGKILL", SMB_UNIT]));
        if !runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]) {
            return true;
        }
        if attempt + 1 < FORCE_ATTEMPTS {
            sleep(FORCE_POLL);
        }
    }
    !runner(&[SYSTEMCTL, "is-active", "--quiet", SMB_UNIT])
}

fn bounded<'a>(command: &'a str, args: &'a [&'a str]) -> Vec<&'a str> {
    let mut bounded = vec![
        TIMEOUT,
        "--foreground",
        "--kill-after",
        KILL_AFTER_TIMEOUT,
        JOB_TIMEOUT,
        command,
    ];
    bounded.extend_from_slice(args);
    bounded
}

fn run_system_command(args: &[&str]) -> bool {
    let Some((command, args)) = args.split_first() else {
        return false;
    };
    Command::new(command)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn is_fuse_mount(path: &Path) -> bool {
    match Command::new(FINDMNT)
        .args(["-n", "-o", "FSTYPE", "--mountpoint"])
        .arg(path)
        .output()
    {
        Ok(output) => {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .starts_with("fuse")
        }
        Err(_) => true,
    }
}

fn force_unmount_blocking(path: &Path) -> bool {
    for attempt in 0..UNMOUNT_ATTEMPTS {
        if !is_fuse_mount(path) {
            return true;
        }
        if run_system_command(&[
            FUSERMOUNT,
            "-u",
            "--",
            path.as_os_str().to_string_lossy().as_ref(),
        ]) {
            return true;
        }
        if attempt + 1 < UNMOUNT_ATTEMPTS {
            sleep(Duration::from_secs(1));
        }
    }
    if !is_fuse_mount(path) {
        return true;
    }
    let _ = run_system_command(&[
        FUSERMOUNT,
        "-z",
        "-u",
        "--",
        path.to_string_lossy().as_ref(),
    ]);
    !is_fuse_mount(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Runner {
        active: Rc<RefCell<bool>>,
        calls: Rc<RefCell<Vec<Vec<String>>>>,
        enabled: bool,
        stop_succeeds: bool,
    }

    impl Runner {
        fn new(active: bool, enabled: bool, stop_succeeds: bool) -> Self {
            Self {
                active: Rc::new(RefCell::new(active)),
                calls: Rc::new(RefCell::new(Vec::new())),
                enabled,
                stop_succeeds,
            }
        }

        fn run(&self, args: &[&str]) -> bool {
            self.calls
                .borrow_mut()
                .push(args.iter().map(|argument| (*argument).to_owned()).collect());
            match args[0] {
                FINDMNT => true,
                FUSERMOUNT => {
                    *self.active.borrow_mut() = false;
                    true
                }
                _ => match args.get(if args[0] == TIMEOUT { 5 } else { 0 }) {
                    Some(&"systemctl") | Some(&SYSTEMCTL) => {
                        match args.get(if args[0] == TIMEOUT { 6 } else { 1 }) {
                            Some(&"is-enabled") => self.enabled,
                            Some(&"is-active") => *self.active.borrow(),
                            Some(&"start") => {
                                *self.active.borrow_mut() = true;
                                true
                            }
                            Some(&"stop") => {
                                if self.stop_succeeds {
                                    *self.active.borrow_mut() = false;
                                }
                                self.stop_succeeds
                            }
                            Some(&"kill") => {
                                *self.active.borrow_mut() = false;
                                true
                            }
                            _ => false,
                        }
                    }
                    _ => false,
                },
            }
        }
    }

    #[test]
    fn start_only_touches_an_enabled_backend() {
        let runner = Runner::new(false, false, true);
        let runner_ref = runner.clone();
        let outcome = start_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStartOutcome::Disabled);
        assert_eq!(runner.calls.borrow().len(), 1);
    }

    #[test]
    fn an_already_active_backend_is_not_restarted() {
        let runner = Runner::new(true, true, true);
        let runner_ref = runner.clone();
        let outcome = start_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStartOutcome::AlreadyActive);
        assert_eq!(
            runner.calls.borrow()[1],
            [SYSTEMCTL, "is-active", "--quiet", SMB_UNIT]
        );
    }

    #[test]
    fn mount_starts_a_stopped_enabled_backend() {
        let runner = Runner::new(false, true, true);
        let runner_ref = runner.clone();
        let outcome = start_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStartOutcome::Started);
        assert_eq!(
            runner.calls.borrow()[2],
            bounded(SYSTEMCTL, &["start", SMB_UNIT])
        );
    }

    #[test]
    fn graceful_stop_precedes_force() {
        let runner = Runner::new(true, true, true);
        let runner_ref = runner.clone();
        let outcome = stop_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStopOutcome::Stopped);
        assert_eq!(
            runner.calls.borrow()[1],
            bounded(SYSTEMCTL, &["stop", SMB_UNIT])
        );
        assert!(!runner.calls.borrow().iter().any(|call| {
            call.windows(2)
                .any(|arguments| arguments == [SYSTEMCTL, "kill"])
        }));
    }

    #[test]
    fn failed_graceful_stop_escalates_to_force() {
        let runner = Runner::new(true, true, false);
        let runner_ref = runner.clone();
        let outcome = stop_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStopOutcome::ForceStopped);
        assert_eq!(
            runner.calls.borrow()[1],
            bounded(SYSTEMCTL, &["stop", SMB_UNIT])
        );
        assert_eq!(
            runner.calls.borrow()[4],
            bounded(SYSTEMCTL, &["kill", "--signal=SIGKILL", SMB_UNIT])
        );
    }

    #[test]
    fn inactive_backend_is_not_stopped() {
        let runner = Runner::new(false, true, true);
        let runner_ref = runner.clone();
        let outcome = stop_with(&move |args| runner_ref.run(args));
        assert_eq!(outcome, BackendStopOutcome::AlreadyInactive);
        assert_eq!(runner.calls.borrow().len(), 1);
    }
}
