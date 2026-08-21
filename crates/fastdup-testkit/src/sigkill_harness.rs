//! Real-process SIGKILL, remount, and durability-window harness.

use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const RECORD_BYTES: usize = 64 * 1_024;
const RECORDS_PER_CASE: usize = 4;
const MOUNT_POLL: Duration = Duration::from_millis(25);
const MOUNT_TIMEOUT: Duration = Duration::from_secs(10);
const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(5);
const SIGKILL_NUMBER: i32 = 9;

/// Real-process crash matrix using the accepted ten-second durability window.
#[derive(Clone, Debug)]
pub struct SigkillRemountConfig {
    daemon: PathBuf,
    run_root: PathBuf,
    kill_delays: Vec<Duration>,
    durability_window: Duration,
}

impl SigkillRemountConfig {
    /// Builds the canonical v1 matrix around every externally observable
    /// scheduler boundary.
    #[must_use]
    pub fn v1(daemon: PathBuf, run_root: PathBuf) -> Self {
        Self {
            daemon,
            run_root,
            kill_delays: vec![
                Duration::ZERO,
                Duration::from_millis(750),
                Duration::from_millis(2_250),
                Duration::from_millis(4_750),
                Duration::from_millis(5_250),
                Duration::from_millis(9_500),
                Duration::from_secs(11),
            ],
            durability_window: Duration::from_secs(10),
        }
    }

    /// Executes every case in a fresh repository below the new run root.
    ///
    /// The only correctness observations are acknowledged POSIX writes, live
    /// reads, actual daemon `SIGKILL`, a new daemon mounted over the same
    /// stores, and byte-exact recovered reads. The harness never inspects
    /// internal Checkpoint phases or durable files.
    ///
    /// # Errors
    ///
    /// Returns configuration, process, mount, syscall, timeout, non-prefix,
    /// mixed-record, or durability-deadline failures. The run root and all
    /// logs remain available for diagnosis.
    pub fn run(self) -> Result<SigkillRemountReport, SigkillHarnessError> {
        let daemon = canonical_file(&self.daemon)?;
        let run_root = create_new_run_root(&self.run_root)?;
        if self.kill_delays.is_empty() || self.durability_window.is_zero() {
            return Err(SigkillHarnessError::InvalidConfiguration);
        }
        let mut cases = Vec::new();
        cases
            .try_reserve_exact(self.kill_delays.len())
            .map_err(|_| SigkillHarnessError::OutOfMemory)?;
        for (ordinal, kill_delay) in self.kill_delays.iter().copied().enumerate() {
            cases.push(run_case(
                &daemon,
                &run_root,
                ordinal,
                kill_delay,
                self.durability_window,
            )?);
        }
        Ok(SigkillRemountReport {
            run_root,
            durability_window: self.durability_window,
            cases,
        })
    }
}

/// Successful evidence from one complete real-process matrix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SigkillRemountReport {
    run_root: PathBuf,
    durability_window: Duration,
    cases: Vec<SigkillCaseReport>,
}

impl SigkillRemountReport {
    #[must_use]
    pub fn run_root(&self) -> &Path {
        &self.run_root
    }

    #[must_use]
    pub const fn durability_window(&self) -> Duration {
        self.durability_window
    }

    #[must_use]
    pub fn cases(&self) -> &[SigkillCaseReport] {
        &self.cases
    }
}

/// One validated kill-offset observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SigkillCaseReport {
    kill_delay: Duration,
    acknowledged_to_kill: Duration,
    acknowledged_records: usize,
    required_records: usize,
    recovered_records: usize,
    recovered_file_present: bool,
}

impl SigkillCaseReport {
    #[must_use]
    pub const fn kill_delay(self) -> Duration {
        self.kill_delay
    }

    #[must_use]
    pub const fn acknowledged_to_kill(self) -> Duration {
        self.acknowledged_to_kill
    }

    #[must_use]
    pub const fn acknowledged_records(self) -> usize {
        self.acknowledged_records
    }

    #[must_use]
    pub const fn required_records(self) -> usize {
        self.required_records
    }

    #[must_use]
    pub const fn recovered_records(self) -> usize {
        self.recovered_records
    }

    #[must_use]
    pub const fn recovered_file_present(self) -> bool {
        self.recovered_file_present
    }

    #[must_use]
    pub const fn deadline_required(self) -> bool {
        self.required_records == self.acknowledged_records
    }
}

fn run_case(
    daemon: &Path,
    run_root: &Path,
    ordinal: usize,
    kill_delay: Duration,
    durability_window: Duration,
) -> Result<SigkillCaseReport, SigkillHarnessError> {
    let case_root = run_root.join(format!("case-{ordinal:02}-{}ms", kill_delay.as_millis()));
    let mount = case_root.join("mount");
    let metadata = case_root.join("metadata");
    let data = case_root.join("data");
    std::fs::create_dir(&case_root)?;
    for directory in [&mount, &metadata, &data] {
        std::fs::create_dir(directory)?;
    }

    let mut process = MountedDaemon::start(
        daemon,
        &mount,
        &metadata,
        &data,
        &case_root.join("ingest-daemon.log"),
    )?;
    let path = mount.join("deadline-stream.bin");
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(RECORD_BYTES * RECORDS_PER_CASE)
        .map_err(|_| SigkillHarnessError::OutOfMemory)?;
    let mut acknowledgements = Vec::new();
    acknowledgements
        .try_reserve_exact(RECORDS_PER_CASE)
        .map_err(|_| SigkillHarnessError::OutOfMemory)?;
    for record in 0..RECORDS_PER_CASE {
        let frame = deterministic_frame(ordinal, record);
        let written = file.write(&frame)?;
        if written != frame.len() {
            return Err(SigkillHarnessError::ShortWrite {
                expected: frame.len(),
                observed: written,
            });
        }
        expected.extend_from_slice(&frame);
        acknowledgements.push(Instant::now());
    }
    let final_acknowledgement = *acknowledgements
        .last()
        .expect("ASSERT: every crash case acknowledges at least one record");
    file.sync_all()?;
    let mut live = vec![0_u8; expected.len()];
    file.read_exact_at(&mut live, 0)?;
    if live != expected {
        return Err(SigkillHarnessError::LiveReadMismatch);
    }
    drop(file);

    let elapsed = final_acknowledgement.elapsed();
    if let Some(remaining) = kill_delay.checked_sub(elapsed) {
        thread::sleep(remaining);
    }
    let kill_instant = Instant::now();
    let acknowledged_to_kill = kill_instant.duration_since(final_acknowledgement);
    let required_records = acknowledgements
        .iter()
        .filter(|acknowledged| kill_instant.duration_since(**acknowledged) >= durability_window)
        .count();
    process.sigkill_and_detach()?;

    let mut recovery = MountedDaemon::start(
        daemon,
        &mount,
        &metadata,
        &data,
        &case_root.join("recovery-daemon.log"),
    )?;
    let (recovered_file_present, recovered) = match std::fs::read(&path) {
        Ok(bytes) => (true, bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (false, Vec::new()),
        Err(error) => return Err(error.into()),
    };
    recovery.sigkill_and_detach()?;

    if recovered.len() % RECORD_BYTES != 0 || !expected.starts_with(&recovered) {
        return Err(SigkillHarnessError::NonAtomicRecoveredPrefix {
            case: ordinal,
            recovered_bytes: recovered.len(),
        });
    }
    let recovered_records = recovered.len() / RECORD_BYTES;
    if recovered_records < required_records {
        return Err(SigkillHarnessError::DurabilityDeadlineMiss {
            case: ordinal,
            required_records,
            recovered_records,
            acknowledged_to_kill,
        });
    }
    Ok(SigkillCaseReport {
        kill_delay,
        acknowledged_to_kill,
        acknowledged_records: acknowledgements.len(),
        required_records,
        recovered_records,
        recovered_file_present,
    })
}

fn deterministic_frame(case: usize, record: usize) -> Vec<u8> {
    let mut bytes = vec![0_u8; RECORD_BYTES];
    bytes[..8].copy_from_slice(b"FASTKILL");
    bytes[8..16].copy_from_slice(
        &u64::try_from(case)
            .expect("ASSERT: bounded case ordinal fits u64")
            .to_le_bytes(),
    );
    bytes[16..24].copy_from_slice(
        &u64::try_from(record)
            .expect("ASSERT: bounded record ordinal fits u64")
            .to_le_bytes(),
    );
    let mut state = u64::try_from(case)
        .expect("ASSERT: bounded case ordinal fits u64")
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ u64::try_from(record)
            .expect("ASSERT: bounded record ordinal fits u64")
            .wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ 0xD1B5_4A32_D192_ED03;
    for byte in &mut bytes[24..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    bytes
}

struct MountedDaemon {
    child: Option<Child>,
    mount: PathBuf,
    log: PathBuf,
}

impl MountedDaemon {
    fn start(
        daemon: &Path,
        mount: &Path,
        metadata: &Path,
        data: &Path,
        log: &Path,
    ) -> Result<Self, SigkillHarnessError> {
        let output = OpenOptions::new().create_new(true).write(true).open(log)?;
        let error_output = output.try_clone()?;
        let child = Command::new(daemon)
            .arg(mount)
            .arg(metadata)
            .arg(data)
            .env("FASTDUP_IO_URING", "off")
            .stdin(Stdio::null())
            .stdout(Stdio::from(output))
            .stderr(Stdio::from(error_output))
            .spawn()?;
        let mut process = Self {
            child: Some(child),
            mount: mount.to_path_buf(),
            log: log.to_path_buf(),
        };
        let deadline = Instant::now() + MOUNT_TIMEOUT;
        loop {
            if is_mounted(&process.mount)? {
                return Ok(process);
            }
            if let Some(status) = process
                .child
                .as_mut()
                .expect("ASSERT: starting daemon owns its child")
                .try_wait()?
            {
                process.child = None;
                return Err(SigkillHarnessError::DaemonExited {
                    status,
                    log: process.log.clone(),
                });
            }
            if Instant::now() >= deadline {
                return Err(SigkillHarnessError::MountTimeout {
                    mount: process.mount.clone(),
                    log: process.log.clone(),
                });
            }
            thread::sleep(MOUNT_POLL);
        }
    }

    fn sigkill_and_detach(&mut self) -> Result<(), SigkillHarnessError> {
        let mut child = self
            .child
            .take()
            .expect("ASSERT: mounted daemon is killed exactly once");
        let kill = child.kill();
        let wait = child.wait();
        kill?;
        let status = wait?;
        if status.signal() != Some(SIGKILL_NUMBER) {
            return Err(SigkillHarnessError::UnexpectedExit {
                status,
                log: self.log.clone(),
            });
        }
        detach_mount(&self.mount)
    }
}

impl Drop for MountedDaemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = detach_mount(&self.mount);
    }
}

fn detach_mount(mount: &Path) -> Result<(), SigkillHarnessError> {
    if !is_mounted(mount)? {
        return Ok(());
    }
    let status = Command::new("umount")
        .arg("--lazy")
        .arg(mount)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(SigkillHarnessError::UnmountFailed {
            mount: mount.to_path_buf(),
            status,
        });
    }
    let deadline = Instant::now() + UNMOUNT_TIMEOUT;
    while is_mounted(mount)? {
        if Instant::now() >= deadline {
            return Err(SigkillHarnessError::UnmountTimeout(mount.to_path_buf()));
        }
        thread::sleep(MOUNT_POLL);
    }
    Ok(())
}

fn is_mounted(expected: &Path) -> io::Result<bool> {
    let mountinfo = std::fs::read("/proc/self/mountinfo")?;
    for line in mountinfo.split(|byte| *byte == b'\n') {
        let Some(encoded) = line.split(|byte| *byte == b' ').nth(4) else {
            continue;
        };
        let decoded = decode_mountinfo_field(encoded)?;
        if std::ffi::OsStr::from_bytes(&decoded) == expected.as_os_str() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn decode_mountinfo_field(encoded: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(encoded.len())
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    let mut cursor = 0;
    while cursor < encoded.len() {
        if encoded[cursor] != b'\\' {
            decoded.push(encoded[cursor]);
            cursor += 1;
            continue;
        }
        let octal = encoded
            .get(cursor + 1..cursor + 4)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short mountinfo escape"))?;
        if !octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid mountinfo escape",
            ));
        }
        let value = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + (octal[2] - b'0');
        decoded.push(value);
        cursor += 4;
    }
    Ok(decoded)
}

fn canonical_file(path: &Path) -> Result<PathBuf, SigkillHarnessError> {
    let canonical = std::fs::canonicalize(path)?;
    if !canonical.is_file() {
        return Err(SigkillHarnessError::DaemonNotFile(canonical));
    }
    Ok(canonical)
}

fn create_new_run_root(path: &Path) -> Result<PathBuf, SigkillHarnessError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if absolute.exists() {
        return Err(SigkillHarnessError::RunRootExists(absolute));
    }
    let parent = absolute
        .parent()
        .ok_or(SigkillHarnessError::InvalidConfiguration)?;
    let name = absolute
        .file_name()
        .ok_or(SigkillHarnessError::InvalidConfiguration)?;
    let canonical_parent = std::fs::canonicalize(parent)?;
    let root = canonical_parent.join(name);
    std::fs::create_dir(&root)?;
    Ok(root)
}

/// Harness setup, process, mount, or recovery-oracle failure.
#[derive(Debug)]
pub enum SigkillHarnessError {
    Io(io::Error),
    InvalidConfiguration,
    OutOfMemory,
    DaemonNotFile(PathBuf),
    RunRootExists(PathBuf),
    DaemonExited {
        status: ExitStatus,
        log: PathBuf,
    },
    UnexpectedExit {
        status: ExitStatus,
        log: PathBuf,
    },
    MountTimeout {
        mount: PathBuf,
        log: PathBuf,
    },
    UnmountFailed {
        mount: PathBuf,
        status: ExitStatus,
    },
    UnmountTimeout(PathBuf),
    ShortWrite {
        expected: usize,
        observed: usize,
    },
    LiveReadMismatch,
    NonAtomicRecoveredPrefix {
        case: usize,
        recovered_bytes: usize,
    },
    DurabilityDeadlineMiss {
        case: usize,
        required_records: usize,
        recovered_records: usize,
        acknowledged_to_kill: Duration,
    },
}

impl fmt::Display for SigkillHarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for SigkillHarnessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SigkillHarnessError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::decode_mountinfo_field;

    #[test]
    fn mountinfo_decoder_preserves_raw_names_and_decodes_kernel_escapes() {
        assert_eq!(
            decode_mountinfo_field(br"/source/a\040b\011c\012d\134e")
                .expect("valid mountinfo escapes decode"),
            b"/source/a b\tc\nd\\e"
        );
        assert_eq!(
            decode_mountinfo_field(b"/source/non-utf8-\xff")
                .expect("unescaped bytes remain byte exact"),
            b"/source/non-utf8-\xff"
        );
    }

    #[test]
    fn mountinfo_decoder_rejects_truncated_and_non_octal_escapes() {
        assert!(decode_mountinfo_field(br"/source/bad\04").is_err());
        assert!(decode_mountinfo_field(br"/source/bad\08x").is_err());
    }
}
