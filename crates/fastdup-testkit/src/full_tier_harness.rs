//! Privileged real-XFS and real-FUSE capacity-exhaustion harness.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::Duration;

use crate::sigkill_harness::{
    MountedDaemon, SigkillHarnessError, canonical_file, create_new_run_root,
};

const MEBIBYTE: u64 = 1_024 * 1_024;
const METADATA_IMAGE_BYTES_V1: u64 = 512 * MEBIBYTE;
const DATA_IMAGE_BYTES_V1: u64 = 320 * MEBIBYTE;
const SMALL_FILE_QUOTA_BYTES_V1: u64 = 64 * MEBIBYTE;
const WRITE_BYTES: usize = 1_024 * 1_024;
const RECLAIM_BYTES: u64 = 32 * MEBIBYTE;
const REQUIRED_CONSECUTIVE_ENOSPC: usize = 3;
const ENOSPC_RETRY_INTERVAL: Duration = Duration::from_secs(7);
const MINIMUM_GROWTH_WRITE_BYTES: usize = 256 * 1_024;
const MINIMUM_GROWTH_CLAIM_BYTES: u64 = 2 * 256 * 1_024 + 4 * 1_024;

/// Configuration for one destructive-to-scratch, privileged ENOSPC proof.
#[derive(Clone, Debug)]
pub struct FullTierEnospcConfig {
    daemon: PathBuf,
    maintenance: PathBuf,
    run_root: PathBuf,
}

impl FullTierEnospcConfig {
    #[must_use]
    pub fn v1(daemon: PathBuf, maintenance: PathBuf, run_root: PathBuf) -> Self {
        Self {
            daemon,
            maintenance,
            run_root,
        }
    }

    /// Creates two private XFS loop filesystems, fills DATA through FUSE until
    /// `ENOSPC`, proves cleanup and reads still work, scrubs offline, and then
    /// remounts the same pools to verify every acknowledged byte.
    ///
    /// The run root must not exist. Images and logs are retained on success or
    /// failure; only the exact mountpoints created by this invocation are
    /// unmounted automatically.
    ///
    /// # Errors
    ///
    /// Returns setup, process, mount, capacity-oracle, scrub, cleanup, or
    /// byte-verification failures while retaining the run root for diagnosis.
    ///
    /// # Panics
    ///
    /// Panics only if a compile-time bounded byte or file count does not fit
    /// the host's integer widths.
    #[allow(clippy::too_many_lines)]
    pub fn run(self) -> Result<FullTierEnospcReport, FullTierEnospcError> {
        let daemon = canonical_file(&self.daemon)?;
        let maintenance = canonical_file(&self.maintenance)?;
        let run_root = create_new_run_root(&self.run_root)?;
        let setup_log = run_root.join("xfs-setup.log");
        let mut metadata = XfsLoopTier::create(
            &run_root,
            "metadata",
            METADATA_IMAGE_BYTES_V1,
            true,
            &setup_log,
        )?;
        let mut data =
            XfsLoopTier::create(&run_root, "data", DATA_IMAGE_BYTES_V1, false, &setup_log)?;
        let fuse_mount = run_root.join("fuse");
        std::fs::create_dir(&fuse_mount)?;
        let quota = SMALL_FILE_QUOTA_BYTES_V1.to_string();
        let mut process = MountedDaemon::start_with_environment(
            &daemon,
            &fuse_mount,
            metadata.mount(),
            data.mount(),
            &run_root.join("ingest-daemon.log"),
            false,
            &[
                ("FASTDUP_SMALL_FILE_QUOTA_BYTES", quota.as_str()),
                ("FASTDUP_SMALL_FILE_PROJECT_ID", "17988"),
                ("FASTDUP_ONLINE_GC", "off"),
            ],
        )?;

        let presented_available_before = available_bytes(&fuse_mount)?;
        let reclaim_path = fuse_mount.join("reclaim.bin");
        let stream_path = fuse_mount.join("enospc-stream.bin");
        write_deterministic_file(&reclaim_path, RECLAIM_BYTES, 0x41)?;
        let stream = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&stream_path)?;
        let accepted_bytes = fill_until_enospc(&stream, DATA_IMAGE_BYTES_V1 * 2)?;
        let presented_available_at_enospc = available_bytes(&fuse_mount)?;
        if accepted_bytes == 0
            || presented_available_before == 0
            || presented_available_at_enospc > MINIMUM_GROWTH_CLAIM_BYTES
        {
            return Err(FullTierEnospcError::InvalidCapacityObservation {
                before: presented_available_before,
                exhausted: presented_available_at_enospc,
            });
        }
        assert_tail_matches(&stream, accepted_bytes, 0x93)?;
        drop(stream);

        std::fs::remove_file(&reclaim_path)?;
        if reclaim_path.exists() {
            return Err(FullTierEnospcError::CleanupDidNotComplete);
        }
        assert_file_tail_matches(&stream_path, accepted_bytes, 0x93)?;
        let small_file = fill_small_file_quota(&fuse_mount)?;
        let small_file_allocated_bytes =
            allocated_bytes_in_directory(&metadata.mount().join(".fastdup-small-file-containers"))?;
        if !(SMALL_FILE_QUOTA_BYTES_V1 / 2..=SMALL_FILE_QUOTA_BYTES_V1)
            .contains(&small_file_allocated_bytes)
        {
            return Err(FullTierEnospcError::SmallFileQuotaNotExercised {
                allocated: small_file_allocated_bytes,
                hard_limit: SMALL_FILE_QUOTA_BYTES_V1,
            });
        }
        process.stop_gracefully()?;

        let scrub_output = Command::new(&maintenance)
            .arg("--offline")
            .arg("scrub")
            .arg(metadata.mount())
            .arg(data.mount())
            .output()?;
        append_output(&run_root.join("offline-scrub.log"), &scrub_output)?;
        if !scrub_output.status.success()
            || !scrub_output
                .stdout
                .windows(b"scrub_ok=true".len())
                .any(|window| window == b"scrub_ok=true")
        {
            return Err(FullTierEnospcError::OfflineScrubFailed(scrub_output.status));
        }

        let mut recovery = MountedDaemon::start_with_environment(
            &daemon,
            &fuse_mount,
            metadata.mount(),
            data.mount(),
            &run_root.join("recovery-daemon.log"),
            false,
            &[
                ("FASTDUP_SMALL_FILE_QUOTA_BYTES", quota.as_str()),
                ("FASTDUP_SMALL_FILE_PROJECT_ID", "17988"),
                ("FASTDUP_ONLINE_GC", "off"),
            ],
        )?;
        verify_deterministic_file(&stream_path, accepted_bytes, 0x93)?;
        if reclaim_path.exists() {
            return Err(FullTierEnospcError::DeletedFileRecovered);
        }
        verify_deterministic_file(
            &fuse_mount.join(&small_file.probe_name),
            u64::try_from(MINIMUM_GROWTH_WRITE_BYTES).expect("bounded write fits u64"),
            small_file.probe_salt,
        )?;
        if fuse_mount.join(&small_file.removed_name).exists() {
            return Err(FullTierEnospcError::DeletedFileRecovered);
        }
        recovery.sigkill_and_detach()?;

        data.unmount()?;
        metadata.unmount()?;
        Ok(FullTierEnospcReport {
            run_root,
            accepted_bytes,
            presented_available_before,
            presented_available_at_enospc,
            rejected_writes: REQUIRED_CONSECUTIVE_ENOSPC,
            small_file_bytes: small_file.accepted_bytes,
            small_file_allocated_bytes,
        })
    }
}

/// Evidence retained after a successful real-tier exhaustion run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullTierEnospcReport {
    run_root: PathBuf,
    accepted_bytes: u64,
    presented_available_before: u64,
    presented_available_at_enospc: u64,
    rejected_writes: usize,
    small_file_bytes: u64,
    small_file_allocated_bytes: u64,
}

impl FullTierEnospcReport {
    #[must_use]
    pub fn run_root(&self) -> &Path {
        &self.run_root
    }

    #[must_use]
    pub const fn accepted_bytes(&self) -> u64 {
        self.accepted_bytes
    }

    #[must_use]
    pub const fn presented_available_before(&self) -> u64 {
        self.presented_available_before
    }

    #[must_use]
    pub const fn presented_available_at_enospc(&self) -> u64 {
        self.presented_available_at_enospc
    }

    #[must_use]
    pub const fn rejected_writes(&self) -> usize {
        self.rejected_writes
    }

    #[must_use]
    pub const fn small_file_bytes(&self) -> u64 {
        self.small_file_bytes
    }

    #[must_use]
    pub const fn small_file_allocated_bytes(&self) -> u64 {
        self.small_file_allocated_bytes
    }
}

struct SmallFileQuotaEvidence {
    accepted_bytes: u64,
    probe_name: String,
    probe_salt: u8,
    removed_name: String,
}

fn fill_small_file_quota(mount: &Path) -> Result<SmallFileQuotaEvidence, FullTierEnospcError> {
    const MAXIMUM_FILES: usize = 512;
    let length = MINIMUM_GROWTH_WRITE_BYTES;
    let length_u64 = u64::try_from(length).expect("bounded Small-File write fits u64");
    let mut accepted = Vec::new();
    let mut ordinal = 0_usize;
    let mut consecutive_enospc = 0_usize;
    let mut current: Option<(String, u8, File)> = None;
    while ordinal < MAXIMUM_FILES && consecutive_enospc < REQUIRED_CONSECUTIVE_ENOSPC {
        if current.is_none() {
            let name = format!("quota-{ordinal:04}.json");
            let salt = u8::try_from(ordinal % 251).expect("bounded salt fits u8") ^ 0xc7;
            match OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(mount.join(&name))
            {
                Ok(file) => current = Some((name, salt, file)),
                Err(error) if is_enospc(&error) => {
                    consecutive_enospc += 1;
                    if consecutive_enospc < REQUIRED_CONSECUTIVE_ENOSPC {
                        thread::sleep(ENOSPC_RETRY_INTERVAL);
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let (_, salt, file) = current
            .as_ref()
            .expect("ASSERT: Small-File fixture is open before its write");
        let bytes = deterministic_bytes(0, length, *salt);
        match file.write_at(&bytes, 0) {
            Ok(written) if written == length => {
                let (name, salt, file) = current
                    .take()
                    .expect("ASSERT: acknowledged Small-File remains open");
                file.sync_all()?;
                accepted.push((name, salt));
                ordinal += 1;
                consecutive_enospc = 0;
            }
            Ok(written) => {
                return Err(FullTierEnospcError::ShortWrite {
                    expected: length,
                    observed: written,
                });
            }
            Err(error) if is_enospc(&error) => {
                consecutive_enospc += 1;
                if file.metadata()?.len() != 0 {
                    return Err(FullTierEnospcError::RejectedWriteBecameVisible);
                }
                if consecutive_enospc < REQUIRED_CONSECUTIVE_ENOSPC {
                    thread::sleep(ENOSPC_RETRY_INTERVAL);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    if consecutive_enospc != REQUIRED_CONSECUTIVE_ENOSPC || accepted.len() < 2 {
        return Err(FullTierEnospcError::SmallFileQuotaDidNotFill);
    }
    if let Some((name, _, file)) = current.take() {
        drop(file);
        std::fs::remove_file(mount.join(name))?;
    }
    let (removed_name, _) = accepted
        .pop()
        .expect("at least two Small Files were accepted");
    std::fs::remove_file(mount.join(&removed_name))?;
    let (probe_name, probe_salt) = accepted[0].clone();
    Ok(SmallFileQuotaEvidence {
        accepted_bytes: u64::try_from(accepted.len())
            .expect("bounded file count fits u64")
            .checked_mul(length_u64)
            .expect("bounded Small-File bytes fit u64"),
        probe_name,
        probe_salt,
        removed_name,
    })
}

fn fill_until_enospc(file: &File, maximum_bytes: u64) -> Result<u64, FullTierEnospcError> {
    let mut accepted = 0_u64;
    let mut consecutive_enospc = 0_usize;
    let mut write_bytes = WRITE_BYTES;
    while accepted < maximum_bytes && consecutive_enospc < REQUIRED_CONSECUTIVE_ENOSPC {
        let remaining = maximum_bytes - accepted;
        let length = usize::try_from(remaining.min(write_bytes as u64))
            .expect("ASSERT: bounded write length fits usize");
        let bytes = deterministic_bytes(accepted, length, 0x93);
        match file.write_at(&bytes, accepted) {
            Ok(0) => return Err(FullTierEnospcError::ZeroLengthWrite),
            Ok(written) => {
                accepted = accepted
                    .checked_add(u64::try_from(written).expect("write length fits u64"))
                    .ok_or(FullTierEnospcError::InvalidConfiguration)?;
                consecutive_enospc = 0;
            }
            Err(error) if is_enospc(&error) => {
                consecutive_enospc += 1;
                if file.metadata()?.len() != accepted {
                    return Err(FullTierEnospcError::RejectedWriteBecameVisible);
                }
                if consecutive_enospc == REQUIRED_CONSECUTIVE_ENOSPC
                    && write_bytes > MINIMUM_GROWTH_WRITE_BYTES
                {
                    write_bytes = MINIMUM_GROWTH_WRITE_BYTES;
                    consecutive_enospc = 0;
                } else if consecutive_enospc < REQUIRED_CONSECUTIVE_ENOSPC {
                    thread::sleep(ENOSPC_RETRY_INTERVAL);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    if consecutive_enospc != REQUIRED_CONSECUTIVE_ENOSPC {
        return Err(FullTierEnospcError::NoEnospcBeforeLimit(maximum_bytes));
    }
    Ok(accepted)
}

fn is_enospc(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::StorageFull || error.raw_os_error() == Some(28)
}

fn write_deterministic_file(path: &Path, length: u64, salt: u8) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let mut offset = 0_u64;
    while offset < length {
        let count = usize::try_from((length - offset).min(WRITE_BYTES as u64))
            .expect("ASSERT: bounded fixture write fits usize");
        file.write_all(&deterministic_bytes(offset, count, salt))?;
        offset += u64::try_from(count).expect("bounded fixture write fits u64");
    }
    file.sync_all()
}

fn verify_deterministic_file(
    path: &Path,
    length: u64,
    salt: u8,
) -> Result<(), FullTierEnospcError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != length {
        return Err(FullTierEnospcError::RecoveredLengthMismatch {
            expected: length,
            observed: file.metadata()?.len(),
        });
    }
    let mut offset = 0_u64;
    let mut observed = vec![0_u8; WRITE_BYTES];
    while offset < length {
        let count = usize::try_from((length - offset).min(WRITE_BYTES as u64))
            .expect("ASSERT: bounded verification read fits usize");
        file.read_exact(&mut observed[..count])?;
        if observed[..count] != deterministic_bytes(offset, count, salt) {
            return Err(FullTierEnospcError::RecoveredBytesMismatch { offset });
        }
        offset += u64::try_from(count).expect("bounded verification read fits u64");
    }
    Ok(())
}

fn assert_file_tail_matches(path: &Path, length: u64, salt: u8) -> Result<(), FullTierEnospcError> {
    let file = File::open(path)?;
    assert_tail_matches(&file, length, salt)
}

fn assert_tail_matches(file: &File, length: u64, salt: u8) -> Result<(), FullTierEnospcError> {
    let count = usize::try_from(length.min(4 * 1_024)).expect("bounded tail fits usize");
    let offset = length - u64::try_from(count).expect("bounded tail fits u64");
    let mut observed = vec![0_u8; count];
    file.read_exact_at(&mut observed, offset)?;
    if observed != deterministic_bytes(offset, count, salt) {
        return Err(FullTierEnospcError::LiveTailMismatch);
    }
    Ok(())
}

fn deterministic_bytes(offset: u64, length: usize, salt: u8) -> Vec<u8> {
    let mut bytes = vec![0_u8; length];
    let mut cursor = 0_usize;
    while cursor < length {
        let absolute = offset + u64::try_from(cursor).expect("bounded buffer offset fits u64");
        let word_offset = absolute / 8;
        let mut value = word_offset ^ (u64::from(salt) << 56) ^ 0xA076_1D64_78BD_642F;
        value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let word = (value ^ (value >> 31)).to_le_bytes();
        let start = usize::try_from(absolute % 8).expect("word byte offset fits usize");
        let count = (8 - start).min(length - cursor);
        bytes[cursor..cursor + count].copy_from_slice(&word[start..start + count]);
        cursor += count;
    }
    bytes
}

fn available_bytes(path: &Path) -> io::Result<u64> {
    let statistics = rustix::fs::statvfs(path).map_err(io::Error::from)?;
    statistics
        .f_bavail
        .checked_mul(statistics.f_frsize.max(1))
        .ok_or_else(|| io::Error::other("statvfs availability overflows u64"))
}

fn allocated_bytes_in_directory(path: &Path) -> io::Result<u64> {
    let mut allocated = path
        .metadata()?
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| io::Error::other("allocated directory bytes overflow u64"))?;
    for entry in std::fs::read_dir(path)? {
        allocated = allocated
            .checked_add(
                entry?
                    .metadata()?
                    .blocks()
                    .checked_mul(512)
                    .ok_or_else(|| io::Error::other("allocated file bytes overflow u64"))?,
            )
            .ok_or_else(|| io::Error::other("allocated tier bytes overflow u64"))?;
    }
    Ok(allocated)
}

struct XfsLoopTier {
    mount: PathBuf,
    mounted: bool,
}

impl XfsLoopTier {
    fn create(
        run_root: &Path,
        name: &str,
        image_bytes: u64,
        project_quota: bool,
        setup_log: &Path,
    ) -> Result<Self, FullTierEnospcError> {
        let image = run_root.join(format!("{name}.xfs"));
        let mount = run_root.join(format!("{name}-mnt"));
        std::fs::create_dir(&mount)?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&image)?;
        file.set_len(image_bytes)?;
        drop(file);
        run_logged(Command::new("mkfs.xfs").arg("-f").arg(&image), setup_log)?;
        let mut command = Command::new("mount");
        command.arg("-o");
        command.arg(if project_quota {
            "loop,prjquota"
        } else {
            "loop"
        });
        command.arg(&image).arg(&mount);
        run_logged(&mut command, setup_log)?;
        Ok(Self {
            mount,
            mounted: true,
        })
    }

    fn mount(&self) -> &Path {
        &self.mount
    }

    fn unmount(&mut self) -> Result<(), FullTierEnospcError> {
        if !self.mounted {
            return Ok(());
        }
        let status = Command::new("umount").arg(&self.mount).status()?;
        if !status.success() {
            return Err(FullTierEnospcError::UnmountFailed {
                mount: self.mount.clone(),
                status,
            });
        }
        self.mounted = false;
        Ok(())
    }
}

impl Drop for XfsLoopTier {
    fn drop(&mut self) {
        if self.mounted {
            let _ = Command::new("umount")
                .arg(&self.mount)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn run_logged(command: &mut Command, log: &Path) -> Result<(), FullTierEnospcError> {
    let debug = format!("{command:?}");
    let output = command.output()?;
    let mut destination = OpenOptions::new().create(true).append(true).open(log)?;
    writeln!(destination, "command={debug} status={}", output.status)?;
    destination.write_all(&output.stdout)?;
    destination.write_all(&output.stderr)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(FullTierEnospcError::CommandFailed {
            command: debug,
            status: output.status,
        })
    }
}

fn append_output(path: &Path, output: &Output) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    writeln!(file, "status={}", output.status)?;
    file.write_all(&output.stdout)?;
    file.write_all(&output.stderr)
}

#[derive(Debug)]
pub enum FullTierEnospcError {
    Io(io::Error),
    Process(SigkillHarnessError),
    InvalidConfiguration,
    CommandFailed { command: String, status: ExitStatus },
    UnmountFailed { mount: PathBuf, status: ExitStatus },
    ZeroLengthWrite,
    NoEnospcBeforeLimit(u64),
    RejectedWriteBecameVisible,
    ShortWrite { expected: usize, observed: usize },
    InvalidCapacityObservation { before: u64, exhausted: u64 },
    LiveTailMismatch,
    CleanupDidNotComplete,
    OfflineScrubFailed(ExitStatus),
    RecoveredLengthMismatch { expected: u64, observed: u64 },
    RecoveredBytesMismatch { offset: u64 },
    DeletedFileRecovered,
    SmallFileQuotaDidNotFill,
    SmallFileQuotaNotExercised { allocated: u64, hard_limit: u64 },
}

impl fmt::Display for FullTierEnospcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for FullTierEnospcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Process(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for FullTierEnospcError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SigkillHarnessError> for FullTierEnospcError {
    fn from(error: SigkillHarnessError) -> Self {
        Self::Process(error)
    }
}
