use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek as _, Write as _};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::{FlockOperation, flock};

pub const APPLIANCE_LEASE_FILE_NAME: &str = ".fastdup-appliance.lease";

/// The repository owner recorded for operator diagnosis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplianceLeaseOwner {
    WritableDaemon,
    OfflineMaintenance,
}

impl ApplianceLeaseOwner {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WritableDaemon => "writable-daemon",
            Self::OfflineMaintenance => "offline-maintenance",
        }
    }
}

impl fmt::Display for ApplianceLeaseOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Exclusive cross-process ownership of one Metadata root.
///
/// The persistent file is diagnostic. The kernel `flock` held by this value is
/// the authority, so process exit and `SIGKILL` release ownership without PID
/// reuse or stale-file heuristics.
#[derive(Debug)]
pub struct ApplianceLease {
    _file: File,
    owner: ApplianceLeaseOwner,
    path: PathBuf,
}

impl ApplianceLease {
    /// Acquires exclusive repository ownership without waiting.
    ///
    /// # Errors
    ///
    /// Returns `WouldBlock` when another process owns the repository, or a
    /// filesystem error while opening, locking, recording, or synchronizing
    /// the lease.
    pub fn acquire(
        metadata_root: impl AsRef<Path>,
        owner: ApplianceLeaseOwner,
    ) -> io::Result<Self> {
        let metadata_root = metadata_root.as_ref();
        let path = metadata_root.join(APPLIANCE_LEASE_FILE_NAME);
        let nofollow = i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits())
            .map_err(|_| io::Error::other("O_NOFOLLOW does not fit OpenOptions custom flags"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(nofollow)
            .open(&path)?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Appliance Lease is not a regular file: {}", path.display()),
            ));
        }
        if let Err(error) = flock(&file, FlockOperation::NonBlockingLockExclusive) {
            if matches!(error, rustix::io::Errno::AGAIN | rustix::io::Errno::ACCESS) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Appliance Lease is already held for Metadata root {}",
                        metadata_root.display()
                    ),
                ));
            }
            return Err(io::Error::from(error));
        }

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        file.set_len(0)?;
        file.rewind()?;
        writeln!(
            file,
            "fastdup-appliance-lease-v1\nowner={}\npid={}",
            owner,
            std::process::id()
        )?;
        file.sync_all()?;
        File::open(metadata_root)?.sync_all()?;

        Ok(Self {
            _file: file,
            owner,
            path,
        })
    }

    #[must_use]
    pub const fn owner(&self) -> ApplianceLeaseOwner {
        self.owner
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
