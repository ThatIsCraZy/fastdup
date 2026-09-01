use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use crate::{COMMIT_METADATA_FLOOR_BYTES_V1, PoolIsolationPolicy};

pub const SMALL_FILE_QUOTA_BYTES_ENV: &str = "FASTDUP_SMALL_FILE_QUOTA_BYTES";
pub const SMALL_FILE_PROJECT_ID_ENV: &str = "FASTDUP_SMALL_FILE_PROJECT_ID";
const SMALL_FILE_DIRECTORY: &str = ".fastdup-small-file-containers";
const DEFAULT_SMALL_FILE_QUOTA_BYTES: u64 = 64 * 1_024 * 1_024 * 1_024;
const DEFAULT_SMALL_FILE_PROJECT_ID: u32 = 17_988;

/// Physical isolation established for policy-selected Small-File Containers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallFileTierIsolation {
    root: PathBuf,
    project_id: u32,
    hard_limit_bytes: u64,
    enforced: bool,
}

#[must_use]
pub fn small_file_container_root(metadata_root: &Path) -> PathBuf {
    metadata_root.join(SMALL_FILE_DIRECTORY)
}

impl SmallFileTierIsolation {
    /// Creates the dedicated Container directory and, in production mode,
    /// assigns and limits one inheriting XFS project.
    ///
    /// Provisioning is intentionally delegated to the standard XFS tools. It
    /// introduces no unsafe syscall wrapper and fails closed if either project
    /// assignment or hard-limit installation is unavailable.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration, unsafe root, insufficient Metadata
    /// reserve, filesystem I/O, or XFS quota-command failures.
    pub fn prepare(
        metadata_root: &Path,
        policy: PoolIsolationPolicy,
    ) -> Result<Self, SmallFileTierIsolationError> {
        let hard_limit_bytes =
            parse_u64_environment(SMALL_FILE_QUOTA_BYTES_ENV, DEFAULT_SMALL_FILE_QUOTA_BYTES)?;
        let project_id_u64 = parse_u64_environment(
            SMALL_FILE_PROJECT_ID_ENV,
            u64::from(DEFAULT_SMALL_FILE_PROJECT_ID),
        )?;
        let project_id = u32::try_from(project_id_u64)
            .ok()
            .filter(|value| *value != 0)
            .ok_or(SmallFileTierIsolationError::InvalidConfiguration)?;
        if hard_limit_bytes == 0 || hard_limit_bytes % 1_024 != 0 {
            return Err(SmallFileTierIsolationError::InvalidConfiguration);
        }

        let root = small_file_container_root(metadata_root);
        std::fs::create_dir_all(&root)?;
        let root_metadata = std::fs::symlink_metadata(&root)?;
        if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
            return Err(SmallFileTierIsolationError::InvalidRoot(root));
        }
        if policy == PoolIsolationPolicy::LabAllowShared {
            return Ok(Self {
                root,
                project_id,
                hard_limit_bytes,
                enforced: false,
            });
        }

        let statistics = rustix::fs::statvfs(metadata_root).map_err(io::Error::from)?;
        let capacity_bytes = statistics
            .f_blocks
            .checked_mul(statistics.f_frsize.max(1))
            .ok_or(SmallFileTierIsolationError::InvalidConfiguration)?;
        let maximum_noncritical = capacity_bytes
            .checked_sub(COMMIT_METADATA_FLOOR_BYTES_V1)
            .ok_or(SmallFileTierIsolationError::MetadataReserveAtRisk)?;
        if hard_limit_bytes > maximum_noncritical {
            return Err(SmallFileTierIsolationError::MetadataReserveAtRisk);
        }

        let project_command = format!("chproj -R {project_id}");
        run(
            "/usr/sbin/xfs_io",
            &["-c", &project_command, "-c", "chattr +P"],
            &root,
        )?;
        let hard_limit_kib = hard_limit_bytes / 1_024;
        let limit_command = format!("limit -p bhard={hard_limit_kib}k {project_id}");
        run(
            "/usr/sbin/xfs_quota",
            &["-x", "-c", &limit_command],
            metadata_root,
        )?;
        let query_command = format!("quota -p -bnNv {project_id}");
        run(
            "/usr/sbin/xfs_quota",
            &["-x", "-c", &query_command],
            metadata_root,
        )?;
        Ok(Self {
            root,
            project_id,
            hard_limit_bytes,
            enforced: true,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub const fn project_id(&self) -> u32 {
        self.project_id
    }

    #[must_use]
    pub const fn hard_limit_bytes(&self) -> u64 {
        self.hard_limit_bytes
    }

    #[must_use]
    pub const fn enforced(&self) -> bool {
        self.enforced
    }
}

fn parse_u64_environment(
    name: &'static str,
    default: u64,
) -> Result<u64, SmallFileTierIsolationError> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| SmallFileTierIsolationError::InvalidEnvironment { name, value }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(SmallFileTierIsolationError::InvalidConfiguration),
    }
}

fn run(
    program: &'static str,
    arguments: &[&str],
    path: &Path,
) -> Result<(), SmallFileTierIsolationError> {
    let status = Command::new(program).args(arguments).arg(path).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(SmallFileTierIsolationError::CommandFailed { program, status })
    }
}

#[derive(Debug)]
pub enum SmallFileTierIsolationError {
    Io(io::Error),
    InvalidConfiguration,
    InvalidEnvironment {
        name: &'static str,
        value: String,
    },
    MetadataReserveAtRisk,
    InvalidRoot(PathBuf),
    CommandFailed {
        program: &'static str,
        status: ExitStatus,
    },
}

impl fmt::Display for SmallFileTierIsolationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "Small-File tier I/O failed: {error}"),
            Self::InvalidConfiguration => formatter.write_str(
                "Small-File project ID and quota must be nonzero; quota bytes must be KiB-aligned",
            ),
            Self::InvalidEnvironment { name, value } => {
                write!(
                    formatter,
                    "{name} must contain decimal ASCII bytes, got {value:?}"
                )
            }
            Self::MetadataReserveAtRisk => formatter.write_str(
                "Small-File hard quota would consume the protected Metadata commit reserve",
            ),
            Self::InvalidRoot(root) => write!(
                formatter,
                "Small-File Container root must be a real directory, not a symlink: {}",
                root.display()
            ),
            Self::CommandFailed { program, status } => {
                write!(
                    formatter,
                    "{program} failed while enforcing Small-File project quota: {status}"
                )
            }
        }
    }
}

impl std::error::Error for SmallFileTierIsolationError {}

impl From<io::Error> for SmallFileTierIsolationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
