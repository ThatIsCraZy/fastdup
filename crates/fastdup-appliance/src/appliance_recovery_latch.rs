use std::io;

use fastdup_store::{FsStorageIo, StorageIo};

pub const APPLIANCE_RECOVERY_LATCH_FILE_NAME: &str = ".fastdup-appliance.recovery-required";

/// Durable recovery requirement observed at the Metadata-root seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplianceRecoveryState {
    Clean,
    RecoveryRequired,
}

/// One armed appliance lifetime.
///
/// Dropping this value deliberately leaves the durable latch armed. Only an
/// explicit clean completion may remove it.
#[derive(Debug)]
pub struct ApplianceRecoveryLatch<I> {
    storage: I,
    prior_recovery_required: bool,
}

impl<I: StorageIo> ApplianceRecoveryLatch<I> {
    /// Audits the canonical latch without changing repository state.
    ///
    /// # Errors
    ///
    /// Returns storage errors or `InvalidData` when the canonical latch is not
    /// the required empty durable object.
    pub fn audit(storage: &I) -> io::Result<ApplianceRecoveryState> {
        if !storage.exists(APPLIANCE_RECOVERY_LATCH_FILE_NAME)? {
            return Ok(ApplianceRecoveryState::Clean);
        }
        let length = storage.object_len(APPLIANCE_RECOVERY_LATCH_FILE_NAME)?;
        let bytes = storage.read(APPLIANCE_RECOVERY_LATCH_FILE_NAME)?;
        if length != 0 || !bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Appliance Recovery Latch must be an empty canonical object",
            ));
        }
        Ok(ApplianceRecoveryState::RecoveryRequired)
    }

    /// Durably arms the recovery requirement before ordinary repository access.
    ///
    /// # Errors
    ///
    /// Returns corruption or storage errors while auditing, creating,
    /// verifying, or synchronizing the latch.
    pub fn arm(storage: I) -> io::Result<Self> {
        let prior_recovery_required =
            Self::audit(&storage)? == ApplianceRecoveryState::RecoveryRequired;
        if !prior_recovery_required {
            storage.create_new(APPLIANCE_RECOVERY_LATCH_FILE_NAME)?;
            if Self::audit(&storage)? != ApplianceRecoveryState::RecoveryRequired {
                return Err(io::Error::other(
                    "created Appliance Recovery Latch did not verify",
                ));
            }
            storage.sync_file(APPLIANCE_RECOVERY_LATCH_FILE_NAME)?;
            storage.sync_root()?;
        }
        Ok(Self {
            storage,
            prior_recovery_required,
        })
    }

    #[must_use]
    pub const fn prior_recovery_required(&self) -> bool {
        self.prior_recovery_required
    }

    /// Clears the latch after the caller completed verified recovery or Scrub.
    ///
    /// # Errors
    ///
    /// Returns corruption or storage errors while verifying, removing, or
    /// synchronizing the canonical latch.
    pub fn clear_after_verified_recovery(storage: &I) -> io::Result<()> {
        if Self::audit(storage)? != ApplianceRecoveryState::RecoveryRequired {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Appliance Recovery Latch is not armed",
            ));
        }
        storage.remove_file(APPLIANCE_RECOVERY_LATCH_FILE_NAME)?;
        storage.sync_root()
    }

    /// Removes and directory-synchronizes the latch after a proven clean end.
    ///
    /// # Errors
    ///
    /// Returns corruption or storage errors while verifying, removing, or
    /// synchronizing the canonical latch.
    pub fn mark_clean(self) -> io::Result<()> {
        Self::clear_after_verified_recovery(&self.storage)
    }
}

impl ApplianceRecoveryLatch<FsStorageIo> {
    /// Audits the filesystem latch without following a non-regular directory entry.
    ///
    /// # Errors
    ///
    /// Returns filesystem, storage, or malformed-latch errors.
    pub fn audit_filesystem(storage: &FsStorageIo) -> io::Result<ApplianceRecoveryState> {
        validate_filesystem_latch_type(storage)?;
        Self::audit(storage)
    }

    /// Durably arms a regular filesystem latch before repository access.
    ///
    /// # Errors
    ///
    /// Returns filesystem, storage, or malformed-latch errors.
    pub fn arm_filesystem(storage: FsStorageIo) -> io::Result<Self> {
        validate_filesystem_latch_type(&storage)?;
        Self::arm(storage)
    }

    /// Clears a verified regular filesystem latch after recovery or Scrub.
    ///
    /// # Errors
    ///
    /// Returns filesystem, storage, or malformed-latch errors.
    pub fn clear_filesystem_after_verified_recovery(storage: &FsStorageIo) -> io::Result<()> {
        validate_filesystem_latch_type(storage)?;
        Self::clear_after_verified_recovery(storage)
    }
}

fn validate_filesystem_latch_type(storage: &FsStorageIo) -> io::Result<()> {
    let path = storage.root().join(APPLIANCE_RECOVERY_LATCH_FILE_NAME);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Appliance Recovery Latch is not a regular file: {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
