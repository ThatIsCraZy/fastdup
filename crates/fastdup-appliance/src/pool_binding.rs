use std::fmt;
use std::fs::File;
use std::io::{self, Read as _};

use fastdup_format::{
    ApplianceId, POOL_IDENTITY_RECORD_BYTES, PoolId, PoolIdentityFormatError, PoolIdentityRecord,
    PoolRole,
};
use fastdup_store::{FsStorageIo, StorageIo};

pub const POOL_IDENTITY_FILE_NAME: &str = ".fastdup-pool.identity";
const POOL_IDENTITY_TEMP_FILE_NAME: &str = ".fastdup-pool.identity.tmp";

/// Verified durable binding of the Metadata and Data Pools to one Appliance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppliancePoolBinding {
    metadata: PoolIdentityRecord,
    data: PoolIdentityRecord,
}

impl AppliancePoolBinding {
    /// Audits an existing Pool pair without changing either root.
    ///
    /// # Errors
    ///
    /// Returns missing, malformed, role-swapped, duplicate, or cross-Appliance
    /// identity errors, plus storage failures.
    pub fn audit<M: StorageIo, D: StorageIo>(
        metadata: &M,
        data: &D,
    ) -> Result<Self, AppliancePoolBindingError> {
        let metadata = read_required(metadata, PoolRole::Metadata)?;
        let data = read_required(data, PoolRole::Data)?;
        Self::validate(metadata, data)
    }

    /// Opens an existing pair or durably initializes bootstrap-only roots.
    ///
    /// A partial first initialization is completed from the already durable
    /// Appliance ID. A missing identity beside ordinary repository objects is
    /// rejected rather than migrated.
    ///
    /// # Errors
    ///
    /// Returns storage, entropy, durable-format, bootstrap-content, ownership,
    /// duplicate-ID, or fixed-role errors.
    pub fn initialize_or_open<M: StorageIo, D: StorageIo>(
        metadata_storage: &M,
        data_storage: &D,
    ) -> Result<Self, AppliancePoolBindingError> {
        let metadata = read_optional(metadata_storage, PoolRole::Metadata)?;
        let data = read_optional(data_storage, PoolRole::Data)?;
        match (metadata, data) {
            (Some(metadata), Some(data)) => Self::validate(metadata, data),
            (None, None) => {
                require_bootstrap_only(metadata_storage, PoolRole::Metadata)?;
                require_bootstrap_only(data_storage, PoolRole::Data)?;
                let mut entropy = File::open("/dev/urandom")?;
                let appliance_id = random_appliance_id(&mut entropy)?;
                let metadata = PoolIdentityRecord::new(
                    appliance_id,
                    random_pool_id(&mut entropy, None)?,
                    PoolRole::Metadata,
                );
                let data = PoolIdentityRecord::new(
                    appliance_id,
                    random_pool_id(&mut entropy, Some(metadata.pool_id()))?,
                    PoolRole::Data,
                );
                publish_identity(metadata_storage, metadata)?;
                publish_identity(data_storage, data)?;
                Self::validate(metadata, data)
            }
            (Some(metadata), None) => {
                validate_role(metadata, PoolRole::Metadata)?;
                require_bootstrap_only(data_storage, PoolRole::Data)?;
                let mut entropy = File::open("/dev/urandom")?;
                let data = PoolIdentityRecord::new(
                    metadata.appliance_id(),
                    random_pool_id(&mut entropy, Some(metadata.pool_id()))?,
                    PoolRole::Data,
                );
                publish_identity(data_storage, data)?;
                Self::validate(metadata, data)
            }
            (None, Some(data)) => {
                validate_role(data, PoolRole::Data)?;
                require_bootstrap_only(metadata_storage, PoolRole::Metadata)?;
                let mut entropy = File::open("/dev/urandom")?;
                let metadata = PoolIdentityRecord::new(
                    data.appliance_id(),
                    random_pool_id(&mut entropy, Some(data.pool_id()))?,
                    PoolRole::Metadata,
                );
                publish_identity(metadata_storage, metadata)?;
                Self::validate(metadata, data)
            }
        }
    }

    fn validate(
        metadata: PoolIdentityRecord,
        data: PoolIdentityRecord,
    ) -> Result<Self, AppliancePoolBindingError> {
        validate_role(metadata, PoolRole::Metadata)?;
        validate_role(data, PoolRole::Data)?;
        if metadata.appliance_id() != data.appliance_id() {
            return Err(AppliancePoolBindingError::ApplianceIdMismatch);
        }
        if metadata.pool_id() == data.pool_id() {
            return Err(AppliancePoolBindingError::DuplicatePoolId);
        }
        Ok(Self { metadata, data })
    }

    #[must_use]
    pub const fn metadata(self) -> PoolIdentityRecord {
        self.metadata
    }

    #[must_use]
    pub const fn data(self) -> PoolIdentityRecord {
        self.data
    }

    /// Audits filesystem-backed identities without following a non-regular
    /// canonical identity entry.
    ///
    /// # Errors
    ///
    /// Returns malformed object types plus all errors from [`Self::audit`].
    pub fn audit_filesystem(
        metadata: &FsStorageIo,
        data: &FsStorageIo,
    ) -> Result<Self, AppliancePoolBindingError> {
        validate_filesystem_identity_type(metadata, PoolRole::Metadata)?;
        validate_filesystem_identity_type(data, PoolRole::Data)?;
        let binding = Self::audit(metadata, data)?;
        validate_filesystem_identity_type(metadata, PoolRole::Metadata)?;
        validate_filesystem_identity_type(data, PoolRole::Data)?;
        Ok(binding)
    }

    /// Initializes or opens filesystem-backed identities while rejecting
    /// non-regular canonical identity entries.
    ///
    /// # Errors
    ///
    /// Returns malformed object types plus all errors from
    /// [`Self::initialize_or_open`].
    pub fn initialize_or_open_filesystem(
        metadata: &FsStorageIo,
        data: &FsStorageIo,
    ) -> Result<Self, AppliancePoolBindingError> {
        validate_filesystem_identity_type(metadata, PoolRole::Metadata)?;
        validate_filesystem_identity_type(data, PoolRole::Data)?;
        let binding = Self::initialize_or_open(metadata, data)?;
        validate_filesystem_identity_type(metadata, PoolRole::Metadata)?;
        validate_filesystem_identity_type(data, PoolRole::Data)?;
        Ok(binding)
    }
}

fn validate_filesystem_identity_type(
    storage: &FsStorageIo,
    role: PoolRole,
) -> Result<(), AppliancePoolBindingError> {
    let path = storage.root().join(POOL_IDENTITY_FILE_NAME);
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(AppliancePoolBindingError::NonRegularIdentity { role }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn read_required<S: StorageIo>(
    storage: &S,
    expected_role: PoolRole,
) -> Result<PoolIdentityRecord, AppliancePoolBindingError> {
    read_optional(storage, expected_role)?.ok_or(AppliancePoolBindingError::MissingIdentity {
        role: expected_role,
    })
}

fn read_optional<S: StorageIo>(
    storage: &S,
    expected_role: PoolRole,
) -> Result<Option<PoolIdentityRecord>, AppliancePoolBindingError> {
    if !storage.exists(POOL_IDENTITY_FILE_NAME)? {
        return Ok(None);
    }
    let length = storage.object_len(POOL_IDENTITY_FILE_NAME)?;
    let expected_length = u64::try_from(POOL_IDENTITY_RECORD_BYTES)
        .expect("ASSERT: fixed Pool identity length fits u64");
    if length != expected_length {
        return Err(AppliancePoolBindingError::InvalidIdentityLength {
            role: expected_role,
            length,
        });
    }
    let bytes = storage.read_exact_at(POOL_IDENTITY_FILE_NAME, 0, POOL_IDENTITY_RECORD_BYTES)?;
    PoolIdentityRecord::decode(&bytes)
        .map(Some)
        .map_err(|source| AppliancePoolBindingError::Format {
            role: expected_role,
            source,
        })
}

fn require_bootstrap_only<S: StorageIo>(
    storage: &S,
    role: PoolRole,
) -> Result<(), AppliancePoolBindingError> {
    for name in storage.list_names()? {
        let allowed = name == POOL_IDENTITY_TEMP_FILE_NAME
            || (role == PoolRole::Metadata
                && matches!(
                    name.as_str(),
                    crate::APPLIANCE_LEASE_FILE_NAME | crate::APPLIANCE_RECOVERY_LATCH_FILE_NAME
                ));
        if !allowed {
            return Err(AppliancePoolBindingError::MissingIdentityInPopulatedPool {
                role,
                first_object: name,
            });
        }
    }
    Ok(())
}

fn publish_identity<S: StorageIo>(
    storage: &S,
    identity: PoolIdentityRecord,
) -> Result<(), AppliancePoolBindingError> {
    if storage.exists(POOL_IDENTITY_TEMP_FILE_NAME)? {
        storage.remove_file(POOL_IDENTITY_TEMP_FILE_NAME)?;
        storage.sync_root()?;
    }
    storage.create_new(POOL_IDENTITY_TEMP_FILE_NAME)?;
    let bytes = identity.encode();
    storage.write_at(POOL_IDENTITY_TEMP_FILE_NAME, 0, &bytes)?;
    storage.set_len(
        POOL_IDENTITY_TEMP_FILE_NAME,
        u64::try_from(POOL_IDENTITY_RECORD_BYTES)
            .expect("ASSERT: fixed Pool identity length fits u64"),
    )?;
    storage.sync_file(POOL_IDENTITY_TEMP_FILE_NAME)?;
    storage.publish_noreplace(POOL_IDENTITY_TEMP_FILE_NAME, POOL_IDENTITY_FILE_NAME)?;
    storage.sync_root()?;
    let published = read_required(storage, identity.role())?;
    if published != identity {
        return Err(AppliancePoolBindingError::PublicationMismatch {
            role: identity.role(),
        });
    }
    Ok(())
}

fn validate_role(
    identity: PoolIdentityRecord,
    expected: PoolRole,
) -> Result<(), AppliancePoolBindingError> {
    if identity.role() != expected {
        return Err(AppliancePoolBindingError::RoleMismatch {
            expected,
            actual: identity.role(),
        });
    }
    Ok(())
}

fn random_appliance_id(random: &mut File) -> Result<ApplianceId, AppliancePoolBindingError> {
    loop {
        let mut bytes = [0_u8; 16];
        random.read_exact(&mut bytes)?;
        if let Some(id) = ApplianceId::new(bytes) {
            return Ok(id);
        }
    }
}

fn random_pool_id(
    random: &mut File,
    excluded: Option<PoolId>,
) -> Result<PoolId, AppliancePoolBindingError> {
    loop {
        let mut bytes = [0_u8; 16];
        random.read_exact(&mut bytes)?;
        if let Some(id) = PoolId::new(bytes)
            && Some(id) != excluded
        {
            return Ok(id);
        }
    }
}

#[derive(Debug)]
pub enum AppliancePoolBindingError {
    Io(io::Error),
    Format {
        role: PoolRole,
        source: PoolIdentityFormatError,
    },
    MissingIdentity {
        role: PoolRole,
    },
    InvalidIdentityLength {
        role: PoolRole,
        length: u64,
    },
    MissingIdentityInPopulatedPool {
        role: PoolRole,
        first_object: String,
    },
    NonRegularIdentity {
        role: PoolRole,
    },
    RoleMismatch {
        expected: PoolRole,
        actual: PoolRole,
    },
    ApplianceIdMismatch,
    DuplicatePoolId,
    PublicationMismatch {
        role: PoolRole,
    },
}

impl fmt::Display for AppliancePoolBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for AppliancePoolBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Format { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for AppliancePoolBindingError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
