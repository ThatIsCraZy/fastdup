use core::fmt;

use crate::crc32c_with_zeroed_u32;

pub const POOL_IDENTITY_RECORD_BYTES: usize = 4_096;
const MAGIC: &[u8; 8] = b"FDPOOL01";
const FORMAT_VERSION: u16 = 1;
const HEADER_BYTES: u16 = 64;
const RECORD_TYPE: u16 = 1;
const CRC32C_ALGORITHM: u16 = 1;
const CHECKSUM_OFFSET: usize = 56;

/// Persistent identity shared by every Pool in one Appliance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplianceId([u8; 16]);

impl ApplianceId {
    #[must_use]
    pub fn new(bytes: [u8; 16]) -> Option<Self> {
        if bytes == [0; 16] {
            None
        } else {
            Some(Self(bytes))
        }
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Persistent identity of one physical storage Pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolId([u8; 16]);

impl PoolId {
    #[must_use]
    pub fn new(bytes: [u8; 16]) -> Option<Self> {
        if bytes == [0; 16] {
            None
        } else {
            Some(Self(bytes))
        }
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Immutable purpose assigned to one Pool at initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum PoolRole {
    Metadata = 1,
    Data = 2,
}

impl PoolRole {
    fn decode(value: u16) -> Result<Self, PoolIdentityFormatError> {
        match value {
            1 => Ok(Self::Metadata),
            2 => Ok(Self::Data),
            _ => Err(PoolIdentityFormatError::UnsupportedRole(value)),
        }
    }
}

/// Complete durable identity of one Pool and its Appliance ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolIdentityRecord {
    appliance_id: ApplianceId,
    pool_id: PoolId,
    role: PoolRole,
}

impl PoolIdentityRecord {
    #[must_use]
    pub const fn new(appliance_id: ApplianceId, pool_id: PoolId, role: PoolRole) -> Self {
        Self {
            appliance_id,
            pool_id,
            role,
        }
    }

    #[must_use]
    pub const fn appliance_id(self) -> ApplianceId {
        self.appliance_id
    }

    #[must_use]
    pub const fn pool_id(self) -> PoolId {
        self.pool_id
    }

    #[must_use]
    pub const fn role(self) -> PoolRole {
        self.role
    }

    #[must_use]
    pub fn encode(self) -> [u8; POOL_IDENTITY_RECORD_BYTES] {
        let mut bytes = [0_u8; POOL_IDENTITY_RECORD_BYTES];
        bytes[0..8].copy_from_slice(MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES);
        put_u16(&mut bytes, 12, RECORD_TYPE);
        put_u16(&mut bytes, 14, self.role as u16);
        put_u32(&mut bytes, 16, 4_096);
        put_u16(&mut bytes, 20, CRC32C_ALGORITHM);
        bytes[24..40].copy_from_slice(&self.appliance_id.bytes());
        bytes[40..56].copy_from_slice(&self.pool_id.bytes());
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    /// Decodes and verifies one complete Pool identity record.
    ///
    /// # Errors
    ///
    /// Returns length, checksum, version, reserved-field, role, or zero-ID
    /// errors. Unknown versions are rejected; there is no migration reader.
    pub fn decode(bytes: &[u8]) -> Result<Self, PoolIdentityFormatError> {
        if bytes.len() != POOL_IDENTITY_RECORD_BYTES {
            return Err(PoolIdentityFormatError::InvalidLength(bytes.len()));
        }
        if &bytes[0..8] != MAGIC {
            return Err(PoolIdentityFormatError::InvalidMagic);
        }
        if crc32c_with_zeroed_u32(bytes, CHECKSUM_OFFSET) != get_u32(bytes, CHECKSUM_OFFSET) {
            return Err(PoolIdentityFormatError::ChecksumMismatch);
        }
        if get_u16(bytes, 8) != FORMAT_VERSION
            || get_u16(bytes, 10) != HEADER_BYTES
            || get_u16(bytes, 12) != RECORD_TYPE
            || usize::try_from(get_u32(bytes, 16)) != Ok(POOL_IDENTITY_RECORD_BYTES)
            || get_u16(bytes, 20) != CRC32C_ALGORITHM
            || get_u16(bytes, 22) != 0
            || bytes[60..].iter().any(|byte| *byte != 0)
        {
            return Err(PoolIdentityFormatError::UnsupportedOrReservedField);
        }
        let role = PoolRole::decode(get_u16(bytes, 14))?;
        let mut appliance_id = [0_u8; 16];
        appliance_id.copy_from_slice(&bytes[24..40]);
        let appliance_id =
            ApplianceId::new(appliance_id).ok_or(PoolIdentityFormatError::ZeroApplianceId)?;
        let mut pool_id = [0_u8; 16];
        pool_id.copy_from_slice(&bytes[40..56]);
        let pool_id = PoolId::new(pool_id).ok_or(PoolIdentityFormatError::ZeroPoolId)?;
        Ok(Self::new(appliance_id, pool_id, role))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PoolIdentityFormatError {
    InvalidLength(usize),
    InvalidMagic,
    ChecksumMismatch,
    UnsupportedOrReservedField,
    UnsupportedRole(u16),
    ZeroApplianceId,
    ZeroPoolId,
}

impl fmt::Display for PoolIdentityFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PoolIdentityFormatError {}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 field"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field"),
    )
}
