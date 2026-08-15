use core::fmt;

use crate::{ExactIndexProfileId, ExactIndexRunSetId};

pub const EXACT_INDEX_ACTIVATION_RECORD_BYTES: usize = 4_096;
const RECORD_BYTES_U32: u32 = 4_096;
const MAGIC: [u8; 8] = *b"FDXACT01";
const FORMAT_VERSION: u16 = 1;
const RECORD_TYPE_ACTIVATE_RUN_SET: u16 = 1;
const HEADER_BYTES: u16 = 160;
const BLAKE3_256_ALGORITHM: u16 = 1;
const CRC32C_ALGORITHM: u16 = 1;
const CHECKSUM_OFFSET: usize = 152;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexActivationHash([u8; 32]);

impl ExactIndexActivationHash {
    pub const ZERO: Self = Self([0; 32]);

    #[must_use]
    pub fn of(encoded_record: &[u8]) -> Self {
        Self(*blake3::hash(encoded_record).as_bytes())
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexActivationRecord {
    generation: u64,
    previous_record_hash: ExactIndexActivationHash,
    run_set_id: ExactIndexRunSetId,
    profile: ExactIndexProfileId,
    run_set_generation: u64,
}

impl ExactIndexActivationRecord {
    /// Constructs one record in a contiguous activation hash chain.
    ///
    /// # Errors
    ///
    /// Generation one requires a zero predecessor; every later generation
    /// requires a nonzero predecessor. Run Set generation must be nonzero.
    pub fn new(
        generation: u64,
        previous_record_hash: ExactIndexActivationHash,
        run_set_id: ExactIndexRunSetId,
        profile: ExactIndexProfileId,
        run_set_generation: u64,
    ) -> Result<Self, ExactIndexActivationError> {
        if generation == 0
            || (generation == 1) != (previous_record_hash == ExactIndexActivationHash::ZERO)
        {
            return Err(ExactIndexActivationError::InvalidChainStart);
        }
        if run_set_generation == 0 {
            return Err(ExactIndexActivationError::InvalidRunSetGeneration);
        }
        Ok(Self {
            generation,
            previous_record_hash,
            run_set_id,
            profile,
            run_set_generation,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn previous_record_hash(self) -> ExactIndexActivationHash {
        self.previous_record_hash
    }

    #[must_use]
    pub const fn run_set_id(self) -> ExactIndexRunSetId {
        self.run_set_id
    }

    #[must_use]
    pub const fn profile(self) -> ExactIndexProfileId {
        self.profile
    }

    #[must_use]
    pub const fn run_set_generation(self) -> u64 {
        self.run_set_generation
    }

    #[must_use]
    pub fn encode(self) -> [u8; EXACT_INDEX_ACTIVATION_RECORD_BYTES] {
        let mut bytes = [0_u8; EXACT_INDEX_ACTIVATION_RECORD_BYTES];
        bytes[0..8].copy_from_slice(&MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, RECORD_TYPE_ACTIVATE_RUN_SET);
        put_u16(&mut bytes, 12, HEADER_BYTES);
        put_u16(&mut bytes, 14, BLAKE3_256_ALGORITHM);
        put_u32(&mut bytes, 16, RECORD_BYTES_U32);
        put_u16(&mut bytes, 20, CRC32C_ALGORITHM);
        put_u64(&mut bytes, 40, self.generation);
        bytes[48..80].copy_from_slice(&self.previous_record_hash.bytes());
        bytes[80..112].copy_from_slice(&self.run_set_id.bytes());
        bytes[112..144].copy_from_slice(&self.profile.bytes());
        put_u64(&mut bytes, 144, self.run_set_generation);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    /// Validates one complete activation record.
    ///
    /// # Errors
    ///
    /// Returns structural, checksum, reserved-field, identity, or chain-start
    /// failures.
    pub fn decode(bytes: &[u8]) -> Result<Self, ExactIndexActivationError> {
        if bytes.len() != EXACT_INDEX_ACTIVATION_RECORD_BYTES {
            return Err(ExactIndexActivationError::InvalidLength(bytes.len()));
        }
        if bytes[0..8] != MAGIC {
            return Err(ExactIndexActivationError::InvalidMagic);
        }
        if crc32c_with_zeroed_field(bytes, CHECKSUM_OFFSET) != get_u32(bytes, CHECKSUM_OFFSET) {
            return Err(ExactIndexActivationError::ChecksumMismatch);
        }
        if get_u16(bytes, 8) != FORMAT_VERSION
            || get_u16(bytes, 10) != RECORD_TYPE_ACTIVATE_RUN_SET
            || get_u16(bytes, 12) != HEADER_BYTES
            || get_u16(bytes, 14) != BLAKE3_256_ALGORITHM
            || get_u32(bytes, 16) != RECORD_BYTES_U32
            || get_u16(bytes, 20) != CRC32C_ALGORITHM
            || get_u16(bytes, 22) != 0
            || get_u64(bytes, 24) != 0
            || get_u64(bytes, 32) != 0
            || bytes[156..].iter().any(|byte| *byte != 0)
        {
            return Err(ExactIndexActivationError::UnsupportedOrReservedField);
        }
        let mut previous = [0_u8; 32];
        previous.copy_from_slice(&bytes[48..80]);
        let mut run_set_id = [0_u8; 32];
        run_set_id.copy_from_slice(&bytes[80..112]);
        let run_set_id = ExactIndexRunSetId::from_bytes(run_set_id)
            .ok_or(ExactIndexActivationError::InvalidRunSetId)?;
        let mut profile = [0_u8; 32];
        profile.copy_from_slice(&bytes[112..144]);
        let profile =
            ExactIndexProfileId::new(profile).ok_or(ExactIndexActivationError::InvalidProfile)?;
        Self::new(
            get_u64(bytes, 40),
            ExactIndexActivationHash::from_bytes(previous),
            run_set_id,
            profile,
            get_u64(bytes, 144),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactIndexActivationError {
    InvalidLength(usize),
    InvalidMagic,
    ChecksumMismatch,
    UnsupportedOrReservedField,
    InvalidChainStart,
    InvalidRunSetId,
    InvalidProfile,
    InvalidRunSetGeneration,
}

impl fmt::Display for ExactIndexActivationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactIndexActivationError {}

fn crc32c_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    let mut checksummed = bytes.to_vec();
    checksummed[field_offset..field_offset + 4].fill(0);
    crc32c::crc32c(&checksummed)
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}
