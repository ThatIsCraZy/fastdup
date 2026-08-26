use core::fmt;

use crate::crc32c_with_zeroed_u32;

pub const CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES: usize = 4_096;
const MAGIC: &[u8; 8] = b"FDCGHW01";
const FORMAT_VERSION: u16 = 1;
const HEADER_BYTES: u16 = 128;
const RECORD_TYPE: u16 = 1;
const BLAKE3_256_ALGORITHM: u16 = 1;
const CRC32C_ALGORITHM: u16 = 1;
const CHECKSUM_OFFSET: usize = 72;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerGenerationHighWaterHash([u8; 32]);

impl ContainerGenerationHighWaterHash {
    pub const ZERO: Self = Self([0; 32]);

    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
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
pub struct ContainerGenerationHighWaterRecord {
    sequence: u64,
    previous_record_hash: ContainerGenerationHighWaterHash,
    reserved_through: u64,
}

impl ContainerGenerationHighWaterRecord {
    /// Constructs one monotonic allocator record.
    ///
    /// # Errors
    ///
    /// Sequence one requires a zero predecessor; successors require a nonzero
    /// predecessor and every record reserves at least one generation.
    pub fn new(
        sequence: u64,
        previous_record_hash: ContainerGenerationHighWaterHash,
        reserved_through: u64,
    ) -> Result<Self, ContainerGenerationHighWaterFormatError> {
        if sequence == 0
            || (sequence == 1) != (previous_record_hash == ContainerGenerationHighWaterHash::ZERO)
        {
            return Err(ContainerGenerationHighWaterFormatError::InvalidChainStart);
        }
        if reserved_through == 0 {
            return Err(ContainerGenerationHighWaterFormatError::InvalidHighWater);
        }
        Ok(Self {
            sequence,
            previous_record_hash,
            reserved_through,
        })
    }

    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn previous_record_hash(self) -> ContainerGenerationHighWaterHash {
        self.previous_record_hash
    }

    #[must_use]
    pub const fn reserved_through(self) -> u64 {
        self.reserved_through
    }

    #[must_use]
    pub fn encode(self) -> [u8; CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES] {
        let mut bytes = [0_u8; CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES];
        bytes[0..8].copy_from_slice(MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES);
        put_u16(&mut bytes, 12, RECORD_TYPE);
        put_u16(&mut bytes, 14, BLAKE3_256_ALGORITHM);
        put_u32(&mut bytes, 16, 4_096);
        put_u16(&mut bytes, 20, CRC32C_ALGORITHM);
        put_u64(&mut bytes, 24, self.sequence);
        bytes[32..64].copy_from_slice(&self.previous_record_hash.bytes());
        put_u64(&mut bytes, 64, self.reserved_through);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    /// Decodes and verifies one complete allocator record.
    ///
    /// # Errors
    ///
    /// Returns length, checksum, version, reserved-byte, or chain-shape errors.
    pub fn decode(bytes: &[u8]) -> Result<Self, ContainerGenerationHighWaterFormatError> {
        if bytes.len() != CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES {
            return Err(ContainerGenerationHighWaterFormatError::InvalidLength(
                bytes.len(),
            ));
        }
        if &bytes[0..8] != MAGIC {
            return Err(ContainerGenerationHighWaterFormatError::InvalidMagic);
        }
        if crc32c_with_zeroed_u32(bytes, CHECKSUM_OFFSET) != get_u32(bytes, CHECKSUM_OFFSET) {
            return Err(ContainerGenerationHighWaterFormatError::ChecksumMismatch);
        }
        if get_u16(bytes, 8) != FORMAT_VERSION
            || get_u16(bytes, 10) != HEADER_BYTES
            || get_u16(bytes, 12) != RECORD_TYPE
            || get_u16(bytes, 14) != BLAKE3_256_ALGORITHM
            || usize::try_from(get_u32(bytes, 16))
                != Ok(CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES)
            || get_u16(bytes, 20) != CRC32C_ALGORITHM
            || get_u16(bytes, 22) != 0
            || bytes[76..].iter().any(|byte| *byte != 0)
        {
            return Err(ContainerGenerationHighWaterFormatError::UnsupportedOrReservedField);
        }
        let mut previous = [0_u8; 32];
        previous.copy_from_slice(&bytes[32..64]);
        Self::new(
            get_u64(bytes, 24),
            ContainerGenerationHighWaterHash::from_bytes(previous),
            get_u64(bytes, 64),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContainerGenerationHighWaterFormatError {
    InvalidLength(usize),
    InvalidMagic,
    ChecksumMismatch,
    UnsupportedOrReservedField,
    InvalidChainStart,
    InvalidHighWater,
}

impl fmt::Display for ContainerGenerationHighWaterFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ContainerGenerationHighWaterFormatError {}

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

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field"),
    )
}
