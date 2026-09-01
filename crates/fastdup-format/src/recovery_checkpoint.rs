use core::fmt;

use crate::{CommitRecord, MAX_METADATA_OBJECT_BYTES, MetadataObjectId, PolicySetId};

pub const RECOVERY_CHECKPOINT_HEADER_BYTES: usize = 4_096;
pub const RECOVERY_CHECKPOINT_FOOTER_BYTES: usize = 4_096;
pub const RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES: usize = 64;
pub const RECOVERY_CHECKPOINT_ENTRY_ALIGNMENT: usize = 64;
pub const RECOVERY_CHECKPOINT_HEAD_BYTES: usize = 4_096;

const HEADER_MAGIC: [u8; 8] = *b"FDRCV001";
const FOOTER_MAGIC: [u8; 8] = *b"FDRCF001";
const ENTRY_MAGIC: [u8; 8] = *b"FDRCM001";
const HEAD_MAGIC: [u8; 8] = *b"FDRCH001";
const FORMAT_VERSION: u16 = 1;
const BLAKE3_256_ALGORITHM: u16 = 1;
const DESCRIPTOR_CRC_OFFSET: usize = 136;
const ENTRY_CRC_OFFSET: usize = 52;
const HEAD_CRC_OFFSET: usize = 104;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointDescriptor {
    generation: u64,
    namespace_root: MetadataObjectId,
    policy_set: PolicySetId,
    object_count: u64,
    file_length: u64,
    body_hash: [u8; 32],
}

impl RecoveryCheckpointDescriptor {
    /// Constructs one authenticated descriptor from the selected Commit.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty object set, an impossible file length, a
    /// zero body hash, or checked-arithmetic overflow.
    pub fn new(
        record: CommitRecord,
        object_count: u64,
        file_length: u64,
        body_hash: [u8; 32],
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        let minimum = RECOVERY_CHECKPOINT_HEADER_BYTES
            .checked_add(crate::COMMIT_RECORD_BYTES)
            .and_then(|length| length.checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES))
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        if object_count == 0
            || file_length
                < u64::try_from(minimum)
                    .map_err(|_| RecoveryCheckpointFormatError::ArithmeticOverflow)?
            || body_hash == [0; 32]
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            generation: record.generation(),
            namespace_root: record.namespace_root(),
            policy_set: record.policy_set(),
            object_count,
            file_length,
            body_hash,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn namespace_root(self) -> MetadataObjectId {
        self.namespace_root
    }

    #[must_use]
    pub const fn policy_set(self) -> PolicySetId {
        self.policy_set
    }

    #[must_use]
    pub const fn object_count(self) -> u64 {
        self.object_count
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn body_hash(self) -> [u8; 32] {
        self.body_hash
    }

    #[must_use]
    pub fn encode_header(self) -> [u8; RECOVERY_CHECKPOINT_HEADER_BYTES] {
        encode_descriptor(HEADER_MAGIC, self)
    }

    #[must_use]
    pub fn encode_footer(self) -> [u8; RECOVERY_CHECKPOINT_FOOTER_BYTES] {
        encode_descriptor(FOOTER_MAGIC, self)
    }

    /// Decodes and authenticates a descriptor header.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, bound, or reserved byte.
    pub fn decode_header(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        decode_descriptor(bytes, HEADER_MAGIC)
    }

    /// Decodes and authenticates a descriptor footer.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, bound, or reserved byte.
    pub fn decode_footer(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        decode_descriptor(bytes, FOOTER_MAGIC)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointEntryHeader {
    object_id: MetadataObjectId,
    encoded_length: u32,
    payload_crc32c: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointHeadRecord {
    generation: u64,
    file_length: u64,
    checkpoint_body_hash: [u8; 32],
    previous_generation: u64,
    previous_record_hash: [u8; 32],
}

impl RecoveryCheckpointHeadRecord {
    /// Constructs the next selector record and links it to the preceding head.
    ///
    /// # Errors
    ///
    /// Returns an error if the predecessor generation is not strictly older.
    pub fn new(
        descriptor: RecoveryCheckpointDescriptor,
        previous: Option<Self>,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        let (previous_generation, previous_record_hash) = previous.map_or((0, [0; 32]), |record| {
            (
                record.generation,
                *blake3::hash(&record.encode()).as_bytes(),
            )
        });
        if previous_generation >= descriptor.generation() {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            generation: descriptor.generation(),
            file_length: descriptor.file_length(),
            checkpoint_body_hash: descriptor.body_hash(),
            previous_generation,
            previous_record_hash,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn checkpoint_body_hash(self) -> [u8; 32] {
        self.checkpoint_body_hash
    }

    #[must_use]
    pub const fn previous_generation(self) -> u64 {
        self.previous_generation
    }

    #[must_use]
    pub const fn previous_record_hash(self) -> [u8; 32] {
        self.previous_record_hash
    }

    #[must_use]
    pub fn record_hash(self) -> [u8; 32] {
        *blake3::hash(&self.encode()).as_bytes()
    }

    #[must_use]
    pub fn encode(self) -> [u8; RECOVERY_CHECKPOINT_HEAD_BYTES] {
        let mut bytes = [0_u8; RECOVERY_CHECKPOINT_HEAD_BYTES];
        bytes[..8].copy_from_slice(&HEAD_MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, 4_096);
        put_u16(&mut bytes, 12, BLAKE3_256_ALGORITHM);
        put_u64(&mut bytes, 16, self.generation);
        put_u64(&mut bytes, 24, self.file_length);
        bytes[32..64].copy_from_slice(&self.checkpoint_body_hash);
        put_u64(&mut bytes, 64, self.previous_generation);
        bytes[72..104].copy_from_slice(&self.previous_record_hash);
        let crc = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, HEAD_CRC_OFFSET, crc);
        bytes
    }

    /// Decodes and authenticates one selector record.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, link, or reserved byte.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        if bytes.len() != RECOVERY_CHECKPOINT_HEAD_BYTES
            || bytes[..8] != HEAD_MAGIC
            || get_u16(bytes, 8) != FORMAT_VERSION
            || usize::from(get_u16(bytes, 10)) != RECOVERY_CHECKPOINT_HEAD_BYTES
            || get_u16(bytes, 12) != BLAKE3_256_ALGORITHM
            || get_u16(bytes, 14) != 0
            || bytes[108..].iter().any(|byte| *byte != 0)
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        let mut checked = [0_u8; RECOVERY_CHECKPOINT_HEAD_BYTES];
        checked.copy_from_slice(bytes);
        let stored_crc = get_u32(bytes, HEAD_CRC_OFFSET);
        put_u32(&mut checked, HEAD_CRC_OFFSET, 0);
        if crc32c::crc32c(&checked) != stored_crc {
            return Err(RecoveryCheckpointFormatError::InvalidChecksum);
        }
        let generation = get_u64(bytes, 16);
        let file_length = get_u64(bytes, 24);
        let mut checkpoint_body_hash = [0_u8; 32];
        checkpoint_body_hash.copy_from_slice(&bytes[32..64]);
        let previous_generation = get_u64(bytes, 64);
        let mut previous_record_hash = [0_u8; 32];
        previous_record_hash.copy_from_slice(&bytes[72..104]);
        if generation == 0
            || file_length == 0
            || checkpoint_body_hash == [0; 32]
            || previous_generation >= generation
            || (previous_generation == 0) != (previous_record_hash == [0; 32])
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            generation,
            file_length,
            checkpoint_body_hash,
            previous_generation,
            previous_record_hash,
        })
    }
}

impl RecoveryCheckpointEntryHeader {
    /// Constructs one authenticated Metadata entry header.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload length is zero, exceeds the Metadata
    /// object bound, or cannot be represented by the format.
    pub fn new(
        object_id: MetadataObjectId,
        encoded_length: usize,
        payload_crc32c: u32,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        if encoded_length == 0 || encoded_length > MAX_METADATA_OBJECT_BYTES {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        Ok(Self {
            object_id,
            encoded_length: u32::try_from(encoded_length)
                .map_err(|_| RecoveryCheckpointFormatError::InvalidEntry)?,
            payload_crc32c,
        })
    }

    #[must_use]
    pub const fn object_id(self) -> MetadataObjectId {
        self.object_id
    }

    #[must_use]
    pub const fn encoded_length(self) -> u32 {
        self.encoded_length
    }

    #[must_use]
    pub const fn payload_crc32c(self) -> u32 {
        self.payload_crc32c
    }

    /// Returns the entry header, payload, and 64-byte alignment padding length.
    ///
    /// # Errors
    ///
    /// Returns an error on checked-arithmetic or integer-conversion overflow.
    pub fn padded_length(self) -> Result<u64, RecoveryCheckpointFormatError> {
        let unpadded = RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES
            .checked_add(
                usize::try_from(self.encoded_length)
                    .map_err(|_| RecoveryCheckpointFormatError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        let padded = unpadded
            .checked_add(RECOVERY_CHECKPOINT_ENTRY_ALIGNMENT - 1)
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?
            / RECOVERY_CHECKPOINT_ENTRY_ALIGNMENT
            * RECOVERY_CHECKPOINT_ENTRY_ALIGNMENT;
        u64::try_from(padded).map_err(|_| RecoveryCheckpointFormatError::ArithmeticOverflow)
    }

    #[must_use]
    pub fn encode(self) -> [u8; RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES] {
        let mut bytes = [0_u8; RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES];
        bytes[..8].copy_from_slice(&ENTRY_MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, 64);
        put_u32(&mut bytes, 12, self.encoded_length);
        bytes[16..48].copy_from_slice(&self.object_id.bytes());
        put_u32(&mut bytes, 48, self.payload_crc32c);
        let crc = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, ENTRY_CRC_OFFSET, crc);
        bytes
    }

    /// Decodes and authenticates one Metadata entry header.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid field, checksum, length, or reserved byte.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        if bytes.len() != RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES
            || bytes[..8] != ENTRY_MAGIC
            || get_u16(bytes, 8) != FORMAT_VERSION
            || usize::from(get_u16(bytes, 10)) != RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES
            || bytes[56..].iter().any(|byte| *byte != 0)
        {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        let mut checked = [0_u8; RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES];
        checked.copy_from_slice(bytes);
        let stored_crc = get_u32(bytes, ENTRY_CRC_OFFSET);
        put_u32(&mut checked, ENTRY_CRC_OFFSET, 0);
        if crc32c::crc32c(&checked) != stored_crc {
            return Err(RecoveryCheckpointFormatError::InvalidChecksum);
        }
        let mut object_id = [0_u8; 32];
        object_id.copy_from_slice(&bytes[16..48]);
        Self::new(
            MetadataObjectId::new(object_id).ok_or(RecoveryCheckpointFormatError::InvalidEntry)?,
            usize::try_from(get_u32(bytes, 12))
                .map_err(|_| RecoveryCheckpointFormatError::InvalidEntry)?,
            get_u32(bytes, 48),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryCheckpointFormatError {
    InvalidDescriptor,
    InvalidEntry,
    InvalidChecksum,
    ArithmeticOverflow,
}

impl fmt::Display for RecoveryCheckpointFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RecoveryCheckpointFormatError {}

fn encode_descriptor(
    magic: [u8; 8],
    descriptor: RecoveryCheckpointDescriptor,
) -> [u8; RECOVERY_CHECKPOINT_HEADER_BYTES] {
    let mut bytes = [0_u8; RECOVERY_CHECKPOINT_HEADER_BYTES];
    bytes[..8].copy_from_slice(&magic);
    put_u16(&mut bytes, 8, FORMAT_VERSION);
    put_u16(&mut bytes, 10, 4_096);
    put_u16(&mut bytes, 12, BLAKE3_256_ALGORITHM);
    put_u64(&mut bytes, 16, descriptor.file_length);
    put_u64(&mut bytes, 24, descriptor.generation);
    put_u64(&mut bytes, 32, descriptor.object_count);
    bytes[40..72].copy_from_slice(&descriptor.namespace_root.bytes());
    bytes[72..104].copy_from_slice(&descriptor.policy_set.bytes());
    bytes[104..136].copy_from_slice(&descriptor.body_hash);
    let crc = crc32c::crc32c(&bytes);
    put_u32(&mut bytes, DESCRIPTOR_CRC_OFFSET, crc);
    bytes
}

fn decode_descriptor(
    bytes: &[u8],
    magic: [u8; 8],
) -> Result<RecoveryCheckpointDescriptor, RecoveryCheckpointFormatError> {
    if bytes.len() != RECOVERY_CHECKPOINT_HEADER_BYTES
        || bytes[..8] != magic
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECOVERY_CHECKPOINT_HEADER_BYTES
        || get_u16(bytes, 12) != BLAKE3_256_ALGORITHM
        || get_u16(bytes, 14) != 0
        || bytes[140..].iter().any(|byte| *byte != 0)
    {
        return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
    }
    let mut checked = [0_u8; RECOVERY_CHECKPOINT_HEADER_BYTES];
    checked.copy_from_slice(bytes);
    let stored_crc = get_u32(bytes, DESCRIPTOR_CRC_OFFSET);
    put_u32(&mut checked, DESCRIPTOR_CRC_OFFSET, 0);
    if crc32c::crc32c(&checked) != stored_crc {
        return Err(RecoveryCheckpointFormatError::InvalidChecksum);
    }
    let mut namespace_root = [0_u8; 32];
    namespace_root.copy_from_slice(&bytes[40..72]);
    let mut policy_set = [0_u8; 32];
    policy_set.copy_from_slice(&bytes[72..104]);
    let mut body_hash = [0_u8; 32];
    body_hash.copy_from_slice(&bytes[104..136]);
    let descriptor = SelfDescriptor {
        generation: get_u64(bytes, 24),
        namespace_root: MetadataObjectId::new(namespace_root)
            .ok_or(RecoveryCheckpointFormatError::InvalidDescriptor)?,
        policy_set: PolicySetId::new(policy_set)
            .ok_or(RecoveryCheckpointFormatError::InvalidDescriptor)?,
        object_count: get_u64(bytes, 32),
        file_length: get_u64(bytes, 16),
        body_hash,
    };
    descriptor.try_into()
}

struct SelfDescriptor {
    generation: u64,
    namespace_root: MetadataObjectId,
    policy_set: PolicySetId,
    object_count: u64,
    file_length: u64,
    body_hash: [u8; 32],
}

impl TryFrom<SelfDescriptor> for RecoveryCheckpointDescriptor {
    type Error = RecoveryCheckpointFormatError;

    fn try_from(value: SelfDescriptor) -> Result<Self, Self::Error> {
        let minimum = RECOVERY_CHECKPOINT_HEADER_BYTES
            .checked_add(crate::COMMIT_RECORD_BYTES)
            .and_then(|length| length.checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES))
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        if value.generation == 0
            || value.object_count == 0
            || value.file_length
                < u64::try_from(minimum)
                    .map_err(|_| RecoveryCheckpointFormatError::ArithmeticOverflow)?
            || value.body_hash == [0; 32]
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            generation: value.generation,
            namespace_root: value.namespace_root,
            policy_set: value.policy_set,
            object_count: value.object_count,
            file_length: value.file_length,
            body_hash: value.body_hash,
        })
    }
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed field"))
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed field"))
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed field"))
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
