use crate::MetadataObjectId;
use core::fmt;

pub const COMMIT_RECORD_BYTES: usize = 4_096;
const COMMIT_RECORD_BYTES_U32: u32 = 4_096;
const COMMIT_MAGIC: &[u8; 8] = b"FDCMIT01";
const EPOCH_FORMAT_VERSION: u16 = 2;
pub const CURRENT_REPOSITORY_FORMAT_EPOCH: u16 = 1;
const RECORD_TYPE_NAMESPACE: u16 = 1;
const HEADER_BYTES: u16 = 176;
const BLAKE3_256_ALGORITHM: u16 = 1;
const CRC32C_ALGORITHM: u16 = 1;
const CHECKSUM_OFFSET: usize = 168;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitRecordHash([u8; 32]);

impl CommitRecordHash {
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
pub struct PolicySetId([u8; 32]);

impl PolicySetId {
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Option<Self> {
        if bytes == [0; 32] {
            None
        } else {
            Some(Self(bytes))
        }
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitRecord {
    generation: u64,
    previous_record_hash: CommitRecordHash,
    namespace_root: MetadataObjectId,
    policy_set: PolicySetId,
    namespace_mutation_cutoff: u64,
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    format_epoch: u16,
}

impl CommitRecord {
    /// Constructs one record in a contiguous generation/hash chain.
    ///
    /// # Errors
    ///
    /// Generation one requires a zero predecessor hash; every later generation
    /// requires a nonzero predecessor hash.
    pub fn new(
        generation: u64,
        previous_record_hash: CommitRecordHash,
        namespace_root: MetadataObjectId,
        policy_set: PolicySetId,
        namespace_mutation_cutoff: u64,
        inode_reservation_end: u64,
        inode_allocation_cursor: u64,
    ) -> Result<Self, CommitFormatError> {
        Self::new_decoded(
            generation,
            previous_record_hash,
            namespace_root,
            policy_set,
            namespace_mutation_cutoff,
            inode_reservation_end,
            inode_allocation_cursor,
            CURRENT_REPOSITORY_FORMAT_EPOCH,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_decoded(
        generation: u64,
        previous_record_hash: CommitRecordHash,
        namespace_root: MetadataObjectId,
        policy_set: PolicySetId,
        namespace_mutation_cutoff: u64,
        inode_reservation_end: u64,
        inode_allocation_cursor: u64,
        format_epoch: u16,
    ) -> Result<Self, CommitFormatError> {
        if generation == 0 || (generation == 1) != (previous_record_hash == CommitRecordHash::ZERO)
        {
            return Err(CommitFormatError::InvalidChainStart);
        }
        if inode_reservation_end < 2 {
            return Err(CommitFormatError::InvalidInodeReservation);
        }
        if inode_allocation_cursor < 2 || inode_allocation_cursor > inode_reservation_end {
            return Err(CommitFormatError::InvalidInodeAllocationCursor);
        }
        Ok(Self {
            generation,
            previous_record_hash,
            namespace_root,
            policy_set,
            namespace_mutation_cutoff,
            inode_reservation_end,
            inode_allocation_cursor,
            format_epoch,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn previous_record_hash(self) -> CommitRecordHash {
        self.previous_record_hash
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
    pub const fn namespace_mutation_cutoff(self) -> u64 {
        self.namespace_mutation_cutoff
    }

    #[must_use]
    pub const fn inode_reservation_end(self) -> u64 {
        self.inode_reservation_end
    }

    #[must_use]
    pub const fn inode_allocation_cursor(self) -> u64 {
        self.inode_allocation_cursor
    }

    #[must_use]
    pub const fn format_epoch(self) -> u16 {
        self.format_epoch
    }

    #[must_use]
    pub fn encode(self) -> [u8; COMMIT_RECORD_BYTES] {
        let mut bytes = [0_u8; COMMIT_RECORD_BYTES];
        bytes[0..8].copy_from_slice(COMMIT_MAGIC);
        put_u16(&mut bytes, 8, EPOCH_FORMAT_VERSION);
        put_u16(&mut bytes, 10, RECORD_TYPE_NAMESPACE);
        put_u16(&mut bytes, 12, HEADER_BYTES);
        put_u16(&mut bytes, 14, BLAKE3_256_ALGORITHM);
        put_u32(&mut bytes, 16, COMMIT_RECORD_BYTES_U32);
        put_u16(&mut bytes, 20, CRC32C_ALGORITHM);
        put_u16(&mut bytes, 22, self.format_epoch);
        put_u64(&mut bytes, 40, self.generation);
        bytes[48..80].copy_from_slice(&self.previous_record_hash.0);
        bytes[80..112].copy_from_slice(&self.namespace_root.bytes());
        bytes[112..144].copy_from_slice(&self.policy_set.bytes());
        put_u64(&mut bytes, 144, self.namespace_mutation_cutoff);
        put_u64(&mut bytes, 152, self.inode_reservation_end);
        put_u64(&mut bytes, 160, self.inode_allocation_cursor);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    /// Validates one complete current Commit Record.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, reserved-field, or chain-start error.
    pub fn decode(bytes: &[u8]) -> Result<Self, CommitFormatError> {
        if bytes.len() != COMMIT_RECORD_BYTES {
            return Err(CommitFormatError::InvalidLength(bytes.len()));
        }
        if &bytes[0..8] != COMMIT_MAGIC {
            return Err(CommitFormatError::InvalidMagic);
        }
        let stored_checksum = get_u32(bytes, CHECKSUM_OFFSET);
        if crc32c_with_zeroed_field(bytes, CHECKSUM_OFFSET) != stored_checksum {
            return Err(CommitFormatError::ChecksumMismatch);
        }
        let format_version = get_u16(bytes, 8);
        let format_epoch = get_u16(bytes, 22);
        if format_version != EPOCH_FORMAT_VERSION
            || format_epoch != CURRENT_REPOSITORY_FORMAT_EPOCH
            || get_u16(bytes, 10) != RECORD_TYPE_NAMESPACE
            || get_u16(bytes, 12) != HEADER_BYTES
            || get_u16(bytes, 14) != BLAKE3_256_ALGORITHM
            || usize::try_from(get_u32(bytes, 16)) != Ok(COMMIT_RECORD_BYTES)
            || get_u16(bytes, 20) != CRC32C_ALGORITHM
            || get_u64(bytes, 24) != 0
            || get_u64(bytes, 32) != 0
            || bytes[172..].iter().any(|byte| *byte != 0)
        {
            return Err(CommitFormatError::UnsupportedOrReservedField);
        }
        let mut previous_hash = [0_u8; 32];
        previous_hash.copy_from_slice(&bytes[48..80]);
        let mut namespace_root = [0_u8; 32];
        namespace_root.copy_from_slice(&bytes[80..112]);
        let namespace_root =
            MetadataObjectId::new(namespace_root).ok_or(CommitFormatError::InvalidNamespaceRoot)?;
        let mut policy_set = [0_u8; 32];
        policy_set.copy_from_slice(&bytes[112..144]);
        let policy_set = PolicySetId::new(policy_set).ok_or(CommitFormatError::InvalidPolicySet)?;
        Self::new_decoded(
            get_u64(bytes, 40),
            CommitRecordHash::from_bytes(previous_hash),
            namespace_root,
            policy_set,
            get_u64(bytes, 144),
            get_u64(bytes, 152),
            get_u64(bytes, 160),
            format_epoch,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitFormatError {
    InvalidLength(usize),
    InvalidMagic,
    ChecksumMismatch,
    UnsupportedOrReservedField,
    InvalidChainStart,
    InvalidNamespaceRoot,
    InvalidPolicySet,
    InvalidInodeReservation,
    InvalidInodeAllocationCursor,
}

impl fmt::Display for CommitFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CommitFormatError {}

use crate::crc32c_with_zeroed_u32 as crc32c_with_zeroed_field;

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
