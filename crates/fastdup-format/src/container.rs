//! Container format facade: stable identities, limits and public proof/codec exports.
//!
//! Private modules separate wire geometry, record codecs, adaptive writing,
//! bounded recovery and verified payload ownership. Public exports remain
//! stable; structural evidence never substitutes for content verification.
use crate::crc32c_with_zeroed_u32;
use core::fmt;

mod adaptive;
mod aligned;
mod compression;
mod dependent;
mod envelope;
mod image;
mod payload;
mod records;
mod recovery;
mod structure;
mod summary;
mod writer;

pub use adaptive::{
    AdaptiveContainerEncoding, PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion,
    PreparedEncodedRecord, PreparedIndependentRecord,
};
pub use aligned::AlignedContainerBytes;
pub use compression::{IncompressibilityGateMetrics, IncompressibilityGatePolicy};
pub use dependent::{
    DependentCodec, DependentDependency, DependentRecord, PreparedDependentRecord,
    PreparedSparseXorRecord, PreparedZstdPrefixRecord, SparseXorRecord, SparseXorRun,
    ZstdPrefixDependency, ZstdPrefixRecord,
};
pub use envelope::{BuildingContainerHeader, ContainerHeader};
pub use image::{SealedContainer, VerifiedContainerImage, VerifiedContainerPublication};
pub use payload::{
    CompressedVerifiedChunkPayload, VerifiedChunkBackingId, VerifiedChunkPayload, VerifiedReadView,
};
pub use records::RawRecord;
pub use recovery::{
    ContainerRecordRange, ContainerRecoveryEnvelope, RecoveryIndexCandidate,
    SealedContainerDescriptor, VerifiedRecordPayloads, VerifiedRecoveryIndex,
};
pub use structure::{ContainerStructure, StructuralChunk};
pub use summary::ContainerIntrinsicSummary;

pub const HEADER_BYTES: usize = 4_096;
pub const RECORD_HEADER_BYTES: usize = 128;
pub const FOOTER_BYTES: u64 = 4_096;
pub const MAX_CONTAINER_BYTES: u64 = 64 * 1_024 * 1_024;
pub const MAX_RECORD_BYTES: usize = 1_024 * 1_024;
pub const MAX_DECODED_RECORD_BYTES: usize = 512 * 1_024;
pub const MAX_LOGICAL_CHUNK_BYTES: usize = 256 * 1_024;
const HEADER_MAGIC: &[u8; 8] = b"FDCTNR01";
const HEADER_BYTES_U16: u16 = 4_096;
const FORMAT_VERSION: u16 = 1;
const CONTAINER_FORMAT_VERSION: u16 = 3;
const SEALED_STATE: u16 = 2;
const CRC32C_ALGORITHM: u16 = 1;
const BLAKE3_256_ALGORITHM: u16 = 1;
const BLAKE3_STRUCTURAL_COMMITMENT_ALGORITHM: u16 = 2;
const CONTAINER_COMMITMENT_DOMAIN_V1: &[u8] = b"fastdup-container-structural-v1\0";
const RECORD_ALIGNMENT: u16 = 64;
const INDEX_HEADER_BYTES: u64 = 64;
const INDEX_ENTRY_BYTES: u64 = 128;
const HEADER_CRC_OFFSET: usize = 104;
const HEADER_SUMMARY_OFFSET: usize = 128;
const CONTAINER_SUMMARY_BYTES: usize = 128;
const RECORD_MAGIC: &[u8; 8] = b"FDRECD01";
pub(crate) const RAW_CODEC: u16 = 1;
pub(crate) const ZSTD_CODEC: u16 = 2;
pub(crate) const ZSTD_PREFIX_CODEC: u16 = 3;
pub(crate) const SPARSE_XOR_CODEC: u16 = 4;
const ZSTD_LEVEL_V1: i32 = 3;
const ZSTD_PREFIX_LEVEL_V1: i32 = 3;
const ZSTD_RESCUE_LEVEL_V1: i32 = 1;
const INCOMPRESSIBILITY_GATE_MIN_BYTES_V1: usize = 128 * 1_024;
const ZSTD_MINIMUM_SAVINGS_BYTES_V1: usize = 4 * 1_024;
const ZSTD_MINIMUM_SAVINGS_PERCENT_V1: u128 = 3;
const CHUNK_TABLE_ENTRY_BYTES: usize = 64;
const RECORD_CRC_OFFSET: usize = 60;
const RAW_PAYLOAD_OFFSET: usize = RECORD_HEADER_BYTES + CHUNK_TABLE_ENTRY_BYTES;
const RAW_PAYLOAD_OFFSET_U32: u32 = 192;
const MIN_RAW_RECORD_BYTES: usize = 256;
const RECORD_HEADER_BYTES_U16: u16 = 128;
const RECORD_HEADER_BYTES_U32: u32 = 128;
const CHUNK_TABLE_ENTRY_BYTES_U16: u16 = 64;
const INDEX_MAGIC: &[u8; 8] = b"FDINDX01";
const INDEX_HEADER_BYTES_USIZE: usize = 64;
const INDEX_ENTRY_BYTES_USIZE: usize = 128;
const INDEX_CRC_OFFSET: usize = 36;
const FOOTER_MAGIC: &[u8; 8] = b"FDFOOT01";
const FOOTER_BYTES_USIZE: usize = 4_096;
const FOOTER_HASH_OFFSET: usize = 96;
const FOOTER_SUMMARY_OFFSET: usize = 192;
const FOOTER_CRC_OFFSET: usize = 128;

/// Opaque physical Location evidence emitted only by a fully verified
/// immutable independent Container record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedChunkLocation {
    chunk_id: ChunkId,
    logical_length: u32,
    container_id: ContainerId,
    container_generation: u64,
    record_offset: u64,
    record_length: u32,
    chunk_ordinal: u32,
    decoded_offset: u32,
    codec_id: u16,
    dependency_id: [u8; 32],
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
}

impl VerifiedChunkLocation {
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn container_id(self) -> ContainerId {
        self.container_id
    }

    #[must_use]
    pub const fn container_generation(self) -> u64 {
        self.container_generation
    }

    #[must_use]
    pub const fn record_offset(self) -> u64 {
        self.record_offset
    }

    #[must_use]
    pub const fn record_length(self) -> u32 {
        self.record_length
    }

    #[must_use]
    pub const fn chunk_ordinal(self) -> u32 {
        self.chunk_ordinal
    }

    #[must_use]
    pub const fn decoded_offset(self) -> u32 {
        self.decoded_offset
    }

    #[must_use]
    pub const fn codec_id(self) -> u16 {
        self.codec_id
    }

    #[must_use]
    pub const fn dependency_id(self) -> [u8; 32] {
        self.dependency_id
    }

    #[must_use]
    pub const fn record_crc32c(self) -> u32 {
        self.record_crc32c
    }

    #[must_use]
    pub const fn record_decoded_length(self) -> u32 {
        self.record_decoded_length
    }

    #[must_use]
    pub const fn record_payload_length(self) -> u32 {
        self.record_payload_length
    }
}

/// Opaque physical Location evidence emitted only by a fully verified
/// immutable RAW Container.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedRawLocation {
    chunk_id: ChunkId,
    logical_length: u32,
    container_id: ContainerId,
    container_generation: u64,
    record_offset: u64,
    record_length: u32,
    record_crc32c: u32,
}

impl VerifiedRawLocation {
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn container_id(self) -> ContainerId {
        self.container_id
    }

    #[must_use]
    pub const fn container_generation(self) -> u64 {
        self.container_generation
    }

    #[must_use]
    pub const fn record_offset(self) -> u64 {
        self.record_offset
    }

    #[must_use]
    pub const fn record_length(self) -> u32 {
        self.record_length
    }

    #[must_use]
    pub const fn record_crc32c(self) -> u32 {
        self.record_crc32c
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkId([u8; 32]);

impl ChunkId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerId([u8; 16]);

impl ContainerId {
    /// Constructs a stable nonzero container identity.
    ///
    /// # Errors
    ///
    /// Returns an error for the all-zero value reserved as invalid.
    pub fn new(bytes: [u8; 16]) -> Result<Self, FormatError> {
        if bytes == [0; 16] {
            return Err(FormatError::ZeroContainerId);
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerLayout {
    pub record_count: u32,
    pub chunk_entry_count: u32,
    pub index_offset: u64,
    pub index_length: u64,
    pub footer_offset: u64,
    pub file_length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FormatError {
    InvalidHeaderLength(usize),
    InvalidHeaderMagic,
    HeaderChecksumMismatch,
    InvalidRecordLength(usize),
    InvalidRecordMagic,
    RecordChecksumMismatch,
    InvalidRawRecord,
    InvalidZstdRecord,
    InvalidZstdPrefixRecord,
    ZstdPrefixBaseMismatch,
    ZstdPrefixBaseRequired,
    InvalidSparseXorRecord,
    InvalidDependentRecord,
    DependentBaseMismatch,
    DependentBaseRequired,
    ZstdFailure,
    CompressionGateFailure,
    ChunkHashMismatch,
    InvalidContainerLength(usize),
    InvalidFooter,
    FooterChecksumMismatch,
    HeaderFooterMismatch,
    InvalidRecoveryIndex,
    IndexChecksumMismatch,
    RecoveryIndexCandidateMismatch,
    IndexRecordMismatch,
    ExactLocationMismatch,
    NonZeroContainerPadding,
    ContainerHashMismatch,
    WriterImageMismatch,
    ContainerNotSealed,
    UnsupportedHeaderField,
    NonZeroHeaderReserved,
    InvalidContainerSummary,
    ContainerSummaryMismatch,
    ZeroContainerId,
    ZeroContainerGeneration,
    InvalidContainerLayout,
    ArithmeticOverflow,
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for FormatError {}

fn crc32c_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    crc32c_with_zeroed_u32(bytes, field_offset)
}

fn align_up(value: u64, alignment: u64) -> Result<u64, FormatError> {
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value
        .checked_add(alignment - remainder)
        .ok_or(FormatError::ArithmeticOverflow)
}

fn align_up_usize(value: usize, alignment: usize) -> Result<usize, FormatError> {
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value
        .checked_add(alignment - remainder)
        .ok_or(FormatError::ArithmeticOverflow)
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
