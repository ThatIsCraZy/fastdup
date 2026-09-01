use core::fmt;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::Arc;

use rayon::prelude::*;

use fastdup_copy_metrics::{CopyClass, record_copy};

use crate::crc32c_with_zeroed_u32;
use crate::exact_index::{ExactIndexEntry, ExactLocationTransition};

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
const CONTAINER_FORMAT_VERSION: u16 = 2;
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
const CONTAINER_SUMMARY_BYTES: usize = 96;
const RECORD_MAGIC: &[u8; 8] = b"FDRECD01";
pub(crate) const RAW_CODEC: u16 = 1;
pub(crate) const ZSTD_CODEC: u16 = 2;
pub(crate) const ZSTD_PREFIX_CODEC: u16 = 3;
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

thread_local! {
    static ADAPTIVE_ENCODER_V1: RefCell<Option<AdaptiveEncoderV1>> =
        const { RefCell::new(None) };
}
const FOOTER_CRC_OFFSET: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildingContainerHeader {
    container_id: ContainerId,
    container_generation: u64,
}

impl BuildingContainerHeader {
    /// Creates an unsealed construction header.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation is zero.
    pub fn new(container_id: ContainerId, container_generation: u64) -> Result<Self, FormatError> {
        if container_generation == 0 {
            return Err(FormatError::ZeroContainerGeneration);
        }
        Ok(Self {
            container_id,
            container_generation,
        })
    }

    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0_u8; HEADER_BYTES];
        bytes[0..8].copy_from_slice(HEADER_MAGIC);
        put_u16(&mut bytes, 8, CONTAINER_FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, 1);
        put_u16(&mut bytes, 14, CRC32C_ALGORITHM);
        put_u16(&mut bytes, 16, BLAKE3_256_ALGORITHM);
        put_u16(&mut bytes, 18, BLAKE3_STRUCTURAL_COMMITMENT_ALGORITHM);
        put_u16(&mut bytes, 20, RECORD_ALIGNMENT);
        bytes[40..56].copy_from_slice(&self.container_id.0);
        put_u64(&mut bytes, 56, self.container_generation);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, HEADER_CRC_OFFSET, checksum);
        bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedContainer {
    header: ContainerHeader,
    records: Vec<RawRecord>,
    locations: Vec<VerifiedChunkLocation>,
    raw_locations: Vec<VerifiedRawLocation>,
    raw_record_count: usize,
    zstd_record_count: usize,
    zstd_prefix_record_count: usize,
}

/// One owned Container image whose decoded payloads and physical bytes were
/// verified together.
///
/// The private fields prevent callers from pairing trusted decoded evidence
/// with unrelated encoded bytes. Maintenance may therefore transplant an
/// independent Record without recompression while ordinary recovery and scrub
/// continue to validate the resulting Container normally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedContainerImage {
    container: SealedContainer,
    bytes: Vec<u8>,
}

/// Payload-free Location evidence produced by the Container writer or a full
/// independent verifier.
///
/// The writer variant relies on the Chunk identities supplied to encoding and
/// on the exact layout, checksums, Recovery Index, and structural commitment it emits.
/// Ordinary reads, recovery, and scrub construct the same type only after
/// independently checking stored bytes. This type never retains decoded
/// logical Chunk payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedContainerPublication {
    header: ContainerHeader,
    locations: Vec<VerifiedChunkLocation>,
    raw_locations: Vec<VerifiedRawLocation>,
    logical_bytes: u64,
    raw_record_count: usize,
    zstd_record_count: usize,
    zstd_prefix_record_count: usize,
}

#[derive(Clone, Copy)]
enum PublicationContainerProof<'a> {
    RecomputedHash,
    ExactWriterImage(&'a [u8]),
}

impl VerifiedContainerPublication {
    #[must_use]
    pub const fn header(&self) -> &ContainerHeader {
        &self.header
    }

    #[must_use]
    pub fn locations(&self) -> &[VerifiedChunkLocation] {
        &self.locations
    }

    #[must_use]
    pub fn raw_locations(&self) -> &[VerifiedRawLocation] {
        &self.raw_locations
    }

    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    #[must_use]
    pub const fn raw_record_count(&self) -> usize {
        self.raw_record_count
    }

    #[must_use]
    pub const fn zstd_record_count(&self) -> usize {
        self.zstd_record_count
    }

    #[must_use]
    pub const fn zstd_prefix_record_count(&self) -> usize {
        self.zstd_prefix_record_count
    }

    /// Reconstructs the immutable Container summary from payload-free writer
    /// or independent-reader publication evidence.
    ///
    /// This scans only compact Location metadata and is intended for
    /// asynchronous GC-catalog publication. It performs no payload read,
    /// decompression, or Chunk hashing.
    ///
    /// # Errors
    ///
    /// Returns an error if the retained Locations no longer form the exact
    /// record groups and layout named by the verified Header.
    pub fn intrinsic_summary(&self) -> Result<ContainerIntrinsicSummary, FormatError> {
        let record_capacity = usize::try_from(self.header.layout.record_count)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut summary = IntrinsicSummaryAccumulator::with_record_capacity(record_capacity)?;
        let mut cursor = 0_usize;
        while cursor < self.locations.len() {
            let first = self.locations[cursor];
            let record_offset = first.record_offset;
            let mut end = cursor + 1;
            while end < self.locations.len() && self.locations[end].record_offset == record_offset {
                end += 1;
            }
            let group = &self.locations[cursor..end];
            if group.iter().enumerate().any(|(ordinal, location)| {
                location.container_id != self.header.container_id
                    || location.container_generation != self.header.container_generation
                    || location.record_offset != record_offset
                    || location.record_length != first.record_length
                    || location.record_decoded_length != first.record_decoded_length
                    || location.codec_id != first.codec_id
                    || location.dependency_id != first.dependency_id
                    || usize::try_from(location.chunk_ordinal) != Ok(ordinal)
            }) {
                return Err(FormatError::ContainerSummaryMismatch);
            }
            summary.observe(
                first.codec_id,
                usize::try_from(first.record_length)
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
                usize::try_from(first.record_decoded_length)
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
                group.len(),
                (first.codec_id == ZSTD_PREFIX_CODEC).then_some(first.dependency_id),
            )?;
            cursor = end;
        }
        summary.finish(self.header.layout)
    }
}

/// Runtime evidence from the version-1 incompressibility gate.
///
/// These counters describe writer work only. They are not serialized and do
/// not authorize any Container bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IncompressibilityGateMetrics {
    disabled_regions: usize,
    eligible_regions: usize,
    size_bypassed_regions: usize,
    lz4_allowed_regions: usize,
    lz4_rejected_regions: usize,
    zstd1_allowed_regions: usize,
    zstd1_rejected_regions: usize,
    target_zstd_trials: usize,
    target_zstd_accepted: usize,
    target_zstd_rejected: usize,
    raw_regions_after_gate: usize,
    scratch_high_water_bytes: usize,
}

impl IncompressibilityGateMetrics {
    #[must_use]
    pub const fn disabled_regions(self) -> usize {
        self.disabled_regions
    }

    #[must_use]
    pub const fn eligible_regions(self) -> usize {
        self.eligible_regions
    }

    #[must_use]
    pub const fn size_bypassed_regions(self) -> usize {
        self.size_bypassed_regions
    }

    #[must_use]
    pub const fn lz4_allowed_regions(self) -> usize {
        self.lz4_allowed_regions
    }

    #[must_use]
    pub const fn lz4_rejected_regions(self) -> usize {
        self.lz4_rejected_regions
    }

    #[must_use]
    pub const fn zstd1_allowed_regions(self) -> usize {
        self.zstd1_allowed_regions
    }

    #[must_use]
    pub const fn zstd1_rejected_regions(self) -> usize {
        self.zstd1_rejected_regions
    }

    #[must_use]
    pub const fn target_zstd_trials(self) -> usize {
        self.target_zstd_trials
    }

    #[must_use]
    pub const fn target_zstd_accepted(self) -> usize {
        self.target_zstd_accepted
    }

    #[must_use]
    pub const fn target_zstd_rejected(self) -> usize {
        self.target_zstd_rejected
    }

    #[must_use]
    pub const fn raw_regions_after_gate(self) -> usize {
        self.raw_regions_after_gate
    }

    #[must_use]
    pub const fn scratch_high_water_bytes(self) -> usize {
        self.scratch_high_water_bytes
    }

    /// Adds disjoint worker or Container observations with checked counters.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ArithmeticOverflow`] if a counter cannot be
    /// represented.
    pub fn checked_merge(&mut self, other: Self) -> Result<(), FormatError> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self
                    .$field
                    .checked_add(other.$field)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            };
        }
        add!(eligible_regions);
        add!(disabled_regions);
        add!(size_bypassed_regions);
        add!(lz4_allowed_regions);
        add!(lz4_rejected_regions);
        add!(zstd1_allowed_regions);
        add!(zstd1_rejected_regions);
        add!(target_zstd_trials);
        add!(target_zstd_accepted);
        add!(target_zstd_rejected);
        add!(raw_regions_after_gate);
        self.scratch_high_water_bytes = self
            .scratch_high_water_bytes
            .max(other.scratch_high_water_bytes);
        Ok(())
    }
}

/// Execution policy for the dependency-free Zstd incompressibility gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncompressibilityGatePolicy {
    /// Measurement baseline that trials target Zstd for every region.
    Off,
    /// Benchmark challenger that rejects after bounded LZ4 alone.
    Lz4Only,
    /// Bounded LZ4 plus Zstd-1 rescue policy accepted by ADR 0052.
    V1,
}

/// One encoded Container image paired with writer-produced publication
/// evidence and non-authoritative gate metrics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdaptiveContainerEncoding {
    bytes: AlignedContainerBytes,
    publication: VerifiedContainerPublication,
    metrics: IncompressibilityGateMetrics,
}

/// One page-aligned, page-sized immutable Container publication image.
///
/// The format's Header, Footer, and complete file length are all 4 KiB
/// aligned. Retaining that geometry in memory lets a storage adapter use the
/// same owned writer image for Linux Direct I/O without a second full-image
/// copy.
pub struct AlignedContainerBytes {
    allocation: Vec<u8>,
    start: usize,
    length: usize,
}

/// Builds one page-aligned image without first zeroing ranges that later hold
/// already initialized Records or the Recovery Index.
///
/// The backing allocation never reallocates beyond its initial capacity, so
/// its aligned start remains stable while safe `Vec` appends initialize the
/// image in durable byte order. An `AlignedContainerBytes` is exposed only
/// after the complete declared image has been initialized.
struct AlignedContainerBuilder {
    allocation: Vec<u8>,
    start: usize,
    length: usize,
}

impl AlignedContainerBytes {
    /// Returns an allocation-free consumed-image sentinel.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            allocation: Vec::new(),
            start: 0,
            length: 0,
        }
    }

    /// Allocates one zero-filled image whose address and length are both
    /// aligned to the Container block size.
    ///
    /// # Panics
    ///
    /// Panics when `length` is zero or is not a multiple of 4 KiB.
    #[must_use]
    pub fn zeroed(length: usize) -> Self {
        assert!(length != 0 && length.is_multiple_of(HEADER_BYTES));
        let allocation_length = length
            .checked_add(HEADER_BYTES - 1)
            .expect("ASSERT: bounded Container alignment allocation cannot overflow");
        let allocation = vec![0; allocation_length];
        let misalignment = allocation.as_ptr().addr() % HEADER_BYTES;
        let start = (HEADER_BYTES - misalignment) % HEADER_BYTES;
        assert!(start + length <= allocation.len());
        Self {
            allocation,
            start,
            length,
        }
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl AlignedContainerBuilder {
    fn new(length: usize) -> Self {
        assert!(length != 0 && length.is_multiple_of(HEADER_BYTES));
        let allocation_length = length
            .checked_add(HEADER_BYTES - 1)
            .expect("ASSERT: bounded Container alignment allocation cannot overflow");
        let mut allocation: Vec<u8> = Vec::with_capacity(allocation_length);
        let misalignment = allocation.as_ptr().addr() % HEADER_BYTES;
        let start = (HEADER_BYTES - misalignment) % HEADER_BYTES;
        allocation.resize(start, 0);
        Self {
            allocation,
            start,
            length,
        }
    }

    fn image_length(&self) -> usize {
        self.allocation.len() - self.start
    }

    fn append(&mut self, bytes: &mut Vec<u8>) {
        let next_length = self
            .image_length()
            .checked_add(bytes.len())
            .expect("ASSERT: bounded Container append length cannot overflow");
        assert!(next_length <= self.length);
        self.allocation.append(bytes);
    }

    fn append_zeroed(&mut self, length: usize) {
        let next_length = self
            .image_length()
            .checked_add(length)
            .expect("ASSERT: bounded Container padding length cannot overflow");
        assert!(next_length <= self.length);
        self.allocation.resize(self.start + next_length, 0);
    }

    fn finish(self) -> AlignedContainerBytes {
        assert_eq!(self.image_length(), self.length);
        assert_eq!(
            self.allocation[self.start..].as_ptr().addr() % HEADER_BYTES,
            0
        );
        AlignedContainerBytes {
            allocation: self.allocation,
            start: self.start,
            length: self.length,
        }
    }
}

impl AsRef<[u8]> for AlignedContainerBytes {
    fn as_ref(&self) -> &[u8] {
        &self.allocation[self.start..self.start + self.length]
    }
}

impl AsMut<[u8]> for AlignedContainerBytes {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.allocation[self.start..self.start + self.length]
    }
}

impl Clone for AlignedContainerBytes {
    fn clone(&self) -> Self {
        let mut cloned = Self::zeroed(self.length);
        cloned.copy_from_slice(self);
        cloned
    }
}

impl fmt::Debug for AlignedContainerBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlignedContainerBytes")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl PartialEq for AlignedContainerBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for AlignedContainerBytes {}

impl std::ops::Deref for AlignedContainerBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::ops::DerefMut for AlignedContainerBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl AdaptiveContainerEncoding {
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub const fn metrics(&self) -> IncompressibilityGateMetrics {
        self.metrics
    }

    /// Consumes the writer result into the immutable image and the Location
    /// evidence derived while that image was encoded.
    #[must_use]
    pub fn into_publication_parts(self) -> (Vec<u8>, VerifiedContainerPublication) {
        (self.bytes.into_vec(), self.publication)
    }

    /// Consumes the writer result without discarding the image's page
    /// alignment required by a Direct-I/O publication adapter.
    #[must_use]
    pub fn into_aligned_publication_parts(
        self,
    ) -> (AlignedContainerBytes, VerifiedContainerPublication) {
        (self.bytes, self.publication)
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into_vec()
    }
}

/// Header/Footer proof for bounded on-demand reads from one sealed Container.
///
/// This descriptor proves the immutable envelope and layout but deliberately
/// does not claim that the complete Container hash or Recovery Index was read.
/// Each returned record must still pass [`Self::decode_raw_candidate`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedContainerDescriptor {
    header: ContainerHeader,
    container_hash: [u8; 32],
}

/// One complete independently verified Encoding Record decode.
///
/// `requested` retains caller order while `all` contains every unique logical
/// Chunk verified as part of the same physical Record. Both vectors share the
/// decoder's one backing allocation. The type has no public constructor: only
/// the format verifier can create this identity evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedRecordPayloads {
    requested: Vec<VerifiedChunkPayload>,
    all: Vec<VerifiedChunkPayload>,
}

impl VerifiedRecordPayloads {
    #[must_use]
    pub fn requested(&self) -> &[VerifiedChunkPayload] {
        &self.requested
    }

    #[must_use]
    pub fn all(&self) -> &[VerifiedChunkPayload] {
        &self.all
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<VerifiedChunkPayload>, Vec<VerifiedChunkPayload>) {
        (self.requested, self.all)
    }
}

/// Paired immutable Container envelope carrying payload-free recovery
/// acceleration.
///
/// The paired descriptor supplies the exact bounded range for a compact
/// Recovery Index before one selected record is read and independently
/// verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerRecoveryEnvelope {
    descriptor: SealedContainerDescriptor,
}

/// One independently decodable record candidate obtained from an
/// authenticated Container Recovery Index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryIndexCandidate {
    container_id: ContainerId,
    container_generation: u64,
    entry: IndexEntry,
}

/// A Container-local Recovery Index whose CRC, canonical order, and record
/// geometry have been validated without reading record payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedRecoveryIndex {
    descriptor: SealedContainerDescriptor,
    entries: Vec<IndexEntry>,
}

impl SealedContainerDescriptor {
    /// Pairs independently read Header and Footer blocks with the physical
    /// object length.
    ///
    /// # Errors
    ///
    /// Returns length, structural, checksum, reserved-field, identity, or
    /// duplicated-layout failures.
    pub fn decode(
        header_bytes: &[u8],
        footer_bytes: &[u8],
        actual_length: u64,
    ) -> Result<Self, FormatError> {
        Self::decode_envelope(header_bytes, footer_bytes, actual_length)
            .map(|(descriptor, _summary)| descriptor)
    }

    /// Decodes only the immutable GC classification facts from a paired
    /// Header/Footer envelope.
    ///
    /// The ordinary Exact-read descriptor intentionally does not retain this
    /// 96-byte value, keeping descriptor-cache entries and write/read command
    /// moves compact. GC callers pay only the already required two 4 KiB
    /// envelope reads and retain the summary in their separate candidate run.
    ///
    /// # Errors
    ///
    /// Returns the same envelope, layout, checksum, and identity failures as
    /// [`Self::decode`].
    pub fn decode_intrinsic_summary(
        header_bytes: &[u8],
        footer_bytes: &[u8],
        actual_length: u64,
    ) -> Result<ContainerIntrinsicSummary, FormatError> {
        Self::decode_envelope(header_bytes, footer_bytes, actual_length)
            .map(|(_descriptor, summary)| summary)
    }

    fn decode_envelope(
        header_bytes: &[u8],
        footer_bytes: &[u8],
        actual_length: u64,
    ) -> Result<(Self, ContainerIntrinsicSummary), FormatError> {
        let actual_length_usize = usize::try_from(actual_length)
            .map_err(|_| FormatError::InvalidContainerLength(usize::MAX))?;
        validate_container_file_length(actual_length_usize)?;
        let footer = decode_footer(footer_bytes)?;
        let (header, intrinsic_summary) = ContainerHeader::decode_with_summary(header_bytes)?;
        let expected_footer_offset = actual_length
            .checked_sub(FOOTER_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if header.container_id != footer.container_id
            || header.container_generation != footer.container_generation
            || header.layout != footer.layout
            || intrinsic_summary != footer.intrinsic_summary
            || header.layout.footer_offset != expected_footer_offset
            || header.layout.file_length != actual_length
        {
            return Err(FormatError::HeaderFooterMismatch);
        }
        Ok((
            Self {
                header,
                container_hash: footer.container_hash,
            },
            intrinsic_summary,
        ))
    }

    #[must_use]
    pub const fn container_id(self) -> ContainerId {
        self.header.container_id
    }

    #[must_use]
    pub const fn container_generation(self) -> u64 {
        self.header.container_generation
    }

    #[must_use]
    pub const fn layout(self) -> ContainerLayout {
        self.header.layout
    }

    #[must_use]
    pub const fn container_hash(self) -> [u8; 32] {
        self.container_hash
    }

    /// Validates an untrusted independent Exact Index candidate against this
    /// Container envelope and returns the only record range that may be read.
    ///
    /// # Errors
    ///
    /// Rejects non-ACTIVE, dependent, mismatched, unaligned, overflowing, or
    /// out-of-record-region Locations. Codec-specific fields are paired again
    /// when the selected record is decoded.
    pub fn record_range(
        self,
        candidate: ExactIndexEntry,
    ) -> Result<ContainerRecordRange, FormatError> {
        let location = candidate.location();
        let record_length = usize::try_from(location.record_length())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let record_end = location
            .record_offset()
            .checked_add(u64::from(location.record_length()))
            .ok_or(FormatError::ArithmeticOverflow)?;
        if candidate.transition() != ExactLocationTransition::Active
            || location.container_id() != self.header.container_id
            || location.container_generation() != self.header.container_generation
            || location.record_offset() < u64::from(HEADER_BYTES_U16)
            || !location
                .record_offset()
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !(MIN_RAW_RECORD_BYTES..=MAX_RECORD_BYTES).contains(&record_length)
            || !record_length.is_multiple_of(usize::from(RECORD_ALIGNMENT))
            || record_end > self.header.layout.index_offset
            || !matches!(
                location.codec_id(),
                RAW_CODEC | ZSTD_CODEC | ZSTD_PREFIX_CODEC
            )
            || location.record_decoded_length() == 0
            || usize::try_from(location.record_decoded_length())
                .map_or(true, |length| length > MAX_DECODED_RECORD_BYTES)
            || location.record_payload_length() == 0
            || location.record_payload_length() > location.record_length()
            || location
                .decoded_offset()
                .checked_add(candidate.logical_length())
                .is_none_or(|end| end > location.record_decoded_length())
            || (location.codec_id() == ZSTD_PREFIX_CODEC && location.dependency_id() == [0; 32])
            || (location.codec_id() != ZSTD_PREFIX_CODEC && location.dependency_id() != [0; 32])
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        if location.codec_id() == RAW_CODEC
            && (location.chunk_ordinal() != 0
                || location.decoded_offset() != 0
                || location.record_decoded_length() != candidate.logical_length()
                || location.record_payload_length() != candidate.logical_length()
                || record_length
                    != raw_record_length(
                        usize::try_from(candidate.logical_length())
                            .map_err(|_| FormatError::ArithmeticOverflow)?,
                    )?)
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        Ok(ContainerRecordRange {
            offset: location.record_offset(),
            length: record_length,
        })
    }

    /// Fully validates one independent RAW or Zstd record selected by an
    /// Exact Index candidate and returns only its paired logical Chunk.
    ///
    /// # Errors
    ///
    /// Returns record structure, CRC, codec/coordinate, Chunk-ID, length, or
    /// candidate-pairing failures. No partial decoded bytes are returned.
    pub fn decode_candidate(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
    ) -> Result<RawRecord, FormatError> {
        let mut records = self.decode_candidates(&[candidate], record_bytes)?;
        records.pop().ok_or(FormatError::ExactLocationMismatch)
    }

    /// Fully validates several Exact candidates naming one independent
    /// Encoding Record while decoding that Record only once.
    ///
    /// Returned Chunks retain candidate order. Repeated candidates may clone
    /// only their already verified logical payload; ordinary distinct Chunk
    /// ordinals transfer ownership directly from the one decoded Record.
    ///
    /// # Errors
    ///
    /// Returns the same structural, coordinate, checksum, codec, Chunk-ID, and
    /// length failures as [`Self::decode_candidate`]. Every candidate must name
    /// the exact same physical Record and no partial result is returned.
    pub fn decode_candidates(
        self,
        candidates: &[ExactIndexEntry],
        record_bytes: &[u8],
    ) -> Result<Vec<RawRecord>, FormatError> {
        let verified = self.decode_candidate_payloads(candidates, record_bytes)?;
        Ok(verified
            .requested
            .into_iter()
            .map(RawRecord::from_verified_payload)
            .collect())
    }

    /// Fully validates one independent Encoding Record and returns both the
    /// requested Chunks and every verified sibling decoded with them.
    ///
    /// This is the bounded read-cache seam. Callers can retain all siblings
    /// without copying the shared decoded Record backing or recomputing Chunk
    /// identities.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::decode_candidates`].
    pub fn decode_candidate_payloads(
        self,
        candidates: &[ExactIndexEntry],
        record_bytes: &[u8],
    ) -> Result<VerifiedRecordPayloads, FormatError> {
        let Some(&first) = candidates.first() else {
            return Ok(VerifiedRecordPayloads {
                requested: Vec::new(),
                all: Vec::new(),
            });
        };
        let range = self.record_range(first)?;
        let first_location = first.location();
        if record_bytes.len() != range.length
            || get_u16(record_bytes, 12) != first_location.codec_id()
            || get_u32(record_bytes, 32) != first_location.record_length()
            || get_u32(record_bytes, 36) != first_location.record_decoded_length()
            || get_u32(record_bytes, 44) != first_location.record_payload_length()
            || get_u32(record_bytes, RECORD_CRC_OFFSET) != first_location.record_crc32c()
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        if first_location.codec_id() == ZSTD_PREFIX_CODEC {
            return Err(FormatError::ZstdPrefixBaseRequired);
        }
        let chunk_count = usize::try_from(get_u32(record_bytes, 56))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut ordinals = Vec::new();
        ordinals
            .try_reserve_exact(candidates.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for &candidate in candidates {
            if self.record_range(candidate)? != range {
                return Err(FormatError::ExactLocationMismatch);
            }
            let location = candidate.location();
            if location.codec_id() != first_location.codec_id()
                || location.record_decoded_length() != first_location.record_decoded_length()
                || location.record_payload_length() != first_location.record_payload_length()
                || location.record_crc32c() != first_location.record_crc32c()
                || location.dependency_id() != [0; 32]
            {
                return Err(FormatError::ExactLocationMismatch);
            }
            let ordinal = usize::try_from(location.chunk_ordinal())
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            if ordinal >= chunk_count {
                return Err(FormatError::ExactLocationMismatch);
            }
            let table_offset = RECORD_HEADER_BYTES
                .checked_add(
                    ordinal
                        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                        .ok_or(FormatError::ArithmeticOverflow)?,
                )
                .ok_or(FormatError::ArithmeticOverflow)?;
            let table_end = table_offset
                .checked_add(CHUNK_TABLE_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if table_end > record_bytes.len()
                || record_bytes[table_offset..table_offset + 32] != candidate.chunk_id().bytes()
                || get_u32(record_bytes, table_offset + 32) != location.decoded_offset()
                || get_u32(record_bytes, table_offset + 36) != candidate.logical_length()
            {
                return Err(FormatError::ExactLocationMismatch);
            }
            ordinals.push(ordinal);
        }

        let decoded = decode_encoding_record(record_bytes)?;
        let all = decoded
            .chunks
            .into_iter()
            .map(RawRecord::into_verified_payload)
            .collect::<Vec<_>>();
        let mut requested = Vec::new();
        requested
            .try_reserve_exact(candidates.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for (&candidate, ordinal) in candidates.iter().zip(ordinals) {
            let payload = all
                .get(ordinal)
                .ok_or(FormatError::ExactLocationMismatch)?
                .clone();
            if payload.chunk_id() != candidate.chunk_id()
                || usize::try_from(candidate.logical_length()) != Ok(payload.len())
            {
                return Err(FormatError::ExactLocationMismatch);
            }
            requested.push(payload);
        }
        Ok(VerifiedRecordPayloads { requested, all })
    }

    /// Fully validates one codec-3 Exact candidate using its resolved Base.
    ///
    /// The Base must be independently decoded and verified by the caller. This
    /// method pairs the Exact Location, durable dependency ID, record CRC,
    /// target table entry, Base bytes, and reconstructed target identity before
    /// returning the logical Chunk.
    ///
    /// # Errors
    ///
    /// Returns an Exact pairing, record, Base, codec, or integrity error.
    pub fn decode_zstd_prefix_candidate(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        base: &[u8],
    ) -> Result<RawRecord, FormatError> {
        self.decode_zstd_prefix_candidate_using(candidate, record_bytes, |bytes| {
            ZstdPrefixRecord::decode(bytes, base)
        })
    }

    /// Fully validates one codec-3 candidate while reusing the identity already
    /// proven by an independent Base decode.
    ///
    /// The target is still decompressed and rehashed. Only the redundant second
    /// full Base hash is replaced by an O(1) capability comparison.
    pub fn decode_zstd_prefix_candidate_with_verified_base(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        self.decode_zstd_prefix_candidate_using(candidate, record_bytes, |bytes| {
            ZstdPrefixRecord::decode_with_verified_base(bytes, base)
        })
    }

    fn decode_zstd_prefix_candidate_using(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<RawRecord, FormatError>,
    ) -> Result<RawRecord, FormatError> {
        let location = candidate.location();
        if location.codec_id() != ZSTD_PREFIX_CODEC {
            return Err(FormatError::ExactLocationMismatch);
        }
        let range = self.record_range(candidate)?;
        if record_bytes.len() != range.length
            || get_u16(record_bytes, 12) != location.codec_id()
            || get_u32(record_bytes, 32) != location.record_length()
            || get_u32(record_bytes, 36) != location.record_decoded_length()
            || get_u32(record_bytes, 44) != location.record_payload_length()
            || get_u32(record_bytes, RECORD_CRC_OFFSET) != location.record_crc32c()
            || record_bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]
                != candidate.chunk_id().bytes()
            || get_u32(record_bytes, RECORD_HEADER_BYTES + 32) != 0
            || get_u32(record_bytes, RECORD_HEADER_BYTES + 36) != candidate.logical_length()
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        let dependency = ZstdPrefixRecord::dependency(record_bytes)?;
        if dependency.chunk_id().bytes() != location.dependency_id()
            || dependency.logical_length() != candidate.logical_length()
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        let record = decode(record_bytes)?;
        if record.chunk_id() != candidate.chunk_id()
            || usize::try_from(candidate.logical_length()) != Ok(record.payload().len())
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        Ok(record)
    }

    /// Fully validates the one stored RAW record selected by an Exact Index
    /// candidate and rehashes its decoded Chunk before returning it.
    ///
    /// # Errors
    ///
    /// Returns record structure, CRC, Chunk-ID, logical-length, or candidate
    /// pairing failures. No partial payload is returned.
    pub fn decode_raw_candidate(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
    ) -> Result<RawRecord, FormatError> {
        if candidate.location().codec_id() != RAW_CODEC {
            return Err(FormatError::ExactLocationMismatch);
        }
        self.decode_candidate(candidate, record_bytes)
    }
}

impl ContainerRecoveryEnvelope {
    /// Pairs Header and Footer and retains only immutable recovery
    /// acceleration. No record payload or Recovery Index bytes are read by
    /// this operation.
    ///
    /// # Errors
    ///
    /// Returns an envelope error when identity, layout, checksums, summary, or
    /// recovery acceleration disagree.
    pub fn decode(
        header_bytes: &[u8],
        footer_bytes: &[u8],
        actual_length: u64,
    ) -> Result<Self, FormatError> {
        let descriptor =
            SealedContainerDescriptor::decode(header_bytes, footer_bytes, actual_length)?;
        Ok(Self { descriptor })
    }

    #[must_use]
    pub const fn container_id(&self) -> ContainerId {
        self.descriptor.container_id()
    }

    #[must_use]
    pub const fn container_generation(&self) -> u64 {
        self.descriptor.container_generation()
    }

    /// Returns the exact bounded range occupied by the Recovery Index.
    ///
    /// # Errors
    ///
    /// Returns overflow when the validated durable length cannot be represented
    /// by this process.
    pub fn recovery_index_range(&self) -> Result<ContainerRecordRange, FormatError> {
        let layout = self.descriptor.layout();
        Ok(ContainerRecordRange {
            offset: layout.index_offset,
            length: usize::try_from(layout.index_length)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        })
    }

    /// Authenticates and decodes the complete compact Recovery Index without
    /// reading record payloads.
    ///
    /// # Errors
    ///
    /// Returns an index length, CRC, order, or record-geometry mismatch.
    pub fn verify_recovery_index(
        &self,
        index_bytes: &[u8],
    ) -> Result<VerifiedRecoveryIndex, FormatError> {
        if index_bytes.len() != self.recovery_index_range()?.length {
            return Err(FormatError::InvalidRecoveryIndex);
        }
        let entries = decode_index(index_bytes, self.descriptor.layout().chunk_entry_count)?;
        if entries
            .iter()
            .any(|entry| !valid_recovery_index_entry_geometry(self.descriptor.layout(), *entry))
        {
            return Err(FormatError::InvalidRecoveryIndex);
        }
        Ok(VerifiedRecoveryIndex {
            descriptor: self.descriptor,
            entries,
        })
    }
}

impl VerifiedRecoveryIndex {
    /// Finds one dependency-free RAW/Zstd candidate for the requested Base.
    /// The returned record must still be read and passed to
    /// [`Self::decode_independent_candidate`].
    #[must_use]
    pub fn find_independent_candidate(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Option<RecoveryIndexCandidate> {
        let first = self
            .entries
            .partition_point(|entry| entry.chunk_id < chunk_id);
        self.entries[first..]
            .iter()
            .take_while(|entry| entry.chunk_id == chunk_id)
            .find(|entry| {
                entry.logical_length == logical_length
                    && matches!(entry.codec_id, RAW_CODEC | ZSTD_CODEC)
                    && entry.dependency_id == [0; 32]
            })
            .copied()
            .map(|entry| RecoveryIndexCandidate {
                container_id: self.descriptor.container_id(),
                container_generation: self.descriptor.container_generation(),
                entry,
            })
    }

    /// Fully validates the selected independent record and returns exactly the
    /// logical Chunk paired by the verified Recovery Index entry.
    ///
    /// # Errors
    ///
    /// Returns a candidate-pairing, record CRC, codec, Chunk-ID, or length
    /// error. No bytes escape before all checks complete.
    pub fn decode_independent_candidate(
        &self,
        candidate: RecoveryIndexCandidate,
        record_bytes: &[u8],
    ) -> Result<RawRecord, FormatError> {
        if candidate.container_id != self.descriptor.container_id()
            || candidate.container_generation != self.descriptor.container_generation()
            || self.entries.binary_search(&candidate.entry).is_err()
        {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        let entry = candidate.entry;
        let range = candidate.record_range()?;
        if record_bytes.len() != range.length
            || get_u16(record_bytes, 12) != entry.codec_id
            || get_u32(record_bytes, 32) != entry.record_length
            || get_u32(record_bytes, 36) != entry.record_decoded_length
            || get_u32(record_bytes, 44) != entry.record_payload_length
            || get_u32(record_bytes, RECORD_CRC_OFFSET) != entry.record_crc32c
        {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        let chunk_count = usize::try_from(get_u32(record_bytes, 56))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let ordinal =
            usize::try_from(entry.chunk_ordinal).map_err(|_| FormatError::ArithmeticOverflow)?;
        let table_offset = RECORD_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        let table_end = table_offset
            .checked_add(CHUNK_TABLE_ENTRY_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if ordinal >= chunk_count
            || table_end > record_bytes.len()
            || record_bytes[table_offset..table_offset + 32] != entry.chunk_id.bytes()
            || get_u32(record_bytes, table_offset + 32) != entry.decoded_offset
            || get_u32(record_bytes, table_offset + 36) != entry.logical_length
            || !matches!(entry.codec_id, RAW_CODEC | ZSTD_CODEC)
            || entry.dependency_id != [0; 32]
        {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        let decoded = decode_encoding_record(record_bytes)?;
        let record = decoded
            .chunks
            .into_iter()
            .nth(ordinal)
            .ok_or(FormatError::RecoveryIndexCandidateMismatch)?;
        if record.chunk_id() != entry.chunk_id
            || usize::try_from(entry.logical_length) != Ok(record.payload().len())
        {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        Ok(record)
    }
}

impl RecoveryIndexCandidate {
    /// Returns the bounded record range named by this verified Index
    /// candidate.
    ///
    /// # Errors
    ///
    /// Returns overflow when the validated durable record length cannot be
    /// represented by this process.
    pub fn record_range(self) -> Result<ContainerRecordRange, FormatError> {
        Ok(ContainerRecordRange {
            offset: self.entry.record_offset,
            length: usize::try_from(self.entry.record_length)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        })
    }
}

/// One prevalidated bounded record range inside a sealed Container.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerRecordRange {
    offset: u64,
    length: usize,
}

impl ContainerRecordRange {
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn length(self) -> usize {
        self.length
    }
}

impl SealedContainer {
    /// Encodes nonempty RAW chunks into one fully sealed container image.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid chunk sizes, layout overflow, or a container
    /// exceeding the v1 bounds.
    ///
    /// # Panics
    ///
    /// Panics if the preflight layout and the independently encoded record or
    /// index lengths disagree, which is an impossible internal writer state.
    pub fn encode(
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_with_writer_evidence(container_id, container_generation, chunks)
            .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes RAW Chunks and retains the Location evidence established by
    /// the writer's existing Chunk hashes and serialized layout.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode`].
    pub fn encode_with_writer_evidence(
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let mut encoded_records = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_records.push(RawRecord::encode(chunk)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes bounded multi-Chunk Compression Regions as independent Zstd
    /// records inside one fully sealed Container.
    ///
    /// The caller chooses complete region boundaries. This writer fixes the
    /// durable codec to Zstd level 3, verifies every chunk partition and
    /// identity, and emits a complete Recovery Index entry per logical Chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty Container or region, an invalid Chunk,
    /// decoded regions above 512 KiB, Zstd failure, layout overflow, or a
    /// Container above the v1 bound.
    pub fn encode_zstd_regions(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<Vec<u8>, FormatError> {
        if regions.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let mut encoded_records = Vec::new();
        encoded_records
            .try_reserve_exact(regions.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for region in regions {
            encoded_records.push(encode_zstd_record(region, ZSTD_LEVEL_V1)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes one codec-3 record per `(Base, target)` pair.
    ///
    /// Every pair must have equal nonzero logical lengths. Bases are named by
    /// BLAKE3 identity but are not copied into this Container. The caller must
    /// ensure each Base has an independently decodable durable Location before
    /// publishing the returned image.
    ///
    /// # Errors
    ///
    /// Returns a Prefix codec, length, allocation, layout, or Container error.
    pub fn encode_zstd_prefix_pairs(
        container_id: ContainerId,
        container_generation: u64,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        if pairs.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let mut encoded_records = Vec::new();
        encoded_records
            .try_reserve_exact(pairs.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for &(base, target) in pairs {
            encoded_records.push(ZstdPrefixRecord::encode(base, target)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes bounded regions using Zstd only when the complete encoded
    /// record saves at least 4 KiB and 3% versus independent RAW records.
    ///
    /// The comparison includes record headers, Chunk Tables, and record
    /// alignment. Recovery Index cost is identical per logical Chunk in both
    /// alternatives. Incompressible regions remain independently decodable
    /// RAW records.
    ///
    /// # Errors
    ///
    /// Returns the same region, codec, arithmetic, and Container layout errors
    /// as [`Self::encode_zstd_regions`].
    pub fn encode_adaptive_regions(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_adaptive_regions_parallel(
            container_id,
            container_generation,
            regions,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes independent Compression Regions on a bounded number of scoped
    /// workers, then merges their results in original region order.
    ///
    /// Workers own disjoint input ordinals and private output vectors, avoiding
    /// shared hot counters and cache-line contention. Runtime scheduling never
    /// changes logical or physical ordering, so one and many workers emit
    /// byte-identical Container images.
    ///
    /// # Errors
    ///
    /// Returns the same region, codec, arithmetic, allocation, and Container
    /// layout errors as [`Self::encode_adaptive_regions`].
    ///
    /// # Panics
    ///
    /// Panics if an encoding worker panics or returns a duplicate/missing
    /// ordinal. Both are impossible internal writer failures after preflight.
    pub fn encode_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes adaptive regions and returns runtime-only gate evidence.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode_adaptive_regions_parallel`].
    pub fn encode_adaptive_regions_parallel_profiled(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        Self::encode_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::V1,
        )
    }

    /// Encodes adaptive regions with an explicit benchmarkable gate policy.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode_adaptive_regions_parallel`].
    pub fn encode_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let prehashed = regions
            .iter()
            .map(|region| {
                region
                    .iter()
                    .map(|bytes| PrehashedChunk::new(ChunkId::of(bytes), bytes))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let prehashed_regions = prehashed.iter().map(Vec::as_slice).collect::<Vec<_>>();
        Self::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &prehashed_regions,
            workers,
            gate,
        )
    }

    /// Encodes adaptive Compression Regions from Chunk identities already
    /// computed by the ingest writer.
    ///
    /// The supplied identities are trusted writer evidence. Publication
    /// carries them forward without hashing the same resident bytes again.
    /// Every ordinary reader, recovery pass, and scrub recomputes the identity
    /// from independently read decoded bytes.
    ///
    /// # Errors
    ///
    /// Returns the same bounded region, codec, allocation, and layout errors as
    /// [`Self::encode_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker result ownership or ordering violates an internal
    /// writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_prehashed_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes prehashed adaptive regions and returns writer publication plus
    /// runtime-only gate evidence.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker ownership, deterministic ordering, or final gate
    /// accounting violates an internal writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel_profiled(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        Self::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::V1,
        )
    }

    /// Encodes prehashed regions with an explicit benchmarkable gate policy.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker ownership, deterministic ordering, or final gate
    /// accounting violates an internal writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|chunks| AdaptiveRegionInput {
                chunks,
                decoded: None,
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
        )
    }

    /// Encodes already contiguous prehashed regions without joining their
    /// decoded bytes into another temporary allocation.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    pub fn encode_prehashed_contiguous_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedContiguousRegion<'_>],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| AdaptiveRegionInput {
                chunks: region.chunks,
                decoded: Some(region.decoded),
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
        )
    }

    /// Encodes an ordered mixture of borrowed and already-materialized
    /// prehashed regions. Fragmented Chunks can avoid a second join while
    /// ordinary contiguous Chunks retain their low-memory borrowed path.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    pub fn encode_mixed_prehashed_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| match *region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => AdaptiveRegionInput {
                    chunks,
                    decoded: None,
                },
                PrehashedAdaptiveRegion::Contiguous(region) => AdaptiveRegionInput {
                    chunks: region.chunks,
                    decoded: Some(region.decoded),
                },
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
        )
    }

    /// Encodes prehashed partial Records together with byte-for-byte copied
    /// independent Records from verified Container images.
    ///
    /// The copied Record CRC and codec parameters are retained. The enclosing
    /// Container metadata and commitment are rebuilt for the new identity.
    ///
    /// # Errors
    ///
    /// Returns bounded format, allocation, compression, or worker errors.
    pub fn encode_prehashed_adaptive_regions_with_transplants_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        transplanted: Vec<PreparedEncodedRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|chunks| AdaptiveRegionInput {
                chunks,
                decoded: None,
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            transplanted,
            Vec::new(),
            Vec::new(),
            workers,
            gate,
        )
    }

    /// Prepares the best independent RAW/Zstd record for one prehashed Chunk.
    ///
    /// This is used only when the same Chunk will enter bounded Prefix trials:
    /// the winning independent fallback is retained, so a rejected dependent
    /// trial never causes a second Zstd encode.
    ///
    /// # Errors
    ///
    /// Returns bounded Chunk, codec, allocation, or record-layout failures.
    pub fn prepare_prehashed_independent_record(
        chunk: PrehashedChunk<'_>,
        gate: IncompressibilityGatePolicy,
    ) -> Result<PreparedIndependentRecord, FormatError> {
        let chunks = [chunk];
        let mut encoded = encode_adaptive_region(&chunks, gate)?;
        if encoded.records.len() != 1 {
            return Err(FormatError::InvalidContainerLayout);
        }
        let Some(record) = encoded.records.pop() else {
            return Err(FormatError::InvalidContainerLayout);
        };
        let record_length = record.record_length()?;
        let mut bytes = vec![0_u8; record_length];
        record.encode_into(&mut bytes)?;
        Ok(PreparedIndependentRecord { bytes })
    }

    /// Encodes independent adaptive regions together with already-compressed
    /// Depth-1 Prefix records in one Container image.
    ///
    /// Prefix frames are consumed and copied only once, directly into the
    /// final Container image. Their target identities are prior writer
    /// evidence; ordinary reads, recovery, and scrub independently decode and
    /// rehash every target.
    ///
    /// # Errors
    ///
    /// Returns the same bounded layout, codec, allocation, and worker errors
    /// as [`Self::encode_mixed_prehashed_adaptive_regions_parallel_profiled_with_gate`].
    pub fn encode_mixed_prehashed_reduction_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        independent: Vec<PreparedIndependentRecord>,
        prefixes: Vec<PreparedZstdPrefixRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| match *region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => AdaptiveRegionInput {
                    chunks,
                    decoded: None,
                },
                PrehashedAdaptiveRegion::Contiguous(region) => AdaptiveRegionInput {
                    chunks: region.chunks,
                    decoded: Some(region.decoded),
                },
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            independent,
            prefixes,
            workers,
            gate,
        )
    }

    fn encode_adaptive_region_inputs_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[AdaptiveRegionInput<'_>],
        transplanted: Vec<PreparedEncodedRecord>,
        independent: Vec<PreparedIndependentRecord>,
        prefixes: Vec<PreparedZstdPrefixRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        if regions.is_empty()
            && transplanted.is_empty()
            && independent.is_empty()
            && prefixes.is_empty()
        {
            return Err(FormatError::InvalidContainerLayout);
        }
        let worker_count = workers.get().min(regions.len().max(1));
        let encoded_by_region = (0..worker_count)
            .into_par_iter()
            .map(|worker| {
                let mut completed = Vec::new();
                for ordinal in (worker..regions.len()).step_by(worker_count) {
                    let input = regions[ordinal];
                    let encoded = if let Some(decoded) = input.decoded {
                        encode_adaptive_region_from_decoded(input.chunks, decoded, gate)?
                    } else {
                        encode_adaptive_region(input.chunks, gate)?
                    };
                    completed.push((ordinal, encoded));
                }
                Ok::<_, FormatError>(completed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoded_by_region = {
            let mut ordered = Vec::new();
            ordered
                .try_reserve_exact(regions.len())
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            ordered.resize_with(regions.len(), || None);
            for completed in encoded_by_region {
                for (ordinal, encoded) in completed {
                    assert!(
                        ordered[ordinal].replace(encoded).is_none(),
                        "ASSERT: each Compression Region has exactly one worker owner"
                    );
                }
            }
            Ok::<_, FormatError>(ordered)
        }?;
        let mut encoded_records = Vec::new();
        let mut gate_metrics = IncompressibilityGateMetrics::default();
        for region in encoded_by_region {
            let encoded =
                region.expect("ASSERT: every Compression Region worker must return its output");
            gate_metrics.checked_merge(encoded.metrics)?;
            encoded_records.extend(encoded.records);
        }
        encoded_records.extend(
            transplanted
                .into_iter()
                .map(AdaptiveRecordPlan::PreparedEncoded),
        );
        encoded_records.extend(
            independent
                .into_iter()
                .map(AdaptiveRecordPlan::PreparedIndependent),
        );
        encoded_records.extend(prefixes.into_iter().map(AdaptiveRecordPlan::ZstdPrefix));
        let encoding = encode_container_from_adaptive_plans(
            container_id,
            container_generation,
            encoded_records,
            workers,
        )?;
        assert_eq!(
            gate_metrics
                .disabled_regions
                .checked_add(gate_metrics.eligible_regions)
                .and_then(|total| total.checked_add(gate_metrics.size_bypassed_regions)),
            Some(regions.len()),
            "ASSERT: every adaptive region has exactly one gate disposition"
        );
        assert_eq!(
            gate_metrics
                .target_zstd_accepted
                .checked_add(gate_metrics.target_zstd_rejected)
                .and_then(|total| total.checked_add(gate_metrics.raw_regions_after_gate)),
            Some(regions.len()),
            "ASSERT: every adaptive region has exactly one final encoding disposition"
        );
        Ok(AdaptiveContainerEncoding {
            metrics: gate_metrics,
            ..encoding
        })
    }

    /// Fully validates and decodes one sealed container image.
    ///
    /// # Errors
    ///
    /// Returns the first structural, checksum, index, or content-integrity error.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        Self::decode_with_hash_workers(bytes, NonZeroUsize::MIN)
    }

    /// Fully validates one sealed Container. The worker argument remains part
    /// of the reader API for callers that share one bounded CPU budget; the v1
    /// structural commitment itself does not scan payload or spawn workers.
    ///
    /// # Errors
    ///
    /// Returns the same integrity error as [`Self::decode`].
    #[allow(clippy::too_many_lines)]
    pub fn decode_with_hash_workers(
        bytes: &[u8],
        permitted_workers: NonZeroUsize,
    ) -> Result<Self, FormatError> {
        Self::decode_with_resolver(bytes, permitted_workers, None)
    }

    /// Fully validates a sealed Container and resolves codec-3 Bases through
    /// one caller-supplied adapter.
    ///
    /// The format module validates each Prefix record CRC and dependency shape
    /// before invoking `resolve`. The returned bytes must match the requested
    /// Base identity and length; the codec verifies both again before target
    /// reconstruction.
    ///
    /// # Errors
    ///
    /// Returns the first Container, resolver, Base, or target-integrity error.
    pub fn decode_with_zstd_prefix_resolver(
        bytes: &[u8],
        resolve: &mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>,
    ) -> Result<Self, FormatError> {
        Self::decode_with_resolver(bytes, NonZeroUsize::MIN, Some(resolve))
    }

    #[allow(clippy::too_many_lines)]
    fn decode_with_resolver(
        bytes: &[u8],
        _permitted_workers: NonZeroUsize,
        mut resolve: Option<&mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>>,
    ) -> Result<Self, FormatError> {
        validate_container_file_length(bytes.len())?;
        let footer_offset = bytes
            .len()
            .checked_sub(FOOTER_BYTES_USIZE)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let footer = decode_footer(&bytes[footer_offset..])?;
        let (header, expected_intrinsic_summary) =
            ContainerHeader::decode_with_summary(&bytes[..HEADER_BYTES])?;
        if header.container_id != footer.container_id
            || header.container_generation != footer.container_generation
            || header.layout != footer.layout
            || expected_intrinsic_summary != footer.intrinsic_summary
            || usize::try_from(header.layout.footer_offset) != Ok(footer_offset)
            || usize::try_from(header.layout.file_length) != Ok(bytes.len())
        {
            return Err(FormatError::HeaderFooterMismatch);
        }
        let index_offset = usize::try_from(header.layout.index_offset)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let index_length = usize::try_from(header.layout.index_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let index_end = index_offset
            .checked_add(index_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if index_end > footer_offset {
            return Err(FormatError::InvalidContainerLayout);
        }

        let record_capacity = usize::try_from(header.layout.record_count)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut records = Vec::with_capacity(record_capacity);
        let mut expected_entries = Vec::with_capacity(record_capacity);
        let mut locations = Vec::with_capacity(record_capacity);
        let mut raw_locations = Vec::with_capacity(record_capacity);
        let mut raw_record_count = 0_usize;
        let mut zstd_record_count = 0_usize;
        let mut zstd_prefix_record_count = 0_usize;
        let mut intrinsic_summary =
            IntrinsicSummaryAccumulator::with_record_capacity(record_capacity)?;
        let mut cursor = HEADER_BYTES;
        for _ in 0..header.layout.record_count {
            let fixed_end = cursor
                .checked_add(RECORD_HEADER_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if fixed_end > index_offset {
                return Err(FormatError::InvalidContainerLayout);
            }
            let record_length = usize::try_from(get_u32(bytes, cursor + 32))
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            let end = cursor
                .checked_add(record_length)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if end > index_offset {
                return Err(FormatError::InvalidContainerLayout);
            }
            let encoded = &bytes[cursor..end];
            intrinsic_summary.observe_encoded_record(encoded)?;
            let decoded = if encoded.len() >= 14 && get_u16(encoded, 12) == ZSTD_PREFIX_CODEC {
                let dependency = ZstdPrefixRecord::dependency(encoded)?;
                let resolver = resolve
                    .as_deref_mut()
                    .ok_or(FormatError::ZstdPrefixBaseRequired)?;
                let base = resolver(dependency)?;
                let record = ZstdPrefixRecord::decode(encoded, &base)?;
                DecodedEncodingRecord {
                    codec: EncodingCodec::ZstdPrefix,
                    logical_bytes: u64::try_from(record.payload().len())
                        .map_err(|_| FormatError::ArithmeticOverflow)?,
                    chunks: vec![record],
                }
            } else {
                decode_encoding_record(encoded)?
            };
            let index_entries = IndexEntry::from_encoded_record(
                encoded,
                u64::try_from(cursor).map_err(|_| FormatError::ArithmeticOverflow)?,
            )?;
            for index_entry in &index_entries {
                locations.push(VerifiedChunkLocation {
                    chunk_id: index_entry.chunk_id,
                    logical_length: index_entry.logical_length,
                    container_id: header.container_id,
                    container_generation: header.container_generation,
                    record_offset: index_entry.record_offset,
                    record_length: index_entry.record_length,
                    chunk_ordinal: index_entry.chunk_ordinal,
                    decoded_offset: index_entry.decoded_offset,
                    codec_id: index_entry.codec_id,
                    dependency_id: index_entry.dependency_id,
                    record_crc32c: index_entry.record_crc32c,
                    record_decoded_length: index_entry.record_decoded_length,
                    record_payload_length: index_entry.record_payload_length,
                });
            }
            match decoded.codec {
                EncodingCodec::Raw => {
                    raw_record_count = raw_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                    let index_entry = index_entries.first().ok_or(FormatError::InvalidRawRecord)?;
                    raw_locations.push(VerifiedRawLocation {
                        chunk_id: index_entry.chunk_id,
                        logical_length: index_entry.logical_length,
                        container_id: header.container_id,
                        container_generation: header.container_generation,
                        record_offset: index_entry.record_offset,
                        record_length: index_entry.record_length,
                        record_crc32c: index_entry.record_crc32c,
                    });
                }
                EncodingCodec::Zstd => {
                    zstd_record_count = zstd_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                }
                EncodingCodec::ZstdPrefix => {
                    zstd_prefix_record_count = zstd_prefix_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                }
            }
            expected_entries.extend(index_entries);
            records.extend(decoded.chunks);
            cursor = end;
        }
        if cursor != index_offset {
            return Err(FormatError::InvalidContainerLayout);
        }
        if intrinsic_summary.finish(header.layout)? != expected_intrinsic_summary {
            return Err(FormatError::ContainerSummaryMismatch);
        }
        expected_entries.sort_unstable();
        let actual_entries = decode_index(
            &bytes[index_offset..index_end],
            header.layout.chunk_entry_count,
        )?;
        if actual_entries != expected_entries {
            return Err(FormatError::IndexRecordMismatch);
        }
        if bytes[index_end..footer_offset]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(FormatError::NonZeroContainerPadding);
        }
        let computed_hash = calculate_container_commitment(bytes, &header)?;
        if computed_hash != footer.container_hash {
            return Err(FormatError::ContainerHashMismatch);
        }
        Ok(Self {
            header,
            records,
            locations,
            raw_locations,
            raw_record_count,
            zstd_record_count,
            zstd_prefix_record_count,
        })
    }

    /// Fully validates a sealed Container for publication without retaining
    /// owned copies of its decoded logical Chunk payloads.
    ///
    /// RAW Chunk identities are hashed directly from the reread image. Zstd
    /// records retain only their one bounded decode buffer while their Chunk
    /// table is verified. The returned evidence is sufficient for Exact-Index
    /// publication and phase metrics, but it cannot serve file reads.
    ///
    /// # Errors
    ///
    /// Returns the same structural, checksum, index, or content-integrity
    /// errors as [`Self::decode_with_hash_workers`].
    #[allow(clippy::too_many_lines)]
    pub fn verify_publication_with_hash_workers(
        bytes: &[u8],
        _permitted_workers: NonZeroUsize,
    ) -> Result<VerifiedContainerPublication, FormatError> {
        Self::verify_publication(bytes, PublicationContainerProof::RecomputedHash, None)
    }

    /// Fully validates a Container without retaining logical payloads while
    /// resolving codec-3 Bases through one caller-owned adapter.
    ///
    /// # Errors
    ///
    /// Returns the first Container, resolver, Base, or target-integrity error.
    pub fn verify_publication_with_zstd_prefix_resolver(
        bytes: &[u8],
        resolve: &mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>,
    ) -> Result<VerifiedContainerPublication, FormatError> {
        Self::verify_publication(
            bytes,
            PublicationContainerProof::RecomputedHash,
            Some(resolve),
        )
    }

    /// Fully validates a publication reread against the exact sealed image
    /// produced and retained by the writer.
    ///
    /// Exact byte equality proves that the reread contains the structural
    /// commitment already computed during encoding, so this path does not
    /// recompute it. Record checksums, decoded Chunk identities, the
    /// Recovery Index, padding, and the Header/Footer envelope are still
    /// independently validated from `bytes`.
    ///
    /// `writer_image` must be the unmodified output of this crate's Container
    /// encoder. Recovery, scrub, and callers without that retained image must
    /// use [`Self::verify_publication_with_hash_workers`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::WriterImageMismatch`] when the reread differs
    /// from the retained writer image, or the first structural, checksum,
    /// index, or Chunk-integrity error found in the reread.
    pub fn verify_publication_against_writer_image(
        bytes: &[u8],
        writer_image: &[u8],
    ) -> Result<VerifiedContainerPublication, FormatError> {
        Self::verify_publication(
            bytes,
            PublicationContainerProof::ExactWriterImage(writer_image),
            None,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn verify_publication(
        bytes: &[u8],
        container_proof: PublicationContainerProof<'_>,
        mut resolve: Option<&mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>>,
    ) -> Result<VerifiedContainerPublication, FormatError> {
        if let PublicationContainerProof::ExactWriterImage(writer_image) = container_proof
            && bytes != writer_image
        {
            return Err(FormatError::WriterImageMismatch);
        }
        validate_container_file_length(bytes.len())?;
        let footer_offset = bytes
            .len()
            .checked_sub(FOOTER_BYTES_USIZE)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let footer = decode_footer(&bytes[footer_offset..])?;
        let (header, expected_intrinsic_summary) =
            ContainerHeader::decode_with_summary(&bytes[..HEADER_BYTES])?;
        if header.container_id != footer.container_id
            || header.container_generation != footer.container_generation
            || header.layout != footer.layout
            || expected_intrinsic_summary != footer.intrinsic_summary
            || usize::try_from(header.layout.footer_offset) != Ok(footer_offset)
            || usize::try_from(header.layout.file_length) != Ok(bytes.len())
        {
            return Err(FormatError::HeaderFooterMismatch);
        }
        let index_offset = usize::try_from(header.layout.index_offset)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let index_length = usize::try_from(header.layout.index_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let index_end = index_offset
            .checked_add(index_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if index_end > footer_offset {
            return Err(FormatError::InvalidContainerLayout);
        }

        let entry_capacity = usize::try_from(header.layout.chunk_entry_count)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut expected_entries = Vec::with_capacity(entry_capacity);
        let mut locations = Vec::with_capacity(entry_capacity);
        let mut raw_locations = Vec::with_capacity(
            usize::try_from(header.layout.record_count)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        let mut logical_bytes = 0_u64;
        let mut raw_record_count = 0_usize;
        let mut zstd_record_count = 0_usize;
        let mut zstd_prefix_record_count = 0_usize;
        let mut intrinsic_summary = IntrinsicSummaryAccumulator::with_record_capacity(
            usize::try_from(header.layout.record_count)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        )?;
        let mut cursor = HEADER_BYTES;
        for _ in 0..header.layout.record_count {
            let fixed_end = cursor
                .checked_add(RECORD_HEADER_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if fixed_end > index_offset {
                return Err(FormatError::InvalidContainerLayout);
            }
            let record_length = usize::try_from(get_u32(bytes, cursor + 32))
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            let end = cursor
                .checked_add(record_length)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if end > index_offset {
                return Err(FormatError::InvalidContainerLayout);
            }
            let encoded = &bytes[cursor..end];
            intrinsic_summary.observe_encoded_record(encoded)?;
            let decoded = if encoded.len() >= 14 && get_u16(encoded, 12) == ZSTD_PREFIX_CODEC {
                let dependency = ZstdPrefixRecord::dependency(encoded)?;
                let resolver = resolve
                    .as_deref_mut()
                    .ok_or(FormatError::ZstdPrefixBaseRequired)?;
                let base = resolver(dependency)?;
                let record = ZstdPrefixRecord::decode(encoded, &base)?;
                DecodedEncodingRecord {
                    codec: EncodingCodec::ZstdPrefix,
                    logical_bytes: u64::try_from(record.payload().len())
                        .map_err(|_| FormatError::ArithmeticOverflow)?,
                    chunks: Vec::new(),
                }
            } else {
                verify_encoding_record(encoded)?
            };
            logical_bytes = logical_bytes
                .checked_add(decoded.logical_bytes)
                .ok_or(FormatError::ArithmeticOverflow)?;
            let index_entries = IndexEntry::from_encoded_record(
                encoded,
                u64::try_from(cursor).map_err(|_| FormatError::ArithmeticOverflow)?,
            )?;
            for index_entry in &index_entries {
                locations.push(VerifiedChunkLocation {
                    chunk_id: index_entry.chunk_id,
                    logical_length: index_entry.logical_length,
                    container_id: header.container_id,
                    container_generation: header.container_generation,
                    record_offset: index_entry.record_offset,
                    record_length: index_entry.record_length,
                    chunk_ordinal: index_entry.chunk_ordinal,
                    decoded_offset: index_entry.decoded_offset,
                    codec_id: index_entry.codec_id,
                    dependency_id: index_entry.dependency_id,
                    record_crc32c: index_entry.record_crc32c,
                    record_decoded_length: index_entry.record_decoded_length,
                    record_payload_length: index_entry.record_payload_length,
                });
            }
            match decoded.codec {
                EncodingCodec::Raw => {
                    raw_record_count = raw_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                    let index_entry = index_entries.first().ok_or(FormatError::InvalidRawRecord)?;
                    raw_locations.push(VerifiedRawLocation {
                        chunk_id: index_entry.chunk_id,
                        logical_length: index_entry.logical_length,
                        container_id: header.container_id,
                        container_generation: header.container_generation,
                        record_offset: index_entry.record_offset,
                        record_length: index_entry.record_length,
                        record_crc32c: index_entry.record_crc32c,
                    });
                }
                EncodingCodec::Zstd => {
                    zstd_record_count = zstd_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                }
                EncodingCodec::ZstdPrefix => {
                    zstd_prefix_record_count = zstd_prefix_record_count
                        .checked_add(1)
                        .ok_or(FormatError::ArithmeticOverflow)?;
                }
            }
            expected_entries.extend(index_entries);
            cursor = end;
        }
        if cursor != index_offset {
            return Err(FormatError::InvalidContainerLayout);
        }
        if intrinsic_summary.finish(header.layout)? != expected_intrinsic_summary {
            return Err(FormatError::ContainerSummaryMismatch);
        }
        expected_entries.sort_unstable();
        let actual_entries = decode_index(
            &bytes[index_offset..index_end],
            header.layout.chunk_entry_count,
        )?;
        if actual_entries != expected_entries {
            return Err(FormatError::IndexRecordMismatch);
        }
        if bytes[index_end..footer_offset]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(FormatError::NonZeroContainerPadding);
        }
        if let PublicationContainerProof::RecomputedHash = container_proof {
            let computed_hash = calculate_container_commitment(bytes, &header)?;
            if computed_hash != footer.container_hash {
                return Err(FormatError::ContainerHashMismatch);
            }
        }
        Ok(VerifiedContainerPublication {
            header,
            locations,
            raw_locations,
            logical_bytes,
            raw_record_count,
            zstd_record_count,
            zstd_prefix_record_count,
        })
    }

    /// Returns the worker count used by the structural Container commitment.
    /// Payload is excluded, so v1 deliberately remains single-threaded.
    #[must_use]
    pub fn container_hash_worker_count(
        _file_length: usize,
        _permitted_workers: NonZeroUsize,
    ) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    #[must_use]
    pub const fn header(&self) -> &ContainerHeader {
        &self.header
    }

    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub const fn raw_record_count(&self) -> usize {
        self.raw_record_count
    }

    #[must_use]
    pub const fn zstd_record_count(&self) -> usize {
        self.zstd_record_count
    }

    #[must_use]
    pub const fn zstd_prefix_record_count(&self) -> usize {
        self.zstd_prefix_record_count
    }

    /// Returns fully verified decoded logical chunks in physical-record order.
    ///
    /// A multi-Chunk Zstd region contributes one item per Chunk Table entry.
    /// The chunks remain owned by this already validated immutable Container.
    #[must_use]
    pub fn records(&self) -> &[RawRecord] {
        &self.records
    }

    /// Returns physical independent Locations proven by this Container's
    /// complete Header, Record, Recovery-Index, Footer, CRC, decoded partition,
    /// and per-Chunk hash checks.
    ///
    /// The opaque evidence covers both RAW and dependency-free Zstd records and
    /// is suitable as Exact-Index rebuild or level-zero publication input.
    #[must_use]
    pub fn locations(&self) -> &[VerifiedChunkLocation] {
        &self.locations
    }

    /// Returns physical RAW Locations proven by this Container's complete
    /// Header, Record, Recovery-Index, Footer, CRC, hash, and Chunk-ID checks.
    ///
    /// The proof is suitable as rebuild input. An Exact Index lookup result is
    /// not equivalent evidence and must never construct this opaque type.
    #[must_use]
    pub fn raw_locations(&self) -> &[VerifiedRawLocation] {
        &self.raw_locations
    }

    #[must_use]
    pub fn chunk(&self, chunk_id: ChunkId) -> Option<&[u8]> {
        self.records
            .iter()
            .find(|record| record.chunk_id() == chunk_id)
            .map(RawRecord::payload)
    }

    /// Returns shared ownership of one logical Chunk already verified as part
    /// of this complete immutable Container decode.
    #[must_use]
    pub fn verified_chunk(&self, chunk_id: ChunkId) -> Option<VerifiedChunkPayload> {
        self.records
            .iter()
            .find(|record| record.chunk_id() == chunk_id)
            .map(|record| record.payload.clone())
    }
}

impl VerifiedContainerImage {
    /// Owns an image only after complete independent verification.
    ///
    /// # Errors
    ///
    /// Returns the same structural, checksum, or content-integrity error as
    /// [`SealedContainer::decode`].
    pub fn decode(bytes: Vec<u8>) -> Result<Self, FormatError> {
        let container = SealedContainer::decode(&bytes)?;
        Ok(Self { container, bytes })
    }

    /// Owns an image after complete verification with Depth-1 Prefix Base
    /// resolution.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`SealedContainer::decode_with_zstd_prefix_resolver`].
    pub fn decode_with_zstd_prefix_resolver(
        bytes: Vec<u8>,
        resolve: &mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>,
    ) -> Result<Self, FormatError> {
        let container = SealedContainer::decode_with_zstd_prefix_resolver(&bytes, resolve)?;
        Ok(Self { container, bytes })
    }

    #[must_use]
    pub const fn container(&self) -> &SealedContainer {
        &self.container
    }

    #[must_use]
    pub fn into_container(self) -> SealedContainer {
        self.container
    }

    /// Extracts one dependency-free RAW/Zstd Record from this verified image.
    /// Prefix Records are intentionally excluded because their durable Base
    /// closure is not automatically carried into a replacement Container.
    ///
    /// # Errors
    ///
    /// Rejects unknown offsets, dependent codecs, or inconsistent verified
    /// location geometry.
    pub fn prepare_encoded_record(
        &self,
        record_offset: u64,
    ) -> Result<PreparedEncodedRecord, FormatError> {
        let first = self
            .container
            .locations
            .iter()
            .find(|location| location.record_offset == record_offset)
            .ok_or(FormatError::InvalidContainerLayout)?;
        if first.dependency_id != [0; 32]
            || (first.codec_id != RAW_CODEC && first.codec_id != ZSTD_CODEC)
        {
            return Err(FormatError::InvalidContainerLayout);
        }
        let start = usize::try_from(record_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
        let length =
            usize::try_from(first.record_length).map_err(|_| FormatError::ArithmeticOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let bytes = self
            .bytes
            .get(start..end)
            .ok_or(FormatError::InvalidContainerLayout)?;
        let chunk_count = self
            .container
            .locations
            .iter()
            .filter(|location| location.record_offset == record_offset)
            .count();
        if chunk_count == 0
            || usize::try_from(get_u32(bytes, 56)) != Ok(chunk_count)
            || self.container.locations.iter().any(|location| {
                location.record_offset == record_offset
                    && (location.record_length != first.record_length
                        || location.codec_id != first.codec_id
                        || location.dependency_id != [0; 32])
            })
        {
            return Err(FormatError::InvalidContainerLayout);
        }
        Ok(PreparedEncodedRecord {
            bytes: bytes.to_vec(),
            chunk_count,
        })
    }
}

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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexEntry {
    chunk_id: ChunkId,
    logical_length: u32,
    record_offset: u64,
    chunk_ordinal: u32,
    decoded_offset: u32,
    record_length: u32,
    codec_id: u16,
    dependency_id: [u8; 32],
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
}

impl IndexEntry {
    fn from_encoded_record(bytes: &[u8], record_offset: u64) -> Result<Vec<Self>, FormatError> {
        let codec_id = get_u16(bytes, 12);
        let dependency_id = if codec_id == ZSTD_PREFIX_CODEC {
            bytes[64..96]
                .try_into()
                .expect("ASSERT: fixed dependency field is 32 bytes")
        } else {
            [0; 32]
        };
        let chunk_count =
            usize::try_from(get_u32(bytes, 56)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(chunk_count)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for chunk_ordinal in 0..chunk_count {
            let table_offset = RECORD_HEADER_BYTES
                .checked_add(
                    chunk_ordinal
                        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                        .ok_or(FormatError::ArithmeticOverflow)?,
                )
                .ok_or(FormatError::ArithmeticOverflow)?;
            let table_end = table_offset
                .checked_add(CHUNK_TABLE_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if table_end > bytes.len() {
                return Err(FormatError::InvalidRecoveryIndex);
            }
            let mut chunk_id = [0_u8; 32];
            chunk_id.copy_from_slice(&bytes[table_offset..table_offset + 32]);
            entries.push(Self {
                chunk_id: ChunkId(chunk_id),
                logical_length: get_u32(bytes, table_offset + 36),
                record_offset,
                chunk_ordinal: u32::try_from(chunk_ordinal)
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
                decoded_offset: get_u32(bytes, table_offset + 32),
                record_length: get_u32(bytes, 32),
                codec_id,
                dependency_id,
                record_crc32c: get_u32(bytes, RECORD_CRC_OFFSET),
                record_decoded_length: get_u32(bytes, 36),
                record_payload_length: get_u32(bytes, 44),
            });
        }
        Ok(entries)
    }

    fn encode(&self, output: &mut [u8]) {
        output[0..32].copy_from_slice(&self.chunk_id.0);
        put_u32(output, 32, self.logical_length);
        put_u32(output, 36, self.decoded_offset);
        put_u64(output, 40, self.record_offset);
        put_u32(output, 48, self.record_length);
        put_u32(output, 52, self.chunk_ordinal);
        put_u16(output, 56, self.codec_id);
        put_u16(output, 58, 0);
        put_u32(output, 60, self.record_crc32c);
        output[64..96].copy_from_slice(&self.dependency_id);
        put_u32(output, 96, self.record_decoded_length);
        put_u32(output, 100, self.record_payload_length);
    }

    fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() != INDEX_ENTRY_BYTES_USIZE {
            return Err(FormatError::InvalidRecoveryIndex);
        }
        let codec_id = get_u16(bytes, 56);
        let dependency_id: [u8; 32] = bytes[64..96]
            .try_into()
            .expect("ASSERT: fixed Recovery Index dependency field is 32 bytes");
        if !matches!(codec_id, RAW_CODEC | ZSTD_CODEC | ZSTD_PREFIX_CODEC)
            || get_u16(bytes, 58) != 0
            || (codec_id == ZSTD_PREFIX_CODEC && dependency_id == [0; 32])
            || (codec_id != ZSTD_PREFIX_CODEC && dependency_id != [0; 32])
            || bytes[104..].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::InvalidRecoveryIndex);
        }
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[0..32]);
        Ok(Self {
            chunk_id: ChunkId(chunk_id),
            logical_length: get_u32(bytes, 32),
            record_offset: get_u64(bytes, 40),
            chunk_ordinal: get_u32(bytes, 52),
            decoded_offset: get_u32(bytes, 36),
            record_length: get_u32(bytes, 48),
            codec_id,
            dependency_id,
            record_crc32c: get_u32(bytes, 60),
            record_decoded_length: get_u32(bytes, 96),
            record_payload_length: get_u32(bytes, 100),
        })
    }
}

#[derive(Clone, Copy)]
struct Footer {
    container_id: ContainerId,
    container_generation: u64,
    layout: ContainerLayout,
    intrinsic_summary: ContainerIntrinsicSummary,
    container_hash: [u8; 32],
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

/// One immutable Chunk payload paired with identity evidence computed earlier
/// in the writer pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrehashedChunk<'a> {
    chunk_id: ChunkId,
    bytes: &'a [u8],
}

/// One Compression Region whose Chunk views partition one existing contiguous
/// decoded buffer.
///
/// This form lets an ingest writer materialize fragmented request data exactly
/// once. Adaptive compression consumes `decoded` directly instead of joining
/// the same Chunks into another temporary vector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrehashedContiguousRegion<'a> {
    chunks: &'a [PrehashedChunk<'a>],
    decoded: &'a [u8],
}

/// One adaptive region supplied either as existing Chunk views or as a buffer
/// the caller already had to materialize from fragments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrehashedAdaptiveRegion<'a> {
    Borrowed(&'a [PrehashedChunk<'a>]),
    Contiguous(PrehashedContiguousRegion<'a>),
}

/// One independently decodable RAW/Zstd record prepared exactly once for a
/// Chunk that also entered bounded dependent-codec trials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedIndependentRecord {
    bytes: Vec<u8>,
}

/// One already verified, position-independent RAW/Zstd Encoding Record.
///
/// Only [`VerifiedContainerImage`] can produce this capability. The Container
/// builder copies the serialized fields byte-for-byte and constructs a new
/// Header, Recovery Index, structural commitment, and Footer around it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedEncodedRecord {
    bytes: Vec<u8>,
    chunk_count: usize,
}

impl PreparedIndependentRecord {
    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        get_u32(&self.bytes, 44) as usize
    }

    #[must_use]
    pub fn target_id(&self) -> ChunkId {
        let mut id = [0_u8; 32];
        id.copy_from_slice(&self.bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]);
        ChunkId::from_bytes(id)
    }
}

impl PreparedEncodedRecord {
    #[must_use]
    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn chunk_count(&self) -> usize {
        self.chunk_count
    }
}

impl<'a> PrehashedContiguousRegion<'a> {
    /// Proves that `chunks` are consecutive, complete views of `decoded`.
    ///
    /// # Errors
    ///
    /// Rejects an empty region, invalid Chunk lengths, a region above the
    /// durable decoded-size bound, or views that do not exactly partition the
    /// supplied buffer in order.
    pub fn new(chunks: &'a [PrehashedChunk<'a>], decoded: &'a [u8]) -> Result<Self, FormatError> {
        if chunks.is_empty() {
            return Err(FormatError::InvalidZstdRecord);
        }
        let mut offset = 0_usize;
        for chunk in chunks {
            validate_logical_chunk_length(chunk.bytes.len())?;
            let end = offset
                .checked_add(chunk.bytes.len())
                .ok_or(FormatError::ArithmeticOverflow)?;
            let expected = decoded
                .get(offset..end)
                .ok_or(FormatError::InvalidZstdRecord)?;
            if expected.as_ptr() != chunk.bytes.as_ptr() {
                return Err(FormatError::InvalidZstdRecord);
            }
            offset = end;
        }
        if offset != decoded.len() || offset > MAX_DECODED_RECORD_BYTES {
            return Err(FormatError::InvalidZstdRecord);
        }
        Ok(Self { chunks, decoded })
    }

    #[must_use]
    pub const fn chunks(self) -> &'a [PrehashedChunk<'a>] {
        self.chunks
    }

    #[must_use]
    pub const fn decoded(self) -> &'a [u8] {
        self.decoded
    }
}

impl<'a> PrehashedChunk<'a> {
    #[must_use]
    pub const fn new(chunk_id: ChunkId, bytes: &'a [u8]) -> Self {
        Self { chunk_id, bytes }
    }

    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EncodingCodec {
    Raw,
    Zstd,
    ZstdPrefix,
}

struct AdaptiveEncoderV1 {
    zstd: zstd::bulk::Compressor<'static>,
    scratch: Box<[u8]>,
}

struct EncodedAdaptiveRegion<'a> {
    records: Vec<AdaptiveRecordPlan<'a>>,
    metrics: IncompressibilityGateMetrics,
}

#[derive(Clone, Copy)]
struct AdaptiveRegionInput<'a> {
    chunks: &'a [PrehashedChunk<'a>],
    decoded: Option<&'a [u8]>,
}

enum AdaptiveRecordPlan<'a> {
    Raw(PrehashedChunk<'a>),
    Zstd {
        chunks: &'a [PrehashedChunk<'a>],
        decoded_length: usize,
        payload: Vec<u8>,
        level: i32,
    },
    PreparedEncoded(PreparedEncodedRecord),
    PreparedIndependent(PreparedIndependentRecord),
    ZstdPrefix(PreparedZstdPrefixRecord),
}

impl AdaptiveRecordPlan<'_> {
    fn record_length(&self) -> Result<usize, FormatError> {
        match self {
            Self::Raw(chunk) => raw_record_length(chunk.bytes.len()),
            Self::Zstd {
                chunks, payload, ..
            } => zstd_record_length(chunks.len(), payload.len()),
            Self::PreparedEncoded(record) => Ok(record.bytes.len()),
            Self::PreparedIndependent(record) => Ok(record.bytes.len()),
            Self::ZstdPrefix(record) => record.record_length(),
        }
    }

    fn chunk_count(&self) -> usize {
        match self {
            Self::Zstd { chunks, .. } => chunks.len(),
            Self::PreparedEncoded(record) => record.chunk_count,
            Self::Raw(_) | Self::PreparedIndependent(_) | Self::ZstdPrefix(_) => 1,
        }
    }

    fn observe_intrinsic_summary(
        &self,
        summary: &mut IntrinsicSummaryAccumulator,
    ) -> Result<(), FormatError> {
        match self {
            Self::Raw(chunk) => {
                summary.observe(RAW_CODEC, self.record_length()?, chunk.bytes.len(), 1, None)
            }
            Self::Zstd {
                chunks,
                decoded_length,
                ..
            } => summary.observe(
                ZSTD_CODEC,
                self.record_length()?,
                *decoded_length,
                chunks.len(),
                None,
            ),
            Self::PreparedEncoded(record) => summary.observe_encoded_record(&record.bytes),
            Self::PreparedIndependent(record) => summary.observe_encoded_record(&record.bytes),
            Self::ZstdPrefix(record) => summary.observe(
                ZSTD_PREFIX_CODEC,
                self.record_length()?,
                usize::try_from(record.logical_length)
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
                1,
                Some(record.dependency.chunk_id.bytes()),
            ),
        }
    }

    fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        match self {
            Self::Raw(chunk) => encode_prehashed_raw_record_into(*chunk, destination),
            Self::Zstd {
                chunks,
                decoded_length,
                payload,
                level,
            } => encode_prehashed_zstd_record_into(
                chunks,
                *decoded_length,
                payload,
                *level,
                destination,
            ),
            Self::PreparedEncoded(record) => {
                if destination.len() != record.bytes.len() {
                    return Err(FormatError::InvalidRecordLength(destination.len()));
                }
                destination.copy_from_slice(&record.bytes);
                Ok(())
            }
            Self::PreparedIndependent(record) => {
                if destination.len() != record.bytes.len() {
                    return Err(FormatError::InvalidRecordLength(destination.len()));
                }
                destination.copy_from_slice(&record.bytes);
                Ok(())
            }
            Self::ZstdPrefix(record) => record.encode_into(destination),
        }
    }
}

#[derive(Debug)]
struct DecodedEncodingRecord {
    codec: EncodingCodec,
    chunks: Vec<RawRecord>,
    logical_bytes: u64,
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region<'a>(
    region: &'a [PrehashedChunk<'a>],
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    if region.is_empty() {
        return Err(FormatError::InvalidZstdRecord);
    }
    if gate == IncompressibilityGatePolicy::Off {
        let decoded_length = prehashed_decoded_length(region)?;
        return encode_adaptive_region_from_input(region, None, decoded_length, gate);
    }
    let decoded = collect_prehashed_decoded(region)?;
    encode_adaptive_region_from_decoded(region, &decoded, gate)
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region_from_decoded<'a>(
    region: &'a [PrehashedChunk<'a>],
    decoded: &[u8],
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    let decoded_length = prehashed_decoded_length(region)?;
    if decoded.len() != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    encode_adaptive_region_from_input(region, Some(decoded), decoded_length, gate)
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region_from_input<'a>(
    region: &'a [PrehashedChunk<'a>],
    decoded: Option<&[u8]>,
    decoded_length: usize,
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    let raw_bytes = region.iter().try_fold(0_usize, |total, chunk| {
        total
            .checked_add(raw_record_length(chunk.bytes.len())?)
            .ok_or(FormatError::ArithmeticOverflow)
    })?;
    let payload_cap = useful_zstd_payload_cap(raw_bytes, region.len())?;
    let mut metrics = IncompressibilityGateMetrics {
        // The worker-local encoder owns one fixed-size destination buffer.
        // Report resident scratch, not merely the smaller cap passed to a
        // particular codec invocation.
        scratch_high_water_bytes: MAX_DECODED_RECORD_BYTES,
        ..IncompressibilityGateMetrics::default()
    };

    let should_try_target = if gate == IncompressibilityGatePolicy::Off {
        metrics.disabled_regions = 1;
        true
    } else if decoded_length < INCOMPRESSIBILITY_GATE_MIN_BYTES_V1 {
        metrics.size_bypassed_regions = 1;
        true
    } else {
        let decoded = decoded.ok_or(FormatError::InvalidZstdRecord)?;
        metrics.eligible_regions = 1;
        with_adaptive_encoder_v1(|encoder| {
            if encoder.lz4_fits(decoded, payload_cap)? {
                metrics.lz4_allowed_regions = 1;
                return Ok(true);
            }
            metrics.lz4_rejected_regions = 1;
            if gate == IncompressibilityGatePolicy::Lz4Only {
                return Ok(false);
            }
            if encoder.zstd_fits(decoded, ZSTD_RESCUE_LEVEL_V1, payload_cap)? {
                metrics.zstd1_allowed_regions = 1;
                Ok(true)
            } else {
                metrics.zstd1_rejected_regions = 1;
                Ok(false)
            }
        })?
    };

    if should_try_target {
        metrics.target_zstd_trials = 1;
        let payload = with_adaptive_encoder_v1(|encoder| match decoded {
            Some(decoded) => encoder.zstd_owned_payload(decoded, ZSTD_LEVEL_V1, payload_cap),
            None => encoder.zstd_fragmented_owned_payload(
                region,
                decoded_length,
                ZSTD_LEVEL_V1,
                payload_cap,
            ),
        })?;
        if let Some(payload) = payload {
            let record_length = zstd_record_length(region.len(), payload.len())?;
            assert!(
                zstd_record_wins(raw_bytes, record_length)?,
                "ASSERT: a payload bounded by the v1 useful cap must beat RAW"
            );
            metrics.target_zstd_accepted = 1;
            return Ok(EncodedAdaptiveRegion {
                records: vec![AdaptiveRecordPlan::Zstd {
                    chunks: region,
                    decoded_length,
                    payload,
                    level: ZSTD_LEVEL_V1,
                }],
                metrics,
            });
        }
        metrics.target_zstd_rejected = 1;
    } else {
        metrics.raw_regions_after_gate = 1;
    }

    let records = region
        .iter()
        .copied()
        .map(AdaptiveRecordPlan::Raw)
        .collect();
    Ok(EncodedAdaptiveRegion { records, metrics })
}

fn useful_zstd_payload_cap(raw_bytes: usize, chunks: usize) -> Result<usize, FormatError> {
    let percent_numerator = raw_bytes
        .checked_mul(
            usize::try_from(ZSTD_MINIMUM_SAVINGS_PERCENT_V1)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    let percentage_saving = percent_numerator
        .checked_add(99)
        .ok_or(FormatError::ArithmeticOverflow)?
        / 100;
    let required_saving = ZSTD_MINIMUM_SAVINGS_BYTES_V1.max(percentage_saving);
    let complete_cap = raw_bytes.saturating_sub(required_saving);
    let aligned_complete_cap = complete_cap - complete_cap % usize::from(RECORD_ALIGNMENT);
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(
            chunks
                .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    Ok(aligned_complete_cap.saturating_sub(payload_offset))
}

#[allow(clippy::too_many_lines)]
fn encode_zstd_record(chunks: &[&[u8]], level: i32) -> Result<Vec<u8>, FormatError> {
    let prehashed = chunks
        .iter()
        .map(|chunk| PrehashedChunk::new(ChunkId::of(chunk), chunk))
        .collect::<Vec<_>>();
    encode_prehashed_zstd_record(&prehashed, level)
}

#[allow(clippy::too_many_lines)]
fn encode_prehashed_zstd_record(
    chunks: &[PrehashedChunk<'_>],
    level: i32,
) -> Result<Vec<u8>, FormatError> {
    if chunks.is_empty() || level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let decoded = collect_prehashed_decoded(chunks)?;
    let payload = compress_zstd_v1(&decoded, level)?;
    encode_prehashed_zstd_record_from_payload(chunks, decoded.len(), &payload, level)
}

fn collect_prehashed_decoded(chunks: &[PrehashedChunk<'_>]) -> Result<Vec<u8>, FormatError> {
    let decoded_length = prehashed_decoded_length(chunks)?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_length)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    for chunk in chunks {
        decoded.extend_from_slice(chunk.bytes);
    }
    record_copy(CopyClass::CompressionRegionMaterialization, decoded.len());
    Ok(decoded)
}

fn prehashed_decoded_length(chunks: &[PrehashedChunk<'_>]) -> Result<usize, FormatError> {
    let mut decoded_length = 0_usize;
    for chunk in chunks {
        validate_logical_chunk_length(chunk.bytes.len())?;
        decoded_length = decoded_length
            .checked_add(chunk.bytes.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    if decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    Ok(decoded_length)
}

#[allow(clippy::too_many_lines)]
fn encode_prehashed_zstd_record_from_payload(
    chunks: &[PrehashedChunk<'_>],
    decoded_length: usize,
    payload: &[u8],
    level: i32,
) -> Result<Vec<u8>, FormatError> {
    let record_length = zstd_record_length(chunks.len(), payload.len())?;
    let mut bytes = vec![0_u8; record_length];
    encode_prehashed_zstd_record_into(chunks, decoded_length, payload, level, &mut bytes)?;
    Ok(bytes)
}

fn zstd_record_length(chunk_count: usize, payload_length: usize) -> Result<usize, FormatError> {
    let table_bytes = chunk_count
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_end = payload_offset
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

#[allow(clippy::too_many_lines)]
fn encode_prehashed_zstd_record_into(
    chunks: &[PrehashedChunk<'_>],
    decoded_length: usize,
    payload: &[u8],
    level: i32,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    if chunks.is_empty() || level != ZSTD_LEVEL_V1 || decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    let table_bytes = chunks
        .len()
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_end = payload_offset
        .checked_add(payload.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = zstd_record_length(chunks.len(), payload.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    bytes.fill(0);
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, ZSTD_CODEC);
    put_u16(bytes, 14, 0);
    put_u32(
        bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        36,
        u32::try_from(decoded_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        40,
        u32::try_from(payload_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        44,
        u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(
        bytes,
        56,
        u32::try_from(chunks.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    bytes[96..100].copy_from_slice(&level.to_le_bytes());

    let mut decoded_offset = 0_usize;
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let table_offset = RECORD_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        bytes[table_offset..table_offset + 32].copy_from_slice(&chunk.chunk_id.0);
        put_u32(
            bytes,
            table_offset + 32,
            u32::try_from(decoded_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        put_u32(
            bytes,
            table_offset + 36,
            u32::try_from(chunk.bytes.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        decoded_offset = decoded_offset
            .checked_add(chunk.bytes.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    bytes[payload_offset..payload_end].copy_from_slice(payload);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

fn compress_zstd_v1(decoded: &[u8], level: i32) -> Result<Vec<u8>, FormatError> {
    if level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    with_adaptive_encoder_v1(|encoder| {
        encoder
            .zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        encoder
            .zstd
            .compress(decoded)
            .map_err(|_| FormatError::ZstdFailure)
    })
}

fn with_adaptive_encoder_v1<T>(
    operation: impl FnOnce(&mut AdaptiveEncoderV1) -> Result<T, FormatError>,
) -> Result<T, FormatError> {
    ADAPTIVE_ENCODER_V1.with(|encoder| {
        let mut encoder = encoder.borrow_mut();
        if encoder.is_none() {
            *encoder = Some(AdaptiveEncoderV1::new()?);
        }
        operation(
            encoder
                .as_mut()
                .expect("ASSERT: worker-local adaptive encoder was initialized"),
        )
    })
}

impl AdaptiveEncoderV1 {
    fn new() -> Result<Self, FormatError> {
        let zstd =
            zstd::bulk::Compressor::new(ZSTD_LEVEL_V1).map_err(|_| FormatError::ZstdFailure)?;
        let scratch = vec![0_u8; MAX_DECODED_RECORD_BYTES].into_boxed_slice();
        Ok(Self { zstd, scratch })
    }

    fn lz4_fits(&mut self, decoded: &[u8], payload_cap: usize) -> Result<bool, FormatError> {
        if payload_cap == 0 {
            return Ok(false);
        }
        let output = self
            .scratch
            .get_mut(..payload_cap)
            .ok_or(FormatError::ArithmeticOverflow)?;
        match lz4::block::compress_to_buffer(decoded, None, false, output) {
            Ok(written) => {
                assert!(
                    written <= payload_cap,
                    "ASSERT: bounded LZ4 cannot exceed its destination"
                );
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::Other => Ok(false),
            Err(_) => Err(FormatError::CompressionGateFailure),
        }
    }

    fn zstd_fits(
        &mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<bool, FormatError> {
        Ok(self.zstd_payload(decoded, level, payload_cap)?.is_some())
    }

    fn zstd_payload<'a>(
        &'a mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<&'a [u8]>, FormatError> {
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        let output = self
            .scratch
            .get_mut(..payload_cap)
            .ok_or(FormatError::ArithmeticOverflow)?;
        match self.zstd.context_mut().compress2(output, decoded) {
            Ok(written) => {
                assert!(
                    written <= payload_cap,
                    "ASSERT: bounded Zstd cannot exceed its destination"
                );
                Ok(Some(&output[..written]))
            }
            Err(error)
                if zstd::zstd_safe::get_error_name(error) == "Destination buffer is too small" =>
            {
                Ok(None)
            }
            Err(_) => Err(FormatError::ZstdFailure),
        }
    }

    fn zstd_owned_payload(
        &mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<Vec<u8>>, FormatError> {
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(payload_cap)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        output.resize(payload_cap, 0);
        match self
            .zstd
            .context_mut()
            .compress2(output.as_mut_slice(), decoded)
        {
            Ok(written) => {
                assert!(
                    written <= payload_cap,
                    "ASSERT: bounded Zstd cannot exceed its owned destination"
                );
                output.truncate(written);
                Ok(Some(output))
            }
            Err(error)
                if zstd::zstd_safe::get_error_name(error) == "Destination buffer is too small" =>
            {
                Ok(None)
            }
            Err(_) => Err(FormatError::ZstdFailure),
        }
    }

    fn zstd_fragmented_owned_payload(
        &mut self,
        chunks: &[PrehashedChunk<'_>],
        decoded_length: usize,
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<Vec<u8>>, FormatError> {
        use zstd::zstd_safe::{InBuffer, OutBuffer};

        // The context keeps one Zstd frame open while each logical Chunk is
        // supplied as the next input slice. The pledged total binds the same
        // decoded length that is serialized into the Record header.
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .context_mut()
            .set_pledged_src_size(Some(
                u64::try_from(decoded_length).map_err(|_| FormatError::ArithmeticOverflow)?,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;

        let mut output = Vec::new();
        output
            .try_reserve_exact(payload_cap)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let written = {
            let mut output_buffer = OutBuffer::around(&mut output);
            for chunk in chunks {
                let mut input = InBuffer::around(chunk.bytes);
                while input.pos < input.src.len() {
                    let input_before = input.pos;
                    let output_before = output_buffer.pos();
                    self.zstd
                        .context_mut()
                        .compress_stream(&mut output_buffer, &mut input)
                        .map_err(|_| FormatError::ZstdFailure)?;
                    if input.pos == input_before && output_buffer.pos() == output_before {
                        return Err(FormatError::ZstdFailure);
                    }
                    // Zstd output is append-only. Once the useful-payload cap
                    // is full with input left, this frame cannot beat RAW.
                    if output_buffer.pos() == output_buffer.capacity()
                        && input.pos < input.src.len()
                    {
                        return Ok(None);
                    }
                }
            }
            loop {
                let remaining = self
                    .zstd
                    .context_mut()
                    .end_stream(&mut output_buffer)
                    .map_err(|_| FormatError::ZstdFailure)?;
                if remaining == 0 {
                    break;
                }
                if output_buffer.pos() == output_buffer.capacity() {
                    return Ok(None);
                }
            }
            output_buffer.pos()
        };
        assert_eq!(
            output.len(),
            written,
            "ASSERT: Zstd initializes exactly its reported output prefix"
        );
        Ok(Some(output))
    }
}

fn zstd_record_wins(raw_bytes: usize, zstd_bytes: usize) -> Result<bool, FormatError> {
    let Some(savings) = raw_bytes.checked_sub(zstd_bytes) else {
        return Ok(false);
    };
    if savings < ZSTD_MINIMUM_SAVINGS_BYTES_V1 {
        return Ok(false);
    }
    let raw = u128::try_from(raw_bytes).map_err(|_| FormatError::ArithmeticOverflow)?;
    let savings = u128::try_from(savings).map_err(|_| FormatError::ArithmeticOverflow)?;
    Ok(savings * 100 >= raw * ZSTD_MINIMUM_SAVINGS_PERCENT_V1)
}

#[allow(clippy::too_many_lines)]
fn decode_encoding_record(bytes: &[u8]) -> Result<DecodedEncodingRecord, FormatError> {
    decode_encoding_record_mode(bytes, true)
}

#[allow(clippy::too_many_lines)]
fn verify_encoding_record(bytes: &[u8]) -> Result<DecodedEncodingRecord, FormatError> {
    decode_encoding_record_mode(bytes, false)
}

#[allow(clippy::too_many_lines)]
fn decode_encoding_record_mode(
    bytes: &[u8],
    retain_payloads: bool,
) -> Result<DecodedEncodingRecord, FormatError> {
    if bytes.len() < MIN_RAW_RECORD_BYTES
        || bytes.len() > MAX_RECORD_BYTES
        || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
    {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    if &bytes[0..8] != RECORD_MAGIC {
        return Err(FormatError::InvalidRecordMagic);
    }
    let declared_length =
        usize::try_from(get_u32(bytes, 32)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if declared_length != bytes.len() {
        return Err(FormatError::InvalidRecordLength(declared_length));
    }
    let stored_checksum = get_u32(bytes, RECORD_CRC_OFFSET);
    if crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET) != stored_checksum {
        return Err(FormatError::RecordChecksumMismatch);
    }
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || (get_u16(bytes, 12) != ZSTD_PREFIX_CODEC && bytes[64..96].iter().any(|byte| *byte != 0))
    {
        return Err(FormatError::InvalidZstdRecord);
    }
    if get_u16(bytes, 12) == RAW_CODEC {
        let (chunk_id, payload) = decode_raw_record_view(bytes)?;
        let logical_bytes =
            u64::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
        let chunks = if retain_payloads {
            vec![RawRecord {
                payload: VerifiedChunkPayload::from_owned(chunk_id, payload.to_vec()),
            }]
        } else {
            Vec::new()
        };
        return Ok(DecodedEncodingRecord {
            codec: EncodingCodec::Raw,
            chunks,
            logical_bytes,
        });
    }
    if get_u16(bytes, 12) == ZSTD_PREFIX_CODEC {
        validate_zstd_prefix_record(bytes)?;
        return Err(FormatError::ZstdPrefixBaseRequired);
    }
    if get_u16(bytes, 12) != ZSTD_CODEC
        || i32::from_le_bytes(
            bytes[96..100]
                .try_into()
                .expect("ASSERT: fixed codec parameter range is four bytes"),
        ) != ZSTD_LEVEL_V1
        || bytes[100..128].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdRecord);
    }

    let decoded_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if decoded_length == 0 || decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    let chunk_count =
        usize::try_from(get_u32(bytes, 56)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if chunk_count == 0 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let table_bytes = chunk_count
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset =
        usize::try_from(get_u32(bytes, 40)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_offset != expected_payload_offset || payload_offset > bytes.len() {
        return Err(FormatError::InvalidZstdRecord);
    }
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length == 0 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let payload_end = payload_offset
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if payload_end > bytes.len()
        || expected_record_length != bytes.len()
        || bytes[payload_end..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdRecord);
    }

    let decoded = zstd::bulk::decompress(&bytes[payload_offset..payload_end], decoded_length)
        .map_err(|_| FormatError::ZstdFailure)?;
    if decoded.len() != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    let mut verified_chunks = Vec::new();
    verified_chunks
        .try_reserve_exact(chunk_count)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut expected_decoded_offset = 0_usize;
    for ordinal in 0..chunk_count {
        let table_offset = RECORD_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        let decoded_offset = usize::try_from(get_u32(bytes, table_offset + 32))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let logical_length = usize::try_from(get_u32(bytes, table_offset + 36))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        validate_logical_chunk_length(logical_length)?;
        let decoded_end = decoded_offset
            .checked_add(logical_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if decoded_offset != expected_decoded_offset
            || decoded_end > decoded.len()
            || get_u64(bytes, table_offset + 40) != 0
            || get_u64(bytes, table_offset + 48) != 0
            || get_u64(bytes, table_offset + 56) != 0
        {
            return Err(FormatError::InvalidZstdRecord);
        }
        let mut stored_id = [0_u8; 32];
        stored_id.copy_from_slice(&bytes[table_offset..table_offset + 32]);
        let payload = &decoded[decoded_offset..decoded_end];
        let chunk_id = ChunkId::of(payload);
        if chunk_id.0 != stored_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        if retain_payloads {
            verified_chunks.push((chunk_id, decoded_offset, logical_length));
        }
        expected_decoded_offset = decoded_end;
    }
    if expected_decoded_offset != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    let chunks = if retain_payloads {
        let backing = Arc::new(decoded);
        verified_chunks
            .into_iter()
            .map(|(chunk_id, offset, length)| {
                Ok(RawRecord {
                    payload: VerifiedChunkPayload::from_shared(
                        chunk_id,
                        Arc::clone(&backing),
                        offset,
                        length,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, FormatError>>()?
    } else {
        Vec::new()
    };
    Ok(DecodedEncodingRecord {
        codec: EncodingCodec::Zstd,
        chunks,
        logical_bytes: u64::try_from(decoded_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?,
    })
}

/// One verified logical dependency named by a Zstd Prefix record.
///
/// The reference contains no physical Location. A reader must resolve it to
/// an independently decodable Chunk before calling [`ZstdPrefixRecord::decode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZstdPrefixDependency {
    chunk_id: ChunkId,
    logical_length: u32,
}

impl ZstdPrefixDependency {
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }
}

/// One already-compressed codec-3 record awaiting Container assembly.
///
/// The opaque value carries prior writer evidence and owns its Zstd frame.
/// Moving it into a Container therefore avoids a second compression and a
/// temporary encoded-record copy in the ingest hot loop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedZstdPrefixRecord {
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: Box<[u8]>,
}

impl PreparedZstdPrefixRecord {
    fn record_length(&self) -> Result<usize, FormatError> {
        zstd_prefix_record_length(self.logical_length, self.frame.len())
    }

    fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        encode_zstd_prefix_record_from_frame_into(
            self.dependency,
            self.target_id,
            self.logical_length,
            &self.frame,
            destination,
        )
    }

    /// Returns the dependency-plus-frame bytes used by the v1 cost policy.
    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        32 + self.frame.len()
    }

    #[must_use]
    pub const fn target_id(&self) -> ChunkId {
        self.target_id
    }
}

/// Field-by-field codec-3 Encoding Record for one Depth-1 Zstd Prefix target.
///
/// The fixed header stores the Base Chunk ID and length. The one-entry Chunk
/// Table stores the target identity and length. The payload is one Zstd frame
/// encoded against the exact Base bytes. Neither Rust memory layout nor a
/// physical Base Location is serialized.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZstdPrefixRecord;

impl ZstdPrefixRecord {
    /// Wraps a Prefix frame produced by an earlier bounded trial without
    /// compressing or hashing the target again.
    ///
    /// # Errors
    ///
    /// Rejects invalid lengths, empty frames, or impossible record geometry.
    pub fn prepare_precompressed(
        base_id: ChunkId,
        logical_length: u32,
        target_id: ChunkId,
        frame: Box<[u8]>,
    ) -> Result<PreparedZstdPrefixRecord, FormatError> {
        if base_id == ChunkId::from_bytes([0; 32]) {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }
        let dependency = ZstdPrefixDependency {
            chunk_id: base_id,
            logical_length,
        };
        zstd_prefix_record_length(logical_length, frame.len())?;
        Ok(PreparedZstdPrefixRecord {
            dependency,
            target_id,
            logical_length,
            frame,
        })
    }

    /// Encodes one same-length target against a verified Base byte slice.
    ///
    /// This function emits a record but does not decide whether Prefix beats
    /// RAW, independent Zstd, or another Delta codec. The caller owns that
    /// versioned physical-cost decision.
    ///
    /// # Errors
    ///
    /// Returns a length, allocation, arithmetic, or Zstd error.
    ///
    /// # Panics
    ///
    /// Panics only if Zstd reports writing beyond the destination supplied to
    /// it, an internal codec-contract violation.
    pub fn encode(base: &[u8], target: &[u8]) -> Result<Vec<u8>, FormatError> {
        validate_logical_chunk_length(base.len())?;
        validate_logical_chunk_length(target.len())?;
        if base.len() != target.len() {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }

        let mut frame = Vec::new();
        frame
            .try_reserve_exact(zstd::zstd_safe::compress_bound(target.len()))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        frame.resize(zstd::zstd_safe::compress_bound(target.len()), 0);
        let mut context = zstd::zstd_safe::CCtx::try_create().ok_or(FormatError::ZstdFailure)?;
        context
            .set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(
                ZSTD_PREFIX_LEVEL_V1,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .set_pledged_src_size(Some(
                u64::try_from(target.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .ref_prefix(base)
            .map_err(|_| FormatError::ZstdFailure)?;
        let written = context
            .compress2(frame.as_mut_slice(), target)
            .map_err(|_| FormatError::ZstdFailure)?;
        assert!(
            written <= frame.len(),
            "ASSERT: Zstd Prefix cannot report bytes beyond its destination"
        );
        frame.truncate(written);
        if frame.is_empty() {
            return Err(FormatError::ZstdFailure);
        }

        encode_zstd_prefix_record_from_frame(
            ZstdPrefixDependency {
                chunk_id: ChunkId::of(base),
                logical_length: u32::try_from(base.len())
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
            },
            ChunkId::of(target),
            u32::try_from(target.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
            &frame,
        )
    }

    /// Validates record structure and CRC before returning its logical Base.
    ///
    /// This method does not decode the target. It is safe to use for planning
    /// a bounded Base lookup because malformed dependency metadata is rejected
    /// before the ID escapes.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, length, or codec error.
    pub fn dependency(bytes: &[u8]) -> Result<ZstdPrefixDependency, FormatError> {
        validate_zstd_prefix_record(bytes)?;
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[64..96]);
        Ok(ZstdPrefixDependency {
            chunk_id: ChunkId::from_bytes(chunk_id),
            logical_length: get_u32(bytes, 100),
        })
    }

    /// Decodes and verifies the exact target against resolved Base bytes.
    ///
    /// The reader checks Base length and BLAKE3 identity before asking Zstd to
    /// decode. It then checks decoded length and target BLAKE3 before returning
    /// any logical bytes.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, Zstd, decoded-length, or target-integrity error.
    pub fn decode(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || ChunkId::of(base) != dependency.chunk_id
        {
            return Err(FormatError::ZstdPrefixBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base)
    }

    /// Decodes one target using a Base whose length and BLAKE3 identity were
    /// already established by the independent record verifier.
    ///
    /// # Errors
    ///
    /// Returns a Base pairing, record, Zstd, decoded-length, or target-integrity
    /// error. The target identity is always recomputed.
    pub fn decode_with_verified_base(
        bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || dependency.chunk_id != base.chunk_id()
        {
            return Err(FormatError::ZstdPrefixBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base.as_slice())
    }

    fn decode_after_base_verification(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let decoded_length =
            usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_offset =
            usize::try_from(get_u32(bytes, 40)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_length =
            usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_end = payload_offset
            .checked_add(payload_length)
            .ok_or(FormatError::ArithmeticOverflow)?;

        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(decoded_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        decoded.resize(decoded_length, 0);
        let mut context = zstd::zstd_safe::DCtx::try_create().ok_or(FormatError::ZstdFailure)?;
        context
            .ref_prefix(base)
            .map_err(|_| FormatError::ZstdFailure)?;
        let written = context
            .decompress(decoded.as_mut_slice(), &bytes[payload_offset..payload_end])
            .map_err(|_| FormatError::ZstdFailure)?;
        if written != decoded_length {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }

        let mut target_id = [0_u8; 32];
        target_id.copy_from_slice(&bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]);
        let target_id = ChunkId::from_bytes(target_id);
        if ChunkId::of(&decoded) != target_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        Ok(RawRecord {
            payload: VerifiedChunkPayload::from_owned(target_id, decoded),
        })
    }
}

fn encode_zstd_prefix_record_from_frame(
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: &[u8],
) -> Result<Vec<u8>, FormatError> {
    let record_length = zstd_prefix_record_length(logical_length, frame.len())?;
    let mut bytes = vec![0_u8; record_length];
    encode_zstd_prefix_record_from_frame_into(
        dependency,
        target_id,
        logical_length,
        frame,
        &mut bytes,
    )?;
    Ok(bytes)
}

fn zstd_prefix_record_length(
    logical_length: u32,
    frame_length: usize,
) -> Result<usize, FormatError> {
    validate_logical_chunk_length(
        usize::try_from(logical_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    )?;
    if frame_length == 0 {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_offset = RAW_PAYLOAD_OFFSET;
    let payload_end = payload_offset
        .checked_add(frame_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

fn encode_zstd_prefix_record_from_frame_into(
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: &[u8],
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    if dependency.logical_length != logical_length {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let record_length = zstd_prefix_record_length(logical_length, frame.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    let payload_offset = RAW_PAYLOAD_OFFSET;
    let payload_end = payload_offset
        .checked_add(frame.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    bytes.fill(0);
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, ZSTD_PREFIX_CODEC);
    put_u32(
        bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 36, logical_length);
    put_u32(bytes, 40, RAW_PAYLOAD_OFFSET_U32);
    put_u32(
        bytes,
        44,
        u32::try_from(frame.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(bytes, 56, 1);
    bytes[64..96].copy_from_slice(&dependency.chunk_id.0);
    bytes[96..100].copy_from_slice(&ZSTD_PREFIX_LEVEL_V1.to_le_bytes());
    put_u32(bytes, 100, dependency.logical_length);
    bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32].copy_from_slice(&target_id.0);
    put_u32(bytes, RECORD_HEADER_BYTES + 36, logical_length);
    bytes[payload_offset..payload_end].copy_from_slice(frame);
    let checksum = crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

fn validate_zstd_prefix_record(bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() < MIN_RAW_RECORD_BYTES
        || bytes.len() > MAX_RECORD_BYTES
        || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
        || &bytes[0..8] != RECORD_MAGIC
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 12) != ZSTD_PREFIX_CODEC
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 32)) != Ok(bytes.len())
        || usize::try_from(get_u32(bytes, 40)) != Ok(RAW_PAYLOAD_OFFSET)
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || get_u32(bytes, 56) != 1
        || i32::from_le_bytes(
            bytes[96..100]
                .try_into()
                .expect("ASSERT: fixed Prefix level field is four bytes"),
        ) != ZSTD_PREFIX_LEVEL_V1
        || bytes[104..128].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    if crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET) != get_u32(bytes, RECORD_CRC_OFFSET) {
        return Err(FormatError::RecordChecksumMismatch);
    }
    let logical_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    validate_logical_chunk_length(logical_length)?;
    if get_u32(bytes, 100) != get_u32(bytes, 36)
        || bytes[64..96].iter().all(|byte| *byte == 0)
        || get_u32(bytes, RECORD_HEADER_BYTES + 32) != 0
        || get_u32(bytes, RECORD_HEADER_BYTES + 36) != get_u32(bytes, 36)
        || bytes[RECORD_HEADER_BYTES + 40..RAW_PAYLOAD_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length == 0 {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    if payload_end > bytes.len()
        || align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))? != bytes.len()
        || bytes[payload_end..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    Ok(())
}

/// One decoded logical Chunk whose complete stored Encoding Record and BLAKE3
/// identity were independently verified.
///
/// Multi-Chunk records share one backing allocation. The private constructors
/// keep this type as verification evidence rather than a caller-supplied claim.
#[derive(Clone)]
pub struct VerifiedChunkPayload {
    chunk_id: ChunkId,
    backing: Arc<Vec<u8>>,
    offset: usize,
    length: usize,
}

impl VerifiedChunkPayload {
    fn from_owned(chunk_id: ChunkId, bytes: Vec<u8>) -> Self {
        let length = bytes.len();
        Self {
            chunk_id,
            backing: Arc::new(bytes),
            offset: 0,
            length,
        }
    }

    fn from_shared(
        chunk_id: ChunkId,
        backing: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
    ) -> Result<Self, FormatError> {
        let end = offset
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if end > backing.len() {
            return Err(FormatError::InvalidZstdRecord);
        }
        Ok(Self {
            chunk_id,
            backing,
            offset,
            length,
        })
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Offset of this logical Chunk inside its verified decoded Record
    /// backing. Exact-location waiters use it to pair a shared Singleflight
    /// result with the requested Chunk-table coordinate in O(1).
    #[must_use]
    pub const fn decoded_offset(&self) -> usize {
        self.offset
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.backing[self.offset..self.offset + self.length]
    }

    /// Returns the one allocation retained by this payload and every sibling
    /// view from the same Encoding Record.
    #[must_use]
    pub fn backing_allocation_bytes(&self) -> usize {
        self.backing.capacity()
    }

    #[must_use]
    pub fn shares_backing_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.backing, &other.backing)
    }

    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        if self.offset == 0 && self.length == self.backing.len() {
            match Arc::try_unwrap(self.backing) {
                Ok(bytes) => bytes,
                Err(backing) => backing.as_slice().to_vec(),
            }
        } else {
            self.as_slice().to_vec()
        }
    }
}

impl fmt::Debug for VerifiedChunkPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedChunkPayload")
            .field("chunk_id", &self.chunk_id)
            .field("length", &self.length)
            .field("backing_bytes", &self.backing.capacity())
            .finish_non_exhaustive()
    }
}

impl PartialEq for VerifiedChunkPayload {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_id == other.chunk_id && self.as_slice() == other.as_slice()
    }
}

impl Eq for VerifiedChunkPayload {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRecord {
    payload: VerifiedChunkPayload,
}

impl RawRecord {
    /// Encodes one nonempty logical chunk as a v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns an error when the chunk or resulting record exceeds v1 bounds.
    pub fn encode(payload: &[u8]) -> Result<Vec<u8>, FormatError> {
        Self::encode_prehashed(PrehashedChunk::new(ChunkId::of(payload), payload))
    }

    fn encode_prehashed(chunk: PrehashedChunk<'_>) -> Result<Vec<u8>, FormatError> {
        let record_length = raw_record_length(chunk.bytes.len())?;
        let mut bytes = vec![0_u8; record_length];
        encode_prehashed_raw_record_into(chunk, &mut bytes)?;
        Ok(bytes)
    }

    /// Validates and decodes one v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, or logical Chunk-ID integrity error.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let (chunk_id, payload) = decode_raw_record_view(bytes)?;
        Ok(Self {
            payload: VerifiedChunkPayload::from_owned(chunk_id, payload.to_vec()),
        })
    }

    fn from_verified_payload(payload: VerifiedChunkPayload) -> Self {
        Self { payload }
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.payload.chunk_id()
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload.into_payload()
    }

    #[must_use]
    pub fn into_verified_payload(self) -> VerifiedChunkPayload {
        self.payload
    }

    #[must_use]
    pub fn verified_payload(&self) -> VerifiedChunkPayload {
        self.payload.clone()
    }
}

fn encode_prehashed_raw_record_into(
    chunk: PrehashedChunk<'_>,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    let payload = chunk.bytes;
    let record_length = raw_record_length(payload.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    bytes.fill(0);
    let record_length_u32 =
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let payload_length_u32 =
        u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, RAW_CODEC);
    put_u16(bytes, 14, 0);
    put_u32(bytes, 32, record_length_u32);
    put_u32(bytes, 36, payload_length_u32);
    put_u32(bytes, 40, RAW_PAYLOAD_OFFSET_U32);
    put_u32(bytes, 44, payload_length_u32);
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(bytes, 56, 1);

    bytes[128..160].copy_from_slice(&chunk.chunk_id.0);
    put_u32(bytes, 160, 0);
    put_u32(bytes, 164, payload_length_u32);
    bytes[RAW_PAYLOAD_OFFSET..RAW_PAYLOAD_OFFSET + payload.len()].copy_from_slice(payload);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

fn decode_raw_record_view(bytes: &[u8]) -> Result<(ChunkId, &[u8]), FormatError> {
    if bytes.len() < MIN_RAW_RECORD_BYTES
        || bytes.len() > MAX_RECORD_BYTES
        || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
    {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    if &bytes[0..8] != RECORD_MAGIC {
        return Err(FormatError::InvalidRecordMagic);
    }
    let declared_length =
        usize::try_from(get_u32(bytes, 32)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if declared_length != bytes.len() {
        return Err(FormatError::InvalidRecordLength(declared_length));
    }
    let stored_checksum = get_u32(bytes, RECORD_CRC_OFFSET);
    if crc32c_with_zeroed_u32(bytes, RECORD_CRC_OFFSET) != stored_checksum {
        return Err(FormatError::RecordChecksumMismatch);
    }
    validate_raw_record_constants(bytes)?;

    let decoded_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    validate_logical_chunk_length(decoded_length)?;
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    let logical_length =
        usize::try_from(get_u32(bytes, 164)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length != decoded_length || logical_length != decoded_length {
        return Err(FormatError::InvalidRawRecord);
    }
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if expected_record_length != bytes.len() || bytes[payload_end..].iter().any(|byte| *byte != 0) {
        return Err(FormatError::InvalidRawRecord);
    }

    let mut stored_id = [0_u8; 32];
    stored_id.copy_from_slice(&bytes[128..160]);
    let payload = &bytes[RAW_PAYLOAD_OFFSET..payload_end];
    let chunk_id = ChunkId::of(payload);
    if chunk_id.0 != stored_id {
        return Err(FormatError::ChunkHashMismatch);
    }
    Ok((chunk_id, payload))
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

/// Exact lifetime-invariant geometry used to rank one immutable Container for
/// GC without reading its record region or Recovery Index.
///
/// The summary deliberately contains no liveness, reference-count, pin, or
/// retirement state. Header and Footer carry identical field-by-field copies;
/// a complete verifier additionally derives the same values from the records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContainerIntrinsicSummary {
    raw_record_count: u32,
    zstd_record_count: u32,
    zstd_prefix_record_count: u32,
    independent_chunk_count: u32,
    dependent_chunk_count: u32,
    raw_encoded_bytes: u64,
    zstd_encoded_bytes: u64,
    zstd_prefix_encoded_bytes: u64,
    raw_decoded_bytes: u64,
    zstd_decoded_bytes: u64,
    zstd_prefix_decoded_bytes: u64,
    single_chunk_record_count: u32,
    multi_chunk_record_count: u32,
    outgoing_dependency_edges: u32,
    unique_outgoing_base_ids: u32,
}

struct IntrinsicSummaryAccumulator {
    summary: ContainerIntrinsicSummary,
    outgoing_base_ids: Vec<[u8; 32]>,
}

impl IntrinsicSummaryAccumulator {
    fn with_record_capacity(record_capacity: usize) -> Result<Self, FormatError> {
        let mut outgoing_base_ids = Vec::new();
        outgoing_base_ids
            .try_reserve_exact(record_capacity)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        Ok(Self {
            summary: ContainerIntrinsicSummary::default(),
            outgoing_base_ids,
        })
    }

    fn observe(
        &mut self,
        codec_id: u16,
        encoded_bytes: usize,
        decoded_bytes: usize,
        chunk_count: usize,
        dependency_id: Option<[u8; 32]>,
    ) -> Result<(), FormatError> {
        if decoded_bytes == 0 || chunk_count == 0 {
            return Err(FormatError::InvalidContainerSummary);
        }
        let encoded_bytes =
            u64::try_from(encoded_bytes).map_err(|_| FormatError::ArithmeticOverflow)?;
        let decoded_bytes =
            u64::try_from(decoded_bytes).map_err(|_| FormatError::ArithmeticOverflow)?;
        let chunk_count =
            u32::try_from(chunk_count).map_err(|_| FormatError::ArithmeticOverflow)?;
        if chunk_count == 1 {
            self.summary.single_chunk_record_count = self
                .summary
                .single_chunk_record_count
                .checked_add(1)
                .ok_or(FormatError::ArithmeticOverflow)?;
        } else {
            self.summary.multi_chunk_record_count = self
                .summary
                .multi_chunk_record_count
                .checked_add(1)
                .ok_or(FormatError::ArithmeticOverflow)?;
        }
        match (codec_id, dependency_id) {
            (RAW_CODEC, None) => {
                self.summary.raw_record_count = self
                    .summary
                    .raw_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.independent_chunk_count = self
                    .summary
                    .independent_chunk_count
                    .checked_add(chunk_count)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.raw_encoded_bytes = self
                    .summary
                    .raw_encoded_bytes
                    .checked_add(encoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.raw_decoded_bytes = self
                    .summary
                    .raw_decoded_bytes
                    .checked_add(decoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            (ZSTD_CODEC, None) => {
                self.summary.zstd_record_count = self
                    .summary
                    .zstd_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.independent_chunk_count = self
                    .summary
                    .independent_chunk_count
                    .checked_add(chunk_count)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.zstd_encoded_bytes = self
                    .summary
                    .zstd_encoded_bytes
                    .checked_add(encoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.zstd_decoded_bytes = self
                    .summary
                    .zstd_decoded_bytes
                    .checked_add(decoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            (ZSTD_PREFIX_CODEC, Some(base_id)) if base_id != [0; 32] && chunk_count == 1 => {
                self.summary.zstd_prefix_record_count = self
                    .summary
                    .zstd_prefix_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.dependent_chunk_count = self
                    .summary
                    .dependent_chunk_count
                    .checked_add(chunk_count)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.zstd_prefix_encoded_bytes = self
                    .summary
                    .zstd_prefix_encoded_bytes
                    .checked_add(encoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.zstd_prefix_decoded_bytes = self
                    .summary
                    .zstd_prefix_decoded_bytes
                    .checked_add(decoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.outgoing_dependency_edges = self
                    .summary
                    .outgoing_dependency_edges
                    .checked_add(chunk_count)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.outgoing_base_ids.push(base_id);
            }
            _ => return Err(FormatError::InvalidContainerSummary),
        }
        Ok(())
    }

    fn observe_encoded_record(&mut self, record: &[u8]) -> Result<(), FormatError> {
        if record.len() < RECORD_HEADER_BYTES
            || usize::try_from(get_u32(record, 32)) != Ok(record.len())
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        let codec_id = get_u16(record, 12);
        let dependency_id = if codec_id == ZSTD_PREFIX_CODEC {
            Some(
                record[64..96]
                    .try_into()
                    .expect("ASSERT: fixed Prefix dependency field is 32 bytes"),
            )
        } else {
            None
        };
        self.observe(
            codec_id,
            record.len(),
            usize::try_from(get_u32(record, 36)).map_err(|_| FormatError::ArithmeticOverflow)?,
            usize::try_from(get_u32(record, 56)).map_err(|_| FormatError::ArithmeticOverflow)?,
            dependency_id,
        )
    }

    fn finish(mut self, layout: ContainerLayout) -> Result<ContainerIntrinsicSummary, FormatError> {
        self.outgoing_base_ids.sort_unstable();
        self.outgoing_base_ids.dedup();
        self.summary.unique_outgoing_base_ids = u32::try_from(self.outgoing_base_ids.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        self.summary.validate(layout)?;
        Ok(self.summary)
    }
}

impl ContainerIntrinsicSummary {
    /// Returns the CRC32C identity stored by rebuildable GC catalog rows.
    ///
    /// The checksum detects a row derived from a different immutable summary;
    /// it remains acceleration metadata and is not deletion authority.
    #[must_use]
    pub fn structural_checksum(self) -> u32 {
        let mut bytes = [0_u8; CONTAINER_SUMMARY_BYTES];
        self.encode(&mut bytes);
        crc32c::crc32c(&bytes)
    }

    /// Conservative bytes needed to materialize every logical Chunk as one
    /// independently decodable RAW record.
    ///
    /// # Errors
    ///
    /// Returns overflow if the summary cannot be represented by the bound.
    pub fn raw_replacement_upper_bound(self) -> Result<u64, FormatError> {
        let logical_bytes = self
            .raw_decoded_bytes
            .checked_add(self.zstd_decoded_bytes)
            .and_then(|bytes| bytes.checked_add(self.zstd_prefix_decoded_bytes))
            .ok_or(FormatError::ArithmeticOverflow)?;
        let chunk_count = u64::from(
            self.independent_chunk_count
                .checked_add(self.dependent_chunk_count)
                .ok_or(FormatError::ArithmeticOverflow)?,
        );
        logical_bytes
            .checked_add(
                chunk_count
                    .checked_mul(255)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)
    }

    #[must_use]
    pub const fn raw_record_count(self) -> u32 {
        self.raw_record_count
    }

    #[must_use]
    pub const fn zstd_record_count(self) -> u32 {
        self.zstd_record_count
    }

    #[must_use]
    pub const fn zstd_prefix_record_count(self) -> u32 {
        self.zstd_prefix_record_count
    }

    #[must_use]
    pub const fn independent_chunk_count(self) -> u32 {
        self.independent_chunk_count
    }

    #[must_use]
    pub const fn dependent_chunk_count(self) -> u32 {
        self.dependent_chunk_count
    }

    #[must_use]
    pub const fn raw_encoded_bytes(self) -> u64 {
        self.raw_encoded_bytes
    }

    #[must_use]
    pub const fn zstd_encoded_bytes(self) -> u64 {
        self.zstd_encoded_bytes
    }

    #[must_use]
    pub const fn zstd_prefix_encoded_bytes(self) -> u64 {
        self.zstd_prefix_encoded_bytes
    }

    #[must_use]
    pub const fn raw_decoded_bytes(self) -> u64 {
        self.raw_decoded_bytes
    }

    #[must_use]
    pub const fn zstd_decoded_bytes(self) -> u64 {
        self.zstd_decoded_bytes
    }

    #[must_use]
    pub const fn zstd_prefix_decoded_bytes(self) -> u64 {
        self.zstd_prefix_decoded_bytes
    }

    #[must_use]
    pub const fn single_chunk_record_count(self) -> u32 {
        self.single_chunk_record_count
    }

    #[must_use]
    pub const fn multi_chunk_record_count(self) -> u32 {
        self.multi_chunk_record_count
    }

    #[must_use]
    pub const fn outgoing_dependency_edges(self) -> u32 {
        self.outgoing_dependency_edges
    }

    #[must_use]
    pub const fn unique_outgoing_base_ids(self) -> u32 {
        self.unique_outgoing_base_ids
    }

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            CONTAINER_SUMMARY_BYTES,
            "ASSERT: intrinsic summary always occupies its fixed durable extent"
        );
        output.fill(0);
        put_u16(output, 0, 1);
        put_u16(output, 2, 0);
        put_u32(output, 4, self.raw_record_count);
        put_u32(output, 8, self.zstd_record_count);
        put_u32(output, 12, self.zstd_prefix_record_count);
        put_u32(output, 16, self.independent_chunk_count);
        put_u32(output, 20, self.dependent_chunk_count);
        put_u64(output, 24, self.raw_encoded_bytes);
        put_u64(output, 32, self.zstd_encoded_bytes);
        put_u64(output, 40, self.zstd_prefix_encoded_bytes);
        put_u64(output, 48, self.raw_decoded_bytes);
        put_u64(output, 56, self.zstd_decoded_bytes);
        put_u64(output, 64, self.zstd_prefix_decoded_bytes);
        put_u32(output, 72, self.single_chunk_record_count);
        put_u32(output, 76, self.multi_chunk_record_count);
        put_u32(output, 80, self.outgoing_dependency_edges);
        put_u32(output, 84, self.unique_outgoing_base_ids);
    }

    fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() != CONTAINER_SUMMARY_BYTES
            || get_u16(input, 0) != 1
            || get_u16(input, 2) != 0
            || input[88..].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        Ok(Self {
            raw_record_count: get_u32(input, 4),
            zstd_record_count: get_u32(input, 8),
            zstd_prefix_record_count: get_u32(input, 12),
            independent_chunk_count: get_u32(input, 16),
            dependent_chunk_count: get_u32(input, 20),
            raw_encoded_bytes: get_u64(input, 24),
            zstd_encoded_bytes: get_u64(input, 32),
            zstd_prefix_encoded_bytes: get_u64(input, 40),
            raw_decoded_bytes: get_u64(input, 48),
            zstd_decoded_bytes: get_u64(input, 56),
            zstd_prefix_decoded_bytes: get_u64(input, 64),
            single_chunk_record_count: get_u32(input, 72),
            multi_chunk_record_count: get_u32(input, 76),
            outgoing_dependency_edges: get_u32(input, 80),
            unique_outgoing_base_ids: get_u32(input, 84),
        })
    }

    fn validate(self, layout: ContainerLayout) -> Result<(), FormatError> {
        let record_count = self
            .raw_record_count
            .checked_add(self.zstd_record_count)
            .and_then(|count| count.checked_add(self.zstd_prefix_record_count))
            .ok_or(FormatError::ArithmeticOverflow)?;
        let chunk_count = self
            .independent_chunk_count
            .checked_add(self.dependent_chunk_count)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let geometry_records = self
            .single_chunk_record_count
            .checked_add(self.multi_chunk_record_count)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let encoded_bytes = self
            .raw_encoded_bytes
            .checked_add(self.zstd_encoded_bytes)
            .and_then(|bytes| bytes.checked_add(self.zstd_prefix_encoded_bytes))
            .ok_or(FormatError::ArithmeticOverflow)?;
        let expected_encoded_bytes = layout
            .index_offset
            .checked_sub(u64::try_from(HEADER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if record_count != layout.record_count
            || geometry_records != layout.record_count
            || chunk_count != layout.chunk_entry_count
            || encoded_bytes != expected_encoded_bytes
            || self.dependent_chunk_count != self.outgoing_dependency_edges
            || self.zstd_prefix_record_count != self.outgoing_dependency_edges
            || self.unique_outgoing_base_ids > self.outgoing_dependency_edges
            || (self.unique_outgoing_base_ids == 0) != (self.outgoing_dependency_edges == 0)
            || self.raw_record_count > self.independent_chunk_count
            || (self.raw_encoded_bytes == 0) != (self.raw_record_count == 0)
            || (self.zstd_encoded_bytes == 0) != (self.zstd_record_count == 0)
            || (self.zstd_prefix_encoded_bytes == 0) != (self.zstd_prefix_record_count == 0)
            || (self.raw_decoded_bytes == 0) != (self.raw_record_count == 0)
            || (self.zstd_decoded_bytes == 0) != (self.zstd_record_count == 0)
            || (self.zstd_prefix_decoded_bytes == 0) != (self.zstd_prefix_record_count == 0)
            || !self
                .raw_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !self
                .zstd_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !self
                .zstd_prefix_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerHeader {
    container_id: ContainerId,
    container_generation: u64,
    layout: ContainerLayout,
}

impl ContainerHeader {
    /// Constructs a sealed header after validating all layout equations.
    ///
    /// # Errors
    ///
    /// Returns an error for zero generation, overflow, or an invalid layout.
    fn sealed(
        container_id: ContainerId,
        container_generation: u64,
        layout: ContainerLayout,
    ) -> Result<Self, FormatError> {
        if container_generation == 0 {
            return Err(FormatError::ZeroContainerGeneration);
        }
        validate_layout(layout)?;
        Ok(Self {
            container_id,
            container_generation,
            layout,
        })
    }

    fn encode(&self, intrinsic_summary: ContainerIntrinsicSummary) -> [u8; HEADER_BYTES] {
        let mut bytes = [0_u8; HEADER_BYTES];
        bytes[0..8].copy_from_slice(HEADER_MAGIC);
        put_u16(&mut bytes, 8, CONTAINER_FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, SEALED_STATE);
        put_u16(&mut bytes, 14, CRC32C_ALGORITHM);
        put_u16(&mut bytes, 16, BLAKE3_256_ALGORITHM);
        put_u16(&mut bytes, 18, BLAKE3_STRUCTURAL_COMMITMENT_ALGORITHM);
        put_u16(&mut bytes, 20, RECORD_ALIGNMENT);
        bytes[40..56].copy_from_slice(&self.container_id.0);
        put_u64(&mut bytes, 56, self.container_generation);
        put_u32(&mut bytes, 64, self.layout.record_count);
        put_u32(&mut bytes, 68, self.layout.chunk_entry_count);
        put_u64(&mut bytes, 72, self.layout.index_offset);
        put_u64(&mut bytes, 80, self.layout.index_length);
        put_u64(&mut bytes, 88, self.layout.footer_offset);
        put_u64(&mut bytes, 96, self.layout.file_length);
        intrinsic_summary.encode(
            &mut bytes[HEADER_SUMMARY_OFFSET..HEADER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES],
        );
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, HEADER_CRC_OFFSET, checksum);
        bytes
    }

    /// Validates and decodes a published, sealed container header.
    ///
    /// # Errors
    ///
    /// Returns a structural or checksum error, including for a BUILDING header.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        Self::decode_with_summary(bytes).map(|(header, _summary)| header)
    }

    fn decode_with_summary(bytes: &[u8]) -> Result<(Self, ContainerIntrinsicSummary), FormatError> {
        if bytes.len() != HEADER_BYTES {
            return Err(FormatError::InvalidHeaderLength(bytes.len()));
        }
        if &bytes[0..8] != HEADER_MAGIC {
            return Err(FormatError::InvalidHeaderMagic);
        }
        let stored_checksum = get_u32(bytes, HEADER_CRC_OFFSET);
        if crc32c_with_zeroed_u32(bytes, HEADER_CRC_OFFSET) != stored_checksum {
            return Err(FormatError::HeaderChecksumMismatch);
        }
        if get_u16(bytes, 12) == 1 {
            return Err(FormatError::ContainerNotSealed);
        }
        validate_header_constants(bytes)?;
        if bytes[22..24] != [0; 2]
            || bytes[24..40] != [0; 16]
            || bytes[108..HEADER_SUMMARY_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
            || bytes[HEADER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES..]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(FormatError::NonZeroHeaderReserved);
        }

        let mut id = [0_u8; 16];
        id.copy_from_slice(&bytes[40..56]);
        let container_id = ContainerId::new(id)?;
        let generation = get_u64(bytes, 56);
        let layout = ContainerLayout {
            record_count: get_u32(bytes, 64),
            chunk_entry_count: get_u32(bytes, 68),
            index_offset: get_u64(bytes, 72),
            index_length: get_u64(bytes, 80),
            footer_offset: get_u64(bytes, 88),
            file_length: get_u64(bytes, 96),
        };
        let intrinsic_summary = ContainerIntrinsicSummary::decode(
            &bytes[HEADER_SUMMARY_OFFSET..HEADER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES],
        )?;
        let header = Self::sealed(container_id, generation, layout)?;
        intrinsic_summary.validate(layout)?;
        Ok((header, intrinsic_summary))
    }

    #[must_use]
    pub const fn container_id(&self) -> ContainerId {
        self.container_id
    }

    #[must_use]
    pub const fn container_generation(&self) -> u64 {
        self.container_generation
    }

    #[must_use]
    pub const fn layout(&self) -> ContainerLayout {
        self.layout
    }
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

fn validate_header_constants(bytes: &[u8]) -> Result<(), FormatError> {
    if get_u16(bytes, 8) != CONTAINER_FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != HEADER_BYTES
        || get_u16(bytes, 12) != SEALED_STATE
        || get_u16(bytes, 14) != CRC32C_ALGORITHM
        || get_u16(bytes, 16) != BLAKE3_256_ALGORITHM
        || get_u16(bytes, 18) != BLAKE3_STRUCTURAL_COMMITMENT_ALGORITHM
        || get_u16(bytes, 20) != RECORD_ALIGNMENT
    {
        return Err(FormatError::UnsupportedHeaderField);
    }
    Ok(())
}

fn validate_layout(layout: ContainerLayout) -> Result<(), FormatError> {
    if layout.record_count == 0
        || layout.chunk_entry_count == 0
        || layout.record_count > layout.chunk_entry_count
        || layout.index_offset < HEADER_BYTES as u64
        || !layout
            .index_offset
            .is_multiple_of(u64::from(RECORD_ALIGNMENT))
        || !layout.footer_offset.is_multiple_of(FOOTER_BYTES)
        || layout.file_length > MAX_CONTAINER_BYTES
        || !layout.file_length.is_multiple_of(FOOTER_BYTES)
    {
        return Err(FormatError::InvalidContainerLayout);
    }

    let entries_length = u64::from(layout.chunk_entry_count)
        .checked_mul(INDEX_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let minimum_records_length = u64::from(layout.record_count)
        .checked_mul(
            u64::try_from(MIN_RAW_RECORD_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    let minimum_index_offset = u64::try_from(HEADER_BYTES)
        .map_err(|_| FormatError::ArithmeticOverflow)?
        .checked_add(minimum_records_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_index_length = INDEX_HEADER_BYTES
        .checked_add(entries_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let index_end = layout
        .index_offset
        .checked_add(layout.index_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_footer_offset = align_up(index_end, FOOTER_BYTES)?;
    let expected_file_length = layout
        .footer_offset
        .checked_add(FOOTER_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;

    if layout.index_offset < minimum_index_offset
        || layout.index_length != expected_index_length
        || layout.footer_offset != expected_footer_offset
        || layout.file_length != expected_file_length
    {
        return Err(FormatError::InvalidContainerLayout);
    }
    Ok(())
}

fn validate_container_file_length(length: usize) -> Result<(), FormatError> {
    let minimum = HEADER_BYTES
        .checked_add(FOOTER_BYTES_USIZE)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let maximum =
        usize::try_from(MAX_CONTAINER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?;
    if length < minimum || length > maximum || !length.is_multiple_of(FOOTER_BYTES_USIZE) {
        return Err(FormatError::InvalidContainerLength(length));
    }
    Ok(())
}

fn valid_recovery_index_entry_geometry(layout: ContainerLayout, entry: IndexEntry) -> bool {
    let Ok(record_length) = usize::try_from(entry.record_length) else {
        return false;
    };
    entry.logical_length != 0
        && usize::try_from(entry.logical_length)
            .is_ok_and(|length| length <= MAX_LOGICAL_CHUNK_BYTES)
        && entry.record_offset >= HEADER_BYTES as u64
        && entry
            .record_offset
            .checked_add(u64::from(entry.record_length))
            .is_some_and(|end| end <= layout.index_offset)
        && entry
            .record_offset
            .is_multiple_of(u64::from(RECORD_ALIGNMENT))
        && (MIN_RAW_RECORD_BYTES..=MAX_RECORD_BYTES).contains(&record_length)
        && record_length.is_multiple_of(usize::from(RECORD_ALIGNMENT))
        && entry.record_decoded_length != 0
        && usize::try_from(entry.record_decoded_length)
            .is_ok_and(|length| length <= MAX_DECODED_RECORD_BYTES)
        && entry.record_payload_length != 0
        && entry.record_payload_length <= entry.record_length
        && entry
            .decoded_offset
            .checked_add(entry.logical_length)
            .is_some_and(|end| end <= entry.record_decoded_length)
}

fn encode_index(entries: &[IndexEntry]) -> Result<Vec<u8>, FormatError> {
    let entries_bytes = entries
        .len()
        .checked_mul(INDEX_ENTRY_BYTES_USIZE)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let length = INDEX_HEADER_BYTES_USIZE
        .checked_add(entries_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let entry_count = u32::try_from(entries.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut bytes = vec![0_u8; length];
    bytes[0..8].copy_from_slice(INDEX_MAGIC);
    put_u16(&mut bytes, 8, FORMAT_VERSION);
    put_u16(&mut bytes, 10, 64);
    put_u16(&mut bytes, 12, 128);
    put_u16(&mut bytes, 14, 1);
    put_u32(&mut bytes, 32, entry_count);
    for (ordinal, entry) in entries.iter().enumerate() {
        let start = INDEX_HEADER_BYTES_USIZE
            .checked_add(
                ordinal
                    .checked_mul(INDEX_ENTRY_BYTES_USIZE)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        entry.encode(&mut bytes[start..start + INDEX_ENTRY_BYTES_USIZE]);
    }
    let checksum = crc32c::crc32c(&bytes);
    put_u32(&mut bytes, INDEX_CRC_OFFSET, checksum);
    Ok(bytes)
}

fn decode_index(bytes: &[u8], expected_count: u32) -> Result<Vec<IndexEntry>, FormatError> {
    if bytes.len() < INDEX_HEADER_BYTES_USIZE || &bytes[0..8] != INDEX_MAGIC {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    let count = get_u32(bytes, 32);
    let count_usize = usize::try_from(count).map_err(|_| FormatError::ArithmeticOverflow)?;
    let entries_bytes = count_usize
        .checked_mul(INDEX_ENTRY_BYTES_USIZE)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_length = INDEX_HEADER_BYTES_USIZE
        .checked_add(entries_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    if bytes.len() != expected_length {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    let stored_checksum = get_u32(bytes, INDEX_CRC_OFFSET);
    if crc32c_with_zeroed_field(bytes, INDEX_CRC_OFFSET) != stored_checksum {
        return Err(FormatError::IndexChecksumMismatch);
    }
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != INDEX_HEADER_BYTES_USIZE
        || usize::from(get_u16(bytes, 12)) != INDEX_ENTRY_BYTES_USIZE
        || get_u16(bytes, 14) != 1
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || count != expected_count
        || bytes[40..64].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    let mut entries = Vec::with_capacity(count_usize);
    for ordinal in 0..count_usize {
        let start = INDEX_HEADER_BYTES_USIZE + ordinal * INDEX_ENTRY_BYTES_USIZE;
        entries.push(IndexEntry::decode(
            &bytes[start..start + INDEX_ENTRY_BYTES_USIZE],
        )?);
    }
    if entries.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    if entries.windows(2).any(|pair| {
        pair[0].chunk_id == pair[1].chunk_id && pair[0].logical_length != pair[1].logical_length
    }) {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    Ok(entries)
}

fn encode_footer(
    output: &mut [u8],
    header: &ContainerHeader,
    intrinsic_summary: ContainerIntrinsicSummary,
) {
    assert_eq!(output.len(), FOOTER_BYTES_USIZE);
    output[0..8].copy_from_slice(FOOTER_MAGIC);
    put_u16(output, 8, CONTAINER_FORMAT_VERSION);
    put_u16(output, 10, 4_096);
    output[12..16].copy_from_slice(b"SEAL");
    output[32..48].copy_from_slice(&header.container_id.0);
    put_u64(output, 48, header.container_generation);
    put_u64(output, 56, header.layout.file_length);
    put_u32(output, 64, header.layout.record_count);
    put_u32(output, 68, header.layout.chunk_entry_count);
    put_u64(output, 72, header.layout.index_offset);
    put_u64(output, 80, header.layout.index_length);
    put_u64(output, 88, header.layout.footer_offset);
    intrinsic_summary.encode(
        &mut output[FOOTER_SUMMARY_OFFSET..FOOTER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES],
    );
}

fn decode_footer(bytes: &[u8]) -> Result<Footer, FormatError> {
    if bytes.len() != FOOTER_BYTES_USIZE || &bytes[0..8] != FOOTER_MAGIC {
        return Err(FormatError::InvalidFooter);
    }
    let stored_checksum = get_u32(bytes, FOOTER_CRC_OFFSET);
    if crc32c_with_zeroed_field(bytes, FOOTER_CRC_OFFSET) != stored_checksum {
        return Err(FormatError::FooterChecksumMismatch);
    }
    if get_u16(bytes, 8) != CONTAINER_FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != FOOTER_BYTES_USIZE
        || &bytes[12..16] != b"SEAL"
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || bytes[132..FOOTER_SUMMARY_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
        || bytes[FOOTER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES..]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidFooter);
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[32..48]);
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(&bytes[FOOTER_HASH_OFFSET..FOOTER_HASH_OFFSET + 32]);
    let layout = ContainerLayout {
        record_count: get_u32(bytes, 64),
        chunk_entry_count: get_u32(bytes, 68),
        index_offset: get_u64(bytes, 72),
        index_length: get_u64(bytes, 80),
        footer_offset: get_u64(bytes, 88),
        file_length: get_u64(bytes, 56),
    };
    validate_layout(layout)?;
    let intrinsic_summary = ContainerIntrinsicSummary::decode(
        &bytes[FOOTER_SUMMARY_OFFSET..FOOTER_SUMMARY_OFFSET + CONTAINER_SUMMARY_BYTES],
    )?;
    intrinsic_summary.validate(layout)?;
    Ok(Footer {
        container_id: ContainerId::new(id)?,
        container_generation: get_u64(bytes, 48),
        layout,
        intrinsic_summary,
        container_hash: hash,
    })
}

fn calculate_container_commitment(
    bytes: &[u8],
    header: &ContainerHeader,
) -> Result<[u8; 32], FormatError> {
    let index_offset =
        usize::try_from(header.layout.index_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
    let index_length =
        usize::try_from(header.layout.index_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let index_end = index_offset
        .checked_add(index_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let footer_offset = usize::try_from(header.layout.footer_offset)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let file_length =
        usize::try_from(header.layout.file_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    if bytes.len() != file_length
        || index_end > footer_offset
        || footer_offset
            .checked_add(FOOTER_BYTES_USIZE)
            .is_none_or(|end| end != bytes.len())
    {
        return Err(FormatError::InvalidContainerLayout);
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTAINER_COMMITMENT_DOMAIN_V1);
    hasher.update(&bytes[..HEADER_BYTES]);
    let mut cursor = HEADER_BYTES;
    for _ in 0..header.layout.record_count {
        let fixed_end = cursor
            .checked_add(RECORD_HEADER_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if fixed_end > index_offset {
            return Err(FormatError::InvalidContainerLayout);
        }
        let chunk_count = usize::try_from(get_u32(bytes, cursor + 56))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let table_end = fixed_end
            .checked_add(
                chunk_count
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        let record_length = usize::try_from(get_u32(bytes, cursor + 32))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let record_end = cursor
            .checked_add(record_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if table_end > record_end || record_end > index_offset {
            return Err(FormatError::InvalidContainerLayout);
        }
        hasher.update(&bytes[cursor..table_end]);
        cursor = record_end;
    }
    if cursor != index_offset {
        return Err(FormatError::InvalidContainerLayout);
    }
    hasher.update(&bytes[index_offset..index_end]);
    let footer = &bytes[footer_offset..];
    hasher.update(&footer[..FOOTER_HASH_OFFSET]);
    hasher.update(&[0_u8; 36]);
    hasher.update(&footer[FOOTER_CRC_OFFSET + 4..]);
    Ok(*hasher.finalize().as_bytes())
}

fn crc32c_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    crc32c_with_zeroed_u32(bytes, field_offset)
}

fn validate_logical_chunk_length(length: usize) -> Result<(), FormatError> {
    if length == 0 || length > MAX_LOGICAL_CHUNK_BYTES {
        return Err(FormatError::InvalidRawRecord);
    }
    Ok(())
}

fn raw_record_length(payload_length: usize) -> Result<usize, FormatError> {
    validate_logical_chunk_length(payload_length)?;
    let unaligned_length = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(unaligned_length, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

fn adaptive_container_layout(
    records: &[AdaptiveRecordPlan<'_>],
) -> Result<(ContainerLayout, ContainerIntrinsicSummary), FormatError> {
    if records.is_empty() {
        return Err(FormatError::InvalidContainerLayout);
    }
    let record_count = u32::try_from(records.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut chunk_entry_count = 0_u32;
    let mut summary = IntrinsicSummaryAccumulator::with_record_capacity(records.len())?;
    let mut index_offset =
        u64::try_from(HEADER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?;
    for record in records {
        record.observe_intrinsic_summary(&mut summary)?;
        chunk_entry_count = chunk_entry_count
            .checked_add(
                u32::try_from(record.chunk_count()).map_err(|_| FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        index_offset = index_offset
            .checked_add(
                u64::try_from(record.record_length()?)
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    let index_length = INDEX_HEADER_BYTES
        .checked_add(
            u64::from(chunk_entry_count)
                .checked_mul(INDEX_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    let footer_offset = align_up(
        index_offset
            .checked_add(index_length)
            .ok_or(FormatError::ArithmeticOverflow)?,
        FOOTER_BYTES,
    )?;
    let file_length = footer_offset
        .checked_add(FOOTER_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let layout = ContainerLayout {
        record_count,
        chunk_entry_count,
        index_offset,
        index_length,
        footer_offset,
        file_length,
    };
    validate_layout(layout)?;
    let summary = summary.finish(layout)?;
    Ok((layout, summary))
}

fn seal_container_envelope(
    container: &mut [u8],
    header: &ContainerHeader,
    intrinsic_summary: ContainerIntrinsicSummary,
    footer_offset: usize,
) -> Result<(), FormatError> {
    container[..HEADER_BYTES].copy_from_slice(&header.encode(intrinsic_summary));
    encode_footer(&mut container[footer_offset..], header, intrinsic_summary);
    let hash = calculate_container_commitment(container, header)?;
    container[footer_offset + FOOTER_HASH_OFFSET..footer_offset + FOOTER_HASH_OFFSET + 32]
        .copy_from_slice(&hash);
    let footer_checksum = crc32c_with_zeroed_field(&container[footer_offset..], FOOTER_CRC_OFFSET);
    put_u32(
        &mut container[footer_offset..],
        FOOTER_CRC_OFFSET,
        footer_checksum,
    );
    Ok(())
}

fn encode_container_from_adaptive_plans(
    container_id: ContainerId,
    container_generation: u64,
    records: Vec<AdaptiveRecordPlan<'_>>,
    _permitted_hash_workers: NonZeroUsize,
) -> Result<AdaptiveContainerEncoding, FormatError> {
    let (layout, intrinsic_summary) = adaptive_container_layout(&records)?;
    let header = ContainerHeader::sealed(container_id, container_generation, layout)?;
    let file_length =
        usize::try_from(layout.file_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let footer_offset =
        usize::try_from(layout.footer_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
    let entry_capacity =
        usize::try_from(layout.chunk_entry_count).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut index_entries = Vec::new();
    index_entries
        .try_reserve_exact(entry_capacity)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut locations = Vec::with_capacity(entry_capacity);
    let mut raw_locations = Vec::with_capacity(records.len());
    let mut logical_bytes = 0_u64;
    let mut raw_record_count = 0_usize;
    let mut zstd_record_count = 0_usize;
    let mut zstd_prefix_record_count = 0_usize;
    let mut container = AlignedContainerBytes::zeroed(file_length);
    let mut cursor = HEADER_BYTES;
    for record in records {
        let record_length = record.record_length()?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        record.encode_into(&mut container[cursor..end])?;
        let record_entries = writer_record_evidence(
            &header,
            &container[cursor..end],
            u64::try_from(cursor).map_err(|_| FormatError::ArithmeticOverflow)?,
            &mut locations,
            &mut raw_locations,
        )?;
        logical_bytes = logical_bytes
            .checked_add(u64::from(get_u32(&container[cursor..end], 36)))
            .ok_or(FormatError::ArithmeticOverflow)?;
        match get_u16(&container[cursor..end], 12) {
            RAW_CODEC => {
                raw_record_count = raw_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            ZSTD_CODEC => {
                zstd_record_count = zstd_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            ZSTD_PREFIX_CODEC => {
                zstd_prefix_record_count = zstd_prefix_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            _ => return Err(FormatError::UnsupportedHeaderField),
        }
        index_entries.extend(record_entries);
        cursor = end;
    }
    assert_eq!(
        u64::try_from(cursor),
        Ok(layout.index_offset),
        "ASSERT: adaptive record plans exactly fill the record region"
    );
    index_entries.sort_unstable();
    let index = encode_index(&index_entries)?;
    assert_eq!(u64::try_from(index.len()), Ok(layout.index_length));
    let index_end = cursor
        .checked_add(index.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    container[cursor..index_end].copy_from_slice(&index);
    seal_container_envelope(&mut container, &header, intrinsic_summary, footer_offset)?;
    Ok(AdaptiveContainerEncoding {
        bytes: container,
        publication: VerifiedContainerPublication {
            header,
            locations,
            raw_locations,
            logical_bytes,
            raw_record_count,
            zstd_record_count,
            zstd_prefix_record_count,
        },
        metrics: IncompressibilityGateMetrics::default(),
    })
}

fn encoded_container_layout(
    records: &[Vec<u8>],
) -> Result<(ContainerLayout, ContainerIntrinsicSummary), FormatError> {
    if records.is_empty() {
        return Err(FormatError::InvalidContainerLayout);
    }
    let record_count = u32::try_from(records.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut chunk_entry_count = 0_u32;
    let mut summary = IntrinsicSummaryAccumulator::with_record_capacity(records.len())?;
    let mut index_offset =
        u64::try_from(HEADER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?;
    for record in records {
        assert!(
            record.len() >= MIN_RAW_RECORD_BYTES
                && record.len() <= MAX_RECORD_BYTES
                && record.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
                && usize::try_from(get_u32(record, 32)) == Ok(record.len())
                && get_u32(record, 56) != 0,
            "ASSERT: internal record writer emitted an impossible structural shape"
        );
        summary.observe_encoded_record(record)?;
        chunk_entry_count = chunk_entry_count
            .checked_add(get_u32(record, 56))
            .ok_or(FormatError::ArithmeticOverflow)?;
        index_offset = index_offset
            .checked_add(u64::try_from(record.len()).map_err(|_| FormatError::ArithmeticOverflow)?)
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    let index_length = INDEX_HEADER_BYTES
        .checked_add(
            u64::from(chunk_entry_count)
                .checked_mul(INDEX_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    let footer_offset = align_up(
        index_offset
            .checked_add(index_length)
            .ok_or(FormatError::ArithmeticOverflow)?,
        FOOTER_BYTES,
    )?;
    let file_length = footer_offset
        .checked_add(FOOTER_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let layout = ContainerLayout {
        record_count,
        chunk_entry_count,
        index_offset,
        index_length,
        footer_offset,
        file_length,
    };
    validate_layout(layout)?;
    let summary = summary.finish(layout)?;
    Ok((layout, summary))
}

#[allow(clippy::too_many_lines)]
fn encode_container_from_records(
    container_id: ContainerId,
    container_generation: u64,
    encoded_records: Vec<Vec<u8>>,
    _permitted_hash_workers: NonZeroUsize,
) -> Result<AdaptiveContainerEncoding, FormatError> {
    let (layout, intrinsic_summary) = encoded_container_layout(&encoded_records)?;
    let mut record_offset = HEADER_BYTES as u64;
    let mut index_entries = Vec::new();
    index_entries
        .try_reserve_exact(
            usize::try_from(layout.chunk_entry_count)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        )
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let header = ContainerHeader::sealed(container_id, container_generation, layout)?;
    let mut locations = Vec::with_capacity(index_entries.capacity());
    let mut raw_locations = Vec::with_capacity(encoded_records.len());
    let mut logical_bytes = 0_u64;
    let mut raw_record_count = 0_usize;
    let mut zstd_record_count = 0_usize;
    let mut zstd_prefix_record_count = 0_usize;
    for encoded in &encoded_records {
        let record_entries = writer_record_evidence(
            &header,
            encoded,
            record_offset,
            &mut locations,
            &mut raw_locations,
        )?;
        logical_bytes = logical_bytes
            .checked_add(u64::from(get_u32(encoded, 36)))
            .ok_or(FormatError::ArithmeticOverflow)?;
        match get_u16(encoded, 12) {
            RAW_CODEC => {
                raw_record_count = raw_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            ZSTD_CODEC => {
                zstd_record_count = zstd_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            ZSTD_PREFIX_CODEC => {
                zstd_prefix_record_count = zstd_prefix_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            _ => return Err(FormatError::UnsupportedHeaderField),
        }
        index_entries.extend(record_entries);
        record_offset = record_offset
            .checked_add(u64::try_from(encoded.len()).map_err(|_| FormatError::ArithmeticOverflow)?)
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    index_entries.sort_unstable();
    let mut index = encode_index(&index_entries)?;
    assert_eq!(record_offset, layout.index_offset);
    assert_eq!(u64::try_from(index.len()), Ok(layout.index_length));
    let file_length_usize =
        usize::try_from(layout.file_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let footer_offset_usize =
        usize::try_from(layout.footer_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut container = AlignedContainerBuilder::new(file_length_usize);
    container.append_zeroed(HEADER_BYTES);
    for mut record in encoded_records {
        record_copy(CopyClass::ContainerAssembly, record.len());
        container.append(&mut record);
    }
    assert_eq!(
        u64::try_from(container.image_length()),
        Ok(layout.index_offset),
        "ASSERT: encoded Records exactly fill the declared record region"
    );
    let index_end = container
        .image_length()
        .checked_add(index.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    container.append(&mut index);
    let padding_length = footer_offset_usize
        .checked_sub(index_end)
        .ok_or(FormatError::ArithmeticOverflow)?;
    container.append_zeroed(padding_length);
    container.append_zeroed(FOOTER_BYTES_USIZE);
    let mut container = container.finish();
    seal_container_envelope(
        &mut container,
        &header,
        intrinsic_summary,
        footer_offset_usize,
    )?;
    Ok(AdaptiveContainerEncoding {
        bytes: container,
        publication: VerifiedContainerPublication {
            header,
            locations,
            raw_locations,
            logical_bytes,
            raw_record_count,
            zstd_record_count,
            zstd_prefix_record_count,
        },
        metrics: IncompressibilityGateMetrics::default(),
    })
}

fn writer_record_evidence(
    header: &ContainerHeader,
    encoded: &[u8],
    record_offset: u64,
    locations: &mut Vec<VerifiedChunkLocation>,
    raw_locations: &mut Vec<VerifiedRawLocation>,
) -> Result<Vec<IndexEntry>, FormatError> {
    let entries = IndexEntry::from_encoded_record(encoded, record_offset)?;
    for entry in &entries {
        locations.push(VerifiedChunkLocation {
            chunk_id: entry.chunk_id,
            logical_length: entry.logical_length,
            container_id: header.container_id,
            container_generation: header.container_generation,
            record_offset: entry.record_offset,
            record_length: entry.record_length,
            chunk_ordinal: entry.chunk_ordinal,
            decoded_offset: entry.decoded_offset,
            codec_id: entry.codec_id,
            dependency_id: entry.dependency_id,
            record_crc32c: entry.record_crc32c,
            record_decoded_length: entry.record_decoded_length,
            record_payload_length: entry.record_payload_length,
        });
    }
    if get_u16(encoded, 12) == RAW_CODEC {
        let entry = entries.first().ok_or(FormatError::InvalidRawRecord)?;
        raw_locations.push(VerifiedRawLocation {
            chunk_id: entry.chunk_id,
            logical_length: entry.logical_length,
            container_id: header.container_id,
            container_generation: header.container_generation,
            record_offset: entry.record_offset,
            record_length: entry.record_length,
            record_crc32c: entry.record_crc32c,
        });
    }
    Ok(entries)
}

fn validate_raw_record_constants(bytes: &[u8]) -> Result<(), FormatError> {
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 12) != RAW_CODEC
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 40)) != Ok(RAW_PAYLOAD_OFFSET)
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || get_u32(bytes, 56) != 1
        || bytes[64..128].iter().any(|byte| *byte != 0)
        || get_u32(bytes, 160) != 0
        || get_u64(bytes, 168) != 0
        || get_u64(bytes, 176) != 0
        || get_u64(bytes, 184) != 0
    {
        return Err(FormatError::InvalidRawRecord);
    }
    Ok(())
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
