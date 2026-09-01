use std::fmt;

use crate::container::{
    MAX_DECODED_RECORD_BYTES, MAX_RECORD_BYTES, RAW_CODEC, SPARSE_XOR_CODEC, VerifiedChunkLocation,
    VerifiedRawLocation, ZSTD_CODEC, ZSTD_PREFIX_CODEC,
};
use crate::{ChunkId, ContainerId, MAX_CONTAINER_BYTES, MAX_LOGICAL_CHUNK_BYTES};

pub const EXACT_INDEX_HEADER_BYTES: usize = 4_096;
pub const EXACT_INDEX_PAGE_BYTES: usize = 4_096;
pub const EXACT_INDEX_ENTRY_BYTES: usize = 128;
const PAGE_HEADER_BYTES: usize = 128;
const ENTRIES_PER_PAGE: usize = 31;
const FOOTER_BYTES: usize = 4_096;
const HEADER_BYTES_U16: u16 = 4_096;
const HEADER_BYTES_U64: u64 = 4_096;
const PAGE_BYTES_U16: u16 = 4_096;
const ENTRY_BYTES_U16: u16 = 128;
const PAGE_HEADER_BYTES_U16: u16 = 128;
const ENTRIES_PER_PAGE_U32: u32 = 31;
const MAX_RUN_BYTES: usize = 1 << 30;
const HEADER_MAGIC: [u8; 8] = *b"FDXIRN01";
const PAGE_MAGIC: [u8; 8] = *b"FDXPG001";
const FOOTER_MAGIC: [u8; 8] = *b"FDXFTR01";
const FORMAT_VERSION: u16 = 1;
const RAW_PAYLOAD_OFFSET_U32: u32 = 192;
const RECORD_ALIGNMENT_U32: u32 = 64;
const MIN_RAW_RECORD_BYTES_U32: u32 = 256;
const HEADER_CRC_OFFSET: usize = 184;
const PAGE_CRC_OFFSET: usize = 20;
const RUN_HASH_OFFSET: usize = 184;
const FOOTER_CRC_OFFSET: usize = 216;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExactIndexProfileId([u8; 32]);

impl ExactIndexProfileId {
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExactLocationTransition {
    Active = 1,
    Retiring = 2,
    Quarantined = 3,
    Removed = 4,
}

impl ExactLocationTransition {
    const fn encode(self) -> u16 {
        match self {
            Self::Active => 1,
            Self::Retiring => 2,
            Self::Quarantined => 3,
            Self::Removed => 4,
        }
    }

    const fn decode(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::Active),
            2 => Some(Self::Retiring),
            3 => Some(Self::Quarantined),
            4 => Some(Self::Removed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexLocation {
    container_id: ContainerId,
    container_generation: u64,
    record_offset: u64,
    record_length: u32,
    chunk_ordinal: u32,
    decoded_offset: u32,
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
    codec_id: u16,
    dependency_id: [u8; 32],
}

impl ExactIndexLocation {
    /// Constructs an independent RAW Container-v1 Location.
    ///
    /// # Errors
    ///
    /// Rejects zero generations, unaligned/out-of-range record coordinates, or
    /// zero logical lengths.
    pub fn raw(
        container_id: ContainerId,
        container_generation: u64,
        record_offset: u64,
        record_length: u32,
        record_crc32c: u32,
    ) -> Result<Self, ExactIndexFormatError> {
        if container_generation == 0 || !valid_raw_location(record_offset, record_length) {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        Ok(Self {
            container_id,
            container_generation,
            record_offset,
            record_length,
            chunk_ordinal: 0,
            decoded_offset: 0,
            record_crc32c,
            record_decoded_length: 0,
            record_payload_length: 0,
            codec_id: RAW_CODEC,
            dependency_id: [0; 32],
        })
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
    pub const fn record_offset(&self) -> u64 {
        self.record_offset
    }

    #[must_use]
    pub const fn record_length(&self) -> u32 {
        self.record_length
    }

    #[must_use]
    pub const fn chunk_ordinal(&self) -> u32 {
        self.chunk_ordinal
    }

    #[must_use]
    pub const fn decoded_offset(&self) -> u32 {
        self.decoded_offset
    }

    #[must_use]
    pub const fn record_crc32c(&self) -> u32 {
        self.record_crc32c
    }

    #[must_use]
    pub const fn record_decoded_length(&self) -> u32 {
        self.record_decoded_length
    }

    #[must_use]
    pub const fn record_payload_length(&self) -> u32 {
        self.record_payload_length
    }

    #[must_use]
    pub const fn codec_id(&self) -> u16 {
        self.codec_id
    }

    #[must_use]
    pub const fn dependency_id(&self) -> [u8; 32] {
        self.dependency_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexEntry {
    chunk_id: ChunkId,
    logical_length: u32,
    transition: ExactLocationTransition,
    location: ExactIndexLocation,
}

impl ExactIndexEntry {
    /// Converts proof emitted by a fully verified immutable independent
    /// Container record into one ACTIVE acceleration entry.
    ///
    /// # Errors
    ///
    /// Returns an integrity error if Container and Exact-Index v1 coordinate
    /// invariants disagree.
    pub fn from_verified(verified: VerifiedChunkLocation) -> Result<Self, ExactIndexFormatError> {
        let location = ExactIndexLocation {
            container_id: verified.container_id(),
            container_generation: verified.container_generation(),
            record_offset: verified.record_offset(),
            record_length: verified.record_length(),
            chunk_ordinal: verified.chunk_ordinal(),
            decoded_offset: verified.decoded_offset(),
            record_crc32c: verified.record_crc32c(),
            record_decoded_length: verified.record_decoded_length(),
            record_payload_length: verified.record_payload_length(),
            codec_id: verified.codec_id(),
            dependency_id: verified.dependency_id(),
        };
        if !valid_location(verified.logical_length(), location) {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        Ok(Self {
            chunk_id: verified.chunk_id(),
            logical_length: verified.logical_length(),
            transition: ExactLocationTransition::Active,
            location,
        })
    }

    /// Converts proof emitted by a fully verified immutable RAW Container into
    /// one ACTIVE acceleration entry without reconstructing physical fields
    /// from caller-supplied scalars.
    ///
    /// # Errors
    ///
    /// Returns an integrity error if the Container reader and Exact Index
    /// writer ever disagree about shared v1 coordinate invariants.
    pub fn from_verified_raw(verified: VerifiedRawLocation) -> Result<Self, ExactIndexFormatError> {
        let location = ExactIndexLocation::raw(
            verified.container_id(),
            verified.container_generation(),
            verified.record_offset(),
            verified.record_length(),
            verified.record_crc32c(),
        )?;
        Self::active(verified.chunk_id(), verified.logical_length(), location)
    }

    /// Constructs one ACTIVE independent RAW Location transition.
    ///
    /// # Errors
    ///
    /// Rejects a zero logical length.
    pub fn active(
        chunk_id: ChunkId,
        logical_length: u32,
        mut location: ExactIndexLocation,
    ) -> Result<Self, ExactIndexFormatError> {
        if location.codec_id != RAW_CODEC
            || expected_raw_record_length(logical_length) != Some(location.record_length)
        {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        location.record_decoded_length = logical_length;
        location.record_payload_length = logical_length;
        Ok(Self {
            chunk_id,
            logical_length,
            transition: ExactLocationTransition::Active,
            location,
        })
    }

    /// Converts one verified ACTIVE physical Location into its RETIRING
    /// transition without allowing any physical coordinate to change.
    ///
    /// The returned entry is acceleration state only. It becomes the durable
    /// selection barrier after a newer Run Set containing it is activated.
    ///
    /// # Errors
    ///
    /// Rejects a source that is not ACTIVE.
    pub fn retiring(active: Self) -> Result<Self, ExactIndexFormatError> {
        if active.transition != ExactLocationTransition::Active {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        Ok(Self {
            transition: ExactLocationTransition::Retiring,
            ..active
        })
    }

    /// Converts one RETIRING physical Location into its REMOVED tombstone.
    ///
    /// # Errors
    ///
    /// Rejects a source that is not RETIRING.
    pub fn removed(retiring: Self) -> Result<Self, ExactIndexFormatError> {
        if retiring.transition != ExactLocationTransition::Retiring {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        Ok(Self {
            transition: ExactLocationTransition::Removed,
            ..retiring
        })
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(&self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn transition(&self) -> ExactLocationTransition {
        self.transition
    }

    #[must_use]
    pub const fn location(&self) -> ExactIndexLocation {
        self.location
    }

    const fn location_key(&self) -> (ChunkId, u32, [u8; 16], u64, u32) {
        (
            self.chunk_id,
            self.logical_length,
            self.location.container_id.bytes(),
            self.location.record_offset,
            self.location.chunk_ordinal,
        )
    }

    fn encode(self, output: &mut [u8]) {
        output[0..32].copy_from_slice(&self.chunk_id.bytes());
        put_u32(output, 32, self.logical_length);
        put_u16(output, 36, self.transition.encode());
        put_u16(output, 38, self.location.codec_id);
        output[40..56].copy_from_slice(&self.location.container_id.bytes());
        put_u64(output, 56, self.location.container_generation);
        put_u64(output, 64, self.location.record_offset);
        put_u32(output, 72, self.location.record_length);
        put_u32(output, 76, self.location.chunk_ordinal);
        put_u32(output, 80, self.location.decoded_offset);
        put_u32(output, 84, self.location.record_crc32c);
        put_u32(output, 88, self.location.record_decoded_length);
        put_u32(output, 92, self.location.record_payload_length);
        output[96..128].copy_from_slice(&self.location.dependency_id);
    }

    fn decode(bytes: &[u8]) -> Result<Self, ExactIndexFormatError> {
        if bytes.len() != EXACT_INDEX_ENTRY_BYTES || get_u32(bytes, 32) == 0 {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[0..32]);
        let mut container_id = [0_u8; 16];
        container_id.copy_from_slice(&bytes[40..56]);
        let entry = Self {
            chunk_id: ChunkId::from_bytes(chunk_id),
            logical_length: get_u32(bytes, 32),
            transition: ExactLocationTransition::decode(get_u16(bytes, 36))
                .ok_or(ExactIndexFormatError::InvalidEntry)?,
            location: ExactIndexLocation {
                container_id: ContainerId::new(container_id)
                    .map_err(|_| ExactIndexFormatError::InvalidEntry)?,
                container_generation: get_u64(bytes, 56),
                record_offset: get_u64(bytes, 64),
                record_length: get_u32(bytes, 72),
                chunk_ordinal: get_u32(bytes, 76),
                decoded_offset: get_u32(bytes, 80),
                record_crc32c: get_u32(bytes, 84),
                record_decoded_length: get_u32(bytes, 88),
                record_payload_length: get_u32(bytes, 92),
                codec_id: get_u16(bytes, 38),
                dependency_id: bytes[96..128]
                    .try_into()
                    .expect("ASSERT: a fixed Exact Index dependency field has 32 bytes"),
            },
        };
        if !valid_location(entry.logical_length, entry.location) {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        Ok(entry)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexRun {
    profile: ExactIndexProfileId,
    generation: u64,
    entries: Vec<ExactIndexEntry>,
}

/// Allocation-bounded canonical encoder for one immutable Exact Index Run.
///
/// The caller supplies the exact output count and key bounds established by a
/// preceding verified merge pass, then feeds canonical entries in complete
/// format-v1 pages. The encoder retains only fixed geometry and one hash state;
/// it never materializes the complete Run.
pub struct ExactIndexRunStreamEncoder {
    profile: ExactIndexProfileId,
    generation: u64,
    entry_count: usize,
    page_count: usize,
    footer_offset: usize,
    file_length: usize,
    key_bounds: [u8; 64],
    header: [u8; EXACT_INDEX_HEADER_BYTES],
    hasher: blake3::Hasher,
    emitted_entries: usize,
    emitted_pages: usize,
    previous_entry: Option<ExactIndexEntry>,
}

impl ExactIndexRunStreamEncoder {
    /// Starts one streaming Run with exact geometry from a verified merge pass.
    ///
    /// # Errors
    ///
    /// Rejects an empty Run, zero generation, reversed key bounds, arithmetic
    /// overflow, or output exceeding the format-v1 one-GiB object bound.
    pub fn new(
        profile: ExactIndexProfileId,
        generation: u64,
        entry_count: usize,
        minimum_chunk_id: ChunkId,
        maximum_chunk_id: ChunkId,
    ) -> Result<Self, ExactIndexFormatError> {
        if generation == 0 {
            return Err(ExactIndexFormatError::InvalidGeneration);
        }
        if entry_count == 0 || minimum_chunk_id > maximum_chunk_id {
            return Err(ExactIndexFormatError::InvalidEntry);
        }
        let file_length = encoded_length(entry_count)?;
        let page_count = page_count(entry_count);
        let footer_offset = EXACT_INDEX_HEADER_BYTES
            .checked_add(
                page_count
                    .checked_mul(EXACT_INDEX_PAGE_BYTES)
                    .ok_or(ExactIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let mut key_bounds = [0_u8; 64];
        key_bounds[..32].copy_from_slice(&minimum_chunk_id.bytes());
        key_bounds[32..].copy_from_slice(&maximum_chunk_id.bytes());
        let mut header = [0_u8; EXACT_INDEX_HEADER_BYTES];
        encode_identity_fields(
            &mut header,
            HEADER_MAGIC,
            profile,
            generation,
            entry_count,
            page_count,
            key_bounds,
            footer_offset,
            file_length,
        )?;
        let header_crc = checksum_with_zero(&header, HEADER_CRC_OFFSET);
        put_u32(&mut header, HEADER_CRC_OFFSET, header_crc);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        Ok(Self {
            profile,
            generation,
            entry_count,
            page_count,
            footer_offset,
            file_length,
            key_bounds,
            header,
            hasher,
            emitted_entries: 0,
            emitted_pages: 0,
            previous_entry: None,
        })
    }

    /// Returns the complete checksummed Header to write at offset zero.
    #[must_use]
    pub const fn header(&self) -> &[u8; EXACT_INDEX_HEADER_BYTES] {
        &self.header
    }

    /// Encodes the next complete canonical entry page and advances the Run hash.
    ///
    /// # Errors
    ///
    /// Rejects a wrong page size, too many pages/entries, duplicate/reordered
    /// Locations, or a Chunk ID paired with conflicting logical lengths.
    pub fn encode_next_page(
        &mut self,
        entries: &[ExactIndexEntry],
    ) -> Result<[u8; EXACT_INDEX_PAGE_BYTES], ExactIndexFormatError> {
        let remaining = self
            .entry_count
            .checked_sub(self.emitted_entries)
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let expected = remaining.min(ENTRIES_PER_PAGE);
        if self.emitted_pages >= self.page_count || entries.len() != expected || expected == 0 {
            return Err(ExactIndexFormatError::InvalidPage);
        }
        if let Some(previous) = self.previous_entry {
            validate_entry_pair(previous, entries[0])?;
        }
        validate_entries(entries)?;
        let mut minimum = [0_u8; 32];
        minimum.copy_from_slice(&self.key_bounds[..32]);
        let mut maximum = [0_u8; 32];
        maximum.copy_from_slice(&self.key_bounds[32..]);
        if (self.emitted_pages == 0 && entries[0].chunk_id != ChunkId::from_bytes(minimum))
            || (self.emitted_pages + 1 == self.page_count
                && entries
                    .last()
                    .is_none_or(|entry| entry.chunk_id != ChunkId::from_bytes(maximum)))
        {
            return Err(ExactIndexFormatError::InvalidPage);
        }
        let mut page = [0_u8; EXACT_INDEX_PAGE_BYTES];
        encode_page(&mut page, self.emitted_pages, entries)?;
        self.hasher.update(&page);
        self.previous_entry = entries.last().copied();
        self.emitted_entries = self
            .emitted_entries
            .checked_add(entries.len())
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        self.emitted_pages = self
            .emitted_pages
            .checked_add(1)
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        Ok(page)
    }

    /// Finishes the exact Footer and returns its verified descriptor.
    ///
    /// # Errors
    ///
    /// Rejects incomplete output or any writer-side Header/Footer disagreement.
    pub fn finish(
        mut self,
    ) -> Result<([u8; EXACT_INDEX_PAGE_BYTES], ExactIndexRunDescriptor), ExactIndexFormatError>
    {
        if self.emitted_entries != self.entry_count || self.emitted_pages != self.page_count {
            return Err(ExactIndexFormatError::NonSequentialAudit);
        }
        let mut footer = [0_u8; EXACT_INDEX_PAGE_BYTES];
        encode_identity_fields(
            &mut footer,
            FOOTER_MAGIC,
            self.profile,
            self.generation,
            self.entry_count,
            self.page_count,
            self.key_bounds,
            self.footer_offset,
            self.file_length,
        )?;
        put_u16(&mut footer, 10, PAGE_BYTES_U16);
        self.hasher.update(&footer);
        let run_hash = *self.hasher.finalize().as_bytes();
        footer[RUN_HASH_OFFSET..RUN_HASH_OFFSET + 32].copy_from_slice(&run_hash);
        let footer_crc = checksum_with_zero(&footer, FOOTER_CRC_OFFSET);
        put_u32(&mut footer, FOOTER_CRC_OFFSET, footer_crc);
        let descriptor = ExactIndexRunDescriptor::decode(
            &self.header,
            &footer,
            u64_from_usize(self.file_length)?,
        )?;
        Ok((footer, descriptor))
    }
}

/// Header/Footer proof for bounded page reads from one immutable run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexRunDescriptor {
    profile: ExactIndexProfileId,
    generation: u64,
    entry_count: usize,
    page_count: usize,
    footer_offset: usize,
    file_length: usize,
    key_bounds: [u8; 64],
    run_hash: [u8; 32],
}

impl ExactIndexRunDescriptor {
    /// Verifies the independently read Header and Footer of one run.
    ///
    /// This deliberately does not claim that the full-file run hash has been
    /// re-read. Each page returned by [`Self::decode_page`] is independently
    /// protected, and every resulting Location remains an unverified lookup
    /// candidate until paired with its immutable Container.
    ///
    /// # Errors
    ///
    /// Rejects invalid checksums, identity disagreement, noncanonical
    /// geometry, unsupported fields, or an object outside the v1 bound.
    pub fn decode(
        header: &[u8],
        footer: &[u8],
        physical_length: u64,
    ) -> Result<Self, ExactIndexFormatError> {
        validate_identity_block(header, HEADER_MAGIC, HEADER_CRC_OFFSET)?;
        validate_identity_block(footer, FOOTER_MAGIC, FOOTER_CRC_OFFSET)?;
        if footer[16..184] != header[16..184]
            || usize::from(get_u16(footer, 10)) != FOOTER_BYTES
            || footer[220..].iter().any(|byte| *byte != 0)
        {
            return Err(ExactIndexFormatError::HeaderFooterMismatch);
        }

        let entry_count = usize::try_from(get_u64(header, 40))
            .map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?;
        let page_count = page_count(entry_count);
        let expected_length = encoded_length(entry_count)?;
        let footer_offset = EXACT_INDEX_HEADER_BYTES
            .checked_add(
                page_count
                    .checked_mul(EXACT_INDEX_PAGE_BYTES)
                    .ok_or(ExactIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let physical_length = usize::try_from(physical_length)
            .map_err(|_| ExactIndexFormatError::InvalidObjectLength(usize::MAX))?;
        if physical_length != expected_length
            || get_u64(header, 48) != u64_from_usize(page_count)?
            || get_u64(header, 160) != HEADER_BYTES_U64
            || get_u64(header, 168) != u64_from_usize(footer_offset)?
            || get_u64(header, 176) != u64_from_usize(physical_length)?
        {
            return Err(ExactIndexFormatError::InvalidHeader);
        }

        let generation = get_u64(header, 32);
        if generation == 0 {
            return Err(ExactIndexFormatError::InvalidGeneration);
        }
        let mut profile = [0_u8; 32];
        profile.copy_from_slice(&header[64..96]);
        let profile =
            ExactIndexProfileId::new(profile).ok_or(ExactIndexFormatError::InvalidHeader)?;
        let mut key_bounds = [0_u8; 64];
        key_bounds.copy_from_slice(&header[96..160]);
        if entry_count == 0 && key_bounds != [0; 64] {
            return Err(ExactIndexFormatError::InvalidHeader);
        }
        let mut run_hash = [0_u8; 32];
        run_hash.copy_from_slice(&footer[RUN_HASH_OFFSET..RUN_HASH_OFFSET + 32]);
        Ok(Self {
            profile,
            generation,
            entry_count,
            page_count,
            footer_offset,
            file_length: physical_length,
            key_bounds,
            run_hash,
        })
    }

    #[must_use]
    pub const fn entry_count(self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub const fn page_count(self) -> usize {
        self.page_count
    }

    #[must_use]
    pub const fn profile(self) -> ExactIndexProfileId {
        self.profile
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn file_length(self) -> usize {
        self.file_length
    }

    #[must_use]
    pub const fn run_hash(self) -> [u8; 32] {
        self.run_hash
    }

    #[must_use]
    pub fn minimum_chunk_id(self) -> ChunkId {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&self.key_bounds[..32]);
        ChunkId::from_bytes(bytes)
    }

    #[must_use]
    pub fn maximum_chunk_id(self) -> ChunkId {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&self.key_bounds[32..]);
        ChunkId::from_bytes(bytes)
    }

    /// Starts a sequential, allocation-bounded complete-file AUDIT.
    #[must_use]
    pub fn begin_hash_audit(self) -> ExactIndexRunHashAudit {
        ExactIndexRunHashAudit {
            descriptor: self,
            hasher: blake3::Hasher::new(),
            next_offset: 0,
            pages_verified: 0,
            previous_entry: None,
        }
    }

    /// Returns the exact offset of a page in this run.
    #[must_use]
    pub fn page_offset(self, page_ordinal: usize) -> Option<u64> {
        if page_ordinal >= self.page_count {
            return None;
        }
        EXACT_INDEX_HEADER_BYTES
            .checked_add(page_ordinal.checked_mul(EXACT_INDEX_PAGE_BYTES)?)
            .and_then(|offset| u64::try_from(offset).ok())
    }

    /// Verifies and decodes one independently fetched 4-KiB page.
    ///
    /// # Errors
    ///
    /// Rejects an out-of-range ordinal or any page geometry, checksum,
    /// reserved-byte, ordering, or key-bound disagreement.
    ///
    /// # Panics
    ///
    /// Panics only if the internal page decoder returns an empty page after
    /// accepting its nonzero descriptor-derived entry count.
    pub fn decode_page(
        self,
        page_ordinal: usize,
        bytes: &[u8],
    ) -> Result<ExactIndexPage, ExactIndexFormatError> {
        if page_ordinal >= self.page_count {
            return Err(ExactIndexFormatError::InvalidPage);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(ENTRIES_PER_PAGE)
            .map_err(|_| ExactIndexFormatError::OutOfMemory)?;
        decode_page(bytes, page_ordinal, self.entry_count, &mut entries)?;
        validate_entries(&entries)?;
        let first = entries
            .first()
            .expect("ASSERT: the page decoder rejects empty pages");
        let last = entries
            .last()
            .expect("ASSERT: the page decoder rejects empty pages");
        let mut minimum = [0_u8; 32];
        minimum.copy_from_slice(&self.key_bounds[..32]);
        let minimum = ChunkId::from_bytes(minimum);
        let mut maximum = [0_u8; 32];
        maximum.copy_from_slice(&self.key_bounds[32..]);
        let maximum = ChunkId::from_bytes(maximum);
        if first.chunk_id < minimum
            || last.chunk_id > maximum
            || (page_ordinal == 0 && first.chunk_id != minimum)
            || (page_ordinal + 1 == self.page_count && last.chunk_id != maximum)
        {
            return Err(ExactIndexFormatError::InvalidPage);
        }
        Ok(ExactIndexPage {
            ordinal: page_ordinal,
            entries,
        })
    }
}

/// Sequential full-run hash and cross-page canonical-order verifier.
pub struct ExactIndexRunHashAudit {
    descriptor: ExactIndexRunDescriptor,
    hasher: blake3::Hasher,
    next_offset: usize,
    pages_verified: usize,
    previous_entry: Option<ExactIndexEntry>,
}

impl ExactIndexRunHashAudit {
    /// Adds the next exact physical byte range to the complete-file hash.
    ///
    /// # Errors
    ///
    /// Rejects nonsequential, overlapping, overflowing, or out-of-file input.
    pub fn update(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ExactIndexFormatError> {
        let offset =
            usize::try_from(offset).map_err(|_| ExactIndexFormatError::NonSequentialAudit)?;
        if offset != self.next_offset {
            return Err(ExactIndexFormatError::NonSequentialAudit);
        }
        let end = offset
            .checked_add(bytes.len())
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        if end > self.descriptor.file_length {
            return Err(ExactIndexFormatError::NonSequentialAudit);
        }

        let zero_start = self
            .descriptor
            .footer_offset
            .checked_add(RUN_HASH_OFFSET)
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let zero_end = self
            .descriptor
            .footer_offset
            .checked_add(FOOTER_CRC_OFFSET + 4)
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let overlap_start = offset.max(zero_start).min(end);
        let overlap_end = end.min(zero_end).max(offset);
        if overlap_start < overlap_end {
            let prefix = overlap_start - offset;
            let suffix = overlap_end - offset;
            self.hasher.update(&bytes[..prefix]);
            self.hasher
                .update(&[0_u8; 36][..overlap_end - overlap_start]);
            self.hasher.update(&bytes[suffix..]);
        } else {
            self.hasher.update(bytes);
        }
        self.next_offset = end;
        Ok(())
    }

    /// Pairs one decoded page with the prior page's final entry.
    ///
    /// # Errors
    ///
    /// Rejects skipped/reordered pages, cross-page key reversal, duplicate
    /// Locations, and one Chunk ID observed with conflicting lengths.
    ///
    /// # Panics
    ///
    /// Panics only if a caller forges an empty `ExactIndexPage`; the public
    /// page decoder rejects that impossible internal state.
    pub fn verify_page(&mut self, page: &ExactIndexPage) -> Result<(), ExactIndexFormatError> {
        if page.ordinal != self.pages_verified {
            return Err(ExactIndexFormatError::InvalidPage);
        }
        let first = page
            .entries
            .first()
            .expect("ASSERT: verified Exact Index pages are never empty");
        if let Some(previous) = self.previous_entry {
            validate_entry_pair(previous, *first)?;
        }
        self.previous_entry = page.entries.last().copied();
        self.pages_verified = self
            .pages_verified
            .checked_add(1)
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        Ok(())
    }

    /// Completes the AUDIT only after every physical byte and page was seen.
    ///
    /// # Errors
    ///
    /// Rejects incomplete input, a missing page, or a full-file hash mismatch.
    #[allow(clippy::missing_const_for_fn)]
    pub fn finish(self) -> Result<(), ExactIndexFormatError> {
        if self.next_offset != self.descriptor.file_length
            || self.pages_verified != self.descriptor.page_count
        {
            return Err(ExactIndexFormatError::NonSequentialAudit);
        }
        if self.hasher.finalize().as_bytes() != &self.descriptor.run_hash {
            return Err(ExactIndexFormatError::RunHashMismatch);
        }
        Ok(())
    }
}

/// Relative position of a complete `(Chunk ID, logical length)` key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactIndexPagePosition {
    Before,
    Within,
    After,
}

/// One independently verified, cache-page-sized Exact Index page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexPage {
    ordinal: usize,
    entries: Vec<ExactIndexEntry>,
}

impl ExactIndexPage {
    #[must_use]
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    #[must_use]
    pub fn entries(&self) -> &[ExactIndexEntry] {
        &self.entries
    }

    /// Compares a complete logical key with this page's inclusive key range.
    ///
    /// # Panics
    ///
    /// Panics only if an internally constructed verified page is empty. The
    /// page decoder rejects that impossible state before construction.
    #[must_use]
    pub fn position(&self, chunk_id: ChunkId, logical_length: u32) -> ExactIndexPagePosition {
        let key = (chunk_id, logical_length);
        let first = self
            .entries
            .first()
            .expect("ASSERT: a verified Exact Index page is never empty");
        let last = self
            .entries
            .last()
            .expect("ASSERT: a verified Exact Index page is never empty");
        if key < (first.chunk_id, first.logical_length) {
            ExactIndexPagePosition::Before
        } else if key > (last.chunk_id, last.logical_length) {
            ExactIndexPagePosition::After
        } else {
            ExactIndexPagePosition::Within
        }
    }

    /// Returns every physical Location transition for one complete key.
    #[must_use]
    pub fn candidates(&self, chunk_id: ChunkId, logical_length: u32) -> &[ExactIndexEntry] {
        let key = (chunk_id, logical_length);
        let start = self
            .entries
            .partition_point(|entry| (entry.chunk_id, entry.logical_length) < key);
        let end = self
            .entries
            .partition_point(|entry| (entry.chunk_id, entry.logical_length) <= key);
        &self.entries[start..end]
    }
}

impl ExactIndexRun {
    /// Canonicalizes and validates one immutable Exact Index run.
    ///
    /// # Errors
    ///
    /// Rejects zero generations, duplicate Locations, Chunk-ID length
    /// conflicts, or a run above the v1 size bound.
    pub fn new(
        profile: ExactIndexProfileId,
        generation: u64,
        mut entries: Vec<ExactIndexEntry>,
    ) -> Result<Self, ExactIndexFormatError> {
        if generation == 0 {
            return Err(ExactIndexFormatError::InvalidGeneration);
        }
        encoded_length(entries.len())?;
        entries.sort_unstable_by_key(ExactIndexEntry::location_key);
        validate_entries(&entries)?;
        Ok(Self {
            profile,
            generation,
            entries,
        })
    }

    #[must_use]
    pub fn entries(&self) -> &[ExactIndexEntry] {
        &self.entries
    }

    #[must_use]
    pub const fn profile(&self) -> ExactIndexProfileId {
        self.profile
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Serializes the complete run with independently checksummed pages.
    ///
    /// # Errors
    ///
    /// Returns a bounded-size or arithmetic error.
    pub fn encode(&self) -> Result<Vec<u8>, ExactIndexFormatError> {
        validate_entries(&self.entries)?;
        let file_length = encoded_length(self.entries.len())?;
        let page_count = page_count(self.entries.len());
        let footer_offset = EXACT_INDEX_HEADER_BYTES
            .checked_add(
                page_count
                    .checked_mul(EXACT_INDEX_PAGE_BYTES)
                    .ok_or(ExactIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
        let mut bytes = vec![0_u8; file_length];
        encode_identity_block(
            &mut bytes[..EXACT_INDEX_HEADER_BYTES],
            HEADER_MAGIC,
            self,
            page_count,
            footer_offset,
            file_length,
        )?;
        let header_crc = checksum_with_zero(&bytes[..EXACT_INDEX_HEADER_BYTES], HEADER_CRC_OFFSET);
        put_u32(&mut bytes, HEADER_CRC_OFFSET, header_crc);

        for (page_ordinal, entries) in self.entries.chunks(ENTRIES_PER_PAGE).enumerate() {
            let start = EXACT_INDEX_HEADER_BYTES + page_ordinal * EXACT_INDEX_PAGE_BYTES;
            encode_page(
                &mut bytes[start..start + EXACT_INDEX_PAGE_BYTES],
                page_ordinal,
                entries,
            )?;
        }
        encode_identity_block(
            &mut bytes[footer_offset..],
            FOOTER_MAGIC,
            self,
            page_count,
            footer_offset,
            file_length,
        )?;
        put_u16(&mut bytes[footer_offset..], 10, PAGE_BYTES_U16);
        let run_hash = calculate_run_hash(&bytes, footer_offset);
        bytes[footer_offset + RUN_HASH_OFFSET..footer_offset + RUN_HASH_OFFSET + 32]
            .copy_from_slice(&run_hash);
        let footer_crc = checksum_with_zero(&bytes[footer_offset..], FOOTER_CRC_OFFSET);
        put_u32(&mut bytes[footer_offset..], FOOTER_CRC_OFFSET, footer_crc);
        Ok(bytes)
    }

    /// Fully validates and decodes one complete immutable run.
    ///
    /// # Errors
    ///
    /// Rejects all geometry, checksum, hash, ordering, identity, reserved-byte,
    /// and bounded-allocation failures.
    ///
    /// # Panics
    ///
    /// Panics only if a previously verified descriptor returns an impossible
    /// page offset for one of its own in-range ordinals.
    pub fn decode(bytes: &[u8]) -> Result<Self, ExactIndexFormatError> {
        if bytes.len() < EXACT_INDEX_HEADER_BYTES + FOOTER_BYTES || bytes.len() > MAX_RUN_BYTES {
            return Err(ExactIndexFormatError::InvalidObjectLength(bytes.len()));
        }
        let footer_offset = bytes.len() - FOOTER_BYTES;
        let descriptor = ExactIndexRunDescriptor::decode(
            &bytes[..EXACT_INDEX_HEADER_BYTES],
            &bytes[footer_offset..],
            u64_from_usize(bytes.len())?,
        )?;
        if calculate_run_hash(bytes, descriptor.footer_offset) != descriptor.run_hash {
            return Err(ExactIndexFormatError::RunHashMismatch);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(descriptor.entry_count)
            .map_err(|_| ExactIndexFormatError::OutOfMemory)?;
        for page_ordinal in 0..descriptor.page_count {
            let start = usize::try_from(
                descriptor
                    .page_offset(page_ordinal)
                    .expect("ASSERT: descriptor page ordinal was prevalidated"),
            )
            .expect("ASSERT: a format-v1 page offset fits usize");
            let page = descriptor
                .decode_page(page_ordinal, &bytes[start..start + EXACT_INDEX_PAGE_BYTES])?;
            entries.extend_from_slice(page.entries());
        }
        validate_entries(&entries)?;
        if entries.len() != descriptor.entry_count || key_bounds(&entries) != descriptor.key_bounds
        {
            return Err(ExactIndexFormatError::InvalidHeader);
        }
        Ok(Self {
            profile: descriptor.profile,
            generation: descriptor.generation,
            entries,
        })
    }
}

fn encode_identity_block(
    block: &mut [u8],
    magic: [u8; 8],
    run: &ExactIndexRun,
    page_count: usize,
    footer_offset: usize,
    file_length: usize,
) -> Result<(), ExactIndexFormatError> {
    encode_identity_fields(
        block,
        magic,
        run.profile,
        run.generation,
        run.entries.len(),
        page_count,
        key_bounds(&run.entries),
        footer_offset,
        file_length,
    )
}

#[allow(clippy::too_many_arguments)]
fn encode_identity_fields(
    block: &mut [u8],
    magic: [u8; 8],
    profile: ExactIndexProfileId,
    generation: u64,
    entry_count: usize,
    page_count: usize,
    key_bounds: [u8; 64],
    footer_offset: usize,
    file_length: usize,
) -> Result<(), ExactIndexFormatError> {
    block[0..8].copy_from_slice(&magic);
    put_u16(block, 8, FORMAT_VERSION);
    put_u16(block, 10, HEADER_BYTES_U16);
    put_u16(block, 12, PAGE_BYTES_U16);
    put_u16(block, 14, ENTRY_BYTES_U16);
    put_u64(block, 32, generation);
    put_u64(
        block,
        40,
        u64::try_from(entry_count).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        block,
        48,
        u64::try_from(page_count).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    put_u32(block, 56, ENTRIES_PER_PAGE_U32);
    put_u32(block, 60, 1);
    block[64..96].copy_from_slice(&profile.bytes());
    block[96..160].copy_from_slice(&key_bounds);
    put_u64(block, 160, HEADER_BYTES_U64);
    put_u64(
        block,
        168,
        u64::try_from(footer_offset).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        block,
        176,
        u64::try_from(file_length).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    Ok(())
}

fn validate_identity_block(
    block: &[u8],
    magic: [u8; 8],
    crc_offset: usize,
) -> Result<(), ExactIndexFormatError> {
    if block.len() != EXACT_INDEX_HEADER_BYTES
        || block[0..8] != magic
        || get_u16(block, 8) != FORMAT_VERSION
        || usize::from(get_u16(block, 12)) != EXACT_INDEX_PAGE_BYTES
        || usize::from(get_u16(block, 14)) != EXACT_INDEX_ENTRY_BYTES
        || get_u64(block, 16) != 0
        || get_u64(block, 24) != 0
        || usize::try_from(get_u32(block, 56)).ok() != Some(ENTRIES_PER_PAGE)
        || get_u32(block, 60) != 1
        || checksum_with_zero(block, crc_offset) != get_u32(block, crc_offset)
    {
        return Err(ExactIndexFormatError::InvalidHeader);
    }
    if magic == HEADER_MAGIC
        && (usize::from(get_u16(block, 10)) != EXACT_INDEX_HEADER_BYTES
            || block[188..].iter().any(|byte| *byte != 0))
    {
        return Err(ExactIndexFormatError::InvalidHeader);
    }
    Ok(())
}

fn encode_page(
    page: &mut [u8],
    ordinal: usize,
    entries: &[ExactIndexEntry],
) -> Result<(), ExactIndexFormatError> {
    page[0..8].copy_from_slice(&PAGE_MAGIC);
    put_u16(page, 8, FORMAT_VERSION);
    put_u16(page, 10, PAGE_HEADER_BYTES_U16);
    put_u16(page, 12, ENTRY_BYTES_U16);
    put_u16(
        page,
        14,
        u16::try_from(entries.len()).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        page,
        16,
        u32::try_from(ordinal).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    );
    let first_entry_ordinal = ordinal
        .checked_mul(ENTRIES_PER_PAGE)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
    put_u64(page, 24, first_entry_ordinal);
    let first = entries.first().ok_or(ExactIndexFormatError::InvalidPage)?;
    let last = entries.last().ok_or(ExactIndexFormatError::InvalidPage)?;
    page[32..64].copy_from_slice(&first.chunk_id.bytes());
    put_u32(page, 64, first.logical_length);
    page[72..104].copy_from_slice(&last.chunk_id.bytes());
    put_u32(page, 104, last.logical_length);
    for (index, entry) in entries.iter().copied().enumerate() {
        let start = PAGE_HEADER_BYTES + index * EXACT_INDEX_ENTRY_BYTES;
        entry.encode(&mut page[start..start + EXACT_INDEX_ENTRY_BYTES]);
    }
    let crc = checksum_with_zero(page, PAGE_CRC_OFFSET);
    put_u32(page, PAGE_CRC_OFFSET, crc);
    Ok(())
}

fn decode_page(
    page: &[u8],
    ordinal: usize,
    total_entries: usize,
    output: &mut Vec<ExactIndexEntry>,
) -> Result<(), ExactIndexFormatError> {
    let remaining = total_entries.saturating_sub(ordinal * ENTRIES_PER_PAGE);
    let expected = remaining.min(ENTRIES_PER_PAGE);
    if page.len() != EXACT_INDEX_PAGE_BYTES
        || page[0..8] != PAGE_MAGIC
        || get_u16(page, 8) != FORMAT_VERSION
        || usize::from(get_u16(page, 10)) != PAGE_HEADER_BYTES
        || usize::from(get_u16(page, 12)) != EXACT_INDEX_ENTRY_BYTES
        || usize::from(get_u16(page, 14)) != expected
        || usize::try_from(get_u32(page, 16)).ok() != Some(ordinal)
        || usize::try_from(get_u64(page, 24)).ok() != Some(ordinal * ENTRIES_PER_PAGE)
        || page[108..PAGE_HEADER_BYTES].iter().any(|byte| *byte != 0)
        || checksum_with_zero(page, PAGE_CRC_OFFSET) != get_u32(page, PAGE_CRC_OFFSET)
    {
        return Err(ExactIndexFormatError::InvalidPage);
    }
    let base = output.len();
    for index in 0..expected {
        let start = PAGE_HEADER_BYTES + index * EXACT_INDEX_ENTRY_BYTES;
        output.push(ExactIndexEntry::decode(
            &page[start..start + EXACT_INDEX_ENTRY_BYTES],
        )?);
    }
    if page[PAGE_HEADER_BYTES + expected * EXACT_INDEX_ENTRY_BYTES..]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(ExactIndexFormatError::InvalidPage);
    }
    let page_entries = &output[base..];
    let first = page_entries
        .first()
        .ok_or(ExactIndexFormatError::InvalidPage)?;
    let last = page_entries
        .last()
        .ok_or(ExactIndexFormatError::InvalidPage)?;
    if page[32..64] != first.chunk_id.bytes()
        || get_u32(page, 64) != first.logical_length
        || page[68..72].iter().any(|byte| *byte != 0)
        || page[72..104] != last.chunk_id.bytes()
        || get_u32(page, 104) != last.logical_length
    {
        return Err(ExactIndexFormatError::InvalidPage);
    }
    Ok(())
}

fn validate_entries(entries: &[ExactIndexEntry]) -> Result<(), ExactIndexFormatError> {
    for pair in entries.windows(2) {
        validate_entry_pair(pair[0], pair[1])?;
    }
    Ok(())
}

fn validate_entry_pair(
    previous: ExactIndexEntry,
    next: ExactIndexEntry,
) -> Result<(), ExactIndexFormatError> {
    if previous.chunk_id == next.chunk_id && previous.logical_length != next.logical_length {
        return Err(ExactIndexFormatError::ChunkLengthConflict);
    }
    if previous.location_key() >= next.location_key() {
        return Err(ExactIndexFormatError::NonCanonicalOrder);
    }
    Ok(())
}

fn page_count(entry_count: usize) -> usize {
    entry_count.div_ceil(ENTRIES_PER_PAGE)
}

fn valid_raw_location(record_offset: u64, record_length: u32) -> bool {
    record_offset >= HEADER_BYTES_U64
        && record_offset.is_multiple_of(u64::from(RECORD_ALIGNMENT_U32))
        && record_length >= MIN_RAW_RECORD_BYTES_U32
        && record_length.is_multiple_of(RECORD_ALIGNMENT_U32)
        && usize::try_from(record_length).is_ok_and(|length| length <= MAX_RECORD_BYTES)
        && record_offset
            .checked_add(u64::from(record_length))
            .is_some_and(|end| end <= MAX_CONTAINER_BYTES)
}

fn valid_location(logical_length: u32, location: ExactIndexLocation) -> bool {
    if logical_length == 0
        || usize::try_from(logical_length).map_or(true, |length| length > MAX_LOGICAL_CHUNK_BYTES)
        || !valid_raw_location(location.record_offset, location.record_length)
        || location.container_generation == 0
    {
        return false;
    }
    match location.codec_id {
        RAW_CODEC => {
            location.dependency_id == [0; 32]
                && location.chunk_ordinal == 0
                && location.decoded_offset == 0
                && location.record_decoded_length == logical_length
                && location.record_payload_length == logical_length
                && expected_raw_record_length(logical_length) == Some(location.record_length)
        }
        ZSTD_CODEC => {
            location.dependency_id == [0; 32]
                && location.record_decoded_length > 0
                && usize::try_from(location.record_decoded_length)
                    .is_ok_and(|length| length <= MAX_DECODED_RECORD_BYTES)
                && location.record_payload_length > 0
                && location.record_payload_length < location.record_length
                && location
                    .decoded_offset
                    .checked_add(logical_length)
                    .is_some_and(|end| end <= location.record_decoded_length)
        }
        ZSTD_PREFIX_CODEC | SPARSE_XOR_CODEC => {
            location.dependency_id != [0; 32]
                && location.chunk_ordinal == 0
                && location.decoded_offset == 0
                && location.record_decoded_length == logical_length
                && location.record_payload_length > 0
                && location.record_payload_length < location.record_length
        }
        _ => false,
    }
}

fn expected_raw_record_length(logical_length: u32) -> Option<u32> {
    if logical_length == 0
        || usize::try_from(logical_length).map_or(true, |length| length > MAX_LOGICAL_CHUNK_BYTES)
    {
        return None;
    }
    logical_length
        .checked_add(RAW_PAYLOAD_OFFSET_U32)?
        .checked_add(RECORD_ALIGNMENT_U32 - 1)
        .map(|length| length / RECORD_ALIGNMENT_U32 * RECORD_ALIGNMENT_U32)
}

fn encoded_length(entry_count: usize) -> Result<usize, ExactIndexFormatError> {
    let length = EXACT_INDEX_HEADER_BYTES
        .checked_add(
            page_count(entry_count)
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .ok_or(ExactIndexFormatError::ArithmeticOverflow)?,
        )
        .and_then(|value| value.checked_add(FOOTER_BYTES))
        .ok_or(ExactIndexFormatError::ArithmeticOverflow)?;
    if length > MAX_RUN_BYTES {
        return Err(ExactIndexFormatError::InvalidObjectLength(length));
    }
    Ok(length)
}

fn u64_from_usize(value: usize) -> Result<u64, ExactIndexFormatError> {
    u64::try_from(value).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)
}

fn key_bounds(entries: &[ExactIndexEntry]) -> [u8; 64] {
    let mut bounds = [0_u8; 64];
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        bounds[..32].copy_from_slice(&first.chunk_id.bytes());
        bounds[32..].copy_from_slice(&last.chunk_id.bytes());
    }
    bounds
}

fn checksum_with_zero(bytes: &[u8], offset: usize) -> u32 {
    let checksum = crc32c::crc32c_append(0, &bytes[..offset]);
    let checksum = crc32c::crc32c_append(checksum, &[0_u8; 4]);
    crc32c::crc32c_append(checksum, &bytes[offset + 4..])
}

fn calculate_run_hash(bytes: &[u8], footer_offset: usize) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    let zero_start = footer_offset + RUN_HASH_OFFSET;
    let zero_end = footer_offset + FOOTER_CRC_OFFSET + 4;
    hasher.update(&bytes[..zero_start]);
    hasher.update(&[0_u8; 36]);
    hasher.update(&bytes[zero_end..]);
    *hasher.finalize().as_bytes()
}

const fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

const fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

const fn get_u64(bytes: &[u8], offset: usize) -> u64 {
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

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactIndexFormatError {
    InvalidGeneration,
    InvalidEntry,
    ChunkLengthConflict,
    NonCanonicalOrder,
    InvalidObjectLength(usize),
    InvalidHeader,
    InvalidPage,
    HeaderFooterMismatch,
    RunHashMismatch,
    ArithmeticOverflow,
    OutOfMemory,
    NonSequentialAudit,
}

impl fmt::Display for ExactIndexFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactIndexFormatError {}
