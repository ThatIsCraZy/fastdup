use core::fmt;
use std::cell::RefCell;
use std::num::NonZeroUsize;

use rayon::prelude::*;

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
const SEALED_STATE: u16 = 2;
const CRC32C_ALGORITHM: u16 = 1;
const BLAKE3_256_ALGORITHM: u16 = 1;
const RECORD_ALIGNMENT: u16 = 64;
const INDEX_HEADER_BYTES: u64 = 64;
const INDEX_ENTRY_BYTES: u64 = 128;
const HEADER_CRC_OFFSET: usize = 104;
const RECORD_MAGIC: &[u8; 8] = b"FDRECD01";
pub(crate) const RAW_CODEC: u16 = 1;
pub(crate) const ZSTD_CODEC: u16 = 2;
const ZSTD_LEVEL_V1: i32 = 3;
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

thread_local! {
    static ZSTD_ENCODER_V1: RefCell<Option<zstd::bulk::Compressor<'static>>> =
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
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, 1);
        put_u16(&mut bytes, 14, CRC32C_ALGORITHM);
        put_u16(&mut bytes, 16, BLAKE3_256_ALGORITHM);
        put_u16(&mut bytes, 18, BLAKE3_256_ALGORITHM);
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
        let actual_length_usize = usize::try_from(actual_length)
            .map_err(|_| FormatError::InvalidContainerLength(usize::MAX))?;
        validate_container_file_length(actual_length_usize)?;
        let footer = decode_footer(footer_bytes)?;
        let header = ContainerHeader::decode(header_bytes)?;
        let expected_footer_offset = actual_length
            .checked_sub(FOOTER_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if header.container_id != footer.container_id
            || header.container_generation != footer.container_generation
            || header.layout != footer.layout
            || header.layout.footer_offset != expected_footer_offset
            || header.layout.file_length != actual_length
        {
            return Err(FormatError::HeaderFooterMismatch);
        }
        Ok(Self {
            header,
            container_hash: footer.container_hash,
        })
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
            || !matches!(location.codec_id(), RAW_CODEC | ZSTD_CODEC)
            || location.record_decoded_length() == 0
            || usize::try_from(location.record_decoded_length())
                .map_or(true, |length| length > MAX_DECODED_RECORD_BYTES)
            || location.record_payload_length() == 0
            || location.record_payload_length() > location.record_length()
            || location
                .decoded_offset()
                .checked_add(candidate.logical_length())
                .is_none_or(|end| end > location.record_decoded_length())
            || location.dependency_id() != [0; 32]
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

    /// Backward-compatible RAW-only range validator.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ExactLocationMismatch`] for a non-RAW candidate
    /// or any failure reported by [`Self::record_range`].
    pub fn raw_record_range(
        self,
        candidate: ExactIndexEntry,
    ) -> Result<ContainerRecordRange, FormatError> {
        if candidate.location().codec_id() != RAW_CODEC {
            return Err(FormatError::ExactLocationMismatch);
        }
        self.record_range(candidate)
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
        let range = self.record_range(candidate)?;
        let location = candidate.location();
        if record_bytes.len() != range.length
            || get_u16(record_bytes, 12) != location.codec_id()
            || get_u32(record_bytes, 32) != location.record_length()
            || get_u32(record_bytes, 36) != location.record_decoded_length()
            || get_u32(record_bytes, 44) != location.record_payload_length()
            || get_u32(record_bytes, RECORD_CRC_OFFSET) != location.record_crc32c()
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        let chunk_count = usize::try_from(get_u32(record_bytes, 56))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
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
        let decoded = decode_encoding_record(record_bytes)?;
        let record = decoded
            .chunks
            .into_iter()
            .nth(ordinal)
            .ok_or(FormatError::ExactLocationMismatch)?;
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
        let mut encoded_records = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_records.push(RawRecord::encode(chunk)?);
        }
        encode_container_from_records(container_id, container_generation, encoded_records)
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
        encode_container_from_records(container_id, container_generation, encoded_records)
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
        if regions.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let worker_count = workers.get().min(regions.len());
        let encoded_by_region = (0..worker_count)
            .into_par_iter()
            .map(|worker| {
                let mut completed = Vec::new();
                for ordinal in (worker..regions.len()).step_by(worker_count) {
                    completed.push((ordinal, encode_adaptive_region(regions[ordinal])?));
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
        for region in encoded_by_region {
            encoded_records.extend(
                region.expect("ASSERT: every Compression Region worker must return its output"),
            );
        }
        encode_container_from_records(container_id, container_generation, encoded_records)
    }

    /// Fully validates and decodes one sealed container image.
    ///
    /// # Errors
    ///
    /// Returns the first structural, checksum, index, or content-integrity error.
    #[allow(clippy::too_many_lines)]
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        validate_container_file_length(bytes.len())?;
        let footer_offset = bytes
            .len()
            .checked_sub(FOOTER_BYTES_USIZE)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let footer = decode_footer(&bytes[footer_offset..])?;
        let header = ContainerHeader::decode(&bytes[..HEADER_BYTES])?;
        if header.container_id != footer.container_id
            || header.container_generation != footer.container_generation
            || header.layout != footer.layout
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
            let decoded = decode_encoding_record(encoded)?;
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
            }
            expected_entries.extend(index_entries);
            records.extend(decoded.chunks);
            cursor = end;
        }
        if cursor != index_offset {
            return Err(FormatError::InvalidContainerLayout);
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
        let computed_hash = calculate_container_hash(bytes, footer_offset);
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
        })
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
            .find(|record| record.chunk_id == chunk_id)
            .map(RawRecord::payload)
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexEntry {
    chunk_id: ChunkId,
    logical_length: u32,
    record_offset: u64,
    chunk_ordinal: u32,
    decoded_offset: u32,
    record_length: u32,
    codec_id: u16,
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
}

impl IndexEntry {
    fn from_encoded_record(bytes: &[u8], record_offset: u64) -> Result<Vec<Self>, FormatError> {
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
                codec_id: get_u16(bytes, 12),
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
        put_u32(output, 96, self.record_decoded_length);
        put_u32(output, 100, self.record_payload_length);
    }

    fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() != INDEX_ENTRY_BYTES_USIZE
            || !matches!(get_u16(bytes, 56), RAW_CODEC | ZSTD_CODEC)
            || get_u16(bytes, 58) != 0
            || bytes[64..96].iter().any(|byte| *byte != 0)
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
            codec_id: get_u16(bytes, 56),
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
    container_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
enum EncodingCodec {
    Raw,
    Zstd,
}

#[derive(Debug)]
struct DecodedEncodingRecord {
    codec: EncodingCodec,
    chunks: Vec<RawRecord>,
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region(region: &[&[u8]]) -> Result<Vec<Vec<u8>>, FormatError> {
    if region.is_empty() {
        return Err(FormatError::InvalidZstdRecord);
    }
    let zstd = encode_zstd_record(region, ZSTD_LEVEL_V1)?;
    let mut raw_records = Vec::new();
    raw_records
        .try_reserve_exact(region.len())
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut raw_bytes = 0_usize;
    for chunk in region {
        let raw = RawRecord::encode(chunk)?;
        raw_bytes = raw_bytes
            .checked_add(raw.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
        raw_records.push(raw);
    }
    if zstd_record_wins(raw_bytes, zstd.len())? {
        Ok(vec![zstd])
    } else {
        Ok(raw_records)
    }
}

#[allow(clippy::too_many_lines)]
fn encode_zstd_record(chunks: &[&[u8]], level: i32) -> Result<Vec<u8>, FormatError> {
    if chunks.is_empty() || level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let mut decoded_length = 0_usize;
    for chunk in chunks {
        validate_logical_chunk_length(chunk.len())?;
        decoded_length = decoded_length
            .checked_add(chunk.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    if decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_length)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    for chunk in chunks {
        decoded.extend_from_slice(chunk);
    }
    let payload = compress_zstd_v1(&decoded, level)?;
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
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    let mut bytes = vec![0_u8; record_length];
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(&mut bytes, 8, FORMAT_VERSION);
    put_u16(&mut bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(&mut bytes, 12, ZSTD_CODEC);
    put_u16(&mut bytes, 14, 0);
    put_u32(
        &mut bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut bytes,
        36,
        u32::try_from(decoded_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut bytes,
        40,
        u32::try_from(payload_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut bytes,
        44,
        u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(&mut bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(&mut bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(
        &mut bytes,
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
        bytes[table_offset..table_offset + 32].copy_from_slice(&ChunkId::of(chunk).0);
        put_u32(
            &mut bytes,
            table_offset + 32,
            u32::try_from(decoded_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut bytes,
            table_offset + 36,
            u32::try_from(chunk.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        decoded_offset = decoded_offset
            .checked_add(chunk.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    bytes[payload_offset..payload_end].copy_from_slice(&payload);
    let checksum = crc32c::crc32c(&bytes);
    put_u32(&mut bytes, RECORD_CRC_OFFSET, checksum);
    let verified = decode_encoding_record(&bytes)?;
    if verified.codec != EncodingCodec::Zstd
        || verified.chunks.len() != chunks.len()
        || verified
            .chunks
            .iter()
            .zip(chunks)
            .any(|(observed, expected)| observed.payload() != *expected)
    {
        return Err(FormatError::InvalidZstdRecord);
    }
    Ok(bytes)
}

fn compress_zstd_v1(decoded: &[u8], level: i32) -> Result<Vec<u8>, FormatError> {
    if level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    ZSTD_ENCODER_V1.with(|encoder| {
        let mut encoder = encoder.borrow_mut();
        if encoder.is_none() {
            *encoder = Some(
                zstd::bulk::Compressor::new(ZSTD_LEVEL_V1).map_err(|_| FormatError::ZstdFailure)?,
            );
        }
        encoder
            .as_mut()
            .expect("ASSERT: worker-local Zstd encoder was initialized")
            .compress(decoded)
            .map_err(|_| FormatError::ZstdFailure)
    })
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
        || bytes[64..96].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdRecord);
    }
    if get_u16(bytes, 12) == RAW_CODEC {
        let record = RawRecord::decode(bytes)?;
        return Ok(DecodedEncodingRecord {
            codec: EncodingCodec::Raw,
            chunks: vec![record],
        });
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
    let mut chunks = Vec::new();
    chunks
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
        let payload = decoded[decoded_offset..decoded_end].to_vec();
        let mut stored_id = [0_u8; 32];
        stored_id.copy_from_slice(&bytes[table_offset..table_offset + 32]);
        let chunk_id = ChunkId::of(&payload);
        if chunk_id.0 != stored_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        chunks.push(RawRecord { chunk_id, payload });
        expected_decoded_offset = decoded_end;
    }
    if expected_decoded_offset != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    Ok(DecodedEncodingRecord {
        codec: EncodingCodec::Zstd,
        chunks,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRecord {
    chunk_id: ChunkId,
    payload: Vec<u8>,
}

impl RawRecord {
    /// Encodes one nonempty logical chunk as a v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns an error when the chunk or resulting record exceeds v1 bounds.
    pub fn encode(payload: &[u8]) -> Result<Vec<u8>, FormatError> {
        let record_length = raw_record_length(payload.len())?;
        let record_length_u32 =
            u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_length_u32 =
            u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut bytes = vec![0_u8; record_length];
        bytes[0..8].copy_from_slice(RECORD_MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, RECORD_HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, RAW_CODEC);
        put_u16(&mut bytes, 14, 0);
        put_u32(&mut bytes, 32, record_length_u32);
        put_u32(&mut bytes, 36, payload_length_u32);
        put_u32(&mut bytes, 40, RAW_PAYLOAD_OFFSET_U32);
        put_u32(&mut bytes, 44, payload_length_u32);
        put_u32(&mut bytes, 48, RECORD_HEADER_BYTES_U32);
        put_u16(&mut bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
        put_u32(&mut bytes, 56, 1);

        let chunk_id = ChunkId::of(payload);
        bytes[128..160].copy_from_slice(&chunk_id.0);
        put_u32(&mut bytes, 160, 0);
        put_u32(&mut bytes, 164, payload_length_u32);
        bytes[RAW_PAYLOAD_OFFSET..RAW_PAYLOAD_OFFSET + payload.len()].copy_from_slice(payload);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, RECORD_CRC_OFFSET, checksum);
        Ok(bytes)
    }

    /// Validates and decodes one v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, or logical Chunk-ID integrity error.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
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
        let mut checksummed = bytes.to_vec();
        checksummed[RECORD_CRC_OFFSET..RECORD_CRC_OFFSET + 4].fill(0);
        if crc32c::crc32c(&checksummed) != stored_checksum {
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
        if expected_record_length != bytes.len()
            || bytes[payload_end..].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::InvalidRawRecord);
        }

        let mut stored_id = [0_u8; 32];
        stored_id.copy_from_slice(&bytes[128..160]);
        let payload = bytes[RAW_PAYLOAD_OFFSET..payload_end].to_vec();
        let chunk_id = ChunkId::of(&payload);
        if chunk_id.0 != stored_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        Ok(Self { chunk_id, payload })
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
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
    pub fn sealed(
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

    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0_u8; HEADER_BYTES];
        bytes[0..8].copy_from_slice(HEADER_MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, SEALED_STATE);
        put_u16(&mut bytes, 14, CRC32C_ALGORITHM);
        put_u16(&mut bytes, 16, BLAKE3_256_ALGORITHM);
        put_u16(&mut bytes, 18, BLAKE3_256_ALGORITHM);
        put_u16(&mut bytes, 20, RECORD_ALIGNMENT);
        bytes[40..56].copy_from_slice(&self.container_id.0);
        put_u64(&mut bytes, 56, self.container_generation);
        put_u32(&mut bytes, 64, self.layout.record_count);
        put_u32(&mut bytes, 68, self.layout.chunk_entry_count);
        put_u64(&mut bytes, 72, self.layout.index_offset);
        put_u64(&mut bytes, 80, self.layout.index_length);
        put_u64(&mut bytes, 88, self.layout.footer_offset);
        put_u64(&mut bytes, 96, self.layout.file_length);
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
        if bytes.len() != HEADER_BYTES {
            return Err(FormatError::InvalidHeaderLength(bytes.len()));
        }
        if &bytes[0..8] != HEADER_MAGIC {
            return Err(FormatError::InvalidHeaderMagic);
        }
        let stored_checksum = get_u32(bytes, HEADER_CRC_OFFSET);
        let mut checksummed = [0_u8; HEADER_BYTES];
        checksummed.copy_from_slice(bytes);
        checksummed[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].fill(0);
        if crc32c::crc32c(&checksummed) != stored_checksum {
            return Err(FormatError::HeaderChecksumMismatch);
        }
        if get_u16(bytes, 12) == 1 {
            return Err(FormatError::ContainerNotSealed);
        }
        validate_header_constants(bytes)?;
        if bytes[22..24] != [0; 2]
            || bytes[24..40] != [0; 16]
            || bytes[108..].iter().any(|byte| *byte != 0)
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
        Self::sealed(container_id, generation, layout)
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
    ZstdFailure,
    ChunkHashMismatch,
    InvalidContainerLength(usize),
    InvalidFooter,
    FooterChecksumMismatch,
    HeaderFooterMismatch,
    InvalidRecoveryIndex,
    IndexChecksumMismatch,
    IndexRecordMismatch,
    ExactLocationMismatch,
    NonZeroContainerPadding,
    ContainerHashMismatch,
    ContainerNotSealed,
    UnsupportedHeaderField,
    NonZeroHeaderReserved,
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
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != HEADER_BYTES
        || get_u16(bytes, 12) != SEALED_STATE
        || get_u16(bytes, 14) != CRC32C_ALGORITHM
        || get_u16(bytes, 16) != BLAKE3_256_ALGORITHM
        || get_u16(bytes, 18) != BLAKE3_256_ALGORITHM
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

fn encode_footer(output: &mut [u8], header: &ContainerHeader) {
    assert_eq!(output.len(), FOOTER_BYTES_USIZE);
    output[0..8].copy_from_slice(FOOTER_MAGIC);
    put_u16(output, 8, FORMAT_VERSION);
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
}

fn decode_footer(bytes: &[u8]) -> Result<Footer, FormatError> {
    if bytes.len() != FOOTER_BYTES_USIZE || &bytes[0..8] != FOOTER_MAGIC {
        return Err(FormatError::InvalidFooter);
    }
    let stored_checksum = get_u32(bytes, FOOTER_CRC_OFFSET);
    if crc32c_with_zeroed_field(bytes, FOOTER_CRC_OFFSET) != stored_checksum {
        return Err(FormatError::FooterChecksumMismatch);
    }
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != FOOTER_BYTES_USIZE
        || &bytes[12..16] != b"SEAL"
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || bytes[132..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidFooter);
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[32..48]);
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(&bytes[FOOTER_HASH_OFFSET..FOOTER_HASH_OFFSET + 32]);
    Ok(Footer {
        container_id: ContainerId::new(id)?,
        container_generation: get_u64(bytes, 48),
        layout: ContainerLayout {
            record_count: get_u32(bytes, 64),
            chunk_entry_count: get_u32(bytes, 68),
            index_offset: get_u64(bytes, 72),
            index_length: get_u64(bytes, 80),
            footer_offset: get_u64(bytes, 88),
            file_length: get_u64(bytes, 56),
        },
        container_hash: hash,
    })
}

fn calculate_container_hash(bytes: &[u8], footer_offset: usize) -> [u8; 32] {
    let hash_start = footer_offset + FOOTER_HASH_OFFSET;
    let checksum_end = footer_offset + FOOTER_CRC_OFFSET + 4;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[..hash_start]);
    hasher.update(&[0_u8; 36]);
    hasher.update(&bytes[checksum_end..]);
    *hasher.finalize().as_bytes()
}

fn crc32c_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    let mut checksummed = bytes.to_vec();
    checksummed[field_offset..field_offset + 4].fill(0);
    crc32c::crc32c(&checksummed)
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

fn encoded_container_layout(records: &[Vec<u8>]) -> Result<ContainerLayout, FormatError> {
    if records.is_empty() {
        return Err(FormatError::InvalidContainerLayout);
    }
    let record_count = u32::try_from(records.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut chunk_entry_count = 0_u32;
    let mut index_offset =
        u64::try_from(HEADER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?;
    for record in records {
        decode_encoding_record(record)?;
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
    Ok(layout)
}

fn encode_container_from_records(
    container_id: ContainerId,
    container_generation: u64,
    encoded_records: Vec<Vec<u8>>,
) -> Result<Vec<u8>, FormatError> {
    let layout = encoded_container_layout(&encoded_records)?;
    let mut record_offset = HEADER_BYTES as u64;
    let mut index_entries = Vec::new();
    index_entries
        .try_reserve_exact(
            usize::try_from(layout.chunk_entry_count)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        )
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    for encoded in &encoded_records {
        index_entries.extend(IndexEntry::from_encoded_record(encoded, record_offset)?);
        record_offset = record_offset
            .checked_add(u64::try_from(encoded.len()).map_err(|_| FormatError::ArithmeticOverflow)?)
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    index_entries.sort_unstable();
    let index = encode_index(&index_entries)?;
    assert_eq!(record_offset, layout.index_offset);
    assert_eq!(u64::try_from(index.len()), Ok(layout.index_length));
    let header = ContainerHeader::sealed(container_id, container_generation, layout)?;
    let file_length_usize =
        usize::try_from(layout.file_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let footer_offset_usize =
        usize::try_from(layout.footer_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut container = vec![0_u8; file_length_usize];
    container[0..HEADER_BYTES].copy_from_slice(&header.encode());
    let mut cursor = HEADER_BYTES;
    for record in encoded_records {
        let end = cursor
            .checked_add(record.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
        container[cursor..end].copy_from_slice(&record);
        cursor = end;
    }
    let index_end = cursor
        .checked_add(index.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    container[cursor..index_end].copy_from_slice(&index);
    encode_footer(&mut container[footer_offset_usize..], &header);
    let hash = calculate_container_hash(&container, footer_offset_usize);
    container
        [footer_offset_usize + FOOTER_HASH_OFFSET..footer_offset_usize + FOOTER_HASH_OFFSET + 32]
        .copy_from_slice(&hash);
    let footer_checksum =
        crc32c_with_zeroed_field(&container[footer_offset_usize..], FOOTER_CRC_OFFSET);
    put_u32(
        &mut container[footer_offset_usize..],
        FOOTER_CRC_OFFSET,
        footer_checksum,
    );
    Ok(container)
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
