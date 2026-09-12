//! Field-by-field envelope/index serialization and structural commitment.
use super::dependent::is_dependent_codec;
use super::summary::ContainerIntrinsicSummary;
use super::{
    BLAKE3_256_ALGORITHM, BLAKE3_STRUCTURAL_COMMITMENT_ALGORITHM, CHUNK_TABLE_ENTRY_BYTES,
    CONTAINER_COMMITMENT_DOMAIN_V1, CONTAINER_FORMAT_VERSION, CONTAINER_SUMMARY_BYTES,
    CRC32C_ALGORITHM, ChunkId, ContainerId, ContainerLayout, FOOTER_BYTES, FOOTER_BYTES_USIZE,
    FOOTER_CRC_OFFSET, FOOTER_HASH_OFFSET, FOOTER_MAGIC, FOOTER_SUMMARY_OFFSET, FORMAT_VERSION,
    FormatError, HEADER_BYTES, HEADER_BYTES_U16, HEADER_CRC_OFFSET, HEADER_MAGIC,
    HEADER_SUMMARY_OFFSET, INDEX_CRC_OFFSET, INDEX_ENTRY_BYTES, INDEX_ENTRY_BYTES_USIZE,
    INDEX_HEADER_BYTES, INDEX_HEADER_BYTES_USIZE, INDEX_MAGIC, MAX_CONTAINER_BYTES,
    MAX_DECODED_RECORD_BYTES, MAX_LOGICAL_CHUNK_BYTES, MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES,
    RAW_CODEC, RECORD_ALIGNMENT, RECORD_CRC_OFFSET, RECORD_HEADER_BYTES, SEALED_STATE,
    SPARSE_XOR_CODEC, ZSTD_CODEC, ZSTD_PREFIX_CODEC, align_up, crc32c_with_zeroed_field, get_u16,
    get_u32, get_u64, put_u16, put_u32, put_u64,
};
use crate::crc32c_with_zeroed_u32;

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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct IndexEntry {
    pub(super) chunk_id: ChunkId,
    pub(super) logical_length: u32,
    pub(super) record_offset: u64,
    pub(super) chunk_ordinal: u32,
    pub(super) decoded_offset: u32,
    pub(super) record_length: u32,
    pub(super) codec_id: u16,
    pub(super) dependency_id: [u8; 32],
    pub(super) record_crc32c: u32,
    pub(super) record_decoded_length: u32,
    pub(super) record_payload_length: u32,
}

impl IndexEntry {
    pub(super) fn from_encoded_record(
        bytes: &[u8],
        record_offset: u64,
    ) -> Result<Vec<Self>, FormatError> {
        let mut entries = Vec::new();
        Self::append_from_encoded_record(bytes, record_offset, &mut entries)?;
        Ok(entries)
    }

    pub(super) fn append_from_encoded_record(
        bytes: &[u8],
        record_offset: u64,
        entries: &mut Vec<Self>,
    ) -> Result<(), FormatError> {
        let codec_id = get_u16(bytes, 12);
        let dependency_id = if is_dependent_codec(codec_id) {
            bytes[64..96]
                .try_into()
                .expect("ASSERT: fixed dependency field is 32 bytes")
        } else {
            [0; 32]
        };
        let chunk_count =
            usize::try_from(get_u32(bytes, 56)).map_err(|_| FormatError::ArithmeticOverflow)?;
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
        Ok(())
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
        if !matches!(
            codec_id,
            RAW_CODEC | ZSTD_CODEC | ZSTD_PREFIX_CODEC | SPARSE_XOR_CODEC
        ) || get_u16(bytes, 58) != 0
            || (is_dependent_codec(codec_id) && dependency_id == [0; 32])
            || (!is_dependent_codec(codec_id) && dependency_id != [0; 32])
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
pub(super) struct Footer {
    pub(super) container_id: ContainerId,
    pub(super) container_generation: u64,
    pub(super) layout: ContainerLayout,
    pub(super) intrinsic_summary: ContainerIntrinsicSummary,
    pub(super) container_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerHeader {
    pub(super) container_id: ContainerId,
    pub(super) container_generation: u64,
    pub(super) layout: ContainerLayout,
}

impl ContainerHeader {
    /// Constructs a sealed header after validating all layout equations.
    ///
    /// # Errors
    ///
    /// Returns an error for zero generation, overflow, or an invalid layout.
    pub(super) fn sealed(
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

    pub(super) fn encode(
        &self,
        intrinsic_summary: ContainerIntrinsicSummary,
    ) -> [u8; HEADER_BYTES] {
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

    pub(super) fn decode_with_summary(
        bytes: &[u8],
    ) -> Result<(Self, ContainerIntrinsicSummary), FormatError> {
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

pub(super) fn validate_layout(layout: ContainerLayout) -> Result<(), FormatError> {
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

pub(super) fn validate_container_file_length(length: usize) -> Result<(), FormatError> {
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

pub(super) fn valid_recovery_index_entry_geometry(
    layout: ContainerLayout,
    entry: IndexEntry,
) -> bool {
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

pub(super) fn encode_index(entries: &[IndexEntry]) -> Result<Vec<u8>, FormatError> {
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

pub(super) fn decode_index(
    bytes: &[u8],
    expected_count: u32,
) -> Result<Vec<IndexEntry>, FormatError> {
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

pub(super) fn encode_footer(
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

pub(super) fn decode_footer(bytes: &[u8]) -> Result<Footer, FormatError> {
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

pub(super) fn calculate_container_commitment(
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
