//! Payload-free structural validation. This is deliberately not a content proof.
use super::{
    CHUNK_TABLE_ENTRY_BYTES, CHUNK_TABLE_ENTRY_BYTES_U16, CONTAINER_COMMITMENT_DOMAIN_V1, ChunkId,
    FOOTER_CRC_OFFSET, FOOTER_HASH_OFFSET, FORMAT_VERSION, FormatError, HEADER_BYTES, IndexEntry,
    IntrinsicSummaryAccumulator, MAX_DECODED_RECORD_BYTES, MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES,
    RAW_CODEC, RECORD_ALIGNMENT, RECORD_HEADER_BYTES, RECORD_HEADER_BYTES_U16,
    RECORD_HEADER_BYTES_U32, RECORD_MAGIC, SPARSE_XOR_CODEC, SealedContainerDescriptor, ZSTD_CODEC,
    ZSTD_LEVEL_V1, ZSTD_PREFIX_CODEC, ZSTD_PREFIX_LEVEL_V1, align_up_usize, decode_index, get_u16,
    get_u32, is_dependent_codec, validate_logical_chunk_length, validate_raw_record_constants,
};

/// A Chunk identity and optional Depth-1 dependency authenticated by Container
/// structure. No decoded bytes, verified Location, or publication proof is exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StructuralChunk {
    pub chunk_id: ChunkId,
    pub logical_length: u32,
    pub dependency: Option<(ChunkId, u32)>,
}

/// Complete Container metadata, including its structural BLAKE3 commitment.
/// Payload checksums and decoded identities still require demand reads or scrub.
#[derive(Clone, Debug)]
pub struct ContainerStructure {
    descriptor: SealedContainerDescriptor,
    chunks: Vec<StructuralChunk>,
}

impl ContainerStructure {
    #[must_use]
    pub const fn descriptor(&self) -> SealedContainerDescriptor {
        self.descriptor
    }

    #[must_use]
    pub fn chunks(&self) -> &[StructuralChunk] {
        &self.chunks
    }

    /// Checks the seal, every Record header/Chunk Table, Recovery Index,
    /// structural commitment, and inter-section padding without reading payload.
    ///
    /// # Errors
    /// Returns a bounded-read, structural, checksum, or identity mismatch.
    pub fn read<E: From<FormatError>>(
        header: &[u8],
        footer: &[u8],
        file_length: u64,
        mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, E>,
    ) -> Result<Self, E> {
        let (descriptor, expected_summary) =
            SealedContainerDescriptor::decode_envelope(header, footer, file_length)?;
        let layout = descriptor.layout();
        let mut hash = blake3::Hasher::new();
        hash.update(CONTAINER_COMMITMENT_DOMAIN_V1);
        hash.update(header);
        let mut entries = Vec::new();
        let mut summary =
            IntrinsicSummaryAccumulator::with_record_capacity(layout.record_count as usize)?;
        let mut cursor = HEADER_BYTES as u64;
        for _ in 0..layout.record_count {
            let fixed = read(cursor, RECORD_HEADER_BYTES)?;
            let (record_length, prefix_length) = record_geometry(&fixed)?;
            let end = cursor
                .checked_add(record_length as u64)
                .filter(|end| *end <= layout.index_offset)
                .ok_or(FormatError::InvalidContainerLayout)?;
            let mut prefix = fixed;
            let table = read(
                cursor + RECORD_HEADER_BYTES as u64,
                prefix_length - RECORD_HEADER_BYTES,
            )?;
            if table.len() != prefix_length - RECORD_HEADER_BYTES {
                return Err(FormatError::InvalidRecoveryIndex.into());
            }
            prefix.extend_from_slice(&table);
            validate_prefix(&prefix, record_length)?;
            let codec = get_u16(&prefix, 12);
            let mut dependency_id = [0_u8; 32];
            dependency_id.copy_from_slice(&prefix[64..96]);
            summary.observe(
                codec,
                record_length,
                get_u32(&prefix, 36) as usize,
                get_u32(&prefix, 56) as usize,
                is_dependent_codec(codec).then_some(dependency_id),
            )?;
            IndexEntry::append_from_encoded_record(&prefix, cursor, &mut entries)?;
            hash.update(&prefix);
            cursor = end;
        }
        if cursor != layout.index_offset || summary.finish(layout)? != expected_summary {
            return Err(FormatError::ContainerSummaryMismatch.into());
        }
        let index = read(
            layout.index_offset,
            usize::try_from(layout.index_length).map_err(|_| FormatError::ArithmeticOverflow)?,
        )?;
        entries.sort_unstable();
        if decode_index(&index, layout.chunk_entry_count)? != entries {
            return Err(FormatError::IndexRecordMismatch.into());
        }
        hash.update(&index);
        hash.update(&footer[..FOOTER_HASH_OFFSET]);
        hash.update(&[0_u8; 36]);
        hash.update(&footer[FOOTER_CRC_OFFSET + 4..]);
        if *hash.finalize().as_bytes() != descriptor.container_hash() {
            return Err(FormatError::ContainerHashMismatch.into());
        }
        let index_end = layout.index_offset + layout.index_length;
        let padding_length = usize::try_from(layout.footer_offset - index_end)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let padding = read(index_end, padding_length)?;
        if padding.len() != padding_length || padding.iter().any(|byte| *byte != 0) {
            return Err(FormatError::NonZeroContainerPadding.into());
        }
        let chunks = entries
            .into_iter()
            .map(|entry| StructuralChunk {
                chunk_id: entry.chunk_id,
                logical_length: entry.logical_length,
                dependency: is_dependent_codec(entry.codec_id)
                    .then_some((ChunkId(entry.dependency_id), entry.logical_length)),
            })
            .collect();
        Ok(Self { descriptor, chunks })
    }
}

fn record_geometry(bytes: &[u8]) -> Result<(usize, usize), FormatError> {
    if bytes.len() != RECORD_HEADER_BYTES || &bytes[..8] != RECORD_MAGIC {
        return Err(FormatError::InvalidRecordMagic);
    }
    let length = get_u32(bytes, 32) as usize;
    let count = get_u32(bytes, 56) as usize;
    let prefix = count
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .and_then(|table| table.checked_add(RECORD_HEADER_BYTES))
        .ok_or(FormatError::ArithmeticOverflow)?;
    if !(MIN_RAW_RECORD_BYTES..=MAX_RECORD_BYTES).contains(&length)
        || !length.is_multiple_of(RECORD_ALIGNMENT as usize)
        || count == 0
        || prefix > length
        || prefix != get_u32(bytes, 40) as usize
    {
        return Err(FormatError::InvalidRecordLength(length));
    }
    Ok((length, prefix))
}

fn validate_prefix(bytes: &[u8], record_length: usize) -> Result<(), FormatError> {
    let codec = get_u16(bytes, 12);
    let decoded = get_u32(bytes, 36) as usize;
    let payload = get_u32(bytes, 44) as usize;
    if get_u16(bytes, 8) != FORMAT_VERSION
        || get_u16(bytes, 10) != RECORD_HEADER_BYTES_U16
        || get_u16(bytes, 14) != 0
        || bytes[16..32].iter().any(|b| *b != 0)
        || get_u32(bytes, 48) != RECORD_HEADER_BYTES_U32
        || get_u16(bytes, 52) != CHUNK_TABLE_ENTRY_BYTES_U16
        || get_u16(bytes, 54) != 0
        || decoded == 0
        || decoded > MAX_DECODED_RECORD_BYTES
        || payload == 0
        || align_up_usize(
            bytes
                .len()
                .checked_add(payload)
                .ok_or(FormatError::ArithmeticOverflow)?,
            RECORD_ALIGNMENT as usize,
        )? != record_length
    {
        return Err(FormatError::InvalidRawRecord);
    }
    match codec {
        RAW_CODEC => {
            validate_raw_record_constants(bytes)?;
            if decoded != payload {
                return Err(FormatError::InvalidRawRecord);
            }
        }
        ZSTD_CODEC
            if get_u32(bytes, 96) == ZSTD_LEVEL_V1 as u32
                && bytes[64..96]
                    .iter()
                    .chain(&bytes[100..128])
                    .all(|b| *b == 0) => {}
        ZSTD_PREFIX_CODEC | SPARSE_XOR_CODEC => {
            if get_u32(bytes, 56) != 1
                || get_u32(bytes, 100) != get_u32(bytes, 36)
                || bytes[64..96].iter().all(|b| *b == 0)
                || get_u32(bytes, 96)
                    != if codec == ZSTD_PREFIX_CODEC {
                        ZSTD_PREFIX_LEVEL_V1 as u32
                    } else {
                        1
                    }
                || (codec == ZSTD_PREFIX_CODEC && bytes[104..128].iter().any(|b| *b != 0))
                || (codec == SPARSE_XOR_CODEC && bytes[112..128].iter().any(|b| *b != 0))
            {
                return Err(FormatError::InvalidDependentRecord);
            }
            if codec == SPARSE_XOR_CODEC {
                let runs = get_u32(bytes, 104) as usize;
                let xor = get_u32(bytes, 108) as usize;
                if runs == 0
                    || xor == 0
                    || runs > decoded
                    || xor > decoded
                    || runs.checked_mul(8).and_then(|v| v.checked_add(xor)) != Some(payload)
                {
                    return Err(FormatError::InvalidDependentRecord);
                }
            }
        }
        _ => return Err(FormatError::InvalidZstdRecord),
    }
    let mut cursor = 0_usize;
    for chunk in bytes[RECORD_HEADER_BYTES..].chunks_exact(CHUNK_TABLE_ENTRY_BYTES) {
        let length = get_u32(chunk, 36) as usize;
        validate_logical_chunk_length(length)?;
        if get_u32(chunk, 32) as usize != cursor || chunk[40..].iter().any(|b| *b != 0) {
            return Err(FormatError::InvalidRecoveryIndex);
        }
        cursor = cursor
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    if cursor != decoded {
        return Err(FormatError::InvalidRecoveryIndex);
    }
    Ok(())
}
