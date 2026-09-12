//! Container layout, record assembly and sealing with writer-carried evidence.
use super::adaptive::{AdaptiveContainerEncoding, AdaptiveRecordPlan};
use super::aligned::AlignedContainerBuilder;
#[cfg(test)]
use super::aligned::AlignedContainerBytes;
use super::compression::IncompressibilityGateMetrics;
use super::envelope::{
    ContainerHeader, IndexEntry, calculate_container_commitment, encode_footer, encode_index,
    validate_layout,
};
use super::image::VerifiedContainerPublication;
use super::summary::{ContainerIntrinsicSummary, IntrinsicSummaryAccumulator};
use super::{
    ContainerId, ContainerLayout, FOOTER_BYTES, FOOTER_BYTES_USIZE, FOOTER_CRC_OFFSET,
    FOOTER_HASH_OFFSET, FormatError, HEADER_BYTES, INDEX_ENTRY_BYTES, INDEX_HEADER_BYTES,
    MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES, RAW_CODEC, RECORD_ALIGNMENT, SPARSE_XOR_CODEC,
    VerifiedChunkLocation, VerifiedRawLocation, ZSTD_CODEC, ZSTD_PREFIX_CODEC, align_up,
    crc32c_with_zeroed_field, get_u16, get_u32, put_u32,
};
use fastdup_copy_metrics::CopyClass;
use fastdup_copy_metrics::record_copy;
use std::num::NonZeroUsize;

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

pub(super) fn encode_container_from_adaptive_plans(
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
    let mut sparse_xor_record_count = 0_usize;
    let mut builder = AlignedContainerBuilder::new(file_length);
    builder.append_zeroed(HEADER_BYTES);
    let mut cursor = HEADER_BYTES;
    for record in records {
        let record_length = record.record_length()?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        record.append_to(&mut builder)?;
        assert_eq!(builder.image_length(), end);
        let container = builder.image();
        writer_record_evidence(
            &header,
            &container[cursor..end],
            u64::try_from(cursor).map_err(|_| FormatError::ArithmeticOverflow)?,
            &mut locations,
            &mut raw_locations,
            &mut index_entries,
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
            SPARSE_XOR_CODEC => {
                sparse_xor_record_count = sparse_xor_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            _ => return Err(FormatError::UnsupportedHeaderField),
        }
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
    builder.append_slice(&index);
    builder.append_zeroed(file_length - index_end);
    let mut container = builder.finish();
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
            sparse_xor_record_count,
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
pub(super) fn encode_container_from_records(
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
    let mut sparse_xor_record_count = 0_usize;
    for encoded in &encoded_records {
        writer_record_evidence(
            &header,
            encoded,
            record_offset,
            &mut locations,
            &mut raw_locations,
            &mut index_entries,
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
            SPARSE_XOR_CODEC => {
                sparse_xor_record_count = sparse_xor_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            _ => return Err(FormatError::UnsupportedHeaderField),
        }
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
            sparse_xor_record_count,
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
    index_entries: &mut Vec<IndexEntry>,
) -> Result<(), FormatError> {
    // The Container already reserved its complete Recovery Index. Append here
    // instead of allocating and copying a temporary vector for every Record.
    let start = index_entries.len();
    IndexEntry::append_from_encoded_record(encoded, record_offset, index_entries)?;
    let entries = &index_entries[start..];
    for entry in entries {
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
    Ok(())
}

#[cfg(test)]
pub(super) fn encode_container_from_adaptive_plans_zeroed(
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
    let mut sparse_xor_record_count = 0_usize;
    let mut container = AlignedContainerBytes::zeroed(file_length);
    let mut cursor = HEADER_BYTES;
    for record in records {
        let record_length = record.record_length()?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        record.encode_into(&mut container[cursor..end])?;
        writer_record_evidence(
            &header,
            &container[cursor..end],
            u64::try_from(cursor).map_err(|_| FormatError::ArithmeticOverflow)?,
            &mut locations,
            &mut raw_locations,
            &mut index_entries,
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
            SPARSE_XOR_CODEC => {
                sparse_xor_record_count = sparse_xor_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            }
            _ => return Err(FormatError::UnsupportedHeaderField),
        }
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
            sparse_xor_record_count,
        },
        metrics: IncompressibilityGateMetrics::default(),
    })
}
