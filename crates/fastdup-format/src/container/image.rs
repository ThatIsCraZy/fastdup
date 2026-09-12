//! Complete container-image decoding and publication/content evidence. All payloads are verified.
use super::adaptive::PreparedEncodedRecord;
use super::dependent::{DependentRecord, ZstdPrefixDependency, is_dependent_codec};
use super::envelope::{
    ContainerHeader, IndexEntry, calculate_container_commitment, decode_footer, decode_index,
    validate_container_file_length,
};
use super::payload::VerifiedChunkPayload;
use super::records::{
    DecodedEncodingRecord, EncodingCodec, RawRecord, decode_encoding_record, verify_encoding_record,
};
use super::summary::{ContainerIntrinsicSummary, IntrinsicSummaryAccumulator};
use super::{
    ChunkId, FOOTER_BYTES_USIZE, FormatError, HEADER_BYTES, RAW_CODEC, RECORD_HEADER_BYTES,
    VerifiedChunkLocation, VerifiedRawLocation, ZSTD_CODEC, ZSTD_PREFIX_CODEC, get_u16, get_u32,
};
use std::num::NonZeroUsize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedContainer {
    header: ContainerHeader,
    records: Vec<RawRecord>,
    locations: Vec<VerifiedChunkLocation>,
    raw_locations: Vec<VerifiedRawLocation>,
    raw_record_count: usize,
    zstd_record_count: usize,
    zstd_prefix_record_count: usize,
    sparse_xor_record_count: usize,
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
    pub(super) header: ContainerHeader,
    pub(super) locations: Vec<VerifiedChunkLocation>,
    pub(super) raw_locations: Vec<VerifiedRawLocation>,
    pub(super) logical_bytes: u64,
    pub(super) raw_record_count: usize,
    pub(super) zstd_record_count: usize,
    pub(super) zstd_prefix_record_count: usize,
    pub(super) sparse_xor_record_count: usize,
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

    #[must_use]
    pub const fn sparse_xor_record_count(&self) -> usize {
        self.sparse_xor_record_count
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
                is_dependent_codec(first.codec_id).then_some(first.dependency_id),
            )?;
            cursor = end;
        }
        summary.finish(self.header.layout)
    }
}

impl SealedContainer {
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

    /// Fully validates a sealed Container and resolves dependent-codec Bases through
    /// one caller-supplied adapter.
    ///
    /// The format module validates each dependent record CRC and dependency shape
    /// before invoking `resolve`. The returned bytes must match the requested
    /// Base identity and length; the codec verifies both again before target
    /// reconstruction.
    ///
    /// # Errors
    ///
    /// Returns the first Container, resolver, Base, or target-integrity error.
    pub fn decode_with_dependent_resolver(
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
        let mut sparse_xor_record_count = 0_usize;
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
            let decoded = if encoded.len() >= 14 && is_dependent_codec(get_u16(encoded, 12)) {
                let codec = get_u16(encoded, 12);
                let dependency = DependentRecord::dependency(encoded)?;
                let resolver = resolve
                    .as_deref_mut()
                    .ok_or(FormatError::DependentBaseRequired)?;
                let base = resolver(dependency)?;
                let record = DependentRecord::decode(encoded, &base)?;
                DecodedEncodingRecord {
                    codec: if codec == ZSTD_PREFIX_CODEC {
                        EncodingCodec::ZstdPrefix
                    } else {
                        EncodingCodec::SparseXor
                    },
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
                EncodingCodec::SparseXor => {
                    sparse_xor_record_count = sparse_xor_record_count
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
            sparse_xor_record_count,
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
    /// resolving dependent-codec Bases through one caller-owned adapter.
    ///
    /// # Errors
    ///
    /// Returns the first Container, resolver, Base, or target-integrity error.
    pub fn verify_publication_with_dependent_resolver(
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
        let mut sparse_xor_record_count = 0_usize;
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
            let decoded = if encoded.len() >= 14 && is_dependent_codec(get_u16(encoded, 12)) {
                let codec = get_u16(encoded, 12);
                let dependency = DependentRecord::dependency(encoded)?;
                let resolver = resolve
                    .as_deref_mut()
                    .ok_or(FormatError::DependentBaseRequired)?;
                let base = resolver(dependency)?;
                let record = DependentRecord::decode(encoded, &base)?;
                DecodedEncodingRecord {
                    codec: if codec == ZSTD_PREFIX_CODEC {
                        EncodingCodec::ZstdPrefix
                    } else {
                        EncodingCodec::SparseXor
                    },
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
                EncodingCodec::SparseXor => {
                    sparse_xor_record_count = sparse_xor_record_count
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
            sparse_xor_record_count,
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

    #[must_use]
    pub const fn sparse_xor_record_count(&self) -> usize {
        self.sparse_xor_record_count
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
    /// complete Header, Record, Recovery-Index, CRC, decoded partition,
    /// and per-Chunk hash checks.
    ///
    /// The opaque evidence covers both RAW and dependency-free Zstd records and
    /// is suitable as Exact-Index rebuild or level-zero publication input.
    #[must_use]
    pub fn locations(&self) -> &[VerifiedChunkLocation] {
        &self.locations
    }

    /// Returns physical RAW Locations proven by this Container's complete
    /// Header, Record, Recovery-Index, CRC, hash, and Chunk-ID checks.
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

    /// Owns an image after complete verification with Depth-1 dependent Base
    /// resolution.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`SealedContainer::decode_with_dependent_resolver`].
    pub fn decode_with_dependent_resolver(
        bytes: Vec<u8>,
        resolve: &mut dyn FnMut(ZstdPrefixDependency) -> Result<Vec<u8>, FormatError>,
    ) -> Result<Self, FormatError> {
        let container = SealedContainer::decode_with_dependent_resolver(&bytes, resolve)?;
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
