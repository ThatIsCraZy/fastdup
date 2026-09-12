//! Bounded envelope/index discovery and selected-record verification. Candidates are not content proofs.
use super::dependent::{DependentDependency, ValidatedDependentRecord, is_dependent_codec};
use super::envelope::{
    ContainerHeader, IndexEntry, decode_footer, decode_index, valid_recovery_index_entry_geometry,
    validate_container_file_length,
};
use super::payload::{VerifiedChunkPayload, VerifiedIndependentRecord};
use super::records::{
    RawRecord, decode_encoding_record, decode_encoding_record_mode, raw_record_length,
};
use super::summary::ContainerIntrinsicSummary;
use super::{
    CHUNK_TABLE_ENTRY_BYTES, ChunkId, ContainerId, ContainerLayout, FOOTER_BYTES, FormatError,
    HEADER_BYTES_U16, MAX_DECODED_RECORD_BYTES, MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES, RAW_CODEC,
    RECORD_ALIGNMENT, RECORD_CRC_OFFSET, RECORD_HEADER_BYTES, SPARSE_XOR_CODEC, ZSTD_CODEC,
    ZSTD_PREFIX_CODEC, get_u16, get_u32,
};
use crate::exact_index::ExactIndexEntry;
use crate::exact_index::ExactLocationTransition;
use std::sync::Arc;

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

/// One unverified Record hint obtained from a checksum-checked Container
/// Recovery Index. It never establishes content identity by itself.
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

    pub(super) fn decode_envelope(
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
                RAW_CODEC | ZSTD_CODEC | ZSTD_PREFIX_CODEC | SPARSE_XOR_CODEC
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
            || (is_dependent_codec(location.codec_id()) && location.dependency_id() == [0; 32])
            || (!is_dependent_codec(location.codec_id()) && location.dependency_id() != [0; 32])
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
        self.decode_candidate_payloads_using(candidates, record_bytes, None)
    }

    /// Verifies an owned Record range, retaining RAW bytes without copying.
    /// Shared batches remain bounded by the storage range limit; callers charge
    /// the complete backing capacity when admitting any views to a cache.
    ///
    /// # Errors
    /// Returns the same verification errors as `decode_candidate_payloads`, or
    /// an invalid range. No bytes are exposed before CRC and Chunk verification.
    pub fn decode_owned_candidate_payloads(
        self,
        candidates: &[ExactIndexEntry],
        backing: &Arc<Vec<u8>>,
        range: std::ops::Range<usize>,
    ) -> Result<VerifiedRecordPayloads, FormatError> {
        let record = backing
            .get(range.clone())
            .ok_or(FormatError::ExactLocationMismatch)?;
        self.decode_candidate_payloads_using(candidates, record, Some((backing, range.start)))
    }

    #[allow(clippy::too_many_lines)]
    fn decode_candidate_payloads_using(
        self,
        candidates: &[ExactIndexEntry],
        record_bytes: &[u8],
        backing: Option<(&Arc<Vec<u8>>, usize)>,
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
        if is_dependent_codec(first_location.codec_id()) {
            return Err(FormatError::DependentBaseRequired);
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

        let decoded = decode_encoding_record_mode(record_bytes, true, backing)?;
        let source = VerifiedIndependentRecord::new(first_location)?;
        let all = decoded
            .chunks
            .into_iter()
            .enumerate()
            .map(|(ordinal, record)| {
                let mut payload = record.into_verified_payload();
                payload.source = Some(source);
                payload.chunk_ordinal = u32::try_from(ordinal).expect("bounded Chunk ordinal");
                payload
            })
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
        if candidate.location().codec_id() != ZSTD_PREFIX_CODEC {
            return Err(FormatError::ExactLocationMismatch);
        }
        self.decode_dependent_candidate_using(candidate, record_bytes, |record| record.decode(base))
    }

    /// Fully validates one codec-3 candidate while reusing the identity already
    /// proven by an independent Base decode.
    ///
    /// The target is still decompressed and rehashed. Only the redundant second
    /// full Base hash is replaced by an O(1) capability comparison.
    ///
    /// # Errors
    ///
    /// Returns an Exact pairing, record, Base, codec, or integrity error.
    pub fn decode_zstd_prefix_candidate_with_verified_base(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        if candidate.location().codec_id() != ZSTD_PREFIX_CODEC {
            return Err(FormatError::ExactLocationMismatch);
        }
        self.decode_dependent_candidate_using(candidate, record_bytes, |record| {
            record.decode_with_verified_base(base)
        })
    }

    /// Fully validates any durable dependent Exact candidate with Base bytes.
    ///
    /// # Errors
    ///
    /// Returns an Exact pairing, record, Base, codec, or integrity error.
    pub fn decode_dependent_candidate(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        base: &[u8],
    ) -> Result<RawRecord, FormatError> {
        self.decode_dependent_candidate_using(candidate, record_bytes, |record| record.decode(base))
    }

    /// Fully validates any durable dependent candidate using an already
    /// verified independent Base payload.
    ///
    /// # Errors
    ///
    /// Returns an Exact pairing, record, Base, codec, or integrity error.
    pub fn decode_dependent_candidate_with_verified_base(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        self.decode_dependent_candidate_using(candidate, record_bytes, |record| {
            record.decode_with_verified_base(base)
        })
    }

    fn decode_dependent_candidate_using(
        self,
        candidate: ExactIndexEntry,
        record_bytes: &[u8],
        decode: impl FnOnce(ValidatedDependentRecord<'_>) -> Result<RawRecord, FormatError>,
    ) -> Result<RawRecord, FormatError> {
        let location = candidate.location();
        if !is_dependent_codec(location.codec_id()) {
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
        let validated = ValidatedDependentRecord::new(record_bytes)?;
        let dependency = validated.dependency;
        if dependency.chunk_id().bytes() != location.dependency_id()
            || dependency.logical_length() != candidate.logical_length()
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        let record = decode(validated)?;
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
    /// Enumerates bounded Record hints. These are not content proofs: the selected
    /// Record still requires complete checksum, codec and Chunk-ID verification.
    pub fn candidates(&self) -> impl Iterator<Item = RecoveryIndexCandidate> + '_ {
        self.entries
            .iter()
            .copied()
            .map(|entry| RecoveryIndexCandidate {
                container_id: self.descriptor.container_id(),
                container_generation: self.descriptor.container_generation(),
                entry,
            })
    }

    /// Verifies one selected Record and returns all independently checked siblings.
    /// Unrelated Container payloads are not needed to prove these Chunk identities.
    /// The resolver must select an independent Base; its bytes are rehashed here.
    ///
    /// # Errors
    /// Rejects foreign hints, mismatched coordinates, CRCs, dependencies, lengths
    /// or decoded Chunk identities. No payload escapes before all checks succeed.
    pub fn decode_candidate_with_resolver(
        &self,
        candidate: RecoveryIndexCandidate,
        record_bytes: &[u8],
        resolve: &mut impl FnMut(DependentDependency) -> Result<Vec<u8>, FormatError>,
    ) -> Result<Vec<VerifiedChunkPayload>, FormatError> {
        if candidate.container_id != self.descriptor.container_id()
            || candidate.container_generation != self.descriptor.container_generation()
            || self.entries.binary_search(&candidate.entry).is_err()
            || record_bytes.len() != candidate.record_range()?.length()
        {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        let chunks = if is_dependent_codec(candidate.entry.codec_id) {
            let record = ValidatedDependentRecord::new(record_bytes)?;
            let base = resolve(record.dependency)?;
            vec![record.decode(&base)?]
        } else {
            decode_encoding_record(record_bytes)?.chunks
        };
        // Derive coordinates from the validated stored Record, never from the hint.
        let mut observed = Vec::new();
        IndexEntry::append_from_encoded_record(
            record_bytes,
            candidate.entry.record_offset,
            &mut observed,
        )?;
        if !observed.contains(&candidate.entry) {
            return Err(FormatError::RecoveryIndexCandidateMismatch);
        }
        Ok(chunks
            .into_iter()
            .map(RawRecord::into_verified_payload)
            .collect())
    }

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
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.entry.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.entry.logical_length
    }

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
