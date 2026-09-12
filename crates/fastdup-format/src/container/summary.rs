//! Immutable container geometry accounting; never liveness or deletion authority.
use super::dependent::is_dependent_codec;
use super::{
    CONTAINER_SUMMARY_BYTES, ContainerLayout, FormatError, HEADER_BYTES, RAW_CODEC,
    RECORD_ALIGNMENT, RECORD_HEADER_BYTES, SPARSE_XOR_CODEC, ZSTD_CODEC, ZSTD_PREFIX_CODEC,
    get_u16, get_u32, get_u64, put_u16, put_u32, put_u64,
};

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
    sparse_xor_record_count: u32,
    independent_chunk_count: u32,
    dependent_chunk_count: u32,
    raw_encoded_bytes: u64,
    zstd_encoded_bytes: u64,
    zstd_prefix_encoded_bytes: u64,
    sparse_xor_encoded_bytes: u64,
    raw_decoded_bytes: u64,
    zstd_decoded_bytes: u64,
    zstd_prefix_decoded_bytes: u64,
    sparse_xor_decoded_bytes: u64,
    single_chunk_record_count: u32,
    multi_chunk_record_count: u32,
    outgoing_dependency_edges: u32,
    unique_outgoing_base_ids: u32,
}

pub(super) struct IntrinsicSummaryAccumulator {
    summary: ContainerIntrinsicSummary,
    outgoing_base_ids: Vec<[u8; 32]>,
}

impl IntrinsicSummaryAccumulator {
    pub(super) fn with_record_capacity(record_capacity: usize) -> Result<Self, FormatError> {
        let mut outgoing_base_ids = Vec::new();
        outgoing_base_ids
            .try_reserve_exact(record_capacity)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        Ok(Self {
            summary: ContainerIntrinsicSummary::default(),
            outgoing_base_ids,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn observe(
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
            (SPARSE_XOR_CODEC, Some(base_id)) if base_id != [0; 32] && chunk_count == 1 => {
                self.summary.sparse_xor_record_count = self
                    .summary
                    .sparse_xor_record_count
                    .checked_add(1)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.dependent_chunk_count = self
                    .summary
                    .dependent_chunk_count
                    .checked_add(chunk_count)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.sparse_xor_encoded_bytes = self
                    .summary
                    .sparse_xor_encoded_bytes
                    .checked_add(encoded_bytes)
                    .ok_or(FormatError::ArithmeticOverflow)?;
                self.summary.sparse_xor_decoded_bytes = self
                    .summary
                    .sparse_xor_decoded_bytes
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

    pub(super) fn observe_encoded_record(&mut self, record: &[u8]) -> Result<(), FormatError> {
        if record.len() < RECORD_HEADER_BYTES
            || usize::try_from(get_u32(record, 32)) != Ok(record.len())
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        let codec_id = get_u16(record, 12);
        let dependency_id = if is_dependent_codec(codec_id) {
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

    pub(super) fn finish(
        mut self,
        layout: ContainerLayout,
    ) -> Result<ContainerIntrinsicSummary, FormatError> {
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
            .and_then(|bytes| bytes.checked_add(self.sparse_xor_decoded_bytes))
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
    pub const fn sparse_xor_record_count(self) -> u32 {
        self.sparse_xor_record_count
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
    pub const fn sparse_xor_encoded_bytes(self) -> u64 {
        self.sparse_xor_encoded_bytes
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
    pub const fn sparse_xor_decoded_bytes(self) -> u64 {
        self.sparse_xor_decoded_bytes
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

    pub(super) fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            CONTAINER_SUMMARY_BYTES,
            "ASSERT: intrinsic summary always occupies its fixed durable extent"
        );
        output.fill(0);
        put_u16(output, 0, 2);
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
        put_u32(output, 88, self.sparse_xor_record_count);
        put_u64(output, 96, self.sparse_xor_encoded_bytes);
        put_u64(output, 104, self.sparse_xor_decoded_bytes);
    }

    pub(super) fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() != CONTAINER_SUMMARY_BYTES
            || get_u16(input, 0) != 2
            || get_u16(input, 2) != 0
            || get_u32(input, 92) != 0
            || input[112..].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        Ok(Self {
            raw_record_count: get_u32(input, 4),
            zstd_record_count: get_u32(input, 8),
            zstd_prefix_record_count: get_u32(input, 12),
            sparse_xor_record_count: get_u32(input, 88),
            independent_chunk_count: get_u32(input, 16),
            dependent_chunk_count: get_u32(input, 20),
            raw_encoded_bytes: get_u64(input, 24),
            zstd_encoded_bytes: get_u64(input, 32),
            zstd_prefix_encoded_bytes: get_u64(input, 40),
            sparse_xor_encoded_bytes: get_u64(input, 96),
            raw_decoded_bytes: get_u64(input, 48),
            zstd_decoded_bytes: get_u64(input, 56),
            zstd_prefix_decoded_bytes: get_u64(input, 64),
            sparse_xor_decoded_bytes: get_u64(input, 104),
            single_chunk_record_count: get_u32(input, 72),
            multi_chunk_record_count: get_u32(input, 76),
            outgoing_dependency_edges: get_u32(input, 80),
            unique_outgoing_base_ids: get_u32(input, 84),
        })
    }

    pub(super) fn validate(self, layout: ContainerLayout) -> Result<(), FormatError> {
        let record_count = self
            .raw_record_count
            .checked_add(self.zstd_record_count)
            .and_then(|count| count.checked_add(self.zstd_prefix_record_count))
            .and_then(|count| count.checked_add(self.sparse_xor_record_count))
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
            .and_then(|bytes| bytes.checked_add(self.sparse_xor_encoded_bytes))
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
            || self
                .zstd_prefix_record_count
                .checked_add(self.sparse_xor_record_count)
                != Some(self.outgoing_dependency_edges)
            || self.unique_outgoing_base_ids > self.outgoing_dependency_edges
            || (self.unique_outgoing_base_ids == 0) != (self.outgoing_dependency_edges == 0)
            || self.raw_record_count > self.independent_chunk_count
            || (self.raw_encoded_bytes == 0) != (self.raw_record_count == 0)
            || (self.zstd_encoded_bytes == 0) != (self.zstd_record_count == 0)
            || (self.zstd_prefix_encoded_bytes == 0) != (self.zstd_prefix_record_count == 0)
            || (self.sparse_xor_encoded_bytes == 0) != (self.sparse_xor_record_count == 0)
            || (self.raw_decoded_bytes == 0) != (self.raw_record_count == 0)
            || (self.zstd_decoded_bytes == 0) != (self.zstd_record_count == 0)
            || (self.zstd_prefix_decoded_bytes == 0) != (self.zstd_prefix_record_count == 0)
            || (self.sparse_xor_decoded_bytes == 0) != (self.sparse_xor_record_count == 0)
            || !self
                .raw_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !self
                .zstd_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !self
                .zstd_prefix_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || !self
                .sparse_xor_encoded_bytes
                .is_multiple_of(u64::from(RECORD_ALIGNMENT))
        {
            return Err(FormatError::InvalidContainerSummary);
        }
        Ok(())
    }
}
