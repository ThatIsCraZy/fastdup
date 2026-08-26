use core::fmt;

use crate::{ContainerId, ContainerIntrinsicSummary, crc32c_with_zeroed_u32};

pub const GC_CANDIDATE_CATALOG_HEADER_BYTES: usize = 4_096;
pub const GC_CANDIDATE_CATALOG_ROW_BYTES: usize = 96;

const HEADER_MAGIC: [u8; 8] = *b"FDGCC001";
const FOOTER_MAGIC: [u8; 8] = *b"FDGCF001";
const HEADER_BYTES_U16: u16 = 4_096;
const ROW_BYTES_U16: u16 = 96;
const FORMAT_VERSION: u16 = 1;
const HASH_ALGORITHM: u16 = 1;
const CRC_ALGORITHM: u16 = 1;
const ROWS_OFFSET: u64 = 4_096;
const HEADER_CRC_OFFSET: usize = 120;
const CATALOG_HASH_OFFSET: usize = 88;
const CATALOG_HASH_BYTES: usize = 32;
const CATALOG_HASH_DOMAIN: &[u8] = b"fastdup-gc-candidate-catalog-v1\0";
const ROW_FLAG_ESTIMATE_KNOWN: u32 = 1 << 0;
const ROW_FLAG_ACTIVE: u32 = 1 << 1;
const ROW_FLAG_RETIRING: u32 = 1 << 2;
const ROW_FLAG_QUARANTINED: u32 = 1 << 3;
const ROW_FLAG_DEPENDENCY_KNOWN: u32 = 1 << 4;
const ROW_ALLOWED_FLAGS: u32 = ROW_FLAG_ESTIMATE_KNOWN
    | ROW_FLAG_ACTIVE
    | ROW_FLAG_RETIRING
    | ROW_FLAG_QUARANTINED
    | ROW_FLAG_DEPENDENCY_KNOWN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcCandidateLocationState {
    Active,
    Retiring,
    Quarantined,
}

impl GcCandidateLocationState {
    const fn flag(self) -> u32 {
        match self {
            Self::Active => ROW_FLAG_ACTIVE,
            Self::Retiring => ROW_FLAG_RETIRING,
            Self::Quarantined => ROW_FLAG_QUARANTINED,
        }
    }

    fn from_flags(flags: u32) -> Result<Self, GcCandidateCatalogError> {
        match flags & (ROW_FLAG_ACTIVE | ROW_FLAG_RETIRING | ROW_FLAG_QUARANTINED) {
            ROW_FLAG_ACTIVE => Ok(Self::Active),
            ROW_FLAG_RETIRING => Ok(Self::Retiring),
            ROW_FLAG_QUARANTINED => Ok(Self::Quarantined),
            _ => Err(GcCandidateCatalogError::InvalidRow),
        }
    }
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcRecordLivenessEstimate {
    dead_bytes: u32,
    wholly_live_bytes: u32,
    partial_bytes: u32,
}

impl GcRecordLivenessEstimate {
    /// Creates conservative encoded-record byte classes for one Container.
    ///
    /// # Errors
    ///
    /// Returns an error if their sum exceeds the immutable record area.
    pub fn new(
        dead_bytes: u32,
        wholly_live_bytes: u32,
        partial_bytes: u32,
        record_area_bytes: u64,
    ) -> Result<Self, GcCandidateCatalogError> {
        let classified = u64::from(dead_bytes)
            .checked_add(u64::from(wholly_live_bytes))
            .and_then(|bytes| bytes.checked_add(u64::from(partial_bytes)))
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        if classified > record_area_bytes {
            return Err(GcCandidateCatalogError::InvalidEstimate);
        }
        Ok(Self {
            dead_bytes,
            wholly_live_bytes,
            partial_bytes,
        })
    }

    #[must_use]
    pub const fn dead_bytes(self) -> u32 {
        self.dead_bytes
    }

    #[must_use]
    pub const fn wholly_live_bytes(self) -> u32 {
        self.wholly_live_bytes
    }

    #[must_use]
    pub const fn partial_bytes(self) -> u32 {
        self.partial_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcDependencyEstimate {
    live_independent_bases: u32,
    incoming_base_fanout: u32,
}

impl GcDependencyEstimate {
    #[must_use]
    pub const fn new(live_independent_bases: u32, incoming_base_fanout: u32) -> Self {
        Self {
            live_independent_bases,
            incoming_base_fanout,
        }
    }

    #[must_use]
    pub const fn live_independent_bases(self) -> u32 {
        self.live_independent_bases
    }

    #[must_use]
    pub const fn incoming_base_fanout(self) -> u32 {
        self.incoming_base_fanout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcCandidateLivenessEstimate {
    reachable_target_count: u32,
    estimated_encoded_coverage: u64,
    records: GcRecordLivenessEstimate,
    dependencies: Option<GcDependencyEstimate>,
}

impl GcCandidateLivenessEstimate {
    /// Creates one non-authoritative estimate derived from a bound Metadata
    /// and Location generation.
    ///
    /// # Errors
    ///
    /// Returns an error if estimated encoded coverage exceeds the immutable
    /// record area.
    pub fn new(
        reachable_target_count: u32,
        estimated_encoded_coverage: u64,
        records: GcRecordLivenessEstimate,
        dependencies: Option<GcDependencyEstimate>,
        record_area_bytes: u64,
    ) -> Result<Self, GcCandidateCatalogError> {
        if estimated_encoded_coverage > record_area_bytes {
            return Err(GcCandidateCatalogError::InvalidEstimate);
        }
        Ok(Self {
            reachable_target_count,
            estimated_encoded_coverage,
            records,
            dependencies,
        })
    }

    #[must_use]
    pub const fn reachable_target_count(self) -> u32 {
        self.reachable_target_count
    }

    #[must_use]
    pub const fn estimated_encoded_coverage(self) -> u64 {
        self.estimated_encoded_coverage
    }

    #[must_use]
    pub const fn records(self) -> GcRecordLivenessEstimate {
        self.records
    }

    #[must_use]
    pub const fn dependencies(self) -> Option<GcDependencyEstimate> {
        self.dependencies
    }
}

/// One fixed-width, non-authoritative Container candidate row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcCandidateCatalogRow {
    container_id: ContainerId,
    container_generation: u64,
    physical_bytes: u64,
    summary_checksum: u32,
    flags: u32,
    reachable_target_count: u32,
    live_independent_bases: u32,
    incoming_base_fanout: u32,
    outgoing_dependency_count: u32,
    estimated_encoded_coverage: u64,
    raw_replacement_upper_bound: u64,
    dead_record_bytes: u32,
    wholly_live_record_bytes: u32,
    partial_record_bytes: u32,
}

impl GcCandidateCatalogRow {
    /// Seeds one active row from immutable publication facts. Liveness remains
    /// explicitly unknown until a generation-bound delta supplies it.
    ///
    /// # Errors
    ///
    /// Returns invalid physical geometry or checked-arithmetic failures.
    pub fn from_intrinsic_summary(
        container_id: ContainerId,
        container_generation: u64,
        physical_bytes: u64,
        summary: ContainerIntrinsicSummary,
    ) -> Result<Self, GcCandidateCatalogError> {
        if container_generation == 0 || physical_bytes == 0 {
            return Err(GcCandidateCatalogError::InvalidRow);
        }
        Ok(Self {
            container_id,
            container_generation,
            physical_bytes,
            summary_checksum: summary.structural_checksum(),
            flags: GcCandidateLocationState::Active.flag(),
            reachable_target_count: 0,
            live_independent_bases: 0,
            incoming_base_fanout: 0,
            outgoing_dependency_count: summary.outgoing_dependency_edges(),
            estimated_encoded_coverage: 0,
            raw_replacement_upper_bound: summary
                .raw_replacement_upper_bound()
                .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?,
            dead_record_bytes: 0,
            wholly_live_record_bytes: 0,
            partial_record_bytes: 0,
        })
    }

    /// Applies one newer non-authoritative liveness estimate without changing
    /// immutable publication facts.
    ///
    /// # Errors
    ///
    /// Returns an error if counts exceed immutable Container bounds.
    pub fn with_estimate(
        mut self,
        state: GcCandidateLocationState,
        estimate: GcCandidateLivenessEstimate,
    ) -> Result<Self, GcCandidateCatalogError> {
        let record_area_bytes = self.record_area_upper_bound()?;
        if estimate.estimated_encoded_coverage > record_area_bytes {
            return Err(GcCandidateCatalogError::InvalidEstimate);
        }
        self.flags = state.flag() | ROW_FLAG_ESTIMATE_KNOWN;
        self.reachable_target_count = estimate.reachable_target_count;
        self.estimated_encoded_coverage = estimate.estimated_encoded_coverage;
        self.dead_record_bytes = estimate.records.dead_bytes;
        self.wholly_live_record_bytes = estimate.records.wholly_live_bytes;
        self.partial_record_bytes = estimate.records.partial_bytes;
        if let Some(dependencies) = estimate.dependencies {
            self.flags |= ROW_FLAG_DEPENDENCY_KNOWN;
            self.live_independent_bases = dependencies.live_independent_bases;
            self.incoming_base_fanout = dependencies.incoming_base_fanout;
        } else {
            self.live_independent_bases = 0;
            self.incoming_base_fanout = 0;
        }
        self.validate()?;
        Ok(self)
    }

    /// Applies one logical target reachability change to hint state.
    ///
    /// A positive delta can initialize an unknown row. A removal from unknown
    /// state stays unknown, and an underflow clears the estimate instead of
    /// manufacturing a zero-live hint. Immutable publication facts and the
    /// Location state never change.
    ///
    /// # Errors
    ///
    /// Returns a checked count overflow or row-invariant failure.
    pub fn with_reachable_target_delta(
        mut self,
        delta: i64,
    ) -> Result<Self, GcCandidateCatalogError> {
        if delta == 0 {
            return Ok(self);
        }
        let state = self.location_state();
        if delta > 0 {
            let increment =
                u32::try_from(delta).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
            self.reachable_target_count = if self.estimate_known() {
                self.reachable_target_count
                    .checked_add(increment)
                    .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?
            } else {
                increment
            };
            self.flags |= ROW_FLAG_ESTIMATE_KNOWN;
        } else {
            if !self.estimate_known() {
                return Ok(self);
            }
            let decrement = u32::try_from(delta.unsigned_abs())
                .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
            let Some(next) = self.reachable_target_count.checked_sub(decrement) else {
                self.flags = state.flag();
                self.reachable_target_count = 0;
                self.live_independent_bases = 0;
                self.incoming_base_fanout = 0;
                self.estimated_encoded_coverage = 0;
                self.dead_record_bytes = 0;
                self.wholly_live_record_bytes = 0;
                self.partial_record_bytes = 0;
                return Ok(self);
            };
            self.reachable_target_count = next;
        }
        self.validate()?;
        Ok(self)
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
    pub const fn physical_bytes(self) -> u64 {
        self.physical_bytes
    }

    #[must_use]
    pub const fn summary_checksum(self) -> u32 {
        self.summary_checksum
    }

    #[must_use]
    /// # Panics
    ///
    /// Panics only if an internal constructor or decoder exposed a row without
    /// exactly one validated Location state.
    pub fn location_state(self) -> GcCandidateLocationState {
        GcCandidateLocationState::from_flags(self.flags)
            .expect("ASSERT: constructed catalog row has one Location state")
    }

    #[must_use]
    pub const fn estimate_known(self) -> bool {
        self.flags & ROW_FLAG_ESTIMATE_KNOWN != 0
    }

    #[must_use]
    pub const fn dependency_estimate_known(self) -> bool {
        self.flags & ROW_FLAG_DEPENDENCY_KNOWN != 0
    }

    #[must_use]
    pub const fn reachable_target_count(self) -> u32 {
        self.reachable_target_count
    }

    #[must_use]
    pub const fn estimated_encoded_coverage(self) -> u64 {
        self.estimated_encoded_coverage
    }

    #[must_use]
    pub const fn raw_replacement_upper_bound(self) -> u64 {
        self.raw_replacement_upper_bound
    }

    #[must_use]
    pub const fn dead_record_bytes(self) -> u32 {
        self.dead_record_bytes
    }

    #[must_use]
    pub const fn wholly_live_record_bytes(self) -> u32 {
        self.wholly_live_record_bytes
    }

    #[must_use]
    pub const fn partial_record_bytes(self) -> u32 {
        self.partial_record_bytes
    }

    #[must_use]
    pub const fn live_independent_bases(self) -> u32 {
        self.live_independent_bases
    }

    #[must_use]
    pub const fn incoming_base_fanout(self) -> u32 {
        self.incoming_base_fanout
    }

    #[must_use]
    pub const fn outgoing_dependency_count(self) -> u32 {
        self.outgoing_dependency_count
    }

    fn record_area_upper_bound(self) -> Result<u64, GcCandidateCatalogError> {
        self.physical_bytes
            .checked_sub(2 * 4_096)
            .ok_or(GcCandidateCatalogError::InvalidRow)
    }

    fn encode(self) -> Result<[u8; GC_CANDIDATE_CATALOG_ROW_BYTES], GcCandidateCatalogError> {
        self.validate()?;
        let mut bytes = [0_u8; GC_CANDIDATE_CATALOG_ROW_BYTES];
        bytes[0..16].copy_from_slice(&self.container_id.bytes());
        put_u64(&mut bytes, 16, self.container_generation);
        put_u64(&mut bytes, 24, self.physical_bytes);
        put_u32(&mut bytes, 32, self.summary_checksum);
        put_u32(&mut bytes, 36, self.flags);
        put_u32(&mut bytes, 40, self.reachable_target_count);
        put_u32(&mut bytes, 44, self.live_independent_bases);
        put_u32(&mut bytes, 48, self.incoming_base_fanout);
        put_u32(&mut bytes, 52, self.outgoing_dependency_count);
        put_u64(&mut bytes, 56, self.estimated_encoded_coverage);
        put_u64(&mut bytes, 64, self.raw_replacement_upper_bound);
        put_u32(&mut bytes, 72, self.dead_record_bytes);
        put_u32(&mut bytes, 76, self.wholly_live_record_bytes);
        put_u32(&mut bytes, 80, self.partial_record_bytes);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, GcCandidateCatalogError> {
        if bytes.len() != GC_CANDIDATE_CATALOG_ROW_BYTES
            || bytes[84..].iter().any(|byte| *byte != 0)
        {
            return Err(GcCandidateCatalogError::InvalidRow);
        }
        let mut id = [0_u8; 16];
        id.copy_from_slice(&bytes[0..16]);
        let row = Self {
            container_id: ContainerId::new(id).map_err(|_| GcCandidateCatalogError::InvalidRow)?,
            container_generation: get_u64(bytes, 16),
            physical_bytes: get_u64(bytes, 24),
            summary_checksum: get_u32(bytes, 32),
            flags: get_u32(bytes, 36),
            reachable_target_count: get_u32(bytes, 40),
            live_independent_bases: get_u32(bytes, 44),
            incoming_base_fanout: get_u32(bytes, 48),
            outgoing_dependency_count: get_u32(bytes, 52),
            estimated_encoded_coverage: get_u64(bytes, 56),
            raw_replacement_upper_bound: get_u64(bytes, 64),
            dead_record_bytes: get_u32(bytes, 72),
            wholly_live_record_bytes: get_u32(bytes, 76),
            partial_record_bytes: get_u32(bytes, 80),
        };
        row.validate()?;
        Ok(row)
    }

    fn validate(self) -> Result<(), GcCandidateCatalogError> {
        GcCandidateLocationState::from_flags(self.flags)?;
        let record_area = self.record_area_upper_bound()?;
        let classified = u64::from(self.dead_record_bytes)
            .checked_add(u64::from(self.wholly_live_record_bytes))
            .and_then(|bytes| bytes.checked_add(u64::from(self.partial_record_bytes)))
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        if self.container_generation == 0
            || self.physical_bytes == 0
            || self.flags & !ROW_ALLOWED_FLAGS != 0
            || self.raw_replacement_upper_bound == 0
            || self.estimated_encoded_coverage > record_area
            || classified > record_area
            || (!self.estimate_known()
                && (self.reachable_target_count != 0
                    || self.estimated_encoded_coverage != 0
                    || classified != 0))
            || (!self.dependency_estimate_known()
                && (self.live_independent_bases != 0 || self.incoming_base_fanout != 0))
        {
            return Err(GcCandidateCatalogError::InvalidRow);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcCandidateCatalogDescriptor {
    generation: u64,
    incorporated_commit_generation: u64,
    incorporated_location_generation: u64,
    row_count: u64,
    footer_offset: u64,
    file_length: u64,
    catalog_hash: [u8; 32],
}

impl GcCandidateCatalogDescriptor {
    /// Decodes paired immutable catalog envelopes without reading all rows.
    ///
    /// # Errors
    ///
    /// Returns checksum, version, length, layout, or mirror failures.
    pub fn decode(
        header: &[u8],
        footer: &[u8],
        actual_length: u64,
    ) -> Result<Self, GcCandidateCatalogError> {
        let first = decode_envelope(header, HEADER_MAGIC)?;
        let second = decode_envelope(footer, FOOTER_MAGIC)?;
        if first != second || first.file_length != actual_length {
            return Err(GcCandidateCatalogError::EnvelopeMismatch);
        }
        first.validate_layout()?;
        Ok(first)
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn incorporated_commit_generation(self) -> u64 {
        self.incorporated_commit_generation
    }

    #[must_use]
    pub const fn incorporated_location_generation(self) -> u64 {
        self.incorporated_location_generation
    }

    #[must_use]
    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub const fn footer_offset(self) -> u64 {
        self.footer_offset
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn catalog_hash(self) -> [u8; 32] {
        self.catalog_hash
    }

    #[must_use]
    pub fn row_offset(self, ordinal: u64) -> Option<u64> {
        if ordinal >= self.row_count {
            return None;
        }
        ROWS_OFFSET.checked_add(ordinal.checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)?)
    }

    /// Returns the first byte after the fixed-width row area. For an empty
    /// catalog this is the common 4 KiB header boundary.
    #[must_use]
    pub fn rows_end(self) -> Option<u64> {
        ROWS_OFFSET.checked_add(
            self.row_count
                .checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)?,
        )
    }

    /// Decodes one exact fixed-width row selected by ordinal.
    ///
    /// # Errors
    ///
    /// Returns row length, reserved-field, or invariant failures.
    pub fn decode_row(
        self,
        ordinal: u64,
        bytes: &[u8],
    ) -> Result<GcCandidateCatalogRow, GcCandidateCatalogError> {
        if ordinal >= self.row_count {
            return Err(GcCandidateCatalogError::RowCountMismatch);
        }
        GcCandidateCatalogRow::decode(bytes)
    }

    #[must_use]
    pub fn start_audit(self) -> GcCandidateCatalogAudit {
        GcCandidateCatalogAudit {
            descriptor: self,
            ordinal: 0,
            previous_id: None,
            hasher: catalog_hasher(self),
        }
    }

    fn validate_layout(self) -> Result<(), GcCandidateCatalogError> {
        let rows_bytes = self
            .row_count
            .checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        let rows_end = ROWS_OFFSET
            .checked_add(rows_bytes)
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        let expected_footer = align_up(rows_end, GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)?;
        let expected_length = expected_footer
            .checked_add(GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        if self.generation == 0
            || self.footer_offset != expected_footer
            || self.file_length != expected_length
        {
            return Err(GcCandidateCatalogError::InvalidLayout);
        }
        Ok(())
    }

    fn encode_envelope(self, magic: [u8; 8]) -> [u8; GC_CANDIDATE_CATALOG_HEADER_BYTES] {
        let mut bytes = [0_u8; GC_CANDIDATE_CATALOG_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&magic);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, HEADER_BYTES_U16);
        put_u16(&mut bytes, 12, ROW_BYTES_U16);
        put_u16(&mut bytes, 14, HASH_ALGORITHM);
        put_u16(&mut bytes, 16, CRC_ALGORITHM);
        put_u64(&mut bytes, 32, self.generation);
        put_u64(&mut bytes, 40, self.incorporated_commit_generation);
        put_u64(&mut bytes, 48, self.incorporated_location_generation);
        put_u64(&mut bytes, 56, self.row_count);
        put_u64(&mut bytes, 64, ROWS_OFFSET);
        put_u64(&mut bytes, 72, self.footer_offset);
        put_u64(&mut bytes, 80, self.file_length);
        bytes[CATALOG_HASH_OFFSET..CATALOG_HASH_OFFSET + CATALOG_HASH_BYTES]
            .copy_from_slice(&self.catalog_hash);
        let checksum = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, HEADER_CRC_OFFSET, checksum);
        bytes
    }
}

pub struct GcCandidateCatalogStreamEncoder {
    descriptor: GcCandidateCatalogDescriptor,
    ordinal: u64,
    previous_id: Option<ContainerId>,
    hasher: blake3::Hasher,
}

impl GcCandidateCatalogStreamEncoder {
    /// Starts a bounded-memory writer for one immutable catalog generation.
    ///
    /// # Errors
    ///
    /// Returns zero generation or checked layout overflow.
    pub fn new(
        generation: u64,
        incorporated_commit_generation: u64,
        incorporated_location_generation: u64,
        row_count: u64,
    ) -> Result<Self, GcCandidateCatalogError> {
        let rows_bytes = row_count
            .checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        let footer_offset = align_up(
            ROWS_OFFSET
                .checked_add(rows_bytes)
                .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?,
            GC_CANDIDATE_CATALOG_HEADER_BYTES as u64,
        )?;
        let file_length = footer_offset
            .checked_add(GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)
            .ok_or(GcCandidateCatalogError::ArithmeticOverflow)?;
        let descriptor = GcCandidateCatalogDescriptor {
            generation,
            incorporated_commit_generation,
            incorporated_location_generation,
            row_count,
            footer_offset,
            file_length,
            catalog_hash: [0; 32],
        };
        descriptor.validate_layout()?;
        Ok(Self {
            descriptor,
            ordinal: 0,
            previous_id: None,
            hasher: catalog_hasher(descriptor),
        })
    }

    /// Serializes the next strictly Container-ID-ordered row.
    ///
    /// # Errors
    ///
    /// Returns duplicate/order, row-count, or row-invariant failures.
    pub fn push(
        &mut self,
        row: GcCandidateCatalogRow,
    ) -> Result<(u64, [u8; GC_CANDIDATE_CATALOG_ROW_BYTES]), GcCandidateCatalogError> {
        if self.ordinal >= self.descriptor.row_count {
            return Err(GcCandidateCatalogError::RowCountMismatch);
        }
        if self
            .previous_id
            .is_some_and(|previous| previous.bytes() >= row.container_id.bytes())
        {
            return Err(GcCandidateCatalogError::NonCanonicalOrder);
        }
        let offset = self
            .descriptor
            .row_offset(self.ordinal)
            .ok_or(GcCandidateCatalogError::RowCountMismatch)?;
        let bytes = row.encode()?;
        self.hasher.update(&bytes);
        self.ordinal += 1;
        self.previous_id = Some(row.container_id);
        Ok((offset, bytes))
    }

    /// Finishes the complete row stream and emits paired envelopes.
    ///
    /// # Errors
    ///
    /// Returns an error unless exactly the declared row count was written.
    pub fn finish(
        self,
    ) -> Result<
        (
            GcCandidateCatalogDescriptor,
            [u8; GC_CANDIDATE_CATALOG_HEADER_BYTES],
            [u8; GC_CANDIDATE_CATALOG_HEADER_BYTES],
        ),
        GcCandidateCatalogError,
    > {
        if self.ordinal != self.descriptor.row_count {
            return Err(GcCandidateCatalogError::RowCountMismatch);
        }
        let mut descriptor = self.descriptor;
        descriptor.catalog_hash = *self.hasher.finalize().as_bytes();
        Ok((
            descriptor,
            descriptor.encode_envelope(HEADER_MAGIC),
            descriptor.encode_envelope(FOOTER_MAGIC),
        ))
    }
}

pub struct GcCandidateCatalogAudit {
    descriptor: GcCandidateCatalogDescriptor,
    ordinal: u64,
    previous_id: Option<ContainerId>,
    hasher: blake3::Hasher,
}

impl GcCandidateCatalogAudit {
    /// Adds one exact row from an independent positional read or mapping.
    ///
    /// # Errors
    ///
    /// Returns row corruption, duplicate/order, or count failures.
    pub fn push(&mut self, bytes: &[u8]) -> Result<GcCandidateCatalogRow, GcCandidateCatalogError> {
        let row = self.descriptor.decode_row(self.ordinal, bytes)?;
        if self
            .previous_id
            .is_some_and(|previous| previous.bytes() >= row.container_id.bytes())
        {
            return Err(GcCandidateCatalogError::NonCanonicalOrder);
        }
        self.hasher.update(bytes);
        self.ordinal += 1;
        self.previous_id = Some(row.container_id);
        Ok(row)
    }

    /// Completes the audit after checking zero padding separately.
    ///
    /// # Errors
    ///
    /// Returns incomplete row streams or a structural hash mismatch.
    pub fn finish(self) -> Result<(), GcCandidateCatalogError> {
        if self.ordinal != self.descriptor.row_count {
            return Err(GcCandidateCatalogError::RowCountMismatch);
        }
        if self.hasher.finalize().as_bytes() != &self.descriptor.catalog_hash {
            return Err(GcCandidateCatalogError::CatalogHashMismatch);
        }
        Ok(())
    }
}

/// Convenience owned representation for tests and bounded rebuild batches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcCandidateCatalog {
    descriptor: GcCandidateCatalogDescriptor,
    rows: Vec<GcCandidateCatalogRow>,
}

impl GcCandidateCatalog {
    /// Builds one canonical catalog in memory.
    ///
    /// # Errors
    ///
    /// Returns duplicate, unordered, allocation, or layout failures.
    pub fn new(
        generation: u64,
        incorporated_commit_generation: u64,
        incorporated_location_generation: u64,
        rows: Vec<GcCandidateCatalogRow>,
    ) -> Result<Self, GcCandidateCatalogError> {
        let mut encoder = GcCandidateCatalogStreamEncoder::new(
            generation,
            incorporated_commit_generation,
            incorporated_location_generation,
            u64::try_from(rows.len()).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?,
        )?;
        for row in &rows {
            encoder.push(*row)?;
        }
        let (descriptor, _, _) = encoder.finish()?;
        Ok(Self { descriptor, rows })
    }

    #[must_use]
    pub const fn descriptor(&self) -> GcCandidateCatalogDescriptor {
        self.descriptor
    }

    #[must_use]
    pub fn rows(&self) -> &[GcCandidateCatalogRow] {
        &self.rows
    }

    /// Encodes the complete file. Large production catalogs should use the
    /// streaming encoder through the repository instead.
    ///
    /// # Errors
    ///
    /// Returns allocation, layout, or canonical-row failures.
    pub fn encode(&self) -> Result<Vec<u8>, GcCandidateCatalogError> {
        let length = usize::try_from(self.descriptor.file_length)
            .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(length)
            .map_err(|_| GcCandidateCatalogError::OutOfMemory)?;
        output.resize(length, 0);
        output[..GC_CANDIDATE_CATALOG_HEADER_BYTES]
            .copy_from_slice(&self.descriptor.encode_envelope(HEADER_MAGIC));
        for (ordinal, row) in self.rows.iter().enumerate() {
            let offset = self
                .descriptor
                .row_offset(
                    u64::try_from(ordinal)
                        .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?,
                )
                .ok_or(GcCandidateCatalogError::RowCountMismatch)?;
            let offset =
                usize::try_from(offset).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
            output[offset..offset + GC_CANDIDATE_CATALOG_ROW_BYTES].copy_from_slice(&row.encode()?);
        }
        let footer_offset = usize::try_from(self.descriptor.footer_offset)
            .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
        output[footer_offset..].copy_from_slice(&self.descriptor.encode_envelope(FOOTER_MAGIC));
        Ok(output)
    }

    /// Independently decodes and audits a complete catalog image.
    ///
    /// # Errors
    ///
    /// Returns envelope, row, ordering, padding, or hash failures.
    pub fn decode(bytes: &[u8]) -> Result<Self, GcCandidateCatalogError> {
        if bytes.len() < 2 * GC_CANDIDATE_CATALOG_HEADER_BYTES {
            return Err(GcCandidateCatalogError::InvalidLength);
        }
        let footer_offset = bytes.len() - GC_CANDIDATE_CATALOG_HEADER_BYTES;
        let descriptor = GcCandidateCatalogDescriptor::decode(
            &bytes[..GC_CANDIDATE_CATALOG_HEADER_BYTES],
            &bytes[footer_offset..],
            u64::try_from(bytes.len()).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?,
        )?;
        if usize::try_from(descriptor.footer_offset) != Ok(footer_offset) {
            return Err(GcCandidateCatalogError::InvalidLayout);
        }
        let rows_end = descriptor
            .rows_end()
            .ok_or(GcCandidateCatalogError::InvalidLayout)?;
        let rows_end =
            usize::try_from(rows_end).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
        if bytes[rows_end..footer_offset].iter().any(|byte| *byte != 0) {
            return Err(GcCandidateCatalogError::NonZeroPadding);
        }
        let capacity = usize::try_from(descriptor.row_count)
            .map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(capacity)
            .map_err(|_| GcCandidateCatalogError::OutOfMemory)?;
        let mut audit = descriptor.start_audit();
        for ordinal in 0..descriptor.row_count {
            let offset = descriptor
                .row_offset(ordinal)
                .ok_or(GcCandidateCatalogError::InvalidLayout)?;
            let offset =
                usize::try_from(offset).map_err(|_| GcCandidateCatalogError::ArithmeticOverflow)?;
            rows.push(audit.push(&bytes[offset..offset + GC_CANDIDATE_CATALOG_ROW_BYTES])?);
        }
        audit.finish()?;
        Ok(Self { descriptor, rows })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GcCandidateCatalogError {
    InvalidLength,
    InvalidEnvelope,
    EnvelopeChecksumMismatch,
    EnvelopeMismatch,
    InvalidLayout,
    InvalidRow,
    InvalidEstimate,
    NonCanonicalOrder,
    RowCountMismatch,
    NonZeroPadding,
    CatalogHashMismatch,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for GcCandidateCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for GcCandidateCatalogError {}

fn catalog_hasher(descriptor: GcCandidateCatalogDescriptor) -> blake3::Hasher {
    let mut metadata = [0_u8; 48];
    put_u64(&mut metadata, 0, descriptor.generation);
    put_u64(&mut metadata, 8, descriptor.incorporated_commit_generation);
    put_u64(
        &mut metadata,
        16,
        descriptor.incorporated_location_generation,
    );
    put_u64(&mut metadata, 24, descriptor.row_count);
    put_u64(&mut metadata, 32, descriptor.footer_offset);
    put_u64(&mut metadata, 40, descriptor.file_length);
    let mut hasher = blake3::Hasher::new();
    hasher.update(CATALOG_HASH_DOMAIN);
    hasher.update(&metadata);
    hasher
}

fn decode_envelope(
    bytes: &[u8],
    magic: [u8; 8],
) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogError> {
    if bytes.len() != GC_CANDIDATE_CATALOG_HEADER_BYTES || bytes[0..8] != magic {
        return Err(GcCandidateCatalogError::InvalidEnvelope);
    }
    if get_u32(bytes, HEADER_CRC_OFFSET) != crc32c_with_zeroed_u32(bytes, HEADER_CRC_OFFSET) {
        return Err(GcCandidateCatalogError::EnvelopeChecksumMismatch);
    }
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != GC_CANDIDATE_CATALOG_HEADER_BYTES
        || usize::from(get_u16(bytes, 12)) != GC_CANDIDATE_CATALOG_ROW_BYTES
        || get_u16(bytes, 14) != HASH_ALGORITHM
        || get_u16(bytes, 16) != CRC_ALGORITHM
        || bytes[18..32].iter().any(|byte| *byte != 0)
        || get_u64(bytes, 64) != ROWS_OFFSET
        || bytes[124..].iter().any(|byte| *byte != 0)
    {
        return Err(GcCandidateCatalogError::InvalidEnvelope);
    }
    let mut catalog_hash = [0_u8; 32];
    catalog_hash
        .copy_from_slice(&bytes[CATALOG_HASH_OFFSET..CATALOG_HASH_OFFSET + CATALOG_HASH_BYTES]);
    Ok(GcCandidateCatalogDescriptor {
        generation: get_u64(bytes, 32),
        incorporated_commit_generation: get_u64(bytes, 40),
        incorporated_location_generation: get_u64(bytes, 48),
        row_count: get_u64(bytes, 56),
        footer_offset: get_u64(bytes, 72),
        file_length: get_u64(bytes, 80),
        catalog_hash,
    })
}

fn align_up(value: u64, alignment: u64) -> Result<u64, GcCandidateCatalogError> {
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value
        .checked_add(alignment - remainder)
        .ok_or(GcCandidateCatalogError::ArithmeticOverflow)
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("ASSERT: fixed u16 field is in bounds"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("ASSERT: fixed u32 field is in bounds"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("ASSERT: fixed u64 field is in bounds"),
    )
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
