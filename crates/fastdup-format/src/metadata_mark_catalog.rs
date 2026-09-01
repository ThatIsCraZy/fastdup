use core::fmt;

use crate::{CommitRecord, MetadataObjectId};

pub const METADATA_MARK_CATALOG_HEADER_BYTES: usize = 4_096;
pub const METADATA_MARK_CATALOG_ROW_BYTES: usize = 32;
const HEADER_BYTES_U64: u64 = 4_096;
const ROW_BYTES_U64: u64 = 32;
const HEADER_MAGIC: [u8; 8] = *b"FDMMARK1";
const FOOTER_MAGIC: [u8; 8] = *b"FDMMARKF";
const FORMAT_VERSION: u16 = 2;
const HASH_OFFSET: usize = 128;
const HASH_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MetadataMarkCatalogRunKind {
    Snapshot = 1,
    Addition = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataMarkCatalogDescriptor {
    run_kind: MetadataMarkCatalogRunKind,
    generation: u64,
    base_generation: u64,
    commit_binding: [u8; 32],
    row_count: u64,
    footer_offset: u64,
    file_length: u64,
    rows_hash: [u8; 32],
}

impl MetadataMarkCatalogDescriptor {
    /// Decodes and cross-checks paired catalog envelopes.
    ///
    /// # Errors
    ///
    /// Returns layout, version, reserved-byte, mirror, length, or envelope-hash
    /// failures.
    pub fn decode(
        header: &[u8],
        footer: &[u8],
        actual_length: u64,
    ) -> Result<Self, MetadataMarkCatalogError> {
        if actual_length < 2 * HEADER_BYTES_U64 {
            return Err(MetadataMarkCatalogError::InvalidEnvelope);
        }
        let first = decode_envelope(header, HEADER_MAGIC)?;
        let second = decode_envelope(footer, FOOTER_MAGIC)?;
        if first != second || first.file_length != actual_length {
            return Err(MetadataMarkCatalogError::InvalidEnvelope);
        }
        first.validate_layout()?;
        Ok(first)
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn run_kind(self) -> MetadataMarkCatalogRunKind {
        self.run_kind
    }

    #[must_use]
    pub const fn base_generation(self) -> u64 {
        self.base_generation
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
    pub fn rows_end(self) -> Option<u64> {
        HEADER_BYTES_U64.checked_add(self.row_count.checked_mul(ROW_BYTES_U64)?)
    }

    #[must_use]
    pub fn row_offset(self, ordinal: u64) -> Option<u64> {
        if ordinal >= self.row_count {
            return None;
        }
        HEADER_BYTES_U64.checked_add(ordinal.checked_mul(ROW_BYTES_U64)?)
    }

    #[must_use]
    pub fn start_audit(self) -> MetadataMarkCatalogAudit {
        MetadataMarkCatalogAudit {
            descriptor: self,
            ordinal: 0,
            previous: None,
            hasher: rows_hasher(
                self.run_kind,
                self.generation,
                self.base_generation,
                self.commit_binding,
                self.row_count,
            ),
        }
    }

    fn validate_layout(self) -> Result<(), MetadataMarkCatalogError> {
        let rows_end = self
            .rows_end()
            .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)?;
        let expected_footer = align_up(rows_end, HEADER_BYTES_U64)?;
        let expected_length = expected_footer
            .checked_add(HEADER_BYTES_U64)
            .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)?;
        let valid_chain = match self.run_kind {
            MetadataMarkCatalogRunKind::Snapshot => self.base_generation == 0,
            MetadataMarkCatalogRunKind::Addition => {
                self.base_generation != 0 && self.base_generation < self.generation
            }
        };
        if self.generation == 0
            || !valid_chain
            || self.footer_offset != expected_footer
            || self.file_length != expected_length
        {
            return Err(MetadataMarkCatalogError::InvalidEnvelope);
        }
        Ok(())
    }

    fn encode_envelope(self, magic: [u8; 8]) -> [u8; METADATA_MARK_CATALOG_HEADER_BYTES] {
        let mut bytes = [0_u8; METADATA_MARK_CATALOG_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&magic);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, 4_096);
        put_u16(&mut bytes, 12, 32);
        put_u16(&mut bytes, 14, self.run_kind as u16);
        put_u64(&mut bytes, 16, self.generation);
        bytes[24..56].copy_from_slice(&self.commit_binding);
        put_u64(&mut bytes, 56, self.row_count);
        put_u64(&mut bytes, 64, HEADER_BYTES_U64);
        put_u64(&mut bytes, 72, self.footer_offset);
        put_u64(&mut bytes, 80, self.file_length);
        bytes[88..120].copy_from_slice(&self.rows_hash);
        put_u64(&mut bytes, 120, self.base_generation);
        let hash = envelope_hash(&bytes);
        bytes[HASH_OFFSET..HASH_OFFSET + HASH_BYTES].copy_from_slice(&hash);
        bytes
    }
}

pub struct MetadataMarkCatalogStreamEncoder {
    descriptor: MetadataMarkCatalogDescriptor,
    ordinal: u64,
    previous: Option<MetadataObjectId>,
    hasher: blake3::Hasher,
}

impl MetadataMarkCatalogStreamEncoder {
    /// Starts one bounded-memory immutable catalog encoder.
    ///
    /// # Errors
    ///
    /// Returns zero-generation or checked-layout overflow failures.
    pub fn new(
        generation: u64,
        commit_binding: [u8; 32],
        row_count: u64,
    ) -> Result<Self, MetadataMarkCatalogError> {
        Self::new_run(
            MetadataMarkCatalogRunKind::Snapshot,
            generation,
            0,
            commit_binding,
            row_count,
        )
    }

    /// Starts one additive run chained to an earlier catalog generation.
    ///
    /// # Errors
    ///
    /// Returns invalid-generation or checked-layout overflow failures.
    pub fn new_addition(
        generation: u64,
        base_generation: u64,
        commit_binding: [u8; 32],
        row_count: u64,
    ) -> Result<Self, MetadataMarkCatalogError> {
        Self::new_run(
            MetadataMarkCatalogRunKind::Addition,
            generation,
            base_generation,
            commit_binding,
            row_count,
        )
    }

    fn new_run(
        run_kind: MetadataMarkCatalogRunKind,
        generation: u64,
        base_generation: u64,
        commit_binding: [u8; 32],
        row_count: u64,
    ) -> Result<Self, MetadataMarkCatalogError> {
        let rows_end = HEADER_BYTES_U64
            .checked_add(
                row_count
                    .checked_mul(ROW_BYTES_U64)
                    .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)?,
            )
            .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)?;
        let footer_offset = align_up(rows_end, HEADER_BYTES_U64)?;
        let file_length = footer_offset
            .checked_add(HEADER_BYTES_U64)
            .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)?;
        let descriptor = MetadataMarkCatalogDescriptor {
            run_kind,
            generation,
            base_generation,
            commit_binding,
            row_count,
            footer_offset,
            file_length,
            rows_hash: [0; 32],
        };
        descriptor.validate_layout()?;
        Ok(Self {
            descriptor,
            ordinal: 0,
            previous: None,
            hasher: rows_hasher(
                run_kind,
                generation,
                base_generation,
                commit_binding,
                row_count,
            ),
        })
    }

    /// Encodes the next strictly Object-ID-ordered row.
    ///
    /// # Errors
    ///
    /// Returns order, duplicate, or declared-row-count failures.
    pub fn push(
        &mut self,
        object_id: MetadataObjectId,
    ) -> Result<(u64, [u8; METADATA_MARK_CATALOG_ROW_BYTES]), MetadataMarkCatalogError> {
        if self.ordinal >= self.descriptor.row_count {
            return Err(MetadataMarkCatalogError::RowCountMismatch);
        }
        if self
            .previous
            .is_some_and(|previous| previous.bytes() >= object_id.bytes())
        {
            return Err(MetadataMarkCatalogError::NonCanonicalOrder);
        }
        let offset = self
            .descriptor
            .row_offset(self.ordinal)
            .ok_or(MetadataMarkCatalogError::RowCountMismatch)?;
        let bytes = object_id.bytes();
        self.hasher.update(&bytes);
        self.ordinal += 1;
        self.previous = Some(object_id);
        Ok((offset, bytes))
    }

    /// Finishes a complete row stream and returns paired envelopes.
    ///
    /// # Errors
    ///
    /// Returns an error unless exactly the declared row count was emitted.
    pub fn finish(
        self,
    ) -> Result<
        (
            MetadataMarkCatalogDescriptor,
            [u8; METADATA_MARK_CATALOG_HEADER_BYTES],
            [u8; METADATA_MARK_CATALOG_HEADER_BYTES],
        ),
        MetadataMarkCatalogError,
    > {
        if self.ordinal != self.descriptor.row_count {
            return Err(MetadataMarkCatalogError::RowCountMismatch);
        }
        let mut descriptor = self.descriptor;
        descriptor.rows_hash = *self.hasher.finalize().as_bytes();
        Ok((
            descriptor,
            descriptor.encode_envelope(HEADER_MAGIC),
            descriptor.encode_envelope(FOOTER_MAGIC),
        ))
    }
}

pub struct MetadataMarkCatalogAudit {
    descriptor: MetadataMarkCatalogDescriptor,
    ordinal: u64,
    previous: Option<MetadataObjectId>,
    hasher: blake3::Hasher,
}

impl MetadataMarkCatalogAudit {
    /// Audits the next exact serialized row.
    ///
    /// # Errors
    ///
    /// Returns row-length, identity, order, duplicate, or count failures.
    pub fn push(&mut self, row: &[u8]) -> Result<MetadataObjectId, MetadataMarkCatalogError> {
        if row.len() != METADATA_MARK_CATALOG_ROW_BYTES || self.ordinal >= self.descriptor.row_count
        {
            return Err(MetadataMarkCatalogError::RowCountMismatch);
        }
        let raw =
            <[u8; 32]>::try_from(row).map_err(|_| MetadataMarkCatalogError::RowCountMismatch)?;
        let object_id =
            MetadataObjectId::new(raw).ok_or(MetadataMarkCatalogError::InvalidObjectId)?;
        if self
            .previous
            .is_some_and(|previous| previous.bytes() >= raw)
        {
            return Err(MetadataMarkCatalogError::NonCanonicalOrder);
        }
        self.hasher.update(row);
        self.ordinal += 1;
        self.previous = Some(object_id);
        Ok(object_id)
    }

    /// Finishes the independent row-stream audit.
    ///
    /// # Errors
    ///
    /// Returns incomplete-stream or whole-row-hash failures.
    pub fn finish(self) -> Result<(), MetadataMarkCatalogError> {
        if self.ordinal != self.descriptor.row_count {
            return Err(MetadataMarkCatalogError::RowCountMismatch);
        }
        if self.hasher.finalize().as_bytes() != &self.descriptor.rows_hash {
            return Err(MetadataMarkCatalogError::RowsHashMismatch);
        }
        Ok(())
    }
}

#[must_use]
/// Binds one catalog generation to an exact ordered Commit segment.
///
/// # Panics
///
/// Panics only on a platform whose address space can hold more than `u64::MAX`
/// Commit records, outside the supported storage envelope.
pub fn metadata_mark_commit_binding(records: &[CommitRecord]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fastdup-metadata-mark-commit-binding-v1\0");
    hasher.update(
        &u64::try_from(records.len())
            .expect("ASSERT: retained Commit count fits u64")
            .to_le_bytes(),
    );
    for record in records {
        hasher.update(&record.encode());
    }
    *hasher.finalize().as_bytes()
}

fn rows_hasher(
    run_kind: MetadataMarkCatalogRunKind,
    generation: u64,
    base_generation: u64,
    commit_binding: [u8; 32],
    row_count: u64,
) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fastdup-metadata-mark-rows-v2\0");
    hasher.update(&(run_kind as u16).to_le_bytes());
    hasher.update(&base_generation.to_le_bytes());
    hasher.update(&generation.to_le_bytes());
    hasher.update(&commit_binding);
    hasher.update(&row_count.to_le_bytes());
    hasher
}

fn decode_envelope(
    bytes: &[u8],
    magic: [u8; 8],
) -> Result<MetadataMarkCatalogDescriptor, MetadataMarkCatalogError> {
    if bytes.len() != METADATA_MARK_CATALOG_HEADER_BYTES
        || bytes.get(0..8) != Some(magic.as_slice())
        || get_u16(bytes, 10)? != 4_096
        || get_u16(bytes, 12)? != 32
        || bytes[160..].iter().any(|byte| *byte != 0)
    {
        return Err(MetadataMarkCatalogError::InvalidEnvelope);
    }
    if get_u16(bytes, 8)? != FORMAT_VERSION {
        return Err(MetadataMarkCatalogError::InvalidEnvelope);
    }
    let run_kind = match get_u16(bytes, 14)? {
        1 => MetadataMarkCatalogRunKind::Snapshot,
        2 => MetadataMarkCatalogRunKind::Addition,
        _ => return Err(MetadataMarkCatalogError::InvalidEnvelope),
    };
    let base_generation = get_u64(bytes, 120)?;
    if bytes[HASH_OFFSET..HASH_OFFSET + HASH_BYTES] != envelope_hash(bytes) {
        return Err(MetadataMarkCatalogError::EnvelopeHashMismatch);
    }
    let descriptor = MetadataMarkCatalogDescriptor {
        run_kind,
        generation: get_u64(bytes, 16)?,
        base_generation,
        commit_binding: bytes[24..56]
            .try_into()
            .expect("ASSERT: checked Metadata catalog binding is 32 bytes"),
        row_count: get_u64(bytes, 56)?,
        footer_offset: get_u64(bytes, 72)?,
        file_length: get_u64(bytes, 80)?,
        rows_hash: bytes[88..120]
            .try_into()
            .expect("ASSERT: checked Metadata catalog row hash is 32 bytes"),
    };
    if get_u64(bytes, 64)? != HEADER_BYTES_U64 {
        return Err(MetadataMarkCatalogError::InvalidEnvelope);
    }
    descriptor.validate_layout()?;
    Ok(descriptor)
}

fn envelope_hash(bytes: &[u8]) -> [u8; 32] {
    assert_eq!(
        bytes.len(),
        METADATA_MARK_CATALOG_HEADER_BYTES,
        "ASSERT: Metadata mark envelope hash receives one complete block"
    );
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fastdup-metadata-mark-envelope-v1\0");
    hasher.update(&bytes[..HASH_OFFSET]);
    hasher.update(&[0; HASH_BYTES]);
    hasher.update(&bytes[HASH_OFFSET + HASH_BYTES..]);
    *hasher.finalize().as_bytes()
}

fn align_up(value: u64, alignment: u64) -> Result<u64, MetadataMarkCatalogError> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or(MetadataMarkCatalogError::ArithmeticOverflow)
    }
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(input: &[u8], offset: usize) -> Result<u16, MetadataMarkCatalogError> {
    Ok(u16::from_le_bytes(
        input
            .get(offset..offset + 2)
            .ok_or(MetadataMarkCatalogError::InvalidEnvelope)?
            .try_into()
            .expect("ASSERT: checked u16 range is exact"),
    ))
}

fn get_u64(input: &[u8], offset: usize) -> Result<u64, MetadataMarkCatalogError> {
    Ok(u64::from_le_bytes(
        input
            .get(offset..offset + 8)
            .ok_or(MetadataMarkCatalogError::InvalidEnvelope)?
            .try_into()
            .expect("ASSERT: checked u64 range is exact"),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataMarkCatalogError {
    InvalidEnvelope,
    EnvelopeHashMismatch,
    RowsHashMismatch,
    InvalidObjectId,
    NonCanonicalOrder,
    NonzeroPadding,
    RowCountMismatch,
    IdentityCollision,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for MetadataMarkCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MetadataMarkCatalogError {}
