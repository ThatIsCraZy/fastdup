use core::fmt;

pub const METADATA_HEADER_BYTES: usize = 4_096;
pub const MAX_METADATA_OBJECT_BYTES: usize = 16 * 1_024 * 1_024;
const METADATA_MAGIC: &[u8; 8] = b"FDMDOBJ1";
const FORMAT_VERSION: u16 = 1;
const BLAKE3_256_ALGORITHM: u16 = 1;
const PAYLOAD_CRC_OFFSET: usize = 80;
const HEADER_CRC_OFFSET: usize = 84;
const OBJECT_ALIGNMENT: usize = 4_096;
const OBJECT_ID_DOMAIN: &[u8] = b"fastdup-metadata-object-v1\0";

pub(crate) const MANIFEST_LEAF_KIND: u16 = 1;
pub(crate) const NAMESPACE_ROOT_KIND: u16 = 2;
pub(crate) const EXACT_INDEX_RUN_SET_KIND: u16 = 3;
pub(crate) const MANIFEST_INNER_KIND: u16 = 4;

/// The durable payload kind selected by one verified Metadata Object envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataObjectKind {
    ManifestLeaf,
    NamespaceRoot,
    ExactIndexRunSet,
    ManifestInnerNode,
    Unknown(u16),
}

/// Fully verifies a generic Metadata Object and returns its durable payload
/// kind without decoding that payload's kind-specific fields.
///
/// # Errors
///
/// Returns any envelope integrity, length, padding, or identity failure.
pub fn metadata_object_kind(bytes: &[u8]) -> Result<MetadataObjectKind, MetadataFormatError> {
    let object = decode_metadata_object(None, bytes)?;
    Ok(match object.kind {
        MANIFEST_LEAF_KIND => MetadataObjectKind::ManifestLeaf,
        NAMESPACE_ROOT_KIND => MetadataObjectKind::NamespaceRoot,
        EXACT_INDEX_RUN_SET_KIND => MetadataObjectKind::ExactIndexRunSet,
        MANIFEST_INNER_KIND => MetadataObjectKind::ManifestInnerNode,
        unknown => MetadataObjectKind::Unknown(unknown),
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MetadataObjectId([u8; 32]);

impl MetadataObjectId {
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

    /// Validates a generic metadata envelope and returns its paired identity.
    ///
    /// # Errors
    ///
    /// Returns any envelope integrity or identity failure.
    pub fn from_encoded(bytes: &[u8]) -> Result<Self, MetadataFormatError> {
        decode_metadata_object(None, bytes).map(|object| object.id)
    }
}

pub(crate) struct DecodedMetadataObject<'a> {
    pub id: MetadataObjectId,
    pub kind: u16,
    pub payload: &'a [u8],
}

pub(crate) fn encode_metadata_object(
    object_kind: u16,
    payload: &[u8],
) -> Result<Vec<u8>, MetadataFormatError> {
    let payload_length =
        u64::try_from(payload.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    let unaligned_length = METADATA_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let file_length = align_up(unaligned_length, OBJECT_ALIGNMENT)?;
    if file_length > MAX_METADATA_OBJECT_BYTES {
        return Err(MetadataFormatError::InvalidObjectLength(file_length));
    }
    let object_id = calculate_object_id(object_kind, payload_length, payload)?;
    let mut bytes = vec![0_u8; file_length];
    bytes[0..8].copy_from_slice(METADATA_MAGIC);
    put_u16(&mut bytes, 8, FORMAT_VERSION);
    put_u16(&mut bytes, 10, 4_096);
    put_u16(&mut bytes, 12, object_kind);
    put_u16(&mut bytes, 14, BLAKE3_256_ALGORITHM);
    put_u64(&mut bytes, 32, payload_length);
    put_u64(
        &mut bytes,
        40,
        u64::try_from(file_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    bytes[48..80].copy_from_slice(&object_id.bytes());
    put_u32(&mut bytes, PAYLOAD_CRC_OFFSET, crc32c::crc32c(payload));
    let header_checksum =
        crc32c_with_zeroed_field(&bytes[..METADATA_HEADER_BYTES], HEADER_CRC_OFFSET);
    put_u32(&mut bytes, HEADER_CRC_OFFSET, header_checksum);
    bytes[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + payload.len()].copy_from_slice(payload);
    Ok(bytes)
}

pub(crate) fn decode_metadata_object(
    expected_kind: Option<u16>,
    bytes: &[u8],
) -> Result<DecodedMetadataObject<'_>, MetadataFormatError> {
    if bytes.len() < METADATA_HEADER_BYTES
        || bytes.len() > MAX_METADATA_OBJECT_BYTES
        || !bytes.len().is_multiple_of(OBJECT_ALIGNMENT)
    {
        return Err(MetadataFormatError::InvalidObjectLength(bytes.len()));
    }
    let header = &bytes[..METADATA_HEADER_BYTES];
    if &header[0..8] != METADATA_MAGIC {
        return Err(MetadataFormatError::InvalidMagic);
    }
    let stored_header_checksum = get_u32(header, HEADER_CRC_OFFSET);
    if crc32c_with_zeroed_field(header, HEADER_CRC_OFFSET) != stored_header_checksum {
        return Err(MetadataFormatError::HeaderChecksumMismatch);
    }
    let object_kind = get_u16(header, 12);
    if get_u16(header, 8) != FORMAT_VERSION
        || usize::from(get_u16(header, 10)) != METADATA_HEADER_BYTES
        || get_u16(header, 14) != BLAKE3_256_ALGORITHM
        || get_u64(header, 16) != 0
        || get_u64(header, 24) != 0
        || expected_kind.is_some_and(|expected| expected != object_kind)
        || header[88..].iter().any(|byte| *byte != 0)
    {
        return Err(MetadataFormatError::UnsupportedOrReservedField);
    }
    let payload_length = usize::try_from(get_u64(header, 32))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    let payload_end = METADATA_HEADER_BYTES
        .checked_add(payload_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    if payload_end > bytes.len()
        || usize::try_from(get_u64(header, 40)) != Ok(bytes.len())
        || align_up(payload_end, OBJECT_ALIGNMENT)? != bytes.len()
    {
        return Err(MetadataFormatError::InvalidObjectLength(bytes.len()));
    }
    if bytes[payload_end..].iter().any(|byte| *byte != 0) {
        return Err(MetadataFormatError::NonZeroPadding);
    }
    let payload = &bytes[METADATA_HEADER_BYTES..payload_end];
    if crc32c::crc32c(payload) != get_u32(header, PAYLOAD_CRC_OFFSET) {
        return Err(MetadataFormatError::PayloadChecksumMismatch);
    }
    let mut stored_id = [0_u8; 32];
    stored_id.copy_from_slice(&header[48..80]);
    let stored_id = MetadataObjectId::new(stored_id).ok_or(MetadataFormatError::ZeroObjectId)?;
    let computed_id = calculate_object_id(
        object_kind,
        u64::try_from(payload.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        payload,
    )?;
    if stored_id != computed_id {
        return Err(MetadataFormatError::ObjectIdMismatch);
    }
    Ok(DecodedMetadataObject {
        id: stored_id,
        kind: object_kind,
        payload,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataFormatError {
    InvalidObjectLength(usize),
    InvalidMagic,
    HeaderChecksumMismatch,
    PayloadChecksumMismatch,
    ObjectIdMismatch,
    ZeroObjectId,
    UnsupportedOrReservedField,
    NonZeroPadding,
    InvalidPayload,
    InvalidExtent,
    InvalidPartition,
    ArithmeticOverflow,
}

impl fmt::Display for MetadataFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MetadataFormatError {}

fn calculate_object_id(
    object_kind: u16,
    payload_length: u64,
    payload: &[u8],
) -> Result<MetadataObjectId, MetadataFormatError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBJECT_ID_DOMAIN);
    hasher.update(&object_kind.to_le_bytes());
    hasher.update(&payload_length.to_le_bytes());
    hasher.update(payload);
    MetadataObjectId::new(*hasher.finalize().as_bytes()).ok_or(MetadataFormatError::ZeroObjectId)
}

fn align_up(value: usize, alignment: usize) -> Result<usize, MetadataFormatError> {
    let mask = alignment
        .checked_sub(1)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    value
        .checked_add(mask)
        .map(|candidate| candidate & !mask)
        .ok_or(MetadataFormatError::ArithmeticOverflow)
}

fn crc32c_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    let mut checksummed = bytes.to_vec();
    checksummed[field_offset..field_offset + 4].fill(0);
    crc32c::crc32c(&checksummed)
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
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 field"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field"),
    )
}
