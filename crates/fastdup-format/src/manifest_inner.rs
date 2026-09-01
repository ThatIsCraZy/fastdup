use core::fmt;

use crate::metadata::{MANIFEST_INNER_KIND, decode_metadata_object, encode_metadata_object};
use crate::{
    MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId,
};

pub const MANIFEST_INNER_HEADER_BYTES: usize = 64;
pub const MANIFEST_CHILD_RANGE_BYTES: usize = 64;

const MANIFEST_INNER_MAGIC: &[u8; 8] = b"FDMANI01";
const FORMAT_VERSION: u16 = 2;

/// One child Metadata Object and the byte range it covers inside its parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestChildRange {
    logical_offset: u64,
    logical_length: u64,
    child: MetadataObjectId,
    allocated_bytes: Option<u64>,
}

impl ManifestChildRange {
    fn validate_range(
        logical_offset: u64,
        logical_length: u64,
    ) -> Result<(), ManifestInnerNodeError> {
        if logical_length == 0 {
            return Err(ManifestInnerNodeError::InvalidChildRange);
        }
        logical_offset
            .checked_add(logical_length)
            .ok_or(ManifestInnerNodeError::ArithmeticOverflow)?;
        Ok(())
    }

    /// Constructs one child range with an authenticated subtree allocation
    /// total. DATA and FILL bytes are allocated; HOLE bytes are not.
    ///
    /// # Errors
    ///
    /// Rejects invalid ranges or an allocation total above the logical range.
    pub fn new_with_allocated_bytes(
        logical_offset: u64,
        logical_length: u64,
        allocated_bytes: u64,
        child: MetadataObjectId,
    ) -> Result<Self, ManifestInnerNodeError> {
        if allocated_bytes > logical_length {
            return Err(ManifestInnerNodeError::InvalidChildRange);
        }
        Self::validate_range(logical_offset, logical_length)?;
        Ok(Self {
            logical_offset,
            logical_length,
            child,
            allocated_bytes: Some(allocated_bytes),
        })
    }

    #[must_use]
    pub const fn logical_offset(self) -> u64 {
        self.logical_offset
    }

    #[must_use]
    pub const fn logical_length(self) -> u64 {
        self.logical_length
    }

    #[must_use]
    pub const fn child(self) -> MetadataObjectId {
        self.child
    }

    /// Returns the authenticated subtree allocation total.
    #[must_use]
    pub const fn allocated_bytes(self) -> Option<u64> {
        self.allocated_bytes
    }
}

/// One immutable Manifest inner node whose children partition its file range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestInnerNode {
    file_length: u64,
    level: u16,
    children: Vec<ManifestChildRange>,
}

impl ManifestInnerNode {
    /// Constructs an inner node from already sorted child ranges.
    ///
    /// Level zero is reserved for Manifest leaves. An inner node is nonempty,
    /// starts at logical offset zero, and its ordered children must cover
    /// `[0, file_length)` exactly without gaps or overlap.
    ///
    /// # Errors
    ///
    /// Rejects a leaf level, invalid range, noncanonical partition, arithmetic
    /// overflow, or a child count that cannot fit one bounded Metadata Object.
    pub fn new(
        file_length: u64,
        level: u16,
        children: Vec<ManifestChildRange>,
    ) -> Result<Self, ManifestInnerNodeError> {
        validate_node(file_length, level, &children)?;
        payload_length(children.len())?;
        Ok(Self {
            file_length,
            level,
            children,
        })
    }

    #[must_use]
    pub const fn file_length(&self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn level(&self) -> u16 {
        self.level
    }

    #[must_use]
    pub fn children(&self) -> &[ManifestChildRange] {
        &self.children
    }

    /// Returns the checked authenticated allocation total.
    ///
    /// # Errors
    ///
    /// Returns arithmetic overflow if corrupt in-memory totals cannot be
    /// summed, although constructors and decoders prevent this state.
    pub fn allocated_bytes(&self) -> Result<Option<u64>, ManifestInnerNodeError> {
        self.children.iter().try_fold(Some(0_u64), |total, child| {
            let total = total.ok_or(ManifestInnerNodeError::InvalidChildRange)?;
            let child_allocated = child
                .allocated_bytes
                .ok_or(ManifestInnerNodeError::InvalidChildRange)?;
            total
                .checked_add(child_allocated)
                .map(Some)
                .ok_or(ManifestInnerNodeError::ArithmeticOverflow)
        })
    }

    /// Encodes the current Manifest inner node in the content-addressed
    /// Metadata Object envelope as object kind 4.
    ///
    /// # Errors
    ///
    /// Returns partition, arithmetic, size, allocation, or envelope failures.
    pub fn encode(&self) -> Result<Vec<u8>, ManifestInnerNodeError> {
        validate_node(self.file_length, self.level, &self.children)?;
        let payload_length = payload_length(self.children.len())?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_length)
            .map_err(|_| ManifestInnerNodeError::OutOfMemory)?;
        payload.resize(payload_length, 0);
        payload[0..8].copy_from_slice(MANIFEST_INNER_MAGIC);
        put_u16(&mut payload, 8, FORMAT_VERSION);
        put_u16(
            &mut payload,
            10,
            u16::try_from(MANIFEST_INNER_HEADER_BYTES)
                .map_err(|_| ManifestInnerNodeError::ArithmeticOverflow)?,
        );
        put_u16(
            &mut payload,
            12,
            u16::try_from(MANIFEST_CHILD_RANGE_BYTES)
                .map_err(|_| ManifestInnerNodeError::ArithmeticOverflow)?,
        );
        put_u16(&mut payload, 14, self.level);
        put_u64(&mut payload, 24, self.file_length);
        put_u32(
            &mut payload,
            32,
            u32::try_from(self.children.len())
                .map_err(|_| ManifestInnerNodeError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut payload,
            36,
            u32::try_from(payload_length)
                .map_err(|_| ManifestInnerNodeError::ArithmeticOverflow)?,
        );
        for (ordinal, child) in self.children.iter().copied().enumerate() {
            let start = entry_start(ordinal)?;
            let entry = &mut payload[start..start + MANIFEST_CHILD_RANGE_BYTES];
            put_u64(entry, 0, child.logical_offset);
            put_u64(entry, 8, child.logical_length);
            entry[16..48].copy_from_slice(&child.child.bytes());
            if let Some(allocated_bytes) = child.allocated_bytes {
                put_u64(entry, 48, allocated_bytes);
            }
        }
        Ok(encode_metadata_object(MANIFEST_INNER_KIND, &payload)?)
    }

    /// Decodes and fully validates one content-addressed inner Manifest node.
    ///
    /// Count and payload equations are proven before reserving the child vector.
    ///
    /// # Errors
    ///
    /// Returns envelope, header, count, allocation, child, or partition errors.
    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestInnerNodeError> {
        let object = decode_metadata_object(Some(MANIFEST_INNER_KIND), bytes)?;
        let payload = object.payload;
        if payload.len() < MANIFEST_INNER_HEADER_BYTES
            || &payload[0..8] != MANIFEST_INNER_MAGIC
            || get_u16(payload, 8) != FORMAT_VERSION
            || usize::from(get_u16(payload, 10)) != MANIFEST_INNER_HEADER_BYTES
            || usize::from(get_u16(payload, 12)) != MANIFEST_CHILD_RANGE_BYTES
            || get_u64(payload, 16) != 0
            || payload[40..MANIFEST_INNER_HEADER_BYTES]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(ManifestInnerNodeError::InvalidPayload);
        }
        let child_count = usize::try_from(get_u32(payload, 32))
            .map_err(|_| ManifestInnerNodeError::ArithmeticOverflow)?;
        if payload_length(child_count)? != payload.len()
            || usize::try_from(get_u32(payload, 36)) != Ok(payload.len())
        {
            return Err(ManifestInnerNodeError::InvalidPayload);
        }

        let mut children = Vec::new();
        children
            .try_reserve_exact(child_count)
            .map_err(|_| ManifestInnerNodeError::OutOfMemory)?;
        for ordinal in 0..child_count {
            let start = entry_start(ordinal)?;
            let entry = &payload[start..start + MANIFEST_CHILD_RANGE_BYTES];
            if entry[56..].iter().any(|byte| *byte != 0) {
                return Err(ManifestInnerNodeError::InvalidChildRange);
            }
            let mut child = [0_u8; 32];
            child.copy_from_slice(&entry[16..48]);
            let child =
                MetadataObjectId::new(child).ok_or(ManifestInnerNodeError::InvalidChildRange)?;
            let range = ManifestChildRange::new_with_allocated_bytes(
                get_u64(entry, 0),
                get_u64(entry, 8),
                get_u64(entry, 48),
                child,
            )?;
            children.push(range);
        }
        Self::new(get_u64(payload, 24), get_u16(payload, 14), children)
    }
}

fn validate_node(
    file_length: u64,
    level: u16,
    children: &[ManifestChildRange],
) -> Result<(), ManifestInnerNodeError> {
    if level == 0 {
        return Err(ManifestInnerNodeError::InvalidLevel);
    }
    if children.is_empty() || file_length == 0 {
        return Err(ManifestInnerNodeError::InvalidPartition);
    }
    let mut expected_offset = 0_u64;
    for child in children {
        if child.logical_length == 0 {
            return Err(ManifestInnerNodeError::InvalidChildRange);
        }
        if child.logical_offset != expected_offset {
            return Err(ManifestInnerNodeError::InvalidPartition);
        }
        if child
            .allocated_bytes
            .is_none_or(|allocated| allocated > child.logical_length)
        {
            return Err(ManifestInnerNodeError::InvalidChildRange);
        }
        expected_offset = child
            .logical_offset
            .checked_add(child.logical_length)
            .ok_or(ManifestInnerNodeError::ArithmeticOverflow)?;
    }
    if expected_offset != file_length {
        return Err(ManifestInnerNodeError::InvalidPartition);
    }
    Ok(())
}

fn payload_length(child_count: usize) -> Result<usize, ManifestInnerNodeError> {
    if child_count == 0 {
        return Err(ManifestInnerNodeError::InvalidPartition);
    }
    let length = MANIFEST_INNER_HEADER_BYTES
        .checked_add(
            child_count
                .checked_mul(MANIFEST_CHILD_RANGE_BYTES)
                .ok_or(ManifestInnerNodeError::ArithmeticOverflow)?,
        )
        .ok_or(ManifestInnerNodeError::ArithmeticOverflow)?;
    if length > MAX_METADATA_OBJECT_BYTES - METADATA_HEADER_BYTES {
        return Err(ManifestInnerNodeError::InvalidPayload);
    }
    Ok(length)
}

fn entry_start(ordinal: usize) -> Result<usize, ManifestInnerNodeError> {
    MANIFEST_INNER_HEADER_BYTES
        .checked_add(
            ordinal
                .checked_mul(MANIFEST_CHILD_RANGE_BYTES)
                .ok_or(ManifestInnerNodeError::ArithmeticOverflow)?,
        )
        .ok_or(ManifestInnerNodeError::ArithmeticOverflow)
}

/// Failure to construct or verify a Manifest inner node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestInnerNodeError {
    Metadata(MetadataFormatError),
    InvalidLevel,
    InvalidChildRange,
    InvalidPartition,
    InvalidPayload,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for ManifestInnerNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ManifestInnerNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Metadata(error) => Some(error),
            Self::InvalidLevel
            | Self::InvalidChildRange
            | Self::InvalidPartition
            | Self::InvalidPayload
            | Self::ArithmeticOverflow
            | Self::OutOfMemory => None,
        }
    }
}

impl From<MetadataFormatError> for ManifestInnerNodeError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
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
