use crate::metadata::{MANIFEST_LEAF_KIND, decode_metadata_object, encode_metadata_object};
use crate::{
    ChunkId, MAX_LOGICAL_CHUNK_BYTES, MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES,
    MetadataFormatError,
};

pub const MANIFEST_HEADER_BYTES: usize = 64;
const MANIFEST_ENTRY_BYTES: usize = 64;
const MANIFEST_MAGIC: &[u8; 8] = b"FDMANL01";
const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestExtent {
    Data {
        logical_length: u64,
        chunk_id: ChunkId,
    },
    Hole {
        logical_length: u64,
    },
    Fill {
        logical_length: u64,
        value: u8,
    },
}

impl ManifestExtent {
    const fn logical_length(&self) -> u64 {
        match *self {
            Self::Data { logical_length, .. }
            | Self::Hole { logical_length }
            | Self::Fill { logical_length, .. } => logical_length,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestLeaf {
    file_length: u64,
    extents: Vec<ManifestExtent>,
}

impl ManifestLeaf {
    /// Constructs one leaf that partitions the complete file range.
    ///
    /// # Errors
    ///
    /// Rejects zero-length extents, oversized DATA chunks, overflow, or a
    /// partition that does not end exactly at EOF.
    pub fn new(
        file_length: u64,
        extents: Vec<ManifestExtent>,
    ) -> Result<Self, MetadataFormatError> {
        validate_partition(file_length, &extents)?;
        payload_length(extents.len())?;
        Ok(Self {
            file_length,
            extents,
        })
    }

    #[must_use]
    pub const fn file_length(&self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub fn extents(&self) -> &[ManifestExtent] {
        &self.extents
    }

    /// Encodes a content-addressed Metadata Object containing `ManifestLeafV1`.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded payload or envelope cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, MetadataFormatError> {
        validate_partition(self.file_length, &self.extents)?;
        let payload_length = payload_length(self.extents.len())?;
        let mut payload = vec![0_u8; payload_length];
        payload[0..8].copy_from_slice(MANIFEST_MAGIC);
        put_u16(&mut payload, 8, FORMAT_VERSION);
        put_u16(&mut payload, 10, 64);
        put_u16(&mut payload, 12, 64);
        put_u64(&mut payload, 16, 0);
        put_u64(&mut payload, 24, self.file_length);
        put_u32(
            &mut payload,
            32,
            u32::try_from(self.extents.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut payload,
            36,
            u32::try_from(payload_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );

        let mut logical_offset = 0_u64;
        for (ordinal, extent) in self.extents.iter().enumerate() {
            let start = MANIFEST_HEADER_BYTES + ordinal * MANIFEST_ENTRY_BYTES;
            let entry = &mut payload[start..start + MANIFEST_ENTRY_BYTES];
            put_u64(entry, 0, logical_offset);
            put_u64(entry, 8, extent.logical_length());
            match extent {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => {
                    put_u16(entry, 16, 1);
                    put_u32(
                        entry,
                        20,
                        u32::try_from(*logical_length)
                            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
                    );
                    entry[24..56].copy_from_slice(&chunk_id.bytes());
                }
                ManifestExtent::Hole { .. } => put_u16(entry, 16, 2),
                ManifestExtent::Fill { value, .. } => {
                    put_u16(entry, 16, 3);
                    entry[56] = *value;
                }
            }
            logical_offset = logical_offset
                .checked_add(extent.logical_length())
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        }
        encode_metadata_object(MANIFEST_LEAF_KIND, &payload)
    }

    /// Fully validates and decodes one `ManifestLeafV1` Metadata Object.
    ///
    /// # Errors
    ///
    /// Returns an envelope, extent, reserved-field, or partition failure.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataFormatError> {
        let object = decode_metadata_object(Some(MANIFEST_LEAF_KIND), bytes)?;
        let payload = object.payload;
        if payload.len() < MANIFEST_HEADER_BYTES || &payload[0..8] != MANIFEST_MAGIC {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if get_u16(payload, 8) != FORMAT_VERSION
            || usize::from(get_u16(payload, 10)) != MANIFEST_HEADER_BYTES
            || usize::from(get_u16(payload, 12)) != MANIFEST_ENTRY_BYTES
            || get_u16(payload, 14) != 0
            || get_u64(payload, 16) != 0
            || payload[40..MANIFEST_HEADER_BYTES]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let extent_count = usize::try_from(get_u32(payload, 32))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        if payload_length(extent_count)? != payload.len()
            || usize::try_from(get_u32(payload, 36)) != Ok(payload.len())
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let mut expected_offset = 0_u64;
        let mut extents = Vec::with_capacity(extent_count);
        for ordinal in 0..extent_count {
            let start = MANIFEST_HEADER_BYTES + ordinal * MANIFEST_ENTRY_BYTES;
            let entry = &payload[start..start + MANIFEST_ENTRY_BYTES];
            let logical_offset = get_u64(entry, 0);
            let logical_length = get_u64(entry, 8);
            let chunk_length = get_u32(entry, 20);
            let mut chunk_id = [0_u8; 32];
            chunk_id.copy_from_slice(&entry[24..56]);
            if logical_offset != expected_offset
                || get_u16(entry, 18) != 0
                || entry[57..64].iter().any(|byte| *byte != 0)
            {
                return Err(MetadataFormatError::InvalidExtent);
            }
            let extent = match get_u16(entry, 16) {
                1 if u64::from(chunk_length) == logical_length && entry[56] == 0 => {
                    ManifestExtent::Data {
                        logical_length,
                        chunk_id: ChunkId::from_bytes(chunk_id),
                    }
                }
                2 if chunk_length == 0 && chunk_id == [0; 32] && entry[56] == 0 => {
                    ManifestExtent::Hole { logical_length }
                }
                3 if chunk_length == 0 && chunk_id == [0; 32] => ManifestExtent::Fill {
                    logical_length,
                    value: entry[56],
                },
                _ => return Err(MetadataFormatError::InvalidExtent),
            };
            expected_offset = expected_offset
                .checked_add(logical_length)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
            extents.push(extent);
        }
        Self::new(get_u64(payload, 24), extents)
    }
}

fn validate_partition(
    file_length: u64,
    extents: &[ManifestExtent],
) -> Result<(), MetadataFormatError> {
    let mut end = 0_u64;
    for extent in extents {
        let length = extent.logical_length();
        if length == 0 {
            return Err(MetadataFormatError::InvalidExtent);
        }
        if matches!(extent, ManifestExtent::Data { .. })
            && length
                > u64::try_from(MAX_LOGICAL_CHUNK_BYTES)
                    .map_err(|_| MetadataFormatError::ArithmeticOverflow)?
        {
            return Err(MetadataFormatError::InvalidExtent);
        }
        end = end
            .checked_add(length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    }
    if end != file_length || (file_length == 0) != extents.is_empty() {
        return Err(MetadataFormatError::InvalidPartition);
    }
    Ok(())
}

fn payload_length(extent_count: usize) -> Result<usize, MetadataFormatError> {
    let length = MANIFEST_HEADER_BYTES
        .checked_add(
            extent_count
                .checked_mul(MANIFEST_ENTRY_BYTES)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?,
        )
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    if length > MAX_METADATA_OBJECT_BYTES - METADATA_HEADER_BYTES {
        return Err(MetadataFormatError::InvalidObjectLength(length));
    }
    Ok(length)
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
