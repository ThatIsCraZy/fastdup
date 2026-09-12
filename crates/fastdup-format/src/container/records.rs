//! Independent RAW/Zstd record encoding and complete record/content validation.
use super::adaptive::PrehashedChunk;
use super::compression::compress_zstd_v1;
use super::dependent::{
    is_dependent_codec, validate_sparse_xor_record, validate_zstd_prefix_record,
};
use super::payload::VerifiedChunkPayload;
use super::{
    CHUNK_TABLE_ENTRY_BYTES, CHUNK_TABLE_ENTRY_BYTES_U16, ChunkId, FORMAT_VERSION, FormatError,
    MAX_DECODED_RECORD_BYTES, MAX_LOGICAL_CHUNK_BYTES, MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES,
    RAW_CODEC, RAW_PAYLOAD_OFFSET, RAW_PAYLOAD_OFFSET_U32, RECORD_ALIGNMENT, RECORD_CRC_OFFSET,
    RECORD_HEADER_BYTES, RECORD_HEADER_BYTES_U16, RECORD_HEADER_BYTES_U32, RECORD_MAGIC,
    SPARSE_XOR_CODEC, ZSTD_CODEC, ZSTD_LEVEL_V1, ZSTD_PREFIX_CODEC, align_up_usize,
    crc32c_with_zeroed_field, get_u16, get_u32, get_u64, put_u16, put_u32,
};
use fastdup_copy_metrics::CopyClass;
use fastdup_copy_metrics::record_copy;
use std::cell::RefCell;
use std::sync::Arc;

thread_local! {
    static RECORD_DECODER: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EncodingCodec {
    Raw,
    Zstd,
    ZstdPrefix,
    SparseXor,
}

#[derive(Debug)]
pub(super) struct DecodedEncodingRecord {
    pub(super) codec: EncodingCodec,
    pub(super) chunks: Vec<RawRecord>,
    pub(super) logical_bytes: u64,
}

#[allow(clippy::too_many_lines)]
pub(super) fn encode_zstd_record(chunks: &[&[u8]], level: i32) -> Result<Vec<u8>, FormatError> {
    let prehashed = chunks
        .iter()
        .map(|chunk| PrehashedChunk::new(ChunkId::of(chunk), chunk))
        .collect::<Vec<_>>();
    encode_prehashed_zstd_record(&prehashed, level)
}

#[allow(clippy::too_many_lines)]
fn encode_prehashed_zstd_record(
    chunks: &[PrehashedChunk<'_>],
    level: i32,
) -> Result<Vec<u8>, FormatError> {
    if chunks.is_empty() || level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let decoded = collect_prehashed_decoded(chunks)?;
    let payload = compress_zstd_v1(&decoded, level)?;
    encode_prehashed_zstd_record_from_payload(chunks, decoded.len(), &payload, level)
}

pub(super) fn collect_prehashed_decoded(
    chunks: &[PrehashedChunk<'_>],
) -> Result<Vec<u8>, FormatError> {
    let decoded_length = prehashed_decoded_length(chunks)?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_length)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    for chunk in chunks {
        decoded.extend_from_slice(chunk.bytes);
    }
    record_copy(CopyClass::CompressionRegionConcatenation, decoded.len());
    Ok(decoded)
}

pub(super) fn prehashed_decoded_length(
    chunks: &[PrehashedChunk<'_>],
) -> Result<usize, FormatError> {
    let mut decoded_length = 0_usize;
    for chunk in chunks {
        validate_logical_chunk_length(chunk.bytes.len())?;
        decoded_length = decoded_length
            .checked_add(chunk.bytes.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    if decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    Ok(decoded_length)
}

#[allow(clippy::too_many_lines)]
fn encode_prehashed_zstd_record_from_payload(
    chunks: &[PrehashedChunk<'_>],
    decoded_length: usize,
    payload: &[u8],
    level: i32,
) -> Result<Vec<u8>, FormatError> {
    let record_length = zstd_record_length(chunks.len(), payload.len())?;
    let mut bytes = vec![0_u8; record_length];
    encode_prehashed_zstd_record_into(chunks, decoded_length, payload, level, &mut bytes)?;
    Ok(bytes)
}

pub(super) fn zstd_record_length(
    chunk_count: usize,
    payload_length: usize,
) -> Result<usize, FormatError> {
    let table_bytes = chunk_count
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_end = payload_offset
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

#[allow(clippy::too_many_lines)]
pub(super) fn encode_prehashed_zstd_record_into(
    chunks: &[PrehashedChunk<'_>],
    decoded_length: usize,
    payload: &[u8],
    level: i32,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    let record_length = zstd_record_length(chunks.len(), payload.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    let metadata_length = RECORD_HEADER_BYTES + chunks.len() * CHUNK_TABLE_ENTRY_BYTES;
    bytes[..metadata_length].fill(0);
    write_prehashed_zstd_metadata(
        chunks,
        decoded_length,
        payload,
        level,
        &mut bytes[..metadata_length],
    )?;
    let payload_end = metadata_length + payload.len();
    bytes[metadata_length..payload_end].copy_from_slice(payload);
    bytes[payload_end..].fill(0);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

pub(super) fn write_prehashed_zstd_metadata(
    chunks: &[PrehashedChunk<'_>],
    decoded_length: usize,
    payload: &[u8],
    level: i32,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    assert_eq!(
        bytes.len(),
        RECORD_HEADER_BYTES + chunks.len() * CHUNK_TABLE_ENTRY_BYTES
    );
    if chunks.is_empty() || level != ZSTD_LEVEL_V1 || decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    let table_bytes = chunks
        .len()
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = zstd_record_length(chunks.len(), payload.len())?;

    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, ZSTD_CODEC);
    put_u16(bytes, 14, 0);
    put_u32(
        bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        36,
        u32::try_from(decoded_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        40,
        u32::try_from(payload_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        44,
        u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(
        bytes,
        56,
        u32::try_from(chunks.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    bytes[96..100].copy_from_slice(&level.to_le_bytes());

    let mut decoded_offset = 0_usize;
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let table_offset = RECORD_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        bytes[table_offset..table_offset + 32].copy_from_slice(&chunk.chunk_id.0);
        put_u32(
            bytes,
            table_offset + 32,
            u32::try_from(decoded_offset).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        put_u32(
            bytes,
            table_offset + 36,
            u32::try_from(chunk.bytes.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
        );
        decoded_offset = decoded_offset
            .checked_add(chunk.bytes.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn decode_encoding_record(bytes: &[u8]) -> Result<DecodedEncodingRecord, FormatError> {
    decode_encoding_record_mode(bytes, true, None)
}

#[allow(clippy::too_many_lines)]
pub(super) fn verify_encoding_record(bytes: &[u8]) -> Result<DecodedEncodingRecord, FormatError> {
    decode_encoding_record_mode(bytes, false, None)
}

#[allow(clippy::too_many_lines)]
pub(super) fn decode_encoding_record_mode(
    bytes: &[u8],
    retain_payloads: bool,
    backing: Option<(&Arc<Vec<u8>>, usize)>,
) -> Result<DecodedEncodingRecord, FormatError> {
    let validated = ValidatedRecord::new(bytes)?;
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || (!is_dependent_codec(get_u16(bytes, 12)) && bytes[64..96].iter().any(|byte| *byte != 0))
    {
        return Err(FormatError::InvalidZstdRecord);
    }
    if get_u16(bytes, 12) == RAW_CODEC {
        let (chunk_id, payload) = decode_raw_record_validated(validated)?;
        let logical_bytes =
            u64::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
        let chunks = if retain_payloads {
            vec![RawRecord {
                payload: if let Some((owner, start)) = backing {
                    let mut view = VerifiedChunkPayload::from_shared(
                        chunk_id,
                        Arc::clone(owner),
                        start + RAW_PAYLOAD_OFFSET,
                        payload.len(),
                    )?;
                    view.decoded_offset = 0;
                    view
                } else {
                    VerifiedChunkPayload::from_owned(chunk_id, payload.to_vec())
                },
            }]
        } else {
            Vec::new()
        };
        return Ok(DecodedEncodingRecord {
            codec: EncodingCodec::Raw,
            chunks,
            logical_bytes,
        });
    }
    if get_u16(bytes, 12) == ZSTD_PREFIX_CODEC {
        validate_zstd_prefix_record(bytes)?;
        return Err(FormatError::ZstdPrefixBaseRequired);
    }
    if get_u16(bytes, 12) == SPARSE_XOR_CODEC {
        validate_sparse_xor_record(bytes)?;
        return Err(FormatError::DependentBaseRequired);
    }
    if get_u16(bytes, 12) != ZSTD_CODEC
        || i32::from_le_bytes(
            bytes[96..100]
                .try_into()
                .expect("ASSERT: fixed codec parameter range is four bytes"),
        ) != ZSTD_LEVEL_V1
        || bytes[100..128].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdRecord);
    }

    let decoded_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if decoded_length == 0 || decoded_length > MAX_DECODED_RECORD_BYTES {
        return Err(FormatError::InvalidZstdRecord);
    }
    let chunk_count =
        usize::try_from(get_u32(bytes, 56)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if chunk_count == 0 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let table_bytes = chunk_count
        .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_payload_offset = RECORD_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_offset =
        usize::try_from(get_u32(bytes, 40)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_offset != expected_payload_offset || payload_offset > bytes.len() {
        return Err(FormatError::InvalidZstdRecord);
    }
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length == 0 {
        return Err(FormatError::InvalidZstdRecord);
    }
    let payload_end = payload_offset
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if payload_end > bytes.len()
        || expected_record_length != bytes.len()
        || bytes[payload_end..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdRecord);
    }

    let decoded = RECORD_DECODER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd::bulk::Decompressor::new().map_err(|_| FormatError::ZstdFailure)?);
        }
        slot.as_mut()
            .expect("initialized Record decoder")
            .decompress(&bytes[payload_offset..payload_end], decoded_length)
            .map_err(|_| FormatError::ZstdFailure)
    })?;
    if decoded.len() != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    let mut verified_chunks = Vec::new();
    verified_chunks
        .try_reserve_exact(chunk_count)
        .map_err(|_| FormatError::ArithmeticOverflow)?;
    let mut expected_decoded_offset = 0_usize;
    for ordinal in 0..chunk_count {
        let table_offset = RECORD_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                    .ok_or(FormatError::ArithmeticOverflow)?,
            )
            .ok_or(FormatError::ArithmeticOverflow)?;
        let decoded_offset = usize::try_from(get_u32(bytes, table_offset + 32))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let logical_length = usize::try_from(get_u32(bytes, table_offset + 36))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        validate_logical_chunk_length(logical_length)?;
        let decoded_end = decoded_offset
            .checked_add(logical_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if decoded_offset != expected_decoded_offset
            || decoded_end > decoded.len()
            || get_u64(bytes, table_offset + 40) != 0
            || get_u64(bytes, table_offset + 48) != 0
            || get_u64(bytes, table_offset + 56) != 0
        {
            return Err(FormatError::InvalidZstdRecord);
        }
        let mut stored_id = [0_u8; 32];
        stored_id.copy_from_slice(&bytes[table_offset..table_offset + 32]);
        let payload = &decoded[decoded_offset..decoded_end];
        let chunk_id = ChunkId::of(payload);
        if chunk_id.0 != stored_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        if retain_payloads {
            verified_chunks.push((chunk_id, decoded_offset, logical_length));
        }
        expected_decoded_offset = decoded_end;
    }
    if expected_decoded_offset != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    let chunks = if retain_payloads {
        let backing = Arc::new(decoded);
        verified_chunks
            .into_iter()
            .map(|(chunk_id, offset, length)| {
                Ok(RawRecord {
                    payload: VerifiedChunkPayload::from_shared(
                        chunk_id,
                        Arc::clone(&backing),
                        offset,
                        length,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, FormatError>>()?
    } else {
        Vec::new()
    };
    Ok(DecodedEncodingRecord {
        codec: EncodingCodec::Zstd,
        chunks,
        logical_bytes: u64::try_from(decoded_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRecord {
    pub(super) payload: VerifiedChunkPayload,
}

impl RawRecord {
    /// Encodes one nonempty logical chunk as a v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns an error when the chunk or resulting record exceeds v1 bounds.
    pub fn encode(payload: &[u8]) -> Result<Vec<u8>, FormatError> {
        Self::encode_prehashed(PrehashedChunk::new(ChunkId::of(payload), payload))
    }

    fn encode_prehashed(chunk: PrehashedChunk<'_>) -> Result<Vec<u8>, FormatError> {
        let record_length = raw_record_length(chunk.bytes.len())?;
        let mut bytes = vec![0_u8; record_length];
        encode_prehashed_raw_record_into(chunk, &mut bytes)?;
        Ok(bytes)
    }

    /// Validates and decodes one v1 RAW record.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, or logical Chunk-ID integrity error.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let (chunk_id, payload) = decode_raw_record_view(bytes)?;
        Ok(Self {
            payload: VerifiedChunkPayload::from_owned(chunk_id, payload.to_vec()),
        })
    }

    pub(super) fn from_verified_payload(payload: VerifiedChunkPayload) -> Self {
        Self { payload }
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.payload.chunk_id()
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload.into_payload()
    }

    #[must_use]
    pub fn into_verified_payload(self) -> VerifiedChunkPayload {
        self.payload
    }

    #[must_use]
    pub fn verified_payload(&self) -> VerifiedChunkPayload {
        self.payload.clone()
    }
}

pub(super) fn encode_prehashed_raw_record_into(
    chunk: PrehashedChunk<'_>,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    let record_length = raw_record_length(chunk.bytes.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    let metadata_length = RAW_PAYLOAD_OFFSET;
    bytes[..metadata_length].fill(0);
    write_prehashed_raw_metadata(chunk, &mut bytes[..metadata_length])?;
    let payload_end = metadata_length + chunk.bytes.len();
    bytes[metadata_length..payload_end].copy_from_slice(chunk.bytes);
    bytes[payload_end..].fill(0);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

pub(super) fn write_prehashed_raw_metadata(
    chunk: PrehashedChunk<'_>,
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    assert_eq!(bytes.len(), RAW_PAYLOAD_OFFSET);
    let payload = chunk.bytes;
    let record_length = raw_record_length(payload.len())?;

    let record_length_u32 =
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    let payload_length_u32 =
        u32::try_from(payload.len()).map_err(|_| FormatError::ArithmeticOverflow)?;
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, RAW_CODEC);
    put_u16(bytes, 14, 0);
    put_u32(bytes, 32, record_length_u32);
    put_u32(bytes, 36, payload_length_u32);
    put_u32(bytes, 40, RAW_PAYLOAD_OFFSET_U32);
    put_u32(bytes, 44, payload_length_u32);
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(bytes, 56, 1);

    bytes[128..160].copy_from_slice(&chunk.chunk_id.0);
    put_u32(bytes, 160, 0);
    put_u32(bytes, 164, payload_length_u32);
    Ok(())
}

/// Length, magic, declared length and complete CRC checked exactly once.
#[derive(Clone, Copy)]
struct ValidatedRecord<'a>(&'a [u8]);

impl<'a> ValidatedRecord<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, FormatError> {
        if bytes.len() < MIN_RAW_RECORD_BYTES
            || bytes.len() > MAX_RECORD_BYTES
            || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
        {
            return Err(FormatError::InvalidRecordLength(bytes.len()));
        }
        if &bytes[0..8] != RECORD_MAGIC {
            return Err(FormatError::InvalidRecordMagic);
        }
        let declared_length =
            usize::try_from(get_u32(bytes, 32)).map_err(|_| FormatError::ArithmeticOverflow)?;
        if declared_length != bytes.len() {
            return Err(FormatError::InvalidRecordLength(declared_length));
        }
        let stored_checksum = get_u32(bytes, RECORD_CRC_OFFSET);
        if crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET) != stored_checksum {
            return Err(FormatError::RecordChecksumMismatch);
        }
        Ok(Self(bytes))
    }
}

fn decode_raw_record_view(bytes: &[u8]) -> Result<(ChunkId, &[u8]), FormatError> {
    decode_raw_record_validated(ValidatedRecord::new(bytes)?)
}

fn decode_raw_record_validated(
    record: ValidatedRecord<'_>,
) -> Result<(ChunkId, &[u8]), FormatError> {
    let bytes = record.0;
    validate_raw_record_constants(bytes)?;

    let decoded_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    validate_logical_chunk_length(decoded_length)?;
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    let logical_length =
        usize::try_from(get_u32(bytes, 164)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length != decoded_length || logical_length != decoded_length {
        return Err(FormatError::InvalidRawRecord);
    }
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let expected_record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if expected_record_length != bytes.len() || bytes[payload_end..].iter().any(|byte| *byte != 0) {
        return Err(FormatError::InvalidRawRecord);
    }

    let mut stored_id = [0_u8; 32];
    stored_id.copy_from_slice(&bytes[128..160]);
    let payload = &bytes[RAW_PAYLOAD_OFFSET..payload_end];
    let chunk_id = ChunkId::of(payload);
    if chunk_id.0 != stored_id {
        return Err(FormatError::ChunkHashMismatch);
    }
    Ok((chunk_id, payload))
}

pub(super) fn validate_logical_chunk_length(length: usize) -> Result<(), FormatError> {
    if length == 0 || length > MAX_LOGICAL_CHUNK_BYTES {
        return Err(FormatError::InvalidRawRecord);
    }
    Ok(())
}

pub(super) fn raw_record_length(payload_length: usize) -> Result<usize, FormatError> {
    validate_logical_chunk_length(payload_length)?;
    let unaligned_length = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(unaligned_length, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

pub(super) fn validate_raw_record_constants(bytes: &[u8]) -> Result<(), FormatError> {
    if get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 12) != RAW_CODEC
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 40)) != Ok(RAW_PAYLOAD_OFFSET)
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || get_u32(bytes, 56) != 1
        || bytes[64..128].iter().any(|byte| *byte != 0)
        || get_u32(bytes, 160) != 0
        || get_u64(bytes, 168) != 0
        || get_u64(bytes, 176) != 0
        || get_u64(bytes, 184) != 0
    {
        return Err(FormatError::InvalidRawRecord);
    }
    Ok(())
}
