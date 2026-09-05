//! Bounded Depth-1 Zstd Reference Prefix trials.
//!
//! This module does not assign a durable codec ID or choose an encoding. It
//! produces one fully bounded trial against one verified, independently
//! decodable Base Chunk. The caller's versioned cost policy decides whether
//! the trial beats RAW, ordinary Zstd, and sparse XOR.

use std::fmt;

use fastdup_format::ChunkId;
use zstd::zstd_safe::DCtx;

const MAXIMUM_LOGICAL_CHUNK_BYTES_V1: usize = 256 * 1_024;
const BASE_DEPENDENCY_BYTES: usize = 32;
const ZSTD_PREFIX_LEVEL_V1: i32 = 3;

/// The logical identity of one independently decodable Base Chunk.
///
/// It deliberately contains no physical Location. Relocation or independent
/// re-encoding of the Base Chunk must not change a dependent record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BaseChunkRef {
    chunk_id: ChunkId,
    logical_length: u32,
}

impl BaseChunkRef {
    pub(crate) const fn new(chunk_id: ChunkId, logical_length: u32) -> Self {
        Self {
            chunk_id,
            logical_length,
        }
    }

    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }
}

/// Borrowed Base bytes whose complete length and BLAKE3 identity were checked.
///
/// Construction is the only way to obtain this capability. The eventual
/// persistent Base reader can therefore pass verified bytes into the Reducer
/// without letting an index hint masquerade as content proof.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedBaseChunk<'a> {
    reference: BaseChunkRef,
    bytes: &'a [u8],
}

impl<'a> VerifiedBaseChunk<'a> {
    /// Hashes and admits one independently decoded Base Chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty Chunk or a Chunk above the v1 maximum.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, ZstdPrefixError> {
        validate_logical_length(bytes.len())?;
        Ok(Self {
            reference: BaseChunkRef {
                chunk_id: ChunkId::of(bytes),
                logical_length: length_u32(bytes.len())?,
            },
            bytes,
        })
    }

    /// Verifies bytes against an expected logical Base identity.
    ///
    /// # Errors
    ///
    /// Returns an identity or length error without exposing a capability when
    /// an Exact- or Similarity-Index hint selected the wrong bytes.
    pub fn from_expected(expected: BaseChunkRef, bytes: &'a [u8]) -> Result<Self, ZstdPrefixError> {
        validate_logical_length(bytes.len())?;
        if length_u32(bytes.len())? != expected.logical_length {
            return Err(ZstdPrefixError::BaseLengthMismatch);
        }
        if ChunkId::of(bytes) != expected.chunk_id {
            return Err(ZstdPrefixError::BaseIdentityMismatch);
        }
        Ok(Self {
            reference: expected,
            bytes,
        })
    }

    pub(crate) fn from_verified_location(
        expected: BaseChunkRef,
        bytes: &'a [u8],
    ) -> Result<Self, ZstdPrefixError> {
        validate_logical_length(bytes.len())?;
        if length_u32(bytes.len())? != expected.logical_length {
            return Err(ZstdPrefixError::BaseLengthMismatch);
        }
        debug_assert_eq!(
            ChunkId::of(bytes),
            expected.chunk_id,
            "ASSERT: verified Location bytes retain the Exact candidate identity"
        );
        Ok(Self {
            reference: expected,
            bytes,
        })
    }

    #[must_use]
    pub const fn reference(self) -> BaseChunkRef {
        self.reference
    }

    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

/// One unaccepted Zstd Prefix trial and its exact dependency-plus-frame cost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZstdPrefixTrial {
    encoding: ZstdPrefixEncoding,
    encoded_payload_bytes: u32,
}

impl ZstdPrefixTrial {
    #[must_use]
    pub const fn encoded_payload_bytes(&self) -> u32 {
        self.encoded_payload_bytes
    }

    #[must_use]
    pub const fn encoding(&self) -> &ZstdPrefixEncoding {
        &self.encoding
    }

    /// Consumes the trial after the common physical-cost policy accepts it.
    #[must_use]
    pub fn into_encoding(self) -> ZstdPrefixEncoding {
        self.encoding
    }
}

/// One Depth-1 Zstd frame that requires the exact named Base Chunk as prefix.
///
/// The frame is not a durable record by itself. A future Container format must
/// serialize every field explicitly and pair it at writer, reader/recovery,
/// and scrub boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZstdPrefixEncoding {
    base: BaseChunkRef,
    target_id: ChunkId,
    logical_length: u32,
    level: i32,
    frame: Box<[u8]>,
}

impl ZstdPrefixEncoding {
    #[must_use]
    pub const fn base(&self) -> BaseChunkRef {
        self.base
    }

    #[must_use]
    pub const fn target_id(&self) -> ChunkId {
        self.target_id
    }

    #[must_use]
    pub const fn logical_length(&self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn level(&self) -> i32 {
        self.level
    }

    #[must_use]
    pub fn frame(&self) -> &[u8] {
        &self.frame
    }

    /// Moves the already-compressed frame into the durable format writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the retained trial fields cannot form a bounded
    /// codec-3 record.
    pub fn into_prepared_record(
        self,
    ) -> Result<fastdup_format::PreparedZstdPrefixRecord, fastdup_format::FormatError> {
        fastdup_format::ZstdPrefixRecord::prepare_precompressed(
            self.base.chunk_id,
            self.logical_length,
            self.target_id,
            self.frame,
        )
    }

    /// Decodes into one exactly sized allocation and verifies the target ID.
    ///
    /// # Errors
    ///
    /// Returns a Base proof, codec, decoded-length, allocation, or target
    /// identity error. No decoded bytes escape before every check succeeds.
    pub fn decode(&self, base: VerifiedBaseChunk<'_>) -> Result<Vec<u8>, ZstdPrefixError> {
        if base.reference != self.base {
            return Err(
                if base.reference.logical_length == self.base.logical_length {
                    ZstdPrefixError::BaseIdentityMismatch
                } else {
                    ZstdPrefixError::BaseLengthMismatch
                },
            );
        }
        if self.logical_length != self.base.logical_length {
            return Err(ZstdPrefixError::TargetLengthMismatch);
        }
        let logical_length = usize::try_from(self.logical_length)
            .map_err(|_| ZstdPrefixError::ArithmeticOverflow)?;
        validate_logical_length(logical_length)?;

        let mut decoded = allocate_zeroed(logical_length)?;
        let mut context = DCtx::try_create().ok_or(ZstdPrefixError::AllocationFailed)?;
        context
            .ref_prefix(base.bytes)
            .map_err(|_| ZstdPrefixError::DecompressionFailed)?;
        let written = context
            .decompress(decoded.as_mut_slice(), &self.frame)
            .map_err(|_| ZstdPrefixError::DecompressionFailed)?;
        if written != logical_length {
            return Err(ZstdPrefixError::DecodedLengthMismatch {
                expected: logical_length,
                actual: written,
            });
        }
        if ChunkId::of(&decoded) != self.target_id {
            return Err(ZstdPrefixError::TargetIdentityMismatch);
        }
        Ok(decoded)
    }
}

/// Stateless encoder for one bounded Zstd Reference Prefix trial.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZstdPrefixCodec;

impl ZstdPrefixCodec {
    /// Encodes a same-length target against one verified Base Chunk.
    ///
    /// `maximum_encoded_payload_bytes` includes the 32-byte Base Chunk ID and
    /// the complete Zstd frame. Returning `Ok(None)` means that no valid frame
    /// fits the caller's useful-cost cap; it is an ordinary independent-codec
    /// fallback rather than a codec failure.
    ///
    /// # Errors
    ///
    /// Returns a validation, allocation, arithmetic, or Zstd failure. Context
    /// creation is fallible, and every allocation is bounded by the smaller of
    /// the caller's cap and Zstd's bound for one maximum-size Chunk.
    ///
    /// # Panics
    ///
    /// Panics only if Zstd reports writing beyond the supplied destination or
    /// if an accepted frame violates the caller-provided cap. Both conditions
    /// are internal codec-contract assertions.
    pub fn encode_trial(
        base: VerifiedBaseChunk<'_>,
        target: &[u8],
        maximum_encoded_payload_bytes: usize,
    ) -> Result<Option<ZstdPrefixTrial>, ZstdPrefixError> {
        let target_id = ChunkId::of(target);
        Self::encode_prehashed_trial(base, target_id, target, maximum_encoded_payload_bytes)
    }

    pub(crate) fn encode_prehashed_trial(
        base: VerifiedBaseChunk<'_>,
        target_id: ChunkId,
        target: &[u8],
        maximum_encoded_payload_bytes: usize,
    ) -> Result<Option<ZstdPrefixTrial>, ZstdPrefixError> {
        validate_logical_length(target.len())?;
        if target.len() != base.bytes.len() {
            return Err(ZstdPrefixError::TargetLengthMismatch);
        }
        let Some(maximum_frame_bytes) =
            maximum_encoded_payload_bytes.checked_sub(BASE_DEPENDENCY_BYTES)
        else {
            return Ok(None);
        };
        if maximum_frame_bytes == 0 {
            return Ok(None);
        }
        let frame_capacity = maximum_frame_bytes.min(zstd::zstd_safe::compress_bound(target.len()));
        if frame_capacity == 0 {
            return Ok(None);
        }

        let mut frame = allocate_zeroed(frame_capacity)?;
        let Some(written) = crate::prefix_context::compress(
            base.bytes,
            target,
            frame.as_mut_slice(),
            ZSTD_PREFIX_LEVEL_V1,
        )?
        else {
            return Ok(None);
        };
        assert!(
            written <= frame_capacity,
            "ASSERT: Zstd cannot report bytes beyond its destination"
        );
        frame.truncate(written);
        if frame.is_empty() {
            return Err(ZstdPrefixError::CompressionFailed);
        }
        let encoded_payload_bytes = BASE_DEPENDENCY_BYTES
            .checked_add(frame.len())
            .ok_or(ZstdPrefixError::ArithmeticOverflow)?;
        assert!(
            encoded_payload_bytes <= maximum_encoded_payload_bytes,
            "ASSERT: a successful Prefix trial stays inside its cost cap"
        );

        Ok(Some(ZstdPrefixTrial {
            encoding: ZstdPrefixEncoding {
                base: base.reference,
                target_id,
                logical_length: length_u32(target.len())?,
                level: ZSTD_PREFIX_LEVEL_V1,
                frame: frame.into_boxed_slice(),
            },
            encoded_payload_bytes: length_u32(encoded_payload_bytes)?,
        }))
    }
}

/// Expected input, resource, codec, and integrity failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZstdPrefixError {
    EmptyChunk,
    ChunkTooLarge,
    BaseLengthMismatch,
    BaseIdentityMismatch,
    TargetLengthMismatch,
    AllocationFailed,
    CompressionFailed,
    DecompressionFailed,
    DecodedLengthMismatch { expected: usize, actual: usize },
    TargetIdentityMismatch,
    ArithmeticOverflow,
}

impl fmt::Display for ZstdPrefixError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChunk => formatter.write_str("Prefix chunks must be nonempty"),
            Self::ChunkTooLarge => formatter.write_str("Prefix chunk exceeds the v1 maximum"),
            Self::BaseLengthMismatch => formatter.write_str("Base Chunk length mismatch"),
            Self::BaseIdentityMismatch => formatter.write_str("Base Chunk identity mismatch"),
            Self::TargetLengthMismatch => {
                formatter.write_str("Prefix target length differs from its Base Chunk")
            }
            Self::AllocationFailed => formatter.write_str("Prefix allocation failed"),
            Self::CompressionFailed => formatter.write_str("Zstd Prefix compression failed"),
            Self::DecompressionFailed => formatter.write_str("Zstd Prefix decompression failed"),
            Self::DecodedLengthMismatch { expected, actual } => write!(
                formatter,
                "Zstd Prefix decoded {actual} bytes instead of {expected}"
            ),
            Self::TargetIdentityMismatch => {
                formatter.write_str("Zstd Prefix target identity mismatch")
            }
            Self::ArithmeticOverflow => formatter.write_str("Prefix size arithmetic overflowed"),
        }
    }
}

impl std::error::Error for ZstdPrefixError {}

fn validate_logical_length(length: usize) -> Result<(), ZstdPrefixError> {
    if length == 0 {
        return Err(ZstdPrefixError::EmptyChunk);
    }
    if length > MAXIMUM_LOGICAL_CHUNK_BYTES_V1 {
        return Err(ZstdPrefixError::ChunkTooLarge);
    }
    Ok(())
}

fn allocate_zeroed(length: usize) -> Result<Vec<u8>, ZstdPrefixError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| ZstdPrefixError::AllocationFailed)?;
    bytes.resize(length, 0);
    Ok(bytes)
}

fn length_u32(length: usize) -> Result<u32, ZstdPrefixError> {
    u32::try_from(length).map_err(|_| ZstdPrefixError::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use super::{VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixError};

    #[test]
    fn corrupted_frame_never_returns_unverified_target_bytes() {
        let base_bytes = patterned(64 * 1_024, 0);
        let mut target = base_bytes.clone();
        target[16_384..16_512].fill(0x7f);
        let base = VerifiedBaseChunk::from_bytes(&base_bytes).expect("base is valid");
        let trial = ZstdPrefixCodec::encode_trial(base, &target, target.len())
            .expect("trial succeeds")
            .expect("target compresses under cap");
        let mut encoding = trial.into_encoding();
        let middle = encoding.frame.len() / 2;
        encoding.frame[middle] ^= 0x80;

        assert!(matches!(
            encoding.decode(base),
            Err(ZstdPrefixError::DecompressionFailed | ZstdPrefixError::TargetIdentityMismatch)
        ));
    }

    fn patterned(length: usize, generation: u8) -> Vec<u8> {
        (0..length)
            .map(|offset| {
                let lane = u8::try_from(offset % 251).expect("fixture lane fits u8");
                lane.wrapping_add(generation)
            })
            .collect()
    }
}
