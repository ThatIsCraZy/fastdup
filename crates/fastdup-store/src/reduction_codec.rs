//! Worker-local independent encoding and decoding.
//!
//! This module chooses only between complete RAW and Zstd payloads. It does
//! not assign durable codec identifiers, serialize records, group logical
//! chunks, or account for record/container metadata; those responsibilities
//! remain with the reduction engine and `fastdup-format`.
//!
//! A [`WorkerCodec`] owns and reuses one Zstd compression context and one Zstd
//! decompression context. It is deliberately used through `&mut self`: one
//! worker owns it, no global lock is required, and Zstd's internal worker pool
//! is explicitly disabled. Prepared dictionary objects are immutable and may
//! be shared between worker-local codecs without sharing mutable codec state.

use std::fmt;
use std::io;
use std::sync::Arc;

use fastdup_format::ChunkId;

const V1_MINIMUM_SAVINGS_BYTES: u64 = 4_096;
const V1_MINIMUM_SAVINGS_PERCENT: u128 = 3;
const PERCENT_DENOMINATOR: u128 = 100;

/// The BLAKE3-256 content identity of exact dictionary bytes.
///
/// This is intentionally distinct from a logical [`ChunkId`] even though both
/// use the same digest algorithm. A dictionary is an immutable decoding
/// dependency, not a logical data chunk.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DictionaryId([u8; 32]);

impl DictionaryId {
    #[must_use]
    pub(crate) const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub(crate) fn of(bytes: &[u8]) -> Self {
        Self(ChunkId::of(bytes).bytes())
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Immutable, content-verified dictionary bytes ready for worker-local use.
///
/// Construction verifies the caller-supplied identity before the bytes can be
/// used. The Zstd contexts load these bytes on first use and retain the loaded
/// dictionary while consecutive jobs use the same ID. The object contains no
/// mutable Zstd context and therefore needs no shared lock.
pub struct PreparedDictionary {
    id: DictionaryId,
    bytes: Arc<[u8]>,
}

impl PreparedDictionary {
    /// Verifies and prepares immutable dictionary bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::EmptyDictionary`] for an empty dependency or
    /// [`CodecError::DictionaryHashMismatch`] when `expected_id` is not the
    /// BLAKE3-256 identity of `bytes`.
    pub(crate) fn new(expected_id: DictionaryId, bytes: Arc<[u8]>) -> Result<Self, CodecError> {
        if bytes.is_empty() {
            return Err(CodecError::EmptyDictionary);
        }
        let actual_id = DictionaryId::of(&bytes);
        if actual_id != expected_id {
            return Err(CodecError::DictionaryHashMismatch {
                expected: expected_id,
                actual: actual_id,
            });
        }
        Ok(Self {
            id: expected_id,
            bytes,
        })
    }

    #[must_use]
    pub(crate) const fn id(&self) -> DictionaryId {
        self.id
    }

    #[must_use]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for PreparedDictionary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedDictionary")
            .field("id", &self.id)
            .field("length", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// One complete independent encoding payload.
///
/// The decoded length is explicit even for RAW so the format writer can pair
/// the same invariant at encode, recovery, and scrub. `dictionary_id` is the
/// only dictionary identity permitted during decode; similar bytes or another
/// valid dictionary are never substituted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndependentEncoding {
    Raw {
        payload: Box<[u8]>,
        decoded_length: usize,
    },
    Zstd {
        payload: Box<[u8]>,
        decoded_length: usize,
        level: i32,
        dictionary_id: Option<DictionaryId>,
    },
}

impl IndependentEncoding {
    #[must_use]
    pub(crate) const fn decoded_length(&self) -> usize {
        match self {
            Self::Raw { decoded_length, .. } | Self::Zstd { decoded_length, .. } => *decoded_length,
        }
    }

    #[must_use]
    pub(crate) fn payload(&self) -> &[u8] {
        match self {
            Self::Raw { payload, .. } | Self::Zstd { payload, .. } => payload,
        }
    }

    #[must_use]
    pub const fn dictionary_id(&self) -> Option<DictionaryId> {
        match self {
            Self::Raw { .. } => None,
            Self::Zstd { dictionary_id, .. } => *dictionary_id,
        }
    }
}

/// The two complete physical-byte costs compared by the v1 policy.
///
/// Payload bytes are always exact: RAW is the complete decoded payload and
/// Zstd is the complete emitted frame. Metadata bytes are supplied separately
/// so the engine can add record headers, chunk tables, alignment, recovery
/// index growth, and amortized container bytes before asking the same policy to
/// make its final choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandidateCosts {
    raw: EncodingCost,
    zstd: EncodingCost,
}

impl CandidateCosts {
    #[must_use]
    pub(crate) const fn from_payload_bytes(
        raw_payload_bytes: u64,
        zstd_payload_bytes: u64,
    ) -> Self {
        Self {
            raw: EncodingCost::payload_only(raw_payload_bytes),
            zstd: EncodingCost::payload_only(zstd_payload_bytes),
        }
    }

    /// Adds codec-specific metadata without changing the measured payloads.
    #[must_use]
    pub const fn with_metadata(self, raw_metadata_bytes: u64, zstd_metadata_bytes: u64) -> Self {
        Self {
            raw: self.raw.with_metadata(raw_metadata_bytes),
            zstd: self.zstd.with_metadata(zstd_metadata_bytes),
        }
    }

    #[must_use]
    pub const fn raw(self) -> EncodingCost {
        self.raw
    }

    #[must_use]
    pub const fn zstd(self) -> EncodingCost {
        self.zstd
    }
}

/// Complete payload and caller-supplied metadata cost for one alternative.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodingCost {
    payload_bytes: u64,
    metadata_bytes: u64,
}

impl EncodingCost {
    #[must_use]
    pub(crate) const fn payload_only(payload_bytes: u64) -> Self {
        Self {
            payload_bytes,
            metadata_bytes: 0,
        }
    }

    #[must_use]
    pub const fn with_metadata(self, metadata_bytes: u64) -> Self {
        Self {
            payload_bytes: self.payload_bytes,
            metadata_bytes,
        }
    }

    #[must_use]
    pub const fn payload_bytes(self) -> u64 {
        self.payload_bytes
    }

    #[must_use]
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes
    }

    fn total_bytes(self) -> Result<u64, CodecError> {
        self.payload_bytes
            .checked_add(self.metadata_bytes)
            .ok_or(CodecError::CostOverflow)
    }
}

/// Output of one deterministic v1 independent-encoding decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodingDecision {
    encoding: IndependentEncoding,
    payload_costs: CandidateCosts,
}

impl EncodingDecision {
    #[must_use]
    pub const fn encoding(&self) -> &IndependentEncoding {
        &self.encoding
    }

    #[must_use]
    pub const fn payload_costs(&self) -> CandidateCosts {
        self.payload_costs
    }

    #[must_use]
    pub(crate) fn into_encoding(self) -> IndependentEncoding {
        self.encoding
    }
}

/// A reusable codec context owned by exactly one reduction worker.
///
/// The type is `Send` through the underlying Zstd contexts but intentionally
/// has no `Sync` wrapper. Moving it between coarse worker assignments is safe;
/// sharing one instance between concurrent jobs is not part of the interface.
pub(crate) struct WorkerCodec {
    compressor: zstd::bulk::Compressor<'static>,
    decompressor: zstd::bulk::Decompressor<'static>,
    encoder_configuration: Option<EncoderConfiguration>,
    decoder_dictionary: DecoderDictionary,
}

impl WorkerCodec {
    /// Creates both reusable contexts and disables Zstd's internal workers.
    ///
    /// # Errors
    ///
    /// Returns the underlying context or parameter initialization error.
    pub(crate) fn new() -> Result<Self, CodecError> {
        let mut compressor = zstd::bulk::Compressor::new(0).map_err(CodecError::Compression)?;
        compressor
            .set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))
            .map_err(CodecError::Compression)?;
        let decompressor = zstd::bulk::Decompressor::new().map_err(CodecError::Decompression)?;
        Ok(Self {
            compressor,
            decompressor,
            encoder_configuration: Some(EncoderConfiguration {
                level: 0,
                dictionary_id: None,
            }),
            decoder_dictionary: DecoderDictionary::None,
        })
    }

    /// Trials Zstd and selects it only when the v1 payload-only threshold wins.
    ///
    /// `decoded_length` is supplied independently and must equal
    /// `decoded.len()`. A nonempty dictionary has already been BLAKE3-verified
    /// by [`PreparedDictionary::new`]. The emitted Zstd payload records that
    /// exact Dictionary ID.
    ///
    /// The returned cost pair contains exact payload bytes. Before durable
    /// format selection, the engine may add alternative-specific metadata via
    /// [`CandidateCosts::with_metadata`] and call [`accept_zstd_v1`] again. If
    /// that final comparison fails, it must use RAW.
    ///
    /// # Errors
    ///
    /// Returns an explicit length, size-conversion, cost, or Zstd encoding
    /// error. An incompressible payload is not an error; it selects RAW.
    pub(crate) fn encode_v1(
        &mut self,
        decoded: &[u8],
        decoded_length: usize,
        level: i32,
        dictionary: Option<&PreparedDictionary>,
    ) -> Result<EncodingDecision, CodecError> {
        validate_declared_length(decoded.len(), decoded_length)?;
        self.configure_compressor(level, dictionary)?;
        let compressed = self
            .compressor
            .compress(decoded)
            .map_err(CodecError::Compression)?;
        let raw_payload_bytes =
            u64::try_from(decoded.len()).map_err(|_| CodecError::SizeOverflow)?;
        let zstd_payload_bytes =
            u64::try_from(compressed.len()).map_err(|_| CodecError::SizeOverflow)?;
        let payload_costs =
            CandidateCosts::from_payload_bytes(raw_payload_bytes, zstd_payload_bytes);

        let encoding = if accept_zstd_v1(payload_costs)? {
            IndependentEncoding::Zstd {
                payload: compressed.into_boxed_slice(),
                decoded_length,
                level,
                dictionary_id: dictionary.map(PreparedDictionary::id),
            }
        } else {
            IndependentEncoding::Raw {
                payload: decoded.to_vec().into_boxed_slice(),
                decoded_length,
            }
        };
        Ok(EncodingDecision {
            encoding,
            payload_costs,
        })
    }

    /// Decodes exactly one independent payload into its declared byte length.
    ///
    /// Zstd dictionary decode requires the exact content ID serialized by the
    /// encoding. Missing, unexpected, or differently identified dictionaries
    /// are rejected before touching the decompressor. A successful call has
    /// verified codec completion and exact decoded length; the reduction
    /// reader must subsequently verify logical Chunk IDs before returning or
    /// caching these bytes.
    ///
    /// # Errors
    ///
    /// Returns integrity errors for declared-length or dictionary mismatches,
    /// and [`CodecError::Decompression`] for an invalid Zstd frame.
    pub(crate) fn decode(
        &mut self,
        encoding: &IndependentEncoding,
        expected_decoded_length: usize,
        dictionary: Option<&PreparedDictionary>,
    ) -> Result<Vec<u8>, CodecError> {
        if expected_decoded_length == 0 {
            return Err(CodecError::EmptyDecodedPayload);
        }
        if encoding.decoded_length() != expected_decoded_length {
            return Err(CodecError::StoredDecodedLengthMismatch {
                stored: encoding.decoded_length(),
                expected: expected_decoded_length,
            });
        }
        match encoding {
            IndependentEncoding::Raw {
                payload,
                decoded_length,
            } => {
                if dictionary.is_some() {
                    return Err(CodecError::UnexpectedDictionary);
                }
                if payload.len() != *decoded_length {
                    return Err(CodecError::DecodedLengthMismatch {
                        expected: *decoded_length,
                        actual: payload.len(),
                    });
                }
                Ok(payload.to_vec())
            }
            IndependentEncoding::Zstd {
                payload,
                decoded_length,
                dictionary_id,
                ..
            } => {
                verify_decode_dictionary(*dictionary_id, dictionary)?;
                self.configure_decompressor(dictionary)?;
                let decoded = self
                    .decompressor
                    .decompress(payload, *decoded_length)
                    .map_err(CodecError::Decompression)?;
                if decoded.len() != *decoded_length {
                    return Err(CodecError::DecodedLengthMismatch {
                        expected: *decoded_length,
                        actual: decoded.len(),
                    });
                }
                Ok(decoded)
            }
        }
    }

    fn configure_compressor(
        &mut self,
        level: i32,
        dictionary: Option<&PreparedDictionary>,
    ) -> Result<(), CodecError> {
        let requested = EncoderConfiguration {
            level,
            dictionary_id: dictionary.map(PreparedDictionary::id),
        };
        if self.encoder_configuration == Some(requested) {
            return Ok(());
        }
        self.compressor
            .set_dictionary(level, dictionary.map_or(&[], PreparedDictionary::bytes))
            .map_err(CodecError::Compression)?;
        self.encoder_configuration = Some(requested);
        Ok(())
    }

    fn configure_decompressor(
        &mut self,
        dictionary: Option<&PreparedDictionary>,
    ) -> Result<(), CodecError> {
        let requested = dictionary.map_or(DecoderDictionary::None, |dictionary| {
            DecoderDictionary::Some(dictionary.id())
        });
        if self.decoder_dictionary == requested {
            return Ok(());
        }
        self.decompressor
            .set_dictionary(dictionary.map_or(&[], PreparedDictionary::bytes))
            .map_err(CodecError::Decompression)?;
        self.decoder_dictionary = requested;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EncoderConfiguration {
    level: i32,
    dictionary_id: Option<DictionaryId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecoderDictionary {
    None,
    Some(DictionaryId),
}

/// Applies the inclusive v1 3%-and-4096-byte acceptance rule.
///
/// The calculation is integer-only and deterministic. `costs` may be payload
/// only or may already include the engine's format metadata. Zstd must be
/// strictly smaller, save at least 4,096 bytes, and save at least 3% of the
/// complete RAW alternative.
pub(crate) fn accept_zstd_v1(costs: CandidateCosts) -> Result<bool, CodecError> {
    let raw_bytes = costs.raw.total_bytes()?;
    let zstd_bytes = costs.zstd.total_bytes()?;
    let Some(savings) = raw_bytes.checked_sub(zstd_bytes) else {
        return Ok(false);
    };
    if savings < V1_MINIMUM_SAVINGS_BYTES {
        return Ok(false);
    }
    Ok(u128::from(savings) * PERCENT_DENOMINATOR
        >= u128::from(raw_bytes) * V1_MINIMUM_SAVINGS_PERCENT)
}

fn validate_declared_length(actual: usize, declared: usize) -> Result<(), CodecError> {
    if actual == 0 || declared == 0 {
        return Err(CodecError::EmptyDecodedPayload);
    }
    if actual != declared {
        return Err(CodecError::DeclaredLengthMismatch { declared, actual });
    }
    Ok(())
}

fn verify_decode_dictionary(
    expected: Option<DictionaryId>,
    provided: Option<&PreparedDictionary>,
) -> Result<(), CodecError> {
    match (expected, provided) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(CodecError::UnexpectedDictionary),
        (Some(expected), None) => Err(CodecError::MissingDictionary(expected)),
        (Some(expected), Some(provided)) if expected == provided.id() => Ok(()),
        (Some(expected), Some(provided)) => Err(CodecError::WrongDictionary {
            expected,
            provided: provided.id(),
        }),
    }
}

/// Expected codec/configuration failures and decode integrity failures.
///
/// Zstd failures are normal `Result` values. Impossible executor ownership or
/// cursor states belong to production-fatal assertions in the caller, not this
/// codec interface.
#[derive(Debug)]
pub enum CodecError {
    EmptyDecodedPayload,
    DeclaredLengthMismatch {
        declared: usize,
        actual: usize,
    },
    StoredDecodedLengthMismatch {
        stored: usize,
        expected: usize,
    },
    DecodedLengthMismatch {
        expected: usize,
        actual: usize,
    },
    EmptyDictionary,
    DictionaryHashMismatch {
        expected: DictionaryId,
        actual: DictionaryId,
    },
    MissingDictionary(DictionaryId),
    UnexpectedDictionary,
    WrongDictionary {
        expected: DictionaryId,
        provided: DictionaryId,
    },
    SizeOverflow,
    CostOverflow,
    Compression(io::Error),
    Decompression(io::Error),
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDecodedPayload => formatter.write_str("decoded payload is empty"),
            Self::DeclaredLengthMismatch { declared, actual } => write!(
                formatter,
                "declared decoded length {declared} disagrees with input length {actual}"
            ),
            Self::StoredDecodedLengthMismatch { stored, expected } => write!(
                formatter,
                "stored decoded length {stored} disagrees with expected length {expected}"
            ),
            Self::DecodedLengthMismatch { expected, actual } => write!(
                formatter,
                "codec produced {actual} decoded bytes instead of {expected}"
            ),
            Self::EmptyDictionary => formatter.write_str("dictionary object is empty"),
            Self::DictionaryHashMismatch { expected, actual } => write!(
                formatter,
                "dictionary content ID {actual:?} disagrees with expected ID {expected:?}"
            ),
            Self::MissingDictionary(id) => {
                write!(formatter, "required dictionary {id:?} was not supplied")
            }
            Self::UnexpectedDictionary => {
                formatter.write_str("a dictionary was supplied for an encoding without one")
            }
            Self::WrongDictionary { expected, provided } => write!(
                formatter,
                "dictionary {provided:?} cannot replace required dictionary {expected:?}"
            ),
            Self::SizeOverflow => formatter.write_str("payload length does not fit u64"),
            Self::CostOverflow => formatter.write_str("complete encoding cost overflowed u64"),
            Self::Compression(error) => write!(formatter, "Zstd compression failed: {error}"),
            Self::Decompression(error) => write!(formatter, "Zstd decompression failed: {error}"),
        }
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compression(error) | Self::Decompression(error) => Some(error),
            Self::EmptyDecodedPayload
            | Self::DeclaredLengthMismatch { .. }
            | Self::StoredDecodedLengthMismatch { .. }
            | Self::DecodedLengthMismatch { .. }
            | Self::EmptyDictionary
            | Self::DictionaryHashMismatch { .. }
            | Self::MissingDictionary(_)
            | Self::UnexpectedDictionary
            | Self::WrongDictionary { .. }
            | Self::SizeOverflow
            | Self::CostOverflow => None,
        }
    }
}
