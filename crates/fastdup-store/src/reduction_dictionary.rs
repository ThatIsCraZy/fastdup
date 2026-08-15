//! Immutable, content-identified Zstd dictionary objects.

use std::fmt;
use std::io;
use std::sync::Arc;

use fastdup_format::ChunkId;

use crate::reduction_codec::{CodecError, DictionaryId, PreparedDictionary};

const MINIMUM_V1_TRAINED_DICTIONARY_BYTES: usize = 256;
const MAXIMUM_V1_DICTIONARY_BYTES: usize = 1_024 * 1_024;
const MAXIMUM_V1_TRAINING_BYTES: usize = 64 * 1_024 * 1_024;

/// Immutable exact dictionary bytes identified by BLAKE3-256.
///
/// The identity covers every byte. Training or replacing a dictionary creates
/// a new object; a decoder may never substitute another object merely because
/// its contents appear similar.
#[derive(Clone, Eq, PartialEq)]
pub struct ReductionDictionary {
    id: [u8; 32],
    bytes: Arc<[u8]>,
}

impl ReductionDictionary {
    /// Copies and content-identifies exact dictionary bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty object or for bytes exceeding the bounded
    /// v1 dictionary-object size.
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, ReductionDictionaryError> {
        let bytes = bytes.as_ref();
        validate_dictionary_length(bytes.len())?;
        Ok(Self {
            id: ChunkId::of(bytes).bytes(),
            bytes: Arc::from(bytes),
        })
    }

    /// Constructs an object only when `expected_id` identifies the exact bytes.
    ///
    /// # Errors
    ///
    /// Returns a normal validation error for empty/oversized input or when the
    /// BLAKE3-256 identity disagrees with `expected_id`.
    pub fn from_verified_bytes(
        expected_id: [u8; 32],
        bytes: impl AsRef<[u8]>,
    ) -> Result<Self, ReductionDictionaryError> {
        let dictionary = Self::from_bytes(bytes)?;
        if dictionary.id != expected_id {
            return Err(ReductionDictionaryError::HashMismatch {
                expected: expected_id,
                actual: dictionary.id,
            });
        }
        Ok(dictionary)
    }

    /// Deterministically trains a version-one Zstd dictionary.
    ///
    /// Sample order and exact sample bytes are significant. Repeating this call
    /// with the same ordered inputs, maximum size, Zstd build, and fastdup
    /// version produces identical bytes and therefore the same content ID.
    /// Training is a cold path; the complete bounded sample set is copied by
    /// the Zstd trainer.
    ///
    /// # Errors
    ///
    /// Returns validation errors for an empty sample set, empty samples,
    /// arithmetic overflow, an unsafe aggregate input size, or a maximum size
    /// outside the v1 range. Zstd training failures are returned as ordinary
    /// [`ReductionDictionaryError::Training`] errors.
    pub fn train_v1<S: AsRef<[u8]>>(
        samples: &[S],
        max_size: usize,
    ) -> Result<Self, ReductionDictionaryError> {
        if !(MINIMUM_V1_TRAINED_DICTIONARY_BYTES..=MAXIMUM_V1_DICTIONARY_BYTES).contains(&max_size)
        {
            return Err(ReductionDictionaryError::InvalidTrainingMaximum {
                requested: max_size,
                minimum: MINIMUM_V1_TRAINED_DICTIONARY_BYTES,
                maximum: MAXIMUM_V1_DICTIONARY_BYTES,
            });
        }
        if samples.is_empty() {
            return Err(ReductionDictionaryError::EmptyTrainingSet);
        }
        let mut total_bytes = 0_usize;
        for (index, sample) in samples.iter().enumerate() {
            let length = sample.as_ref().len();
            if length == 0 {
                return Err(ReductionDictionaryError::EmptyTrainingSample(index));
            }
            total_bytes = total_bytes
                .checked_add(length)
                .ok_or(ReductionDictionaryError::TrainingSizeOverflow)?;
            if total_bytes > MAXIMUM_V1_TRAINING_BYTES {
                return Err(ReductionDictionaryError::TrainingInputTooLarge {
                    length: total_bytes,
                    maximum: MAXIMUM_V1_TRAINING_BYTES,
                });
            }
        }

        let bytes = zstd::dict::from_samples(samples, max_size)
            .map_err(ReductionDictionaryError::Training)?;
        Self::from_bytes(bytes)
    }

    #[must_use]
    pub const fn id(&self) -> [u8; 32] {
        self.id
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `false`; construction rejects empty dictionary objects.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Re-verifies an immutable object while creating the internal codec-side form.
impl TryFrom<&ReductionDictionary> for PreparedDictionary {
    type Error = CodecError;

    fn try_from(dictionary: &ReductionDictionary) -> Result<Self, Self::Error> {
        Self::new(
            DictionaryId::new(dictionary.id),
            Arc::clone(&dictionary.bytes),
        )
    }
}

impl fmt::Debug for ReductionDictionary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReductionDictionary")
            .field("id", &self.id)
            .field("length", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Expected dictionary validation and training failures.
#[derive(Debug)]
pub enum ReductionDictionaryError {
    EmptyBytes,
    DictionaryTooLarge {
        length: usize,
        maximum: usize,
    },
    HashMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    InvalidTrainingMaximum {
        requested: usize,
        minimum: usize,
        maximum: usize,
    },
    EmptyTrainingSet,
    EmptyTrainingSample(usize),
    TrainingSizeOverflow,
    TrainingInputTooLarge {
        length: usize,
        maximum: usize,
    },
    Training(io::Error),
}

impl fmt::Display for ReductionDictionaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBytes => formatter.write_str("dictionary bytes are empty"),
            Self::DictionaryTooLarge { length, maximum } => write!(
                formatter,
                "dictionary length {length} exceeds the v1 maximum {maximum}"
            ),
            Self::HashMismatch { expected, actual } => write!(
                formatter,
                "dictionary content ID {actual:?} disagrees with expected ID {expected:?}"
            ),
            Self::InvalidTrainingMaximum {
                requested,
                minimum,
                maximum,
            } => write!(
                formatter,
                "training maximum {requested} lies outside {minimum}..={maximum}"
            ),
            Self::EmptyTrainingSet => formatter.write_str("dictionary training set is empty"),
            Self::EmptyTrainingSample(index) => {
                write!(formatter, "dictionary training sample {index} is empty")
            }
            Self::TrainingSizeOverflow => {
                formatter.write_str("dictionary training byte count overflowed usize")
            }
            Self::TrainingInputTooLarge { length, maximum } => write!(
                formatter,
                "dictionary training input length {length} exceeds the v1 maximum {maximum}"
            ),
            Self::Training(error) => write!(formatter, "Zstd dictionary training failed: {error}"),
        }
    }
}

impl std::error::Error for ReductionDictionaryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Training(error) => Some(error),
            Self::EmptyBytes
            | Self::DictionaryTooLarge { .. }
            | Self::HashMismatch { .. }
            | Self::InvalidTrainingMaximum { .. }
            | Self::EmptyTrainingSet
            | Self::EmptyTrainingSample(_)
            | Self::TrainingSizeOverflow
            | Self::TrainingInputTooLarge { .. } => None,
        }
    }
}

fn validate_dictionary_length(length: usize) -> Result<(), ReductionDictionaryError> {
    if length == 0 {
        return Err(ReductionDictionaryError::EmptyBytes);
    }
    if length > MAXIMUM_V1_DICTIONARY_BYTES {
        return Err(ReductionDictionaryError::DictionaryTooLarge {
            length,
            maximum: MAXIMUM_V1_DICTIONARY_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ReductionDictionary;
    use crate::reduction_codec::{IndependentEncoding, WorkerCodec};

    #[test]
    fn prepared_dictionary_round_trips_through_one_worker_context() {
        let dictionary = ReductionDictionary::from_bytes(
            b"rocky-generation-path-state-clean-virtual-machine-backup",
        )
        .expect("fixture dictionary is valid");
        let prepared = crate::reduction_codec::PreparedDictionary::try_from(&dictionary)
            .expect("bridge re-verifies bytes");
        let input = b"rocky-generation-path-state-clean-virtual-machine-backup"
            .iter()
            .copied()
            .cycle()
            .take(128 * 1_024)
            .collect::<Vec<_>>();
        let mut codec = WorkerCodec::new().expect("worker codec initializes");
        let decision = codec
            .encode_v1(&input, input.len(), 3, Some(&prepared))
            .expect("dictionary encoding succeeds");
        assert!(matches!(
            decision.encoding(),
            IndependentEncoding::Zstd { .. }
        ));
        assert_eq!(
            decision
                .encoding()
                .dictionary_id()
                .map(crate::reduction_codec::DictionaryId::bytes),
            Some(dictionary.id())
        );
        assert_eq!(
            codec
                .decode(decision.encoding(), input.len(), Some(&prepared))
                .expect("the exact dictionary decodes"),
            input
        );

        let other = ReductionDictionary::from_bytes(b"different dictionary bytes")
            .expect("other fixture dictionary is valid");
        let other = crate::reduction_codec::PreparedDictionary::try_from(&other)
            .expect("other dictionary prepares");
        assert!(
            codec
                .decode(decision.encoding(), input.len(), Some(&other))
                .is_err()
        );
    }
}
