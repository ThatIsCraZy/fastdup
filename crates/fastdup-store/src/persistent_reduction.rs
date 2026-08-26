//! Pool-wide write-through candidate resolution.
//!
//! One immutable Similarity snapshot stays paired with the exact immutable
//! Exact Run Set that can resolve all of its candidates. Newer Exact L0
//! activations may proceed independently; this pinned pair remains coherent
//! for the mount lifetime and is replaced only by a later paired recovery.

use std::fmt;
use std::sync::Arc;

use fastdup_format::{
    ChunkId, IncompressibilityGatePolicy, PrehashedChunk, PreparedIndependentRecord,
    PreparedZstdPrefixRecord, SealedContainer,
};

use crate::exact_index_repository::{ExactIndexGenerationPin, ExactIndexGenerationSnapshot};
use crate::reduction_prefix::{BaseChunkRef, VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixTrial};
use crate::similarity_index_repository::{RecoveredSimilarityIndex, SimilarityIndexStoreError};
use crate::{ContainerRepository, StorageIo};

const MAXIMUM_PREFIX_TRIALS_V1: usize = 4;
const DEPENDENT_MINIMUM_SAVINGS_BYTES_V1: usize = 4_096;
const DEPENDENT_MINIMUM_SAVINGS_PERCENT_V1: usize = 5;

/// One immutable, coherently bound Exact/Similarity pair for write-through.
pub struct PersistentReductionIndex<I> {
    exact: ExactIndexGenerationSnapshot<I>,
    similarity: Arc<RecoveredSimilarityIndex<I>>,
}

impl<I: Clone + StorageIo> fmt::Debug for PersistentReductionIndex<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentReductionIndex")
            .field("exact_activation", &self.exact.activation())
            .field("similarity", &self.similarity.status())
            .finish()
    }
}

impl<I: Clone + StorageIo> PersistentReductionIndex<I> {
    /// Binds a recovered Similarity snapshot to its exact resolver.
    ///
    /// # Errors
    ///
    /// Returns a binding error when the family does not name this Exact Run
    /// Set or either durable identity is invalid.
    pub fn new(
        exact: &ExactIndexGenerationPin<I>,
        similarity: Arc<RecoveredSimilarityIndex<I>>,
    ) -> Result<Self, PersistentReductionError> {
        let exact_id = exact
            .run_set()
            .id()
            .map_err(|_| PersistentReductionError::IndexBindingMismatch)?;
        if similarity.status().source_exact_run_set_id() != Some(exact_id) {
            return Err(PersistentReductionError::IndexBindingMismatch);
        }
        Ok(Self {
            exact: exact.snapshot(),
            similarity,
        })
    }

    /// Plans one candidate Chunk without repeated target hashing or encoding.
    ///
    /// Target identity and Base identity are reused from prior verified work;
    /// the hot path does not repeat either BLAKE3 hash. Candidate I/O remains
    /// bounded, and unusable or stale Exact locations are skipped.
    ///
    /// # Errors
    ///
    /// Returns candidate-index corruption, Base verification, allocation, or
    /// codec failures. An ordinary miss or rejected Prefix is not an error.
    ///
    /// # Panics
    ///
    /// Panics if the prepared durable Prefix loses the prehashed target
    /// identity retained by its accepted trial.
    pub fn plan_chunk<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> Result<PersistentChunkPlan, PersistentReductionError> {
        let Some(exact) = self.exact.try_pin() else {
            return Ok(PersistentChunkPlan::NoCandidates);
        };
        let candidates = self.similarity.candidates_prehashed(target_id, target)?;
        if candidates.is_empty() {
            return Ok(PersistentChunkPlan::NoCandidates);
        }
        let independent = SealedContainer::prepare_prehashed_independent_record(
            PrehashedChunk::new(target_id, target),
            IncompressibilityGatePolicy::Off,
        )
        .map_err(|_| PersistentReductionError::IndependentCodec)?;
        let maximum_encoded_payload_bytes = independent.encoded_payload_bytes();
        let mut best: Option<ZstdPrefixTrial> = None;
        for candidate in candidates.into_iter().take(MAXIMUM_PREFIX_TRIALS_V1) {
            let Some(base_bytes) = containers.find_verified_independent_base_with_index(
                &exact,
                candidate.chunk_id(),
                candidate.logical_length(),
            ) else {
                continue;
            };
            let expected = BaseChunkRef::new(candidate.chunk_id(), candidate.logical_length());
            let base = VerifiedBaseChunk::from_verified_location(expected, &base_bytes)
                .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
            let Some(trial) = ZstdPrefixCodec::encode_prehashed_trial(
                base,
                target_id,
                target,
                maximum_encoded_payload_bytes,
            )
            .map_err(|_| PersistentReductionError::PrefixCodec)?
            else {
                continue;
            };
            if best.as_ref().is_none_or(|current| {
                trial.encoded_payload_bytes() < current.encoded_payload_bytes()
            }) {
                best = Some(trial);
            }
        }
        let Some(best) = best else {
            return Ok(PersistentChunkPlan::Independent(independent));
        };
        if !accept_dependent_v1(
            independent.encoded_payload_bytes(),
            usize::try_from(best.encoded_payload_bytes())
                .map_err(|_| PersistentReductionError::PrefixCodec)?,
        ) {
            return Ok(PersistentChunkPlan::Independent(independent));
        }
        let prefix = best
            .into_encoding()
            .into_prepared_record()
            .map_err(|_| PersistentReductionError::PrefixCodec)?;
        assert_eq!(
            prefix.target_id(),
            target_id,
            "ASSERT: accepted Prefix trial retains the prehashed target identity"
        );
        Ok(PersistentChunkPlan::ZstdPrefix(prefix))
    }
}

pub enum PersistentChunkPlan {
    NoCandidates,
    Independent(PreparedIndependentRecord),
    ZstdPrefix(PreparedZstdPrefixRecord),
}

fn accept_dependent_v1(independent_bytes: usize, dependent_bytes: usize) -> bool {
    let Some(savings) = independent_bytes.checked_sub(dependent_bytes) else {
        return false;
    };
    savings >= DEPENDENT_MINIMUM_SAVINGS_BYTES_V1
        && savings.saturating_mul(100)
            >= independent_bytes.saturating_mul(DEPENDENT_MINIMUM_SAVINGS_PERCENT_V1)
}

#[derive(Debug)]
pub enum PersistentReductionError {
    Similarity(SimilarityIndexStoreError),
    IndexBindingMismatch,
    VerifiedBaseMismatch,
    IndependentCodec,
    PrefixCodec,
}

impl fmt::Display for PersistentReductionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PersistentReductionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Similarity(error) => Some(error),
            Self::IndexBindingMismatch
            | Self::VerifiedBaseMismatch
            | Self::IndependentCodec
            | Self::PrefixCodec => None,
        }
    }
}

impl From<SimilarityIndexStoreError> for PersistentReductionError {
    fn from(error: SimilarityIndexStoreError) -> Self {
        Self::Similarity(error)
    }
}
