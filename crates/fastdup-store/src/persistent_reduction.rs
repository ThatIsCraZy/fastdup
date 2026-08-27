//! Pool-wide write-through candidate resolution.
//!
//! One immutable Similarity snapshot stays paired with the exact immutable
//! Exact Run Set that can resolve all of its candidates. Newer Exact L0
//! activations may proceed independently; this pinned pair remains coherent
//! for the mount lifetime and is replaced only by a later paired recovery.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fastdup_format::{
    ChunkId, IncompressibilityGatePolicy, PrehashedChunk, PreparedIndependentRecord,
    PreparedZstdPrefixRecord, SealedContainer,
};

use crate::exact_index_repository::{ExactIndexGenerationPin, ExactIndexGenerationSnapshot};
use crate::reduction_prefix::{BaseChunkRef, VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixTrial};
use crate::similarity_index_repository::{RecoveredSimilarityIndex, SimilarityIndexStoreError};
use crate::{ContainerRepository, SimilarityIndexPageCacheStatus, StorageIo};

const MAXIMUM_PREFIX_TRIALS_V1: usize = 4;
const DEPENDENT_MINIMUM_SAVINGS_BYTES_V1: usize = 4_096;
const DEPENDENT_MINIMUM_SAVINGS_PERCENT_V1: usize = 5;

/// One immutable, coherently bound Exact/Similarity pair for write-through.
pub struct PersistentReductionIndex<I> {
    exact: ExactIndexGenerationSnapshot<I>,
    similarity: Arc<RecoveredSimilarityIndex<I>>,
    queries: AtomicU64,
    candidates: AtomicU64,
    base_reads: AtomicU64,
    base_read_bytes: AtomicU64,
    prefix_trials: AtomicU64,
    accepted_prefixes: AtomicU64,
    independent_fallbacks: AtomicU64,
    no_candidate_fallbacks: AtomicU64,
    saved_payload_bytes: AtomicU64,
    errors: AtomicU64,
}

impl<I: Clone + StorageIo> fmt::Debug for PersistentReductionIndex<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentReductionIndex")
            .field("exact_activation", &self.exact.activation())
            .field("similarity", &self.similarity.status())
            .finish_non_exhaustive()
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
            queries: AtomicU64::new(0),
            candidates: AtomicU64::new(0),
            base_reads: AtomicU64::new(0),
            base_read_bytes: AtomicU64::new(0),
            prefix_trials: AtomicU64::new(0),
            accepted_prefixes: AtomicU64::new(0),
            independent_fallbacks: AtomicU64::new(0),
            no_candidate_fallbacks: AtomicU64::new(0),
            saved_payload_bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    /// Returns lock-free, payload-free advanced-reduction telemetry.
    #[must_use]
    pub fn status(&self) -> PersistentReductionStatus {
        PersistentReductionStatus {
            enabled: true,
            queries: self.queries.load(Ordering::Relaxed),
            candidates: self.candidates.load(Ordering::Relaxed),
            base_reads: self.base_reads.load(Ordering::Relaxed),
            base_read_bytes: self.base_read_bytes.load(Ordering::Relaxed),
            prefix_trials: self.prefix_trials.load(Ordering::Relaxed),
            accepted_prefixes: self.accepted_prefixes.load(Ordering::Relaxed),
            independent_fallbacks: self.independent_fallbacks.load(Ordering::Relaxed),
            no_candidate_fallbacks: self.no_candidate_fallbacks.load(Ordering::Relaxed),
            saved_payload_bytes: self.saved_payload_bytes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        self.similarity.page_cache_status()
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
        self.queries.fetch_add(1, Ordering::Relaxed);
        match self.plan_chunk_inner(containers, target_id, target) {
            Ok(plan) => Ok(plan),
            Err(error) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    fn plan_chunk_inner<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> Result<PersistentChunkPlan, PersistentReductionError> {
        let Some(exact) = self.exact.try_pin() else {
            self.no_candidate_fallbacks.fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::NoCandidates);
        };
        let candidates = self.similarity.candidates_prehashed(target_id, target)?;
        self.candidates.fetch_add(
            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if candidates.is_empty() {
            self.no_candidate_fallbacks.fetch_add(1, Ordering::Relaxed);
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
            self.base_reads.fetch_add(1, Ordering::Relaxed);
            let Some(base_bytes) = containers.find_verified_independent_base_with_index(
                &exact,
                candidate.chunk_id(),
                candidate.logical_length(),
            ) else {
                continue;
            };
            self.base_read_bytes.fetch_add(
                u64::try_from(base_bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let expected = BaseChunkRef::new(candidate.chunk_id(), candidate.logical_length());
            let base = VerifiedBaseChunk::from_verified_location(expected, &base_bytes)
                .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
            self.prefix_trials.fetch_add(1, Ordering::Relaxed);
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
            self.independent_fallbacks.fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::Independent(independent));
        };
        if !accept_dependent_v1(
            independent.encoded_payload_bytes(),
            usize::try_from(best.encoded_payload_bytes())
                .map_err(|_| PersistentReductionError::PrefixCodec)?,
        ) {
            self.independent_fallbacks.fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::Independent(independent));
        }
        let saved_payload_bytes = independent
            .encoded_payload_bytes()
            .saturating_sub(usize::try_from(best.encoded_payload_bytes()).unwrap_or(usize::MAX));
        let prefix = best
            .into_encoding()
            .into_prepared_record()
            .map_err(|_| PersistentReductionError::PrefixCodec)?;
        assert_eq!(
            prefix.target_id(),
            target_id,
            "ASSERT: accepted Prefix trial retains the prehashed target identity"
        );
        self.accepted_prefixes.fetch_add(1, Ordering::Relaxed);
        self.saved_payload_bytes.fetch_add(
            u64::try_from(saved_payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(PersistentChunkPlan::ZstdPrefix(prefix))
    }
}

/// Lock-free aggregate evidence for one mount-pinned reduction snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PersistentReductionStatus {
    enabled: bool,
    queries: u64,
    candidates: u64,
    base_reads: u64,
    base_read_bytes: u64,
    prefix_trials: u64,
    accepted_prefixes: u64,
    independent_fallbacks: u64,
    no_candidate_fallbacks: u64,
    saved_payload_bytes: u64,
    errors: u64,
}

macro_rules! reduction_status_getter {
    ($name:ident, $field:ident, $type:ty) => {
        #[must_use]
        pub const fn $name(self) -> $type {
            self.$field
        }
    };
}

impl PersistentReductionStatus {
    reduction_status_getter!(enabled, enabled, bool);
    reduction_status_getter!(queries, queries, u64);
    reduction_status_getter!(candidates, candidates, u64);
    reduction_status_getter!(base_reads, base_reads, u64);
    reduction_status_getter!(base_read_bytes, base_read_bytes, u64);
    reduction_status_getter!(prefix_trials, prefix_trials, u64);
    reduction_status_getter!(accepted_prefixes, accepted_prefixes, u64);
    reduction_status_getter!(independent_fallbacks, independent_fallbacks, u64);
    reduction_status_getter!(no_candidate_fallbacks, no_candidate_fallbacks, u64);
    reduction_status_getter!(saved_payload_bytes, saved_payload_bytes, u64);
    reduction_status_getter!(errors, errors, u64);
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
