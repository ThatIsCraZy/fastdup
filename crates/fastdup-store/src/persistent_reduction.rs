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
    ChunkId, DependentCodec, IncompressibilityGatePolicy, PrehashedChunk, PreparedDependentRecord,
    PreparedIndependentRecord, SealedContainer,
};

use crate::exact_index_repository::{ExactIndexGenerationPin, ExactIndexGenerationSnapshot};
use crate::reduction_prefix::{BaseChunkRef, VerifiedBaseChunk, ZstdPrefixCodec};
use crate::reduction_similarity::{IndependentBaseRef, SparseXorDelta};
use crate::similarity_index_repository::{RecoveredSimilarityIndex, SimilarityIndexStoreError};
use crate::{ContainerRepository, SimilarityIndexPageCacheStatus, StorageIo};

const MAXIMUM_DEPENDENT_TRIALS_V1: usize = 4;
const DEPENDENT_MINIMUM_SAVINGS_BYTES_V1: usize = 4_096;
const DEPENDENT_MINIMUM_SAVINGS_PERCENT_V1: usize = 5;
const REDUCTION_COUNTER_STRIPES: usize = 64;
const _: () = assert!(REDUCTION_COUNTER_STRIPES.is_power_of_two());

#[repr(C, align(64))]
#[derive(Debug, Default)]
struct ReductionCounterStripe {
    queries: AtomicU64,
    candidates: AtomicU64,
    base_reads: AtomicU64,
    base_read_bytes: AtomicU64,
    prefix_trials: AtomicU64,
    sparse_xor_trials: AtomicU64,
    accepted_prefixes: AtomicU64,
    accepted_sparse_xor: AtomicU64,
    independent_fallbacks: AtomicU64,
    no_candidate_fallbacks: AtomicU64,
    saved_payload_bytes: AtomicU64,
    errors: AtomicU64,
}

#[derive(Debug)]
struct ReductionCounters {
    stripes: Box<[ReductionCounterStripe]>,
}

impl ReductionCounters {
    fn new() -> Self {
        let stripes = (0..REDUCTION_COUNTER_STRIPES)
            .map(|_| ReductionCounterStripe::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { stripes }
    }

    fn for_chunk(&self, chunk_id: ChunkId) -> &ReductionCounterStripe {
        &self.stripes[reduction_counter_stripe(chunk_id)]
    }

    fn snapshot(&self) -> ReductionCounterValues {
        self.stripes
            .iter()
            .fold(ReductionCounterValues::default(), |mut total, stripe| {
                total.add_assign(ReductionCounterValues {
                    queries: stripe.queries.load(Ordering::Relaxed),
                    candidates: stripe.candidates.load(Ordering::Relaxed),
                    base_reads: stripe.base_reads.load(Ordering::Relaxed),
                    base_read_bytes: stripe.base_read_bytes.load(Ordering::Relaxed),
                    prefix_trials: stripe.prefix_trials.load(Ordering::Relaxed),
                    sparse_xor_trials: stripe.sparse_xor_trials.load(Ordering::Relaxed),
                    accepted_prefixes: stripe.accepted_prefixes.load(Ordering::Relaxed),
                    accepted_sparse_xor: stripe.accepted_sparse_xor.load(Ordering::Relaxed),
                    independent_fallbacks: stripe.independent_fallbacks.load(Ordering::Relaxed),
                    no_candidate_fallbacks: stripe.no_candidate_fallbacks.load(Ordering::Relaxed),
                    saved_payload_bytes: stripe.saved_payload_bytes.load(Ordering::Relaxed),
                    errors: stripe.errors.load(Ordering::Relaxed),
                });
                total
            })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ReductionCounterValues {
    queries: u64,
    candidates: u64,
    base_reads: u64,
    base_read_bytes: u64,
    prefix_trials: u64,
    sparse_xor_trials: u64,
    accepted_prefixes: u64,
    accepted_sparse_xor: u64,
    independent_fallbacks: u64,
    no_candidate_fallbacks: u64,
    saved_payload_bytes: u64,
    errors: u64,
}

impl ReductionCounterValues {
    fn add_assign(&mut self, other: Self) {
        self.queries = self.queries.saturating_add(other.queries);
        self.candidates = self.candidates.saturating_add(other.candidates);
        self.base_reads = self.base_reads.saturating_add(other.base_reads);
        self.base_read_bytes = self.base_read_bytes.saturating_add(other.base_read_bytes);
        self.prefix_trials = self.prefix_trials.saturating_add(other.prefix_trials);
        self.sparse_xor_trials = self
            .sparse_xor_trials
            .saturating_add(other.sparse_xor_trials);
        self.accepted_prefixes = self
            .accepted_prefixes
            .saturating_add(other.accepted_prefixes);
        self.accepted_sparse_xor = self
            .accepted_sparse_xor
            .saturating_add(other.accepted_sparse_xor);
        self.independent_fallbacks = self
            .independent_fallbacks
            .saturating_add(other.independent_fallbacks);
        self.no_candidate_fallbacks = self
            .no_candidate_fallbacks
            .saturating_add(other.no_candidate_fallbacks);
        self.saved_payload_bytes = self
            .saved_payload_bytes
            .saturating_add(other.saved_payload_bytes);
        self.errors = self.errors.saturating_add(other.errors);
    }
}

/// One immutable, coherently bound Exact/Similarity pair for write-through.
pub struct PersistentReductionIndex<I> {
    exact: ExactIndexGenerationSnapshot<I>,
    similarity: Arc<RecoveredSimilarityIndex<I>>,
    counters: ReductionCounters,
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
            counters: ReductionCounters::new(),
        })
    }

    /// Returns lock-free, payload-free advanced-reduction telemetry.
    #[must_use]
    pub fn status(&self) -> PersistentReductionStatus {
        let counters = self.counters.snapshot();
        PersistentReductionStatus {
            enabled: true,
            queries: counters.queries,
            candidates: counters.candidates,
            base_reads: counters.base_reads,
            base_read_bytes: counters.base_read_bytes,
            prefix_trials: counters.prefix_trials,
            sparse_xor_trials: counters.sparse_xor_trials,
            accepted_prefixes: counters.accepted_prefixes,
            accepted_sparse_xor: counters.accepted_sparse_xor,
            independent_fallbacks: counters.independent_fallbacks,
            no_candidate_fallbacks: counters.no_candidate_fallbacks,
            saved_payload_bytes: counters.saved_payload_bytes,
            errors: counters.errors,
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
        let counters = self.counters.for_chunk(target_id);
        counters.queries.fetch_add(1, Ordering::Relaxed);
        match self.plan_chunk_inner(containers, target_id, target, counters) {
            Ok(plan) => Ok(plan),
            Err(error) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn plan_chunk_inner<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
        counters: &ReductionCounterStripe,
    ) -> Result<PersistentChunkPlan, PersistentReductionError> {
        let Some(exact) = self.exact.try_pin() else {
            counters
                .no_candidate_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::NoCandidates);
        };
        let candidates = self.similarity.candidates_prehashed(target_id, target)?;
        counters.candidates.fetch_add(
            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if candidates.is_empty() {
            counters
                .no_candidate_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::NoCandidates);
        }
        let independent = SealedContainer::prepare_prehashed_independent_record(
            PrehashedChunk::new(target_id, target),
            IncompressibilityGatePolicy::Off,
        )
        .map_err(|_| PersistentReductionError::IndependentCodec)?;
        let maximum_encoded_payload_bytes = independent.encoded_payload_bytes();
        let mut best: Option<(usize, PreparedDependentRecord)> = None;
        let mut remaining_trials = MAXIMUM_DEPENDENT_TRIALS_V1;
        for candidate in candidates {
            if remaining_trials == 0 {
                break;
            }
            counters.base_reads.fetch_add(1, Ordering::Relaxed);
            let Some(base_bytes) = containers.find_verified_independent_base_with_index(
                &exact,
                candidate.chunk_id(),
                candidate.logical_length(),
            ) else {
                continue;
            };
            counters.base_read_bytes.fetch_add(
                u64::try_from(base_bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let expected = BaseChunkRef::new(candidate.chunk_id(), candidate.logical_length());
            let base = VerifiedBaseChunk::from_verified_location(expected, &base_bytes)
                .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
            let sparse_base = IndependentBaseRef::from_verified_identity(
                candidate.chunk_id(),
                candidate.logical_length(),
                &base_bytes,
            )
            .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
            counters.sparse_xor_trials.fetch_add(1, Ordering::Relaxed);
            remaining_trials -= 1;
            let sparse =
                SparseXorDelta::encode_prehashed_trial(sparse_base, &base_bytes, target_id, target)
                    .map_err(|_| PersistentReductionError::SparseXorCodec)?;
            if sparse.cost().run_count() != 0 {
                let sparse_bytes = usize::try_from(sparse.cost().encoded_payload_bytes())
                    .map_err(|_| PersistentReductionError::SparseXorCodec)?;
                let prepared = sparse
                    .into_encoding()
                    .into_prepared_record()
                    .map(PreparedDependentRecord::from)
                    .map_err(|_| PersistentReductionError::SparseXorCodec)?;
                if best.as_ref().is_none_or(|(bytes, _)| sparse_bytes < *bytes) {
                    best = Some((sparse_bytes, prepared));
                }
            }

            if remaining_trials != 0 {
                counters.prefix_trials.fetch_add(1, Ordering::Relaxed);
                remaining_trials -= 1;
                if let Some(trial) = ZstdPrefixCodec::encode_prehashed_trial(
                    base,
                    target_id,
                    target,
                    maximum_encoded_payload_bytes,
                )
                .map_err(|_| PersistentReductionError::PrefixCodec)?
                {
                    let prefix_bytes = usize::try_from(trial.encoded_payload_bytes())
                        .map_err(|_| PersistentReductionError::PrefixCodec)?;
                    let prepared = trial
                        .into_encoding()
                        .into_prepared_record()
                        .map(PreparedDependentRecord::from)
                        .map_err(|_| PersistentReductionError::PrefixCodec)?;
                    if best.as_ref().is_none_or(|(bytes, _)| prefix_bytes < *bytes) {
                        best = Some((prefix_bytes, prepared));
                    }
                }
            }
        }
        let Some((best_bytes, best)) = best else {
            counters
                .independent_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::Independent(independent));
        };
        if !accept_dependent_v1(independent.encoded_payload_bytes(), best_bytes) {
            counters
                .independent_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PersistentChunkPlan::Independent(independent));
        }
        let saved_payload_bytes = independent
            .encoded_payload_bytes()
            .saturating_sub(best_bytes);
        assert_eq!(
            best.target_id(),
            target_id,
            "ASSERT: accepted dependent trial retains the prehashed target identity"
        );
        match best.codec() {
            DependentCodec::ZstdPrefix => {
                counters.accepted_prefixes.fetch_add(1, Ordering::Relaxed);
            }
            DependentCodec::SparseXor => {
                counters.accepted_sparse_xor.fetch_add(1, Ordering::Relaxed);
            }
        }
        counters.saved_payload_bytes.fetch_add(
            u64::try_from(saved_payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(PersistentChunkPlan::Dependent(best))
    }
}

fn reduction_counter_stripe(chunk_id: ChunkId) -> usize {
    let bytes = chunk_id.bytes();
    let first = u64::from_le_bytes(bytes[..8].try_into().expect("ASSERT: exact hash slice"));
    let second = u64::from_le_bytes(bytes[8..16].try_into().expect("ASSERT: exact hash slice"));
    let mixed = first ^ second.rotate_left(23);
    usize::try_from(mixed).expect("ASSERT: x86-64 usize holds a u64")
        & (REDUCTION_COUNTER_STRIPES - 1)
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
    sparse_xor_trials: u64,
    accepted_prefixes: u64,
    accepted_sparse_xor: u64,
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
    reduction_status_getter!(sparse_xor_trials, sparse_xor_trials, u64);
    reduction_status_getter!(accepted_prefixes, accepted_prefixes, u64);
    reduction_status_getter!(accepted_sparse_xor, accepted_sparse_xor, u64);
    reduction_status_getter!(independent_fallbacks, independent_fallbacks, u64);
    reduction_status_getter!(no_candidate_fallbacks, no_candidate_fallbacks, u64);
    reduction_status_getter!(saved_payload_bytes, saved_payload_bytes, u64);
    reduction_status_getter!(errors, errors, u64);
}

pub enum PersistentChunkPlan {
    NoCandidates,
    Independent(PreparedIndependentRecord),
    Dependent(PreparedDependentRecord),
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
    SparseXorCodec,
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
            | Self::PrefixCodec
            | Self::SparseXorCodec => None,
        }
    }
}

impl From<SimilarityIndexStoreError> for PersistentReductionError {
    fn from(error: SimilarityIndexStoreError) -> Self {
        Self::Similarity(error)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn telemetry_stripes_are_cache_line_separated_and_sum_on_the_cold_path() {
        let counters = ReductionCounters::new();
        assert_eq!(align_of::<ReductionCounterStripe>(), 64);
        assert_eq!(size_of::<ReductionCounterStripe>() % 64, 0);
        assert_eq!(counters.stripes.as_ptr().addr() % 64, 0);

        let first_id = ChunkId::of(b"first reduction telemetry stripe");
        let first_ordinal = reduction_counter_stripe(first_id);
        let second_id = (0_u64..10_000)
            .map(|nonce| ChunkId::of(&nonce.to_le_bytes()))
            .find(|chunk_id| reduction_counter_stripe(*chunk_id) != first_ordinal)
            .expect("fixture search finds another telemetry stripe");
        let first = counters.for_chunk(first_id);
        let second = counters.for_chunk(second_id);

        first.queries.fetch_add(3, Ordering::Relaxed);
        first.candidates.fetch_add(7, Ordering::Relaxed);
        second.queries.fetch_add(5, Ordering::Relaxed);
        second.errors.fetch_add(2, Ordering::Relaxed);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.queries, 8);
        assert_eq!(snapshot.candidates, 7);
        assert_eq!(snapshot.errors, 2);
    }
}
