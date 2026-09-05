//! Pool-wide write-through candidate resolution.
//!
//! Online queries retain immutable Similarity runs and pin current Exact for
//! candidate resolution. Missing/stale hints lose only an optimization; every
//! chosen base is resolved and verified through the ordinary DATA read path.
//! The frozen-pair constructor remains available for offline callers.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fastdup_format::{
    ChunkId, DependentCodec, IncompressibilityGatePolicy, PrehashedChunk, PreparedDependentRecord,
    PreparedIndependentRecord, SealedContainer,
};

use crate::exact_index_repository::{ExactIndexGenerationPin, ExactIndexGenerationSnapshot};
use crate::online_similarity::OnlineSimilarityRepository;
use crate::reduction_prefix::{BaseChunkRef, VerifiedBaseChunk, ZstdPrefixCodec};
use crate::reduction_similarity::SimilarityFingerprint;
use crate::reduction_similarity::{IndependentBaseRef, SparseXorDelta};
use crate::similarity_index_repository::{
    RecoveredSimilarityIndex, SimilarityBaseCandidate, SimilarityIndexStoreError,
};
use crate::{ContainerRepository, SimilarityIndexPageCacheStatus, StorageIo, VerifiedReadCache};
use fastdup_format::SimilarityIndexEntry;

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
    fingerprint_ns: AtomicU64,
    candidate_lookup_ns: AtomicU64,
    base_read_ns: AtomicU64,
    codec_trial_ns: AtomicU64,
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
                    fingerprint_ns: stripe.fingerprint_ns.load(Ordering::Relaxed),
                    candidate_lookup_ns: stripe.candidate_lookup_ns.load(Ordering::Relaxed),
                    base_read_ns: stripe.base_read_ns.load(Ordering::Relaxed),
                    codec_trial_ns: stripe.codec_trial_ns.load(Ordering::Relaxed),
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
    fingerprint_ns: u64,
    candidate_lookup_ns: u64,
    base_read_ns: u64,
    codec_trial_ns: u64,
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
        self.fingerprint_ns = self.fingerprint_ns.saturating_add(other.fingerprint_ns);
        self.candidate_lookup_ns = self
            .candidate_lookup_ns
            .saturating_add(other.candidate_lookup_ns);
        self.base_read_ns = self.base_read_ns.saturating_add(other.base_read_ns);
        self.codec_trial_ns = self.codec_trial_ns.saturating_add(other.codec_trial_ns);
    }
}

/// Bounded write-through planning over a frozen pair or online Similarity views.
pub struct PersistentReductionIndex<I> {
    source: ReductionSource<I>,
    counters: ReductionCounters,
}

enum ReductionSource<I> {
    Frozen {
        exact: ExactIndexGenerationSnapshot<I>,
        similarity: Arc<RecoveredSimilarityIndex<I>>,
    },
    Online(Arc<OnlineSimilarityRepository<I>>),
}

struct ReductionBatchSnapshot<I> {
    online: Option<Arc<crate::online_similarity::OnlineReductionGeneration<I>>>,
    exact: Option<ExactIndexGenerationPin<I>>,
}

enum PreparedChunkTrial<'a> {
    Ready(PersistentChunkPlan),
    Trials(ChunkTrial<'a>),
}

struct ChunkTrial<'a> {
    target_id: ChunkId,
    target: &'a [u8],
    independent: PreparedIndependentRecord,
    candidates: std::vec::IntoIter<SimilarityBaseCandidate>,
    maximum_encoded_payload_bytes: usize,
    best: Option<(usize, PreparedDependentRecord)>,
    remaining_trials: usize,
}

impl<I: Clone + StorageIo> fmt::Debug for PersistentReductionIndex<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentReductionIndex")
            .field("online", &matches!(self.source, ReductionSource::Online(_)))
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
            source: ReductionSource::Frozen {
                exact: exact.snapshot(),
                similarity,
            },
            counters: ReductionCounters::new(),
        })
    }

    #[must_use]
    pub fn online(repository: Arc<OnlineSimilarityRepository<I>>) -> Self {
        Self {
            source: ReductionSource::Online(repository),
            counters: ReductionCounters::new(),
        }
    }

    fn pin_batch(&self) -> ReductionBatchSnapshot<I> {
        let online = match &self.source {
            ReductionSource::Online(repository) => repository.pin(),
            ReductionSource::Frozen { .. } => None,
        };
        let exact = match &self.source {
            ReductionSource::Frozen { exact, .. } => exact.try_pin(),
            ReductionSource::Online(_) => online
                .as_ref()
                .and_then(|generation| generation.pin_exact()),
        };
        ReductionBatchSnapshot { online, exact }
    }

    /// Plans a bounded ingest batch against one pinned Exact/Similarity view.
    /// The caller owns target-byte admission; CPU phases acquire shared permits
    /// and release them before the coordinator performs bounded Base reads.
    /// Results retain input order, including independent fallbacks after errors.
    ///
    /// # Panics
    /// Panics if a worker fails to return its uniquely assigned result.
    pub fn plan_batch_for_publication_cached<C: StorageIo + Sync>(
        &self,
        containers: &ContainerRepository<C>,
        targets: &[PrehashedChunk<'_>],
        cache: Option<&VerifiedReadCache>,
        workers: NonZeroUsize,
        admission: &crate::WorkerPermits,
    ) -> Vec<(PersistentChunkPlan, Option<SimilarityIndexEntry>)>
    where
        I: Send + Sync,
    {
        let snapshot = self.pin_batch();
        let prepared = map_admitted(targets.to_vec(), workers, admission, |target| {
            let counters = self.counters.for_chunk(target.chunk_id());
            let fingerprint = timed_fingerprint(target.bytes(), counters)
                .map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
            let entry = SimilarityIndexEntry::new(
                target.chunk_id(),
                u32::try_from(target.bytes().len())
                    .map_err(|_| SimilarityIndexStoreError::InvalidTarget)?,
                fingerprint.profile(),
                fingerprint.superfeatures(),
                fingerprint.sketch(),
            )
            .map_err(SimilarityIndexStoreError::from)?;
            counters.queries.fetch_add(1, Ordering::Relaxed);
            let trial = self
                .prepare_trial(
                    target.chunk_id(),
                    target.bytes(),
                    &fingerprint,
                    counters,
                    &snapshot,
                )
                .unwrap_or_else(|_| {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    PreparedChunkTrial::Ready(PersistentChunkPlan::NoCandidates)
                });
            Ok::<_, SimilarityIndexStoreError>((trial, entry))
        });
        let mut ordered = std::iter::repeat_with(|| None)
            .take(targets.len())
            .collect::<Vec<_>>();
        let mut trials = Vec::new();
        for (ordinal, prepared) in prepared.into_iter().enumerate() {
            match prepared {
                Ok((PreparedChunkTrial::Ready(plan), hint)) => {
                    ordered[ordinal] = Some((plan, Some(hint)));
                }
                Ok((PreparedChunkTrial::Trials(trial), hint)) => {
                    trials.push((ordinal, trial, hint));
                }
                Err(_) => ordered[ordinal] = Some((PersistentChunkPlan::NoCandidates, None)),
            }
        }
        let mut trials = trials.into_iter();
        loop {
            // At most eight independently verified Base owners survive a CPU
            // boundary. No speculative candidates beyond the original trial
            // budget are read. The coherent Exact pin lives through all waves.
            let mut wave = trials.by_ref().take(8).collect::<Vec<_>>();
            if wave.is_empty() {
                break;
            }
            while !wave.is_empty() {
                let mut ready = Vec::new();
                for (ordinal, mut trial, hint) in wave {
                    if let Some((candidate, base)) =
                        self.read_next_trial_base(&mut trial, containers, cache, &snapshot)
                    {
                        ready.push((ordinal, trial, hint, candidate, base));
                    } else {
                        let plan = trial.finish(self.counters.for_chunk(hint.chunk_id()));
                        let hint =
                            (!matches!(plan, PersistentChunkPlan::Dependent(_))).then_some(hint);
                        ordered[ordinal] = Some((plan, hint));
                    }
                }
                let completed = map_admitted(
                    ready,
                    workers,
                    admission,
                    |(ordinal, mut trial, hint, candidate, base)| {
                        let counters = self.counters.for_chunk(trial.target_id);
                        let result = trial.run_base(candidate, base.as_slice(), counters);
                        if result.is_err() {
                            counters.errors.fetch_add(1, Ordering::Relaxed);
                        }
                        (ordinal, result.map(|()| trial), hint)
                    },
                );
                wave = Vec::new();
                for (ordinal, result, hint) in completed {
                    match result {
                        Ok(trial) => wave.push((ordinal, trial, hint)),
                        Err(_) => {
                            ordered[ordinal] =
                                Some((PersistentChunkPlan::NoCandidates, Some(hint)));
                        }
                    }
                }
            }
        }
        ordered
            .into_iter()
            .map(|plan| plan.expect("ASSERT: every bounded planning job completed"))
            .collect()
    }

    /// Fingerprints once for both lookup and later independent-base admission.
    /// The returned entry is only an optimization hint; callers may publish it
    /// after the corresponding independent Container and Exact Location.
    ///
    /// # Errors
    /// Returns the same errors as `plan_chunk`, plus invalid fingerprint input.
    pub fn plan_chunk_for_publication<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> Result<(PersistentChunkPlan, Option<SimilarityIndexEntry>), PersistentReductionError> {
        self.plan_chunk_for_publication_cached(containers, target_id, target, None)
    }

    /// Plans with a shared bounded frontend cache for independently verified Bases.
    /// Recovery and offline callers use `plan_chunk_for_publication` instead.
    ///
    /// # Errors
    /// Returns the same errors as `plan_chunk_for_publication`.
    pub fn plan_chunk_for_publication_cached<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
        cache: Option<&VerifiedReadCache>,
    ) -> Result<(PersistentChunkPlan, Option<SimilarityIndexEntry>), PersistentReductionError> {
        let snapshot = self.pin_batch();
        self.plan_publication_in_batch(containers, target_id, target, cache, &snapshot)
    }

    fn plan_publication_in_batch<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
        cache: Option<&VerifiedReadCache>,
        snapshot: &ReductionBatchSnapshot<I>,
    ) -> Result<(PersistentChunkPlan, Option<SimilarityIndexEntry>), PersistentReductionError> {
        let fingerprint = timed_fingerprint(target, self.counters.for_chunk(target_id))
            .map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
        let entry = SimilarityIndexEntry::new(
            target_id,
            u32::try_from(target.len()).map_err(|_| SimilarityIndexStoreError::InvalidTarget)?,
            fingerprint.profile(),
            fingerprint.superfeatures(),
            fingerprint.sketch(),
        )
        .map_err(SimilarityIndexStoreError::from)?;
        let counters = self.counters.for_chunk(target_id);
        counters.queries.fetch_add(1, Ordering::Relaxed);
        let plan = if let Ok(plan) = self.plan_chunk_inner(
            containers,
            target_id,
            target,
            &fingerprint,
            counters,
            cache,
            snapshot,
        ) {
            plan
        } else {
            counters.errors.fetch_add(1, Ordering::Relaxed);
            PersistentChunkPlan::NoCandidates
        };
        let hint = (!matches!(plan, PersistentChunkPlan::Dependent(_))).then_some(entry);
        Ok((plan, hint))
    }

    /// Returns lock-free, payload-free advanced-reduction telemetry.
    #[must_use]
    pub fn status(&self) -> PersistentReductionStatus {
        let counters = self.counters.snapshot();
        PersistentReductionStatus {
            online: match &self.source {
                ReductionSource::Online(index) => index.status(),
                ReductionSource::Frozen { .. } => crate::OnlineSimilarityStatus::default(),
            },
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
            fingerprint_ns: counters.fingerprint_ns,
            candidate_lookup_ns: counters.candidate_lookup_ns,
            base_read_ns: counters.base_read_ns,
            codec_trial_ns: counters.codec_trial_ns,
        }
    }

    #[must_use]
    pub fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        match &self.source {
            ReductionSource::Frozen { similarity, .. } => similarity.page_cache_status(),
            ReductionSource::Online(online) => online.page_cache_status(),
        }
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
        let fingerprint = timed_fingerprint(target, self.counters.for_chunk(target_id))
            .map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
        let counters = self.counters.for_chunk(target_id);
        counters.queries.fetch_add(1, Ordering::Relaxed);
        match self.plan_chunk_inner(
            containers,
            target_id,
            target,
            &fingerprint,
            counters,
            None,
            &self.pin_batch(),
        ) {
            Ok(plan) => Ok(plan),
            Err(error) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_chunk_inner<C: StorageIo>(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
        fingerprint: &SimilarityFingerprint,
        counters: &ReductionCounterStripe,
        cache: Option<&VerifiedReadCache>,
        snapshot: &ReductionBatchSnapshot<I>,
    ) -> Result<PersistentChunkPlan, PersistentReductionError> {
        match self.prepare_trial(target_id, target, fingerprint, counters, snapshot)? {
            PreparedChunkTrial::Ready(plan) => Ok(plan),
            PreparedChunkTrial::Trials(mut trial) => {
                while let Some((candidate, base)) =
                    self.read_next_trial_base(&mut trial, containers, cache, snapshot)
                {
                    trial.run_base(candidate, base.as_slice(), counters)?;
                }
                Ok(trial.finish(counters))
            }
        }
    }

    fn prepare_trial<'a>(
        &self,
        target_id: ChunkId,
        target: &'a [u8],
        fingerprint: &SimilarityFingerprint,
        counters: &ReductionCounterStripe,
        snapshot: &ReductionBatchSnapshot<I>,
    ) -> Result<PreparedChunkTrial<'a>, PersistentReductionError> {
        let lookup_phase = ReductionTimer::new(&counters.candidate_lookup_ns);
        let online = &snapshot.online;
        let exact = snapshot.exact.as_ref();
        let Some(_) = exact else {
            counters
                .no_candidate_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PreparedChunkTrial::Ready(PersistentChunkPlan::NoCandidates));
        };
        let length =
            u32::try_from(target.len()).map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
        let candidates = match &self.source {
            ReductionSource::Frozen { similarity, .. } => {
                similarity.candidates_fingerprinted(target_id, fingerprint, length)?
            }
            ReductionSource::Online(_) => online
                .as_ref()
                .ok_or(PersistentReductionError::IndexBindingMismatch)?
                .candidates(target_id, fingerprint, length)?,
        };
        drop(lookup_phase);
        counters.candidates.fetch_add(
            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if candidates.is_empty() {
            counters
                .no_candidate_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PreparedChunkTrial::Ready(PersistentChunkPlan::NoCandidates));
        }
        let codec_phase = ReductionTimer::new(&counters.codec_trial_ns);
        let independent = SealedContainer::prepare_prehashed_independent_record(
            PrehashedChunk::new(target_id, target),
            IncompressibilityGatePolicy::Off,
        )
        .map_err(|_| PersistentReductionError::IndependentCodec)?;
        drop(codec_phase);
        let Some(maximum_encoded_payload_bytes) =
            dependent_payload_cap(independent.encoded_payload_bytes())
        else {
            counters
                .independent_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PreparedChunkTrial::Ready(PersistentChunkPlan::Independent(
                independent,
            )));
        };
        Ok(PreparedChunkTrial::Trials(ChunkTrial {
            target_id,
            target,
            independent,
            candidates: candidates
                .into_iter()
                .take(4)
                .collect::<Vec<_>>()
                .into_iter(),
            maximum_encoded_payload_bytes,
            best: None,
            remaining_trials: MAXIMUM_DEPENDENT_TRIALS_V1,
        }))
    }

    fn read_next_trial_base<C: StorageIo>(
        &self,
        trial: &mut ChunkTrial<'_>,
        containers: &ContainerRepository<C>,
        cache: Option<&VerifiedReadCache>,
        snapshot: &ReductionBatchSnapshot<I>,
    ) -> Option<(
        SimilarityBaseCandidate,
        fastdup_format::VerifiedChunkPayload,
    )> {
        if trial.remaining_trials == 0 {
            return None;
        }
        let exact = snapshot.exact.as_ref()?;
        let counters = self.counters.for_chunk(trial.target_id);
        let _base_phase = ReductionTimer::new(&counters.base_read_ns);
        for candidate in trial.candidates.by_ref() {
            counters.base_reads.fetch_add(1, Ordering::Relaxed);
            if let Some(base) = containers.find_verified_independent_base_payload_with_index(
                exact,
                candidate.chunk_id(),
                candidate.logical_length(),
                cache,
            ) {
                counters.base_read_bytes.fetch_add(
                    u64::try_from(base.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                return Some((candidate, base));
            }
        }
        None
    }
}

impl ChunkTrial<'_> {
    fn run_base(
        &mut self,
        candidate: SimilarityBaseCandidate,
        base_bytes: &[u8],
        counters: &ReductionCounterStripe,
    ) -> Result<(), PersistentReductionError> {
        let _trial_phase = ReductionTimer::new(&counters.codec_trial_ns);
        let Self {
            target_id,
            target,
            maximum_encoded_payload_bytes,
            best,
            remaining_trials,
            ..
        } = self;
        let trial_cap = best
            .as_ref()
            .map_or(*maximum_encoded_payload_bytes, |(bytes, _)| {
                (*maximum_encoded_payload_bytes).min(bytes.saturating_sub(1))
            });
        let expected = BaseChunkRef::new(candidate.chunk_id(), candidate.logical_length());
        let base = VerifiedBaseChunk::from_verified_location(expected, base_bytes)
            .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
        let sparse_base = IndependentBaseRef::from_verified_identity(
            candidate.chunk_id(),
            candidate.logical_length(),
            base_bytes,
        )
        .map_err(|_| PersistentReductionError::VerifiedBaseMismatch)?;
        counters.sparse_xor_trials.fetch_add(1, Ordering::Relaxed);
        *remaining_trials -= 1;
        let sparse = SparseXorDelta::encode_bounded_prehashed_trial(
            sparse_base,
            base_bytes,
            *target_id,
            target,
            trial_cap,
        )
        .map_err(|_| PersistentReductionError::SparseXorCodec)?;
        if let Some(sparse) = sparse.filter(|s| s.cost().run_count() != 0) {
            let sparse_bytes = usize::try_from(sparse.cost().encoded_payload_bytes())
                .map_err(|_| PersistentReductionError::SparseXorCodec)?;
            let prepared = sparse
                .into_encoding()
                .into_prepared_record()
                .map(PreparedDependentRecord::from)
                .map_err(|_| PersistentReductionError::SparseXorCodec)?;
            if best.as_ref().is_none_or(|(bytes, _)| sparse_bytes < *bytes) {
                *best = Some((sparse_bytes, prepared));
            }
        }

        if *remaining_trials != 0 {
            counters.prefix_trials.fetch_add(1, Ordering::Relaxed);
            *remaining_trials -= 1;
            if let Some(trial) = ZstdPrefixCodec::encode_prehashed_trial(
                base,
                *target_id,
                target,
                best.as_ref()
                    .map_or(*maximum_encoded_payload_bytes, |(bytes, _)| {
                        (*maximum_encoded_payload_bytes).min(bytes.saturating_sub(1))
                    }),
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
                    *best = Some((prefix_bytes, prepared));
                }
            }
        }
        Ok(())
    }

    fn finish(self, counters: &ReductionCounterStripe) -> PersistentChunkPlan {
        let Self {
            independent,
            best,
            target_id,
            ..
        } = self;
        let Some((best_bytes, best)) = best else {
            counters
                .independent_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return PersistentChunkPlan::Independent(independent);
        };
        if !accept_dependent_v1(independent.encoded_payload_bytes(), best_bytes) {
            counters
                .independent_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return PersistentChunkPlan::Independent(independent);
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
        PersistentChunkPlan::Dependent(best)
    }
}

struct ReductionTimer<'a> {
    counter: &'a AtomicU64,
    started: std::time::Instant,
}

impl<'a> ReductionTimer<'a> {
    fn new(counter: &'a AtomicU64) -> Self {
        Self {
            counter,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for ReductionTimer<'_> {
    fn drop(&mut self) {
        let ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(ns))
            });
    }
}

fn timed_fingerprint(
    target: &[u8],
    counters: &ReductionCounterStripe,
) -> Result<SimilarityFingerprint, crate::reduction_similarity::SimilarityError> {
    let _phase = ReductionTimer::new(&counters.fingerprint_ns);
    SimilarityFingerprint::v1(target)
}

fn map_admitted<T: Send, R: Send>(
    inputs: Vec<T>,
    desired: NonZeroUsize,
    admission: &crate::WorkerPermits,
    apply: impl Fn(T) -> R + Sync,
) -> Vec<R> {
    admission.map(inputs, desired, apply)
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
    online: crate::OnlineSimilarityStatus,
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
    fingerprint_ns: u64,
    candidate_lookup_ns: u64,
    base_read_ns: u64,
    codec_trial_ns: u64,
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
    reduction_status_getter!(fingerprint_ns, fingerprint_ns, u64);
    reduction_status_getter!(candidate_lookup_ns, candidate_lookup_ns, u64);
    reduction_status_getter!(base_read_ns, base_read_ns, u64);
    reduction_status_getter!(codec_trial_ns, codec_trial_ns, u64);

    reduction_status_getter!(online, online, crate::OnlineSimilarityStatus);
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

fn dependent_payload_cap(independent_bytes: usize) -> Option<usize> {
    let savings = DEPENDENT_MINIMUM_SAVINGS_BYTES_V1.max(
        independent_bytes
            .checked_mul(DEPENDENT_MINIMUM_SAVINGS_PERCENT_V1)?
            .div_ceil(100),
    );
    independent_bytes
        .checked_sub(savings)
        .filter(|cap| *cap > 32)
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
    fn small_cpu_batches_leave_unused_permits_for_concurrent_stages() {
        let total = NonZeroUsize::new(10).unwrap();
        let admission = crate::WorkerPermits::new(total);
        let observed = map_admitted(vec![()], total, &admission, |()| {
            let spare = admission
                .try_acquire(NonZeroUsize::new(9).unwrap())
                .unwrap();
            spare.workers().get()
        });
        assert_eq!(observed, [9]);
        assert_eq!(admission.available(), 10);
        assert!(map_admitted(Vec::<()>::new(), total, &admission, |()| ()).is_empty());

        let held = admission.acquire(NonZeroUsize::new(8).unwrap());
        let outputs = map_admitted((0..32).collect(), total, &admission, |n| n * n);
        assert_eq!(outputs, (0..32).map(|n| n * n).collect::<Vec<_>>());
        assert_eq!(admission.available(), 2);
        drop(held);
        assert_eq!(admission.available(), 10);
    }

    #[test]
    fn dependent_caps_match_both_acceptance_thresholds_including_rounding() {
        for independent in [1_usize, 2048, 4096, 4128, 4129, 8192, 81_921, 262_144] {
            for dependent in [0, 32, 33, 45, 4096, independent.saturating_sub(4096)] {
                if dependent > 32 {
                    assert_eq!(
                        dependent_payload_cap(independent).is_some_and(|cap| dependent <= cap),
                        accept_dependent_v1(independent, dependent)
                    );
                }
            }
        }
    }

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
