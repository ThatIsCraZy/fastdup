use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::io;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::time::Instant;

use fastdup_format::{
    ChunkId, ContainerId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES,
    ExactIndexActivationError, ExactIndexActivationRecord, ExactIndexEntry, ExactIndexFormatError,
    ExactIndexPage, ExactIndexPagePosition, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunDescriptor, ExactIndexRunHashAudit, ExactIndexRunRef, ExactIndexRunSet,
    ExactIndexRunSetError, ExactIndexRunSetId, ExactIndexRunStreamEncoder, ExactLocationTransition,
    MAX_METADATA_OBJECT_BYTES,
};

use crate::exact_activation_log::{ExactActivationLog, ExactActivationLogError};
use crate::read_cache::{
    MemoryPressureSnapshot, SYSTEM_REFRESH_INTERVAL, shared_cache_reserve_bytes,
};
use crate::reduction_filter::{BlockedBloomHint, BloomLookupHint};
use crate::{ContainerRepository, StorageIo, StoreError};

pub const MAX_EXACT_LOOKUP_CANDIDATES: usize = 64;
pub const MAX_ACTIVE_EXACT_INDEX_FAMILIES: usize = 64;
const EXACT_INDEX_COMPACTION_FANIN: usize = 4;
const EXACT_INDEX_PAGE_CACHE_FALLBACK_SLOTS: usize = 256;
const EXACT_INDEX_PAGE_CACHE_MINIMUM_BYTES: u64 = 1_024 * 1_024;
const EXACT_INDEX_PAGE_CACHE_MAXIMUM_BYTES: u64 = 256 * 1_024 * 1_024;
const EXACT_INDEX_PAGE_CACHE_RAM_DIVISOR: u64 = 128;
const EXACT_RUN_MEMBERSHIP_RAM_DIVISOR: u64 = 32;
const EXACT_RUN_MEMBERSHIP_MINIMUM_BYTES: u64 = 1_024 * 1_024;
const EXACT_RUN_MEMBERSHIP_MAXIMUM_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
/// Compatibility name for the former physical-Run bound.
///
/// The bound applies to logical Run families since Run-Set v2. Physical
/// partitions within one family do not consume additional lookup precedence.
pub const MAX_ACTIVE_EXACT_INDEX_RUNS: usize = MAX_ACTIVE_EXACT_INDEX_FAMILIES;
pub const EXACT_INDEX_RUN_PARTITION_TARGET_ENTRIES: usize = 262_144;

/// One complete, key-disjoint output generation of Exact Index compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexRunFamily {
    runs: Vec<ExactIndexRunRef>,
    family_generation: u64,
    last_generation: u64,
}

impl ExactIndexRunFamily {
    fn new(runs: Vec<ExactIndexRunRef>) -> Result<Self, ExactIndexStoreError> {
        let profile = runs
            .first()
            .copied()
            .ok_or(ExactIndexStoreError::InvalidCompactionInput)?
            .profile();
        let canonical = ExactIndexRunSet::new(profile, 1, runs)?;
        if canonical.family_count() != 1 {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        let canonical_runs = canonical.runs();
        let family_generation = canonical_runs[0].family_generation();
        let last_generation = canonical_runs[canonical_runs.len() - 1].generation();
        Ok(Self {
            runs: canonical_runs.to_vec(),
            family_generation,
            last_generation,
        })
    }

    #[must_use]
    pub fn runs(&self) -> &[ExactIndexRunRef] {
        &self.runs
    }

    #[must_use]
    pub const fn family_generation(&self) -> u64 {
        self.family_generation
    }

    #[must_use]
    pub const fn last_generation(&self) -> u64 {
        self.last_generation
    }
}

#[derive(Clone, Debug)]
struct CompactionInputFamily {
    refs: Vec<ExactIndexRunRef>,
    family_generation: u64,
}

/// Durable immutable Exact Index run publication and bounded lookup module.
#[derive(Clone, Debug)]
pub struct ExactIndexRunRepository<I> {
    storage: I,
    publish_lock: Arc<Mutex<()>>,
    generation_publish_lock: Arc<Mutex<()>>,
    active_generation: Arc<RwLock<Option<Arc<ExactIndexGenerationState<I>>>>>,
    retired_generations: Arc<Mutex<Vec<Weak<ExactIndexGenerationState<I>>>>>,
    page_cache: Arc<ExactIndexPageCache>,
    fixed_membership_snapshot: Option<MemoryPressureSnapshot>,
    membership_counters: Arc<ExactRunMembershipCounters>,
}

#[derive(Debug)]
struct ExactIndexGenerationState<I> {
    index: ActivatedExactIndex<I>,
    pins: ExactIndexPinState,
}

#[repr(align(64))]
#[derive(Debug)]
struct ExactIndexPinState {
    active: AtomicUsize,
    accepting: AtomicBool,
    wait: Mutex<()>,
    drained: Condvar,
}

const _: () = assert!(std::mem::align_of::<ExactIndexPinState>() == 64);

/// Process-local lease for one immutable activated Exact Index generation.
///
/// A pin permits reads through that exact generation after a newer generation
/// marks one of its Locations RETIRING. Physical deletion must wait for the
/// corresponding [`ExactIndexGenerationDrain`] to complete.
pub struct ExactIndexGenerationPin<I> {
    state: Arc<ExactIndexGenerationState<I>>,
}

impl<I> Clone for ExactIndexGenerationPin<I> {
    fn clone(&self) -> Self {
        self.state
            .pins
            .active
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: Exact generation pin count cannot overflow");
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<I> Deref for ExactIndexGenerationPin<I> {
    type Target = ActivatedExactIndex<I>;

    fn deref(&self) -> &Self::Target {
        &self.state.index
    }
}

impl<I> fmt::Debug for ExactIndexGenerationPin<I>
where
    ActivatedExactIndex<I>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactIndexGenerationPin")
            .field("activation", &self.state.index.record())
            .finish_non_exhaustive()
    }
}

impl<I> Drop for ExactIndexGenerationPin<I> {
    fn drop(&mut self) {
        let previous = self.state.pins.active.fetch_sub(1, AtomicOrdering::Release);
        assert!(
            previous != 0,
            "ASSERT: Exact generation pin release has a matching acquisition"
        );
        if previous == 1 {
            let _wait = self
                .state
                .pins
                .wait
                .lock()
                .expect("ASSERT: Exact generation drain wait lock poisoned during release");
            self.state.pins.drained.notify_all();
        }
    }
}

impl<I> ExactIndexGenerationPin<I> {
    /// Creates an uncounted immutable snapshot handle. New work must call
    /// [`ExactIndexGenerationSnapshot::try_pin`]; once RETIRING activation
    /// closes the generation, that admission fails without touching DATA.
    #[must_use]
    pub fn snapshot(&self) -> ExactIndexGenerationSnapshot<I> {
        ExactIndexGenerationSnapshot {
            state: Arc::clone(&self.state),
        }
    }
}

/// Immutable generation reference that admits only work started before its
/// retirement barrier.
pub struct ExactIndexGenerationSnapshot<I> {
    state: Arc<ExactIndexGenerationState<I>>,
}

impl<I> ExactIndexGenerationSnapshot<I> {
    #[must_use]
    pub fn activation(&self) -> ExactIndexActivationRecord {
        self.state.index.record()
    }

    /// Pins one operation unless a newer activation already closed admission.
    ///
    /// # Panics
    ///
    /// Panics if the process-local pin count overflows. That is an impossible
    /// production `ASSERT`, not a recoverable resource condition.
    #[must_use]
    pub fn try_pin(&self) -> Option<ExactIndexGenerationPin<I>> {
        if !self.state.pins.accepting.load(AtomicOrdering::Acquire) {
            return None;
        }
        self.state
            .pins
            .active
            .fetch_update(AtomicOrdering::Acquire, AtomicOrdering::Relaxed, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: Exact generation pin count cannot overflow");
        if !self.state.pins.accepting.load(AtomicOrdering::Acquire) {
            drop(ExactIndexGenerationPin {
                state: Arc::clone(&self.state),
            });
            return None;
        }
        Some(ExactIndexGenerationPin {
            state: Arc::clone(&self.state),
        })
    }
}

impl<I> fmt::Debug for ExactIndexGenerationSnapshot<I>
where
    ActivatedExactIndex<I>: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactIndexGenerationSnapshot")
            .field("activation", &self.state.index.record())
            .finish_non_exhaustive()
    }
}

/// Wait capability for the exact generation displaced by one activation.
#[derive(Debug)]
pub struct ExactIndexGenerationDrain<I> {
    states: Vec<Arc<ExactIndexGenerationState<I>>>,
}

impl<I> ExactIndexGenerationDrain<I> {
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.states
            .iter()
            .all(|state| state.pins.active.load(AtomicOrdering::Acquire) == 0)
    }

    /// Waits until every reader, writer, and reduction-snapshot pin on the
    /// displaced generation has been released.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the generation-drain wait lock.
    pub fn wait(self) {
        for state in self.states {
            let mut wait = state
                .pins
                .wait
                .lock()
                .expect("ASSERT: Exact generation drain lock poisoned while waiting");
            while state.pins.active.load(AtomicOrdering::Acquire) != 0 {
                wait = state
                    .pins
                    .drained
                    .wait(wait)
                    .expect("ASSERT: Exact generation drain lock poisoned after wake");
            }
        }
    }
}

/// Result of one atomic Exact generation activation.
#[derive(Debug)]
pub struct ExactIndexGenerationTransition<I> {
    current: ExactIndexGenerationPin<I>,
    retired: Option<ExactIndexGenerationDrain<I>>,
}

impl<I> ExactIndexGenerationTransition<I> {
    #[must_use]
    pub const fn current(&self) -> &ExactIndexGenerationPin<I> {
        &self.current
    }

    #[must_use]
    pub fn into_retired(self) -> Option<ExactIndexGenerationDrain<I>> {
        self.retired
    }
}

fn pin_exact_generation<I>(
    state: &Arc<ExactIndexGenerationState<I>>,
) -> ExactIndexGenerationPin<I> {
    assert!(
        state.pins.accepting.load(AtomicOrdering::Acquire),
        "ASSERT: current Exact generation accepts new pins"
    );
    state
        .pins
        .active
        .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |active| {
            active.checked_add(1)
        })
        .expect("ASSERT: Exact generation pin count cannot overflow");
    ExactIndexGenerationPin {
        state: Arc::clone(state),
    }
}

/// Fixed-capacity Exact-Index hot-page cache evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExactIndexPageCacheStatus {
    hits: u64,
    misses: u64,
    resident_pages: u64,
    evictions: u64,
    pressure_rejections: u64,
    target_pages: u64,
    capacity_pages: u64,
    reserve_bytes: u64,
    effective_limit_bytes: u64,
    available_bytes: u64,
    swap_used_bytes: u64,
}

impl ExactIndexPageCacheStatus {
    #[must_use]
    pub const fn hits(self) -> u64 {
        self.hits
    }

    #[must_use]
    pub const fn misses(self) -> u64 {
        self.misses
    }

    #[must_use]
    pub const fn resident_pages(self) -> u64 {
        self.resident_pages
    }

    #[must_use]
    pub const fn evictions(self) -> u64 {
        self.evictions
    }

    #[must_use]
    pub const fn pressure_rejections(self) -> u64 {
        self.pressure_rejections
    }

    #[must_use]
    pub const fn target_pages(self) -> u64 {
        self.target_pages
    }

    #[must_use]
    pub const fn capacity_pages(self) -> u64 {
        self.capacity_pages
    }

    #[must_use]
    pub const fn reserve_bytes(self) -> u64 {
        self.reserve_bytes
    }

    #[must_use]
    pub const fn effective_limit_bytes(self) -> u64 {
        self.effective_limit_bytes
    }

    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }

    #[must_use]
    pub const fn swap_used_bytes(self) -> u64 {
        self.swap_used_bytes
    }

    /// Returns hit rate in basis points, or zero before the first lookup.
    #[must_use]
    pub fn hit_rate_basis_points(self) -> u64 {
        let total = self.hits.saturating_add(self.misses);
        self.hits
            .saturating_mul(10_000)
            .checked_div(total)
            .unwrap_or(0)
    }
}

/// Process-lifetime evidence for immutable active-Run membership probes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExactRunMembershipStatus {
    filter_count: u64,
    allocated_bytes: u64,
    probes: u64,
    definitely_absent: u64,
    requires_exact_lookup: u64,
}

impl ExactRunMembershipStatus {
    #[must_use]
    pub const fn filter_count(self) -> u64 {
        self.filter_count
    }

    #[must_use]
    pub const fn allocated_bytes(self) -> u64 {
        self.allocated_bytes
    }

    #[must_use]
    pub const fn probes(self) -> u64 {
        self.probes
    }

    #[must_use]
    pub const fn definitely_absent(self) -> u64 {
        self.definitely_absent
    }

    #[must_use]
    pub const fn requires_exact_lookup(self) -> u64 {
        self.requires_exact_lookup
    }
}

#[derive(Debug, Default)]
struct ExactRunMembershipCounters {
    probes: AtomicU64,
    definitely_absent: AtomicU64,
    requires_exact_lookup: AtomicU64,
}

/// Payload-free evidence from pairing every ACTIVE index entry with its
/// immutable Container location during an offline scrub.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexLocationAudit {
    activation: ExactIndexActivationRecord,
    active_locations: u64,
}

impl ExactIndexLocationAudit {
    #[must_use]
    pub const fn activation(self) -> ExactIndexActivationRecord {
        self.activation
    }

    #[must_use]
    pub const fn active_locations(self) -> u64 {
        self.active_locations
    }
}

impl<I: Clone + StorageIo> ExactIndexRunRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
            generation_publish_lock: Arc::new(Mutex::new(())),
            active_generation: Arc::new(RwLock::new(None)),
            retired_generations: Arc::new(Mutex::new(Vec::new())),
            page_cache: Arc::new(ExactIndexPageCache::build(snapshot, true)),
            fixed_membership_snapshot: None,
            membership_counters: Arc::new(ExactRunMembershipCounters::default()),
        }
    }

    /// Constructs a repository with a deterministic, manually fixed memory
    /// snapshot for tests and embedded runtimes with an external governor.
    ///
    /// The ordinary constructor samples host/cgroup pressure automatically.
    /// This variant deliberately does not refresh `/proc`; callers must create
    /// a new repository to apply another snapshot.
    #[must_use]
    pub fn new_with_memory_snapshot(storage: I, snapshot: MemoryPressureSnapshot) -> Self {
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
            generation_publish_lock: Arc::new(Mutex::new(())),
            active_generation: Arc::new(RwLock::new(None)),
            retired_generations: Arc::new(Mutex::new(Vec::new())),
            page_cache: Arc::new(ExactIndexPageCache::build(snapshot, false)),
            fixed_membership_snapshot: Some(snapshot),
            membership_counters: Arc::new(ExactRunMembershipCounters::default()),
        }
    }

    /// Returns repository-wide bounded Exact-Index hot-page cache evidence.
    #[must_use]
    pub fn page_cache_status(&self) -> ExactIndexPageCacheStatus {
        self.page_cache.status()
    }

    /// Durably publishes one immutable run without activating it.
    ///
    /// Idempotent retry succeeds only when an existing canonical name has the
    /// same profile, generation, and complete run hash. A different run under
    /// the same identity is an integrity failure.
    ///
    /// # Errors
    ///
    /// Returns format, I/O, writer-reread, collision, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics if the repository's writer lock is poisoned, or if a validated
    /// format-v1 object violates its own fixed page geometry. Both are
    /// production-fatal internal `ASSERT` failures.
    pub fn publish(
        &self,
        run: &ExactIndexRun,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index run publication lock poisoned");
        let encoded = run.encode()?;
        let expected = descriptor_from_complete_bytes(&encoded)?;
        let temporary_name = temporary_name(run.profile(), run.generation());
        let published_name = published_name(run.profile(), run.generation());

        if self.storage.exists(&published_name)? {
            let observed = self.audit_named(&published_name)?;
            verify_expected_descriptor(expected, observed)?;
            self.storage.sync_root()?;
            return Ok(observed);
        }

        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        for (page_ordinal, page) in encoded.chunks(EXACT_INDEX_PAGE_BYTES).enumerate() {
            assert_eq!(
                page.len(),
                EXACT_INDEX_PAGE_BYTES,
                "ASSERT: Exact Index Run v1 always consists of complete 4-KiB pages"
            );
            let offset = page_ordinal
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .and_then(|value| u64::try_from(value).ok())
                .expect("ASSERT: a bounded Exact Index run offset fits u64");
            self.storage.write_at(&temporary_name, offset, page)?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len())
                .expect("ASSERT: a bounded Exact Index run length fits u64"),
        )?;
        let observed = self.audit_named(&temporary_name)?;
        verify_expected_descriptor(expected, observed)?;
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name)?;
                verify_expected_descriptor(expected, raced)?;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(observed)
    }

    /// Opens one published run using only its exact length, Header, and Footer.
    ///
    /// The returned reader performs bounded 4-KiB page reads. It does not make
    /// negative lookup results authoritative.
    ///
    /// # Errors
    ///
    /// Returns I/O, envelope-integrity, or requested-identity errors.
    pub fn open(
        &self,
        profile: ExactIndexProfileId,
        generation: u64,
    ) -> Result<ExactIndexRunReader<I>, ExactIndexStoreError> {
        let name = published_name(profile, generation);
        let descriptor = self.open_named(&name)?;
        verify_requested_identity(profile, generation, descriptor)?;
        Ok(ExactIndexRunReader {
            storage: self.storage.clone(),
            name,
            descriptor,
            page_cache: Arc::clone(&self.page_cache),
            membership: None,
            membership_counters: Arc::clone(&self.membership_counters),
        })
    }

    /// Sequentially verifies every page, cross-page ordering, and the complete
    /// run hash without materializing the run or its full key map.
    ///
    /// # Errors
    ///
    /// Returns I/O, format-integrity, requested-identity, or AUDIT failures.
    pub fn audit(
        &self,
        profile: ExactIndexProfileId,
        generation: u64,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let descriptor = self.audit_named(&published_name(profile, generation))?;
        verify_requested_identity(profile, generation, descriptor)?;
        Ok(descriptor)
    }

    /// Streams a bounded-fanin set of fully audited immutable Runs into one new Run.
    ///
    /// For a repeated physical Location the transition from the newest source
    /// Run generation wins. Every other Location is retained, including
    /// tombstones needed to shadow still-active older Runs. The output is
    /// canonical and independent of source discovery order.
    ///
    /// This publishes the resulting Run but does not activate it. The caller
    /// must activate one complete replacement Run Set only after every retained
    /// dependency is durable.
    ///
    /// # Errors
    ///
    /// Rejects fewer than two inputs, duplicate/mismatched source identities,
    /// a nonmonotonic target generation, source corruption, Chunk-ID length
    /// conflicts, output above the Run-v1 object bound, or publication I/O.
    ///
    /// # Panics
    ///
    /// Panics only if the verified K-way cursor loses or reorders its own
    /// current entry. This is an impossible production `ASSERT` failure.
    pub fn compact(
        &self,
        inputs: &[ExactIndexRunRef],
        target_generation: u64,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        if inputs.len() < 2 || inputs.len() > MAX_ACTIVE_EXACT_INDEX_RUNS {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        let profile = inputs[0].profile();
        let mut ordered_inputs = Vec::new();
        ordered_inputs
            .try_reserve_exact(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        ordered_inputs.extend_from_slice(inputs);
        ordered_inputs.sort_unstable_by_key(|run| run.generation());
        if ordered_inputs.iter().any(|run| run.profile() != profile)
            || ordered_inputs
                .windows(2)
                .any(|pair| pair[0].generation() == pair[1].generation())
            || ordered_inputs
                .last()
                .is_none_or(|run| target_generation <= run.generation())
        {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }

        let summary = self.merge_compaction_inputs(&ordered_inputs, |_| Ok(()))?;
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index streaming compaction lock poisoned");
        self.publish_streamed_compaction(&ordered_inputs, profile, target_generation, summary)
    }

    /// Compacts complete source families into one key-partitioned Run family.
    ///
    /// Output partitions never split one Chunk ID. Every partition is fully
    /// audited and synchronized before one final directory sync makes the
    /// complete unpublished family reusable by a later Run-Set activation.
    ///
    /// # Errors
    ///
    /// Rejects incomplete/mixed input families, invalid level/generation
    /// transitions, source corruption, an unsplittable Run-v1-sized hot key,
    /// excessive partition count, or publication I/O.
    ///
    /// # Panics
    ///
    /// Panics only if verified merge summaries disagree with the second pass
    /// or the format writer rejects its own previously verified descriptors.
    pub fn compact_family(
        &self,
        inputs: &[ExactIndexRunRef],
        target_level: u16,
        first_generation: u64,
    ) -> Result<ExactIndexRunFamily, ExactIndexStoreError> {
        let input_families =
            validate_family_compaction_inputs(inputs, target_level, first_generation)?;
        let profile = input_families[0].refs[0].profile();
        let summaries = self.compaction_partition_summaries(&input_families)?;
        let partition_count = u16::try_from(summaries.len())
            .map_err(|_| ExactIndexStoreError::TooManyRunPartitions)?;
        first_generation
            .checked_add(u64::from(partition_count) - 1)
            .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index family compaction lock poisoned");
        let descriptors =
            self.publish_streamed_family(&input_families, profile, first_generation, &summaries)?;
        assert_eq!(
            descriptors.len(),
            summaries.len(),
            "ASSERT: streamed family descriptor count must equal its first-pass partition count"
        );
        let mut runs = Vec::new();
        runs.try_reserve_exact(descriptors.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for (ordinal, descriptor) in descriptors.into_iter().enumerate() {
            runs.push(ExactIndexRunRef::family_partition(
                target_level,
                first_generation,
                u16::try_from(ordinal).map_err(|_| ExactIndexStoreError::TooManyRunPartitions)?,
                partition_count,
                descriptor,
            )?);
        }
        ExactIndexRunFamily::new(runs)
    }

    /// Publishes and activates one Run Set after fully auditing every named
    /// immutable Run. The final selected-slot sync is the only commit point.
    ///
    /// # Errors
    ///
    /// Returns dependency, content-address, chain, I/O, reread, or durability
    /// errors. A failed activation never changes Namespace durability.
    ///
    /// # Panics
    ///
    /// Panics if the shared publication lock is poisoned or fixed format-v1
    /// sizes violate their compile-time geometry.
    pub fn activate(
        &self,
        run_set: &ExactIndexRunSet,
    ) -> Result<ActivatedExactIndex<I>, ExactIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index activation lock poisoned");
        let readers = self.verify_run_set_dependencies(run_set)?;
        let encoded = run_set.encode()?;
        let run_set_id = ExactIndexRunSetId::from_encoded(&encoded)?;
        self.publish_run_set(run_set_id, &encoded)?;
        let log = ExactActivationLog::new(&self.storage);
        let snapshot = log.load_for_append().map_err(map_activation_log_error)?;
        if let Some(last) = snapshot.last_record() {
            if last.run_set_id() == run_set_id {
                if last.profile() != run_set.profile()
                    || last.run_set_generation() != run_set.generation()
                {
                    return Err(ExactIndexStoreError::DependencyMismatch);
                }
                log.sync_selected(&snapshot)
                    .map_err(map_activation_log_error)?;
                return ActivatedExactIndex::new(last, run_set.clone(), readers);
            }
            if run_set.generation() <= last.run_set_generation() {
                return Err(ExactIndexStoreError::NonMonotonicRunSetGeneration);
            }
        }
        let generation = snapshot.last_record().map_or(Ok(1), |record| {
            record
                .generation()
                .checked_add(1)
                .ok_or(ExactIndexStoreError::ActivationWalCorrupt)
        })?;
        let previous_hash = snapshot
            .last_hash()
            .unwrap_or(fastdup_format::ExactIndexActivationHash::ZERO);
        let record = ExactIndexActivationRecord::new(
            generation,
            previous_hash,
            run_set_id,
            run_set.profile(),
            run_set.generation(),
        )?;
        log.append(&snapshot, record)
            .map_err(map_activation_log_error)?;
        ActivatedExactIndex::new(record, run_set.clone(), readers)
    }

    /// Recovers the newest contiguous activation record and verifies its exact
    /// Run Set plus every pinned immutable Run dependency.
    ///
    /// A torn final record is ignored. A complete invalid chain or invalid
    /// dependency disables this index generation with an error; it never rolls
    /// Namespace metadata back.
    ///
    /// # Errors
    ///
    /// Returns activation-chain, Run Set, Run, identity, I/O, or integrity
    /// failures.
    pub fn recover_active(&self) -> Result<Option<ActivatedExactIndex<I>>, ExactIndexStoreError> {
        let log = ExactActivationLog::new(&self.storage);
        let Some(snapshot) = log.load_for_recovery().map_err(map_activation_log_error)? else {
            return Ok(None);
        };
        let Some(record) = snapshot.last_record() else {
            return Ok(None);
        };
        self.open_activated_record(record).map(Some)
    }

    /// Recovers the durable active generation and installs it behind the
    /// process-local pin seam.
    ///
    /// Repeated recovery of the already installed activation returns another
    /// pin instead of displacing the same generation.
    ///
    /// # Errors
    ///
    /// Returns the same recovery and dependency errors as [`Self::recover_active`].
    ///
    /// # Panics
    ///
    /// Panics if the process-local generation publication lock is poisoned or
    /// its pin count overflows.
    pub fn recover_active_generation(
        &self,
    ) -> Result<Option<ExactIndexGenerationPin<I>>, ExactIndexStoreError> {
        let _generation = self
            .generation_publish_lock
            .lock()
            .expect("ASSERT: Exact generation publication lock poisoned");
        let Some(active) = self.recover_active()? else {
            return Ok(None);
        };
        if let Some(current) = self.pin_matching_generation(active.record()) {
            return Ok(Some(current));
        }
        let transition = self.install_active_generation(active);
        Ok(Some(transition.current))
    }

    /// Pins the currently installed process-local Exact generation.
    ///
    /// This performs no storage I/O. `None` means recovery has not installed a
    /// usable generation or the appliance deliberately runs in scan fallback.
    ///
    /// # Panics
    ///
    /// Panics if the process-local active-generation lock is poisoned or its
    /// pin count overflows.
    #[must_use]
    pub fn pin_active_generation(&self) -> Option<ExactIndexGenerationPin<I>> {
        let active = self
            .active_generation
            .read()
            .expect("ASSERT: active Exact generation lock poisoned");
        active.as_ref().map(|state| pin_exact_generation(state))
    }

    /// Derives the effective RETIRING Container set from one fully opened
    /// immutable generation. Older ACTIVE occurrences of the same physical
    /// Location are shadowed before Container identities are returned.
    ///
    /// # Errors
    ///
    /// Returns touched-page integrity, I/O, allocation, or merge failures.
    pub fn retiring_containers(
        &self,
        generation: &ExactIndexGenerationPin<I>,
    ) -> Result<BTreeMap<[u8; 16], ContainerId>, ExactIndexStoreError> {
        let mut containers = BTreeMap::new();
        for entry in self.retiring_entries(generation)? {
            let container_id = entry.location().container_id();
            containers.insert(container_id.bytes(), container_id);
        }
        Ok(containers)
    }

    /// Returns every effective RETIRING physical Location in one fully opened
    /// immutable generation.
    ///
    /// The result is recovery authority rather than a candidate hint: the
    /// generation merge shadows older ACTIVE and already-REMOVED occurrences
    /// of the same physical Location before returning entries.
    ///
    /// # Errors
    ///
    /// Returns touched-page integrity, I/O, allocation, or merge failures.
    pub fn retiring_entries(
        &self,
        generation: &ExactIndexGenerationPin<I>,
    ) -> Result<Vec<ExactIndexEntry>, ExactIndexStoreError> {
        let families = compaction_families_from_run_set(generation.run_set())?;
        let mut entries = Vec::new();
        self.merge_compaction_families(&families, |entry| {
            if entry.transition() == fastdup_format::ExactLocationTransition::Retiring {
                entries
                    .try_reserve(1)
                    .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
                entries.push(entry);
            }
            Ok(())
        })?;
        Ok(entries)
    }

    /// Publishes one immutable level-zero transition family and atomically
    /// activates it on top of the latest durable Run Set.
    ///
    /// All repository clones serialize the complete read/publish/compact/
    /// activate transaction. The newest transition for a repeated physical
    /// Location therefore cannot be lost by a concurrent ordinary L0 append.
    /// The returned drain names the generation displaced at the activation
    /// commit point.
    ///
    /// # Errors
    ///
    /// Rejects empty entries, profile mismatch, generation exhaustion,
    /// invalid transitions, compaction failure, or publication/activation I/O.
    ///
    /// # Panics
    ///
    /// Panics if a shared publication lock is poisoned or an already verified
    /// format invariant is violated by its writer.
    pub fn append_level_zero(
        &self,
        profile: ExactIndexProfileId,
        entries: Vec<ExactIndexEntry>,
    ) -> Result<ExactIndexGenerationTransition<I>, ExactIndexStoreError> {
        if entries.is_empty() {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        let _generation = self
            .generation_publish_lock
            .lock()
            .expect("ASSERT: Exact generation publication lock poisoned");
        let previous = self.recover_active()?;
        self.append_level_zero_from(profile, entries, previous.as_ref())
    }

    /// Appends one L0 family only if the named Exact activation is still the
    /// durable predecessor at the serialized generation commit point.
    ///
    /// # Errors
    ///
    /// Returns [`ExactIndexStoreError::ActivationChanged`] when another L0
    /// publisher won the race, plus the ordinary append errors.
    ///
    /// # Panics
    ///
    /// Panics if a shared publication lock is poisoned or an already verified
    /// format invariant is violated by its writer.
    pub fn append_level_zero_if_active(
        &self,
        profile: ExactIndexProfileId,
        expected: ExactIndexActivationRecord,
        entries: Vec<ExactIndexEntry>,
    ) -> Result<ExactIndexGenerationTransition<I>, ExactIndexStoreError> {
        if entries.is_empty() {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        let _generation = self
            .generation_publish_lock
            .lock()
            .expect("ASSERT: Exact generation publication lock poisoned");
        let previous = self.recover_active()?;
        if previous.as_ref().map(ActivatedExactIndex::record) != Some(expected) {
            return Err(ExactIndexStoreError::ActivationChanged);
        }
        self.append_level_zero_from(profile, entries, previous.as_ref())
    }

    fn append_level_zero_from(
        &self,
        profile: ExactIndexProfileId,
        entries: Vec<ExactIndexEntry>,
        previous: Option<&ActivatedExactIndex<I>>,
    ) -> Result<ExactIndexGenerationTransition<I>, ExactIndexStoreError> {
        if previous.is_some_and(|active| active.run_set().profile() != profile) {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        validate_level_zero_transitions(previous, &entries)?;
        let mut newest_run_generation = self
            .discover_run_generation_high_water(profile)?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
        let run = ExactIndexRun::new(profile, newest_run_generation, entries)?;
        let descriptor = self.publish(&run)?;
        let mut run_refs =
            previous.map_or_else(Vec::new, |active| active.run_set().runs().to_vec());
        run_refs
            .try_reserve(1)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        run_refs.push(ExactIndexRunRef::new(0, descriptor)?);
        while let Some((source_level, inputs)) = select_level_zero_compaction(&run_refs) {
            let first_output_generation = newest_run_generation
                .checked_add(1)
                .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
            let target_level = source_level
                .checked_add(1)
                .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
            let compacted = self.compact_family(&inputs, target_level, first_output_generation)?;
            newest_run_generation = compacted.last_generation();
            run_refs.retain(|run| {
                !inputs
                    .iter()
                    .any(|input| input.generation() == run.generation())
            });
            run_refs
                .try_reserve(compacted.runs().len())
                .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
            run_refs.extend_from_slice(compacted.runs());
        }
        let run_set_generation = previous.map_or(Ok(1), |active| {
            active
                .run_set()
                .generation()
                .checked_add(1)
                .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)
        })?;
        let run_set = ExactIndexRunSet::new(profile, run_set_generation, run_refs)?;
        let active = self.activate(&run_set)?;
        Ok(self.install_active_generation(active))
    }

    fn pin_matching_generation(
        &self,
        record: ExactIndexActivationRecord,
    ) -> Option<ExactIndexGenerationPin<I>> {
        let active = self
            .active_generation
            .read()
            .expect("ASSERT: active Exact generation lock poisoned");
        active
            .as_ref()
            .filter(|state| state.index.record() == record)
            .map(pin_exact_generation)
    }

    fn install_active_generation(
        &self,
        active: ActivatedExactIndex<I>,
    ) -> ExactIndexGenerationTransition<I> {
        let state = Arc::new(ExactIndexGenerationState {
            index: active,
            pins: ExactIndexPinState {
                active: AtomicUsize::new(0),
                accepting: AtomicBool::new(true),
                wait: Mutex::new(()),
                drained: Condvar::new(),
            },
        });
        let current = pin_exact_generation(&state);
        let mut installed = self
            .active_generation
            .write()
            .expect("ASSERT: active Exact generation lock poisoned during activation");
        let retired = installed.take().map(|state| {
            state.pins.accepting.store(false, AtomicOrdering::Release);
            let mut retired = self
                .retired_generations
                .lock()
                .expect("ASSERT: retired Exact generation registry lock poisoned");
            retired.retain(|generation| generation.strong_count() != 0);
            retired.push(Arc::downgrade(&state));
            let states = retired.iter().filter_map(Weak::upgrade).collect();
            ExactIndexGenerationDrain { states }
        });
        *installed = Some(state);
        ExactIndexGenerationTransition { current, retired }
    }

    /// Audits both bounded Activation-Log slots and the selected immutable
    /// Run-Set dependency graph without changing activation state.
    ///
    /// This is the offline-scrub pairing for the writer and recovery slot
    /// invariants. A corrupt inactive peer is reported rather than silently
    /// discarded, because it could otherwise be mistaken for rotation
    /// evidence after another fault.
    ///
    /// # Errors
    ///
    /// Returns slot topology, hash-chain, Run Set, Run, identity, I/O, or
    /// integrity failures.
    pub fn audit_activation_log(
        &self,
    ) -> Result<Option<ExactIndexActivationRecord>, ExactIndexStoreError> {
        let log = ExactActivationLog::new(&self.storage);
        let Some(snapshot) = log.load_for_recovery().map_err(map_activation_log_error)? else {
            return Ok(None);
        };
        let Some(record) = snapshot.last_record() else {
            return Ok(None);
        };
        self.open_activated_record(record)?;
        Ok(Some(record))
    }

    /// Audits the complete selected index graph and pairs every ACTIVE entry
    /// with the exact immutable Container record it accelerates.
    ///
    /// This deliberately performs random Container reads and is intended for
    /// offline scrub, not lookup or mount recovery. Non-ACTIVE transitions are
    /// authenticated by the Run audit but have no live DATA dependency.
    ///
    /// # Errors
    ///
    /// Returns activation, Run-Set, Run, page, Container, identity, I/O, or
    /// checked-counter failures.
    ///
    /// # Panics
    ///
    /// Panics if a format-verified logical length does not fit the host address
    /// space. Supported production targets have at least 32-bit `usize`.
    pub fn audit_active_locations<J: StorageIo>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<Option<ExactIndexLocationAudit>, ExactIndexStoreError> {
        let log = ExactActivationLog::new(&self.storage);
        let Some(snapshot) = log.load_for_recovery().map_err(map_activation_log_error)? else {
            return Ok(None);
        };
        let Some(record) = snapshot.last_record() else {
            return Ok(None);
        };
        let active = self.open_activated_record(record)?;
        self.audit_run_set_global_invariants(active.run_set())?;
        // Membership hints cover every persisted transition, including
        // tombstones. Check their no-false-negative invariant independently
        // from effective Location selection.
        for reader in &active.readers {
            for page_ordinal in 0..reader.descriptor.page_count() {
                let page = reader.read_page(page_ordinal)?;
                for entry in page.entries() {
                    if reader.membership.as_ref().is_some_and(|membership| {
                        membership.probe_for_exact_lookup(
                            entry.chunk_id(),
                            usize::try_from(entry.logical_length())
                                .expect("ASSERT: Exact logical length fits usize"),
                        ) == BloomLookupHint::DefinitelyAbsent
                    }) {
                        return Err(ExactIndexStoreError::MembershipFalseNegative);
                    }
                }
            }
        }
        let families = compaction_families_from_run_set(active.run_set())?;
        let mut active_locations = 0_u64;
        self.merge_compaction_families(&families, |entry| {
            if entry.transition() != fastdup_format::ExactLocationTransition::Active {
                return Ok(());
            }
            if entry.location().dependency_id() == [0; 32] {
                containers.read_verified_location(entry)?;
            } else {
                let base_id = fastdup_format::ChunkId::from_bytes(entry.location().dependency_id());
                let base = containers
                    .find_verified_independent_base_with_index(
                        &active,
                        base_id,
                        entry.logical_length(),
                    )
                    .ok_or(ExactIndexStoreError::DependencyMismatch)?;
                containers.read_verified_zstd_prefix_location(entry, &base)?;
            }
            active_locations = active_locations
                .checked_add(1)
                .ok_or(ExactIndexStoreError::CounterOverflow)?;
            Ok(())
        })?;
        Ok(Some(ExactIndexLocationAudit {
            activation: record,
            active_locations,
        }))
    }

    /// Streams all logical Run families through one bounded K-way merge to
    /// verify cross-family Chunk-length and physical-transition invariants.
    ///
    /// No complete Chunk map or output Run is materialized. Memory is bounded
    /// by one verified page and one heap entry per active family.
    ///
    /// # Errors
    ///
    /// Returns Run-Set, Run/page/hash, cross-family identity, I/O, allocation,
    /// or checked-arithmetic failures.
    pub(crate) fn audit_run_set_global_invariants(
        &self,
        run_set: &ExactIndexRunSet,
    ) -> Result<(), ExactIndexStoreError> {
        if run_set.runs().is_empty() {
            return Ok(());
        }
        let families = compaction_families_from_run_set(run_set)?;
        self.merge_compaction_families(&families, |_| Ok(()))?;
        Ok(())
    }

    /// Returns the greatest generation named by any immutable Run for one
    /// profile, including unpublished/orphaned rebuild output.
    ///
    /// Rebuilders use this allocator high-water so retries never collide with
    /// a different immutable object left behind before activation.
    ///
    /// # Errors
    ///
    /// Returns directory I/O or a malformed canonical Run name.
    pub fn discover_run_generation_high_water(
        &self,
        profile: ExactIndexProfileId,
    ) -> Result<Option<u64>, ExactIndexStoreError> {
        let mut high_water = None;
        for name in self.storage.list_names()? {
            let Some((observed_profile, generation)) = parse_run_name(&name)? else {
                continue;
            };
            if observed_profile == profile {
                high_water =
                    Some(high_water.map_or(generation, |value: u64| value.max(generation)));
            }
        }
        Ok(high_water)
    }

    fn open_activated_record(
        &self,
        record: ExactIndexActivationRecord,
    ) -> Result<ActivatedExactIndex<I>, ExactIndexStoreError> {
        let run_set = self.read_run_set(record.run_set_id())?;
        if run_set.profile() != record.profile()
            || run_set.generation() != record.run_set_generation()
            || run_set.id()? != record.run_set_id()
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let readers = self.verify_run_set_dependencies(&run_set)?;
        ActivatedExactIndex::new(record, run_set, readers)
    }

    fn open_named(&self, name: &str) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        Ok(self.read_envelope(name)?.descriptor)
    }

    fn read_envelope(&self, name: &str) -> Result<OpenedRunEnvelope, ExactIndexStoreError> {
        let file_length = self.storage.object_len(name)?;
        if file_length < 2 * u64::try_from(EXACT_INDEX_PAGE_BYTES).expect("ASSERT: 4 KiB fits u64")
        {
            return Err(ExactIndexFormatError::InvalidObjectLength(
                usize::try_from(file_length).unwrap_or(usize::MAX),
            )
            .into());
        }
        let footer_offset = file_length
            .checked_sub(u64::try_from(EXACT_INDEX_PAGE_BYTES).expect("ASSERT: 4 KiB fits u64"))
            .expect("ASSERT: minimum run length was checked");
        let header = self
            .storage
            .read_exact_at(name, 0, EXACT_INDEX_HEADER_BYTES)?;
        let footer = self
            .storage
            .read_exact_at(name, footer_offset, EXACT_INDEX_PAGE_BYTES)?;
        let descriptor = ExactIndexRunDescriptor::decode(&header, &footer, file_length)?;
        Ok(OpenedRunEnvelope {
            descriptor,
            header,
            footer,
            footer_offset,
        })
    }

    fn audit_named(&self, name: &str) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let envelope = self.read_envelope(name)?;
        self.audit_opened_run(name, &envelope, |_| {})?;
        Ok(envelope.descriptor)
    }

    fn audit_named_with_membership(
        &self,
        name: &str,
        maximum_bytes: usize,
    ) -> Result<(ExactIndexRunDescriptor, Option<Arc<BlockedBloomHint>>), ExactIndexStoreError>
    {
        let envelope = self.read_envelope(name)?;
        let descriptor = envelope.descriptor;
        let mut membership = (maximum_bytes != 0)
            .then(|| BlockedBloomHint::new(descriptor.entry_count(), maximum_bytes).ok())
            .flatten();
        self.audit_opened_run(name, &envelope, |entry| {
            if let Some(filter) = &mut membership {
                let logical_length = usize::try_from(entry.logical_length())
                    .expect("ASSERT: Exact logical length fits usize");
                filter.insert_hint(entry.chunk_id(), logical_length);
                assert_eq!(
                    filter.probe_for_exact_lookup(entry.chunk_id(), logical_length),
                    BloomLookupHint::RequiresExactLookup,
                    "ASSERT: inserting an Exact Run key cannot produce a Bloom false negative"
                );
            }
        })?;
        Ok((descriptor, membership.map(Arc::new)))
    }

    fn audit_opened_run(
        &self,
        name: &str,
        envelope: &OpenedRunEnvelope,
        mut visit: impl FnMut(&ExactIndexEntry),
    ) -> Result<(), ExactIndexStoreError> {
        let descriptor = envelope.descriptor;
        let mut audit = descriptor.begin_hash_audit();
        audit.update(0, &envelope.header)?;
        for page_ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(page_ordinal)
                .expect("ASSERT: descriptor page ordinal was prevalidated");
            let bytes = self
                .storage
                .read_exact_at(name, offset, EXACT_INDEX_PAGE_BYTES)?;
            let page = descriptor.decode_page(page_ordinal, &bytes)?;
            audit.verify_page(&page)?;
            for entry in page.entries() {
                visit(entry);
            }
            audit.update(offset, &bytes)?;
        }
        audit.update(envelope.footer_offset, &envelope.footer)?;
        audit.finish()?;
        Ok(())
    }

    fn merge_compaction_inputs<F>(
        &self,
        inputs: &[ExactIndexRunRef],
        mut emit: F,
    ) -> Result<CompactionSummary, ExactIndexStoreError>
    where
        F: FnMut(ExactIndexEntry) -> Result<(), ExactIndexStoreError>,
    {
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for run_ref in inputs.iter().copied() {
            sources.push(CompactionSource::open(self, run_ref)?);
        }
        let mut heap = BinaryHeap::new();
        heap.try_reserve(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for (source_ordinal, source) in sources.iter().enumerate() {
            heap.push(CompactionHeapEntry::new(
                source.current(),
                source.generation,
                source_ordinal,
            ));
        }

        let mut summary = CompactionSummary::default();
        let mut previous_location_key = None;
        let mut previous_output = None;
        while let Some(candidate) = heap.pop() {
            let source = &mut sources[candidate.source_ordinal];
            assert_eq!(
                source.current(),
                candidate.entry,
                "ASSERT: compaction heap entry must equal its source cursor"
            );
            let location_key = compaction_location_key(candidate.entry);
            if previous_location_key != Some(location_key) {
                if let Some(previous) = previous_output {
                    verify_compaction_output_pair(previous, candidate.entry)?;
                }
                emit(candidate.entry)?;
                summary.observe(candidate.entry)?;
                previous_output = Some(candidate.entry);
                previous_location_key = Some(location_key);
            }
            source.advance()?;
            if let Some(next) = source.current_optional() {
                heap.push(CompactionHeapEntry::new(
                    next,
                    source.generation,
                    candidate.source_ordinal,
                ));
            }
        }
        if sources.iter().any(|source| !source.finished) {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        summary.finish()
    }

    fn merge_compaction_families<F>(
        &self,
        inputs: &[CompactionInputFamily],
        mut emit: F,
    ) -> Result<CompactionSummary, ExactIndexStoreError>
    where
        F: FnMut(ExactIndexEntry) -> Result<(), ExactIndexStoreError>,
    {
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for family in inputs {
            sources.push(CompactionFamilySource::open(self, family)?);
        }
        let mut heap = BinaryHeap::new();
        heap.try_reserve(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for (source_ordinal, source) in sources.iter().enumerate() {
            heap.push(CompactionHeapEntry::new(
                source.current(),
                source.family_generation,
                source_ordinal,
            ));
        }

        let mut summary = CompactionSummary::default();
        let mut previous_location_key = None;
        let mut previous_output = None;
        while let Some(candidate) = heap.pop() {
            let source = &mut sources[candidate.source_ordinal];
            assert_eq!(
                source.current(),
                candidate.entry,
                "ASSERT: family compaction heap entry must equal its source cursor"
            );
            let location_key = compaction_location_key(candidate.entry);
            if previous_location_key != Some(location_key) {
                if let Some(previous) = previous_output {
                    verify_compaction_output_pair(previous, candidate.entry)?;
                }
                emit(candidate.entry)?;
                summary.observe(candidate.entry)?;
                previous_output = Some(candidate.entry);
                previous_location_key = Some(location_key);
            }
            source.advance(self)?;
            if let Some(next) = source.current_optional() {
                heap.push(CompactionHeapEntry::new(
                    next,
                    source.family_generation,
                    candidate.source_ordinal,
                ));
            }
        }
        if sources.iter().any(|source| !source.finished()) {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        summary.finish()
    }

    fn compaction_partition_summaries(
        &self,
        inputs: &[CompactionInputFamily],
    ) -> Result<Vec<CompactionSummary>, ExactIndexStoreError> {
        let mut summaries = Vec::new();
        let mut current = CompactionSummary::default();
        let global = self.merge_compaction_families(inputs, |entry| {
            if current.entry_count >= EXACT_INDEX_RUN_PARTITION_TARGET_ENTRIES
                && current.maximum_chunk_id != Some(entry.chunk_id())
            {
                summaries
                    .try_reserve(1)
                    .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
                summaries.push(std::mem::take(&mut current).finish()?);
            }
            current.observe(entry)
        })?;
        summaries
            .try_reserve(1)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        summaries.push(current.finish()?);
        let observed_count = summaries.iter().try_fold(0_usize, |total, summary| {
            total
                .checked_add(summary.entry_count)
                .ok_or(ExactIndexStoreError::OutOfMemory)
        })?;
        if observed_count != global.entry_count
            || summaries
                .first()
                .and_then(|summary| summary.minimum_chunk_id)
                != global.minimum_chunk_id
            || summaries
                .last()
                .and_then(|summary| summary.maximum_chunk_id)
                != global.maximum_chunk_id
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(summaries)
    }

    fn publish_streamed_family(
        &self,
        inputs: &[CompactionInputFamily],
        profile: ExactIndexProfileId,
        first_generation: u64,
        summaries: &[CompactionSummary],
    ) -> Result<Vec<ExactIndexRunDescriptor>, ExactIndexStoreError> {
        let first_summary = summaries
            .first()
            .copied()
            .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
        let mut output = Some(StreamedPartitionOutput::new(
            self,
            profile,
            first_generation,
            first_summary,
        )?);
        let mut partition_ordinal = 0_usize;
        let mut emitted_in_partition = 0_usize;
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(summaries.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let observed = self.merge_compaction_families(inputs, |entry| {
            if emitted_in_partition == summaries[partition_ordinal].entry_count {
                descriptors.push(
                    output
                        .take()
                        .expect("ASSERT: every family partition owns one active writer")
                        .finish(self)?,
                );
                partition_ordinal = partition_ordinal
                    .checked_add(1)
                    .ok_or(ExactIndexStoreError::DependencyMismatch)?;
                let summary = summaries
                    .get(partition_ordinal)
                    .copied()
                    .ok_or(ExactIndexStoreError::DependencyMismatch)?;
                let generation = first_generation
                    .checked_add(
                        u64::try_from(partition_ordinal)
                            .map_err(|_| ExactIndexStoreError::TooManyRunPartitions)?,
                    )
                    .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
                output = Some(StreamedPartitionOutput::new(
                    self, profile, generation, summary,
                )?);
                emitted_in_partition = 0;
            }
            output
                .as_mut()
                .expect("ASSERT: active family partition writer exists")
                .push(&self.storage, entry)?;
            emitted_in_partition = emitted_in_partition
                .checked_add(1)
                .ok_or(ExactIndexStoreError::DependencyMismatch)?;
            Ok(())
        })?;
        let expected_entries = summaries.iter().try_fold(0_usize, |total, summary| {
            total
                .checked_add(summary.entry_count)
                .ok_or(ExactIndexStoreError::OutOfMemory)
        })?;
        if observed.entry_count != expected_entries
            || emitted_in_partition != summaries[partition_ordinal].entry_count
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        descriptors.push(
            output
                .take()
                .expect("ASSERT: final family partition writer exists")
                .finish(self)?,
        );
        if descriptors.len() != summaries.len() {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        self.storage.sync_root()?;
        Ok(descriptors)
    }

    fn publish_streamed_compaction(
        &self,
        inputs: &[ExactIndexRunRef],
        profile: ExactIndexProfileId,
        generation: u64,
        expected_summary: CompactionSummary,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let mut encoder = ExactIndexRunStreamEncoder::new(
            profile,
            generation,
            expected_summary.entry_count,
            expected_summary
                .minimum_chunk_id
                .ok_or(ExactIndexStoreError::InvalidCompactionInput)?,
            expected_summary
                .maximum_chunk_id
                .ok_or(ExactIndexStoreError::InvalidCompactionInput)?,
        )?;
        let temporary_name = temporary_name(profile, generation);
        let published_name = published_name(profile, generation);
        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        self.storage
            .write_at(&temporary_name, 0, encoder.header())?;

        let mut page_entries = Vec::new();
        page_entries
            .try_reserve_exact(31)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut page_ordinal = 0_usize;
        let observed_summary = self.merge_compaction_inputs(inputs, |entry| {
            page_entries.push(entry);
            if page_entries.len() == 31 {
                write_streamed_page(
                    &self.storage,
                    &temporary_name,
                    &mut encoder,
                    page_ordinal,
                    &page_entries,
                )?;
                page_entries.clear();
                page_ordinal = page_ordinal
                    .checked_add(1)
                    .ok_or(ExactIndexStoreError::DependencyMismatch)?;
            }
            Ok(())
        })?;
        if !page_entries.is_empty() {
            write_streamed_page(
                &self.storage,
                &temporary_name,
                &mut encoder,
                page_ordinal,
                &page_entries,
            )?;
        }
        if observed_summary != expected_summary {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let (footer, expected) = encoder.finish()?;
        let footer_offset = u64::try_from(expected.file_length() - EXACT_INDEX_PAGE_BYTES)
            .map_err(|_| ExactIndexStoreError::DependencyMismatch)?;
        self.storage
            .write_at(&temporary_name, footer_offset, &footer)?;
        self.storage.set_len(
            &temporary_name,
            u64::try_from(expected.file_length())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?,
        )?;
        let observed = self.audit_named(&temporary_name)?;
        verify_expected_descriptor(expected, observed)?;
        self.storage.sync_file(&temporary_name)?;
        if self.storage.exists(&published_name)? {
            let raced = self.audit_named(&published_name)?;
            verify_expected_descriptor(expected, raced)?;
        } else {
            match self
                .storage
                .publish_noreplace(&temporary_name, &published_name)
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let raced = self.audit_named(&published_name)?;
                    verify_expected_descriptor(expected, raced)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.storage.sync_root()?;
        Ok(observed)
    }

    fn verify_run_set_dependencies(
        &self,
        run_set: &ExactIndexRunSet,
    ) -> Result<Vec<ExactIndexRunReader<I>>, ExactIndexStoreError> {
        if run_set.family_count() > MAX_ACTIVE_EXACT_INDEX_FAMILIES {
            return Err(ExactIndexStoreError::TooManyActiveRuns);
        }
        let mut readers = Vec::new();
        readers
            .try_reserve_exact(run_set.runs().len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut membership_bytes_remaining = self.membership_budget_bytes_now();
        for run_ref in run_set.runs().iter().copied() {
            let name = published_name(run_ref.profile(), run_ref.generation());
            let (descriptor, membership) =
                self.audit_named_with_membership(&name, membership_bytes_remaining)?;
            verify_requested_identity(run_ref.profile(), run_ref.generation(), descriptor)?;
            verify_run_reference(run_ref, descriptor)?;
            if let Some(filter) = &membership {
                membership_bytes_remaining = membership_bytes_remaining
                    .checked_sub(filter.allocated_bytes())
                    .expect("ASSERT: admitted Run membership fits its remaining budget");
            }
            readers.push(ExactIndexRunReader {
                storage: self.storage.clone(),
                name,
                descriptor,
                page_cache: Arc::clone(&self.page_cache),
                membership,
                membership_counters: Arc::clone(&self.membership_counters),
            });
        }
        Ok(readers)
    }

    fn membership_budget_bytes_now(&self) -> usize {
        let snapshot = self.fixed_membership_snapshot.unwrap_or_else(|| {
            MemoryPressureSnapshot::read_system()
                .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1))
        });
        exact_run_membership_budget(snapshot)
    }

    fn publish_run_set(
        &self,
        run_set_id: ExactIndexRunSetId,
        encoded: &[u8],
    ) -> Result<(), ExactIndexStoreError> {
        let published_name = run_set_name(run_set_id);
        if self.storage.exists(&published_name)? {
            let observed = self.read_run_set(run_set_id)?;
            if observed.id()? != run_set_id {
                return Err(ExactIndexStoreError::PublishVerificationMismatch);
            }
            self.storage.sync_root()?;
            return Ok(());
        }
        let temporary_name = format!(".{published_name}.building");
        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        for (ordinal, page) in encoded.chunks(EXACT_INDEX_PAGE_BYTES).enumerate() {
            let offset = ordinal
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .and_then(|value| u64::try_from(value).ok())
                .expect("ASSERT: a Metadata-v1 object offset fits u64");
            self.storage.write_at(&temporary_name, offset, page)?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len()).expect("ASSERT: Metadata-v1 length fits u64"),
        )?;
        let reread = self.storage.read(&temporary_name)?;
        if reread != encoded || ExactIndexRunSetId::from_encoded(&reread)? != run_set_id {
            return Err(ExactIndexStoreError::PublishVerificationMismatch);
        }
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let observed = self.read_run_set(run_set_id)?;
                if observed.id()? != run_set_id {
                    return Err(ExactIndexStoreError::PublishVerificationMismatch);
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(())
    }

    fn read_run_set(
        &self,
        run_set_id: ExactIndexRunSetId,
    ) -> Result<ExactIndexRunSet, ExactIndexStoreError> {
        let name = run_set_name(run_set_id);
        let length = self.storage.object_len(&name)?;
        if length > u64::try_from(MAX_METADATA_OBJECT_BYTES).expect("ASSERT: 16 MiB fits u64") {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let encoded = self.storage.read(&name)?;
        let run_set = ExactIndexRunSet::decode(&encoded)?;
        if ExactIndexRunSetId::from_encoded(&encoded)? != run_set_id {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(run_set)
    }
}

#[derive(Clone, Debug)]
pub struct ActivatedExactIndex<I> {
    record: ExactIndexActivationRecord,
    run_set: ExactIndexRunSet,
    readers: Vec<ExactIndexRunReader<I>>,
    lookup_families: Vec<ExactIndexLookupFamily>,
}

#[derive(Clone, Debug)]
struct ExactIndexLookupFamily {
    level: u16,
    family_generation: u64,
    reader_indices: Vec<usize>,
}

impl<I> ActivatedExactIndex<I> {
    fn new(
        record: ExactIndexActivationRecord,
        run_set: ExactIndexRunSet,
        readers: Vec<ExactIndexRunReader<I>>,
    ) -> Result<Self, ExactIndexStoreError> {
        if readers.len() != run_set.runs().len()
            || run_set.family_count() > MAX_ACTIVE_EXACT_INDEX_FAMILIES
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let mut lookup_families: Vec<ExactIndexLookupFamily> = Vec::new();
        lookup_families
            .try_reserve_exact(run_set.family_count())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for (reader_index, run_ref) in run_set.runs().iter().copied().enumerate() {
            if let Some(family) = lookup_families.iter_mut().find(|family| {
                family.level == run_ref.level()
                    && family.family_generation == run_ref.family_generation()
            }) {
                family.reader_indices.push(reader_index);
            } else {
                let mut reader_indices = Vec::new();
                reader_indices
                    .try_reserve_exact(usize::from(run_ref.partition_count()))
                    .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
                reader_indices.push(reader_index);
                lookup_families.push(ExactIndexLookupFamily {
                    level: run_ref.level(),
                    family_generation: run_ref.family_generation(),
                    reader_indices,
                });
            }
        }
        for family in &mut lookup_families {
            family
                .reader_indices
                .sort_unstable_by_key(|index| run_set.runs()[*index].partition_ordinal());
        }
        lookup_families.sort_unstable_by_key(|family| {
            (Reverse(family.family_generation), Reverse(family.level))
        });
        if lookup_families.len() != run_set.family_count() {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(Self {
            record,
            run_set,
            readers,
            lookup_families,
        })
    }

    #[must_use]
    pub const fn record(&self) -> ExactIndexActivationRecord {
        self.record
    }

    #[must_use]
    pub const fn run_set(&self) -> &ExactIndexRunSet {
        &self.run_set
    }

    #[must_use]
    pub fn run_count(&self) -> usize {
        self.readers.len()
    }

    #[must_use]
    pub fn family_count(&self) -> usize {
        self.lookup_families.len()
    }

    /// Returns current filter residency and process-lifetime probe evidence.
    ///
    /// # Panics
    ///
    /// Panics if the bounded active reader set violates shared-counter or
    /// memory-accounting invariants.
    #[must_use]
    pub fn membership_status(&self) -> ExactRunMembershipStatus {
        let filter_count = self
            .readers
            .iter()
            .filter(|reader| reader.membership.is_some())
            .count();
        let allocated_bytes = self.readers.iter().fold(0_usize, |total, reader| {
            total
                .checked_add(
                    reader
                        .membership
                        .as_ref()
                        .map_or(0, |filter| filter.allocated_bytes()),
                )
                .expect("ASSERT: active Run membership byte accounting cannot overflow")
        });
        let Some(counters) = self
            .readers
            .first()
            .map(|reader| &reader.membership_counters)
        else {
            return ExactRunMembershipStatus::default();
        };
        assert!(
            self.readers
                .iter()
                .all(|reader| Arc::ptr_eq(&reader.membership_counters, counters)),
            "ASSERT: one active Exact Index shares one membership counter set"
        );
        ExactRunMembershipStatus {
            filter_count: u64::try_from(filter_count)
                .expect("ASSERT: active membership filter count fits u64"),
            allocated_bytes: u64::try_from(allocated_bytes)
                .expect("ASSERT: active membership bytes fit u64"),
            probes: counters.probes.load(AtomicOrdering::Relaxed),
            definitely_absent: counters.definitely_absent.load(AtomicOrdering::Relaxed),
            requires_exact_lookup: counters.requires_exact_lookup.load(AtomicOrdering::Relaxed),
        }
    }
}

impl<I: StorageIo> ActivatedExactIndex<I> {
    /// Checks whether one unpublished ACTIVE overlay Location remains
    /// selectable in this generation.
    ///
    /// A newer RETIRING/REMOVED transition for the same physical Location
    /// rejects the overlay. A location absent from a complete lookup is a new
    /// publication and remains selectable. An incomplete negative is rejected
    /// conservatively.
    ///
    /// # Errors
    ///
    /// Returns touched-page I/O, integrity, or bounded-allocation failures.
    pub fn permits_active_overlay(
        &self,
        candidate: ExactIndexEntry,
    ) -> Result<bool, ExactIndexStoreError> {
        if candidate.transition() != ExactLocationTransition::Active {
            return Ok(false);
        }
        let lookup = self.lookup_transitions(candidate.chunk_id(), candidate.logical_length())?;
        if let Some(current) = lookup
            .candidates()
            .iter()
            .find(|current| current.location() == candidate.location())
        {
            return Ok(current.transition() == ExactLocationTransition::Active);
        }
        Ok(lookup.complete())
    }

    /// Returns a newest-Run-first bounded transition prefix across the active
    /// Run Set. Callers must merge transitions by complete physical Location
    /// identity and verify any selected ACTIVE candidate against its Container.
    ///
    /// `complete=true` covers this Run Set only. It never makes a negative
    /// result authoritative for durable content.
    ///
    /// # Errors
    ///
    /// Returns touched-page I/O, integrity, or bounded-allocation failures.
    pub fn lookup_transitions(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexLookup, ExactIndexStoreError> {
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(MAX_EXACT_LOOKUP_CANDIDATES)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut complete = true;
        for family in &self.lookup_families {
            let partition_ordinal = family
                .reader_indices
                .partition_point(|index| self.run_set.runs()[*index].maximum_chunk_id() < chunk_id);
            let Some(&index) = family.reader_indices.get(partition_ordinal) else {
                continue;
            };
            let run_ref = self.run_set.runs()[index];
            if chunk_id < run_ref.minimum_chunk_id() {
                continue;
            }
            let lookup = self.readers[index].lookup(chunk_id, logical_length)?;
            complete &= lookup.complete();
            let remaining = MAX_EXACT_LOOKUP_CANDIDATES - candidates.len();
            if lookup.candidates().len() > remaining {
                candidates.extend_from_slice(&lookup.candidates()[..remaining]);
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            candidates.extend_from_slice(lookup.candidates());
            if candidates.len() == MAX_EXACT_LOOKUP_CANDIDATES {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
        }
        Ok(ExactIndexLookup {
            candidates,
            complete,
        })
    }
}

#[derive(Clone, Debug)]
struct OpenedRunEnvelope {
    descriptor: ExactIndexRunDescriptor,
    header: Vec<u8>,
    footer: Vec<u8>,
    footer_offset: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompactionSummary {
    entry_count: usize,
    minimum_chunk_id: Option<ChunkId>,
    maximum_chunk_id: Option<ChunkId>,
}

struct StreamedPartitionOutput {
    encoder: ExactIndexRunStreamEncoder,
    temporary_name: String,
    published_name: String,
    page_entries: Vec<ExactIndexEntry>,
    page_ordinal: usize,
}

impl StreamedPartitionOutput {
    fn new<I: Clone + StorageIo>(
        repository: &ExactIndexRunRepository<I>,
        profile: ExactIndexProfileId,
        generation: u64,
        summary: CompactionSummary,
    ) -> Result<Self, ExactIndexStoreError> {
        let encoder = ExactIndexRunStreamEncoder::new(
            profile,
            generation,
            summary.entry_count,
            summary
                .minimum_chunk_id
                .ok_or(ExactIndexStoreError::InvalidCompactionInput)?,
            summary
                .maximum_chunk_id
                .ok_or(ExactIndexStoreError::InvalidCompactionInput)?,
        )?;
        let temporary_name = temporary_name(profile, generation);
        let published_name = published_name(profile, generation);
        match repository.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        repository
            .storage
            .write_at(&temporary_name, 0, encoder.header())?;
        let mut page_entries = Vec::new();
        page_entries
            .try_reserve_exact(31)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        Ok(Self {
            encoder,
            temporary_name,
            published_name,
            page_entries,
            page_ordinal: 0,
        })
    }

    fn push<I: StorageIo>(
        &mut self,
        storage: &I,
        entry: ExactIndexEntry,
    ) -> Result<(), ExactIndexStoreError> {
        self.page_entries.push(entry);
        if self.page_entries.len() == 31 {
            write_streamed_page(
                storage,
                &self.temporary_name,
                &mut self.encoder,
                self.page_ordinal,
                &self.page_entries,
            )?;
            self.page_entries.clear();
            self.page_ordinal = self
                .page_ordinal
                .checked_add(1)
                .ok_or(ExactIndexStoreError::DependencyMismatch)?;
        }
        Ok(())
    }

    fn finish<I: Clone + StorageIo>(
        mut self,
        repository: &ExactIndexRunRepository<I>,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        if !self.page_entries.is_empty() {
            write_streamed_page(
                &repository.storage,
                &self.temporary_name,
                &mut self.encoder,
                self.page_ordinal,
                &self.page_entries,
            )?;
        }
        let (footer, expected) = self.encoder.finish()?;
        let footer_offset = u64::try_from(expected.file_length() - EXACT_INDEX_PAGE_BYTES)
            .map_err(|_| ExactIndexStoreError::DependencyMismatch)?;
        repository
            .storage
            .write_at(&self.temporary_name, footer_offset, &footer)?;
        repository.storage.set_len(
            &self.temporary_name,
            u64::try_from(expected.file_length())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?,
        )?;
        let observed = repository.audit_named(&self.temporary_name)?;
        verify_expected_descriptor(expected, observed)?;
        repository.storage.sync_file(&self.temporary_name)?;
        if repository.storage.exists(&self.published_name)? {
            let raced = repository.audit_named(&self.published_name)?;
            verify_expected_descriptor(expected, raced)?;
        } else {
            match repository
                .storage
                .publish_noreplace(&self.temporary_name, &self.published_name)
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let raced = repository.audit_named(&self.published_name)?;
                    verify_expected_descriptor(expected, raced)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(observed)
    }
}

impl CompactionSummary {
    fn observe(&mut self, entry: ExactIndexEntry) -> Result<(), ExactIndexStoreError> {
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or(ExactIndexStoreError::OutOfMemory)?;
        self.minimum_chunk_id.get_or_insert(entry.chunk_id());
        self.maximum_chunk_id = Some(entry.chunk_id());
        Ok(())
    }

    fn finish(self) -> Result<Self, ExactIndexStoreError> {
        if self.entry_count == 0
            || self.minimum_chunk_id.is_none()
            || self.maximum_chunk_id.is_none()
        {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompactionHeapEntry {
    entry: ExactIndexEntry,
    location_key: (ChunkId, u32, [u8; 16], u64, u32),
    source_generation: u64,
    source_ordinal: usize,
}

impl CompactionHeapEntry {
    fn new(entry: ExactIndexEntry, source_generation: u64, source_ordinal: usize) -> Self {
        Self {
            entry,
            location_key: compaction_location_key(entry),
            source_generation,
            source_ordinal,
        }
    }
}

impl Ord for CompactionHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .location_key
            .cmp(&self.location_key)
            .then_with(|| self.source_generation.cmp(&other.source_generation))
            .then_with(|| other.source_ordinal.cmp(&self.source_ordinal))
    }
}

impl PartialOrd for CompactionHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct CompactionSource<I> {
    storage: I,
    name: String,
    descriptor: ExactIndexRunDescriptor,
    generation: u64,
    footer: Vec<u8>,
    footer_offset: u64,
    audit: Option<ExactIndexRunHashAudit>,
    page: Option<ExactIndexPage>,
    page_entry_ordinal: usize,
    next_page_ordinal: usize,
    finished: bool,
}

struct CompactionFamilySource<I> {
    partitions: Vec<ExactIndexRunRef>,
    next_partition_ordinal: usize,
    current: CompactionSource<I>,
    family_generation: u64,
}

impl<I: Clone + StorageIo> CompactionFamilySource<I> {
    fn open(
        repository: &ExactIndexRunRepository<I>,
        family: &CompactionInputFamily,
    ) -> Result<Self, ExactIndexStoreError> {
        let first = family
            .refs
            .first()
            .copied()
            .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
        Ok(Self {
            partitions: family.refs.clone(),
            next_partition_ordinal: 1,
            current: CompactionSource::open(repository, first)?,
            family_generation: family.family_generation,
        })
    }

    fn current(&self) -> ExactIndexEntry {
        self.current.current()
    }

    fn current_optional(&self) -> Option<ExactIndexEntry> {
        self.current.current_optional()
    }

    fn advance(
        &mut self,
        repository: &ExactIndexRunRepository<I>,
    ) -> Result<(), ExactIndexStoreError> {
        self.current.advance()?;
        if self.current.finished && self.next_partition_ordinal < self.partitions.len() {
            let next = self.partitions[self.next_partition_ordinal];
            self.next_partition_ordinal = self
                .next_partition_ordinal
                .checked_add(1)
                .ok_or(ExactIndexStoreError::DependencyMismatch)?;
            self.current = CompactionSource::open(repository, next)?;
        }
        Ok(())
    }

    fn finished(&self) -> bool {
        self.current.finished && self.next_partition_ordinal == self.partitions.len()
    }
}

impl<I: Clone + StorageIo> CompactionSource<I> {
    fn open(
        repository: &ExactIndexRunRepository<I>,
        run_ref: ExactIndexRunRef,
    ) -> Result<Self, ExactIndexStoreError> {
        let name = published_name(run_ref.profile(), run_ref.generation());
        let envelope = repository.read_envelope(&name)?;
        verify_requested_identity(run_ref.profile(), run_ref.generation(), envelope.descriptor)?;
        verify_run_reference(run_ref, envelope.descriptor)?;
        let mut audit = envelope.descriptor.begin_hash_audit();
        audit.update(0, &envelope.header)?;
        let mut source = Self {
            storage: repository.storage.clone(),
            name,
            descriptor: envelope.descriptor,
            generation: run_ref.generation(),
            footer: envelope.footer,
            footer_offset: envelope.footer_offset,
            audit: Some(audit),
            page: None,
            page_entry_ordinal: 0,
            next_page_ordinal: 0,
            finished: false,
        };
        source.load_next_page()?;
        if source.page.is_none() {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        Ok(source)
    }

    fn current(&self) -> ExactIndexEntry {
        self.current_optional()
            .expect("ASSERT: active compaction source must expose one current entry")
    }

    fn current_optional(&self) -> Option<ExactIndexEntry> {
        self.page
            .as_ref()
            .and_then(|page| page.entries().get(self.page_entry_ordinal))
            .copied()
    }

    fn advance(&mut self) -> Result<(), ExactIndexStoreError> {
        let page = self
            .page
            .as_ref()
            .expect("ASSERT: only an active compaction source can advance");
        self.page_entry_ordinal = self
            .page_entry_ordinal
            .checked_add(1)
            .ok_or(ExactIndexStoreError::DependencyMismatch)?;
        if self.page_entry_ordinal < page.entries().len() {
            return Ok(());
        }
        self.load_next_page()
    }

    fn load_next_page(&mut self) -> Result<(), ExactIndexStoreError> {
        if self.next_page_ordinal == self.descriptor.page_count() {
            let mut audit = self
                .audit
                .take()
                .expect("ASSERT: a compaction source hash audit finishes exactly once");
            audit.update(self.footer_offset, &self.footer)?;
            audit.finish()?;
            self.page = None;
            self.finished = true;
            return Ok(());
        }
        let page_ordinal = self.next_page_ordinal;
        let offset = self
            .descriptor
            .page_offset(page_ordinal)
            .expect("ASSERT: verified compaction page ordinal is in range");
        let bytes = self
            .storage
            .read_exact_at(&self.name, offset, EXACT_INDEX_PAGE_BYTES)?;
        let page = self.descriptor.decode_page(page_ordinal, &bytes)?;
        let audit = self
            .audit
            .as_mut()
            .expect("ASSERT: active compaction source retains its hash audit");
        audit.verify_page(&page)?;
        audit.update(offset, &bytes)?;
        self.page = Some(page);
        self.page_entry_ordinal = 0;
        self.next_page_ordinal = self
            .next_page_ordinal
            .checked_add(1)
            .ok_or(ExactIndexStoreError::DependencyMismatch)?;
        Ok(())
    }
}

fn verify_compaction_output_pair(
    previous: ExactIndexEntry,
    next: ExactIndexEntry,
) -> Result<(), ExactIndexStoreError> {
    if previous.chunk_id() == next.chunk_id() && previous.logical_length() != next.logical_length()
    {
        return Err(ExactIndexFormatError::ChunkLengthConflict.into());
    }
    if compaction_location_key(previous) >= compaction_location_key(next) {
        return Err(ExactIndexFormatError::NonCanonicalOrder.into());
    }
    Ok(())
}

fn select_level_zero_compaction(runs: &[ExactIndexRunRef]) -> Option<(u16, Vec<ExactIndexRunRef>)> {
    let mut by_level = BTreeMap::<u16, BTreeMap<u64, Vec<ExactIndexRunRef>>>::new();
    for run in runs.iter().copied() {
        by_level
            .entry(run.level())
            .or_default()
            .entry(run.family_generation())
            .or_default()
            .push(run);
    }
    for (level, families) in by_level {
        if families.len() < EXACT_INDEX_COMPACTION_FANIN {
            continue;
        }
        let mut candidates = Vec::new();
        for (_, mut family) in families.into_iter().take(EXACT_INDEX_COMPACTION_FANIN) {
            family.sort_unstable_by_key(|run| run.partition_ordinal());
            candidates.extend(family);
        }
        return Some((level, candidates));
    }
    None
}

fn compaction_families_from_run_set(
    run_set: &ExactIndexRunSet,
) -> Result<Vec<CompactionInputFamily>, ExactIndexStoreError> {
    let mut grouped = BTreeMap::<(u16, u64), Vec<ExactIndexRunRef>>::new();
    for run in run_set.runs().iter().copied() {
        grouped
            .entry((run.level(), run.family_generation()))
            .or_default()
            .push(run);
    }
    let mut families = Vec::new();
    families
        .try_reserve_exact(grouped.len())
        .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
    for ((_, family_generation), mut refs) in grouped {
        refs.sort_unstable_by_key(|run| run.partition_ordinal());
        families.push(CompactionInputFamily {
            refs,
            family_generation,
        });
    }
    Ok(families)
}

fn validate_family_compaction_inputs(
    inputs: &[ExactIndexRunRef],
    target_level: u16,
    first_generation: u64,
) -> Result<Vec<CompactionInputFamily>, ExactIndexStoreError> {
    let first = inputs
        .first()
        .copied()
        .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
    let source_level = target_level
        .checked_sub(1)
        .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
    if first.level() != source_level {
        return Err(ExactIndexStoreError::InvalidCompactionInput);
    }
    let canonical = ExactIndexRunSet::new(first.profile(), 1, inputs.to_vec())?;
    if canonical.family_count() < 2
        || canonical.family_count() > MAX_ACTIVE_EXACT_INDEX_FAMILIES
        || canonical
            .runs()
            .iter()
            .any(|run| run.level() != source_level)
        || canonical
            .runs()
            .iter()
            .any(|run| first_generation <= run.generation())
    {
        return Err(ExactIndexStoreError::InvalidCompactionInput);
    }

    let mut ordered = canonical.runs().to_vec();
    ordered.sort_unstable_by_key(|run| {
        (
            run.family_generation(),
            run.partition_ordinal(),
            run.generation(),
        )
    });
    let mut families: Vec<CompactionInputFamily> = Vec::new();
    families
        .try_reserve_exact(canonical.family_count())
        .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
    for run in ordered {
        if let Some(family) = families
            .last_mut()
            .filter(|family| family.family_generation == run.family_generation())
        {
            family.refs.push(run);
        } else {
            let mut refs = Vec::new();
            refs.try_reserve_exact(usize::from(run.partition_count()))
                .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
            refs.push(run);
            families.push(CompactionInputFamily {
                refs,
                family_generation: run.family_generation(),
            });
        }
    }
    if families.len() != canonical.family_count() {
        return Err(ExactIndexStoreError::DependencyMismatch);
    }
    Ok(families)
}

fn write_streamed_page<I: StorageIo>(
    storage: &I,
    temporary_name: &str,
    encoder: &mut ExactIndexRunStreamEncoder,
    page_ordinal: usize,
    entries: &[ExactIndexEntry],
) -> Result<(), ExactIndexStoreError> {
    let page = encoder.encode_next_page(entries)?;
    let offset = EXACT_INDEX_HEADER_BYTES
        .checked_add(
            page_ordinal
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .ok_or(ExactIndexStoreError::DependencyMismatch)?,
        )
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(ExactIndexStoreError::DependencyMismatch)?;
    storage.write_at(temporary_name, offset, &page)?;
    Ok(())
}

const _: () = assert!(std::mem::align_of::<ExactIndexPageCacheSlot>() == 64);

#[derive(Clone, Debug)]
struct CachedExactIndexPage {
    run_hash: [u8; 32],
    page_ordinal: usize,
    page: Arc<ExactIndexPage>,
}

#[repr(align(64))]
#[derive(Debug)]
struct ExactIndexPageCacheSlot {
    page: Mutex<Option<CachedExactIndexPage>>,
}

impl Default for ExactIndexPageCacheSlot {
    fn default() -> Self {
        Self {
            page: Mutex::new(None),
        }
    }
}

/// Repository-wide, pressure-bounded cache of independently verified 4-KiB
/// Exact-Index pages.
///
/// Direct mapping keeps lookup allocation-free and bounds both pointer chasing
/// and replacement work. A collision merely evicts another acceleration entry;
/// it cannot affect Exact-Index or DATA correctness.
struct ExactIndexPageCache {
    slots: Box<[ExactIndexPageCacheSlot]>,
    admission: Mutex<()>,
    target_pages: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    resident_pages: AtomicU64,
    evictions: AtomicU64,
    pressure_rejections: AtomicU64,
    effective_limit_bytes: AtomicU64,
    available_bytes: AtomicU64,
    swap_used_bytes: AtomicU64,
    automatic_pressure: bool,
    started: Instant,
    last_refresh_millis: AtomicU64,
}

impl fmt::Debug for ExactIndexPageCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactIndexPageCache")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ExactIndexPageCache {
    fn build(snapshot: MemoryPressureSnapshot, automatic_pressure: bool) -> Self {
        let slot_count = exact_page_cache_capacity(snapshot);
        let slots = std::iter::repeat_with(ExactIndexPageCacheSlot::default)
            .take(slot_count)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        assert_eq!(
            slots.len(),
            slot_count,
            "ASSERT: Exact Index page cache construction is complete"
        );
        assert!(
            slots.len().is_power_of_two(),
            "ASSERT: Exact Index page-cache geometry is direct-map compatible"
        );
        let cache = Self {
            slots,
            admission: Mutex::new(()),
            target_pages: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            resident_pages: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            pressure_rejections: AtomicU64::new(0),
            effective_limit_bytes: AtomicU64::new(snapshot.effective_limit_bytes()),
            available_bytes: AtomicU64::new(snapshot.available_bytes()),
            swap_used_bytes: AtomicU64::new(snapshot.swap_used_bytes()),
            automatic_pressure,
            started: Instant::now(),
            last_refresh_millis: AtomicU64::new(0),
        };
        cache.apply_pressure_snapshot(snapshot);
        cache
    }

    fn get(&self, run_hash: [u8; 32], page_ordinal: usize) -> Option<Arc<ExactIndexPage>> {
        self.refresh_pressure_if_due();
        let slot = &self.slots[exact_page_cache_slot(run_hash, page_ordinal, self.slots.len())];
        let cached = slot
            .page
            .lock()
            .expect("ASSERT: Exact Index page-cache slot lock poisoned");
        let found = cached
            .as_ref()
            .filter(|cached| cached.run_hash == run_hash && cached.page_ordinal == page_ordinal)
            .map(|cached| Arc::clone(&cached.page));
        if found.is_some() {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.misses.fetch_add(1, AtomicOrdering::Relaxed);
        }
        found
    }

    fn insert(&self, run_hash: [u8; 32], page_ordinal: usize, page: Arc<ExactIndexPage>) {
        assert_eq!(
            page.ordinal(),
            page_ordinal,
            "ASSERT: an Exact Index page-cache key matches the verified page ordinal"
        );
        self.refresh_pressure_if_due();
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: Exact Index page-cache admission lock poisoned");
        let target = self.target_pages.load(AtomicOrdering::Acquire);
        if target == 0 {
            self.pressure_rejections
                .fetch_add(1, AtomicOrdering::Relaxed);
            return;
        }
        let slot = &self.slots[exact_page_cache_slot(run_hash, page_ordinal, self.slots.len())];
        let mut cached = slot
            .page
            .lock()
            .expect("ASSERT: Exact Index page-cache slot lock poisoned");
        match cached.as_ref() {
            None => {
                if self.resident_pages.load(AtomicOrdering::Acquire) >= target {
                    self.pressure_rejections
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    return;
                }
                self.resident_pages.fetch_add(1, AtomicOrdering::Relaxed);
            }
            Some(previous)
                if previous.run_hash != run_hash || previous.page_ordinal != page_ordinal =>
            {
                self.evictions.fetch_add(1, AtomicOrdering::Relaxed);
            }
            Some(_) => {}
        }
        *cached = Some(CachedExactIndexPage {
            run_hash,
            page_ordinal,
            page,
        });
    }

    fn status(&self) -> ExactIndexPageCacheStatus {
        self.refresh_pressure_if_due();
        let effective_limit_bytes = self.effective_limit_bytes.load(AtomicOrdering::Relaxed);
        ExactIndexPageCacheStatus {
            hits: self.hits.load(AtomicOrdering::Relaxed),
            misses: self.misses.load(AtomicOrdering::Relaxed),
            resident_pages: self.resident_pages.load(AtomicOrdering::Relaxed),
            evictions: self.evictions.load(AtomicOrdering::Relaxed),
            pressure_rejections: self.pressure_rejections.load(AtomicOrdering::Relaxed),
            target_pages: self.target_pages.load(AtomicOrdering::Relaxed),
            capacity_pages: u64::try_from(self.slots.len())
                .expect("ASSERT: Exact Index page-cache capacity fits u64"),
            reserve_bytes: shared_cache_reserve_bytes(effective_limit_bytes),
            effective_limit_bytes,
            available_bytes: self.available_bytes.load(AtomicOrdering::Relaxed),
            swap_used_bytes: self.swap_used_bytes.load(AtomicOrdering::Relaxed),
        }
    }

    fn refresh_pressure_if_due(&self) {
        if !self.automatic_pressure {
            return;
        }
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let interval = u64::try_from(SYSTEM_REFRESH_INTERVAL.as_millis())
            .expect("ASSERT: memory refresh interval fits u64 milliseconds");
        let previous = self.last_refresh_millis.load(AtomicOrdering::Relaxed);
        if elapsed.saturating_sub(previous) < interval
            || self
                .last_refresh_millis
                .compare_exchange(
                    previous,
                    elapsed,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        self.apply_pressure_snapshot(snapshot);
    }

    fn apply_pressure_snapshot(&self, snapshot: MemoryPressureSnapshot) {
        let reserve = shared_cache_reserve_bytes(snapshot.effective_limit_bytes());
        let available_for_cache = snapshot.available_bytes().saturating_sub(reserve);
        let page_bytes = exact_page_cache_accounted_page_bytes();
        let capacity = u64::try_from(self.slots.len())
            .expect("ASSERT: Exact Index page-cache capacity fits u64");
        let target = if snapshot.swap_used_bytes() == 0 {
            (available_for_cache / page_bytes).min(capacity)
        } else {
            0
        };
        self.effective_limit_bytes
            .store(snapshot.effective_limit_bytes(), AtomicOrdering::Relaxed);
        self.available_bytes
            .store(snapshot.available_bytes(), AtomicOrdering::Relaxed);
        self.swap_used_bytes
            .store(snapshot.swap_used_bytes(), AtomicOrdering::Relaxed);
        self.target_pages.store(target, AtomicOrdering::Release);
        if self.resident_pages.load(AtomicOrdering::Acquire) > target {
            self.purge();
        }
    }

    fn purge(&self) {
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: Exact Index page-cache admission lock poisoned");
        let mut removed = 0_u64;
        for slot in &self.slots {
            if slot
                .page
                .lock()
                .expect("ASSERT: Exact Index page-cache slot lock poisoned")
                .take()
                .is_some()
            {
                removed = removed
                    .checked_add(1)
                    .expect("ASSERT: Exact Index cached-page count cannot overflow");
            }
        }
        let previous = self.resident_pages.swap(0, AtomicOrdering::AcqRel);
        assert_eq!(
            removed, previous,
            "ASSERT: Exact Index page-cache resident accounting matches its slots"
        );
        self.evictions.fetch_add(removed, AtomicOrdering::Relaxed);
    }
}

fn exact_run_membership_budget(snapshot: MemoryPressureSnapshot) -> usize {
    if snapshot.swap_used_bytes() != 0 || snapshot.effective_limit_bytes() == 0 {
        return 0;
    }
    let hard_limit = (snapshot.effective_limit_bytes() / EXACT_RUN_MEMBERSHIP_RAM_DIVISOR).clamp(
        EXACT_RUN_MEMBERSHIP_MINIMUM_BYTES,
        EXACT_RUN_MEMBERSHIP_MAXIMUM_BYTES,
    );
    let available = snapshot
        .available_bytes()
        .saturating_sub(shared_cache_reserve_bytes(snapshot.effective_limit_bytes()));
    usize::try_from(hard_limit.min(available)).unwrap_or(usize::MAX)
}

fn exact_page_cache_capacity(snapshot: MemoryPressureSnapshot) -> usize {
    if snapshot.effective_limit_bytes() == 0 {
        return EXACT_INDEX_PAGE_CACHE_FALLBACK_SLOTS;
    }
    let hard_bytes = (snapshot.effective_limit_bytes() / EXACT_INDEX_PAGE_CACHE_RAM_DIVISOR).clamp(
        EXACT_INDEX_PAGE_CACHE_MINIMUM_BYTES,
        EXACT_INDEX_PAGE_CACHE_MAXIMUM_BYTES,
    );
    let requested = usize::try_from(hard_bytes / exact_page_cache_accounted_page_bytes())
        .unwrap_or(usize::MAX)
        .max(1);
    floor_power_of_two(requested)
}

fn exact_page_cache_accounted_page_bytes() -> u64 {
    u64::try_from(EXACT_INDEX_PAGE_BYTES + size_of::<ExactIndexPageCacheSlot>())
        .expect("ASSERT: Exact Index accounted page bytes fit u64")
}

fn floor_power_of_two(value: usize) -> usize {
    let next = value
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX / 2 + 1);
    if next == value { value } else { next / 2 }
}

fn exact_page_cache_slot(run_hash: [u8; 32], page_ordinal: usize, slot_count: usize) -> usize {
    assert!(
        slot_count.is_power_of_two(),
        "ASSERT: Exact Index page-cache slot count is a power of two"
    );
    let mut lane = [0_u8; 8];
    lane.copy_from_slice(&run_hash[..8]);
    let page = u64::try_from(page_ordinal)
        .expect("ASSERT: an Exact Index page ordinal fits the cache hash domain");
    let mixed =
        u64::from_le_bytes(lane) ^ page.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ page.rotate_left(29);
    let mask =
        u64::try_from(slot_count - 1).expect("ASSERT: the Exact Index page-cache mask fits u64");
    usize::try_from(mixed & mask).expect("ASSERT: a masked Exact Index page-cache slot fits usize")
}

/// Open immutable run handle retaining only its verified envelope.
#[derive(Clone, Debug)]
pub struct ExactIndexRunReader<I> {
    storage: I,
    name: String,
    descriptor: ExactIndexRunDescriptor,
    page_cache: Arc<ExactIndexPageCache>,
    membership: Option<Arc<BlockedBloomHint>>,
    membership_counters: Arc<ExactRunMembershipCounters>,
}

impl<I: StorageIo> ExactIndexRunReader<I> {
    /// Returns a bounded prefix of Location candidates for one exact key.
    ///
    /// `complete=false` means the key has more physical transitions than the
    /// hard candidate bound. Even `complete=true` is complete only for this
    /// immutable run; an Exact Index negative is never content authority.
    ///
    /// # Errors
    ///
    /// Returns exact-range I/O or touched-page integrity failures.
    ///
    /// # Panics
    ///
    /// Panics if a format-v1 logical length does not fit the host address
    /// space. Supported production targets have at least 32-bit `usize`.
    pub fn lookup(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexLookup, ExactIndexStoreError> {
        if let Some(membership) = &self.membership {
            self.membership_counters
                .probes
                .fetch_add(1, AtomicOrdering::Relaxed);
            let hint = membership.probe_for_exact_lookup(
                chunk_id,
                usize::try_from(logical_length).expect("ASSERT: Exact logical length fits usize"),
            );
            match hint {
                BloomLookupHint::DefinitelyAbsent => {
                    self.membership_counters
                        .definitely_absent
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(ExactIndexLookup {
                        candidates: Vec::new(),
                        complete: true,
                    });
                }
                BloomLookupHint::RequiresExactLookup => {
                    self.membership_counters
                        .requires_exact_lookup
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
        }
        let mut lower = 0;
        let mut upper = self.descriptor.page_count();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let page = self.read_page(middle)?;
            match page.position(chunk_id, logical_length) {
                ExactIndexPagePosition::After => lower = middle + 1,
                ExactIndexPagePosition::Before | ExactIndexPagePosition::Within => upper = middle,
            }
        }

        let mut candidates = Vec::new();
        let mut page_ordinal = lower;
        while page_ordinal < self.descriptor.page_count() {
            let page = self.read_page(page_ordinal)?;
            let matches = page.candidates(chunk_id, logical_length);
            if matches.is_empty() {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: true,
                });
            }
            let remaining = MAX_EXACT_LOOKUP_CANDIDATES - candidates.len();
            let accepted = matches.len().min(remaining);
            candidates
                .try_reserve_exact(accepted)
                .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
            candidates.extend_from_slice(&matches[..accepted]);
            let key_reaches_page_end = page.entries().last().is_some_and(|entry| {
                entry.chunk_id() == chunk_id && entry.logical_length() == logical_length
            });
            if matches.len() > remaining {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            if !key_reaches_page_end || page_ordinal + 1 == self.descriptor.page_count() {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: true,
                });
            }
            if candidates.len() == MAX_EXACT_LOOKUP_CANDIDATES {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            page_ordinal += 1;
        }
        Ok(ExactIndexLookup {
            candidates,
            complete: true,
        })
    }

    fn read_page(&self, page_ordinal: usize) -> Result<Arc<ExactIndexPage>, ExactIndexStoreError> {
        let run_hash = self.descriptor.run_hash();
        if let Some(page) = self.page_cache.get(run_hash, page_ordinal) {
            return Ok(page);
        }
        let offset = self
            .descriptor
            .page_offset(page_ordinal)
            .ok_or(ExactIndexFormatError::InvalidPage)?;
        let bytes = self
            .storage
            .read_exact_at(&self.name, offset, EXACT_INDEX_PAGE_BYTES)?;
        let page = Arc::new(self.descriptor.decode_page(page_ordinal, &bytes)?);
        self.page_cache
            .insert(run_hash, page_ordinal, Arc::clone(&page));
        Ok(page)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexLookup {
    candidates: Vec<ExactIndexEntry>,
    complete: bool,
}

impl ExactIndexLookup {
    #[must_use]
    pub fn candidates(&self) -> &[ExactIndexEntry] {
        &self.candidates
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

#[derive(Debug)]
pub enum ExactIndexStoreError {
    Io(io::Error),
    Container(StoreError),
    Format(ExactIndexFormatError),
    IdentityMismatch,
    PublishVerificationMismatch,
    OutOfMemory,
    Activation(ExactIndexActivationError),
    RunSet(ExactIndexRunSetError),
    ActivationWalCorrupt,
    DependencyMismatch,
    NonMonotonicRunSetGeneration,
    TooManyActiveRuns,
    TooManyRunPartitions,
    InvalidCompactionInput,
    InvalidLocationTransition,
    ActivationChanged,
    CounterOverflow,
    MembershipFalseNegative,
}

fn validate_level_zero_transitions<I: StorageIo>(
    previous: Option<&ActivatedExactIndex<I>>,
    entries: &[ExactIndexEntry],
) -> Result<(), ExactIndexStoreError> {
    let mut locations = BTreeMap::new();
    for entry in entries {
        let location_key = exact_location_identity(*entry);
        if locations.insert(location_key, entry.transition()).is_some() {
            return Err(ExactIndexStoreError::InvalidLocationTransition);
        }
        let Some(previous) = previous else {
            if entry.transition() != ExactLocationTransition::Active {
                return Err(ExactIndexStoreError::InvalidLocationTransition);
            }
            continue;
        };
        let lookup = previous.lookup_transitions(entry.chunk_id(), entry.logical_length())?;
        let current = lookup
            .candidates()
            .iter()
            .find(|current| current.location() == entry.location())
            .map(ExactIndexEntry::transition);
        if current.is_none() && !lookup.complete() {
            return Err(ExactIndexStoreError::InvalidLocationTransition);
        }
        if !valid_location_transition(current, entry.transition()) {
            return Err(ExactIndexStoreError::InvalidLocationTransition);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ExactLocationIdentity {
    chunk_id: ChunkId,
    logical_length: u32,
    container_id: [u8; 16],
    container_generation: u64,
    record_offset: u64,
    record_length: u32,
    chunk_ordinal: u32,
    decoded_offset: u32,
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
    codec_id: u16,
    dependency_id: [u8; 32],
}

fn exact_location_identity(entry: ExactIndexEntry) -> ExactLocationIdentity {
    let location = entry.location();
    ExactLocationIdentity {
        chunk_id: entry.chunk_id(),
        logical_length: entry.logical_length(),
        container_id: location.container_id().bytes(),
        container_generation: location.container_generation(),
        record_offset: location.record_offset(),
        record_length: location.record_length(),
        chunk_ordinal: location.chunk_ordinal(),
        decoded_offset: location.decoded_offset(),
        record_crc32c: location.record_crc32c(),
        record_decoded_length: location.record_decoded_length(),
        record_payload_length: location.record_payload_length(),
        codec_id: location.codec_id(),
        dependency_id: location.dependency_id(),
    }
}

const fn valid_location_transition(
    current: Option<ExactLocationTransition>,
    proposed: ExactLocationTransition,
) -> bool {
    matches!(
        (current, proposed),
        (
            None | Some(ExactLocationTransition::Active),
            ExactLocationTransition::Active
        ) | (
            Some(ExactLocationTransition::Active | ExactLocationTransition::Retiring),
            ExactLocationTransition::Retiring
        ) | (
            Some(ExactLocationTransition::Active | ExactLocationTransition::Quarantined),
            ExactLocationTransition::Quarantined
        ) | (
            Some(
                ExactLocationTransition::Retiring
                    | ExactLocationTransition::Quarantined
                    | ExactLocationTransition::Removed
            ),
            ExactLocationTransition::Removed
        )
    )
}

impl fmt::Display for ExactIndexStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactIndexStoreError {}

impl From<io::Error> for ExactIndexStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<StoreError> for ExactIndexStoreError {
    fn from(error: StoreError) -> Self {
        Self::Container(error)
    }
}

impl From<ExactIndexFormatError> for ExactIndexStoreError {
    fn from(error: ExactIndexFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<ExactIndexActivationError> for ExactIndexStoreError {
    fn from(error: ExactIndexActivationError) -> Self {
        Self::Activation(error)
    }
}

impl From<ExactIndexRunSetError> for ExactIndexStoreError {
    fn from(error: ExactIndexRunSetError) -> Self {
        Self::RunSet(error)
    }
}

fn map_activation_log_error(error: ExactActivationLogError) -> ExactIndexStoreError {
    match error {
        ExactActivationLogError::Io(error) => ExactIndexStoreError::Io(error),
        ExactActivationLogError::OutOfMemory => ExactIndexStoreError::OutOfMemory,
        ExactActivationLogError::PublishVerificationMismatch => {
            ExactIndexStoreError::PublishVerificationMismatch
        }
        ExactActivationLogError::SlotTooLarge
        | ExactActivationLogError::BrokenChain
        | ExactActivationLogError::DivergentSlots
        | ExactActivationLogError::NeedsRepair
        | ExactActivationLogError::EmptyAfterInitialization => {
            ExactIndexStoreError::ActivationWalCorrupt
        }
    }
}

fn descriptor_from_complete_bytes(
    bytes: &[u8],
) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
    let footer_offset = bytes
        .len()
        .checked_sub(EXACT_INDEX_PAGE_BYTES)
        .ok_or(ExactIndexFormatError::InvalidObjectLength(bytes.len()))?;
    Ok(ExactIndexRunDescriptor::decode(
        &bytes[..EXACT_INDEX_HEADER_BYTES],
        &bytes[footer_offset..],
        u64::try_from(bytes.len()).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    )?)
}

fn verify_expected_descriptor(
    expected: ExactIndexRunDescriptor,
    observed: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if expected.profile() != observed.profile()
        || expected.generation() != observed.generation()
        || expected.file_length() != observed.file_length()
        || expected.run_hash() != observed.run_hash()
    {
        return Err(ExactIndexStoreError::PublishVerificationMismatch);
    }
    Ok(())
}

fn verify_requested_identity(
    profile: ExactIndexProfileId,
    generation: u64,
    descriptor: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if descriptor.profile() != profile || descriptor.generation() != generation {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    Ok(())
}

fn verify_run_reference(
    run_ref: ExactIndexRunRef,
    descriptor: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if run_ref.profile() != descriptor.profile()
        || run_ref.generation() != descriptor.generation()
        || run_ref.run_hash() != descriptor.run_hash()
        || run_ref.file_length()
            != u64::try_from(descriptor.file_length())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?
        || run_ref.entry_count()
            != u64::try_from(descriptor.entry_count())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?
        || run_ref.minimum_chunk_id() != descriptor.minimum_chunk_id()
        || run_ref.maximum_chunk_id() != descriptor.maximum_chunk_id()
    {
        return Err(ExactIndexStoreError::DependencyMismatch);
    }
    Ok(())
}

fn compaction_location_key(entry: ExactIndexEntry) -> (ChunkId, u32, [u8; 16], u64, u32) {
    let location = entry.location();
    (
        entry.chunk_id(),
        entry.logical_length(),
        location.container_id().bytes(),
        location.record_offset(),
        location.chunk_ordinal(),
    )
}

fn temporary_name(profile: ExactIndexProfileId, generation: u64) -> String {
    format!(".{}.building", published_name(profile, generation))
}

fn published_name(profile: ExactIndexProfileId, generation: u64) -> String {
    format!("{}.{generation:016x}.fdx", encode_hex(profile.bytes()))
}

fn parse_run_name(name: &str) -> Result<Option<(ExactIndexProfileId, u64)>, ExactIndexStoreError> {
    if name.strip_suffix(".fdx").is_none() {
        return Ok(None);
    }
    if name.len() != 85 || name.as_bytes().get(64) != Some(&b'.') {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    let mut profile_bytes = [0_u8; 32];
    decode_hex_into(&name.as_bytes()[..64], &mut profile_bytes)?;
    let generation = u64::from_str_radix(&name[65..81], 16)
        .map_err(|_| ExactIndexStoreError::IdentityMismatch)?;
    if generation == 0 {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    let profile =
        ExactIndexProfileId::new(profile_bytes).ok_or(ExactIndexStoreError::IdentityMismatch)?;
    Ok(Some((profile, generation)))
}

fn decode_hex_into(encoded: &[u8], output: &mut [u8]) -> Result<(), ExactIndexStoreError> {
    if encoded.len() != output.len() * 2 {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    for (pair, byte) in encoded.chunks_exact(2).zip(output) {
        let high = decode_hex_nibble(pair[0]).ok_or(ExactIndexStoreError::IdentityMismatch)?;
        let low = decode_hex_nibble(pair[1]).ok_or(ExactIndexStoreError::IdentityMismatch)?;
        *byte = (high << 4) | low;
    }
    Ok(())
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn run_set_name(run_set_id: ExactIndexRunSetId) -> String {
    format!("{}.fdxset", encode_hex(run_set_id.bytes()))
}

fn encode_hex<const N: usize>(bytes: [u8; N]) -> String {
    let mut encoded = String::with_capacity(N * 2);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .expect("ASSERT: writing into an owned String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_page_cache_capacity_and_target_follow_live_headroom() {
        let gib = 1_024_u64 * 1_024 * 1_024;
        let healthy = MemoryPressureSnapshot::new(128 * gib, 96 * gib, 0);
        let cache = ExactIndexPageCache::build(healthy, false);
        let status = cache.status();

        assert_eq!(std::mem::align_of::<ExactIndexPageCacheSlot>(), 64);
        assert!(status.capacity_pages().is_power_of_two());
        assert!(status.capacity_pages() > 256);
        assert_eq!(status.target_pages(), status.capacity_pages());
        assert_eq!(status.reserve_bytes(), 32 * gib);

        cache.apply_pressure_snapshot(MemoryPressureSnapshot::new(128 * gib, 32 * gib, 0));
        assert_eq!(cache.status().target_pages(), 0);

        cache.apply_pressure_snapshot(MemoryPressureSnapshot::new(128 * gib, 96 * gib, 1));
        let swapped = cache.status();
        assert_eq!(swapped.target_pages(), 0);
        assert_eq!(swapped.swap_used_bytes(), 1);
    }

    #[test]
    fn exact_run_membership_budget_preserves_headroom_and_closes_on_swap() {
        let gib = 1_024_u64 * 1_024 * 1_024;

        assert_eq!(
            exact_run_membership_budget(MemoryPressureSnapshot::new(16 * gib, 12 * gib, 0)),
            512 * 1_024 * 1_024
        );
        assert_eq!(
            exact_run_membership_budget(MemoryPressureSnapshot::new(16 * gib, 4 * gib, 0)),
            0,
            "the shared 4-GiB reserve wins over optional membership hints"
        );
        assert_eq!(
            exact_run_membership_budget(MemoryPressureSnapshot::new(16 * gib, 12 * gib, 1)),
            0,
            "any observed Swap disables the next active filter set"
        );
    }
}
