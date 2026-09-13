use crate::immutable_write::{ImmutableWriteBuffer, write_image};
use crate::page_cache::ACCOUNTED_PAGE_BYTES;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};

use fastdup_format::{
    ChunkId, ContainerId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES,
    ExactIndexActivationError, ExactIndexActivationRecord, ExactIndexEntry, ExactIndexFormatError,
    ExactIndexPage, ExactIndexPagePosition, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunDescriptor, ExactIndexRunHashAudit, ExactIndexRunRef, ExactIndexRunSet,
    ExactIndexRunSetError, ExactIndexRunSetId, ExactIndexRunStreamEncoder, ExactLocationTransition,
    MAX_METADATA_OBJECT_BYTES,
};

use crate::exact_activation_log::{
    ActivationLogSnapshot, ExactActivationLog, ExactActivationLogError,
};
use crate::exact_index_read::ImmutableExactIndexRun;
use crate::read_cache::{MemoryPressureSnapshot, shared_cache_reserve_bytes};
use crate::reduction_filter::{BlockedBloomHint, BloomLookupHint};
use crate::{ContainerRepository, StorageIo, StoreError};

pub const MAX_EXACT_LOOKUP_CANDIDATES: usize = 64;
pub const MAX_ACTIVE_EXACT_INDEX_FAMILIES: usize = 64;
const EXACT_INDEX_COMPACTION_FANIN: usize = 4;
const EXACT_INDEX_PAGE_CACHE_FALLBACK_SLOTS: usize = 256;
const EXACT_INDEX_PAGE_CACHE_MINIMUM_BYTES: u64 = 1_024 * 1_024;
const EXACT_INDEX_PAGE_CACHE_MAXIMUM_BYTES: u64 = 256 * 1_024 * 1_024;
const EXACT_INDEX_PAGE_CACHE_RAM_DIVISOR: u64 = 128;
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
    // Required live writer state, bounded by one 64-record slot. This is not
    // evictable read acceleration. Taking it before I/O makes failure revoke it.
    activation_writer: Arc<Mutex<Option<ActivationLogSnapshot>>>,
    active_generation: Arc<RwLock<Option<Arc<ExactIndexGenerationState<I>>>>>,
    retired_generations: Arc<Mutex<Vec<Weak<ExactIndexGenerationState<I>>>>>,
    page_cache: Arc<ExactIndexPageCache>,
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
    leased_run_count: u64,
    positional_run_count: u64,
    leased_page_bounds_bytes: u64,
    filter_count: u64,
    allocated_bytes: u64,
    huge_page_advised_filter_count: u64,
    huge_page_advised_bytes: u64,
    probes: u64,
    definitely_absent: u64,
    requires_exact_lookup: u64,
}

impl ExactRunMembershipStatus {
    #[must_use]
    pub const fn leased_run_count(self) -> u64 {
        self.leased_run_count
    }

    #[must_use]
    pub const fn positional_run_count(self) -> u64 {
        self.positional_run_count
    }

    #[must_use]
    pub const fn leased_page_bounds_bytes(self) -> u64 {
        self.leased_page_bounds_bytes
    }

    #[must_use]
    pub const fn filter_count(self) -> u64 {
        self.filter_count
    }

    #[must_use]
    pub const fn allocated_bytes(self) -> u64 {
        self.allocated_bytes
    }

    #[must_use]
    pub const fn huge_page_advised_filter_count(self) -> u64 {
        self.huge_page_advised_filter_count
    }

    #[must_use]
    pub const fn huge_page_advised_bytes(self) -> u64 {
        self.huge_page_advised_bytes
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
            activation_writer: Arc::new(Mutex::new(None)),
            active_generation: Arc::new(RwLock::new(None)),
            retired_generations: Arc::new(Mutex::new(Vec::new())),
            page_cache: Arc::new(ExactIndexPageCache::build(snapshot, true)),
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
            activation_writer: Arc::new(Mutex::new(None)),
            active_generation: Arc::new(RwLock::new(None)),
            retired_generations: Arc::new(Mutex::new(Vec::new())),
            page_cache: Arc::new(ExactIndexPageCache::build(snapshot, false)),
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
        self.publish_run(run, false)
            .map(|(descriptor, _)| descriptor)
    }

    fn publish_run(
        &self,
        run: &ExactIndexRun,
        owned_writer: bool,
    ) -> Result<(ExactIndexRunDescriptor, Option<RunWriterEvidence>), ExactIndexStoreError> {
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
            return Ok((observed, None));
        }

        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let mut evidence = owned_writer
            .then(|| RunWriterEvidence::new(expected.entry_count(), &self.page_cache))
            .transpose()?;
        for (page_ordinal, page) in encoded.chunks(EXACT_INDEX_PAGE_BYTES).enumerate() {
            assert_eq!(
                page.len(),
                EXACT_INDEX_PAGE_BYTES,
                "ASSERT: Exact Index Run v1 always consists of complete 4-KiB pages"
            );
            if page_ordinal > 0
                && page_ordinal <= expected.page_count()
                && let Some(evidence) = &mut evidence
            {
                let decoded = expected.decode_page(page_ordinal - 1, page)?;
                evidence.observe(decoded.entries(), page)?;
            }
        }
        write_image(&self.storage, &temporary_name, &encoded)?;
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len())
                .expect("ASSERT: a bounded Exact Index run length fits u64"),
        )?;
        let observed = if owned_writer {
            expected
        } else {
            self.audit_named(&temporary_name)?
        };
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
                evidence = None;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok((observed, evidence))
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
            mapping: None,
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        self.compact_family_using(
            inputs,
            target_level,
            first_generation,
            &mut Vec::new(),
            false,
        )
    }

    fn compact_family_using(
        &self,
        inputs: &[ExactIndexRunRef],
        target_level: u16,
        first_generation: u64,
        readers: &mut Vec<ExactIndexRunReader<I>>,
        owned_writer: bool,
    ) -> Result<ExactIndexRunFamily, ExactIndexStoreError> {
        let input_families =
            validate_family_compaction_inputs(inputs, target_level, first_generation)?;
        let profile = input_families[0].refs[0].profile();
        let summaries = self.compaction_partition_summaries(&input_families, readers)?;
        let partition_count = u16::try_from(summaries.len())
            .map_err(|_| ExactIndexStoreError::TooManyRunPartitions)?;
        first_generation
            .checked_add(u64::from(partition_count) - 1)
            .ok_or(ExactIndexStoreError::NonMonotonicRunSetGeneration)?;
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index family compaction lock poisoned");
        let (descriptors, outputs) = self.publish_streamed_family(
            &input_families,
            profile,
            first_generation,
            &summaries,
            readers,
            owned_writer,
        )?;
        readers.extend(outputs);
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
        let _generation = self
            .generation_publish_lock
            .lock()
            .expect("ASSERT: Exact generation publication lock poisoned");
        self.activate_with_readers(run_set, &[], false)
    }

    fn activate_with_readers(
        &self,
        run_set: &ExactIndexRunSet,
        reusable: &[ExactIndexRunReader<I>],
        owned_writer: bool,
    ) -> Result<ActivatedExactIndex<I>, ExactIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index activation lock poisoned");
        let readers = self.verify_run_set_dependencies_with_readers(run_set, reusable)?;
        let encoded = run_set.encode()?;
        let run_set_id = ExactIndexRunSetId::from_encoded(&encoded)?;
        self.publish_run_set(run_set_id, &encoded, owned_writer)?;
        let log = ExactActivationLog::new(&self.storage);
        let mut writer = self
            .activation_writer
            .lock()
            .expect("ASSERT: Exact activation writer lock poisoned");
        let previous = writer.take();
        let snapshot = match previous.filter(|_| owned_writer && !crate::read_intent::independent())
        {
            Some(snapshot) => snapshot,
            None => log.load_for_append().map_err(map_activation_log_error)?,
        };
        if let Some(last) = snapshot.last_record() {
            if last.run_set_id() == run_set_id {
                if last.profile() != run_set.profile()
                    || last.run_set_generation() != run_set.generation()
                {
                    return Err(ExactIndexStoreError::DependencyMismatch);
                }
                log.sync_selected(&snapshot)
                    .map_err(map_activation_log_error)?;
                *writer = Some(snapshot);
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
        let next = log
            .append(&snapshot, record, !owned_writer)
            .map_err(map_activation_log_error)?;
        *writer = Some(next);
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
    ///
    /// # Panics
    ///
    /// Panics if the serialized activation writer lock is poisoned.
    pub fn recover_active(&self) -> Result<Option<ActivatedExactIndex<I>>, ExactIndexStoreError> {
        let _generation = self
            .generation_publish_lock
            .lock()
            .expect("ASSERT: Exact generation publication lock poisoned");
        self.recover_active_locked()
    }

    fn recover_active_locked(&self) -> Result<Option<ActivatedExactIndex<I>>, ExactIndexStoreError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let mut writer = self
            .activation_writer
            .lock()
            .expect("ASSERT: Exact activation writer lock poisoned");
        *writer = None;
        let log = ExactActivationLog::new(&self.storage);
        let Some(snapshot) = log.load_for_recovery().map_err(map_activation_log_error)? else {
            return Ok(None);
        };
        let Some(record) = snapshot.last_record() else {
            return Ok(None);
        };
        let active = self.open_activated_record(record)?;
        *writer = Some(snapshot);
        Ok(Some(active))
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
        let Some(active) = self.recover_active_locked()? else {
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
        let previous = self.recover_for_append()?;
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
        let previous = self.recover_for_append()?;
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
        let owned_writer = !crate::read_intent::independent();
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
        let (descriptor, evidence) = self.publish_run(&run, owned_writer)?;
        let mut readers = previous.map_or_else(Vec::new, |active| active.readers.clone());
        readers.push(self.reader_from_writer(descriptor, evidence)?);
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
            let compacted = self.compact_family_using(
                &inputs,
                target_level,
                first_output_generation,
                &mut readers,
                owned_writer,
            )?;
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
        let active = self.activate_with_readers(&run_set, &readers, owned_writer)?;
        Ok(self.install_active_generation(active))
    }

    /// The exclusive writer carries its synchronized selector across appends.
    /// Unknown or failed state is reconstructed before another mutation.
    fn recover_for_append(&self) -> Result<Option<ActivatedExactIndex<I>>, ExactIndexStoreError> {
        let log = ExactActivationLog::new(&self.storage);
        let mut writer = self
            .activation_writer
            .lock()
            .expect("ASSERT: Exact activation writer lock poisoned");
        if crate::read_intent::independent() {
            *writer = None;
        }
        let trusted = writer.is_some();
        let _independent =
            (!trusted).then(|| crate::ReadIntentScope::enter(crate::ReadIntent::Independent));
        let snapshot = match writer.as_ref() {
            Some(snapshot) => snapshot.clone(),
            None => log.load_for_append().map_err(map_activation_log_error)?,
        };
        let Some(record) = snapshot.last_record() else {
            *writer = Some(snapshot);
            return Ok(None);
        };
        if trusted
            && !crate::read_intent::independent()
            && let Some(pin) = self.pin_matching_generation(record)
            && pin.readers.iter().all(|reader| reader.mapping.is_some())
        {
            return ActivatedExactIndex::new(record, pin.run_set.clone(), pin.readers.clone())
                .map(Some);
        }
        let run_set = self.read_activated_run_set(record)?;
        let readers = self.verify_run_set_dependencies(&run_set)?;
        *writer = Some(snapshot);
        ActivatedExactIndex::new(record, run_set, readers).map(Some)
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let log = ExactActivationLog::new(&self.storage);
        let Some(snapshot) = log.load_for_recovery().map_err(map_activation_log_error)? else {
            return Ok(None);
        };
        let Some(record) = snapshot.last_record() else {
            return Ok(None);
        };
        let active = self.open_activated_record(record)?;
        self.audit_run_set_global_invariants(active.run_set())?;
        if active.run_set().runs().is_empty() {
            assert!(
                active.readers.is_empty(),
                "ASSERT: an empty Exact Run Set cannot own immutable Run readers"
            );
            return Ok(Some(ExactIndexLocationAudit {
                activation: record,
                active_locations: 0,
            }));
        }
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
                containers.read_verified_dependent_location(entry, &base)?;
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

    fn read_activated_run_set(
        &self,
        record: ExactIndexActivationRecord,
    ) -> Result<ExactIndexRunSet, ExactIndexStoreError> {
        let run_set = self.read_run_set(record.run_set_id())?;
        if run_set.profile() != record.profile()
            || run_set.generation() != record.run_set_generation()
            || run_set.id()? != record.run_set_id()
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(run_set)
    }

    fn open_activated_record(
        &self,
        record: ExactIndexActivationRecord,
    ) -> Result<ActivatedExactIndex<I>, ExactIndexStoreError> {
        let run_set = self.read_activated_run_set(record)?;
        let readers = self.verify_run_set_dependencies(&run_set)?;
        ActivatedExactIndex::new(record, run_set, readers)
    }

    fn open_named(&self, name: &str) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        Ok(self.read_envelope(name)?.descriptor)
    }

    fn read_envelope(&self, name: &str) -> Result<OpenedRunEnvelope, ExactIndexStoreError> {
        let _read_reason =
            crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexEnvelope);
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let envelope = self.read_envelope(name)?;
        self.audit_opened_run(name, &envelope, |_| {})?;
        Ok(envelope.descriptor)
    }

    fn audit_named_with_membership(
        &self,
        name: &str,
        maximum_bytes: usize,
    ) -> Result<AuditedExactRun, ExactIndexStoreError> {
        let envelope = {
            let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
            self.read_envelope(name)?
        };
        let descriptor = envelope.descriptor;
        let mut membership = (maximum_bytes != 0)
            .then(|| BlockedBloomHint::new(descriptor.entry_count(), maximum_bytes).ok())
            .flatten();
        let mut visit = |entry: &ExactIndexEntry| {
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
        };
        let expected_length = u64::try_from(descriptor.file_length())
            .map_err(|_| ExactIndexStoreError::CounterOverflow)?;
        let mapping =
            if let Some(lease) = self.storage.lease_immutable_file(name, expected_length)? {
                Some(Arc::new(ImmutableExactIndexRun::open(
                    lease, descriptor, &mut visit,
                )?))
            } else {
                self.audit_opened_run(name, &envelope, &mut visit)?;
                None
            };
        Ok(AuditedExactRun {
            descriptor,
            membership: {
                membership.map(|filter| {
                    Arc::new(CachedRunMembership::new(
                        &self.page_cache.membership,
                        descriptor.run_hash(),
                        filter,
                    ))
                })
            },
            mapping,
        })
    }

    fn audit_opened_run(
        &self,
        name: &str,
        envelope: &OpenedRunEnvelope,
        mut visit: impl FnMut(&ExactIndexEntry),
    ) -> Result<(), ExactIndexStoreError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexAudit);
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
        emit: F,
    ) -> Result<CompactionSummary, ExactIndexStoreError>
    where
        F: FnMut(ExactIndexEntry) -> Result<(), ExactIndexStoreError>,
    {
        self.merge_compaction_families_using(inputs, &[], emit)
    }

    fn merge_compaction_families_using<F>(
        &self,
        inputs: &[CompactionInputFamily],
        readers: &[ExactIndexRunReader<I>],
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
            sources.push(CompactionFamilySource::open(self, family, readers)?);
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
            source.advance(self, readers)?;
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
        readers: &[ExactIndexRunReader<I>],
    ) -> Result<Vec<CompactionSummary>, ExactIndexStoreError> {
        let mut summaries = Vec::new();
        let mut current = CompactionSummary::default();
        let global = self.merge_compaction_families_using(inputs, readers, |entry| {
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
        readers: &[ExactIndexRunReader<I>],
        owned_writer: bool,
    ) -> Result<(Vec<ExactIndexRunDescriptor>, Vec<ExactIndexRunReader<I>>), ExactIndexStoreError>
    {
        let first_summary = summaries
            .first()
            .copied()
            .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
        let mut output = Some(StreamedPartitionOutput::new(
            self,
            profile,
            first_generation,
            first_summary,
            owned_writer,
        )?);
        let mut partition_ordinal = 0_usize;
        let mut emitted_in_partition = 0_usize;
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(summaries.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let observed = self.merge_compaction_families_using(inputs, readers, |entry| {
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
                    self,
                    profile,
                    generation,
                    summary,
                    owned_writer,
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
        let mut outputs = Vec::new();
        let mut published = Vec::new();
        for (descriptor, evidence) in descriptors {
            published.push(descriptor);
            if owned_writer {
                outputs.push(self.reader_from_writer(descriptor, evidence)?);
            }
        }
        Ok((published, outputs))
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
        let mut output = ImmutableWriteBuffer::new()?;
        output.append(&self.storage, &temporary_name, encoder.header())?;

        let mut page_entries = Vec::new();
        page_entries
            .try_reserve_exact(31)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let observed_summary = self.merge_compaction_inputs(inputs, |entry| {
            page_entries.push(entry);
            if page_entries.len() == 31 {
                write_streamed_page(
                    &self.storage,
                    &temporary_name,
                    &mut encoder,
                    &mut output,
                    &page_entries,
                )?;
                page_entries.clear();
            }
            Ok(())
        })?;
        if !page_entries.is_empty() {
            write_streamed_page(
                &self.storage,
                &temporary_name,
                &mut encoder,
                &mut output,
                &page_entries,
            )?;
        }
        if observed_summary != expected_summary {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let (footer, expected) = encoder.finish()?;
        output.append(&self.storage, &temporary_name, &footer)?;
        output.finish(&self.storage, &temporary_name)?;
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
        self.verify_run_set_dependencies_with_readers(run_set, &[])
    }

    fn verify_run_set_dependencies_with_readers(
        &self,
        run_set: &ExactIndexRunSet,
        reusable: &[ExactIndexRunReader<I>],
    ) -> Result<Vec<ExactIndexRunReader<I>>, ExactIndexStoreError> {
        if run_set.family_count() > MAX_ACTIVE_EXACT_INDEX_FAMILIES {
            return Err(ExactIndexStoreError::TooManyActiveRuns);
        }
        let mut readers = Vec::new();
        readers
            .try_reserve_exact(run_set.runs().len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut membership_bytes_remaining = self.membership_budget_bytes_now();
        let mut mapped_mode = None;
        let reusable: BTreeMap<_, _> = reusable
            .iter()
            .filter(|reader| reader.mapping.is_some())
            .map(|reader| (reader.name.as_str(), reader))
            .collect();
        for run_ref in run_set.runs().iter().copied() {
            let name = published_name(run_ref.profile(), run_ref.generation());
            let audited = if let Some(reader) = reusable.get(name.as_str()) {
                // The Arc retains the same audited file and immutable lease.
                // Match all Run identity fields before making it selectable.
                verify_requested_identity(
                    run_ref.profile(),
                    run_ref.generation(),
                    reader.descriptor,
                )?;
                verify_run_reference(run_ref, reader.descriptor)?;
                AuditedExactRun {
                    descriptor: reader.descriptor,
                    mapping: reader.mapping.clone(),
                    membership: reader
                        .membership
                        .as_ref()
                        .filter(|filter| {
                            filter.allocated_bytes() != 0
                                && filter.allocated_bytes() <= membership_bytes_remaining
                        })
                        .map(Arc::clone),
                }
            } else {
                self.audit_named_with_membership(&name, membership_bytes_remaining)?
            };
            let descriptor = audited.descriptor;
            let membership = audited.membership;
            let mapping = audited.mapping;
            let is_mapped = mapping.is_some();
            if mapped_mode
                .replace(is_mapped)
                .is_some_and(|mode| mode != is_mapped)
            {
                return Err(ExactIndexStoreError::DependencyMismatch);
            }
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
                mapping,
                membership,
                membership_counters: Arc::clone(&self.membership_counters),
            });
        }
        Ok(readers)
    }

    fn membership_budget_bytes_now(&self) -> usize {
        usize::try_from(self.page_cache.cache.capacity()).unwrap_or(usize::MAX)
    }

    fn reader_from_writer(
        &self,
        descriptor: ExactIndexRunDescriptor,
        evidence: Option<RunWriterEvidence>,
    ) -> Result<ExactIndexRunReader<I>, ExactIndexStoreError> {
        let name = published_name(descriptor.profile(), descriptor.generation());
        let audited = if let Some(evidence) = evidence
            && let Some(lease) = self
                .storage
                .lease_immutable_file(&name, descriptor.file_length() as u64)?
        {
            let mapping = ImmutableExactIndexRun::from_writer(
                lease,
                descriptor,
                evidence.bounds,
                evidence.pages,
            )?;
            let membership = evidence.membership.map(|filter| {
                Arc::new(CachedRunMembership::new(
                    &self.page_cache.membership,
                    descriptor.run_hash(),
                    filter,
                ))
            });
            AuditedExactRun {
                descriptor,
                membership,
                mapping: Some(Arc::new(mapping)),
            }
        } else {
            self.audit_named_with_membership(&name, self.membership_budget_bytes_now())?
        };
        Ok(ExactIndexRunReader {
            storage: self.storage.clone(),
            name,
            descriptor: audited.descriptor,
            page_cache: Arc::clone(&self.page_cache),
            mapping: audited.mapping,
            membership: audited.membership,
            membership_counters: Arc::clone(&self.membership_counters),
        })
    }

    fn publish_run_set(
        &self,
        run_set_id: ExactIndexRunSetId,
        encoded: &[u8],
        owned_writer: bool,
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
        write_image(&self.storage, &temporary_name, encoded)?;
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len()).expect("ASSERT: Metadata-v1 length fits u64"),
        )?;
        if !owned_writer {
            let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
            let reread = self.storage.read(&temporary_name)?;
            if reread != encoded || ExactIndexRunSetId::from_encoded(&reread)? != run_set_id {
                return Err(ExactIndexStoreError::PublishVerificationMismatch);
            }
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
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
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
        let leased_run_count = self
            .readers
            .iter()
            .filter(|reader| reader.mapping.is_some())
            .count();
        let positional_run_count = self.readers.len().saturating_sub(leased_run_count);
        let leased_page_bounds_bytes = self.readers.iter().fold(0_usize, |total, reader| {
            total
                .checked_add(
                    reader
                        .mapping
                        .as_ref()
                        .map_or(0, |mapping| mapping.page_bounds_bytes()),
                )
                .expect("ASSERT: active mapped Exact page-bound bytes fit usize")
        });
        let filter_count = self
            .readers
            .iter()
            .filter(|reader| {
                reader
                    .membership
                    .as_ref()
                    .is_some_and(|filter| filter.resident().is_some())
            })
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
        let (huge_page_advised_filter_count, huge_page_advised_bytes) = self
            .readers
            .iter()
            .filter_map(|reader| reader.membership.as_ref())
            .filter(|filter| filter.huge_page_advised())
            .fold((0_usize, 0_usize), |(count, bytes), filter| {
                (
                    count
                        .checked_add(1)
                        .expect("ASSERT: active THP membership count cannot overflow"),
                    bytes
                        .checked_add(filter.allocated_bytes())
                        .expect("ASSERT: active THP membership bytes cannot overflow"),
                )
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
            leased_run_count: u64::try_from(leased_run_count)
                .expect("ASSERT: active mapped Exact Run count fits u64"),
            positional_run_count: u64::try_from(positional_run_count)
                .expect("ASSERT: active positional Exact Run count fits u64"),
            leased_page_bounds_bytes: u64::try_from(leased_page_bounds_bytes)
                .expect("ASSERT: active mapped Exact page-bound bytes fit u64"),
            filter_count: u64::try_from(filter_count)
                .expect("ASSERT: active membership filter count fits u64"),
            allocated_bytes: u64::try_from(allocated_bytes)
                .expect("ASSERT: active membership bytes fit u64"),
            huge_page_advised_filter_count: u64::try_from(huge_page_advised_filter_count)
                .expect("ASSERT: active THP membership count fits u64"),
            huge_page_advised_bytes: u64::try_from(huge_page_advised_bytes)
                .expect("ASSERT: active THP membership bytes fit u64"),
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
        let complete = self.lookup_transitions_into(chunk_id, logical_length, &mut candidates)?;
        Ok(ExactIndexLookup {
            candidates,
            complete,
        })
    }

    /// Reuses caller-owned candidate storage for one bounded lookup.
    ///
    /// The buffer is cleared before use and retains its capacity afterwards,
    /// allowing one frontend Read Plan to resolve many logical Chunks without
    /// one allocation per key.
    pub(crate) fn lookup_transitions_into(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
        candidates: &mut Vec<ExactIndexEntry>,
    ) -> Result<bool, ExactIndexStoreError> {
        candidates.clear();
        if candidates.capacity() < MAX_EXACT_LOOKUP_CANDIDATES {
            candidates
                .try_reserve_exact(MAX_EXACT_LOOKUP_CANDIDATES)
                .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        }
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
            complete &= self.readers[index].lookup_into(
                chunk_id,
                logical_length,
                candidates,
                MAX_EXACT_LOOKUP_CANDIDATES,
            )?;
            if candidates.len() == MAX_EXACT_LOOKUP_CANDIDATES {
                return Ok(false);
            }
        }
        Ok(complete)
    }
}

#[derive(Clone, Debug)]
struct OpenedRunEnvelope {
    descriptor: ExactIndexRunDescriptor,
    header: Vec<u8>,
    footer: Vec<u8>,
    footer_offset: u64,
}

struct AuditedExactRun {
    descriptor: ExactIndexRunDescriptor,
    membership: Option<Arc<CachedRunMembership>>,
    mapping: Option<Arc<ImmutableExactIndexRun>>,
}

/// Transient evidence carried from the encoder into one publication. Retained
/// page bytes have no private budget or directory; they use the common engine.
struct RunWriterEvidence {
    pages: crate::ReadCacheNamespace,
    bounds: Vec<crate::exact_index_read::ExactPageKeyBounds>,
    membership: Option<BlockedBloomHint>,
}

impl RunWriterEvidence {
    fn new(entries: usize, cache: &ExactIndexPageCache) -> Result<Self, ExactIndexStoreError> {
        let mut bounds = Vec::new();
        bounds
            .try_reserve_exact(entries.div_ceil(31))
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let capacity = usize::try_from(cache.cache.capacity()).unwrap_or(usize::MAX);
        let membership = (capacity != 0)
            .then(|| BlockedBloomHint::new(entries, capacity).ok())
            .flatten();
        Ok(Self {
            pages: cache.cache.sibling(crate::ReadCacheClass::StorageRange),
            bounds,
            membership,
        })
    }

    fn observe(
        &mut self,
        entries: &[ExactIndexEntry],
        bytes: &[u8],
    ) -> Result<(), ExactIndexStoreError> {
        if entries.is_empty() || bytes.len() != EXACT_INDEX_PAGE_BYTES {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        self.bounds
            .try_reserve(1)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        self.bounds
            .push(crate::exact_index_read::ExactPageKeyBounds::from_entries(
                entries,
            ));
        if let Some(filter) = &mut self.membership {
            for entry in entries {
                filter.insert_hint(entry.chunk_id(), entry.logical_length() as usize);
            }
        }
        self.pages.insert(
            crate::ReadCacheKey {
                identity: [0; 32],
                ordinal: self.bounds.len() as u64,
            },
            Arc::new(bytes.to_vec()),
            (bytes.len() + std::mem::size_of::<Vec<u8>>()) as u64,
            bytes.len() as u64,
        );
        Ok(())
    }
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
    output: ImmutableWriteBuffer,
    evidence: Option<RunWriterEvidence>,
}

impl StreamedPartitionOutput {
    fn new<I: Clone + StorageIo>(
        repository: &ExactIndexRunRepository<I>,
        profile: ExactIndexProfileId,
        generation: u64,
        summary: CompactionSummary,
        owned_writer: bool,
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
        let mut output = ImmutableWriteBuffer::new()?;
        output.append(&repository.storage, &temporary_name, encoder.header())?;
        let mut page_entries = Vec::new();
        page_entries
            .try_reserve_exact(31)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        Ok(Self {
            encoder,
            temporary_name,
            published_name,
            page_entries,
            output,
            evidence: owned_writer
                .then(|| RunWriterEvidence::new(summary.entry_count, &repository.page_cache))
                .transpose()?,
        })
    }

    fn push<I: StorageIo>(
        &mut self,
        storage: &I,
        entry: ExactIndexEntry,
    ) -> Result<(), ExactIndexStoreError> {
        self.page_entries.push(entry);
        if self.page_entries.len() == 31 {
            let page = write_streamed_page(
                storage,
                &self.temporary_name,
                &mut self.encoder,
                &mut self.output,
                &self.page_entries,
            )?;
            if let Some(evidence) = &mut self.evidence {
                evidence.observe(&self.page_entries, &page)?;
            }
            self.page_entries.clear();
        }
        Ok(())
    }

    fn finish<I: Clone + StorageIo>(
        mut self,
        repository: &ExactIndexRunRepository<I>,
    ) -> Result<(ExactIndexRunDescriptor, Option<RunWriterEvidence>), ExactIndexStoreError> {
        if !self.page_entries.is_empty() {
            let page = write_streamed_page(
                &repository.storage,
                &self.temporary_name,
                &mut self.encoder,
                &mut self.output,
                &self.page_entries,
            )?;
            if let Some(evidence) = &mut self.evidence {
                evidence.observe(&self.page_entries, &page)?;
            }
        }
        let (footer, expected) = self.encoder.finish()?;
        self.output
            .append(&repository.storage, &self.temporary_name, &footer)?;
        self.output
            .finish(&repository.storage, &self.temporary_name)?;
        repository.storage.set_len(
            &self.temporary_name,
            u64::try_from(expected.file_length())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?,
        )?;
        let observed = if self.evidence.is_some() {
            expected
        } else {
            repository.audit_named(&self.temporary_name)?
        };
        verify_expected_descriptor(expected, observed)?;
        repository.storage.sync_file(&self.temporary_name)?;
        if repository.storage.exists(&self.published_name)? {
            let raced = repository.audit_named(&self.published_name)?;
            verify_expected_descriptor(expected, raced)?;
            self.evidence = None;
        } else {
            match repository
                .storage
                .publish_noreplace(&self.temporary_name, &self.published_name)
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let raced = repository.audit_named(&self.published_name)?;
                    verify_expected_descriptor(expected, raced)?;
                    self.evidence = None;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok((observed, self.evidence))
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
    reader: Option<ExactIndexRunReader<I>>,
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
        readers: &[ExactIndexRunReader<I>],
    ) -> Result<Self, ExactIndexStoreError> {
        let first = family
            .refs
            .first()
            .copied()
            .ok_or(ExactIndexStoreError::InvalidCompactionInput)?;
        Ok(Self {
            partitions: family.refs.clone(),
            next_partition_ordinal: 1,
            current: CompactionSource::open_using(repository, first, readers)?,
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
        readers: &[ExactIndexRunReader<I>],
    ) -> Result<(), ExactIndexStoreError> {
        self.current.advance()?;
        if self.current.finished && self.next_partition_ordinal < self.partitions.len() {
            let next = self.partitions[self.next_partition_ordinal];
            self.next_partition_ordinal = self
                .next_partition_ordinal
                .checked_add(1)
                .ok_or(ExactIndexStoreError::DependencyMismatch)?;
            self.current = CompactionSource::open_using(repository, next, readers)?;
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
        Self::open_using(repository, run_ref, &[])
    }

    fn open_using(
        repository: &ExactIndexRunRepository<I>,
        run_ref: ExactIndexRunRef,
        readers: &[ExactIndexRunReader<I>],
    ) -> Result<Self, ExactIndexStoreError> {
        let name = published_name(run_ref.profile(), run_ref.generation());
        if !crate::read_intent::independent()
            && let Some(reader) = readers
                .iter()
                .find(|reader| reader.name == name && reader.mapping.is_some())
        {
            verify_run_reference(run_ref, reader.descriptor)?;
            let mut source = Self {
                storage: repository.storage.clone(),
                name,
                descriptor: reader.descriptor,
                generation: run_ref.generation(),
                footer: Vec::new(),
                footer_offset: 0,
                audit: None,
                page: None,
                page_entry_ordinal: 0,
                next_page_ordinal: 0,
                finished: false,
                reader: Some(reader.clone()),
            };
            source.load_next_page()?;
            return Ok(source);
        }
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
            reader: None,
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
        let _read_reason =
            crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexCompaction);
        if self.next_page_ordinal == self.descriptor.page_count() {
            if let Some(mut audit) = self.audit.take() {
                audit.update(self.footer_offset, &self.footer)?;
                audit.finish()?;
            }
            self.page = None;
            self.finished = true;
            return Ok(());
        }
        let page_ordinal = self.next_page_ordinal;
        if let Some(reader) = &self.reader {
            self.page = Some((*reader.read_page(page_ordinal)?).clone());
            self.page_entry_ordinal = 0;
            self.next_page_ordinal += 1;
            return Ok(());
        }
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
    output: &mut ImmutableWriteBuffer,
    entries: &[ExactIndexEntry],
) -> Result<[u8; EXACT_INDEX_PAGE_BYTES], ExactIndexStoreError> {
    let page = encoder.encode_next_page(entries)?;
    output.append(storage, temporary_name, &page)?;
    Ok(page)
}

/// Repository-wide, pressure-bounded cache of independently verified 4-KiB
/// Exact-Index pages.
///
/// Lazy sharded maps keep lookup allocation-free and admit only the shared
/// budget granted to this pool. FIFO replacement discards acceleration only;
/// it cannot affect Exact-Index or DATA correctness.
#[derive(Debug)]
struct ExactIndexPageCache {
    cache: crate::ReadCacheNamespace,
    membership: crate::ReadCacheNamespace,
    capacity_pages: u64,
    automatic_pressure: bool,
}
impl ExactIndexPageCache {
    fn build(snapshot: MemoryPressureSnapshot, automatic_pressure: bool) -> Self {
        let capacity_pages = if automatic_pressure {
            snapshot.effective_limit_bytes() / ACCOUNTED_PAGE_BYTES
        } else {
            exact_page_cache_capacity(snapshot) as u64
        };
        let namespace = if automatic_pressure {
            crate::ReadCacheNamespace::system(crate::ReadCacheClass::ExactPage)
        } else {
            crate::ReadCacheNamespace::isolated(crate::ReadCacheClass::ExactPage, 0)
        };
        let cache = Self {
            membership: namespace.sibling(crate::ReadCacheClass::ExactMembership),
            cache: namespace,
            capacity_pages,
            automatic_pressure,
        };
        cache.apply_pressure_snapshot(snapshot);
        cache
    }
    fn get(&self, run_hash: [u8; 32], page_ordinal: usize) -> Option<Arc<ExactIndexPage>> {
        self.cache.get(crate::ReadCacheKey {
            identity: run_hash,
            ordinal: page_ordinal as u64,
        })
    }
    fn insert(&self, run_hash: [u8; 32], page_ordinal: usize, page: Arc<ExactIndexPage>) {
        assert_eq!(
            page.ordinal(),
            page_ordinal,
            "ASSERT: verified Exact page ordinal"
        );
        self.cache.insert(
            crate::ReadCacheKey {
                identity: run_hash,
                ordinal: page_ordinal as u64,
            },
            page,
            EXACT_INDEX_PAGE_BYTES as u64,
            EXACT_INDEX_PAGE_BYTES as u64,
        );
    }
    fn status(&self) -> ExactIndexPageCacheStatus {
        let stats = self.cache.stats();
        let pressure = self.cache.pressure();
        ExactIndexPageCacheStatus {
            hits: stats.hits,
            misses: stats.misses,
            resident_pages: stats.entries,
            evictions: stats.evictions,
            pressure_rejections: stats.rejections,
            target_pages: self.cache.capacity() / ACCOUNTED_PAGE_BYTES,
            capacity_pages: self.capacity_pages,
            reserve_bytes: shared_cache_reserve_bytes(pressure.effective_limit_bytes()),
            effective_limit_bytes: pressure.effective_limit_bytes(),
            available_bytes: pressure.available_bytes(),
            swap_used_bytes: pressure.swap_used_bytes(),
        }
    }
    fn apply_pressure_snapshot(&self, snapshot: MemoryPressureSnapshot) {
        if !self.automatic_pressure {
            self.cache.update_pressure(
                snapshot,
                self.capacity_pages * ACCOUNTED_PAGE_BYTES,
                shared_cache_reserve_bytes(snapshot.effective_limit_bytes()),
            );
        }
    }
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
    ACCOUNTED_PAGE_BYTES
}

fn floor_power_of_two(value: usize) -> usize {
    let next = value
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX / 2 + 1);
    if next == value { value } else { next / 2 }
}

#[derive(Debug)]
struct CachedRunMembership {
    cache: crate::ReadCacheNamespace,
    key: crate::ReadCacheKey,
}
impl CachedRunMembership {
    fn new(
        cache: &crate::ReadCacheNamespace,
        identity: [u8; 32],
        filter: BlockedBloomHint,
    ) -> Self {
        let key = crate::ReadCacheKey {
            identity,
            ordinal: 0,
        };
        let bytes = filter.allocated_bytes() as u64 + size_of::<BlockedBloomHint>() as u64;
        cache.insert(key, Arc::new(filter), bytes, EXACT_INDEX_PAGE_BYTES as u64);
        Self {
            cache: cache.clone(),
            key,
        }
    }
    fn resident(&self) -> Option<Arc<BlockedBloomHint>> {
        self.cache.peek(self.key)
    }
    fn allocated_bytes(&self) -> usize {
        self.resident().map_or(0, |filter| filter.allocated_bytes())
    }
    fn huge_page_advised(&self) -> bool {
        self.resident()
            .is_some_and(|filter| filter.huge_page_advised())
    }
    fn probe_for_exact_lookup(&self, chunk: ChunkId, length: usize) -> BloomLookupHint {
        self.cache
            .get::<BlockedBloomHint>(self.key)
            .map_or(BloomLookupHint::RequiresExactLookup, |filter| {
                filter.probe_for_exact_lookup(chunk, length)
            })
    }
}

/// Open immutable Run handle backed by bounded reads or an audited active mapping.
#[derive(Clone, Debug)]
pub struct ExactIndexRunReader<I> {
    storage: I,
    name: String,
    descriptor: ExactIndexRunDescriptor,
    page_cache: Arc<ExactIndexPageCache>,
    mapping: Option<Arc<ImmutableExactIndexRun>>,
    membership: Option<Arc<CachedRunMembership>>,
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
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(MAX_EXACT_LOOKUP_CANDIDATES)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let complete = self.lookup_into(
            chunk_id,
            logical_length,
            &mut candidates,
            MAX_EXACT_LOOKUP_CANDIDATES,
        )?;
        Ok(ExactIndexLookup {
            candidates,
            complete,
        })
    }

    fn lookup_into(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
        candidates: &mut Vec<ExactIndexEntry>,
        maximum_candidates: usize,
    ) -> Result<bool, ExactIndexStoreError> {
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexLookup);
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
                    return Ok(true);
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
            let position = if let Some(mapping) = &self.mapping {
                mapping.page_position(middle, chunk_id, logical_length)?
            } else {
                self.read_page(middle)?.position(chunk_id, logical_length)
            };
            match position {
                ExactIndexPagePosition::After => lower = middle + 1,
                ExactIndexPagePosition::Before | ExactIndexPagePosition::Within => upper = middle,
            }
        }

        let mut page_ordinal = lower;
        while page_ordinal < self.descriptor.page_count() {
            let page = self.read_page(page_ordinal)?;
            let matches = page.candidates(chunk_id, logical_length);
            if matches.is_empty() {
                return Ok(true);
            }
            let remaining = maximum_candidates.saturating_sub(candidates.len());
            let accepted = matches.len().min(remaining);
            candidates.extend_from_slice(&matches[..accepted]);
            let key_reaches_page_end = page.entries().last().is_some_and(|entry| {
                entry.chunk_id() == chunk_id && entry.logical_length() == logical_length
            });
            if matches.len() > remaining {
                return Ok(false);
            }
            if !key_reaches_page_end || page_ordinal + 1 == self.descriptor.page_count() {
                return Ok(true);
            }
            if candidates.len() == maximum_candidates {
                return Ok(false);
            }
            page_ordinal += 1;
        }
        Ok(true)
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
        let page = if let Some(mapping) = &self.mapping {
            Arc::new(
                self.descriptor
                    .decode_page(page_ordinal, &mapping.page(offset)?)?,
            )
        } else {
            let bytes = self
                .storage
                .read_exact_at(&self.name, offset, EXACT_INDEX_PAGE_BYTES)?;
            Arc::new(self.descriptor.decode_page(page_ordinal, &bytes)?)
        };
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

    fn reuse_fixture(ordinal: u64) -> ExactIndexEntry {
        let location = fastdup_format::ExactIndexLocation::raw(
            ContainerId::new([71; 16]).unwrap(),
            1,
            4096,
            256,
            0,
        )
        .unwrap();
        ExactIndexEntry::active(ChunkId::of(&ordinal.to_le_bytes()), 32, location).unwrap()
    }

    fn reuse_repository(label: &str) -> ExactIndexRunRepository<crate::FsStorageIo> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.artifacts/tests")
            .join(format!("exact-reuse-{label}-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).unwrap();
        }
        let repository = ExactIndexRunRepository::new_with_memory_snapshot(
            crate::FsStorageIo::open(&root).unwrap(),
            MemoryPressureSnapshot::new(8 << 30, 6 << 30, 0),
        );
        // Storage representations share this fixture's deterministic governor,
        // just as production representations share the system cache.
        repository
            .storage
            .immutable_leases
            .range_cache
            .set(
                repository
                    .page_cache
                    .cache
                    .sibling(crate::ReadCacheClass::StorageRange),
            )
            .unwrap();
        repository
    }

    #[test]
    fn immutable_index_publication_batches_physical_writes() {
        for mode in ["run", "compaction", "family"] {
            let repository = reuse_repository(&format!("write-batch-{mode}"));
            let profile = ExactIndexProfileId::new([97; 32]).unwrap();
            let entries: Vec<_> = (0..16_000).map(reuse_fixture).collect();
            let expected = ExactIndexRun::new(profile, 3, entries.clone()).unwrap();
            let mut inputs = Vec::new();
            if mode != "run" {
                for (i, part) in entries.chunks(8000).enumerate() {
                    let run = ExactIndexRun::new(profile, i as u64 + 1, part.to_vec()).unwrap();
                    let descriptor = repository.publish(&run).unwrap();
                    inputs.push(ExactIndexRunRef::new(0, descriptor).unwrap());
                }
            }
            let before = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get);
            let descriptor = match mode {
                "run" => repository.publish(&expected).unwrap(),
                "compaction" => repository.compact(&inputs, 3).unwrap(),
                _ => {
                    repository.compact_family(&inputs, 1, 3).unwrap();
                    repository.audit(profile, 3).unwrap()
                }
            };
            let writes = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get) - before;
            let batches = expected.encode().unwrap().len().div_ceil(1024 * 1024);
            eprintln!(
                "{mode}: bytes={}, physical writes={writes}",
                descriptor.file_length()
            );
            assert_eq!(
                writes,
                2 + 2 * batches,
                "{mode}: one body/head pair per MiB, plus initial heads"
            );
            assert_eq!(repository.audit(profile, 3).unwrap(), descriptor);
            let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
            assert_eq!(
                repository
                    .storage
                    .read(&published_name(profile, 3))
                    .unwrap(),
                expected.encode().unwrap()
            );
        }
    }

    #[test]
    fn append_shares_audited_runs_but_recovery_reaudits_and_pressure_drops_hints() {
        let repository = reuse_repository("mapped");
        let profile = ExactIndexProfileId::new([61; 32]).unwrap();
        repository
            .append_level_zero(profile, (0..1024).map(reuse_fixture).collect())
            .unwrap();
        let old = repository.pin_active_generation().unwrap();
        let old_reader = &old.readers[0];
        assert!(old_reader.membership.is_some());
        repository
            .append_level_zero(profile, vec![reuse_fixture(2000)])
            .unwrap();
        let current = repository.pin_active_generation().unwrap();
        let shared = current
            .readers
            .iter()
            .find(|reader| reader.name == old_reader.name)
            .unwrap();
        assert!(Arc::ptr_eq(
            shared.mapping.as_ref().unwrap(),
            old_reader.mapping.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(
            shared.membership.as_ref().unwrap(),
            old_reader.membership.as_ref().unwrap()
        ));
        let recovered = repository.recover_active().unwrap().unwrap();
        let independent = recovered
            .readers
            .iter()
            .find(|reader| reader.name == old_reader.name)
            .unwrap();
        assert!(!Arc::ptr_eq(
            independent.mapping.as_ref().unwrap(),
            old_reader.mapping.as_ref().unwrap()
        ));
        // Swap disables new membership admission, including reused hints.
        repository
            .page_cache
            .apply_pressure_snapshot(MemoryPressureSnapshot::new(8 << 30, 6 << 30, 1));
        repository
            .append_level_zero(profile, vec![reuse_fixture(2001)])
            .unwrap();
        let pressured = repository.pin_active_generation().unwrap();
        assert!(
            pressured
                .readers
                .iter()
                .all(|reader| reader.membership.is_none())
        );
        let shared = pressured
            .readers
            .iter()
            .find(|reader| reader.name == old_reader.name)
            .unwrap();
        assert!(Arc::ptr_eq(
            shared.mapping.as_ref().unwrap(),
            old_reader.mapping.as_ref().unwrap()
        ));
        assert!(
            !pressured
                .lookup_transitions(reuse_fixture(17).chunk_id(), 32)
                .unwrap()
                .candidates()
                .is_empty()
        );
    }

    #[test]
    fn online_append_carries_wal_state_through_rotation_without_slot_reads() {
        let mut repository = reuse_repository("online-wal-state");
        let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        repository.storage.metadata_reads = Some(Arc::clone(&counters));
        let profile = ExactIndexProfileId::new([93; 32]).unwrap();
        repository
            .append_level_zero(profile, vec![reuse_fixture(1)])
            .unwrap();
        let slot_reads = || {
            counters
                .rows()
                .iter()
                .filter(|row| row.object == "control" && row.mode == "directFile")
                .map(|row| row.operations)
                .sum::<u64>()
        };
        let before = slot_reads();
        for ordinal in 2..=70 {
            repository
                .append_level_zero(profile, vec![reuse_fixture(ordinal)])
                .unwrap();
        }
        assert_eq!(
            slot_reads() - before,
            0,
            "online appends must advance their WAL cursor without rereading slots"
        );
        let physical_before = crate::direct_io::READ_BYTES.with(std::cell::Cell::get);
        for ordinal in 71..=140 {
            repository
                .append_level_zero(profile, vec![reuse_fixture(ordinal)])
                .unwrap();
        }
        assert_eq!(
            crate::direct_io::READ_BYTES.with(std::cell::Cell::get) - physical_before,
            0,
            "warm online publication, compaction and WAL rotation must perform no file-content reads"
        );
        let recovered = repository.recover_active().unwrap().unwrap();
        assert_eq!(recovered.record().generation(), 140);
        assert!(
            slot_reads() > before,
            "recovery must independently read the stored slots"
        );
        assert!(repository.audit_activation_log().unwrap().is_some());
    }

    #[test]
    fn online_append_uses_writer_run_evidence_instead_of_disk_audits() {
        let mut repository = reuse_repository("online-run-proof");
        let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        repository.storage.metadata_reads = Some(Arc::clone(&counters));
        let profile = ExactIndexProfileId::new([94; 32]).unwrap();
        for ordinal in 1..=70 {
            repository
                .append_level_zero(profile, vec![reuse_fixture(ordinal)])
                .unwrap();
        }
        let audits = || {
            counters
                .rows()
                .iter()
                .filter(|row| row.reason == "indexAudit")
                .map(|row| row.operations)
                .sum::<u64>()
        };
        assert_eq!(audits(), 0, "new Runs are already checked by their writer");
        assert_eq!(
            repository
                .recover_active()
                .unwrap()
                .unwrap()
                .record()
                .generation(),
            70
        );
        assert!(
            audits() > 0,
            "independent recovery still verifies stored Runs"
        );
    }

    #[test]
    fn warm_writer_pages_do_not_hide_corruption_from_recovery_or_scrub() {
        let repository = reuse_repository("writer-corruption");
        let profile = ExactIndexProfileId::new([95; 32]).unwrap();
        let entry = reuse_fixture(1);
        repository.append_level_zero(profile, vec![entry]).unwrap();
        let pin = repository.pin_active_generation().unwrap();
        assert_eq!(
            pin.lookup_transitions(entry.chunk_id(), 32)
                .unwrap()
                .candidates()
                .len(),
            1
        );
        let reader = &pin.readers[0];
        let file =
            crate::direct_io::open(&repository.storage.root().join(&reader.name), true).unwrap();
        let offset = EXACT_INDEX_PAGE_BYTES as u64 + 100;
        let old = crate::direct_io::read(&file, offset, 1).unwrap()[0];
        // Simulate damage below the cooperating lease/mutation seam.
        crate::direct_io::write(&file, offset, &[old ^ 1]).unwrap();
        file.sync_all().unwrap();
        assert_eq!(
            pin.lookup_transitions(entry.chunk_id(), 32)
                .unwrap()
                .candidates()
                .len(),
            1
        );
        assert!(repository.recover_active().is_err());
        assert!(repository.activation_writer.lock().unwrap().is_none());
        assert!(repository.audit_activation_log().is_err());
        assert!(
            repository
                .append_level_zero(profile, vec![reuse_fixture(2)])
                .is_err(),
            "failed recovery must revoke reuse of the installed writer generation"
        );
    }

    #[test]
    fn writer_pages_are_reclaimable_and_cold_compaction_keeps_results() {
        let mut repository = reuse_repository("writer-eviction");
        let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        repository.storage.metadata_reads = Some(Arc::clone(&counters));
        let profile = ExactIndexProfileId::new([96; 32]).unwrap();
        for ordinal in 1..=3 {
            repository
                .append_level_zero(profile, vec![reuse_fixture(ordinal)])
                .unwrap();
        }
        repository
            .page_cache
            .apply_pressure_snapshot(MemoryPressureSnapshot::new(8 << 30, 6 << 30, 1));
        let before: u64 = counters.rows().iter().map(|row| row.operations).sum();
        repository
            .append_level_zero(profile, vec![reuse_fixture(4)])
            .unwrap();
        assert!(
            counters
                .rows()
                .iter()
                .map(|row| row.operations)
                .sum::<u64>()
                > before,
            "eviction must remove writer pages and permit real cold reads"
        );
        let pin = repository.pin_active_generation().unwrap();
        for ordinal in 1..=4 {
            assert_eq!(
                pin.lookup_transitions(reuse_fixture(ordinal).chunk_id(), 32)
                    .unwrap()
                    .candidates()
                    .len(),
                1
            );
        }
        assert_eq!(
            repository.audit_activation_log().unwrap(),
            Some(pin.record())
        );
    }

    #[test]
    fn append_matches_durable_activation_before_reusing_cached_generation() {
        let repository = reuse_repository("changed-selector");
        let profile = ExactIndexProfileId::new([62; 32]).unwrap();
        repository
            .append_level_zero(profile, vec![reuse_fixture(1)])
            .unwrap();
        // Public activation deliberately does not install the process snapshot.
        repository
            .activate(&ExactIndexRunSet::new(profile, 2, vec![]).unwrap())
            .unwrap();
        repository
            .append_level_zero(profile, vec![reuse_fixture(2)])
            .unwrap();
        let current = repository.pin_active_generation().unwrap();
        assert!(
            current
                .lookup_transitions(reuse_fixture(1).chunk_id(), 32)
                .unwrap()
                .candidates()
                .is_empty()
        );
        assert!(
            !current
                .lookup_transitions(reuse_fixture(2).chunk_id(), 32)
                .unwrap()
                .candidates()
                .is_empty()
        );
    }

    #[test]
    fn reusable_mapping_does_not_authorize_a_different_run_hash() {
        let repository = reuse_repository("wrong-hash");
        let profile = ExactIndexProfileId::new([63; 32]).unwrap();
        repository
            .append_level_zero(profile, vec![reuse_fixture(1)])
            .unwrap();
        let old = repository.pin_active_generation().unwrap();
        let different = ExactIndexRun::new(profile, 1, vec![reuse_fixture(2)])
            .unwrap()
            .encode()
            .unwrap();
        let descriptor = descriptor_from_complete_bytes(&different).unwrap();
        let run_set = ExactIndexRunSet::new(
            profile,
            2,
            vec![ExactIndexRunRef::new(0, descriptor).unwrap()],
        )
        .unwrap();
        assert!(
            repository
                .activate_with_readers(&run_set, &old.readers, false)
                .is_err()
        );
        assert_eq!(
            repository.recover_active().unwrap().unwrap().record(),
            old.record()
        );
    }

    #[derive(Clone, Debug)]
    struct ReuseFaultStorage {
        inner: crate::FsStorageIo,
        fault: Arc<AtomicUsize>,
    }

    impl StorageIo for ReuseFaultStorage {
        fn create_new(&self, name: &str) -> io::Result<()> {
            self.inner.create_new(name)
        }
        fn exists(&self, name: &str) -> io::Result<bool> {
            self.inner.exists(name)
        }
        fn read(&self, name: &str) -> io::Result<Vec<u8>> {
            self.inner.read(name)
        }
        fn object_len(&self, name: &str) -> io::Result<u64> {
            self.inner.object_len(name)
        }
        fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
            self.inner.read_exact_at(name, offset, length)
        }
        fn list_names(&self) -> io::Result<Vec<String>> {
            self.inner.list_names()
        }
        fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
            self.inner.set_len(name, length)
        }
        fn publish_noreplace(&self, temporary: &str, published: &str) -> io::Result<()> {
            self.inner.publish_noreplace(temporary, published)
        }
        fn remove_file(&self, name: &str) -> io::Result<()> {
            self.inner.remove_file(name)
        }
        fn sync_root(&self) -> io::Result<()> {
            self.inner.sync_root()
        }
        fn lease_immutable_file(
            &self,
            name: &str,
            expected_length: u64,
        ) -> io::Result<Option<crate::ImmutableFileLease>> {
            self.inner.lease_immutable_file(name, expected_length)
        }
        fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
            if matches!(
                name,
                "exact-index.activation.wal" | "exact-index.activation.1.wal"
            ) && self
                .fault
                .compare_exchange(1, 0, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
                .is_ok()
            {
                return Err(io::Error::other("injected before activation write"));
            }
            self.inner.write_at(name, offset, bytes)
        }
        fn sync_file(&self, name: &str) -> io::Result<()> {
            self.inner.sync_file(name)?;
            if matches!(
                name,
                "exact-index.activation.wal" | "exact-index.activation.1.wal"
            ) && self
                .fault
                .compare_exchange(2, 0, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
                .is_ok()
            {
                return Err(io::Error::other("injected after activation sync"));
            }
            Ok(())
        }
    }

    #[test]
    fn reused_mapping_append_recovers_and_retries_before_write_and_after_sync_faults() {
        for mode in [1, 2] {
            let initial = reuse_repository(&format!("fault-{mode}"));
            let storage = ReuseFaultStorage {
                inner: initial.storage,
                fault: Arc::new(AtomicUsize::new(0)),
            };
            let repository = ExactIndexRunRepository::new(storage.clone());
            let profile = ExactIndexProfileId::new([64; 32]).unwrap();
            repository
                .append_level_zero(profile, vec![reuse_fixture(1)])
                .unwrap();
            let old = repository.pin_active_generation().unwrap();
            assert!(old.readers[0].mapping.is_some());
            storage.fault.store(mode, AtomicOrdering::Relaxed);
            assert!(
                repository
                    .append_level_zero(profile, vec![reuse_fixture(2)])
                    .is_err()
            );
            assert_eq!(storage.fault.load(AtomicOrdering::Relaxed), 0);
            assert_eq!(
                repository.pin_active_generation().unwrap().record(),
                old.record()
            );
            let recovered = repository.recover_active().unwrap().unwrap();
            assert_eq!(recovered.run_set().generation(), mode as u64);
            assert_eq!(
                repository.audit_activation_log().unwrap(),
                Some(recovered.record())
            );
            repository
                .append_level_zero(profile, vec![reuse_fixture(3)])
                .unwrap();
            let current = repository.pin_active_generation().unwrap();
            for ordinal in [1, 3] {
                assert!(
                    !current
                        .lookup_transitions(reuse_fixture(ordinal).chunk_id(), 32)
                        .unwrap()
                        .candidates()
                        .is_empty()
                );
            }
            assert_eq!(
                current
                    .lookup_transitions(reuse_fixture(2).chunk_id(), 32)
                    .unwrap()
                    .candidates()
                    .is_empty(),
                mode == 1
            );
        }
    }

    #[test]
    fn exact_page_cache_capacity_and_target_follow_live_headroom() {
        let gib = 1_024_u64 * 1_024 * 1_024;
        let healthy = MemoryPressureSnapshot::new(128 * gib, 96 * gib, 0);
        let cache = ExactIndexPageCache::build(healthy, false);
        let status = cache.status();

        assert!(ACCOUNTED_PAGE_BYTES > EXACT_INDEX_PAGE_BYTES as u64);
        assert!(status.capacity_pages().is_power_of_two());
        assert!(status.capacity_pages() > 256);
        assert_eq!(status.target_pages(), status.capacity_pages());
        assert_eq!(
            status.reserve_bytes(),
            shared_cache_reserve_bytes(128 * gib)
        );

        cache.apply_pressure_snapshot(MemoryPressureSnapshot::new(
            128 * gib,
            shared_cache_reserve_bytes(128 * gib),
            0,
        ));
        assert_eq!(cache.status().target_pages(), 0);

        cache.apply_pressure_snapshot(MemoryPressureSnapshot::new(128 * gib, 96 * gib, 1));
        let swapped = cache.status();
        assert_eq!(swapped.target_pages(), 0);
        assert_eq!(swapped.swap_used_bytes(), 1);
    }

    #[test]
    fn membership_residency_reclaims_with_the_common_cache() {
        let cache =
            crate::ReadCacheNamespace::isolated(crate::ReadCacheClass::ExactMembership, 1 << 20);
        let mut filter = BlockedBloomHint::new(1000, 1 << 20).unwrap();
        let chunk = ChunkId::of(b"cached membership");
        filter.insert_hint(chunk, 17);
        let hint = CachedRunMembership::new(&cache, [4; 32], filter);
        assert!(hint.allocated_bytes() > 0);
        assert_eq!(
            hint.probe_for_exact_lookup(chunk, 17),
            BloomLookupHint::RequiresExactLookup
        );
        cache.set_capacity(0);
        assert_eq!(hint.allocated_bytes(), 0);
        assert_eq!(
            hint.probe_for_exact_lookup(ChunkId::of(b"missing"), 17),
            BloomLookupHint::RequiresExactLookup
        );
    }
}
