use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use fastdup_format::{ChunkId, ExactIndexEntry, ExactLocationTransition};
use fastdup_store::{MemoryPressureSnapshot, shared_cache_reserve_bytes};
use hashbrown::HashTable;

use crate::proof_cache_trace::ProofKey;

const ACCOUNTED_ENTRY_BYTES: usize = 224;
const MAXIMUM_EVICTION_STEPS: usize = 256;
const DEFAULT_SHARDS: usize = 256;
const EFFECTIVE_RAM_DIVISOR: u64 = 50;
const MAXIMUM_HARD_LIMIT_BYTES: u64 = 2 * 1_024 * 1_024 * 1_024;
const REFRESH_MILLIS: u64 = 250;
const NONE: u32 = u32::MAX;

/// How a fully verified historical proof entered the cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoricalProofAdmission {
    /// A newly published Chunk starts in S3-FIFO Small probation.
    Published,
    /// A physically reverified Exact reuse enters Main immediately.
    ExactReuse,
}

/// Hard geometry and process-memory reserve for the Historical Proof Cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HistoricalProofCacheConfig {
    hard_limit_bytes: usize,
    reserve_bytes: u64,
    shard_count: NonZeroUsize,
}

impl HistoricalProofCacheConfig {
    /// Derives the production policy from one conservative memory snapshot.
    ///
    /// The cache may grow to at most five percent of effective RAM and 2 GiB.
    /// Its live target is stricter and normally uses at most two percent.
    #[must_use]
    fn conservative(snapshot: MemoryPressureSnapshot) -> Self {
        let effective = snapshot.effective_limit_bytes().max(1);
        let metadata = DEFAULT_SHARDS * size_of::<CacheShard>();
        let minimum = metadata + DEFAULT_SHARDS * ACCOUNTED_ENTRY_BYTES;
        let hard = usize::try_from((effective / 20).min(MAXIMUM_HARD_LIMIT_BYTES))
            .unwrap_or(usize::MAX)
            .max(minimum);
        Self {
            hard_limit_bytes: hard,
            reserve_bytes: shared_cache_reserve_bytes(effective),
            shard_count: NonZeroUsize::new(DEFAULT_SHARDS)
                .expect("ASSERT: default Historical Proof Cache shard count is nonzero"),
        }
    }
}

/// Rebuildable S3-FIFO cache status for observability and pressure tests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoricalProofCacheStatus {
    hits: u64,
    misses: u64,
    admissions: u64,
    admission_rejections: u64,
    allocation_rejections: u64,
    evictions: u64,
    ghost_hits: u64,
    entry_count: usize,
    target_entries: usize,
    resident_bytes: usize,
    metadata_bytes: usize,
    hard_limit_bytes: usize,
    reserve_bytes: u64,
    maximum_eviction_steps: usize,
    effective_limit_bytes: u64,
    available_bytes: u64,
    swap_used_bytes: u64,
}

macro_rules! status_getter {
    ($name:ident, $field:ident, $type:ty) => {
        #[must_use]
        pub const fn $name(self) -> $type {
            self.$field
        }
    };
}

impl HistoricalProofCacheStatus {
    status_getter!(hits, hits, u64);
    status_getter!(misses, misses, u64);
    status_getter!(admissions, admissions, u64);
    status_getter!(admission_rejections, admission_rejections, u64);
    status_getter!(allocation_rejections, allocation_rejections, u64);
    status_getter!(evictions, evictions, u64);
    status_getter!(ghost_hits, ghost_hits, u64);
    status_getter!(entry_count, entry_count, usize);
    status_getter!(target_entries, target_entries, usize);
    status_getter!(resident_bytes, resident_bytes, usize);
    status_getter!(metadata_bytes, metadata_bytes, usize);
    status_getter!(hard_limit_bytes, hard_limit_bytes, usize);
    status_getter!(reserve_bytes, reserve_bytes, u64);
    status_getter!(maximum_eviction_steps, maximum_eviction_steps, usize);
    status_getter!(effective_limit_bytes, effective_limit_bytes, u64);
    status_getter!(available_bytes, available_bytes, u64);
    status_getter!(swap_used_bytes, swap_used_bytes, u64);

    /// Returns hits divided by all probes on a 0-10,000 scale.
    ///
    /// # Panics
    ///
    /// Panics only if bounded integer ratio arithmetic exceeds 10,000 basis
    /// points, which would violate the hit/miss counter invariant.
    #[must_use]
    pub fn hit_rate_basis_points(self) -> u64 {
        let probes = u128::from(self.hits) + u128::from(self.misses);
        if probes == 0 {
            return 0;
        }
        u64::try_from(u128::from(self.hits) * 10_000 / probes)
            .expect("ASSERT: Historical Proof Cache hit rate is bounded")
    }
}

#[derive(Debug)]
pub(crate) enum HistoricalProofCacheError {
    OutOfMemory,
}

impl fmt::Display for HistoricalProofCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => formatter.write_str("Historical Proof Cache allocation failed"),
        }
    }
}

impl Error for HistoricalProofCacheError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Queue {
    Small,
    Main,
}

#[derive(Debug)]
struct CacheSlot {
    entry: Option<ExactIndexEntry>,
    next: u32,
    frequency: u8,
    queue: Queue,
}

impl CacheSlot {
    fn vacant() -> Self {
        Self {
            entry: None,
            next: NONE,
            frequency: 0,
            queue: Queue::Small,
        }
    }
}

#[derive(Debug, Default)]
struct FifoQueue {
    head: u32,
    tail: u32,
    len: usize,
}

impl FifoQueue {
    const fn empty() -> Self {
        Self {
            head: NONE,
            tail: NONE,
            len: 0,
        }
    }
}

#[derive(Debug)]
struct ShardState {
    slots: Vec<CacheSlot>,
    free: Vec<u32>,
    index: HashTable<u32>,
    small: FifoQueue,
    main: FifoQueue,
    ghost: HashMap<u64, u64>,
    ghost_fifo: VecDeque<(u64, u64)>,
    next_ghost_epoch: u64,
    entries: usize,
    counters: HistoricalShardCounters,
}

#[derive(Clone, Copy, Debug, Default)]
struct HistoricalShardCounters {
    hits: u64,
    misses: u64,
    admissions: u64,
    admission_rejections: u64,
    allocation_rejections: u64,
    evictions: u64,
    ghost_hits: u64,
    maximum_eviction_steps: usize,
}

impl HistoricalShardCounters {
    fn add_assign(&mut self, other: Self) {
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.admissions = self.admissions.saturating_add(other.admissions);
        self.admission_rejections = self
            .admission_rejections
            .saturating_add(other.admission_rejections);
        self.allocation_rejections = self
            .allocation_rejections
            .saturating_add(other.allocation_rejections);
        self.evictions = self.evictions.saturating_add(other.evictions);
        self.ghost_hits = self.ghost_hits.saturating_add(other.ghost_hits);
        self.maximum_eviction_steps = self
            .maximum_eviction_steps
            .max(other.maximum_eviction_steps);
    }
}

impl Default for ShardState {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            index: HashTable::new(),
            small: FifoQueue::empty(),
            main: FifoQueue::empty(),
            ghost: HashMap::new(),
            ghost_fifo: VecDeque::new(),
            next_ghost_epoch: 0,
            entries: 0,
            counters: HistoricalShardCounters::default(),
        }
    }
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct CacheShard {
    state: Mutex<ShardState>,
}

/// Sharded, pressure-aware production S3-FIFO for historical DATA proofs.
///
/// Lookups lock one cache-line-separated shard. Pressure refresh takes the
/// global write gate only on its cold 250-ms path. Cache misses, rejected
/// admissions, and allocation failures never affect storage correctness.
pub(crate) struct HistoricalProofCache {
    config: HistoricalProofCacheConfig,
    shards: Box<[CacheShard]>,
    pressure_gate: RwLock<()>,
    target_entries: AtomicUsize,
    entry_count: AtomicUsize,
    unsharded_misses: AtomicU64,
    unsharded_admission_rejections: AtomicU64,
    effective_limit_bytes: AtomicU64,
    available_bytes: AtomicU64,
    swap_used_bytes: AtomicU64,
    automatic_pressure: bool,
    started: Instant,
    last_refresh_millis: AtomicU64,
}

impl fmt::Debug for HistoricalProofCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoricalProofCache")
            .field("status", &self.status())
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

impl HistoricalProofCache {
    /// Constructs the production cache with automatic host/cgroup pressure sampling.
    ///
    /// # Errors
    ///
    /// Returns an initial shard allocation failure. A sampling failure starts
    /// with admission disabled and retries on the normal refresh path.
    pub(crate) fn new_system() -> Result<Self, HistoricalProofCacheError> {
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        Self::build(
            HistoricalProofCacheConfig::conservative(snapshot),
            snapshot,
            true,
        )
    }

    /// Constructs a deterministic manually refreshed cache.
    ///
    /// # Errors
    ///
    /// Returns an allocation failure for the fixed shard directory.
    #[cfg(test)]
    fn new_with_snapshot(
        config: HistoricalProofCacheConfig,
        snapshot: MemoryPressureSnapshot,
    ) -> Result<Self, HistoricalProofCacheError> {
        Self::build(config, snapshot, false)
    }

    fn build(
        config: HistoricalProofCacheConfig,
        snapshot: MemoryPressureSnapshot,
        automatic_pressure: bool,
    ) -> Result<Self, HistoricalProofCacheError> {
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(config.shard_count.get())
            .map_err(|_| HistoricalProofCacheError::OutOfMemory)?;
        shards.resize_with(config.shard_count.get(), CacheShard::default);
        let cache = Self {
            config,
            shards: shards.into_boxed_slice(),
            pressure_gate: RwLock::new(()),
            target_entries: AtomicUsize::new(0),
            entry_count: AtomicUsize::new(0),
            unsharded_misses: AtomicU64::new(0),
            unsharded_admission_rejections: AtomicU64::new(0),
            effective_limit_bytes: AtomicU64::new(0),
            available_bytes: AtomicU64::new(0),
            swap_used_bytes: AtomicU64::new(0),
            automatic_pressure,
            started: Instant::now(),
            last_refresh_millis: AtomicU64::new(0),
        };
        cache.apply_memory_pressure(snapshot);
        Ok(cache)
    }

    /// Returns a verified Location on a full Chunk ID and length match.
    #[must_use]
    pub(crate) fn get(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        self.maybe_refresh_pressure();
        let Ok(logical_length) = u32::try_from(logical_length) else {
            self.unsharded_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let key = ProofKey::new(chunk_id, logical_length);
        let hash = proof_hash(key);
        let shard = &self.shards[shard_index(hash, self.shards.len())];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: Historical Proof Cache shard lock poisoned");
        let Some(slot_index) = find_slot(&state, key, hash) else {
            state.counters.misses = state.counters.misses.saturating_add(1);
            return None;
        };
        let slot = state
            .slots
            .get_mut(slot_index)
            .expect("ASSERT: Historical Proof Cache index points inside its slot arena");
        let entry = slot
            .entry
            .expect("ASSERT: Historical Proof Cache index points to an occupied slot");
        assert_entry_key(entry, key);
        slot.frequency = slot.frequency.saturating_add(1).min(3);
        state.counters.hits = state.counters.hits.saturating_add(1);
        Some(entry)
    }

    /// Admits one fully verified ACTIVE Location using the selected S3-FIFO origin rule.
    ///
    /// Allocation or pressure rejection is deliberately silent to callers.
    pub(crate) fn admit(&self, entry: ExactIndexEntry, admission: HistoricalProofAdmission) {
        assert_eq!(
            entry.transition(),
            ExactLocationTransition::Active,
            "ASSERT: Historical Proof Cache accepts only ACTIVE verified Locations"
        );
        self.maybe_refresh_pressure();
        let _pressure = self
            .pressure_gate
            .read()
            .expect("ASSERT: Historical Proof Cache pressure gate poisoned");
        let target = self.target_entries.load(Ordering::Acquire);
        let key = ProofKey::new(entry.chunk_id(), entry.logical_length());
        let hash = proof_hash(key);
        let shard_ordinal = shard_index(hash, self.shards.len());
        let shard_target = target_for_shard(target, shard_ordinal, self.shards.len());
        if shard_target == 0 {
            self.unsharded_admission_rejections
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let shard = &self.shards[shard_ordinal];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: Historical Proof Cache shard lock poisoned");
        if let Some(slot_index) = find_slot(&state, key, hash) {
            let slot = state
                .slots
                .get_mut(slot_index)
                .expect("ASSERT: Historical Proof Cache index points inside its slot arena");
            let previous = slot
                .entry
                .replace(entry)
                .expect("ASSERT: Historical Proof Cache index points to an occupied slot");
            assert_entry_key(previous, key);
            return;
        }
        if !reserve_admission(&mut state) {
            state.counters.allocation_rejections =
                state.counters.allocation_rejections.saturating_add(1);
            return;
        }
        let mut steps = 0_usize;
        let mut evictions = 0_usize;
        while state.entries >= shard_target {
            if !evict_one(&mut state, shard_target, &mut steps, &mut evictions) {
                state.counters.maximum_eviction_steps =
                    state.counters.maximum_eviction_steps.max(steps);
                state.counters.admission_rejections =
                    state.counters.admission_rejections.saturating_add(1);
                return;
            }
        }
        state.counters.maximum_eviction_steps = state.counters.maximum_eviction_steps.max(steps);
        if evictions != 0 {
            state.counters.evictions = state.counters.evictions.saturating_add(
                u64::try_from(evictions).expect("ASSERT: bounded eviction count fits u64"),
            );
            self.entry_count.fetch_sub(evictions, Ordering::Release);
        }
        let ghost_tag = ghost_tag(key);
        let ghost_hit = state.ghost.remove(&ghost_tag).is_some();
        if ghost_hit {
            state.counters.ghost_hits = state.counters.ghost_hits.saturating_add(1);
        }
        let queue = if admission == HistoricalProofAdmission::ExactReuse || ghost_hit {
            Queue::Main
        } else {
            Queue::Small
        };
        insert_entry(&mut state, entry, key, hash, queue)
            .expect("ASSERT: admission pre-reserved one slot and one Swiss-table entry");
        self.entry_count.fetch_add(1, Ordering::Release);
        state.counters.admissions = state.counters.admissions.saturating_add(1);
        assert!(
            self.entry_count.load(Ordering::Acquire) <= target,
            "ASSERT: Historical Proof Cache exceeded its distributed target"
        );
    }

    /// Applies an explicit pressure sample and purges history on Swap use.
    ///
    /// # Panics
    ///
    /// Panics when called on the automatically sampled production instance.
    #[cfg(test)]
    fn update_memory_pressure(&self, snapshot: MemoryPressureSnapshot) {
        assert!(
            !self.automatic_pressure,
            "ASSERT: automatic Historical Proof Cache owns its pressure sampler"
        );
        self.apply_memory_pressure(snapshot);
    }

    #[must_use]
    pub(crate) fn status(&self) -> HistoricalProofCacheStatus {
        self.maybe_refresh_pressure();
        let entries = self.entry_count.load(Ordering::Acquire);
        let mut counters = self.counters();
        counters.misses = counters
            .misses
            .saturating_add(self.unsharded_misses.load(Ordering::Relaxed));
        counters.admission_rejections = counters
            .admission_rejections
            .saturating_add(self.unsharded_admission_rejections.load(Ordering::Relaxed));
        HistoricalProofCacheStatus {
            hits: counters.hits,
            misses: counters.misses,
            admissions: counters.admissions,
            admission_rejections: counters.admission_rejections,
            allocation_rejections: counters.allocation_rejections,
            evictions: counters.evictions,
            ghost_hits: counters.ghost_hits,
            entry_count: entries,
            target_entries: self.target_entries.load(Ordering::Acquire),
            resident_bytes: entries.saturating_mul(ACCOUNTED_ENTRY_BYTES),
            metadata_bytes: self.shards.len().saturating_mul(size_of::<CacheShard>()),
            hard_limit_bytes: self.config.hard_limit_bytes,
            reserve_bytes: self.config.reserve_bytes,
            maximum_eviction_steps: counters.maximum_eviction_steps,
            effective_limit_bytes: self.effective_limit_bytes.load(Ordering::Acquire),
            available_bytes: self.available_bytes.load(Ordering::Acquire),
            swap_used_bytes: self.swap_used_bytes.load(Ordering::Acquire),
        }
    }

    fn maybe_refresh_pressure(&self) {
        if !self.automatic_pressure {
            return;
        }
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let previous = self.last_refresh_millis.load(Ordering::Relaxed);
        if elapsed.saturating_sub(previous) < REFRESH_MILLIS
            || self
                .last_refresh_millis
                .compare_exchange(previous, elapsed, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        self.apply_memory_pressure(snapshot);
    }

    fn apply_memory_pressure(&self, snapshot: MemoryPressureSnapshot) {
        let _pressure = self
            .pressure_gate
            .write()
            .expect("ASSERT: Historical Proof Cache pressure gate poisoned");
        let available = snapshot
            .available_bytes()
            .min(snapshot.effective_limit_bytes());
        let headroom = available.saturating_sub(self.config.reserve_bytes);
        let budget = if snapshot.swap_used_bytes() == 0 {
            headroom
                .checked_div(4)
                .unwrap_or(0)
                .min(snapshot.effective_limit_bytes() / EFFECTIVE_RAM_DIVISOR)
                .min(u64::try_from(self.config.hard_limit_bytes).unwrap_or(u64::MAX))
        } else {
            0
        };
        let metadata = self.shards.len().saturating_mul(size_of::<CacheShard>());
        let target = usize::try_from(budget)
            .unwrap_or(usize::MAX)
            .saturating_sub(metadata)
            / ACCOUNTED_ENTRY_BYTES;
        self.effective_limit_bytes
            .store(snapshot.effective_limit_bytes(), Ordering::Release);
        self.available_bytes
            .store(snapshot.available_bytes(), Ordering::Release);
        self.swap_used_bytes
            .store(snapshot.swap_used_bytes(), Ordering::Release);
        let previous_target = self.target_entries.swap(target, Ordering::AcqRel);
        if self.entry_count.load(Ordering::Acquire) > target
            || target < previous_target && self.entry_count.load(Ordering::Acquire) != 0
        {
            self.clear_locked();
        }
    }

    fn clear_locked(&self) {
        let mut removed = 0_usize;
        for shard in &self.shards {
            let mut state = shard
                .state
                .lock()
                .expect("ASSERT: Historical Proof Cache shard lock poisoned");
            removed = removed
                .checked_add(state.entries)
                .expect("ASSERT: Historical Proof Cache entry count cannot overflow");
            let counters = state.counters;
            *state = ShardState::default();
            state.counters = counters;
        }
        let accounted = self.entry_count.swap(0, Ordering::AcqRel);
        assert_eq!(
            accounted, removed,
            "ASSERT: Historical Proof Cache global and sharded counts agree"
        );
    }

    fn counters(&self) -> HistoricalShardCounters {
        self.shards
            .iter()
            .fold(HistoricalShardCounters::default(), |mut total, shard| {
                let state = shard
                    .state
                    .lock()
                    .expect("ASSERT: Historical Proof Cache shard lock poisoned");
                total.add_assign(state.counters);
                total
            })
    }
}

fn reserve_admission(state: &mut ShardState) -> bool {
    if state.free.is_empty()
        && (state.slots.try_reserve(1).is_err() || state.free.try_reserve(1).is_err())
    {
        return false;
    }
    let slots = &state.slots;
    state
        .index
        .try_reserve(1, |slot| slot_hash(slots, *slot))
        .is_ok()
}

fn insert_entry(
    state: &mut ShardState,
    entry: ExactIndexEntry,
    key: ProofKey,
    hash: u64,
    queue: Queue,
) -> Result<(), ()> {
    let index = if let Some(index) = state.free.pop() {
        index
    } else {
        let index = u32::try_from(state.slots.len()).map_err(|_| ())?;
        state.slots.push(CacheSlot::vacant());
        index
    };
    let slot = &mut state.slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")];
    assert!(
        slot.entry.is_none(),
        "ASSERT: allocated cache slot is vacant"
    );
    *slot = CacheSlot {
        entry: Some(entry),
        next: NONE,
        frequency: 0,
        queue,
    };
    push_queue(state, queue, index);
    let slots = &state.slots;
    state
        .index
        .insert_unique(hash, index, |slot| slot_hash(slots, *slot));
    state.entries = state
        .entries
        .checked_add(1)
        .expect("ASSERT: Historical Proof Cache shard count cannot overflow");
    assert_entry_key(entry, key);
    Ok(())
}

fn evict_one(
    state: &mut ShardState,
    target: usize,
    steps: &mut usize,
    evictions: &mut usize,
) -> bool {
    *steps = steps.saturating_add(1);
    if *steps > MAXIMUM_EVICTION_STEPS {
        *steps = MAXIMUM_EVICTION_STEPS;
        return false;
    }
    let small_target = (target / 10).max(1);
    if state.small.len >= small_target && state.small.len != 0 {
        let index = pop_queue(state, Queue::Small);
        let frequency =
            state.slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")].frequency;
        if frequency > 1 {
            let slot =
                &mut state.slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")];
            slot.queue = Queue::Main;
            slot.frequency = 0;
            push_queue(state, Queue::Main, index);
        } else {
            remove_resident(state, index, true);
            *evictions += 1;
        }
        return true;
    }
    if state.main.len != 0 {
        let index = pop_queue(state, Queue::Main);
        let slot = &mut state.slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")];
        if slot.frequency != 0 {
            slot.frequency -= 1;
            push_queue(state, Queue::Main, index);
        } else {
            remove_resident(state, index, false);
            *evictions += 1;
        }
        return true;
    }
    if state.small.len != 0 {
        let index = pop_queue(state, Queue::Small);
        remove_resident(state, index, true);
        *evictions += 1;
        return true;
    }
    false
}

fn remove_resident(state: &mut ShardState, index: u32, remember_ghost: bool) {
    let slot_ordinal = usize::try_from(index).expect("ASSERT: u32 slot fits usize");
    let entry = state.slots[slot_ordinal]
        .entry
        .expect("ASSERT: selected S3-FIFO victim is occupied");
    let key = ProofKey::new(entry.chunk_id(), entry.logical_length());
    let hash = proof_hash(key);
    let slots = &state.slots;
    let removed = state
        .index
        .find_entry(hash, |candidate| slot_matches(slots, *candidate, key))
        .expect("ASSERT: selected S3-FIFO victim owns one Swiss-table entry")
        .remove()
        .0;
    assert_eq!(removed, index, "ASSERT: S3-FIFO removed its selected slot");
    if remember_ghost {
        remember_ghost_entry(state, ghost_tag(key));
    }
    state.slots[slot_ordinal] = CacheSlot::vacant();
    state.free.push(index);
    state.entries -= 1;
}

fn remember_ghost_entry(state: &mut ShardState, tag: u64) {
    state.next_ghost_epoch = state
        .next_ghost_epoch
        .checked_add(1)
        .expect("ASSERT: Historical Proof Cache Ghost epoch cannot overflow");
    let epoch = state.next_ghost_epoch;
    if state.ghost.try_reserve(1).is_err() || state.ghost_fifo.try_reserve(1).is_err() {
        return;
    }
    state.ghost.insert(tag, epoch);
    state.ghost_fifo.push_back((tag, epoch));
    let target = state.entries.saturating_sub(state.small.len).max(1);
    while state.ghost_fifo.len() > target {
        let (old_tag, old_epoch) = state
            .ghost_fifo
            .pop_front()
            .expect("ASSERT: oversized Ghost FIFO is nonempty");
        if state.ghost.get(&old_tag) == Some(&old_epoch) {
            state.ghost.remove(&old_tag);
        }
    }
}

fn push_queue(state: &mut ShardState, queue: Queue, index: u32) {
    let fifo = match queue {
        Queue::Small => &mut state.small,
        Queue::Main => &mut state.main,
    };
    if fifo.tail == NONE {
        assert_eq!(fifo.head, NONE, "ASSERT: empty FIFO has no head");
        fifo.head = index;
    } else {
        state.slots[usize::try_from(fifo.tail).expect("ASSERT: u32 slot fits usize")].next = index;
    }
    fifo.tail = index;
    fifo.len = fifo
        .len
        .checked_add(1)
        .expect("ASSERT: S3-FIFO queue length cannot overflow");
}

fn pop_queue(state: &mut ShardState, queue: Queue) -> u32 {
    let fifo = match queue {
        Queue::Small => &mut state.small,
        Queue::Main => &mut state.main,
    };
    let index = fifo.head;
    assert_ne!(index, NONE, "ASSERT: cannot pop an empty S3-FIFO queue");
    let slot = &mut state.slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")];
    assert_eq!(slot.queue, queue, "ASSERT: slot belongs to the popped FIFO");
    fifo.head = slot.next;
    slot.next = NONE;
    fifo.len -= 1;
    if fifo.len == 0 {
        assert_eq!(fifo.head, NONE, "ASSERT: final FIFO slot has no successor");
        fifo.tail = NONE;
    }
    index
}

fn find_slot(state: &ShardState, key: ProofKey, hash: u64) -> Option<usize> {
    state
        .index
        .find(hash, |candidate| {
            slot_matches(&state.slots, *candidate, key)
        })
        .map(|index| usize::try_from(*index).expect("ASSERT: u32 slot fits usize"))
}

fn slot_matches(slots: &[CacheSlot], index: u32, key: ProofKey) -> bool {
    slots
        .get(usize::try_from(index).expect("ASSERT: u32 slot fits usize"))
        .and_then(|slot| slot.entry)
        .is_some_and(|entry| {
            entry.chunk_id() == key.chunk_id() && entry.logical_length() == key.logical_length()
        })
}

fn slot_hash(slots: &[CacheSlot], index: u32) -> u64 {
    let entry = slots[usize::try_from(index).expect("ASSERT: u32 slot fits usize")]
        .entry
        .expect("ASSERT: Swiss-table entry points to an occupied slot");
    proof_hash(ProofKey::new(entry.chunk_id(), entry.logical_length()))
}

fn assert_entry_key(entry: ExactIndexEntry, key: ProofKey) {
    assert_eq!(
        entry.chunk_id(),
        key.chunk_id(),
        "ASSERT: Historical Proof Cache key matches its verified Location"
    );
    assert_eq!(
        entry.logical_length(),
        key.logical_length(),
        "ASSERT: Historical Proof Cache length matches its verified Location"
    );
}

fn proof_hash(key: ProofKey) -> u64 {
    let bytes = key.chunk_id().bytes();
    let low = u64::from_le_bytes(
        bytes[0..8]
            .try_into()
            .expect("ASSERT: Chunk ID prefix is 8 bytes"),
    );
    let high = u64::from_le_bytes(
        bytes[24..32]
            .try_into()
            .expect("ASSERT: Chunk ID suffix is 8 bytes"),
    );
    low ^ high.rotate_left(23) ^ u64::from(key.logical_length()).wrapping_mul(0x9E37_79B1_85EB_CA87)
}

fn ghost_tag(key: ProofKey) -> u64 {
    proof_hash(key) ^ 0xD6E8_FEB8_6659_FD93
}

fn shard_index(hash: u64, shards: usize) -> usize {
    let folded = hash ^ (hash >> 32);
    let low = u32::try_from(folded & u64::from(u32::MAX)).expect("ASSERT: masked hash fits u32");
    usize::try_from(low).expect("ASSERT: u32 hash fits usize") & (shards - 1)
}

fn target_for_shard(total: usize, ordinal: usize, shards: usize) -> usize {
    total / shards + usize::from(ordinal < total % shards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{ContainerId, ExactIndexLocation};

    fn entry(ordinal: u64) -> ExactIndexEntry {
        let mut chunk = [0_u8; 32];
        chunk[..8].copy_from_slice(&ordinal.to_le_bytes());
        let mut container = [0_u8; 16];
        container[..8].copy_from_slice(
            &ordinal
                .checked_add(1)
                .expect("worked ordinal does not overflow")
                .to_le_bytes(),
        );
        let logical_length = 64_u32 * 1_024;
        let record_length = logical_length
            .checked_add(255)
            .expect("worked RAW record length fits")
            / 64
            * 64;
        let location = ExactIndexLocation::raw(
            ContainerId::new(container).expect("worked Container identity is nonzero"),
            ordinal.checked_add(1).expect("worked generation fits"),
            4_096,
            record_length,
            u32::try_from(ordinal).unwrap_or(u32::MAX),
        )
        .expect("worked RAW Location is valid");
        ExactIndexEntry::active(ChunkId::from_bytes(chunk), logical_length, location)
            .expect("worked Exact entry is valid")
    }

    fn cache() -> HistoricalProofCache {
        let config = HistoricalProofCacheConfig {
            hard_limit_bytes: 16 * 1_024,
            reserve_bytes: 0,
            shard_count: NonZeroUsize::new(1).expect("one shard is nonzero"),
        };
        HistoricalProofCache::new_with_snapshot(
            config,
            MemoryPressureSnapshot::new(1 << 30, 1 << 30, 0),
        )
        .expect("worked cache allocation succeeds")
    }

    #[test]
    fn exact_reuse_stays_in_main_during_a_published_scan() {
        let cache = cache();
        let capacity = cache.status().target_entries();
        assert!(capacity >= 10, "fixture must exercise eviction");
        let hot = entry(1);
        cache.admit(hot, HistoricalProofAdmission::ExactReuse);
        for ordinal in 2..=u64::try_from(capacity * 4).expect("worked scan fits u64") {
            cache.admit(entry(ordinal), HistoricalProofAdmission::Published);
        }
        assert_eq!(
            cache.get(hot.chunk_id(), u64::from(hot.logical_length())),
            Some(hot)
        );
        let status = cache.status();
        assert!(status.evictions() > 0);
        assert!(status.entry_count() <= status.target_entries());
        assert!(status.maximum_eviction_steps() <= MAXIMUM_EVICTION_STEPS);
    }

    #[test]
    fn swap_pressure_purges_all_historical_state() {
        let cache = cache();
        let proof = entry(500);
        cache.admit(proof, HistoricalProofAdmission::Published);
        assert_eq!(
            cache.get(proof.chunk_id(), u64::from(proof.logical_length())),
            Some(proof)
        );
        assert_eq!(cache.status().admissions(), 1);
        assert_eq!(cache.status().hits(), 1);
        cache.update_memory_pressure(MemoryPressureSnapshot::new(1 << 30, 1 << 30, 1));
        assert_eq!(cache.status().target_entries(), 0);
        assert_eq!(cache.status().entry_count(), 0);
        assert_eq!(cache.status().admissions(), 1);
        assert_eq!(cache.status().hits(), 1);
        assert_eq!(
            cache.get(proof.chunk_id(), u64::from(proof.logical_length())),
            None
        );
        assert_eq!(cache.status().misses(), 1);
    }

    #[test]
    fn shard_and_slot_geometry_stays_cache_local_and_conservatively_accounted() {
        assert_eq!(std::mem::align_of::<CacheShard>(), 64);
        assert!(std::mem::size_of::<CacheSlot>() <= 160);
        assert!(ACCOUNTED_ENTRY_BYTES >= std::mem::size_of::<CacheSlot>() + 32);
        assert!(DEFAULT_SHARDS.is_power_of_two());
    }
}
