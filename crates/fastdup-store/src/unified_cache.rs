//! One owner and replacement policy for all reusable storage reads.
//!
//! Typed namespaces carry identity and telemetry, never a private byte quota,
//! resident map, or victim list. Hits lock one shard. Admission, allocation
//! accounting and pressure reclamation are serialized outside I/O and codecs.

use crate::{CacheFallback, CacheObservation, CachePool, MemoryPressureSnapshot};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Instant;

const SHARDS: usize = 64;
const ADMISSION_STEPS: usize = 4096;
// Covers the key, value/owner handles and spare HashMap/FIFO capacity. Empty
// maps are released; nonempty maps are shrunk whenever ownership is removed.
const ENTRY_BYTES: u64 = 512;
const MAX_FLIGHTS: usize = 128;
const STALE_CLOCK_SCAN_THRESHOLD: usize = 1024;
const VERIFIED_DATA_SHARE_BASIS_POINTS: u64 = 2_000;
const VERIFIED_DATA_MINIMUM_BYTES: u64 = 4096 + ENTRY_BYTES;
const PROTECTED_EXACT_SHARE_BASIS_POINTS: u64 = 7_000;
const PROTECTED_EXACT_MINIMUM_BYTES: u64 = 4096 + ENTRY_BYTES;

type SharedLoad = Result<Arc<dyn Any + Send + Sync>, Arc<io::Error>>;
pub(crate) type AdmissionGroup<T> = (Vec<(ReadCacheKey, Arc<T>, u64)>, u64);
#[derive(Default)]
struct Flight {
    result: Mutex<Option<SharedLoad>>,
    ready: Condvar,
}

struct FlightLeader<'a> {
    core: &'a Core,
    key: Key,
    flight: Arc<Flight>,
}
impl Drop for FlightLeader<'_> {
    fn drop(&mut self) {
        // Also wakes waiters on loader unwind. No I/O runs under either lock.
        let mut result = self.flight.result.lock().expect("ASSERT: flight poisoned");
        if result.is_none() {
            *result = Some(Err(Arc::new(io::Error::other(
                "shared read loader interrupted",
            ))));
        }
        self.flight.ready.notify_all();
        drop(result);
        self.core
            .flights
            .lock()
            .expect("ASSERT: flight directory poisoned")
            .remove(&self.key);
    }
}

/// Reusable representations competing in the same cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadCacheClass {
    Data,
    LocationProof,
    HistoricalProof,
    ContainerDescriptor,
    ContainerImage,
    ExactPage,
    SimilarityPage,
    MetadataObject,
    ManifestNode,
    StorageRange,
    StorageHandle,
    ExactMembership,
    ExactPageBounds,
    ReverseDependencies,
}

impl ReadCacheClass {
    const fn hit_credit(self) -> u8 {
        match self {
            Self::Data
            | Self::LocationProof
            | Self::HistoricalProof
            | Self::ContainerDescriptor => 16,
            _ => 1,
        }
    }

    #[must_use]
    pub(crate) const fn budget_group(self) -> ReadCacheBudgetGroup {
        match self {
            Self::Data => ReadCacheBudgetGroup::VerifiedData,
            Self::ExactPage | Self::ExactMembership | Self::ExactPageBounds => {
                ReadCacheBudgetGroup::ProtectedExact
            }
            _ => ReadCacheBudgetGroup::Other,
        }
    }

    const fn pinned(self) -> bool {
        matches!(self, Self::ExactMembership)
    }
}

/// Resident classes governed by the common displacement and Verified DATA rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadCacheBudgetGroup {
    VerifiedData,
    ProtectedExact,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvictionScope {
    VerifiedData,
    ProtectedExact,
    Unprotected,
    All,
}

/// An identity within an immutable repository namespace. Both fields are
/// compared in full; hashes are used only to select a shard.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReadCacheKey {
    pub identity: [u8; 32],
    pub ordinal: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Key {
    namespace: u64,
    object: ReadCacheKey,
}

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    hit_bytes: AtomicU64,
    admissions: AtomicU64,
    evictions: AtomicU64,
    rejections: AtomicU64,
    entries: AtomicU64,
    resident: AtomicU64,
}

/// Per-representation observations; capacity belongs to the common cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub hit_bytes: u64,
    pub admissions: u64,
    pub evictions: u64,
    pub rejections: u64,
    pub entries: u64,
    pub resident_bytes: u64,
}

impl Counters {
    fn snapshot(&self) -> ReadCacheStats {
        ReadCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            hit_bytes: self.hit_bytes.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
            resident_bytes: self.resident.load(Ordering::Relaxed),
        }
    }
}

struct Allocation {
    bytes: u64,
    counters: Arc<Counters>,
}

struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    allocation: Arc<Allocation>,
    hit_bytes: u64,
    class: ReadCacheClass,
    credit: u8,
}

#[derive(Default)]
struct Entries {
    map: HashMap<Key, Entry>,
    verified_clock: VecDeque<Key>,
    protected_clock: VecDeque<Key>,
    other_clock: VecDeque<Key>,
    pinned_clock: VecDeque<Key>,
    stale_keys: usize,
}

impl Entries {
    fn clock(&mut self, group: ReadCacheBudgetGroup) -> &mut VecDeque<Key> {
        match group {
            ReadCacheBudgetGroup::VerifiedData => &mut self.verified_clock,
            ReadCacheBudgetGroup::ProtectedExact => &mut self.protected_clock,
            ReadCacheBudgetGroup::Other => &mut self.other_clock,
        }
    }

    fn pop(&mut self, scope: EvictionScope) -> Option<Key> {
        match scope {
            EvictionScope::VerifiedData => self.verified_clock.pop_front(),
            EvictionScope::ProtectedExact => self.protected_clock.pop_front(),
            EvictionScope::Unprotected => self
                .verified_clock
                .pop_front()
                .or_else(|| self.other_clock.pop_front()),
            EvictionScope::All => self
                .verified_clock
                .pop_front()
                .or_else(|| self.other_clock.pop_front())
                .or_else(|| self.protected_clock.pop_front()),
        }
    }

    fn push(&mut self, class: ReadCacheClass, key: Key) {
        if class.pinned() {
            self.pinned_clock.push_back(key);
        } else {
            self.clock(class.budget_group()).push_back(key);
        }
    }

    fn reserve(&mut self, class: ReadCacheClass) -> Result<(), ()> {
        if class.pinned() {
            self.pinned_clock.try_reserve(1).map_err(|_| ())
        } else {
            self.clock(class.budget_group())
                .try_reserve(1)
                .map_err(|_| ())
        }
    }

    fn clocks_len(&self) -> usize {
        self.verified_clock.len()
            + self.protected_clock.len()
            + self.other_clock.len()
            + self.pinned_clock.len()
    }

    fn reclaim_clocks(&mut self) {
        if self.stale_keys == 0
            || self.stale_keys < STALE_CLOCK_SCAN_THRESHOLD
            || self.stale_keys.saturating_mul(2) <= self.clocks_len()
        {
            return;
        }
        let Self {
            map,
            verified_clock,
            protected_clock,
            other_clock,
            pinned_clock,
            stale_keys,
        } = self;
        for clock in [verified_clock, protected_clock, other_clock, pinned_clock] {
            clock.retain(|key| map.contains_key(key));
        }
        *stale_keys = 0;
    }

    fn purge_namespace(&mut self, namespace: u64) -> Vec<Entry> {
        let Self {
            map,
            verified_clock,
            protected_clock,
            other_clock,
            pinned_clock,
            stale_keys,
        } = self;
        let mut removed = Vec::new();
        for clock in [verified_clock, protected_clock, other_clock, pinned_clock] {
            let mut retained = VecDeque::new();
            while let Some(key) = clock.pop_front() {
                if key.namespace == namespace {
                    if let Some(entry) = map.remove(&key) {
                        removed.push(entry);
                    } else {
                        *stale_keys = stale_keys.saturating_sub(1);
                    }
                } else if map.contains_key(&key) {
                    retained.push_back(key);
                } else {
                    *stale_keys = stale_keys.saturating_sub(1);
                }
            }
            *clock = retained;
        }
        removed
    }

    fn compact(&mut self) {
        // The per-entry charge includes up to 2x spare capacity. Reallocating
        // a VecDeque after every eviction would turn replacement quadratic.
        if self.map.capacity() > self.map.len().saturating_mul(2) {
            self.map.shrink_to_fit();
        }
        for clock in [
            &mut self.verified_clock,
            &mut self.protected_clock,
            &mut self.other_clock,
            &mut self.pinned_clock,
        ] {
            if clock.capacity() > clock.len().saturating_mul(2) {
                clock.shrink_to_fit();
            }
        }
    }
}

#[repr(align(64))]
struct Shard(Mutex<Entries>);

#[derive(Default)]
struct Admission {
    resident: u64,
    pinned_bytes: u64,
    target: u64,
    cursor: usize,
    verified_bytes: u64,
    protected_bytes: u64,
    other_bytes: u64,
}

impl Admission {
    fn charge(&mut self, class: ReadCacheClass, bytes: u64) {
        if class.pinned() {
            self.pinned_bytes = self.pinned_bytes.saturating_add(bytes);
            return;
        }
        self.resident = self.resident.saturating_add(bytes);
        match class.budget_group() {
            ReadCacheBudgetGroup::VerifiedData => {
                self.verified_bytes = self.verified_bytes.saturating_add(bytes);
            }
            ReadCacheBudgetGroup::ProtectedExact => {
                self.protected_bytes = self.protected_bytes.saturating_add(bytes);
            }
            ReadCacheBudgetGroup::Other => {
                self.other_bytes = self.other_bytes.saturating_add(bytes);
            }
        }
    }

    fn uncharge(&mut self, class: ReadCacheClass, bytes: u64) {
        if class.pinned() {
            self.pinned_bytes = self.pinned_bytes.saturating_sub(bytes);
            return;
        }
        self.resident = self.resident.saturating_sub(bytes);
        match class.budget_group() {
            ReadCacheBudgetGroup::VerifiedData => {
                self.verified_bytes = self.verified_bytes.saturating_sub(bytes);
            }
            ReadCacheBudgetGroup::ProtectedExact => {
                self.protected_bytes = self.protected_bytes.saturating_sub(bytes);
            }
            ReadCacheBudgetGroup::Other => {
                self.other_bytes = self.other_bytes.saturating_sub(bytes);
            }
        }
    }

    const fn total_resident(&self) -> u64 {
        self.resident.saturating_add(self.pinned_bytes)
    }

    fn unprotected_bytes(&self) -> u64 {
        self.verified_bytes.saturating_add(self.other_bytes)
    }
}

struct Core {
    shards: Box<[Shard]>,
    admission: Mutex<Admission>,
    flights: Mutex<HashMap<Key, Arc<Flight>>>,
    counters: Counters,
    started: Instant,
    next_refresh: AtomicU64,
    snapshot: Mutex<MemoryPressureSnapshot>,
    // Resident owners precede the one broker lease in drop order.
    pool: Option<CachePool>,
}

fn stable_capacity_target(target: u64) -> u64 {
    const MIB: u64 = 1 << 20;
    let granularity = (2 * MIB).min(target);
    target
        .checked_div(granularity)
        .unwrap_or_default()
        .saturating_mul(granularity)
}

fn verified_data_limit(target: u64) -> u64 {
    let target = stable_capacity_target(target);
    if target == 0 {
        return 0;
    }
    let share = target.saturating_mul(VERIFIED_DATA_SHARE_BASIS_POINTS) / 10_000;
    share.max(VERIFIED_DATA_MINIMUM_BYTES).min(target)
}

fn protected_exact_limit(target: u64) -> u64 {
    let target = stable_capacity_target(target);
    if target == 0 {
        return 0;
    }
    let share = target.saturating_mul(PROTECTED_EXACT_SHARE_BASIS_POINTS) / 10_000;
    share.max(PROTECTED_EXACT_MINIMUM_BYTES).min(target)
}

impl Core {
    fn new(target: u64, automatic: bool) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Shard(Mutex::new(Entries::default())))
                .collect(),
            admission: Mutex::new(Admission {
                target,
                ..Admission::default()
            }),
            flights: Mutex::new(HashMap::new()),
            counters: Counters::default(),
            started: Instant::now(),
            next_refresh: AtomicU64::new(0),
            snapshot: Mutex::new(MemoryPressureSnapshot::new(0, 0, 0)),
            pool: automatic.then(|| {
                CachePool::system(
                    "unifiedRead",
                    CacheFallback::Data,
                    Self::fixed_bytes(),
                    u64::MAX,
                )
            }),
        }
    }

    fn fixed_bytes() -> u64 {
        (size_of::<Self>() + SHARDS * size_of::<Shard>() + MAX_FLIGHTS * 1024) as u64
    }

    fn shard(key: Key) -> usize {
        let mut hash = key.namespace ^ key.object.ordinal;
        for bytes in key.object.identity.chunks_exact(8) {
            hash = hash.rotate_left(13)
                ^ u64::from_le_bytes(bytes.try_into().expect("ASSERT: eight identity bytes"));
        }
        usize::try_from(hash & (SHARDS as u64 - 1)).expect("ASSERT: shard index fits usize")
    }

    fn refresh(&self) {
        let Some(pool) = &self.pool else { return };
        // The shared CAS window is the only throttle. A per-thread memo would
        // freeze sampling once every live thread has seen one sample, because
        // no later thread ever reaches the window again.
        let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let next = self.next_refresh.load(Ordering::Relaxed);
        if now < next
            || self
                .next_refresh
                .compare_exchange(
                    next,
                    now.saturating_add(250),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        *self
            .snapshot
            .lock()
            .expect("ASSERT: cache pressure poisoned") = snapshot;
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        let observed = self.counters.snapshot();
        let target = pool.target_with_pinned_floor(
            snapshot,
            CacheObservation {
                hits: observed.hits,
                misses: observed.misses,
                hit_bytes: observed.hit_bytes,
                evictions: observed.evictions,
                resident_bytes: Self::fixed_bytes().saturating_add(state.total_resident()),
            },
            state.pinned_bytes,
        );
        state.target = target.saturating_sub(Self::fixed_bytes());
        // Class ceilings are common-owner placement rules, not private caches.
        // A lowered target must immediately reclaim an over-large protected
        // Exact working set before the ordinary pressure pass considers all
        // residents.
        let ceilings = self.reclaim_class_ceilings(&mut state);
        // Pressure eviction does not grant second chances. Unprotected classes
        // yield before protected Exact acceleration; all cache owners leave
        // before the common lease is returned to the memory broker.
        let reached = ceilings && self.reclaim_pressure(&mut state);
        debug_assert!(reached, "ASSERT: charged cache has an owner");
        if reached {
            pool.applied_with_pinned_floor(
                target,
                Self::fixed_bytes().saturating_add(state.resident),
                state.pinned_bytes,
            );
        }
    }

    fn get<T: Any + Send + Sync>(&self, key: Key, counters: &Counters) -> Option<Arc<T>> {
        self.refresh();
        let mut shard = self.shards[Self::shard(key)]
            .0
            .lock()
            .expect("ASSERT: cache shard poisoned");
        if let Some(entry) = shard.map.get_mut(&key) {
            let value = Arc::clone(&entry.value)
                .downcast::<T>()
                .expect("ASSERT: a typed cache namespace cannot change representation type");
            entry.credit = entry
                .credit
                .saturating_add(entry.class.hit_credit())
                .min(32);
            for observation in [counters, &self.counters] {
                observation.hits.fetch_add(1, Ordering::Relaxed);
                observation
                    .hit_bytes
                    .fetch_add(entry.hit_bytes, Ordering::Relaxed);
            }
            return Some(value);
        }
        for observation in [counters, &self.counters] {
            observation.misses.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    fn reclaim_pressure(&self, state: &mut Admission) -> bool {
        while state.resident > state.target {
            if state.unprotected_bytes() > 0 && self.evict(state, false, EvictionScope::Unprotected)
            {
                continue;
            }
            if !self.evict(state, false, EvictionScope::All) {
                return false;
            }
        }
        true
    }

    fn reclaim_class_ceilings(&self, state: &mut Admission) -> bool {
        let limit = protected_exact_limit(state.target);
        while state.protected_bytes > limit {
            if !self.evict(state, false, EvictionScope::ProtectedExact) {
                return false;
            }
        }
        true
    }

    fn uncharge(&self, state: &mut Admission, entry: Entry) {
        let counters = &entry.allocation.counters;
        let payload = if Arc::strong_count(&entry.allocation) == 1 {
            entry.allocation.bytes
        } else {
            0
        };
        let charge = ENTRY_BYTES + payload;
        state.uncharge(entry.class, charge);
        for observation in [counters.as_ref(), &self.counters] {
            observation.resident.fetch_sub(charge, Ordering::Relaxed);
            observation.entries.fetch_sub(1, Ordering::Relaxed);
            observation.evictions.fetch_add(1, Ordering::Relaxed);
        }
        // The returned immutable Arc may outlive eviction. That reader memory
        // continues to reduce measured headroom; eviction cannot invalidate it.
        drop(entry);
    }

    fn evict(&self, state: &mut Admission, second_chance: bool, scope: EvictionScope) -> bool {
        for _ in 0..SHARDS {
            let index = state.cursor % SHARDS;
            state.cursor = state.cursor.wrapping_add(1);
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: cache shard poisoned");
            // Stale heads must not mask live owners behind them: consume this
            // shard's scoped queue until it yields a victim or runs empty.
            while let Some(key) = shard.pop(scope) {
                let Some(entry) = shard.map.get_mut(&key) else {
                    shard.stale_keys = shard.stale_keys.saturating_sub(1);
                    shard.reclaim_clocks();
                    continue;
                };
                let class = entry.class;
                if second_chance && entry.credit != 0 {
                    entry.credit -= 1;
                    shard.push(class, key);
                    return true;
                }
                let entry = shard
                    .map
                    .remove(&key)
                    .expect("ASSERT: cache clock covers owners");
                shard.compact();
                self.uncharge(state, entry);
                return true;
            }
        }
        false
    }

    fn remove(&self, key: Key) {
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        let mut shard = self.shards[Self::shard(key)]
            .0
            .lock()
            .expect("ASSERT: cache shard poisoned");
        if let Some(entry) = shard.map.remove(&key) {
            shard.stale_keys += 1;
            shard.reclaim_clocks();
            shard.compact();
            self.uncharge(&mut state, entry);
        }
    }

    fn purge_tracked_namespace(&self, keys: Vec<Key>) {
        if keys.is_empty() {
            return;
        }
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        for key in keys {
            let mut shard = self.shards[Self::shard(key)]
                .0
                .lock()
                .expect("ASSERT: cache shard poisoned");
            if let Some(entry) = shard.map.remove(&key) {
                shard.stale_keys += 1;
                shard.reclaim_clocks();
                shard.compact();
                self.uncharge(&mut state, entry);
            }
        }
    }

    fn purge_namespace(&self, namespace: u64) {
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        for slot in &self.shards {
            let mut shard = slot.0.lock().expect("ASSERT: cache shard poisoned");
            for entry in shard.purge_namespace(namespace) {
                self.uncharge(&mut state, entry);
            }
            shard.compact();
        }
    }
}

struct Namespace {
    core: Arc<Core>,
    id: u64,
    class: ReadCacheClass,
    counters: Arc<Counters>,
    owned: Option<Mutex<Vec<Key>>>,
}

impl Namespace {
    fn purge_ownership(&self) {
        if let Some(owned) = &self.owned {
            let keys =
                std::mem::take(&mut *owned.lock().expect("ASSERT: cache owner list poisoned"));
            self.core.purge_tracked_namespace(keys);
        } else {
            self.core.purge_namespace(self.id);
        }
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        self.purge_ownership();
    }
}

/// A typed client's identity and counters in the unified cache. Clones share
/// entries; a fresh namespace cannot borrow a different repository's evidence.
#[derive(Clone)]
pub struct ReadCacheNamespace(Arc<Namespace>);

impl fmt::Debug for ReadCacheNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCacheNamespace")
            .field("class", &self.0.class)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl ReadCacheNamespace {
    /// Joins the process cache without reserving a representation-specific share.
    #[must_use]
    pub fn system(class: ReadCacheClass) -> Self {
        Self::attach(Self::system_core(), class, false)
    }

    /// Uses the same implementation with a deterministic budget for tests or
    /// an externally governed embedded repository.
    #[must_use]
    pub fn isolated(class: ReadCacheClass, capacity: u64) -> Self {
        Self::attach(Arc::new(Core::new(capacity, false)), class, false)
    }

    fn system_core() -> Arc<Core> {
        static CORE: OnceLock<Arc<Core>> = OnceLock::new();
        Arc::clone(CORE.get_or_init(|| Arc::new(Core::new(0, true))))
    }

    fn attach(core: Arc<Core>, class: ReadCacheClass, tracked: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id, u64::MAX, "ASSERT: cache namespace identities exhausted");
        Self(Arc::new(Namespace {
            core,
            id,
            class,
            counters: Arc::new(Counters::default()),
            owned: tracked.then(|| Mutex::new(Vec::new())),
        }))
    }

    /// Creates another representation in the same cache and memory budget.
    #[must_use]
    pub fn sibling(&self, class: ReadCacheClass) -> Self {
        Self::attach(Arc::clone(&self.0.core), class, false)
    }

    #[must_use]
    pub(crate) fn ephemeral_sibling(&self, class: ReadCacheClass) -> Self {
        Self::attach(Arc::clone(&self.0.core), class, true)
    }

    fn key(&self, object: ReadCacheKey) -> Key {
        Key {
            namespace: self.0.id,
            object,
        }
    }

    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self, key: ReadCacheKey) -> Option<Arc<T>> {
        if crate::read_intent::independent() {
            return None;
        }
        self.0.core.get(self.key(key), &self.0.counters)
    }

    /// Shares overlapping cold work for one complete immutable request. This
    /// retains only an in-flight result; ordinary admission remains explicit.
    /// Independent verification always performs its own load. Saturation
    /// falls back to the caller's bounded ordinary I/O.
    ///
    /// # Errors
    /// Returns the loader's error to every waiter.
    /// # Panics
    /// Panics for poisoned coordination or inconsistent request types.
    pub(crate) fn coalesce<T: Any + Send + Sync>(
        &self,
        object: ReadCacheKey,
        fetch: impl FnOnce() -> io::Result<Arc<T>>,
    ) -> io::Result<Arc<T>> {
        if crate::read_intent::independent() {
            return fetch();
        }
        let core = &self.0.core;
        let key = self.key(object);
        let mut directory = core
            .flights
            .lock()
            .expect("ASSERT: flight directory poisoned");
        let (flight, leader) = if let Some(flight) = directory.get(&key) {
            (Arc::clone(flight), false)
        } else if directory.len() < MAX_FLIGHTS {
            let flight = Arc::new(Flight::default());
            directory.insert(key, Arc::clone(&flight));
            (flight, true)
        } else {
            drop(directory);
            return fetch();
        };
        drop(directory);
        if leader {
            let _guard = FlightLeader {
                core,
                key,
                flight: Arc::clone(&flight),
            };
            let loaded = fetch();
            let shared = loaded
                .map(|value| value as Arc<dyn Any + Send + Sync>)
                .map_err(Arc::new);
            *flight.result.lock().expect("ASSERT: flight poisoned") = Some(shared);
        }
        let result = flight.result.lock().expect("ASSERT: flight poisoned");
        let result = flight
            .ready
            .wait_while(result, |result| result.is_none())
            .expect("ASSERT: flight poisoned");
        match result
            .as_ref()
            .expect("ASSERT: completed flight has a result")
        {
            Ok(value) => Ok(Arc::clone(value)
                .downcast::<T>()
                .expect("ASSERT: shared request type")),
            Err(error) => Err(error.raw_os_error().map_or_else(
                || io::Error::new(error.kind(), Arc::clone(error)),
                io::Error::from_raw_os_error,
            )),
        }
    }

    /// Inspects residency without recording a hit. Intended for cold telemetry.
    ///
    /// # Panics
    /// Panics on a poisoned shard or inconsistent representation type.
    #[must_use]
    pub fn peek<T: Any + Send + Sync>(&self, object: ReadCacheKey) -> Option<Arc<T>> {
        let key = self.key(object);
        let shard = self.0.core.shards[Core::shard(key)]
            .0
            .lock()
            .expect("ASSERT: cache shard poisoned");
        shard.map.get(&key).map(|entry| {
            Arc::clone(&entry.value)
                .downcast::<T>()
                .expect("ASSERT: namespace type")
        })
    }

    /// Admits one already validated representation. Rejection affects only reuse.
    pub fn insert<T: Any + Send + Sync>(
        &self,
        key: ReadCacheKey,
        value: Arc<T>,
        bytes: u64,
        hit_bytes: u64,
    ) {
        self.insert_group(vec![(key, value, hit_bytes)], bytes);
    }

    /// Charges a shared backing once, even when several logical views own it.
    /// The caller retains all incoming views until this operation completes.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    pub fn insert_group<T: Any + Send + Sync>(
        &self,
        values: Vec<(ReadCacheKey, Arc<T>, u64)>,
        bytes: u64,
    ) {
        self.insert_groups(vec![(values, bytes)]);
    }

    /// Reserves a whole read's representations before inserting any of them.
    /// Each group shares one backing; independently compressed siblings use
    /// separate groups so eviction releases their exact ownership charges.
    /// New siblings cannot displace one another during this admission.
    ///
    /// Scan intent fills only eviction-free headroom: it reserves under the
    /// same class ceilings but skips every victim search, so it can neither
    /// displace a resident entry nor exceed the common target. Independent
    /// intent still declines all admission except pinned working acceleration.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn insert_groups<T: Any + Send + Sync>(
        &self,
        groups: Vec<AdmissionGroup<T>>,
    ) -> u64 {
        let scan_fill = !self.0.class.pinned() && crate::read_intent::scan();
        if groups.is_empty()
            || (crate::read_intent::bypass_admission() && !self.0.class.pinned() && !scan_fill)
        {
            return 0;
        }
        let core = &self.0.core;
        core.refresh();
        let mut state = core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        // Repeated or concurrent admission must not evict useful entries just
        // to discover that this complete identity is already resident.
        let mut seen = std::collections::HashSet::new();
        let groups: Vec<_> = groups
            .into_iter()
            .filter_map(|(values, bytes)| {
                let values: Vec<_> = values
                    .into_iter()
                    .filter(|(object, _, _)| {
                        if !seen.insert(*object) {
                            return false;
                        }
                        let key = self.key(*object);
                        !core.shards[Core::shard(key)]
                            .0
                            .lock()
                            .expect("ASSERT: cache shard poisoned")
                            .map
                            .contains_key(&key)
                    })
                    .collect();
                (!values.is_empty()).then_some((values, bytes))
            })
            .collect();
        if groups.is_empty() {
            return 0;
        }
        let added = groups.iter().fold(0_u64, |sum, (values, bytes)| {
            sum.saturating_add(*bytes)
                .saturating_add(ENTRY_BYTES.saturating_mul(values.len() as u64))
        });
        let reject = || {
            self.0.counters.rejections.fetch_add(1, Ordering::Relaxed);
        };
        let pinned = self.0.class.pinned();
        if !pinned && added > state.target {
            reject();
            return 0;
        }
        let group = self.0.class.budget_group();
        let mut steps = if pinned || scan_fill {
            0
        } else {
            ADMISSION_STEPS
        };
        if !pinned {
            if group == ReadCacheBudgetGroup::VerifiedData {
                let limit = verified_data_limit(state.target);
                while state.verified_bytes.saturating_add(added) > limit && steps != 0 {
                    if !core.evict(&mut state, true, EvictionScope::VerifiedData) {
                        break;
                    }
                    steps -= 1;
                }
                if state.verified_bytes.saturating_add(added) > limit {
                    reject();
                    return 0;
                }
            }
            if group == ReadCacheBudgetGroup::ProtectedExact {
                let limit = protected_exact_limit(state.target);
                while state.protected_bytes.saturating_add(added) > limit && steps != 0 {
                    if !core.evict(&mut state, true, EvictionScope::ProtectedExact) {
                        break;
                    }
                    steps -= 1;
                }
                if state.protected_bytes.saturating_add(added) > limit {
                    reject();
                    return 0;
                }
            }
            while state.resident.saturating_add(added) > state.target && steps != 0 {
                let scope = if group == ReadCacheBudgetGroup::ProtectedExact {
                    EvictionScope::ProtectedExact
                } else {
                    EvictionScope::Unprotected
                };
                if core.evict(&mut state, true, scope) {
                    steps -= 1;
                    continue;
                }
                if group == ReadCacheBudgetGroup::ProtectedExact
                    && core.evict(&mut state, true, EvictionScope::Unprotected)
                {
                    steps -= 1;
                    continue;
                }
                break;
            }
            if state.resident.saturating_add(added) > state.target {
                reject();
                return 0;
            }
        }
        let mut admitted = 0;
        let tracked = self.0.owned.is_some();
        let mut owned_keys = if tracked {
            Vec::with_capacity(groups.iter().map(|(values, _)| values.len()).sum())
        } else {
            Vec::new()
        };
        for (values, bytes) in groups {
            let allocation = Arc::new(Allocation {
                bytes,
                counters: Arc::clone(&self.0.counters),
            });
            let mut charged = false;
            for (object, value, hit_bytes) in values {
                let key = self.key(object);
                let mut shard = core.shards[Core::shard(key)]
                    .0
                    .lock()
                    .expect("ASSERT: cache shard poisoned");
                if shard.map.contains_key(&key) {
                    continue;
                }
                if shard.map.try_reserve(1).is_err() || shard.reserve(self.0.class).is_err() {
                    shard.compact();
                    reject();
                    continue;
                }
                shard.map.insert(
                    key,
                    Entry {
                        value,
                        allocation: Arc::clone(&allocation),
                        hit_bytes,
                        class: self.0.class,
                        credit: 0,
                    },
                );
                shard.push(self.0.class, key);
                if tracked {
                    owned_keys.push(key);
                }
                let charge = ENTRY_BYTES + if charged { 0 } else { bytes };
                charged = true;
                admitted += 1;
                state.charge(self.0.class, charge);
                for observation in [self.0.counters.as_ref(), &core.counters] {
                    observation.resident.fetch_add(charge, Ordering::Relaxed);
                    observation.entries.fetch_add(1, Ordering::Relaxed);
                    observation.admissions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if !owned_keys.is_empty()
            && let Some(owned) = &self.0.owned
        {
            owned
                .lock()
                .expect("ASSERT: cache owner list poisoned")
                .extend(owned_keys);
        }
        admitted
    }

    pub fn remove(&self, key: ReadCacheKey) {
        self.0.core.remove(self.key(key));
    }

    pub fn clear(&self) {
        self.0.purge_ownership();
    }

    #[must_use]
    pub fn stats(&self) -> ReadCacheStats {
        self.0.core.refresh();
        self.0.counters.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn common_resident_bytes(&self) -> u64 {
        self.0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned")
            .resident
    }

    /// Common capacity, not a quota assigned to this representation.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.0.core.refresh();
        self.0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned")
            .target
    }

    /// Current 20% ceiling for Verified DATA within this common cache target.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn verified_data_limit(&self) -> u64 {
        self.0.core.refresh();
        let state = self
            .0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        verified_data_limit(state.target)
    }

    /// Current 70% ceiling for protected Exact acceleration.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn protected_exact_limit(&self) -> u64 {
        self.0.core.refresh();
        let state = self
            .0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        protected_exact_limit(state.target)
    }

    /// Common-cache bytes currently charged to protected Exact acceleration.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn protected_resident_bytes(&self) -> u64 {
        self.0.core.refresh();
        self.0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned")
            .protected_bytes
    }

    /// Bytes charged to read-cache classes that must never be evicted.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn pinned_resident_bytes(&self) -> u64 {
        self.0.core.refresh();
        self.0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned")
            .pinned_bytes
    }

    /// Last pressure sample from the common memory governor.
    ///
    /// # Panics
    /// Panics if the pressure sampler lock has been poisoned.
    #[must_use]
    pub fn pressure(&self) -> MemoryPressureSnapshot {
        self.0.core.refresh();
        *self
            .0
            .core
            .snapshot
            .lock()
            .expect("ASSERT: cache pressure poisoned")
    }

    /// Supplies an external pressure sample for an isolated cache.
    ///
    /// # Panics
    /// Panics for a production cache or poisoned ownership locks.
    pub fn update_pressure(&self, snapshot: MemoryPressureSnapshot, hard: u64, reserve: u64) {
        let capacity = if snapshot.swap_used_bytes() == 0 {
            snapshot.available_bytes().saturating_sub(reserve).min(hard)
        } else {
            0
        };
        self.set_capacity(capacity);
        *self
            .0
            .core
            .snapshot
            .lock()
            .expect("ASSERT: cache pressure poisoned") = snapshot;
    }

    /// Cold telemetry visits resident values without allocating a copy of the
    /// directory. The visitor must not call back into the cache or perform I/O.
    pub(crate) fn visit<T: Any + Send + Sync>(&self, mut visitor: impl FnMut(&T)) {
        for slot in &self.0.core.shards {
            let shard = slot.0.lock().expect("ASSERT: cache shard poisoned");
            for (key, entry) in &shard.map {
                if key.namespace == self.0.id {
                    visitor(
                        entry
                            .value
                            .downcast_ref::<T>()
                            .expect("ASSERT: namespace type"),
                    );
                }
            }
        }
    }

    /// Removes only the representation observed by a failed validation. A
    /// concurrently replaced, valid value must remain available.
    pub(crate) fn remove_if<T: Any + Send + Sync>(
        &self,
        object: ReadCacheKey,
        predicate: impl FnOnce(&T) -> bool,
    ) {
        let core = &self.0.core;
        let key = self.key(object);
        let mut state = core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        let mut shard = core.shards[Core::shard(key)]
            .0
            .lock()
            .expect("ASSERT: cache shard poisoned");
        if shard
            .map
            .get(&key)
            .and_then(|entry| entry.value.downcast_ref::<T>())
            .is_some_and(predicate)
        {
            let entry = shard
                .map
                .remove(&key)
                .expect("ASSERT: selected cache entry exists");
            shard.stale_keys += 1;
            shard.reclaim_clocks();
            shard.compact();
            core.uncharge(&mut state, entry);
        }
    }

    /// Changes an externally governed cache after releasing excess ownership.
    ///
    /// # Panics
    /// Panics for a production cache, which owns its pressure sampler.
    pub fn set_capacity(&self, capacity: u64) {
        let core = &self.0.core;
        assert!(
            core.pool.is_none(),
            "ASSERT: production cache owns pressure sampling"
        );
        let mut state = core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        state.target = capacity;
        let reached = core.reclaim_class_ceilings(&mut state) && core.reclaim_pressure(&mut state);
        debug_assert!(reached, "ASSERT: charged cache has an owner");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_misses_propagate_success_and_errors_without_sharing_independent_reads() {
        use std::sync::mpsc;
        for fail in [false, true] {
            let cache = ReadCacheNamespace::isolated(ReadCacheClass::StorageRange, 1 << 20);
            let key = ReadCacheKey {
                identity: [55; 32],
                ordinal: 1,
            };
            let calls = AtomicU64::new(0);
            let (started, ready) = mpsc::channel();
            let (release, finish) = mpsc::channel();
            std::thread::scope(|scope| {
                let cache_ref = &cache;
                let calls_ref = &calls;
                let primary = scope.spawn(move || {
                    cache_ref.coalesce(key, || {
                        calls_ref.fetch_add(1, Ordering::Relaxed);
                        started.send(()).unwrap();
                        finish.recv().unwrap();
                        if fail {
                            Err(io::ErrorKind::UnexpectedEof.into())
                        } else {
                            Ok(Arc::new(vec![7_u8; 32]))
                        }
                    })
                });
                ready.recv().unwrap();
                let followers: Vec<_> = (0..8)
                    .map(|_| {
                        scope.spawn(|| {
                            cache.coalesce(key, || {
                                calls.fetch_add(1, Ordering::Relaxed);
                                Ok(Arc::new(vec![9_u8; 32]))
                            })
                        })
                    })
                    .collect();
                let deadline = Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let attached = cache
                        .0
                        .core
                        .flights
                        .lock()
                        .unwrap()
                        .get(&cache.key(key))
                        .map_or(0, Arc::strong_count);
                    if attached >= 11 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "all shared-read waiters must attach"
                    );
                    std::thread::yield_now();
                }
                {
                    let _independent =
                        crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
                    let fresh = cache
                        .coalesce(key, || Ok(Arc::new(vec![3_u8; 32])))
                        .unwrap();
                    assert_eq!(*fresh, vec![3; 32]);
                }
                release.send(()).unwrap();
                let expected = primary.join().unwrap();
                for follower in followers {
                    let result = follower.join().unwrap();
                    match (&expected, result) {
                        (Ok(expected), Ok(value)) => assert!(Arc::ptr_eq(expected, &value)),
                        (Err(expected), Err(error)) => assert_eq!(expected.kind(), error.kind()),
                        _ => panic!("all shared readers must observe the same completion"),
                    }
                }
                assert_eq!(calls.load(Ordering::Relaxed), 1);
                assert!(cache.0.core.flights.lock().unwrap().is_empty());
            });
        }
    }

    #[test]
    fn scan_fills_headroom_without_displacing_and_nested_independent_intent_cannot_be_downgraded() {
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::MetadataObject, 2048);
        let warm = ReadCacheKey {
            identity: [1; 32],
            ordinal: 0,
        };
        let cold = ReadCacheKey {
            identity: [2; 32],
            ordinal: 0,
        };
        cache.insert(warm, Arc::new(17_u64), 8, 4096);
        {
            let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
            assert_eq!(*cache.get::<u64>(warm).unwrap(), 17);
            cache.insert(cold, Arc::new(19_u64), 8, 4096);
            assert_eq!(
                *cache.get::<u64>(cold).unwrap(),
                19,
                "a Scan miss fills eviction-free headroom"
            );
            let charge = 8 + ENTRY_BYTES;
            let mut filled = 0_u64;
            while cache.stats().resident_bytes + charge <= cache.capacity() {
                cache.insert(
                    ReadCacheKey {
                        identity: [9; 32],
                        ordinal: filled,
                    },
                    Arc::new(21_u64),
                    8,
                    4096,
                );
                filled += 1;
                assert!(cache.stats().resident_bytes <= cache.capacity());
            }
            let at_capacity = ReadCacheKey {
                identity: [3; 32],
                ordinal: 0,
            };
            cache.insert(at_capacity, Arc::new(23_u64), 8, 4096);
            assert!(
                cache.get::<u64>(at_capacity).is_none(),
                "a full Scan pass declines instead of displacing a resident entry"
            );
            assert_eq!(*cache.get::<u64>(warm).unwrap(), 17);
            let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
            let _demand = crate::ReadIntentScope::enter(crate::ReadIntent::Demand);
            assert!(cache.get::<u64>(warm).is_none());
        }
        assert_eq!(*cache.get::<u64>(warm).unwrap(), 17);
    }

    use super::*;
    fn key(n: u64) -> ReadCacheKey {
        ReadCacheKey {
            identity: [1; 32],
            ordinal: n,
        }
    }

    #[test]
    fn representations_compete_for_one_budget_and_views_survive_eviction() {
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 2 * (4096 + ENTRY_BYTES));
        let metadata = data.sibling(ReadCacheClass::MetadataObject);
        data.insert(key(1), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        let held = data.get::<Vec<u8>>(key(1)).unwrap();
        metadata.insert(key(2), Arc::new(vec![8_u8; 4096]), 4096, 4096);
        for i in 3..100 {
            metadata.insert(key(i), Arc::new(vec![9_u8; 4096]), 4096, 4096);
            assert!(
                data.stats().resident_bytes + metadata.stats().resident_bytes <= data.capacity()
            );
        }
        assert_eq!(held.as_slice(), &[7; 4096]);
        metadata.set_capacity(0);
        assert_eq!(
            data.stats().resident_bytes + metadata.stats().resident_bytes,
            0
        );
        assert_eq!(held.as_slice(), &[7; 4096]);
    }

    #[test]
    fn shared_backing_is_charged_once_and_released_by_its_last_cache_view() {
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100_000);
        let value = Arc::new(vec![1_u8; 4096]);
        cache.insert_group(
            vec![(key(1), Arc::clone(&value), 2048), (key(2), value, 2048)],
            4096,
        );
        assert_eq!(cache.stats().resident_bytes, 4096 + 2 * ENTRY_BYTES);
        cache.remove(key(1));
        assert_eq!(cache.stats().resident_bytes, 4096 + ENTRY_BYTES);
        cache.remove(key(2));
        assert_eq!(cache.stats().resident_bytes, 0);
    }

    #[test]
    fn concurrent_admission_cannot_exceed_shared_capacity_or_duplicate_identity() {
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::ExactPage, 8192);
        std::thread::scope(|threads| {
            for _ in 0..8 {
                threads.spawn(|| {
                    for _ in 0..100 {
                        cache.insert(key(1), Arc::new(42_u64), 8, 4096);
                        assert_eq!(*cache.get::<u64>(key(1)).unwrap(), 42);
                    }
                });
            }
        });
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().resident_bytes, 8 + ENTRY_BYTES);
        let other = cache.sibling(ReadCacheClass::ExactPage);
        assert!(other.get::<u64>(key(1)).is_none());
    }

    #[test]
    fn verified_data_ceiling_preserves_exact_pages_it_cannot_displace() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let exact = data.sibling(ReadCacheClass::ExactPage);
        exact.insert(key(0), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        for ordinal in 1..100 {
            data.insert(key(ordinal), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        }
        let limit = data.verified_data_limit();
        assert!(limit > 0);
        assert!(data.stats().resident_bytes <= limit);
        assert_eq!(*exact.get::<Vec<u8>>(key(0)).unwrap(), vec![9; 4096]);
    }

    #[test]
    fn protected_exact_admission_leaves_room_for_other_acceleration() {
        let charge = 4096 + ENTRY_BYTES;
        let common = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let exact = common.sibling(ReadCacheClass::ExactPage);
        let other = common.sibling(ReadCacheClass::StorageRange);

        for ordinal in 0..100 {
            exact.insert(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        let limit = common.protected_exact_limit();
        assert_eq!(limit, 70 * charge);
        assert_eq!(common.protected_resident_bytes(), limit);

        for ordinal in 200..230 {
            other.insert(key(ordinal), Arc::new(vec![3_u8; 4096]), 4096, 4096);
        }
        assert!(common.common_resident_bytes() <= 100 * charge);
        assert_eq!(common.protected_resident_bytes(), limit);
        assert_eq!(common.common_resident_bytes(), 100 * charge);
        assert_eq!(other.stats().resident_bytes, 30 * charge);
    }

    #[test]
    fn lowered_target_reclaims_protected_exact_to_its_ceiling() {
        let charge = 4096 + ENTRY_BYTES;
        let common = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let exact = common.sibling(ReadCacheClass::ExactPage);
        for ordinal in 0..70 {
            exact.insert(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        assert_eq!(common.protected_resident_bytes(), 70 * charge);
        common.set_capacity(50 * charge);
        assert_eq!(common.protected_resident_bytes(), 35 * charge);
        assert_eq!(common.common_resident_bytes(), 35 * charge);
    }

    #[test]
    fn pressure_reclaims_verified_data_before_protected_exact() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 40 * charge);
        let exact = data.sibling(ReadCacheClass::ExactPage);
        exact.insert(key(0), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        for ordinal in 1..40 {
            data.insert(key(ordinal), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        }
        data.set_capacity(charge);
        assert_eq!(exact.stats().resident_bytes, charge);
        assert_eq!(data.stats().resident_bytes, 0);
    }

    #[test]
    fn pressure_reclamation_consumes_stale_heads_before_reporting_no_victim() {
        let charge = 4096 + ENTRY_BYTES;
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::ExactPage, 200 * charge);
        let mut stale_heads = Vec::new();
        let mut survivors = Vec::new();
        let mut seen = [false; SHARDS];
        let mut ordinal = 0_u64;
        while stale_heads.len() < SHARDS && ordinal < 1_048_576 {
            let slot = Core::shard(cache.key(key(ordinal)));
            if !seen[slot] {
                seen[slot] = true;
                stale_heads.push(ordinal);
            }
            ordinal += 1;
        }
        assert_eq!(
            stale_heads.len(),
            SHARDS,
            "ASSERT: ordinals cover every shard"
        );
        seen = [false; SHARDS];
        while survivors.len() < SHARDS && ordinal < 2_097_152 {
            let slot = Core::shard(cache.key(key(ordinal)));
            if !seen[slot] {
                seen[slot] = true;
                survivors.push(ordinal);
            }
            ordinal += 1;
        }
        assert_eq!(
            survivors.len(),
            SHARDS,
            "ASSERT: ordinals cover every shard"
        );
        for ordinal in stale_heads.iter().chain(survivors.iter()) {
            cache.insert(key(*ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        for ordinal in &stale_heads {
            cache.remove(key(*ordinal));
        }
        // Each shard clock now begins with one stale key hiding one live owner.
        cache.set_capacity(2 * charge);
        assert!(
            cache.protected_resident_bytes() <= cache.protected_exact_limit(),
            "stale heads must not stop ceiling reclamation"
        );
        assert!(cache.common_resident_bytes() <= 2 * charge);
        assert_eq!(cache.stats().resident_bytes, cache.common_resident_bytes());
    }

    #[test]
    fn pinned_exact_membership_survives_capacity_and_pressure() {
        let charge = 4096 + ENTRY_BYTES;
        let membership = ReadCacheNamespace::isolated(ReadCacheClass::ExactMembership, 20 * charge);
        let exact = membership.sibling(ReadCacheClass::ExactPage);
        membership.insert(key(9), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        let pinned = membership.pinned_resident_bytes();
        assert_eq!(pinned, charge);
        assert_eq!(membership.common_resident_bytes(), 0);
        assert_eq!(membership.protected_resident_bytes(), 0);

        membership.set_capacity(0);
        assert_eq!(*membership.get::<Vec<u8>>(key(9)).unwrap(), vec![9; 4096]);
        assert_eq!(membership.pinned_resident_bytes(), pinned);

        membership.set_capacity(20 * charge);
        for ordinal in 0..25 {
            exact.insert(key(ordinal), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        }
        assert_eq!(*membership.get::<Vec<u8>>(key(9)).unwrap(), vec![9; 4096]);
        assert_eq!(membership.pinned_resident_bytes(), pinned);
        assert!(membership.protected_resident_bytes() <= membership.protected_exact_limit());
        assert!(membership.common_resident_bytes() <= 20 * charge);
    }

    #[test]
    fn pinned_exact_membership_admits_during_independent_recovery() {
        let charge = 4096 + ENTRY_BYTES;
        let membership = ReadCacheNamespace::isolated(ReadCacheClass::ExactMembership, charge);
        let independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        membership.insert(key(1), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        drop(independent);
        assert_eq!(*membership.peek::<Vec<u8>>(key(1)).unwrap(), vec![9; 4096]);
        assert_eq!(membership.pinned_resident_bytes(), charge);
        assert_eq!(membership.common_resident_bytes(), 0);
    }

    #[test]
    fn tracked_namespace_drop_releases_the_common_ownership() {
        let parent = ReadCacheNamespace::isolated(ReadCacheClass::ExactPage, 100_000);
        let tracked = parent.ephemeral_sibling(ReadCacheClass::ExactPage);
        let value = Arc::new(vec![1_u8; 100]);
        tracked.insert_group(
            (0..10)
                .map(|ordinal| (key(ordinal), Arc::clone(&value), 100))
                .collect(),
            100,
        );
        assert_eq!(parent.common_resident_bytes(), 100 + 10 * ENTRY_BYTES);
        drop(tracked);
        assert_eq!(parent.common_resident_bytes(), 0);
    }

    #[test]
    fn pressure_sampling_resamples_from_a_reused_thread_after_the_window() {
        let charge = 4096 + ENTRY_BYTES;
        let core = Core::new(charge, true);
        core.refresh();
        let armed = core.next_refresh.load(Ordering::Relaxed);
        assert!(armed > 0, "ASSERT: the first sample arms the window");
        core.refresh();
        assert_eq!(
            core.next_refresh.load(Ordering::Relaxed),
            armed,
            "one shared sample must rate-limit every caller within its window"
        );
        std::thread::sleep(std::time::Duration::from_millis(260));
        core.refresh();
        assert!(
            core.next_refresh.load(Ordering::Relaxed) > armed,
            "a surviving thread must resample pressure once the window expires"
        );
    }
}
