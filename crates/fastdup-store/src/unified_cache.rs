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
    clock: VecDeque<Key>,
}

impl Entries {
    fn compact(&mut self) {
        // The per-entry charge includes up to 2x spare capacity. Reallocating
        // a VecDeque after every eviction would turn replacement quadratic.
        if self.map.capacity() > self.map.len().saturating_mul(2) {
            self.map.shrink_to_fit();
        }
        if self.clock.capacity() > self.clock.len().saturating_mul(2) {
            self.clock.shrink_to_fit();
        }
    }
}

#[repr(align(64))]
struct Shard(Mutex<Entries>);

#[derive(Default)]
struct Admission {
    resident: u64,
    target: u64,
    cursor: usize,
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
        let target = pool.target(
            snapshot,
            CacheObservation {
                hits: observed.hits,
                misses: observed.misses,
                hit_bytes: observed.hit_bytes,
                evictions: observed.evictions,
                resident_bytes: Self::fixed_bytes().saturating_add(state.resident),
            },
        );
        state.target = target.saturating_sub(Self::fixed_bytes());
        // Pressure eviction does not grant second chances. All cache owners
        // leave before the common lease is returned to the memory broker.
        while state.resident > state.target {
            assert!(
                self.evict(&mut state, false),
                "ASSERT: charged cache has an owner"
            );
        }
        pool.applied(target, Self::fixed_bytes().saturating_add(state.resident));
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

    fn uncharge(&self, state: &mut Admission, entry: Entry) {
        let counters = &entry.allocation.counters;
        let payload = if Arc::strong_count(&entry.allocation) == 1 {
            entry.allocation.bytes
        } else {
            0
        };
        let charge = ENTRY_BYTES + payload;
        state.resident -= charge;
        for observation in [counters.as_ref(), &self.counters] {
            observation.resident.fetch_sub(charge, Ordering::Relaxed);
            observation.entries.fetch_sub(1, Ordering::Relaxed);
            observation.evictions.fetch_add(1, Ordering::Relaxed);
        }
        // The returned immutable Arc may outlive eviction. That reader memory
        // continues to reduce measured headroom; eviction cannot invalidate it.
        drop(entry);
    }

    fn evict(&self, state: &mut Admission, second_chance: bool) -> bool {
        for _ in 0..SHARDS {
            let index = state.cursor % SHARDS;
            state.cursor = state.cursor.wrapping_add(1);
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: cache shard poisoned");
            let Some(key) = shard.clock.pop_front() else {
                continue;
            };
            let entry = shard
                .map
                .get_mut(&key)
                .expect("ASSERT: cache clock covers owners");
            if second_chance && entry.credit != 0 {
                entry.credit -= 1;
                shard.clock.push_back(key);
                return true;
            }
            let entry = shard
                .map
                .remove(&key)
                .expect("ASSERT: selected cache entry exists");
            shard.compact();
            self.uncharge(state, entry);
            return true;
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
            shard.clock.retain(|candidate| *candidate != key);
            shard.compact();
            self.uncharge(&mut state, entry);
        }
    }

    fn purge_namespace(&self, namespace: u64) {
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        for slot in &self.shards {
            let mut shard = slot.0.lock().expect("ASSERT: cache shard poisoned");
            let mut remaining = VecDeque::new();
            while let Some(key) = shard.clock.pop_front() {
                if key.namespace == namespace {
                    let entry = shard
                        .map
                        .remove(&key)
                        .expect("ASSERT: cache clock entry exists");
                    self.uncharge(&mut state, entry);
                } else {
                    remaining.push_back(key);
                }
            }
            shard.clock = remaining;
            shard.map.shrink_to_fit();
        }
    }
}

struct Namespace {
    core: Arc<Core>,
    id: u64,
    class: ReadCacheClass,
    counters: Arc<Counters>,
}

impl Drop for Namespace {
    fn drop(&mut self) {
        self.core.purge_namespace(self.id);
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
        static CORE: OnceLock<Arc<Core>> = OnceLock::new();
        Self::attach(
            Arc::clone(CORE.get_or_init(|| Arc::new(Core::new(0, true)))),
            class,
        )
    }

    /// Uses the same implementation with a deterministic budget for tests or
    /// an externally governed embedded repository.
    #[must_use]
    pub fn isolated(class: ReadCacheClass, capacity: u64) -> Self {
        Self::attach(Arc::new(Core::new(capacity, false)), class)
    }

    fn attach(core: Arc<Core>, class: ReadCacheClass) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id, u64::MAX, "ASSERT: cache namespace identities exhausted");
        Self(Arc::new(Namespace {
            core,
            id,
            class,
            counters: Arc::new(Counters::default()),
        }))
    }

    /// Creates another representation in the same cache and memory budget.
    #[must_use]
    pub fn sibling(&self, class: ReadCacheClass) -> Self {
        Self::attach(Arc::clone(&self.0.core), class)
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
    #[allow(clippy::too_many_lines)]
    pub(crate) fn insert_groups<T: Any + Send + Sync>(
        &self,
        groups: Vec<AdmissionGroup<T>>,
    ) -> u64 {
        if groups.is_empty() || crate::read_intent::bypass_admission() {
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
        if added > state.target {
            reject();
            return 0;
        }
        let mut steps = ADMISSION_STEPS;
        while state.resident.saturating_add(added) > state.target && steps != 0 {
            core.evict(&mut state, true);
            steps -= 1;
        }
        if state.resident.saturating_add(added) > state.target {
            reject();
            return 0;
        }
        let mut admitted = 0;
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
                if shard.map.try_reserve(1).is_err() || shard.clock.try_reserve(1).is_err() {
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
                shard.clock.push_back(key);
                let charge = ENTRY_BYTES + if charged { 0 } else { bytes };
                charged = true;
                admitted += 1;
                state.resident += charge;
                for observation in [self.0.counters.as_ref(), &core.counters] {
                    observation.resident.fetch_add(charge, Ordering::Relaxed);
                    observation.entries.fetch_add(1, Ordering::Relaxed);
                    observation.admissions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        admitted
    }

    pub fn remove(&self, key: ReadCacheKey) {
        self.0.core.remove(self.key(key));
    }

    pub fn clear(&self) {
        self.0.core.purge_namespace(self.0.id);
    }

    #[must_use]
    pub fn stats(&self) -> ReadCacheStats {
        self.0.core.refresh();
        self.0.counters.snapshot()
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
            shard.clock.retain(|candidate| *candidate != key);
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
        while state.resident > capacity {
            assert!(
                core.evict(&mut state, false),
                "ASSERT: cache residency has an owner"
            );
        }
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
    fn scan_hits_do_not_admit_and_nested_independent_intent_cannot_be_downgraded() {
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
            assert!(cache.get::<u64>(cold).is_none());
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
}
