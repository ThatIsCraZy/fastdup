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
// Guaranteed resident shares under contention. Uncontested capacity may be
// borrowed by any class; a borrower above its guarantee yields first. The
// Exact guarantee dominates because dedup hashes are needed by every ingest,
// verification, GC, and restore decision.
const VERIFIED_DATA_SHARE_BASIS_POINTS: u64 = 2_000;
const VERIFIED_DATA_MINIMUM_BYTES: u64 = 4096 + ENTRY_BYTES;
const PROTECTED_EXACT_SHARE_BASIS_POINTS: u64 = 7_000;
const PROTECTED_EXACT_MINIMUM_BYTES: u64 = 4096 + ENTRY_BYTES;
// TinyLFU-style frequency sketch per shard. Counters saturate at 15 and are
// halved after a bounded number of increments so stale popularity decays.
const SKETCH_COUNTERS: usize = 8192;
const SKETCH_MAX: u8 = 15;
const SKETCH_RESET_OPS: u32 = 65_536;
// Bounded ghost fingerprints of evicted keys per shard. A re-requested ghost
// key is proven reuse and re-enters with eviction credit.
const GHOST_CAPACITY: usize = 1024;
const GHOST_ENTRY_CREDIT: u8 = 16;
// Foreground pressure refresh evicts only a bounded batch; the background
// reclaimer finishes large target reductions off the read path.
const REFRESH_RECLAIM_ENTRIES: usize = 512;
const RECLAIMER_BATCH_ENTRIES: usize = 4096;
const RECLAIMER_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

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
}

impl ReadCacheClass {
    const fn hit_credit(self) -> u8 {
        match self {
            Self::Data
            | Self::LocationProof
            | Self::HistoricalProof
            | Self::ContainerDescriptor => 16,
            // Index pages are metadata with dense reuse during candidate
            // search and publication maintenance. One credit per hit made a
            // queried page evictable after a single aging sweep, unlike the
            // streaming classes above. Exact pages stay protected by their
            // budget group, so this does not outrank the Exact fast path.
            Self::SimilarityPage => 8,
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
        // Membership filters and page-key bounds of active Runs are the
        // dedup fast path: every Exact decision starts there, their bytes
        // are a small fraction of the Runs they describe, and losing them
        // degrades lookups to per-page binary-search I/O. They stay
        // resident while their Run is active and leave with it.
        matches!(self, Self::ExactMembership | Self::ExactPageBounds)
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
    Other,
    Unprotected,
    All,
}

impl EvictionScope {
    const fn of_group(group: ReadCacheBudgetGroup) -> Self {
        match group {
            ReadCacheBudgetGroup::VerifiedData => Self::VerifiedData,
            ReadCacheBudgetGroup::ProtectedExact => Self::ProtectedExact,
            ReadCacheBudgetGroup::Other => Self::Other,
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
    ghost_hits: AtomicU64,
    sketch_rejections: AtomicU64,
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
    pub ghost_hits: u64,
    pub sketch_rejections: u64,
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
            ghost_hits: self.ghost_hits.load(Ordering::Relaxed),
            sketch_rejections: self.sketch_rejections.load(Ordering::Relaxed),
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

struct Entries {
    // Keys carry full content identities; a fast non-cryptographic hasher
    // is sufficient because equality always compares the complete key.
    map: hashbrown::HashMap<Key, Entry>,
    verified_clock: VecDeque<Key>,
    protected_clock: VecDeque<Key>,
    other_clock: VecDeque<Key>,
    pinned_clock: VecDeque<Key>,
    stale_keys: usize,
    sketch: Box<[u8]>,
    sketch_ops: u32,
    ghost: VecDeque<u64>,
}

impl Default for Entries {
    fn default() -> Self {
        Self {
            map: hashbrown::HashMap::new(),
            verified_clock: VecDeque::new(),
            protected_clock: VecDeque::new(),
            other_clock: VecDeque::new(),
            pinned_clock: VecDeque::new(),
            stale_keys: 0,
            sketch: vec![0; SKETCH_COUNTERS].into_boxed_slice(),
            sketch_ops: 0,
            ghost: VecDeque::new(),
        }
    }
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
            EvictionScope::Other => self.other_clock.pop_front(),
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

    fn sketch_increment(&mut self, fingerprint: u64) {
        let index = usize::try_from(fingerprint >> 32).unwrap_or(0) & (SKETCH_COUNTERS - 1);
        let counter = &mut self.sketch[index];
        *counter = (*counter).saturating_add(1).min(SKETCH_MAX);
        self.sketch_ops = self.sketch_ops.saturating_add(1);
        if self.sketch_ops >= SKETCH_RESET_OPS {
            self.sketch_ops = 0;
            for counter in &mut self.sketch {
                *counter /= 2;
            }
        }
    }

    fn sketch_estimate(&self, fingerprint: u64) -> u8 {
        let index = usize::try_from(fingerprint >> 32).unwrap_or(0) & (SKETCH_COUNTERS - 1);
        self.sketch[index]
    }

    fn ghost_remember(&mut self, fingerprint: u64) {
        if self.ghost.iter().any(|known| *known == fingerprint) {
            return;
        }
        if self.ghost.len() == GHOST_CAPACITY {
            self.ghost.pop_front();
        }
        self.ghost.push_back(fingerprint);
    }

    fn ghost_take(&mut self, fingerprint: u64) -> bool {
        let Some(position) = self.ghost.iter().position(|known| *known == fingerprint) else {
            return false;
        };
        self.ghost.remove(position);
        true
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

    fn clock_len(&self, scope: EvictionScope) -> usize {
        match scope {
            EvictionScope::VerifiedData => self.verified_clock.len(),
            EvictionScope::ProtectedExact => self.protected_clock.len(),
            EvictionScope::Other => self.other_clock.len(),
            EvictionScope::Unprotected => self.verified_clock.len() + self.other_clock.len(),
            EvictionScope::All => self.clocks_len(),
        }
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
            ..
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
            ..
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
    // Broker lease (including fixed bytes) whose eviction is still in
    // progress. Set by pressure refresh, acknowledged once resident bytes
    // reach the target, possibly by the background reclaimer.
    pending_pool_target: Option<u64>,
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

    const fn group_bytes(&self, group: ReadCacheBudgetGroup) -> u64 {
        match group {
            ReadCacheBudgetGroup::VerifiedData => self.verified_bytes,
            ReadCacheBudgetGroup::ProtectedExact => self.protected_bytes,
            ReadCacheBudgetGroup::Other => self.other_bytes,
        }
    }

    fn group_floor(&self, group: ReadCacheBudgetGroup) -> u64 {
        match group {
            ReadCacheBudgetGroup::VerifiedData => verified_data_limit(self.target),
            ReadCacheBudgetGroup::ProtectedExact => protected_exact_limit(self.target),
            ReadCacheBudgetGroup::Other => self
                .target
                .saturating_sub(verified_data_limit(self.target))
                .saturating_sub(protected_exact_limit(self.target)),
        }
    }

    /// Bytes by which a group currently borrows beyond its guarantee.
    fn group_excess(&self, group: ReadCacheBudgetGroup) -> u64 {
        self.group_bytes(group)
            .saturating_sub(self.group_floor(group))
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
        (size_of::<Self>()
            + SHARDS * size_of::<Shard>()
            + SHARDS * (SKETCH_COUNTERS + GHOST_CAPACITY * size_of::<u64>())
            + MAX_FLIGHTS * 1024) as u64
    }

    fn fingerprint(key: Key) -> u64 {
        let mut hash = key.namespace ^ key.object.ordinal;
        for bytes in key.object.identity.chunks_exact(8) {
            hash = hash.rotate_left(13)
                ^ u64::from_le_bytes(bytes.try_into().expect("ASSERT: eight identity bytes"));
        }
        // Final avalanche so shard bits, sketch bits, and ghost equality
        // draw on independent parts of the identity mix.
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
        hash ^= hash >> 33;
        hash
    }

    fn shard(key: Key) -> usize {
        usize::try_from(Self::fingerprint(key) & (SHARDS as u64 - 1))
            .expect("ASSERT: shard index fits usize")
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
        // Pressure eviction does not grant second chances. Borrowers above
        // their guaranteed share yield first, protected Exact last. The
        // foreground caller evicts only a bounded batch; the background
        // reclaimer finishes a large reduction and returns the lease.
        let reached = self.reclaim_toward_target(&mut state, REFRESH_RECLAIM_ENTRIES);
        if reached {
            state.pending_pool_target = None;
            pool.applied_with_pinned_floor(
                target,
                Self::fixed_bytes().saturating_add(state.resident),
                state.pinned_bytes,
            );
        } else {
            state.pending_pool_target = Some(target);
        }
    }

    /// Continues a target reduction that foreground refresh left pending.
    fn reclaim_pending(&self) {
        let Some(pool) = &self.pool else { return };
        let mut state = self
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        if state.pending_pool_target.is_none() && state.resident <= state.target {
            return;
        }
        let reached = self.reclaim_toward_target(&mut state, RECLAIMER_BATCH_ENTRIES);
        if reached && let Some(target) = state.pending_pool_target.take() {
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
        shard.sketch_increment(Self::fingerprint(key));
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

    /// Victim order under contention: the group borrowing furthest beyond
    /// its guarantee yields first. Ties prefer Verified DATA, then Other,
    /// then protected Exact, keeping dedup acceleration resident longest.
    fn pressure_victim_scope(state: &Admission) -> EvictionScope {
        let mut best: Option<(u64, ReadCacheBudgetGroup)> = None;
        for group in [
            ReadCacheBudgetGroup::VerifiedData,
            ReadCacheBudgetGroup::Other,
            ReadCacheBudgetGroup::ProtectedExact,
        ] {
            let excess = state.group_excess(group);
            if excess > 0 && best.is_none_or(|(known, _)| excess > known) {
                best = Some((excess, group));
            }
        }
        if let Some((_, group)) = best {
            return EvictionScope::of_group(group);
        }
        if state.unprotected_bytes() > 0 {
            EvictionScope::Unprotected
        } else {
            EvictionScope::All
        }
    }

    /// Admission victims mirror the pressure order. When nobody borrows
    /// beyond a guarantee, the incoming group churns its own FIFO instead
    /// of taking another group's guaranteed share.
    fn admission_victim_scope(
        state: &Admission,
        incoming: ReadCacheBudgetGroup,
    ) -> Option<EvictionScope> {
        // The incoming group never blocks its own growth while another
        // group still borrows beyond its guarantee.
        let mut best: Option<(u64, ReadCacheBudgetGroup)> = None;
        for group in [
            ReadCacheBudgetGroup::VerifiedData,
            ReadCacheBudgetGroup::Other,
            ReadCacheBudgetGroup::ProtectedExact,
        ] {
            if group == incoming {
                continue;
            }
            let excess = state.group_excess(group);
            if excess > 0 && best.is_none_or(|(known, _)| excess > known) {
                best = Some((excess, group));
            }
        }
        if let Some((_, group)) = best {
            return Some(EvictionScope::of_group(group));
        }
        (state.group_bytes(incoming) > 0).then(|| EvictionScope::of_group(incoming))
    }

    fn pinned_cap_bytes(&self) -> u64 {
        if self.pool.is_none() {
            return u64::MAX;
        }
        let effective = self
            .snapshot
            .lock()
            .expect("ASSERT: cache pressure poisoned")
            .effective_limit_bytes();
        if effective == 0 {
            u64::MAX
        } else {
            effective / 3
        }
    }

    fn reclaim_toward_target(&self, state: &mut Admission, max_evictions: usize) -> bool {
        let mut evicted = 0_usize;
        // Spend a bounded examination budget on never-hit entries first.
        // Pure FIFO pressure eviction would flush proven index pages at the
        // same rate as one-touch admissions whenever the Broker target
        // falls, which is exactly when their hit credit must matter.
        let mut cold_budget = ADMISSION_STEPS;
        let mut cold_exhausted = false;
        while state.resident > state.target {
            if evicted >= max_evictions {
                return false;
            }
            let scope = Self::pressure_victim_scope(state);
            // No global cold fallback: the selected borrower yields (cold
            // first, then credited FIFO) before a group at or below its
            // guarantee loses entries to a scope-All search.
            let cold_evicted = !cold_exhausted && self.evict_cold(state, scope, &mut cold_budget);
            if cold_evicted {
                evicted += 1;
                continue;
            }
            cold_exhausted = true;
            if !self.evict(state, false, scope)
                && (scope == EvictionScope::All || !self.evict(state, false, EvictionScope::All))
            {
                return false;
            }
            evicted += 1;
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
                    // Halving ages credit in a bounded number of passes; a
                    // fully credited entry survives six sweeps, not 32.
                    entry.credit /= 2;
                    shard.push(class, key);
                    return true;
                }
                let entry = shard
                    .map
                    .remove(&key)
                    .expect("ASSERT: cache clock covers owners");
                shard.ghost_remember(Self::fingerprint(key));
                shard.compact();
                self.uncharge(state, entry);
                return true;
            }
        }
        false
    }

    /// Evicts one entry that never earned hit credit. Credited entries are
    /// requeued untouched, so an unproven admission candidate can churn
    /// cold space without ever displacing a warm working set. Examination
    /// work is bounded by `budget` across the whole call.
    fn evict_cold(&self, state: &mut Admission, scope: EvictionScope, budget: &mut usize) -> bool {
        for _ in 0..SHARDS {
            if *budget == 0 {
                return false;
            }
            let index = state.cursor % SHARDS;
            state.cursor = state.cursor.wrapping_add(1);
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: cache shard poisoned");
            // Charge only entries actually examined: deducting the whole
            // clock length up front spent the entire budget on the first
            // shard whenever one queue exceeded it, silently disabling
            // cold-first eviction at production scale.
            let mut examined = shard.clock_len(scope).min(*budget);
            while examined > 0 {
                examined -= 1;
                *budget -= 1;
                let Some(key) = shard.pop(scope) else { break };
                let Some(entry) = shard.map.get_mut(&key) else {
                    shard.stale_keys = shard.stale_keys.saturating_sub(1);
                    shard.reclaim_clocks();
                    continue;
                };
                if entry.credit != 0 {
                    let class = entry.class;
                    shard.push(class, key);
                    continue;
                }
                let entry = shard
                    .map
                    .remove(&key)
                    .expect("ASSERT: cache clock covers owners");
                shard.ghost_remember(Self::fingerprint(key));
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
        static RECLAIMER: OnceLock<()> = OnceLock::new();
        let core = Arc::clone(CORE.get_or_init(|| Arc::new(Core::new(0, true))));
        RECLAIMER.get_or_init(|| {
            let worker = Arc::clone(&core);
            // A spawn failure only loses the fast path: foreground refresh
            // still reclaims its bounded batch on every sampling window.
            let _ = std::thread::Builder::new()
                .name("fastdup-cache-reclaim".into())
                .spawn(move || {
                    loop {
                        std::thread::park_timeout(RECLAIMER_INTERVAL);
                        worker.refresh();
                        worker.reclaim_pending();
                    }
                });
        });
        core
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
    pub(crate) fn insert_groups<T: Any + Send + Sync>(
        &self,
        groups: Vec<AdmissionGroup<T>>,
    ) -> u64 {
        self.insert_groups_inner(groups, false)
    }

    /// Speculative writer warming: DATA offered without a demand miss may
    /// fill (and FIFO-recycle within) its guaranteed share, but it never
    /// borrows another group's idle share and never evicts outside DATA.
    /// Demand reads and restore prefetch keep the borrowing path above.
    pub(crate) fn insert_groups_writer_warming<T: Any + Send + Sync>(
        &self,
        groups: Vec<AdmissionGroup<T>>,
    ) -> u64 {
        self.insert_groups_inner(groups, true)
    }

    #[allow(clippy::too_many_lines)]
    fn insert_groups_inner<T: Any + Send + Sync>(
        &self,
        groups: Vec<AdmissionGroup<T>>,
        writer_warming: bool,
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
        // The same shard visit records whether any key already proved reuse
        // (sketch estimate or ghost presence) for the admission gate below.
        let mut seen = std::collections::HashSet::new();
        let groups: Vec<_> = groups
            .into_iter()
            .filter_map(|(values, bytes)| {
                let mut proven = false;
                let values: Vec<_> = values
                    .into_iter()
                    .filter(|(object, _, _)| {
                        if !seen.insert(*object) {
                            return false;
                        }
                        let key = self.key(*object);
                        let shard = core.shards[Core::shard(key)]
                            .0
                            .lock()
                            .expect("ASSERT: cache shard poisoned");
                        let fingerprint = Core::fingerprint(key);
                        proven |= shard.sketch_estimate(fingerprint) >= 2
                            || shard.ghost.iter().any(|known| *known == fingerprint);
                        !shard.map.contains_key(&key)
                    })
                    .collect();
                (!values.is_empty()).then_some((values, bytes, proven))
            })
            .collect();
        if groups.is_empty() {
            return 0;
        }
        let added = groups.iter().fold(0_u64, |sum, (values, bytes, _)| {
            sum.saturating_add(*bytes)
                .saturating_add(ENTRY_BYTES.saturating_mul(values.len() as u64))
        });
        let reject = || {
            self.0.counters.rejections.fetch_add(1, Ordering::Relaxed);
        };
        let pinned = self.0.class.pinned();
        if pinned {
            // Pinned Exact acceleration is false-negative-safe when absent,
            // so a hard cap protects the process without risking correctness.
            if state.pinned_bytes.saturating_add(added) > core.pinned_cap_bytes() {
                reject();
                return 0;
            }
        } else if added > state.target {
            reject();
            return 0;
        }
        if writer_warming && !pinned && self.0.class == ReadCacheClass::Data {
            // Authoritative under the admission lock: recycle only DATA's
            // own FIFO window; never borrow beyond its guaranteed share and
            // never take a victim from an index group.
            let limit = verified_data_limit(state.target);
            let mut steps = ADMISSION_STEPS;
            while (state.verified_bytes.saturating_add(added) > limit
                || state.resident.saturating_add(added) > state.target)
                && steps != 0
            {
                if !core.evict(&mut state, false, EvictionScope::VerifiedData) {
                    break;
                }
                steps -= 1;
            }
            if state.verified_bytes.saturating_add(added) > limit
                || state.resident.saturating_add(added) > state.target
            {
                reject();
                return 0;
            }
        }
        if !pinned && state.resident.saturating_add(added) > state.target {
            if scan_fill {
                reject();
                return 0;
            }
            // One-hit DATA streams must not displace a warm working set:
            // an unproven candidate (no sketch demand, no ghost hit) may
            // only evict entries that never earned hit credit, so a new
            // workload still replaces cold residents on first demand while
            // credited entries stay. Uncontested headroom above admits
            // everything, and Exact classes are exempt because dedup
            // hashes are always on the critical path.
            let cold_only = self.0.class == ReadCacheClass::Data
                && !groups.iter().any(|(_, _, proven)| *proven);
            let group = self.0.class.budget_group();
            let mut steps = ADMISSION_STEPS;
            let mut cold_budget = ADMISSION_STEPS;
            while state.resident.saturating_add(added) > state.target && steps != 0 {
                let Some(scope) = Core::admission_victim_scope(&state, group) else {
                    break;
                };
                let freed = if cold_only {
                    // An unproven candidate rejects rather than hunting cold
                    // victims in groups that hold their guarantee.
                    core.evict_cold(&mut state, scope, &mut cold_budget)
                } else {
                    core.evict(&mut state, true, scope)
                        || core.evict(&mut state, true, EvictionScope::All)
                };
                if !freed {
                    break;
                }
                steps -= 1;
            }
            if state.resident.saturating_add(added) > state.target {
                if cold_only {
                    for observation in [self.0.counters.as_ref(), &core.counters] {
                        observation
                            .sketch_rejections
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                reject();
                return 0;
            }
        }
        let mut admitted = 0;
        let tracked = self.0.owned.is_some();
        let mut owned_keys = if tracked {
            Vec::with_capacity(groups.iter().map(|(values, _, _)| values.len()).sum())
        } else {
            Vec::new()
        };
        for (values, bytes, _) in groups {
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
                let fingerprint = Core::fingerprint(key);
                shard.sketch_increment(fingerprint);
                let credit = if shard.ghost_take(fingerprint) {
                    for observation in [self.0.counters.as_ref(), &core.counters] {
                        observation.ghost_hits.fetch_add(1, Ordering::Relaxed);
                    }
                    GHOST_ENTRY_CREDIT
                } else {
                    0
                };
                shard.map.insert(
                    key,
                    Entry {
                        value,
                        allocation: Arc::clone(&allocation),
                        hit_bytes,
                        class: self.0.class,
                        credit,
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

    /// Current eviction credit of a resident entry, or zero when absent.
    /// Used by representation-promotion policy without recording a hit.
    pub(crate) fn credit(&self, object: ReadCacheKey) -> u8 {
        let key = self.key(object);
        let shard = self.0.core.shards[Core::shard(key)]
            .0
            .lock()
            .expect("ASSERT: cache shard poisoned");
        shard.map.get(&key).map_or(0, |entry| entry.credit)
    }

    /// Swaps the representation of one resident singleton entry, charging
    /// only the byte delta. Growth never evicts another entry; it fails
    /// when the delta does not fit the common target.
    pub(crate) fn replace<T: Any + Send + Sync>(
        &self,
        object: ReadCacheKey,
        value: Arc<T>,
        new_bytes: u64,
        hit_bytes: u64,
    ) -> bool {
        if crate::read_intent::bypass_admission() && !self.0.class.pinned() {
            return false;
        }
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
        let Some(entry) = shard.map.get_mut(&key) else {
            return false;
        };
        if Arc::strong_count(&entry.allocation) != 1 {
            return false;
        }
        let old_bytes = entry.allocation.bytes;
        let growth = new_bytes.saturating_sub(old_bytes);
        if growth > 0
            && !self.0.class.pinned()
            && state.resident.saturating_add(growth) > state.target
        {
            return false;
        }
        let counters = Arc::clone(&entry.allocation.counters);
        entry.value = value;
        entry.allocation = Arc::new(Allocation {
            bytes: new_bytes,
            counters: Arc::clone(&counters),
        });
        entry.hit_bytes = hit_bytes;
        let shrink = old_bytes.saturating_sub(new_bytes);
        if growth > 0 {
            state.charge(self.0.class, growth);
            for observation in [counters.as_ref(), &core.counters] {
                observation.resident.fetch_add(growth, Ordering::Relaxed);
            }
        } else if shrink > 0 {
            state.uncharge(self.0.class, shrink);
            for observation in [counters.as_ref(), &core.counters] {
                observation.resident.fetch_sub(shrink, Ordering::Relaxed);
            }
        }
        true
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

    /// Cheap pre-copy eligibility for writer warming: the chunk can fit
    /// the DATA share by recycling DATA's own window, without borrowing.
    /// The authoritative check repeats under the admission lock on insert.
    pub(crate) fn writer_warming_may_fit(&self, bytes: u64) -> bool {
        self.0.core.refresh();
        let state = self
            .0
            .core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        bytes <= verified_data_limit(state.target)
            && state
                .target
                .saturating_sub(state.resident)
                .saturating_add(state.verified_bytes)
                >= bytes
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

    /// Bytes still available to protected Exact acceleration when measured
    /// against the stable effective capacity rather than the damped
    /// short-term target. Startup structure building uses this so Exact
    /// filters do not wait for the broker's growth smoothing.
    ///
    /// # Panics
    /// Panics if cache ownership locks have been poisoned.
    #[must_use]
    pub fn stable_protected_budget_bytes(&self) -> u64 {
        let pressure = self.pressure();
        if pressure.swap_used_bytes() != 0 || pressure.effective_limit_bytes() == 0 {
            return 0;
        }
        let reserve = crate::cache_budget::cache_memory_reserve(pressure.effective_limit_bytes());
        let stable =
            stable_capacity_target(pressure.effective_limit_bytes().saturating_sub(reserve));
        protected_exact_limit(stable)
            .saturating_sub(self.protected_resident_bytes())
            .saturating_sub(self.pinned_resident_bytes())
    }

    /// Guaranteed 20% share for Verified DATA within this common cache target.
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

    /// Guaranteed 70% share for protected Exact acceleration.
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
        let capacity = crate::memory_budget::swap_scaled_capacity(
            snapshot.available_bytes().saturating_sub(reserve).min(hard),
            snapshot.swap_used_bytes(),
            snapshot.effective_limit_bytes(),
        );
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
        let reached = core.reclaim_toward_target(&mut state, usize::MAX);
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
    fn verified_data_borrows_uncontested_capacity_and_yields_to_exact_growth() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let exact = data.sibling(ReadCacheClass::ExactPage);
        exact.insert(key(0), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        // Prove the DATA keys so the one-hit gate does not apply once the
        // cache fills; each miss still records demand in the sketch.
        for ordinal in 1..100 {
            let _ = data.get::<Vec<u8>>(key(ordinal));
            let _ = data.get::<Vec<u8>>(key(ordinal));
            data.insert(key(ordinal), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        }
        let limit = data.verified_data_limit();
        assert!(limit > 0);
        assert!(
            data.stats().resident_bytes > limit,
            "DATA borrows capacity the Exact working set does not contest"
        );
        assert_eq!(*exact.get::<Vec<u8>>(key(0)).unwrap(), vec![9; 4096]);
        // Exact growth reclaims borrowed DATA first until the Exact
        // guarantee is resident; DATA keeps the still-uncontested rest.
        for ordinal in 100..169 {
            exact.insert(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        assert_eq!(
            data.protected_resident_bytes(),
            data.protected_exact_limit()
        );
        assert_eq!(data.stats().resident_bytes, 30 * charge);
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
        assert_eq!(
            common.protected_resident_bytes(),
            100 * charge,
            "Exact borrows the whole uncontested target beyond its guarantee"
        );

        for ordinal in 200..230 {
            other.insert(key(ordinal), Arc::new(vec![3_u8; 4096]), 4096, 4096);
        }
        assert!(common.common_resident_bytes() <= 100 * charge);
        assert_eq!(common.protected_resident_bytes(), limit);
        assert_eq!(common.common_resident_bytes(), 100 * charge);
        assert_eq!(other.stats().resident_bytes, 30 * charge);
    }

    #[test]
    fn lowered_target_reclaims_protected_exact_to_the_target_not_its_guarantee() {
        let charge = 4096 + ENTRY_BYTES;
        let common = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let exact = common.sibling(ReadCacheClass::ExactPage);
        for ordinal in 0..70 {
            exact.insert(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        assert_eq!(common.protected_resident_bytes(), 70 * charge);
        common.set_capacity(50 * charge);
        // The guarantee only orders victims between groups; pressure still
        // reaches the lowered target exactly instead of over-shrinking.
        assert_eq!(common.protected_resident_bytes(), 50 * charge);
        assert_eq!(common.common_resident_bytes(), 50 * charge);
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
    fn pressure_reclamation_evicts_cold_entries_before_credited_ones() {
        let charge = 4096 + ENTRY_BYTES;
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::SimilarityPage, 64 * charge);
        for ordinal in 0..64 {
            cache.insert(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096, 4096);
        }
        // Warm a minority: a lowered target must spend its evictions on the
        // never-hit majority first instead of FIFO-flushing proven pages.
        for ordinal in 0..8 {
            assert!(cache.get::<Vec<u8>>(key(ordinal)).is_some());
        }
        cache.set_capacity(32 * charge);
        for ordinal in 0..8 {
            assert!(
                cache.peek::<Vec<u8>>(key(ordinal)).is_some(),
                "credited entry {ordinal} must survive pressure while cold entries remain"
            );
        }
        assert_eq!(cache.stats().resident_bytes, 32 * charge);
    }

    #[test]
    fn cold_eviction_budget_charges_only_examined_entries() {
        let charge = 4096 + ENTRY_BYTES;
        let cache = ReadCacheNamespace::isolated(ReadCacheClass::Data, 4096 * charge);
        for ordinal in 0..512 {
            cache.insert(key(ordinal), Arc::new(vec![3_u8; 4096]), 4096, 4096);
        }
        let core = &cache.0.core;
        let mut state = core
            .admission
            .lock()
            .expect("ASSERT: cache admission poisoned");
        let mut budget = 3_usize;
        assert!(core.evict_cold(&mut state, EvictionScope::All, &mut budget));
        assert_eq!(
            budget, 2,
            "one examined cold victim must cost one budget unit, not a whole shard scan"
        );
    }

    #[test]
    fn writer_warming_recycles_only_the_data_share() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 100 * charge);
        let similarity = data.sibling(ReadCacheClass::SimilarityPage);
        for ordinal in 0..10 {
            similarity.insert(
                key(10_000 + ordinal),
                Arc::new(vec![7_u8; 4096]),
                4096,
                4096,
            );
        }
        for ordinal in 0..60 {
            let admitted = data.insert_groups_writer_warming(vec![(
                vec![(key(ordinal), Arc::new(vec![9_u8; 4096]), 4096)],
                4096,
            )]);
            assert_eq!(admitted, 1);
        }
        // DATA share is 20% of the 100-entry target: 60 warmed chunks must
        // FIFO-recycle inside at most 20 resident entries (plus one
        // boundary entry), while every similarity page stays resident.
        assert!(
            data.stats().resident_bytes <= 21 * charge,
            "writer warming borrowed beyond the DATA share: {}",
            data.stats().resident_bytes
        );
        for ordinal in 0..10 {
            assert!(
                similarity.peek::<Vec<u8>>(key(10_000 + ordinal)).is_some(),
                "writer warming displaced similarity entry {ordinal}"
            );
        }
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
            cache.protected_resident_bytes() <= 2 * charge,
            "stale heads must not stop target reclamation"
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
        assert_eq!(membership.protected_resident_bytes(), 20 * charge);
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

    #[test]
    fn unproven_data_cannot_displace_a_warm_working_set_until_redemanded() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 4 * charge);
        for ordinal in 0..4 {
            data.insert(key(ordinal), Arc::new(vec![7_u8; 4096]), 4096, 4096);
            // Warm the resident set: one hit earns eviction credit.
            assert!(data.get::<Vec<u8>>(key(ordinal)).is_some());
        }
        data.insert(key(9), Arc::new(vec![8_u8; 4096]), 4096, 4096);
        assert!(data.peek::<Vec<u8>>(key(9)).is_none());
        assert_eq!(data.stats().sketch_rejections, 1);
        // Two demands prove the key; the next admission displaces FIFO DATA.
        assert!(data.get::<Vec<u8>>(key(9)).is_none());
        assert!(data.get::<Vec<u8>>(key(9)).is_none());
        data.insert(key(9), Arc::new(vec![8_u8; 4096]), 4096, 4096);
        assert_eq!(*data.get::<Vec<u8>>(key(9)).unwrap(), vec![8; 4096]);
        assert_eq!(data.stats().entries, 4);
    }

    #[test]
    fn evicted_keys_return_through_the_ghost_with_credit() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, charge);
        data.insert(key(1), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        // Prove key 2 so its admission may evict key 1 into the ghost.
        assert!(data.get::<Vec<u8>>(key(2)).is_none());
        assert!(data.get::<Vec<u8>>(key(2)).is_none());
        data.insert(key(2), Arc::new(vec![8_u8; 4096]), 4096, 4096);
        assert!(data.peek::<Vec<u8>>(key(1)).is_none(), "key 1 was evicted");
        data.insert(key(1), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        assert_eq!(data.stats().ghost_hits, 1);
        assert_eq!(data.credit(key(1)), GHOST_ENTRY_CREDIT);
    }

    #[test]
    fn replace_charges_only_the_delta_and_never_evicts() {
        let charge = 4096 + ENTRY_BYTES;
        let data = ReadCacheNamespace::isolated(ReadCacheClass::Data, 3 * charge);
        data.insert(key(1), Arc::new(vec![7_u8; 4096]), 4096, 4096);
        assert!(data.replace(key(1), Arc::new(vec![7_u8; 8192]), 8192, 8192));
        assert_eq!(data.stats().resident_bytes, 8192 + ENTRY_BYTES);
        assert!(!data.replace(key(1), Arc::new(vec![7_u8; 16_384]), 16_384, 16_384));
        assert_eq!(data.stats().resident_bytes, 8192 + ENTRY_BYTES);
        assert!(data.replace(key(1), Arc::new(vec![7_u8; 1024]), 1024, 4096));
        assert_eq!(data.stats().resident_bytes, 1024 + ENTRY_BYTES);
        assert!(!data.replace(key(2), Arc::new(vec![7_u8; 1024]), 1024, 1024));
    }

    #[test]
    fn pinned_page_bounds_survive_zero_capacity_like_membership_filters() {
        let charge = 4096 + ENTRY_BYTES;
        let bounds = ReadCacheNamespace::isolated(ReadCacheClass::ExactPageBounds, charge);
        bounds.insert(key(4), Arc::new(vec![5_u8; 4096]), 4096, 4096);
        assert_eq!(bounds.pinned_resident_bytes(), charge);
        bounds.set_capacity(0);
        assert_eq!(*bounds.get::<Vec<u8>>(key(4)).unwrap(), vec![5; 4096]);
    }
}
