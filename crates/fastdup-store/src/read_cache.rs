use fastdup_format::ChunkId;
use std::array;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) use crate::memory_budget::{MemoryPressureSnapshot, SYSTEM_REFRESH_INTERVAL};

const CACHE_WAYS: usize = 4;
const CACHE_SLOT_TARGET_BYTES: usize = 16 * 1_024;
const MINIMUM_SYSTEM_RESERVE_BYTES: u64 = 4 * 1_024 * 1_024 * 1_024;
const MAXIMUM_DEFAULT_CACHE_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;

/// Hard geometry and memory reserve for the shared verified read cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedReadCacheConfig {
    hard_limit_bytes: usize,
    reserve_bytes: u64,
    shard_count: NonZeroUsize,
}

impl VerifiedReadCacheConfig {
    /// Builds an explicit cache policy.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero hard limit, non-power-of-two shard count,
    /// or geometry too small to hold one four-way set per shard.
    pub fn new(
        hard_limit_bytes: usize,
        reserve_bytes: u64,
        shard_count: NonZeroUsize,
    ) -> Result<Self, VerifiedReadCacheError> {
        if hard_limit_bytes == 0 {
            return Err(VerifiedReadCacheError::ZeroHardLimit);
        }
        if !shard_count.get().is_power_of_two() {
            return Err(VerifiedReadCacheError::ShardCountNotPowerOfTwo);
        }
        Ok(Self {
            hard_limit_bytes,
            reserve_bytes,
            shard_count,
        })
    }

    /// Derives a conservative default from a complete system snapshot.
    ///
    /// One quarter of effective RAM (at least 4 GiB) remains outside the cache.
    /// Cache RAM itself is capped at one eighth of effective RAM and 8 GiB.
    ///
    /// # Panics
    ///
    /// Panics only if the internally clamped worker-derived shard count is
    /// zero, which would violate [`NonZeroUsize`] and platform thread-count
    /// invariants.
    #[must_use]
    pub fn conservative(snapshot: MemoryPressureSnapshot) -> Self {
        let effective = snapshot.effective_limit_bytes().max(1);
        let reserve = shared_cache_reserve_bytes(effective);
        let hard = (effective / 8).clamp(64 * 1_024, MAXIMUM_DEFAULT_CACHE_BYTES);
        let workers = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let shards = workers.next_power_of_two().min(64);
        Self {
            hard_limit_bytes: usize::try_from(hard).unwrap_or(usize::MAX),
            reserve_bytes: reserve,
            shard_count: NonZeroUsize::new(shards)
                .expect("ASSERT: conservative shard count is nonzero"),
        }
    }

    #[must_use]
    pub const fn hard_limit_bytes(self) -> usize {
        self.hard_limit_bytes
    }

    #[must_use]
    pub const fn reserve_bytes(self) -> u64 {
        self.reserve_bytes
    }

    #[must_use]
    pub const fn shard_count(self) -> NonZeroUsize {
        self.shard_count
    }
}

/// Returns the process headroom that all rebuildable caches must leave free.
///
/// Cache modules share this rule so independent byte budgets cannot redefine
/// the memory needed by Dirty DATA, reduction workers, XFS, and device queues.
#[must_use]
pub fn shared_cache_reserve_bytes(effective_limit_bytes: u64) -> u64 {
    (effective_limit_bytes / 4).max(MINIMUM_SYSTEM_RESERVE_BYTES)
}

#[derive(Debug)]
pub enum VerifiedReadCacheError {
    ZeroHardLimit,
    ShardCountNotPowerOfTwo,
    GeometryTooSmall,
    OutOfMemory,
    SystemMemory(io::Error),
}

impl fmt::Display for VerifiedReadCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroHardLimit => formatter.write_str("verified read-cache limit is zero"),
            Self::ShardCountNotPowerOfTwo => {
                formatter.write_str("verified read-cache shard count is not a power of two")
            }
            Self::GeometryTooSmall => {
                formatter.write_str("verified read-cache limit is too small for its shards")
            }
            Self::OutOfMemory => formatter.write_str("verified read-cache allocation failed"),
            Self::SystemMemory(error) => {
                write!(formatter, "memory-pressure sampling failed: {error}")
            }
        }
    }
}

impl std::error::Error for VerifiedReadCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SystemMemory(error) => Some(error),
            Self::ZeroHardLimit
            | Self::ShardCountNotPowerOfTwo
            | Self::GeometryTooSmall
            | Self::OutOfMemory => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedReadCacheStatus {
    hits: u64,
    misses: u64,
    admissions: u64,
    evictions: u64,
    pressure_rejections: u64,
    oversized_rejections: u64,
    entry_count: usize,
    resident_bytes: usize,
    target_bytes: usize,
    metadata_bytes: usize,
    hard_limit_bytes: usize,
    reserve_bytes: u64,
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

impl VerifiedReadCacheStatus {
    status_getter!(hits, hits, u64);
    status_getter!(misses, misses, u64);
    status_getter!(admissions, admissions, u64);
    status_getter!(evictions, evictions, u64);
    status_getter!(pressure_rejections, pressure_rejections, u64);
    status_getter!(oversized_rejections, oversized_rejections, u64);
    status_getter!(entry_count, entry_count, usize);
    status_getter!(resident_bytes, resident_bytes, usize);
    status_getter!(target_bytes, target_bytes, usize);
    status_getter!(metadata_bytes, metadata_bytes, usize);
    status_getter!(hard_limit_bytes, hard_limit_bytes, usize);
    status_getter!(reserve_bytes, reserve_bytes, u64);
    status_getter!(effective_limit_bytes, effective_limit_bytes, u64);
    status_getter!(available_bytes, available_bytes, u64);
    status_getter!(swap_used_bytes, swap_used_bytes, u64);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CacheKey {
    chunk_id: ChunkId,
    logical_length: u64,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    key: CacheKey,
    payload: VerifiedChunkPayload,
}

/// Shared ownership of one already verified decoded Chunk payload.
///
/// The owner adopts a decoder-produced `Vec` without copying its payload.
/// Clones share those exact bytes until the final Manifest assembly copies the
/// requested slice into the frontend reply.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct VerifiedChunkPayload(Arc<Vec<u8>>);

impl VerifiedChunkPayload {
    pub(crate) fn from_verified_store(bytes: Vec<u8>) -> Self {
        Self(Arc::new(bytes))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    fn allocation_bytes(&self) -> usize {
        self.0.capacity()
    }

    #[cfg(test)]
    fn payload_address(&self) -> *const u8 {
        self.0.as_ptr()
    }
}

impl fmt::Debug for VerifiedChunkPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedChunkPayload")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct CacheSet {
    ways: [Option<CacheEntry>; CACHE_WAYS],
    next_victim: usize,
}

impl Default for CacheSet {
    fn default() -> Self {
        Self {
            ways: array::from_fn(|_| None),
            next_victim: 0,
        }
    }
}

#[derive(Debug)]
struct CacheShardState {
    sets: Box<[CacheSet]>,
    counters: CacheShardCounters,
}

#[derive(Clone, Copy, Debug, Default)]
struct CacheShardCounters {
    hits: u64,
    misses: u64,
    admissions: u64,
    evictions: u64,
}

impl CacheShardCounters {
    fn add_assign(&mut self, other: Self) {
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.admissions = self.admissions.saturating_add(other.admissions);
        self.evictions = self.evictions.saturating_add(other.evictions);
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
struct CacheShard {
    state: Mutex<CacheShardState>,
}

/// Bounded, sharded cache of immutable bytes that have already passed complete
/// stored-encoding and logical-identity verification.
///
/// Entries are four-way set associative. Demand hits touch one cache-line-
/// separated shard and at most four pointers; there is no global LRU chain.
/// Admission is serialized only after the expensive Container read/VERIFY has
/// completed so exact byte accounting cannot overrun the current target.
pub struct VerifiedReadCache {
    config: VerifiedReadCacheConfig,
    shards: Box<[CacheShard]>,
    metadata_bytes: usize,
    admission: Mutex<()>,
    target_bytes: AtomicUsize,
    resident_bytes: AtomicUsize,
    entry_count: AtomicUsize,
    pressure_rejections: AtomicU64,
    oversized_rejections: AtomicU64,
    effective_limit_bytes: AtomicU64,
    available_bytes: AtomicU64,
    swap_used_bytes: AtomicU64,
    automatic_pressure: bool,
    started: Instant,
    last_refresh_millis: AtomicU64,
}

impl fmt::Debug for VerifiedReadCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedReadCache")
            .field("status", &self.status())
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

impl VerifiedReadCache {
    /// Constructs a cache with automatic host/cgroup pressure refresh.
    ///
    /// # Errors
    ///
    /// Returns system-sampling, invalid-geometry, or allocation failures.
    pub fn new_system() -> Result<Self, VerifiedReadCacheError> {
        let snapshot =
            MemoryPressureSnapshot::read_system().map_err(VerifiedReadCacheError::SystemMemory)?;
        let config = VerifiedReadCacheConfig::conservative(snapshot);
        Self::build(config, snapshot, true)
    }

    /// Constructs an explicitly configured cache with automatic pressure
    /// refresh. This is intended for deployment overrides.
    ///
    /// # Errors
    ///
    /// Returns system-sampling, invalid-geometry, or allocation failures.
    pub fn new(config: VerifiedReadCacheConfig) -> Result<Self, VerifiedReadCacheError> {
        let snapshot =
            MemoryPressureSnapshot::read_system().map_err(VerifiedReadCacheError::SystemMemory)?;
        Self::build(config, snapshot, true)
    }

    /// Constructs a deterministic manually refreshed cache for tests and
    /// embedded runtimes with an external memory governor.
    ///
    /// # Errors
    ///
    /// Returns invalid-geometry or allocation failures.
    pub fn new_with_snapshot(
        config: VerifiedReadCacheConfig,
        snapshot: MemoryPressureSnapshot,
    ) -> Result<Self, VerifiedReadCacheError> {
        Self::build(config, snapshot, false)
    }

    fn build(
        config: VerifiedReadCacheConfig,
        snapshot: MemoryPressureSnapshot,
        automatic_pressure: bool,
    ) -> Result<Self, VerifiedReadCacheError> {
        let shard_count = config.shard_count.get();
        let approximate_set_bytes = CACHE_WAYS
            .checked_mul(CACHE_SLOT_TARGET_BYTES)
            .and_then(|payload| payload.checked_add(size_of::<CacheSet>()))
            .ok_or(VerifiedReadCacheError::GeometryTooSmall)?;
        let mut set_count = config.hard_limit_bytes / approximate_set_bytes;
        set_count -= set_count % shard_count;
        if set_count < shard_count {
            return Err(VerifiedReadCacheError::GeometryTooSmall);
        }
        let sets_per_shard = set_count / shard_count;
        let metadata_bytes = set_count
            .checked_mul(size_of::<CacheSet>())
            .ok_or(VerifiedReadCacheError::GeometryTooSmall)?;
        if metadata_bytes >= config.hard_limit_bytes {
            return Err(VerifiedReadCacheError::GeometryTooSmall);
        }
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(shard_count)
            .map_err(|_| VerifiedReadCacheError::OutOfMemory)?;
        for _ in 0..shard_count {
            let mut sets = Vec::new();
            sets.try_reserve_exact(sets_per_shard)
                .map_err(|_| VerifiedReadCacheError::OutOfMemory)?;
            sets.resize_with(sets_per_shard, CacheSet::default);
            shards.push(CacheShard {
                state: Mutex::new(CacheShardState {
                    sets: sets.into_boxed_slice(),
                    counters: CacheShardCounters::default(),
                }),
            });
        }
        let cache = Self {
            config,
            shards: shards.into_boxed_slice(),
            metadata_bytes,
            admission: Mutex::new(()),
            target_bytes: AtomicUsize::new(0),
            resident_bytes: AtomicUsize::new(0),
            entry_count: AtomicUsize::new(0),
            pressure_rejections: AtomicU64::new(0),
            oversized_rejections: AtomicU64::new(0),
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

    /// Applies a fresh pressure sample and immediately purges payloads when
    /// Swap is in use or the configured reserve is no longer available.
    ///
    /// # Panics
    ///
    /// Panics when called on a system-governed cache, when an earlier impossible
    /// cache invariant poisoned an internal lock, or resident accounting
    /// disagrees with the sharded entry set.
    pub fn update_memory_pressure(&self, snapshot: MemoryPressureSnapshot) {
        assert!(
            !self.automatic_pressure,
            "ASSERT: an automatic cache accepts pressure only from its system sampler"
        );
        self.apply_memory_pressure(snapshot);
    }

    fn apply_memory_pressure(&self, snapshot: MemoryPressureSnapshot) {
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: verified read-cache admission lock poisoned");
        let total_target = if snapshot.swap_used_bytes() != 0 {
            0
        } else {
            let available = snapshot
                .available_bytes()
                .min(snapshot.effective_limit_bytes());
            let headroom = available.saturating_sub(self.config.reserve_bytes);
            usize::try_from(headroom)
                .unwrap_or(usize::MAX)
                .min(self.config.hard_limit_bytes)
        };
        let payload_target = total_target.saturating_sub(self.metadata_bytes);
        self.effective_limit_bytes
            .store(snapshot.effective_limit_bytes(), Ordering::Release);
        self.available_bytes
            .store(snapshot.available_bytes(), Ordering::Release);
        self.swap_used_bytes
            .store(snapshot.swap_used_bytes(), Ordering::Release);
        self.target_bytes.store(payload_target, Ordering::Release);
        if self.resident_bytes.load(Ordering::Acquire) > payload_target {
            self.clear_locked();
        }
    }

    #[must_use]
    pub fn status(&self) -> VerifiedReadCacheStatus {
        let counters = self.counters();
        VerifiedReadCacheStatus {
            hits: counters.hits,
            misses: counters.misses,
            admissions: counters.admissions,
            evictions: counters.evictions,
            pressure_rejections: self.pressure_rejections.load(Ordering::Relaxed),
            oversized_rejections: self.oversized_rejections.load(Ordering::Relaxed),
            entry_count: self.entry_count.load(Ordering::Acquire),
            resident_bytes: self.resident_bytes.load(Ordering::Acquire),
            target_bytes: self.target_bytes.load(Ordering::Acquire),
            metadata_bytes: self.metadata_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            reserve_bytes: self.config.reserve_bytes,
            effective_limit_bytes: self.effective_limit_bytes.load(Ordering::Acquire),
            available_bytes: self.available_bytes.load(Ordering::Acquire),
            swap_used_bytes: self.swap_used_bytes.load(Ordering::Acquire),
        }
    }

    pub(crate) fn get(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<VerifiedChunkPayload> {
        self.maybe_refresh_pressure();
        let key = CacheKey {
            chunk_id,
            logical_length,
        };
        let hash = cache_hash(key);
        let shard = &self.shards[hash & (self.shards.len() - 1)];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: verified read-cache shard lock poisoned");
        let set_index = (hash / self.shards.len()) % state.sets.len();
        let payload = state.sets[set_index]
            .ways
            .iter()
            .flatten()
            .find(|entry| entry.key == key)
            .map(|entry| entry.payload.clone());
        if let Some(payload) = payload {
            assert_eq!(
                u64::try_from(payload.len()).ok(),
                Some(logical_length),
                "ASSERT: verified cache entry length changed after admission"
            );
            state.counters.hits = state.counters.hits.saturating_add(1);
            return Some(payload);
        }
        state.counters.misses = state.counters.misses.saturating_add(1);
        None
    }

    pub(crate) fn admit_verified(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        payload: VerifiedChunkPayload,
    ) {
        assert_eq!(
            u64::try_from(payload.len()).ok(),
            Some(logical_length),
            "ASSERT: Store returned a verified Chunk with the wrong length"
        );
        assert_eq!(
            ChunkId::of(payload.as_slice()),
            chunk_id,
            "ASSERT: Store returned bytes under the wrong verified Chunk ID"
        );
        self.maybe_refresh_pressure();
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: verified read-cache admission lock poisoned");
        let target = self.target_bytes.load(Ordering::Acquire);
        if target == 0 {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if payload.allocation_bytes() > target {
            self.oversized_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let key = CacheKey {
            chunk_id,
            logical_length,
        };
        let hash = cache_hash(key);
        let shard = &self.shards[hash & (self.shards.len() - 1)];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: verified read-cache shard lock poisoned");
        let set_index = (hash / self.shards.len()) % state.sets.len();
        let set = &mut state.sets[set_index];
        if set.ways.iter().flatten().any(|entry| entry.key == key) {
            return;
        }
        let victim = set
            .ways
            .iter()
            .position(Option::is_none)
            .unwrap_or(set.next_victim);
        let victim_bytes = set.ways[victim]
            .as_ref()
            .map_or(0, |entry| entry.payload.allocation_bytes());
        let resident = self.resident_bytes.load(Ordering::Acquire);
        let proposed = resident
            .checked_sub(victim_bytes)
            .and_then(|remaining| remaining.checked_add(payload.allocation_bytes()))
            .expect("ASSERT: verified read-cache resident accounting overflowed");
        if proposed > target {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let replaced = set.ways[victim].replace(CacheEntry { key, payload });
        set.next_victim = (victim + 1) % CACHE_WAYS;
        self.resident_bytes.store(proposed, Ordering::Release);
        if replaced.is_some() {
            state.counters.evictions = state.counters.evictions.saturating_add(1);
        } else {
            self.entry_count.fetch_add(1, Ordering::Release);
        }
        state.counters.admissions = state.counters.admissions.saturating_add(1);
        assert!(
            proposed <= target,
            "ASSERT: verified read cache exceeded its current payload target"
        );
        assert!(
            proposed.saturating_add(self.metadata_bytes) <= self.config.hard_limit_bytes,
            "ASSERT: verified read cache exceeded its hard RAM limit"
        );
    }

    fn maybe_refresh_pressure(&self) {
        if !self.automatic_pressure {
            return;
        }
        let elapsed_millis = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let previous = self.last_refresh_millis.load(Ordering::Relaxed);
        if elapsed_millis.saturating_sub(previous)
            < u64::try_from(SYSTEM_REFRESH_INTERVAL.as_millis())
                .expect("ASSERT: refresh interval fits u64")
            || self
                .last_refresh_millis
                .compare_exchange(
                    previous,
                    elapsed_millis,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        match MemoryPressureSnapshot::read_system() {
            Ok(snapshot) => self.apply_memory_pressure(snapshot),
            Err(_) => self.apply_memory_pressure(MemoryPressureSnapshot::new(0, 0, 1)),
        }
    }

    fn clear_locked(&self) {
        let mut removed = 0_usize;
        for shard in &self.shards {
            let mut shard_removed = 0_usize;
            let mut state = shard
                .state
                .lock()
                .expect("ASSERT: verified read-cache shard lock poisoned");
            for set in &mut state.sets {
                for way in &mut set.ways {
                    if way.take().is_some() {
                        shard_removed = shard_removed
                            .checked_add(1)
                            .expect("ASSERT: cache entry count cannot overflow");
                    }
                }
                set.next_victim = 0;
            }
            removed = removed
                .checked_add(shard_removed)
                .expect("ASSERT: cache entry count cannot overflow");
            state.counters.evictions = state.counters.evictions.saturating_add(
                u64::try_from(shard_removed).expect("ASSERT: shard cache entry count fits u64"),
            );
        }
        let previous_count = self.entry_count.swap(0, Ordering::AcqRel);
        assert_eq!(
            removed, previous_count,
            "ASSERT: verified read-cache entry accounting disagreed with its shards"
        );
        self.resident_bytes.store(0, Ordering::Release);
    }

    fn counters(&self) -> CacheShardCounters {
        self.shards
            .iter()
            .fold(CacheShardCounters::default(), |mut total, shard| {
                let state = shard
                    .state
                    .lock()
                    .expect("ASSERT: verified read-cache shard lock poisoned");
                total.add_assign(state.counters);
                total
            })
    }
}

fn cache_hash(key: CacheKey) -> usize {
    let bytes = key.chunk_id.bytes();
    let first = u64::from_le_bytes(bytes[..8].try_into().expect("ASSERT: exact hash slice"));
    let second = u64::from_le_bytes(bytes[8..16].try_into().expect("ASSERT: exact hash slice"));
    let mixed = first ^ second.rotate_left(17) ^ key.logical_length.rotate_left(31);
    usize::try_from(mixed).unwrap_or_else(|_| {
        usize::try_from(mixed ^ (mixed >> 32)).expect("ASSERT: folded hash fits usize")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_are_cache_line_separated_and_total_memory_is_hard_bounded() {
        let config = VerifiedReadCacheConfig::new(
            2 * 1_024 * 1_024,
            256 * 1_024,
            NonZeroUsize::new(4).expect("four shards"),
        )
        .expect("valid cache geometry");
        let cache = VerifiedReadCache::new_with_snapshot(
            config,
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 4 * 1_024 * 1_024, 0),
        )
        .expect("construct worked cache");

        assert_eq!(align_of::<CacheShard>(), 64);
        assert_eq!(size_of::<CacheShard>() % 64, 0);
        assert_eq!(cache.shards.as_ptr().addr() % 64, 0);
        let status = cache.status();
        assert!(status.metadata_bytes() > 0);
        assert!(
            status
                .target_bytes()
                .checked_add(status.metadata_bytes())
                .is_some_and(|bytes| bytes <= status.hard_limit_bytes())
        );
    }

    #[test]
    fn five_colliding_verified_chunks_replace_only_one_four_way_victim() {
        let cache = VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(2 * 1_024 * 1_024, 0, NonZeroUsize::MIN)
                .expect("valid one-shard geometry"),
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 8 * 1_024 * 1_024, 0),
        )
        .expect("construct collision cache");
        let set_count = cache.shards[0]
            .state
            .lock()
            .expect("ASSERT: fixture shard lock poisoned")
            .sets
            .len();
        let mut by_set = vec![Vec::<(ChunkId, Vec<u8>)>::new(); set_count];
        let collision = (0_u64..10_000).find_map(|nonce| {
            let mut bytes = vec![0_u8; CACHE_SLOT_TARGET_BYTES];
            bytes[..8].copy_from_slice(&nonce.to_le_bytes());
            let chunk_id = ChunkId::of(&bytes);
            let key = CacheKey {
                chunk_id,
                logical_length: u64::try_from(bytes.len()).expect("fixture length fits u64"),
            };
            let index = cache_hash(key) % set_count;
            by_set[index].push((chunk_id, bytes));
            (by_set[index].len() == CACHE_WAYS + 1).then(|| std::mem::take(&mut by_set[index]))
        });
        let collision = collision.expect("pigeonhole search finds five colliding fixture IDs");
        for (chunk_id, bytes) in &collision {
            cache.admit_verified(
                *chunk_id,
                u64::try_from(bytes.len()).expect("fixture length fits u64"),
                VerifiedChunkPayload::from_verified_store(bytes.clone()),
            );
        }

        assert_eq!(cache.status().entry_count(), CACHE_WAYS);
        assert_eq!(cache.status().evictions(), 1);
        assert_eq!(
            cache.get(
                collision[0].0,
                u64::try_from(collision[0].1.len()).expect("fixture length fits u64")
            ),
            None
        );
        for (chunk_id, bytes) in &collision[1..] {
            assert_eq!(
                cache.get(
                    *chunk_id,
                    u64::try_from(bytes.len()).expect("fixture length fits u64")
                ),
                Some(VerifiedChunkPayload::from_verified_store(bytes.clone()))
            );
        }
    }

    #[test]
    fn admission_and_hit_retain_the_decoder_owned_payload_allocation() {
        let cache = VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(2 * 1_024 * 1_024, 0, NonZeroUsize::MIN)
                .expect("valid cache geometry"),
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 8 * 1_024 * 1_024, 0),
        )
        .expect("construct ownership cache");
        let mut bytes = Vec::with_capacity(128 * 1_024);
        bytes.extend_from_slice(&b"decoder-owned verified payload".repeat(1_024));
        assert!(bytes.capacity() > bytes.len());
        let allocation_bytes = bytes.capacity();
        let address = bytes.as_ptr();
        let chunk_id = ChunkId::of(&bytes);
        let logical_length = u64::try_from(bytes.len()).expect("fixture length fits u64");
        let payload = VerifiedChunkPayload::from_verified_store(bytes);
        assert_eq!(payload.payload_address(), address);

        cache.admit_verified(chunk_id, logical_length, payload.clone());
        let hit = cache
            .get(chunk_id, logical_length)
            .expect("admitted verified payload is resident");

        assert_eq!(payload.payload_address(), address);
        assert_eq!(hit.payload_address(), address);
        assert_eq!(cache.status().resident_bytes(), allocation_bytes);
    }

    #[test]
    fn unrelated_host_and_shared_cgroup_swap_do_not_close_fastdup_cache_admission() {
        let config = VerifiedReadCacheConfig::new(2 * 1_024 * 1_024, 0, NonZeroUsize::MIN)
            .expect("valid cache geometry");
        let snapshot = MemoryPressureSnapshot::with_swap_state(
            8 * 1_024 * 1_024,
            8 * 1_024 * 1_024,
            0,
            4 * 1_024 * 1_024,
            3 * 1_024 * 1_024,
            Some(0),
        );
        let cache = VerifiedReadCache::new_with_snapshot(config, snapshot)
            .expect("construct host-Swap cache");

        assert!(cache.status().target_bytes() > 0);
        assert_eq!(cache.status().swap_used_bytes(), 0);
    }
}
