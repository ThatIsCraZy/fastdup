use crate::{CacheObservation, CachePool};
use fastdup_format::ChunkId;
pub(crate) use fastdup_format::VerifiedChunkPayload;
use std::array;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) use crate::memory_budget::{MemoryPressureSnapshot, SYSTEM_REFRESH_INTERVAL};

mod compression;
use compression::{CachedPayload, Compression};

const CACHE_WAYS: usize = 4;
const CACHE_SLOT_TARGET_BYTES: usize = 16 * 1_024;
const MAX_RECLAIM_STEPS_PER_GROUP: usize = 256;

/// Deterministic geometry and reserve for isolated tests and embedded runtimes.
/// Production residency is governed by the process-wide unified cache.
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
    /// Geometry permits the full shared budget; the adaptive broker assigns
    /// actual bytes according to observed reuse while preserving 8% headroom.
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
        let hard = effective.saturating_sub(reserve).max(64 * 1_024);
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
    crate::cache_memory_reserve(effective_limit_bytes)
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
    location_proofs: crate::ReadCacheStats,
    compressed_resident_bytes: usize,
    compressed_logical_bytes: usize,
    compression_attempts: u64,
    compressed_admissions: u64,
    compression_nanos: u64,
    compressed_hits: u64,
    decompressions: u64,
    decompression_nanos: u64,
    promotions: u64,
    demotions: u64,
    compression_failures: u64,
    compression_bypasses: u64,
    codec_working_bytes: usize,
    codec_peak_working_bytes: usize,
    codec_max_working_bytes: usize,
    buffer_pool: fastdup_format::BufferPoolStatus,
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
    status_getter!(location_proofs, location_proofs, crate::ReadCacheStats);
    status_getter!(compressed_resident_bytes, compressed_resident_bytes, usize);
    status_getter!(compressed_logical_bytes, compressed_logical_bytes, usize);
    status_getter!(compression_attempts, compression_attempts, u64);
    status_getter!(compressed_admissions, compressed_admissions, u64);
    status_getter!(compression_nanos, compression_nanos, u64);
    status_getter!(compressed_hits, compressed_hits, u64);
    status_getter!(decompressions, decompressions, u64);
    status_getter!(decompression_nanos, decompression_nanos, u64);
    status_getter!(promotions, promotions, u64);
    status_getter!(demotions, demotions, u64);
    status_getter!(compression_failures, compression_failures, u64);
    status_getter!(compression_bypasses, compression_bypasses, u64);
    status_getter!(codec_working_bytes, codec_working_bytes, usize);
    status_getter!(codec_peak_working_bytes, codec_peak_working_bytes, usize);
    status_getter!(codec_max_working_bytes, codec_max_working_bytes, usize);
    status_getter!(buffer_pool, buffer_pool, fastdup_format::BufferPoolStatus);
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
    payload: CachedPayload,
    recent_hits: u8,
    last_hit_epoch: u64,
    backing_charge: Arc<CacheBackingCharge>,
}

impl CacheEntry {
    fn matches(&self, key: CacheKey) -> bool {
        self.payload.chunk_id() == key.chunk_id
            && u64::try_from(self.payload.len()).ok() == Some(key.logical_length)
    }
}

#[derive(Debug)]
struct CacheBackingCharge {
    bytes: usize,
    compressed_logical: usize,
}

/// Result of one verified read operation.
///
/// Requested payloads retain logical caller order. Admission groups retain all
/// Chunk views sharing one decoded Record or encoded RAW batch backing so the
/// cache can account and admit that allocation exactly once.
#[derive(Clone, Debug)]
pub(crate) struct VerifiedChunkRead {
    requested: Vec<VerifiedChunkPayload>,
    admission_groups: Vec<Vec<VerifiedChunkPayload>>,
}

impl VerifiedChunkRead {
    pub(crate) fn new(
        requested: Vec<VerifiedChunkPayload>,
        admission_groups: Vec<Vec<VerifiedChunkPayload>>,
    ) -> Self {
        if admission_groups.len() <= 1 {
            let admission_groups = if admission_groups.first().is_some_and(Vec::is_empty) {
                Vec::new()
            } else {
                admission_groups
            };
            return Self {
                requested,
                admission_groups,
            };
        }
        let mut merged: Vec<Vec<VerifiedChunkPayload>> = Vec::new();
        let mut groups = admission_groups.into_iter();
        // Keep the allocation-free scan for small reads and shared-owner
        // batches. Promote only after seeing 32 different owners with enough
        // remaining work to amortize building the temporary index.
        while let Some(group) = groups.next() {
            let Some(first) = group.first() else {
                continue;
            };
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| existing[0].shares_backing_with(first))
            {
                existing.extend(group);
            } else {
                merged.push(group);
            }
            if merged.len() == 32 && groups.len() >= 32 {
                break;
            }
        }
        if groups.len() != 0 {
            let mut owners = hashbrown::HashMap::with_capacity(merged.len() + groups.len());
            owners.extend(
                merged
                    .iter()
                    .enumerate()
                    .map(|(ordinal, group)| (group[0].backing_id(), ordinal)),
            );
            for group in groups {
                let Some(first) = group.first() else {
                    continue;
                };
                let ordinal = *owners.entry(first.backing_id()).or_insert(merged.len());
                if ordinal < merged.len() {
                    merged[ordinal].extend(group);
                } else {
                    merged.push(group);
                }
            }
        }
        Self {
            requested,
            admission_groups: merged,
        }
    }

    pub(crate) fn single(
        requested: VerifiedChunkPayload,
        admission_group: Vec<VerifiedChunkPayload>,
    ) -> Self {
        Self::new(vec![requested], vec![admission_group])
    }

    pub(crate) fn into_parts(self) -> (Vec<VerifiedChunkPayload>, Vec<Vec<VerifiedChunkPayload>>) {
        (self.requested, self.admission_groups)
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
    hit_bytes: u64,
    hits: u64,
    misses: u64,
    admissions: u64,
    evictions: u64,
}

impl CacheShardCounters {
    fn add_assign(&mut self, other: Self) {
        self.hit_bytes = self.hit_bytes.saturating_add(other.hit_bytes);
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

#[derive(Debug, Default)]
struct CacheAdmission {
    reclaim_cursor: usize,
    compression_cursor: usize,
}

/// Verified DATA and compact physical-source views in the unified read cache.
/// The common owner governs allocation, admission, replacement and pressure;
/// verification-only callers need not retain the payload backing. The former
/// four-way implementation is retained solely as a test/replay comparator.
pub struct VerifiedReadCache {
    unified: Option<crate::ReadCacheNamespace>,
    location_proofs: Option<crate::ReadCacheNamespace>,
    config: VerifiedReadCacheConfig,
    shards: Box<[CacheShard]>,
    metadata_bytes: usize,
    admission: Mutex<CacheAdmission>,
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
    compression: Compression,
    budget_pool: Option<CachePool>,
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
        Self::build(config, snapshot, true, false)
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
        Self::build(config, snapshot, false, false)
    }

    // Historical policy retained only as a test/replay comparator.
    #[cfg(test)]
    fn new_legacy_with_snapshot(
        config: VerifiedReadCacheConfig,
        snapshot: MemoryPressureSnapshot,
    ) -> Result<Self, VerifiedReadCacheError> {
        Self::build(config, snapshot, false, true)
    }

    fn build(
        config: VerifiedReadCacheConfig,
        snapshot: MemoryPressureSnapshot,
        automatic_pressure: bool,
        legacy: bool,
    ) -> Result<Self, VerifiedReadCacheError> {
        let shard_count = config.shard_count.get();
        let approximate_set_bytes = CACHE_WAYS
            .checked_mul(CACHE_SLOT_TARGET_BYTES)
            .and_then(|payload| payload.checked_add(size_of::<CacheSet>()))
            .ok_or(VerifiedReadCacheError::GeometryTooSmall)?;
        let mut set_count = config.hard_limit_bytes / approximate_set_bytes;
        set_count -= set_count % shard_count;
        if set_count < shard_count && legacy {
            return Err(VerifiedReadCacheError::GeometryTooSmall);
        }
        if !legacy {
            set_count = 0;
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
        for _ in 0..if legacy { shard_count } else { 0 } {
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
        let unified = (!legacy).then(|| {
            if automatic_pressure {
                crate::ReadCacheNamespace::system(crate::ReadCacheClass::Data)
            } else {
                crate::ReadCacheNamespace::isolated(crate::ReadCacheClass::Data, 0)
            }
        });
        let cache = Self {
            location_proofs: unified
                .as_ref()
                .map(|cache| cache.sibling(crate::ReadCacheClass::LocationProof)),
            unified,
            config,
            shards: shards.into_boxed_slice(),
            metadata_bytes,
            admission: Mutex::new(CacheAdmission::default()),
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
            compression: Compression::new(snapshot, automatic_pressure),
            budget_pool: None,
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
        if let Some(cache) = &self.unified {
            if !self.automatic_pressure {
                cache.update_pressure(
                    snapshot,
                    self.config.hard_limit_bytes as u64,
                    self.config.reserve_bytes,
                );
            }
            self.compression.refresh_buffers(snapshot);
            self.target_bytes.store(
                usize::try_from(cache.capacity()).unwrap_or(usize::MAX),
                Ordering::Release,
            );
            self.effective_limit_bytes
                .store(snapshot.effective_limit_bytes(), Ordering::Release);
            self.available_bytes
                .store(snapshot.available_bytes(), Ordering::Release);
            self.swap_used_bytes
                .store(snapshot.swap_used_bytes(), Ordering::Release);
            return;
        }

        self.compression.refresh_buffers(snapshot);
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: verified read-cache admission lock poisoned");
        let total_target = if let Some(pool) = &self.budget_pool {
            let counters = self.counters();
            usize::try_from(pool.target(
                snapshot,
                CacheObservation {
                    hits: counters.hits,
                    misses: counters.misses,
                    evictions: counters.evictions,
                    hit_bytes: counters.hit_bytes,
                    resident_bytes: (self.resident_bytes.load(Ordering::Acquire)
                        + self.metadata_bytes) as u64,
                },
            ))
            .unwrap_or(usize::MAX)
        } else if snapshot.swap_used_bytes() != 0 {
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
            if self.budget_pool.is_some() && payload_target != 0 {
                let mut steps = self
                    .shards
                    .iter()
                    .map(|shard| {
                        shard
                            .state
                            .lock()
                            .expect("ASSERT: cache shard lock poisoned")
                            .sets
                            .len()
                            * CACHE_WAYS
                    })
                    .sum();
                self.reclaim_locked(&mut admission, &mut steps, payload_target, None);
            } else {
                self.clear_locked();
            }
        }
        if let Some(pool) = &self.budget_pool {
            pool.applied(
                total_target as u64,
                (self.resident_bytes.load(Ordering::Acquire) + self.metadata_bytes) as u64,
            );
        }
    }

    /// Samples consistent resident accounting and cumulative counters.
    ///
    /// # Panics
    /// Panics only if an earlier internal invariant poisoned a cache lock.
    #[must_use]
    pub fn status(&self) -> VerifiedReadCacheStatus {
        self.maybe_refresh_pressure();
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: cache admission lock poisoned");
        let counters = self.counters();
        let mut resident = self.resident_bytes.load(Ordering::Acquire);
        let mut entries = self.entry_count.load(Ordering::Acquire);
        let mut compressed = self.compression.resident.load(Ordering::Relaxed);
        let mut logical = self.compression.logical.load(Ordering::Relaxed);
        if let Some(cache) = &self.unified {
            let stats = cache.stats();
            resident = usize::try_from(stats.resident_bytes).unwrap_or(usize::MAX);
            entries = usize::try_from(stats.entries).unwrap_or(usize::MAX);
            compressed = 0;
            logical = 0;
            cache.visit::<CachedPayload>(|payload| {
                if let CachedPayload::Compressed(value) = payload {
                    compressed += value.resident_bytes();
                    logical += value.logical_length();
                }
            });
        }
        VerifiedReadCacheStatus {
            location_proofs: self.location_proofs.as_ref().map_or_else(
                crate::ReadCacheStats::default,
                crate::ReadCacheNamespace::stats,
            ),
            compressed_resident_bytes: compressed,
            compressed_logical_bytes: logical,
            compression_attempts: self.compression.attempts.load(Ordering::Relaxed),
            compressed_admissions: self.compression.admitted.load(Ordering::Relaxed),
            compression_nanos: self.compression.compress_ns.load(Ordering::Relaxed),
            compressed_hits: self.compression.hits.load(Ordering::Relaxed),
            decompressions: self.compression.decodes.load(Ordering::Relaxed),
            decompression_nanos: self.compression.decode_ns.load(Ordering::Relaxed),
            promotions: self.compression.promotions.load(Ordering::Relaxed),
            demotions: self.compression.demotions.load(Ordering::Relaxed),
            compression_failures: self.compression.failures.load(Ordering::Relaxed),
            compression_bypasses: self.compression.bypasses.load(Ordering::Relaxed),
            codec_peak_working_bytes: self.compression.peak_working.load(Ordering::Relaxed),
            codec_working_bytes: self.compression.working(),
            codec_max_working_bytes: self.compression.maximum_working(),
            buffer_pool: self.compression.buffers.status(),
            hits: counters.hits,
            misses: counters.misses,
            admissions: counters.admissions,
            evictions: counters.evictions,
            pressure_rejections: self.pressure_rejections.load(Ordering::Relaxed),
            oversized_rejections: self.oversized_rejections.load(Ordering::Relaxed),
            entry_count: entries,
            resident_bytes: resident,
            target_bytes: self.target_bytes.load(Ordering::Acquire),
            metadata_bytes: self.metadata_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            reserve_bytes: self.config.reserve_bytes,
            effective_limit_bytes: self.effective_limit_bytes.load(Ordering::Acquire),
            available_bytes: self.available_bytes.load(Ordering::Acquire),
            swap_used_bytes: self.swap_used_bytes.load(Ordering::Acquire),
        }
    }

    /// Small physical-source evidence in the same owner as the DATA view.
    /// Returning a proof never returns bytes or selects a live generation.
    pub(crate) fn has_verified_location(&self, candidate: fastdup_format::ExactIndexEntry) -> bool {
        self.location_proofs.as_ref().is_some_and(|cache| {
            cache
                .get::<fastdup_format::ExactIndexEntry>(location_proof_key(candidate))
                .is_some_and(|entry| *entry == candidate)
        })
    }

    // Call only after this exact candidate's complete stored Record, logical
    // bytes and any Base have been verified, never on an unverified Exact hit.
    pub(crate) fn admit_verified_location(&self, entry: fastdup_format::ExactIndexEntry) {
        if let Some(cache) = &self.location_proofs {
            cache.insert(
                location_proof_key(entry),
                Arc::new(entry),
                size_of::<fastdup_format::ExactIndexEntry>() as u64,
                u64::from(entry.location().record_length()),
            );
        }
    }

    pub(crate) fn admit_location_proofs(&self, payloads: &[VerifiedChunkPayload]) {
        for payload in payloads {
            if let Some(location) = payload.verified_location()
                && let Ok(entry) = fastdup_format::ExactIndexEntry::from_verified(location)
            {
                self.admit_verified_location(entry);
            }
        }
    }

    pub(crate) fn get(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Option<VerifiedChunkPayload> {
        if crate::read_intent::independent() {
            return None;
        }
        self.maybe_refresh_pressure();
        if let Some(cache) = &self.unified {
            let key = crate::ReadCacheKey {
                identity: chunk_id.bytes(),
                ordinal: logical_length,
            };
            let value = cache.get::<CachedPayload>(key)?;
            return match value.as_ref() {
                CachedPayload::Decoded(payload) => Some(payload.clone()),
                CachedPayload::Compressed(value) => self.compressed_hit(value, 0),
            };
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
        let epoch = self.compression.epoch.load(Ordering::Relaxed);
        let window = self.entry_count.load(Ordering::Relaxed).max(1) as u64;
        let found = state.sets[set_index]
            .ways
            .iter_mut()
            .flatten()
            .find(|entry| entry.matches(key))
            .map(|entry| {
                entry.recent_hits = if epoch.saturating_sub(entry.last_hit_epoch) > window {
                    1
                } else {
                    entry.recent_hits.saturating_add(1)
                };
                entry.last_hit_epoch = epoch;
                (entry.payload.clone(), entry.recent_hits)
            });
        let Some((cached, recent_hits)) = found else {
            state.counters.misses = state.counters.misses.saturating_add(1);
            return None;
        };
        let payload = match cached {
            CachedPayload::Decoded(payload) => payload,
            CachedPayload::Compressed(value) => {
                drop(state);
                let payload = self.compressed_hit(&value, recent_hits);
                state = shard
                    .state
                    .lock()
                    .expect("ASSERT: cache shard lock poisoned");
                let Some(payload) = payload else {
                    state.counters.misses = state.counters.misses.saturating_add(1);
                    return None;
                };
                payload
            }
        };
        assert_eq!(
            payload.len() as u64,
            logical_length,
            "ASSERT: verified cache entry length changed after admission"
        );
        state.counters.hit_bytes = state.counters.hit_bytes.saturating_add(logical_length);
        state.counters.hits = state.counters.hits.saturating_add(1);
        Some(payload)
    }

    #[cfg(test)]
    pub(crate) fn admit_verified(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        payload: VerifiedChunkPayload,
    ) {
        assert_eq!(payload.chunk_id(), chunk_id);
        assert_eq!(u64::try_from(payload.len()).ok(), Some(logical_length));
        self.admit_decoded_group(vec![payload]);
    }

    /// Atomically accounts one decoder backing while admitting any number of
    /// verified Chunk views from that Encoding Record.
    #[allow(clippy::too_many_lines)]
    fn admit_group(&self, payloads: Vec<CachedPayload>) {
        if crate::read_intent::bypass_admission() {
            return;
        }
        let Some(first) = payloads.first() else {
            return;
        };
        let (allocation_bytes, compressed_logical) = match first {
            CachedPayload::Decoded(first) => {
                for payload in &payloads {
                    let CachedPayload::Decoded(payload) = payload else {
                        panic!("ASSERT: mixed cache admission representations");
                    };
                    assert!(
                        first.shares_backing_with(payload),
                        "ASSERT: one decoded admission group shares one backing"
                    );
                }
                (first.backing_allocation_bytes(), 0)
            }
            CachedPayload::Compressed(value) => {
                assert_eq!(
                    payloads.len(),
                    1,
                    "ASSERT: compressed chunks own independent storage"
                );
                (value.resident_bytes(), value.logical_length())
            }
        };
        self.maybe_refresh_pressure();
        if let Some(cache) = &self.unified {
            let values = payloads
                .into_iter()
                .map(|value| {
                    let length = value.len() as u64;
                    (
                        crate::ReadCacheKey {
                            identity: value.chunk_id().bytes(),
                            ordinal: length,
                        },
                        Arc::new(value),
                        length,
                    )
                })
                .collect();
            let admitted = cache.insert_groups(vec![(values, allocation_bytes as u64)]);
            if compressed_logical != 0 {
                self.compression
                    .admitted
                    .fetch_add(admitted, Ordering::Relaxed);
            }
            return;
        }
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: verified read-cache admission lock poisoned");
        let target = self.target_bytes.load(Ordering::Acquire);
        if target == 0 {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if allocation_bytes > target {
            self.oversized_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let backing_charge = Arc::new(CacheBackingCharge {
            bytes: allocation_bytes,
            compressed_logical,
        });
        let mut admitted_group_refs = 0_usize;
        let mut reclaim_steps = MAX_RECLAIM_STEPS_PER_GROUP;
        for payload in payloads {
            let key = CacheKey {
                chunk_id: payload.chunk_id(),
                logical_length: u64::try_from(payload.len())
                    .expect("ASSERT: verified logical Chunk length fits u64"),
            };
            let hash = cache_hash(key);
            let shard = &self.shards[hash & (self.shards.len() - 1)];
            loop {
                let mut state = shard
                    .state
                    .lock()
                    .expect("ASSERT: verified read-cache shard lock poisoned");
                let set_index = (hash / self.shards.len()) % state.sets.len();
                let set = &mut state.sets[set_index];
                if set.ways.iter().flatten().any(|entry| entry.matches(key)) {
                    break;
                }
                let victim = set
                    .ways
                    .iter()
                    .position(Option::is_none)
                    .unwrap_or(set.next_victim);
                let victim_is_group = set.ways[victim]
                    .as_ref()
                    .is_some_and(|entry| Arc::ptr_eq(&entry.backing_charge, &backing_charge));
                let victim_bytes = set.ways[victim].as_ref().map_or(0, |entry| {
                    if victim_is_group {
                        usize::from(admitted_group_refs == 1) * entry.backing_charge.bytes
                    } else if Arc::strong_count(&entry.backing_charge) == 1 {
                        entry.backing_charge.bytes
                    } else {
                        0
                    }
                });
                let remaining_group_refs = admitted_group_refs - usize::from(victim_is_group);
                let added_bytes = if remaining_group_refs == 0 {
                    backing_charge.bytes
                } else {
                    0
                };
                let resident = self.resident_bytes.load(Ordering::Acquire);
                let proposed = resident
                    .checked_sub(victim_bytes)
                    .and_then(|remaining| remaining.checked_add(added_bytes))
                    .expect("ASSERT: verified read-cache resident accounting overflowed");
                if proposed > target {
                    if reclaim_steps != 0 {
                        // Reclaim outside the target shard: readers only ever hold
                        // one shard, and admission is already globally serialized.
                        // Protect this group's admitted views so its local reference
                        // accounting stays valid across the retry.
                        drop(state);
                        self.reclaim_locked(
                            &mut admission,
                            &mut reclaim_steps,
                            target.saturating_sub(added_bytes),
                            Some(&backing_charge),
                        );
                        continue;
                    }
                    self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                let replaced = set.ways[victim].replace(CacheEntry {
                    payload,
                    recent_hits: 0,
                    last_hit_epoch: self.compression.epoch.load(Ordering::Relaxed),
                    backing_charge: Arc::clone(&backing_charge),
                });
                if victim_bytes != 0 {
                    self.uncharge(
                        &replaced
                            .as_ref()
                            .expect("ASSERT: charged victim exists")
                            .backing_charge,
                    );
                }
                if added_bytes != 0 {
                    self.charge(&backing_charge);
                }
                admitted_group_refs = remaining_group_refs + 1;
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
                break;
            }
        }
    }

    /// Persistent round-robin reclamation; the caller holds admission and no
    /// shard lock. A group has a fixed probe budget even when a backing spans
    /// many sets. Later admissions continue the cursor instead of rescanning
    /// the same prefix. Cache hits do not touch this cursor or a global lock.
    fn reclaim_locked(
        &self,
        admission: &mut CacheAdmission,
        remaining_steps: &mut usize,
        resident_target: usize,
        protected: Option<&Arc<CacheBackingCharge>>,
    ) {
        while *remaining_steps != 0 && self.resident_bytes.load(Ordering::Acquire) > resident_target
        {
            *remaining_steps -= 1;
            let cursor = admission.reclaim_cursor;
            let shard = &self.shards[cursor % self.shards.len()];
            let mut state = shard
                .state
                .lock()
                .expect("ASSERT: verified read-cache shard lock poisoned");
            let shard_cursor = cursor / self.shards.len();
            let set_index = (shard_cursor / CACHE_WAYS) % state.sets.len();
            let way = shard_cursor % CACHE_WAYS;
            let slots = self.shards.len() * state.sets.len() * CACHE_WAYS;
            admission.reclaim_cursor = (cursor + 1) % slots;
            let slot = &mut state.sets[set_index].ways[way];
            if slot.as_ref().is_none_or(|entry| {
                protected.is_some_and(|protected| Arc::ptr_eq(&entry.backing_charge, protected))
            }) {
                continue;
            }
            let entry = slot
                .take()
                .expect("ASSERT: selected reclaim slot is populated");
            // Caller payload views do not own CacheBackingCharge. Only the
            // final cache view releases its one shared allocation charge.
            if Arc::strong_count(&entry.backing_charge) == 1 {
                let bytes = entry.backing_charge.bytes;
                self.uncharge(&entry.backing_charge);
                let previous = self.resident_bytes.fetch_sub(bytes, Ordering::AcqRel);
                assert!(
                    previous >= bytes,
                    "ASSERT: reclaimed backing was fully charged"
                );
            }
            let previous = self.entry_count.fetch_sub(1, Ordering::AcqRel);
            assert!(previous != 0, "ASSERT: reclaimed cache entry was counted");
            state.counters.evictions = state.counters.evictions.saturating_add(1);
        }
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
        self.compression.resident.store(0, Ordering::Release);
        self.compression.logical.store(0, Ordering::Release);
    }

    fn counters(&self) -> CacheShardCounters {
        if let Some(cache) = &self.unified {
            let stats = cache.stats();
            return CacheShardCounters {
                hits: stats.hits,
                misses: stats.misses,
                admissions: stats.admissions,
                evictions: stats.evictions,
                hit_bytes: stats.hit_bytes,
            };
        }

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

// A logical Chunk can have several independently checked physical copies.
// Keep them as distinct residents in the common directory, including while
// predecessor and successor Exact generations are both pinned. Compare the
// complete entry on every hit as well; the digest is only a directory key.
fn location_proof_key(entry: fastdup_format::ExactIndexEntry) -> crate::ReadCacheKey {
    let location = entry.location();
    let mut hash = blake3::Hasher::new();
    hash.update(b"fastdup/verified-location/v1");
    hash.update(&entry.chunk_id().bytes());
    hash.update(&entry.logical_length().to_le_bytes());
    hash.update(&location.container_id().bytes());
    hash.update(&location.container_generation().to_le_bytes());
    hash.update(&location.record_offset().to_le_bytes());
    hash.update(&location.record_length().to_le_bytes());
    hash.update(&location.chunk_ordinal().to_le_bytes());
    hash.update(&location.decoded_offset().to_le_bytes());
    hash.update(&location.record_crc32c().to_le_bytes());
    hash.update(&location.record_decoded_length().to_le_bytes());
    hash.update(&location.record_payload_length().to_le_bytes());
    hash.update(&location.codec_id().to_le_bytes());
    hash.update(&location.dependency_id());
    crate::ReadCacheKey {
        identity: *hash.finalize().as_bytes(),
        ordinal: 0,
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
#[path = "read_cache/reclamation_tests.rs"]
mod reclamation_tests;

#[cfg(test)]
#[path = "read_cache/compression_tests.rs"]
mod compression_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn linear_groups(groups: Vec<Vec<VerifiedChunkPayload>>) -> Vec<Vec<VerifiedChunkPayload>> {
        let mut merged: Vec<Vec<VerifiedChunkPayload>> = Vec::new();
        for group in groups {
            let Some(first) = group.first() else {
                continue;
            };
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| existing[0].shares_backing_with(first))
            {
                existing.extend(group);
            } else {
                merged.push(group);
            }
        }
        merged
    }

    #[test]
    fn admission_group_index_preserves_first_owner_and_payload_order() {
        let owners = (0..128_u32)
            .map(|n| verified_payload(&n.to_le_bytes()))
            .collect::<Vec<_>>();
        for count in [1, 4, 16, 32, 64, 128] {
            for unique in [4, 128] {
                let groups = (0..count)
                    .map(|n| vec![owners[n % unique].clone()])
                    .collect::<Vec<_>>();
                let expected = linear_groups(groups.clone());
                let (_, actual) =
                    VerifiedChunkRead::new(vec![owners[0].clone()], groups).into_parts();
                assert_eq!(actual.len(), expected.len());
                for (actual, expected) in actual.iter().zip(&expected) {
                    assert_eq!(actual, expected);
                    assert_eq!(actual[0].backing_id(), expected[0].backing_id());
                }
            }
        }
    }

    #[test]
    #[ignore = "manual release-mode read admission grouping A/B"]
    fn read_admission_grouping_microbenchmark() {
        use std::hint::black_box;
        let owners = (0..256_u32)
            .map(|n| verified_payload(&n.to_le_bytes()))
            .collect::<Vec<_>>();
        for count in [1, 4, 16, 32, 64, 128, 256] {
            for unique in [4, count] {
                let fixture = (0..count)
                    .map(|n| vec![owners[n % unique].clone()])
                    .collect::<Vec<_>>();
                let mut samples = [Vec::new(), Vec::new()];
                for round in 0..11 {
                    for side in 0..2 {
                        let side = (side + round) % 2;
                        let batches = (0..500).map(|_| fixture.clone()).collect::<Vec<_>>();
                        let start = Instant::now();
                        for groups in batches {
                            if side == 0 {
                                black_box(linear_groups(groups));
                            } else {
                                black_box(VerifiedChunkRead::new(Vec::new(), groups));
                            }
                        }
                        samples[side].push(start.elapsed());
                    }
                }
                for samples in &mut samples {
                    samples.sort_unstable();
                }
                println!(
                    "read_grouping groups={count} unique={unique} linear_ns={:.1} indexed_ns={:.1} speedup={:.3}",
                    samples[0][5].as_secs_f64() * 2_000_000.0,
                    samples[1][5].as_secs_f64() * 2_000_000.0,
                    samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
                );
            }
        }
    }

    pub(super) fn verified_payload(bytes: &[u8]) -> VerifiedChunkPayload {
        let encoded = fastdup_format::RawRecord::encode(bytes).expect("encode fixture Record");
        fastdup_format::RawRecord::decode(&encoded)
            .expect("decode and verify fixture Record")
            .into_verified_payload()
    }

    #[test]
    fn full_byte_budget_admits_a_new_workload_into_an_empty_set() {
        let cache = VerifiedReadCache::new_legacy_with_snapshot(
            VerifiedReadCacheConfig::new(2 * 1024 * 1024, 0, NonZeroUsize::MIN).unwrap(),
            MemoryPressureSnapshot::new(8 * 1024 * 1024, 8 * 1024 * 1024, 0),
        )
        .unwrap();
        cache.compression.enabled.store(false, Ordering::Relaxed);
        let old = verified_payload(&vec![31; 65536]);
        let allocation = old.backing_allocation_bytes();
        let set_count = cache.shards[0].state.lock().unwrap().sets.len();
        let set_for = |payload: &VerifiedChunkPayload| {
            cache_hash(CacheKey {
                chunk_id: payload.chunk_id(),
                logical_length: u64::try_from(payload.len()).unwrap(),
            }) % set_count
        };
        let new = (32..=255)
            .map(|value| verified_payload(&vec![value; 65536]))
            .find(|payload| set_for(payload) != set_for(&old))
            .unwrap();
        cache.update_memory_pressure(MemoryPressureSnapshot::new(
            8 * 1024 * 1024,
            u64::try_from(cache.status().metadata_bytes() + allocation).unwrap(),
            0,
        ));
        cache.admit_decoded_group(vec![old]);
        assert_eq!(cache.status().resident_bytes(), allocation);
        cache.admit_decoded_group(vec![new.clone()]);
        assert_eq!(
            cache.get(new.chunk_id(), 65536),
            Some(new),
            "a full byte budget must recycle old data instead of permanently rejecting a new working set"
        );
        assert!(cache.status().resident_bytes() <= allocation);
    }

    #[test]
    fn shards_are_cache_line_separated_and_total_memory_is_hard_bounded() {
        let config = VerifiedReadCacheConfig::new(
            2 * 1_024 * 1_024,
            256 * 1_024,
            NonZeroUsize::new(4).expect("four shards"),
        )
        .expect("valid cache geometry");
        let cache = VerifiedReadCache::new_legacy_with_snapshot(
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
        let cache = VerifiedReadCache::new_legacy_with_snapshot(
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
                verified_payload(bytes),
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
                Some(verified_payload(bytes))
            );
        }
    }

    #[test]
    fn admission_and_hit_share_and_charge_the_decoder_owned_payload_allocation_once() {
        let cache = VerifiedReadCache::new_legacy_with_snapshot(
            VerifiedReadCacheConfig::new(2 * 1_024 * 1_024, 0, NonZeroUsize::MIN)
                .expect("valid cache geometry"),
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 8 * 1_024 * 1_024, 0),
        )
        .expect("construct ownership cache");
        cache.compression.enabled.store(false, Ordering::Relaxed);
        let mut bytes = Vec::with_capacity(128 * 1_024);
        bytes.extend_from_slice(&b"decoder-owned verified payload".repeat(1_024));
        assert!(bytes.capacity() > bytes.len());
        let chunk_id = ChunkId::of(&bytes);
        let logical_length = u64::try_from(bytes.len()).expect("fixture length fits u64");
        let payload = verified_payload(&bytes);
        let allocation_bytes = payload.backing_allocation_bytes();

        cache.admit_verified(chunk_id, logical_length, payload.clone());
        let hit = cache
            .get(chunk_id, logical_length)
            .expect("admitted verified payload is resident");

        assert!(payload.shares_backing_with(&hit));
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
        let cache = VerifiedReadCache::new_legacy_with_snapshot(config, snapshot)
            .expect("construct host-Swap cache");

        assert!(cache.status().target_bytes() > 0);
        assert_eq!(cache.status().swap_used_bytes(), 0);
    }
    #[test]
    #[ignore = "manual release-mode verified DATA hit-path A/B"]
    fn adaptive_budget_hit_path_benchmark() {
        let cache = VerifiedReadCache::new_legacy_with_snapshot(
            VerifiedReadCacheConfig::new(1024 * 1024, 0, NonZeroUsize::MIN).unwrap(),
            MemoryPressureSnapshot::new(8 * 1024 * 1024, 8 * 1024 * 1024, 0),
        )
        .unwrap();
        let payload = verified_payload(b"cache hit payload");
        let id = payload.chunk_id();
        let length = payload.len() as u64;
        cache.admit_decoded_group(vec![payload]);
        assert!(cache.get(id, length).is_some());
        for round in 0..7 {
            let start = Instant::now();
            for _ in 0..500_000 {
                std::hint::black_box(cache.get(id, length).unwrap());
            }
            println!(
                "verified_hit round={round} queries=500000 elapsed_ns={}",
                start.elapsed().as_nanos()
            );
        }
    }
}
