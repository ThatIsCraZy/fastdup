use std::fmt;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use fastdup_format::{ChunkId, ExactIndexEntry, ExactLocationTransition};
use fastdup_store::{MemoryPressureSnapshot, shared_cache_reserve_bytes};

const ACCOUNTED_ENTRY_BYTES: usize = 224;
const DEFAULT_SHARDS: usize = 256;
const REFRESH_MILLIS: u64 = 250;

fn stable_capacity_entries(capacity: u64, entry_bytes: u64) -> usize {
    const MIB: u64 = 1 << 20;
    if capacity == 0 || entry_bytes == 0 {
        return 0;
    }
    let granularity_bytes = (2 * MIB).min(capacity);
    let granularity_entries =
        usize::try_from(granularity_bytes.div_ceil(entry_bytes)).unwrap_or(usize::MAX);
    let entries = usize::try_from(capacity / entry_bytes).unwrap_or(usize::MAX);
    entries
        .checked_div(granularity_entries)
        .unwrap_or(entries)
        .saturating_mul(granularity_entries)
}

/// How a committed online dependency entered the historical proof cache.
/// The unified owner no longer keeps separate probation queues; both
/// origins share FIFO admission, hit credit, and ghost re-entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoricalProofAdmission {
    /// A newly published Chunk with no reuse evidence yet.
    Published,
    /// A selected Exact reuse; this is not payload proof.
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
    #[must_use]
    pub(crate) fn conservative(snapshot: MemoryPressureSnapshot) -> Self {
        let effective = snapshot.effective_limit_bytes().max(1);
        let hard = usize::try_from(effective.saturating_sub(shared_cache_reserve_bytes(effective)))
            .unwrap_or(usize::MAX)
            .max(ACCOUNTED_ENTRY_BYTES);
        Self {
            hard_limit_bytes: hard,
            reserve_bytes: shared_cache_reserve_bytes(effective),
            shard_count: NonZeroUsize::new(DEFAULT_SHARDS)
                .expect("ASSERT: default Historical Proof Cache shard count is nonzero"),
        }
    }
}

/// Historical-proof observations within the common read cache.
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

/// Typed historical DATA proofs owned by the process-wide unified cache.
/// Admission, replacement, ghost re-entry, and pressure all follow the
/// common owner; this adapter only carries identity and telemetry.
///
/// Cache misses, rejected admissions, and allocation failures never
/// affect storage correctness.
pub(crate) struct HistoricalProofCache {
    unified: fastdup_store::ReadCacheNamespace,
    config: HistoricalProofCacheConfig,
    target_entries: AtomicUsize,
    unsharded_misses: AtomicU64,
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
            .finish_non_exhaustive()
    }
}

impl HistoricalProofCache {
    /// Constructs the production cache with automatic host/cgroup pressure
    /// sampling. A sampling failure starts with admission disabled and
    /// retries on the normal refresh path.
    pub(crate) fn new_system() -> Self {
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        Self::build(
            HistoricalProofCacheConfig::conservative(snapshot),
            snapshot,
            true,
        )
    }

    /// Constructs a deterministic manually refreshed cache.
    #[cfg(test)]
    pub(crate) fn new_with_snapshot(
        config: HistoricalProofCacheConfig,
        snapshot: MemoryPressureSnapshot,
    ) -> Self {
        Self::build(config, snapshot, false)
    }

    fn build(
        config: HistoricalProofCacheConfig,
        snapshot: MemoryPressureSnapshot,
        automatic_pressure: bool,
    ) -> Self {
        debug_assert!(config.shard_count.get().is_power_of_two());
        let unified = if automatic_pressure {
            fastdup_store::ReadCacheNamespace::system(
                fastdup_store::ReadCacheClass::HistoricalProof,
            )
        } else {
            fastdup_store::ReadCacheNamespace::isolated(
                fastdup_store::ReadCacheClass::HistoricalProof,
                0,
            )
        };
        let cache = Self {
            unified,
            config,
            target_entries: AtomicUsize::new(0),
            unsharded_misses: AtomicU64::new(0),
            effective_limit_bytes: AtomicU64::new(0),
            available_bytes: AtomicU64::new(0),
            swap_used_bytes: AtomicU64::new(0),
            automatic_pressure,
            started: Instant::now(),
            last_refresh_millis: AtomicU64::new(0),
        };
        cache.apply_memory_pressure(snapshot);
        cache
    }

    /// Returns a verified Location on a full Chunk ID and length match.
    #[must_use]
    pub(crate) fn get(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        if fastdup_store::ReadIntentScope::current() == fastdup_store::ReadIntent::Independent {
            return None;
        }
        self.maybe_refresh_pressure();
        let Ok(logical_length) = u32::try_from(logical_length) else {
            self.unsharded_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        self.unified
            .get::<ExactIndexEntry>(fastdup_store::ReadCacheKey {
                identity: chunk_id.bytes(),
                ordinal: u64::from(logical_length),
            })
            .map(|value| *value)
    }

    /// Admits one fully verified ACTIVE Location.
    ///
    /// Allocation or pressure rejection is deliberately silent to callers.
    pub(crate) fn admit(&self, entry: ExactIndexEntry, admission: HistoricalProofAdmission) {
        if fastdup_store::ReadIntentScope::current() != fastdup_store::ReadIntent::Demand {
            return;
        }
        assert_eq!(
            entry.transition(),
            ExactLocationTransition::Active,
            "ASSERT: Historical Proof Cache accepts only ACTIVE verified Locations"
        );
        let _ = admission;
        self.maybe_refresh_pressure();
        let key = fastdup_store::ReadCacheKey {
            identity: entry.chunk_id().bytes(),
            ordinal: u64::from(entry.logical_length()),
        };
        self.unified.remove(key);
        self.unified.insert(
            key,
            Arc::new(entry),
            size_of::<ExactIndexEntry>() as u64,
            u64::from(entry.logical_length()),
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
        let stats = self.unified.stats();
        HistoricalProofCacheStatus {
            hits: stats.hits,
            misses: stats
                .misses
                .saturating_add(self.unsharded_misses.load(Ordering::Relaxed)),
            admissions: stats.admissions,
            admission_rejections: stats.rejections,
            allocation_rejections: 0,
            evictions: stats.evictions,
            ghost_hits: stats.ghost_hits,
            entry_count: usize::try_from(stats.entries).unwrap_or(usize::MAX),
            target_entries: self.target_entries.load(Ordering::Acquire),
            resident_bytes: usize::try_from(stats.resident_bytes).unwrap_or(usize::MAX),
            metadata_bytes: 0,
            hard_limit_bytes: self.config.hard_limit_bytes,
            reserve_bytes: self.config.reserve_bytes,
            maximum_eviction_steps: 0,
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
        if !self.automatic_pressure {
            self.unified.update_pressure(
                snapshot,
                u64::try_from(self.config.hard_limit_bytes).unwrap_or(u64::MAX),
                self.config.reserve_bytes,
            );
        }
        self.target_entries.store(
            stable_capacity_entries(self.unified.capacity(), ACCOUNTED_ENTRY_BYTES as u64),
            Ordering::Release,
        );
        self.effective_limit_bytes
            .store(snapshot.effective_limit_bytes(), Ordering::Release);
        self.available_bytes
            .store(snapshot.available_bytes(), Ordering::Release);
        self.swap_used_bytes
            .store(snapshot.swap_used_bytes(), Ordering::Release);
    }
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
            hard_limit_bytes: 4 * 1_024 * 1_024,
            reserve_bytes: 0,
            shard_count: NonZeroUsize::new(1).expect("one shard is nonzero"),
        };
        let cache = HistoricalProofCache::new_with_snapshot(
            config,
            MemoryPressureSnapshot::new(1 << 30, 1 << 30, 0),
        );
        // Bound the isolated unified cache to a small working set so the
        // fixture exercises eviction without a huge entry count.
        cache.update_memory_pressure(MemoryPressureSnapshot::new(1 << 30, 32 * 1_024, 0));
        cache
    }

    #[test]
    fn warm_proof_survives_a_published_scan() {
        let cache = cache();
        let hot = entry(1);
        cache.admit(hot, HistoricalProofAdmission::ExactReuse);
        for _ in 0..3 {
            assert_eq!(
                cache.get(hot.chunk_id(), u64::from(hot.logical_length())),
                Some(hot)
            );
        }
        for ordinal in 2..=220_u64 {
            cache.admit(entry(ordinal), HistoricalProofAdmission::Published);
        }
        assert_eq!(
            cache.get(hot.chunk_id(), u64::from(hot.logical_length())),
            Some(hot),
            "credited proofs must survive cold scan admissions"
        );
        let status = cache.status();
        assert!(status.evictions() > 0);
        assert!(status.resident_bytes() <= 32 * 1_024);
    }

    #[test]
    fn shrinking_reclaims_resident_proofs_to_the_new_capacity() {
        let cache = cache();
        for ordinal in 1..=40_u64 {
            cache.admit(entry(ordinal), HistoricalProofAdmission::Published);
        }
        let resident = cache.status().resident_bytes();
        assert!(resident > 0);
        cache.update_memory_pressure(MemoryPressureSnapshot::new(1 << 30, 8 * 1_024, 0));
        assert!(cache.status().resident_bytes() <= 8 * 1_024);
        assert!(cache.status().resident_bytes() < resident);
        for ordinal in 1000..1100_u64 {
            cache.admit(entry(ordinal), HistoricalProofAdmission::Published);
        }
        assert!(cache.status().resident_bytes() <= 8 * 1_024);
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
        // Swap beyond the idle-residue grace band purges all history.
        cache.update_memory_pressure(MemoryPressureSnapshot::new(1 << 30, 1 << 30, 1 << 28));
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
}
