use crate::read_cache::{
    MemoryPressureSnapshot, SYSTEM_REFRESH_INTERVAL, shared_cache_reserve_bytes,
};
use fastdup_format::{ContainerId, SealedContainerDescriptor};
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

const SHARD_COUNT: usize = 256;
const HARD_CAPACITY_ENTRIES: usize = 16_777_216;
const MINIMUM_CONTAINER_BYTES: u64 = 32 * 1_024 * 1_024;
const ACCOUNTED_ENTRY_BYTES: usize = 160;
const EFFECTIVE_RAM_DIVISOR: u64 = 50;
const _: () = assert!(SHARD_COUNT.is_power_of_two());
const _: () = assert!(HARD_CAPACITY_ENTRIES.is_multiple_of(SHARD_COUNT));
const _: () =
    assert!(HARD_CAPACITY_ENTRIES as u64 * MINIMUM_CONTAINER_BYTES >= 500 * 1_024_u64.pow(4));

#[derive(Debug, Default)]
struct ShardState {
    entries: HashMap<[u8; 16], SealedContainerDescriptor>,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct DescriptorShard {
    state: Mutex<ShardState>,
}

/// Dynamically resident cache of verified immutable Container envelopes.
///
/// The hard addressable capacity covers at least 500 TiB using the current
/// minimum 32-MiB Container size. Shards allocate only as descriptors arrive.
/// A pressure gate serializes rare target changes with cold admissions; hot
/// lookups touch one shard and never take a process-global lock.
#[derive(Debug)]
pub(crate) struct ContainerDescriptorCache {
    shards: Box<[DescriptorShard]>,
    pressure_gate: RwLock<()>,
    target_entries: AtomicUsize,
    entry_count: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    admissions: AtomicU64,
    evictions: AtomicU64,
    pressure_rejections: AtomicU64,
    allocation_rejections: AtomicU64,
    effective_limit_bytes: AtomicU64,
    available_bytes: AtomicU64,
    swap_used_bytes: AtomicU64,
    automatic_pressure: bool,
    started: Instant,
    last_refresh_millis: AtomicU64,
}

/// Process-local telemetry for verified Container-envelope reuse.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContainerDescriptorCacheStatus {
    hits: u64,
    misses: u64,
    admissions: u64,
    evictions: u64,
    pressure_rejections: u64,
    allocation_rejections: u64,
    capacity: usize,
    target_entries: usize,
    entry_count: usize,
    resident_bytes: usize,
    metadata_bytes: usize,
    hard_coverage_bytes: u64,
    target_coverage_bytes: u64,
    effective_limit_bytes: u64,
    available_bytes: u64,
    swap_used_bytes: u64,
}

impl ContainerDescriptorCacheStatus {
    #[must_use]
    pub const fn hits(self) -> u64 {
        self.hits
    }

    #[must_use]
    pub const fn misses(self) -> u64 {
        self.misses
    }

    /// Returns hits divided by all probes on a 0-10,000 scale.
    ///
    /// # Panics
    ///
    /// Panics only if integer arithmetic produces a value above 10,000,
    /// violating the internal ratio bound.
    #[must_use]
    pub fn hit_rate_basis_points(self) -> u64 {
        let probes = u128::from(self.hits) + u128::from(self.misses);
        if probes == 0 {
            return 0;
        }
        u64::try_from(u128::from(self.hits) * 10_000 / probes)
            .expect("ASSERT: cache hit rate is at most 10,000 basis points")
    }

    #[must_use]
    pub const fn admissions(self) -> u64 {
        self.admissions
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
    pub const fn allocation_rejections(self) -> u64 {
        self.allocation_rejections
    }

    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    #[must_use]
    pub const fn target_entries(self) -> usize {
        self.target_entries
    }

    #[must_use]
    pub const fn entry_count(self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub const fn resident_bytes(self) -> usize {
        self.resident_bytes
    }

    #[must_use]
    pub const fn metadata_bytes(self) -> usize {
        self.metadata_bytes
    }

    #[must_use]
    pub const fn hard_coverage_bytes(self) -> u64 {
        self.hard_coverage_bytes
    }

    #[must_use]
    pub const fn target_coverage_bytes(self) -> u64 {
        self.target_coverage_bytes
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
}

impl ContainerDescriptorCache {
    pub(crate) fn new_system() -> Self {
        let snapshot = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        Self::build(snapshot, true)
    }

    pub(crate) fn new_with_snapshot(snapshot: MemoryPressureSnapshot) -> Self {
        Self::build(snapshot, false)
    }

    fn build(snapshot: MemoryPressureSnapshot, automatic_pressure: bool) -> Self {
        let mut shards = Vec::new();
        if shards.try_reserve_exact(SHARD_COUNT).is_ok() {
            shards.resize_with(SHARD_COUNT, DescriptorShard::default);
        }
        let cache = Self {
            shards: shards.into_boxed_slice(),
            pressure_gate: RwLock::new(()),
            target_entries: AtomicUsize::new(0),
            entry_count: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            pressure_rejections: AtomicU64::new(0),
            allocation_rejections: AtomicU64::new(0),
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

    pub(crate) fn get(&self, container_id: ContainerId) -> Option<SealedContainerDescriptor> {
        self.maybe_refresh_pressure();
        let Some(shard) = self.shards.get(shard_index(container_id)) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let state = shard
            .state
            .lock()
            .expect("ASSERT: Container descriptor cache shard lock poisoned");
        let descriptor = state.entries.get(&container_id.bytes()).copied();
        if let Some(descriptor) = descriptor {
            assert_eq!(
                descriptor.container_id(),
                container_id,
                "ASSERT: cached Container descriptor changed identity"
            );
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        descriptor
    }

    pub(crate) fn insert(&self, container_id: ContainerId, descriptor: SealedContainerDescriptor) {
        assert_eq!(
            descriptor.container_id(),
            container_id,
            "ASSERT: a cached Container descriptor must retain its canonical identity"
        );
        self.maybe_refresh_pressure();
        let _pressure = self
            .pressure_gate
            .read()
            .expect("ASSERT: Container descriptor pressure gate poisoned");
        let target = self.target_entries.load(Ordering::Acquire);
        let index = shard_index(container_id);
        let Some(shard) = self.shards.get(index) else {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let shard_target = target_for_shard(target, index);
        if shard_target == 0 {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: Container descriptor cache shard lock poisoned");
        let key = container_id.bytes();
        if let Some(existing) = state.entries.get(&key) {
            assert_eq!(
                *existing, descriptor,
                "ASSERT: immutable Container identity cannot acquire a different envelope"
            );
            return;
        }
        if state.entries.len() >= shard_target {
            let victim = *state
                .entries
                .keys()
                .next()
                .expect("ASSERT: a full descriptor-cache shard is nonempty");
            assert!(
                state.entries.remove(&victim).is_some(),
                "ASSERT: selected descriptor-cache victim must exist"
            );
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.entry_count.fetch_sub(1, Ordering::Release);
        }
        if state.entries.try_reserve(1).is_err() {
            self.allocation_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        assert!(
            state.entries.insert(key, descriptor).is_none(),
            "ASSERT: a new descriptor-cache key cannot replace an entry"
        );
        let resident = self.entry_count.fetch_add(1, Ordering::Release) + 1;
        self.admissions.fetch_add(1, Ordering::Relaxed);
        assert!(
            resident <= target,
            "ASSERT: descriptor cache exceeded its distributed target"
        );
    }

    pub(crate) fn status(&self) -> ContainerDescriptorCacheStatus {
        self.maybe_refresh_pressure();
        let entries = self.entry_count.load(Ordering::Acquire);
        let target = self.target_entries.load(Ordering::Acquire);
        ContainerDescriptorCacheStatus {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            pressure_rejections: self.pressure_rejections.load(Ordering::Relaxed),
            allocation_rejections: self.allocation_rejections.load(Ordering::Relaxed),
            capacity: self.hard_capacity_entries(),
            target_entries: target,
            entry_count: entries,
            resident_bytes: entries.saturating_mul(ACCOUNTED_ENTRY_BYTES),
            metadata_bytes: self
                .shards
                .len()
                .saturating_mul(size_of::<DescriptorShard>()),
            hard_coverage_bytes: coverage_bytes(self.hard_capacity_entries()),
            target_coverage_bytes: coverage_bytes(target),
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
        let interval = u64::try_from(SYSTEM_REFRESH_INTERVAL.as_millis())
            .expect("ASSERT: memory refresh interval fits u64 milliseconds");
        let previous = self.last_refresh_millis.load(Ordering::Relaxed);
        if elapsed.saturating_sub(previous) < interval
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
            .expect("ASSERT: Container descriptor pressure gate poisoned");
        let reserve = shared_cache_reserve_bytes(snapshot.effective_limit_bytes());
        let headroom = snapshot.available_bytes().saturating_sub(reserve);
        let fraction_budget = snapshot.effective_limit_bytes() / EFFECTIVE_RAM_DIVISOR;
        let budget = if snapshot.swap_used_bytes() == 0 {
            headroom.min(fraction_budget)
        } else {
            0
        };
        let target = usize::try_from(budget / ACCOUNTED_ENTRY_BYTES as u64)
            .unwrap_or(usize::MAX)
            .min(self.hard_capacity_entries());
        self.effective_limit_bytes
            .store(snapshot.effective_limit_bytes(), Ordering::Release);
        self.available_bytes
            .store(snapshot.available_bytes(), Ordering::Release);
        self.swap_used_bytes
            .store(snapshot.swap_used_bytes(), Ordering::Release);
        self.target_entries.store(target, Ordering::Release);
        if self.entry_count.load(Ordering::Acquire) > target {
            self.clear_locked();
        }
    }

    fn clear_locked(&self) {
        let mut removed = 0_usize;
        for shard in &self.shards {
            let mut state = shard
                .state
                .lock()
                .expect("ASSERT: Container descriptor cache shard lock poisoned");
            removed = removed
                .checked_add(state.entries.len())
                .expect("ASSERT: descriptor cache entry count cannot overflow");
            state.entries = HashMap::new();
        }
        let accounted = self.entry_count.swap(0, Ordering::AcqRel);
        assert_eq!(
            removed, accounted,
            "ASSERT: descriptor cache entry accounting disagreed with its shards"
        );
        self.evictions.fetch_add(
            u64::try_from(removed).expect("ASSERT: descriptor entry count fits u64"),
            Ordering::Relaxed,
        );
    }

    fn hard_capacity_entries(&self) -> usize {
        if self.shards.len() == SHARD_COUNT {
            HARD_CAPACITY_ENTRIES
        } else {
            0
        }
    }
}

fn target_for_shard(total: usize, shard: usize) -> usize {
    total / SHARD_COUNT + usize::from(shard < total % SHARD_COUNT)
}

fn coverage_bytes(entries: usize) -> u64 {
    u64::try_from(entries)
        .unwrap_or(u64::MAX)
        .saturating_mul(MINIMUM_CONTAINER_BYTES)
}

fn shard_index(container_id: ContainerId) -> usize {
    descriptor_hash(container_id) & (SHARD_COUNT - 1)
}

fn descriptor_hash(container_id: ContainerId) -> usize {
    let bytes = container_id.bytes();
    let low = u64::from_le_bytes(bytes[..8].try_into().expect("ASSERT: exact ID half"));
    let high = u64::from_le_bytes(bytes[8..].try_into().expect("ASSERT: exact ID half"));
    let mut mixed = low ^ high.rotate_left(23);
    mixed ^= mixed >> 30;
    mixed = mixed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed ^= mixed >> 27;
    mixed = mixed.wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;
    usize::try_from(mixed).unwrap_or_else(|_| {
        usize::try_from((mixed ^ (mixed >> 32)) & u64::from(u32::MAX))
            .expect("ASSERT: folded descriptor hash fits usize")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{HEADER_BYTES, SealedContainer};

    fn descriptor(id: ContainerId) -> SealedContainerDescriptor {
        let sealed =
            SealedContainer::encode(id, 7, &[b"cache fixture"]).expect("encode descriptor fixture");
        let footer_offset = sealed.len() - 4_096;
        SealedContainerDescriptor::decode(
            &sealed[..HEADER_BYTES],
            &sealed[footer_offset..],
            u64::try_from(sealed.len()).expect("fixture length fits u64"),
        )
        .expect("decode descriptor")
    }

    #[test]
    fn hard_capacity_covers_five_hundred_tib_without_eager_entries() {
        let gib = 1_024_u64.pow(3);
        let cache = ContainerDescriptorCache::new_with_snapshot(MemoryPressureSnapshot::new(
            128 * gib,
            96 * gib,
            0,
        ));
        let status = cache.status();
        assert_eq!(status.capacity(), 16_777_216);
        assert_eq!(status.target_entries(), status.capacity());
        assert!(status.hard_coverage_bytes() >= 500 * 1_024_u64.pow(4));
        assert!(status.target_coverage_bytes() >= 500 * 1_024_u64.pow(4));
        assert_eq!(status.entry_count(), 0);
        assert_eq!(status.resident_bytes(), 0);
        assert!(status.metadata_bytes() < 1024 * 1024);
        assert!(
            size_of::<([u8; 16], SealedContainerDescriptor)>() < ACCOUNTED_ENTRY_BYTES,
            "per-entry accounting must include HashMap load/control overhead"
        );
    }

    #[test]
    fn insert_and_lookup_preserve_exact_identity() {
        let gib = 1_024_u64.pow(3);
        let cache = ContainerDescriptorCache::new_with_snapshot(MemoryPressureSnapshot::new(
            128 * gib,
            96 * gib,
            0,
        ));
        let id = ContainerId::new([0x55; 16]).expect("nonzero ID");
        let descriptor = descriptor(id);

        assert_eq!(cache.get(id), None);
        cache.insert(id, descriptor);
        assert_eq!(cache.get(id), Some(descriptor));
        let status = cache.status();
        assert_eq!(status.misses(), 1);
        assert_eq!(status.hits(), 1);
        assert_eq!(status.hit_rate_basis_points(), 5_000);
        assert_eq!(status.admissions(), 1);
        assert_eq!(status.entry_count(), 1);
        assert_eq!(status.resident_bytes(), ACCOUNTED_ENTRY_BYTES);
    }

    #[test]
    fn swap_pressure_refuses_descriptor_admission() {
        let gib = 1_024_u64.pow(3);
        let cache = ContainerDescriptorCache::new_with_snapshot(MemoryPressureSnapshot::new(
            128 * gib,
            96 * gib,
            1,
        ));
        let id = ContainerId::new([0x56; 16]).expect("nonzero ID");
        cache.insert(id, descriptor(id));

        assert_eq!(cache.get(id), None);
        let status = cache.status();
        assert_eq!(status.target_entries(), 0);
        assert_eq!(status.entry_count(), 0);
        assert_eq!(status.pressure_rejections(), 1);
        assert_eq!(status.swap_used_bytes(), 1);
    }

    #[test]
    fn later_swap_pressure_releases_resident_shard_storage() {
        let gib = 1_024_u64.pow(3);
        let cache = ContainerDescriptorCache::new_with_snapshot(MemoryPressureSnapshot::new(
            128 * gib,
            96 * gib,
            0,
        ));
        let id = ContainerId::new([0x57; 16]).expect("nonzero ID");
        cache.insert(id, descriptor(id));
        assert_eq!(cache.status().entry_count(), 1);

        cache.apply_memory_pressure(MemoryPressureSnapshot::new(128 * gib, 96 * gib, 1));

        assert_eq!(cache.get(id), None);
        let status = cache.status();
        assert_eq!(status.target_entries(), 0);
        assert_eq!(status.entry_count(), 0);
        assert_eq!(status.resident_bytes(), 0);
        assert_eq!(status.evictions(), 1);
    }
}
