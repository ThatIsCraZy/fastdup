//! Typed Container-envelope verification views in the unified read cache.
use crate::{
    MemoryPressureSnapshot, ReadCacheClass, ReadCacheKey, ReadCacheNamespace,
    shared_cache_reserve_bytes,
};
use fastdup_format::{ContainerId, SealedContainerDescriptor};
use std::sync::Arc;
const HARD_CAPACITY_ENTRIES: usize = 16_777_216;
const MINIMUM_CONTAINER_BYTES: u64 = 32 * 1024 * 1024;
const ACCOUNTED_ENTRY_BYTES: usize = size_of::<SealedContainerDescriptor>() + 512;

#[derive(Debug)]
pub(crate) struct ContainerDescriptorCache {
    cache: ReadCacheNamespace,
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
        Self {
            cache: ReadCacheNamespace::system(ReadCacheClass::ContainerDescriptor),
        }
    }
    pub(crate) fn new_with_snapshot(snapshot: MemoryPressureSnapshot) -> Self {
        let cache = Self {
            cache: ReadCacheNamespace::isolated(ReadCacheClass::ContainerDescriptor, 0),
        };
        cache.apply_memory_pressure(snapshot);
        cache
    }
    fn key(id: ContainerId) -> ReadCacheKey {
        let mut identity = [0; 32];
        identity[..16].copy_from_slice(&id.bytes());
        ReadCacheKey {
            identity,
            ordinal: 0,
        }
    }
    pub(crate) fn get(&self, id: ContainerId) -> Option<SealedContainerDescriptor> {
        self.cache
            .get::<SealedContainerDescriptor>(Self::key(id))
            .map(|value| *value)
    }
    pub(crate) fn insert(&self, id: ContainerId, descriptor: SealedContainerDescriptor) {
        assert_eq!(id, descriptor.container_id(), "ASSERT: descriptor identity");
        self.cache.insert(
            Self::key(id),
            Arc::new(descriptor),
            size_of::<SealedContainerDescriptor>() as u64,
            8192,
        );
    }
    fn apply_memory_pressure(&self, snapshot: MemoryPressureSnapshot) {
        self.cache.update_pressure(
            snapshot,
            (HARD_CAPACITY_ENTRIES * ACCOUNTED_ENTRY_BYTES) as u64,
            shared_cache_reserve_bytes(snapshot.effective_limit_bytes()),
        );
    }
    pub(crate) fn status(&self) -> ContainerDescriptorCacheStatus {
        let stats = self.cache.stats();
        let pressure = self.cache.pressure();
        let target = usize::try_from(self.cache.capacity() / ACCOUNTED_ENTRY_BYTES as u64)
            .unwrap_or(usize::MAX);
        ContainerDescriptorCacheStatus {
            hits: stats.hits,
            misses: stats.misses,
            admissions: stats.admissions,
            evictions: stats.evictions,
            pressure_rejections: stats.rejections,
            allocation_rejections: 0,
            capacity: HARD_CAPACITY_ENTRIES,
            target_entries: target.min(HARD_CAPACITY_ENTRIES),
            entry_count: usize::try_from(stats.entries).unwrap_or(usize::MAX),
            resident_bytes: usize::try_from(stats.resident_bytes).unwrap_or(usize::MAX),
            metadata_bytes: 0,
            hard_coverage_bytes: coverage_bytes(HARD_CAPACITY_ENTRIES),
            target_coverage_bytes: coverage_bytes(target.min(HARD_CAPACITY_ENTRIES)),
            effective_limit_bytes: pressure.effective_limit_bytes(),
            available_bytes: pressure.available_bytes(),
            swap_used_bytes: pressure.swap_used_bytes(),
        }
    }
}
fn coverage_bytes(entries: usize) -> u64 {
    (entries as u64).saturating_mul(MINIMUM_CONTAINER_BYTES)
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
