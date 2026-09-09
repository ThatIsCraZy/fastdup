//! Repository-local verified Manifest nodes. Recovery/scrub never consult this cache.
use crate::manifest_tree::{DecodedManifestNode, ManifestTreeError, decode_manifest_node};
use crate::{CacheFallback, CacheObservation, CachePool, MemoryPressureSnapshot};
use fastdup_format::MetadataObjectId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SHARDS: usize = 64;
const ENTRY_OVERHEAD: u64 = 512;

#[derive(Debug)]
struct Entry {
    node: Arc<DecodedManifestNode>,
    charge: u64,
    encoded_bytes: u64,
}
#[derive(Debug, Default)]
struct Entries {
    map: HashMap<[u8; 32], Entry>,
    fifo: VecDeque<[u8; 32]>,
}
#[derive(Debug, Default)]
#[repr(align(64))]
struct Shard(Mutex<Entries>);
#[derive(Debug, Default)]
struct Admission {
    resident: u64,
    target: u64,
    cursor: usize,
}

/// Only immutable decoded objects live here, not root pins or liveness proofs.
/// Each `GenerationRepository` owns a distinct cache; clones share that instance.
#[derive(Debug)]
pub(crate) struct ManifestNodeCache {
    shards: Box<[Shard]>,
    admission: Mutex<Admission>,
    hits: AtomicU64,
    misses: AtomicU64,
    hit_bytes: AtomicU64,
    evictions: AtomicU64,
    started: Instant,
    next_refresh: AtomicU64,
    // Last field: resident cache ownership is dropped before its lease.
    pool: Option<CachePool>,
}

impl ManifestNodeCache {
    pub(crate) fn system() -> Self {
        let mut cache = Self::new();
        cache.pool = Some(CachePool::system(
            "manifestNodes",
            CacheFallback::Metadata,
            cache.fixed_bytes(),
            u64::MAX,
        ));
        cache.refresh();
        cache
    }

    fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Shard::default()).collect(),
            admission: Mutex::new(Admission::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            hit_bytes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            started: Instant::now(),
            next_refresh: AtomicU64::new(0),
            pool: None,
        }
    }
    fn fixed_bytes(&self) -> u64 {
        (size_of::<Self>() + self.shards.len() * size_of::<Shard>()) as u64
    }
    fn shard(id: MetadataObjectId) -> usize {
        usize::from(id.bytes()[0]) & (SHARDS - 1)
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
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        let pressure = MemoryPressureSnapshot::read_system()
            .unwrap_or_else(|_| MemoryPressureSnapshot::new(0, 0, 1));
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: Manifest admission lock poisoned");
        let fixed = self.fixed_bytes();
        let target = pool.target(
            pressure,
            CacheObservation {
                hits: self.hits.load(Ordering::Relaxed),
                misses: self.misses.load(Ordering::Relaxed),
                evictions: self.evictions.load(Ordering::Relaxed),
                hit_bytes: self.hit_bytes.load(Ordering::Relaxed),
                resident_bytes: fixed + admission.resident,
            },
        );
        admission.target = target.saturating_sub(fixed);
        self.trim(&mut admission, 0);
        pool.applied(target, fixed + admission.resident);
    }

    /// Returns validated immutable bytes on a miss, even when admission is denied.
    /// No cache or controller lock is held during storage I/O/verification.
    pub(crate) fn read<F>(
        &self,
        id: MetadataObjectId,
        read: F,
    ) -> Result<Arc<DecodedManifestNode>, ManifestTreeError>
    where
        F: FnOnce() -> Result<Vec<u8>, ManifestTreeError>,
    {
        self.refresh();
        {
            let shard = self.shards[Self::shard(id)]
                .0
                .lock()
                .expect("ASSERT: Manifest shard lock poisoned");
            if let Some(entry) = shard.map.get(&id.bytes()) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                self.hit_bytes
                    .fetch_add(entry.encoded_bytes, Ordering::Relaxed);
                return Ok(Arc::clone(&entry.node));
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let encoded = read()?;
        if MetadataObjectId::from_encoded(&encoded)? != id {
            return Err(ManifestTreeError::IdentityMismatch(id));
        }
        let encoded_bytes = encoded.len() as u64;
        let mut bytes = Some(encoded);
        let node = Arc::new(decode_manifest_node(id, &mut |_| {
            Ok(bytes
                .take()
                .expect("ASSERT: decode reads exactly one object"))
        })?);
        // Leaf/inner decoders reserve their exact element count. Twice the
        // encoded length bounds decoded elements, Arc/header and allocator
        // padding; per-entry charge covers sparse map/FIFO capacity growth.
        let charge = encoded_bytes
            .saturating_mul(2)
            .saturating_add(ENTRY_OVERHEAD);
        self.insert(id, Arc::clone(&node), charge, encoded_bytes);
        Ok(node)
    }

    fn insert(
        &self,
        id: MetadataObjectId,
        node: Arc<DecodedManifestNode>,
        charge: u64,
        encoded_bytes: u64,
    ) {
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: Manifest admission lock poisoned");
        if charge > admission.target {
            return;
        }
        {
            let shard = self.shards[Self::shard(id)]
                .0
                .lock()
                .expect("ASSERT: Manifest shard lock poisoned");
            if shard.map.contains_key(&id.bytes()) {
                return;
            }
        }
        self.trim(&mut admission, charge);
        let mut shard = self.shards[Self::shard(id)]
            .0
            .lock()
            .expect("ASSERT: Manifest shard lock poisoned");
        if shard.map.try_reserve(1).is_err() || shard.fifo.try_reserve(1).is_err() {
            shard.map.shrink_to_fit();
            shard.fifo.shrink_to_fit();
            return;
        }
        shard.map.insert(
            id.bytes(),
            Entry {
                node,
                charge,
                encoded_bytes,
            },
        );
        shard.fifo.push_back(id.bytes());
        admission.resident += charge;
        assert!(
            admission.resident <= admission.target,
            "ASSERT: Manifest residency fits lease"
        );
    }
    fn trim(&self, admission: &mut Admission, incoming: u64) {
        while admission.resident > admission.target.saturating_sub(incoming) {
            let index = admission.cursor % SHARDS;
            admission.cursor = admission.cursor.wrapping_add(1);
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: Manifest shard lock poisoned");
            if let Some(key) = shard.fifo.pop_front() {
                let entry = shard
                    .map
                    .remove(&key)
                    .expect("ASSERT: Manifest FIFO entry exists");
                admission.resident -= entry.charge;
                self.evictions.fetch_add(1, Ordering::Relaxed);
                // Return map/FIFO capacity together with ownership. Held Arc
                // views remain working memory, never invalidated by eviction.
                shard.map.shrink_to_fit();
                shard.fifo.shrink_to_fit();
            }
        }
    }

    #[cfg(test)]
    fn limited(target: u64) -> Self {
        let cache = Self::new();
        cache.admission.lock().unwrap().target = target;
        cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest_tree::{
        allocated_bytes_in_manifest_tree_range_decoded, encode_manifest_tree,
        read_manifest_tree_range_decoded,
    };
    use fastdup_format::ManifestExtent;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    fn tree() -> (MetadataObjectId, u64, BTreeMap<MetadataObjectId, Vec<u8>>) {
        let extents = (0..4096)
            .map(|i| ManifestExtent::Fill {
                logical_length: 65536,
                value: (i % 251) as u8,
            })
            .collect::<Vec<_>>();
        let size = 4096 * 65536;
        let tree = encode_manifest_tree(size, &extents).unwrap();
        (tree.root(), size, tree.objects().iter().cloned().collect())
    }
    #[test]
    fn adjacent_clone_ranges_share_verified_decoded_nodes_and_preserve_validation() {
        let (root, size, objects) = tree();
        assert!(
            objects.len() > 2,
            "must exercise inner nodes and multiple leaves"
        );
        let cache = ManifestNodeCache::limited(8 * 1024 * 1024);
        let reads = AtomicUsize::new(0);
        let mut load = |id| {
            cache.read(id, || {
                reads.fetch_add(1, Ordering::Relaxed);
                Ok(objects[&id].clone())
            })
        };
        let first = read_manifest_tree_range_decoded(root, size, 65536, 4 * 1024 * 1024, &mut load)
            .unwrap();
        let cold = reads.load(Ordering::Relaxed);
        assert!(cold >= 2);
        assert_eq!(
            allocated_bytes_in_manifest_tree_range_decoded(
                root,
                size,
                65536,
                4 * 1024 * 1024,
                &mut load
            )
            .unwrap(),
            4 * 1024 * 1024
        );
        let again = read_manifest_tree_range_decoded(root, size, 65536, 4 * 1024 * 1024, &mut load)
            .unwrap();
        assert_eq!(first, again);
        assert_eq!(
            reads.load(Ordering::Relaxed),
            cold,
            "warm clone must perform zero metadata reads"
        );
        assert!(
            read_manifest_tree_range_decoded(root, size + 1, 0, 1, &mut load).is_err(),
            "a cache hit still checks the caller's expected tree length"
        );
        let held = load(root).unwrap();
        {
            let mut state = cache.admission.lock().unwrap();
            state.target = 0;
            cache.trim(&mut state, 0);
            assert_eq!(state.resident, 0);
        }
        assert!(matches!(held.as_ref(), DecodedManifestNode::Inner(_)));
        let mut corrupt = objects[&root].clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(
            cache.read(root, || Ok(corrupt)).is_err(),
            "evicted nodes must be independently verified again"
        );
        let isolated = ManifestNodeCache::limited(8 * 1024 * 1024);
        assert!(
            isolated
                .read(root, || Err(ManifestTreeError::InvalidTree))
                .is_err()
        );
    }
    #[test]
    fn concurrent_misses_charge_one_entry_and_pressure_keeps_owned_views_valid() {
        let (root, _, objects) = tree();
        let cache = ManifestNodeCache::limited(8 * 1024 * 1024);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        cache.read(root, || Ok(objects[&root].clone())).unwrap();
                    }
                });
            }
        });
        let shard = cache.shards[ManifestNodeCache::shard(root)]
            .0
            .lock()
            .unwrap();
        assert_eq!(shard.map.len(), 1);
        assert_eq!(
            shard.map[&root.bytes()].charge,
            cache.admission.lock().unwrap().resident
        );
    }
    #[test]
    #[ignore = "A/B timing benchmark; run explicitly in release mode with --nocapture"]
    fn benchmark_manifest_clone_ranges() {
        let (root, size, objects) = tree();
        let mut results = Vec::new();
        for budget in [0, 8 * 1024 * 1024] {
            let cache = ManifestNodeCache::limited(budget);
            let reads = AtomicUsize::new(0);
            let start = Instant::now();
            for i in 0..2000 {
                let offset = (i % 48) * 4 * 1024 * 1024;
                let result =
                    read_manifest_tree_range_decoded(root, size, offset, 4 * 1024 * 1024, |id| {
                        cache.read(id, || {
                            reads.fetch_add(1, Ordering::Relaxed);
                            Ok(objects[&id].clone())
                        })
                    })
                    .unwrap();
                std::hint::black_box(result);
            }
            results.push((start.elapsed(), reads.load(Ordering::Relaxed)));
        }
        eprintln!(
            "manifest_range_ab uncached={:?} cached={:?}",
            results[0], results[1]
        );
        assert!(results[1].1 < results[0].1 / 100);
    }
}
