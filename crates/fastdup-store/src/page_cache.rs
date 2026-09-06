//! Lazy page storage: one shard on hits, no process-global replacement list.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) const PAGE_CACHE_SHARDS: usize = 256;
pub(crate) const ACCOUNTED_PAGE_BYTES: u64 = 4096 + 512;
type Key = ([u8; 32], usize);

struct Pages<P> {
    entries: HashMap<Key, Arc<P>>,
    fifo: VecDeque<Key>,
}
impl<P> Default for Pages<P> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            fifo: VecDeque::new(),
        }
    }
}
#[repr(align(64))]
struct Shard<P>(Mutex<Pages<P>>);
pub(crate) struct LazyPageCache<P> {
    shards: Box<[Shard<P>]>,
    cursor: AtomicUsize,
}

impl<P> LazyPageCache<P> {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..PAGE_CACHE_SHARDS)
                .map(|_| Shard(Mutex::new(Pages::default())))
                .collect(),
            cursor: AtomicUsize::new(0),
        }
    }
    pub(crate) fn metadata_bytes(&self) -> u64 {
        (self.shards.len() * std::mem::size_of::<Shard<P>>()) as u64
    }
    fn ordinal(hash: [u8; 32], page: usize) -> usize {
        let seed = usize::from_le_bytes(hash[..8].try_into().expect("ASSERT: x86-64 hash width"));
        (seed ^ page.wrapping_mul(0x9e37_79b9_7f4a_7c15)) & (PAGE_CACHE_SHARDS - 1)
    }
    pub(crate) fn get(&self, hash: [u8; 32], page: usize) -> Option<Arc<P>> {
        self.shards[Self::ordinal(hash, page)]
            .0
            .lock()
            .expect("ASSERT: page shard lock poisoned")
            .entries
            .get(&(hash, page))
            .cloned()
    }
    /// Returns (resident delta, evictions), or None for a rejected admission.
    /// The caller serializes admissions and target changes for its pool.
    pub(crate) fn insert(
        &self,
        hash: [u8; 32],
        ordinal: usize,
        page: Arc<P>,
        target: u64,
    ) -> Option<(i64, u64)> {
        let index = Self::ordinal(hash, ordinal);
        let quota = target / PAGE_CACHE_SHARDS as u64
            + u64::from((index as u64) < target % PAGE_CACHE_SHARDS as u64);
        if quota == 0 {
            return None;
        }
        let mut shard = self.shards[index]
            .0
            .lock()
            .expect("ASSERT: page shard lock poisoned");
        let key = (hash, ordinal);
        if shard.entries.contains_key(&key) {
            return Some((0, 0));
        }
        if shard.entries.try_reserve(1).is_err() || shard.fifo.try_reserve(1).is_err() {
            return None;
        }
        let mut removed = 0;
        while shard.entries.len() as u64 >= quota {
            let victim = shard
                .fifo
                .pop_front()
                .expect("ASSERT: page FIFO covers resident entries");
            assert!(
                shard.entries.remove(&victim).is_some(),
                "ASSERT: page victim exists"
            );
            removed += 1;
        }
        shard.entries.insert(key, page);
        shard.fifo.push_back(key);
        Some((
            1 - i64::try_from(removed).expect("ASSERT: allocated page count fits i64"),
            removed,
        ))
    }
    pub(crate) fn evict_one(&self) -> bool {
        for _ in 0..PAGE_CACHE_SHARDS {
            let index = self.cursor.fetch_add(1, Ordering::Relaxed) % PAGE_CACHE_SHARDS;
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: page shard lock poisoned");
            if let Some(key) = shard.fifo.pop_front() {
                assert!(
                    shard.entries.remove(&key).is_some(),
                    "ASSERT: page victim exists"
                );
                return true;
            }
        }
        false
    }
    /// Trims each shard and returns all vacated allocation capacity to the allocator.
    pub(crate) fn trim(&self, target: u64) -> u64 {
        let mut removed = 0;
        for (index, slot) in self.shards.iter().enumerate() {
            let quota = target / PAGE_CACHE_SHARDS as u64
                + u64::from((index as u64) < target % PAGE_CACHE_SHARDS as u64);
            let mut shard = slot.0.lock().expect("ASSERT: page shard lock poisoned");
            while shard.entries.len() as u64 > quota {
                let victim = shard
                    .fifo
                    .pop_front()
                    .expect("ASSERT: page FIFO covers resident entries");
                assert!(
                    shard.entries.remove(&victim).is_some(),
                    "ASSERT: page victim exists"
                );
                removed += 1;
            }
            shard.entries.shrink_to_fit();
            shard.fifo.shrink_to_fit();
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lazy_pages_grow_beyond_old_geometry_and_survive_shrink_as_owned_views() {
        let cache = LazyPageCache::new();
        let id = [7; 32];
        for page in 0..4096 {
            assert_eq!(cache.insert(id, page, Arc::new(page), 4096), Some((1, 0)));
        }
        for page in 0..4096 {
            assert_eq!(cache.get(id, page).as_deref(), Some(&page));
        }
        let held = cache.get(id, 0).unwrap();
        assert_eq!(cache.trim(256), 3840);
        assert_eq!(
            *held, 0,
            "eviction cannot invalidate a live verified reader"
        );
        assert_eq!(cache.trim(0), 256);
        for page in 0..4096 {
            assert!(cache.get(id, page).is_none());
        }
    }
    #[test]
    fn colliding_workloads_replace_and_reuse_the_new_pages() {
        let cache = LazyPageCache::new();
        let id = [3; 32];
        for generation in 0..3 {
            for i in 0..1024 {
                let page = generation * 1024 + i;
                cache.insert(id, page, Arc::new(page), 1024).unwrap();
            }
            for i in 0..1024 {
                let page = generation * 1024 + i;
                assert_eq!(cache.get(id, page).as_deref(), Some(&page));
            }
        }
        assert_eq!(cache.trim(0), 1024);
    }
}
