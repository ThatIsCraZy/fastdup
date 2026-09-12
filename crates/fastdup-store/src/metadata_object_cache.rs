//! Repository-local content-identified Metadata bytes; never durable or liveness authority.
use crate::manifest_tree::ManifestTreeError;
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
    node: Arc<Vec<u8>>,
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

/// Only immutable content-identified bytes live here, not root pins or liveness proofs.
/// Each `GenerationRepository` owns a distinct cache; clones share that instance.
#[derive(Debug)]
pub(crate) struct MetadataObjectCache {
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

impl MetadataObjectCache {
    pub(crate) fn system() -> Self {
        let mut cache = Self::new();
        cache.pool = Some(CachePool::system(
            "metadataObjects",
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
            .expect("ASSERT: Metadata admission lock poisoned");
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

    /// Returns content-identified bytes; callers still check graph structure and relationships.
    /// No cache or controller lock is held during storage I/O/verification.
    pub(crate) fn read<F>(
        &self,
        id: MetadataObjectId,
        read: F,
    ) -> Result<Arc<Vec<u8>>, ManifestTreeError>
    where
        F: FnOnce() -> Result<Vec<u8>, ManifestTreeError>,
    {
        if BYPASS.with(std::cell::Cell::get) {
            let encoded = read()?;
            if MetadataObjectId::from_encoded(&encoded)? != id {
                return Err(ManifestTreeError::IdentityMismatch(id));
            }
            return Ok(Arc::new(encoded));
        }
        self.refresh();
        {
            let shard = self.shards[Self::shard(id)]
                .0
                .lock()
                .expect("ASSERT: Metadata shard lock poisoned");
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
        // Capacity, not length: a backend may return a generously allocated Vec.
        let charge = (encoded.capacity() as u64).saturating_add(ENTRY_OVERHEAD);
        let node = Arc::new(encoded);
        self.insert(id, Arc::clone(&node), charge, encoded_bytes);
        Ok(node)
    }

    pub(crate) fn invalidate(&self, id: MetadataObjectId) {
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: Metadata admission lock poisoned");
        let mut shard = self.shards[Self::shard(id)]
            .0
            .lock()
            .expect("ASSERT: Metadata shard lock poisoned");
        if let Some(entry) = shard.map.remove(&id.bytes()) {
            shard.fifo.retain(|key| *key != id.bytes());
            admission.resident -= entry.charge;
            self.evictions.fetch_add(1, Ordering::Relaxed);
            shard.map.shrink_to_fit();
            shard.fifo.shrink_to_fit();
        }
    }

    fn insert(&self, id: MetadataObjectId, node: Arc<Vec<u8>>, charge: u64, encoded_bytes: u64) {
        let mut admission = self
            .admission
            .lock()
            .expect("ASSERT: Metadata admission lock poisoned");
        if charge > admission.target {
            return;
        }
        {
            let shard = self.shards[Self::shard(id)]
                .0
                .lock()
                .expect("ASSERT: Metadata shard lock poisoned");
            if shard.map.contains_key(&id.bytes()) {
                return;
            }
        }
        self.trim(&mut admission, charge);
        let mut shard = self.shards[Self::shard(id)]
            .0
            .lock()
            .expect("ASSERT: Metadata shard lock poisoned");
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
            "ASSERT: Metadata residency fits lease"
        );
    }
    fn trim(&self, admission: &mut Admission, incoming: u64) {
        while admission.resident > admission.target.saturating_sub(incoming) {
            let index = admission.cursor % SHARDS;
            admission.cursor = admission.cursor.wrapping_add(1);
            let mut shard = self.shards[index]
                .0
                .lock()
                .expect("ASSERT: Metadata shard lock poisoned");
            if let Some(key) = shard.fifo.pop_front() {
                let entry = shard
                    .map
                    .remove(&key)
                    .expect("ASSERT: Metadata FIFO entry exists");
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
    pub(crate) fn limited(target: u64) -> Self {
        let cache = Self::new();
        cache.admission.lock().unwrap().target = target;
        cache
    }
}

thread_local! {
    static BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
/// Independent recovery, scrub and deletion proofs cannot consult live caches.
/// Thread-local because all repository graph walks are synchronous. !Send.
pub(crate) struct IndependentRead(bool, std::marker::PhantomData<std::rc::Rc<()>>);
impl IndependentRead {
    pub(crate) fn enter() -> Self {
        Self(BYPASS.with(|v| v.replace(true)), std::marker::PhantomData)
    }
}
impl Drop for IndependentRead {
    fn drop(&mut self) {
        BYPASS.with(|v| v.set(self.0));
    }
}

#[cfg(test)]
#[path = "metadata_object_cache_tests.rs"]
mod tests;
