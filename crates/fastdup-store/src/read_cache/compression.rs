use super::{
    CacheBackingCharge, CacheKey, ChunkId, MemoryPressureSnapshot, VerifiedChunkPayload,
    VerifiedReadCache, cache_hash, shared_cache_reserve_bytes,
};
use fastdup_format::{CompressedVerifiedChunkPayload, MAX_LOGICAL_CHUNK_BYTES};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::{Condvar, TryLockError};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(super) enum CachedPayload {
    Decoded(VerifiedChunkPayload),
    Compressed(CompressedVerifiedChunkPayload),
}

impl CachedPayload {
    pub(super) fn chunk_id(&self) -> ChunkId {
        match self {
            Self::Decoded(value) => value.chunk_id(),
            Self::Compressed(value) => value.chunk_id(),
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Decoded(value) => value.len(),
            Self::Compressed(value) => value.logical_length(),
        }
    }
}

#[derive(Debug, Default)]
struct Utility {
    decode_ns: u64,
    extra_bytes: u64,
}

/// One shared workspace bound, independent of resident-cache representation.
/// Optional compression never waits; mandatory RAM decompression may wait for
/// another codec operation, without holding a shard or admission lock.
pub(super) struct Compression {
    used: Mutex<usize>,
    ready: Condvar,
    maximum: usize,
    utility: Mutex<Utility>,
    pub(super) epoch: AtomicU64,
    pub(super) resident: AtomicUsize,
    pub(super) logical: AtomicUsize,
    pub(super) attempts: AtomicU64,
    pub(super) admitted: AtomicU64,
    pub(super) compress_ns: AtomicU64,
    pub(super) hits: AtomicU64,
    pub(super) decodes: AtomicU64,
    pub(super) decode_ns: AtomicU64,
    pub(super) promotions: AtomicU64,
    pub(super) demotions: AtomicU64,
    pub(super) failures: AtomicU64,
    pub(super) bypasses: AtomicU64,
    pub(super) peak_working: AtomicUsize,
    #[cfg(test)]
    pub(super) enabled: std::sync::atomic::AtomicBool,
}

// LZ4's bounded scratch plus source/output and allocator margin. Returned
// immutable reader views become ordinary working memory, as with cold reads.
const CODEC_WORK: usize = 2 * MAX_LOGICAL_CHUNK_BYTES + 128 * 1024;

impl Compression {
    pub(super) fn new(snapshot: MemoryPressureSnapshot) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let reserve = usize::try_from(shared_cache_reserve_bytes(snapshot.effective_limit_bytes()))
            .unwrap_or(usize::MAX);
        Self {
            used: Mutex::new(0),
            ready: Condvar::new(),
            maximum: cpus
                .saturating_mul(CODEC_WORK + MAX_LOGICAL_CHUNK_BYTES)
                .min(reserve)
                .max(CODEC_WORK + MAX_LOGICAL_CHUNK_BYTES),
            utility: Mutex::new(Utility::default()),
            epoch: AtomicU64::new(0),
            resident: AtomicUsize::new(0),
            logical: AtomicUsize::new(0),
            attempts: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            compress_ns: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            decodes: AtomicU64::new(0),
            decode_ns: AtomicU64::new(0),
            promotions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            bypasses: AtomicU64::new(0),
            peak_working: AtomicUsize::new(0),
            #[cfg(test)]
            enabled: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub(super) fn working(&self) -> usize {
        *self
            .used
            .lock()
            .expect("ASSERT: cache codec workspace lock poisoned")
    }

    pub(super) fn maximum_working(&self) -> usize {
        self.maximum
    }

    fn permit(&self, bytes: usize, wait: bool) -> Option<Workspace<'_>> {
        if bytes > self.maximum {
            return None;
        }
        let mut used = self.used.lock().ok()?;
        while *used > self.maximum - bytes {
            if !wait {
                return None;
            }
            used = self.ready.wait(used).ok()?;
        }
        *used += bytes;
        self.peak_working.fetch_max(*used, Ordering::Relaxed);
        Some(Workspace { owner: self, bytes })
    }

    pub(super) fn prefer_decoded(&self, ns: u64, extra: usize, recent_hits: u8) -> bool {
        // Compare CPU time avoided per extra resident byte against observed
        // decodes. No fixed hot/cold byte split. Unknown/busy evidence retains
        // the compressed copy; optional promotion must not block readers.
        let Ok(mut utility) = self.utility.try_lock() else {
            return false;
        };
        let extra = extra.max(1) as u64;
        let prefer = recent_hits >= 2
            && (utility.extra_bytes == 0
                || u128::from(ns) * u128::from(recent_hits) * u128::from(utility.extra_bytes)
                    >= u128::from(utility.decode_ns) * u128::from(extra));
        if utility.extra_bytes == 0 {
            utility.decode_ns = ns.max(1);
            utility.extra_bytes = extra;
        } else {
            utility.decode_ns = utility.decode_ns - utility.decode_ns / 32 + ns / 32;
            utility.extra_bytes = utility.extra_bytes - utility.extra_bytes / 32 + extra / 32;
        }
        prefer
    }
}

struct Workspace<'a> {
    owner: &'a Compression,
    bytes: usize,
}
impl Drop for Workspace<'_> {
    fn drop(&mut self) {
        let mut used = self
            .owner
            .used
            .lock()
            .expect("ASSERT: cache workspace lock poisoned");
        *used -= self.bytes;
        self.owner.ready.notify_all();
    }
}

impl VerifiedReadCache {
    pub(crate) fn admit_decoded_group(&self, payloads: Vec<VerifiedChunkPayload>) {
        if payloads.is_empty() {
            return;
        }
        for payload in &payloads {
            assert!(
                payloads[0].shares_backing_with(payload),
                "ASSERT: decoded admission shares one backing"
            );
        }
        self.maybe_refresh_pressure();
        if self.target_bytes.load(Ordering::Acquire) == 0 {
            self.pressure_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.compression.epoch.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if !self.compression.enabled.load(Ordering::Relaxed) {
            self.admit_group(payloads.into_iter().map(CachedPayload::Decoded).collect());
            return;
        }
        self.demote_cold_victim(&payloads[0]);
        // Either compress the complete shared allocation's admitted Chunk
        // views or keep its zero-copy representation. Keeping some compressed
        // siblings while a raw sibling pins the whole backing would waste RAM.
        let work = payloads
            .iter()
            .fold(CODEC_WORK, |sum, value| sum.saturating_add(value.len()));
        let compressed = self.compression.permit(work, false).and_then(|_workspace| {
            let start = Instant::now();
            self.compression.attempts.fetch_add(1, Ordering::Relaxed);
            let result: Option<Vec<_>> = payloads
                .iter()
                .map(VerifiedChunkPayload::compress_for_cache)
                .collect();
            self.compression
                .compress_ns
                .fetch_add(nanos(start), Ordering::Relaxed);
            result
        });
        if let Some(compressed) = compressed {
            // Charge the compact owned bytes, not an LZ4 compressBound buffer.
            let total = compressed
                .iter()
                .map(CompressedVerifiedChunkPayload::resident_bytes)
                .sum::<usize>();
            if total < payloads[0].backing_allocation_bytes() {
                for value in compressed {
                    self.admit_group(vec![CachedPayload::Compressed(value)]);
                }
                return;
            }
        }
        self.compression.bypasses.fetch_add(1, Ordering::Relaxed);
        self.admit_group(payloads.into_iter().map(CachedPayload::Decoded).collect());
    }

    pub(super) fn compressed_hit(
        &self,
        value: &CompressedVerifiedChunkPayload,
        recent_hits: u8,
    ) -> Option<VerifiedChunkPayload> {
        if let Some(payload) = value.live_view() {
            self.compression.hits.fetch_add(1, Ordering::Relaxed);
            return Some(payload);
        }
        let _workspace = self.compression.permit(CODEC_WORK, true)?;
        let start = Instant::now();
        let Some((payload, decoded)) = value.decompress() else {
            self.compression.failures.fetch_add(1, Ordering::Relaxed);
            self.invalidate_compressed(value);
            return None;
        };
        let ns = nanos(start);
        self.compression.hits.fetch_add(1, Ordering::Relaxed);
        if decoded {
            self.compression.decodes.fetch_add(1, Ordering::Relaxed);
            self.compression.decode_ns.fetch_add(ns, Ordering::Relaxed);
        }
        let extra = payload
            .backing_allocation_bytes()
            .saturating_sub(value.resident_bytes());
        if decoded && self.compression.prefer_decoded(ns, extra, recent_hits) {
            self.promote(&payload);
        }
        Some(payload)
    }

    fn invalidate_compressed(&self, value: &CompressedVerifiedChunkPayload) {
        let _admission = self
            .admission
            .lock()
            .expect("ASSERT: cache admission lock poisoned");
        let key = CacheKey {
            chunk_id: value.chunk_id(),
            logical_length: value.logical_length() as u64,
        };
        let hash = cache_hash(key);
        let shard = &self.shards[hash & (self.shards.len() - 1)];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: cache shard lock poisoned");
        let set_index = (hash / self.shards.len()) % state.sets.len();
        let Some(slot) = state.sets[set_index].ways.iter_mut().find(|slot| {
            slot.as_ref().is_some_and(|entry| {
                matches!(&entry.payload,
                CachedPayload::Compressed(current) if current.shares_backing_with(value))
            })
        }) else {
            return;
        };
        let entry = slot.take().expect("ASSERT: invalid cache entry exists");
        assert_eq!(
            Arc::strong_count(&entry.backing_charge),
            1,
            "ASSERT: compressed ownership is independent"
        );
        self.uncharge(&entry.backing_charge);
        self.resident_bytes
            .fetch_sub(entry.backing_charge.bytes, Ordering::AcqRel);
        self.entry_count.fetch_sub(1, Ordering::AcqRel);
        state.counters.evictions = state.counters.evictions.saturating_add(1);
    }

    fn demote_cold_victim(&self, incoming: &VerifiedChunkPayload) {
        if self
            .resident_bytes
            .load(Ordering::Acquire)
            .saturating_add(incoming.backing_allocation_bytes())
            <= self.target_bytes.load(Ordering::Acquire)
        {
            return;
        }
        // One bounded candidate on a separate demotion cursor; codec work happens
        // outside admission/shard locks. Shared raw backings stay shared until
        // their final cache view is eligible, avoiding a duplicate RAM charge.
        let candidate = {
            let Ok(mut admission) = self.admission.try_lock() else {
                return;
            };
            let cursor = admission.compression_cursor;
            let shard = &self.shards[cursor % self.shards.len()];
            let state = shard
                .state
                .lock()
                .expect("ASSERT: cache shard lock poisoned");
            let set_index = (cursor / self.shards.len()) % state.sets.len();
            admission.compression_cursor = (cursor + 1) % (self.shards.len() * state.sets.len());
            let epoch = self.compression.epoch.load(Ordering::Relaxed);
            let window = self.entry_count.load(Ordering::Relaxed).max(1) as u64;
            state.sets[set_index]
                .ways
                .iter()
                .flatten()
                .find_map(|entry| {
                    let CachedPayload::Decoded(payload) = &entry.payload else {
                        return None;
                    };
                    (Arc::strong_count(&entry.backing_charge) == 1
                        && epoch.saturating_sub(entry.last_hit_epoch) > window)
                        .then(|| payload.clone())
                })
        };
        let Some(payload) = candidate else {
            return;
        };
        let Some(_workspace) = self.compression.permit(CODEC_WORK, false) else {
            return;
        };
        let start = Instant::now();
        self.compression.attempts.fetch_add(1, Ordering::Relaxed);
        let compressed = payload.compress_for_cache();
        self.compression
            .compress_ns
            .fetch_add(nanos(start), Ordering::Relaxed);
        let Some(value) = compressed else {
            return;
        };
        let Ok(_admission) = self.admission.try_lock() else {
            return;
        };
        let key = CacheKey {
            chunk_id: payload.chunk_id(),
            logical_length: payload.len() as u64,
        };
        let hash = cache_hash(key);
        let shard = &self.shards[hash & (self.shards.len() - 1)];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: cache shard lock poisoned");
        let set_index = (hash / self.shards.len()) % state.sets.len();
        let Some(entry) = state.sets[set_index]
            .ways
            .iter_mut()
            .flatten()
            .find(|entry| entry.matches(key))
        else {
            return;
        };
        let CachedPayload::Decoded(current) = &entry.payload else {
            return;
        };
        let epoch = self.compression.epoch.load(Ordering::Relaxed);
        if !current.shares_backing_with(&payload)
            || Arc::strong_count(&entry.backing_charge) != 1
            || epoch.saturating_sub(entry.last_hit_epoch)
                <= self.entry_count.load(Ordering::Relaxed).max(1) as u64
        {
            return;
        }
        let bytes = value.resident_bytes();
        let saved = entry.backing_charge.bytes.saturating_sub(bytes);
        if saved == 0 {
            return;
        }
        let charge = Arc::new(CacheBackingCharge {
            bytes,
            compressed_logical: value.logical_length(),
        });
        self.charge(&charge);
        entry.payload = CachedPayload::Compressed(value);
        entry.backing_charge = charge;
        entry.recent_hits = 0;
        self.resident_bytes.fetch_sub(saved, Ordering::AcqRel);
        self.compression.demotions.fetch_add(1, Ordering::Relaxed);
    }

    fn promote(&self, payload: &VerifiedChunkPayload) {
        let _admission = match self.admission.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => return,
        };
        let key = CacheKey {
            chunk_id: payload.chunk_id(),
            logical_length: payload.len() as u64,
        };
        let hash = cache_hash(key);
        let shard = &self.shards[hash & (self.shards.len() - 1)];
        let mut state = shard
            .state
            .lock()
            .expect("ASSERT: cache shard lock poisoned");
        let set_index = (hash / self.shards.len()) % state.sets.len();
        let Some(entry) = state.sets[set_index]
            .ways
            .iter_mut()
            .flatten()
            .find(|entry| entry.matches(key))
        else {
            return;
        };
        if !matches!(entry.payload, CachedPayload::Compressed(_)) {
            return;
        }
        let target = self.target_bytes.load(Ordering::Acquire);
        let bytes = payload.backing_allocation_bytes();
        let extra = bytes.saturating_sub(entry.backing_charge.bytes);
        // Avoiding a decode must never evict another DATA-saving entry.
        // With no free lease, the hot payload stays compressed and RAM-only.
        if self
            .resident_bytes
            .load(Ordering::Acquire)
            .saturating_add(extra)
            > target
        {
            return;
        }
        self.uncharge(&entry.backing_charge);
        let charge = Arc::new(CacheBackingCharge {
            bytes,
            compressed_logical: 0,
        });
        self.charge(&charge);
        entry.payload = CachedPayload::Decoded(payload.clone());
        entry.backing_charge = charge;
        self.resident_bytes.fetch_add(extra, Ordering::AcqRel);
        self.compression.promotions.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn charge(&self, backing: &CacheBackingCharge) {
        if backing.compressed_logical != 0 {
            self.compression
                .resident
                .fetch_add(backing.bytes, Ordering::Relaxed);
            self.compression
                .logical
                .fetch_add(backing.compressed_logical, Ordering::Relaxed);
            self.compression.admitted.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(super) fn uncharge(&self, backing: &CacheBackingCharge) {
        if backing.compressed_logical != 0 {
            self.compression
                .resident
                .fetch_sub(backing.bytes, Ordering::Relaxed);
            self.compression
                .logical
                .fetch_sub(backing.compressed_logical, Ordering::Relaxed);
        }
    }
}

fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    use crate::VerifiedReadCacheConfig;

    #[test]
    fn hot_admission_compares_decode_work_per_extra_byte() {
        let codec = Compression::new(MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0));
        assert!(!codec.prefer_decoded(100_000, 1024, 1));
        assert!(!codec.prefer_decoded(100, 65536, 2));
        assert!(codec.prefer_decoded(200_000, 1024, 2));
    }

    #[test]
    fn codec_workspace_is_bounded_and_released_on_all_paths() {
        let codec = Compression::new(MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0));
        let held = codec.permit(codec.maximum, false).unwrap();
        assert!(codec.permit(1, false).is_none());
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                let _work = codec.permit(CODEC_WORK, true).unwrap();
                assert!(codec.working() <= codec.maximum);
            });
            drop(held);
            waiter.join().unwrap();
        });
        assert_eq!(codec.working(), 0);
        assert!(codec.permit(codec.maximum + 1, false).is_none());
    }

    #[test]
    fn a_cold_single_owner_is_recompressed_under_admission_pressure() {
        let cache = VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(4 << 20, 0, NonZeroUsize::MIN).unwrap(),
            MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0),
        )
        .unwrap();
        let payload = crate::read_cache::tests::verified_payload(&vec![73; 65536]);
        cache.compression.enabled.store(false, Ordering::Relaxed);
        cache.admit_decoded_group(vec![payload.clone()]);
        cache.compression.enabled.store(true, Ordering::Relaxed);
        let target = cache.metadata_bytes + cache.status().resident_bytes();
        cache.update_memory_pressure(MemoryPressureSnapshot::new(128 << 20, target as u64, 0));
        let key = CacheKey {
            chunk_id: payload.chunk_id(),
            logical_length: 65536,
        };
        let sets = cache.shards[0].state.lock().unwrap().sets.len();
        cache.admission.lock().unwrap().compression_cursor = cache_hash(key) % sets;
        cache.compression.epoch.store(100, Ordering::Relaxed);
        cache.demote_cold_victim(&payload);
        assert_eq!(cache.status().demotions(), 1);
        assert!(cache.status().resident_bytes() < 65536 / 4);
        assert_eq!(cache.get(payload.chunk_id(), 65536).unwrap(), payload);
        crate::read_cache::reclamation_tests::assert_accounting(&cache);
    }
}

#[cfg(test)]
#[test]
fn invalidation_removes_only_the_failed_copy_and_allows_fresh_admission() {
    let cache = VerifiedReadCache::new_with_snapshot(
        crate::VerifiedReadCacheConfig::new(4 << 20, 0, NonZeroUsize::MIN).unwrap(),
        MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0),
    )
    .unwrap();
    let value = crate::read_cache::tests::verified_payload(&vec![81; 65536]);
    let old = value.compress_for_cache().unwrap();
    cache.admit_group(vec![CachedPayload::Compressed(old.clone())]);
    cache.invalidate_compressed(&old);
    assert_eq!(cache.status().resident_bytes(), 0);
    assert_eq!(cache.status().entry_count(), 0);
    cache.admit_decoded_group(vec![value.clone()]);
    cache.invalidate_compressed(&old); // Late failure must not remove a replacement.
    assert_eq!(cache.get(value.chunk_id(), 65536).unwrap(), value);
    crate::read_cache::reclamation_tests::assert_accounting(&cache);
}
