use super::{
    ChunkId, MemoryPressureSnapshot, VerifiedChunkPayload, VerifiedReadCache,
    shared_cache_reserve_bytes,
};
use crate::{CacheFallback, CacheObservation, CachePool};
use fastdup_format::{BufferPool, CompressedVerifiedChunkPayload, MAX_LOGICAL_CHUNK_BYTES};
use std::num::NonZeroUsize;
use std::sync::Condvar;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    pub(super) buffers: BufferPool,
    buffer_refresh: Mutex<()>,
    buffer_budget: Option<CachePool>,
    used: Mutex<usize>,
    ready: Condvar,
    maximum: usize,
    utility: Mutex<Utility>,
    pub(super) epoch: AtomicU64,
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
    pub(super) fn new(snapshot: MemoryPressureSnapshot, automatic: bool) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let reserve = usize::try_from(shared_cache_reserve_bytes(snapshot.effective_limit_bytes()))
            .unwrap_or(usize::MAX);
        Self {
            buffers: BufferPool::new(),
            buffer_refresh: Mutex::new(()),
            buffer_budget: automatic.then(|| {
                CachePool::system(
                    "codecBuffers",
                    CacheFallback::Memory,
                    BufferPool::metadata_bytes() as u64,
                    (32 * (MAX_LOGICAL_CHUNK_BYTES + 2048) + BufferPool::metadata_bytes()) as u64,
                )
            }),
            used: Mutex::new(0),
            ready: Condvar::new(),
            maximum: cpus
                .saturating_mul(CODEC_WORK + MAX_LOGICAL_CHUNK_BYTES)
                .min(reserve)
                .max(CODEC_WORK + MAX_LOGICAL_CHUNK_BYTES),
            utility: Mutex::new(Utility::default()),
            epoch: AtomicU64::new(0),
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

    pub(super) fn refresh_buffers(&self, snapshot: MemoryPressureSnapshot) {
        let _refresh = self
            .buffer_refresh
            .lock()
            .expect("ASSERT: buffer refresh lock poisoned");
        let observed = self.buffers.status();
        let fixed = BufferPool::metadata_bytes() as u64;
        let target = if let Some(pool) = &self.buffer_budget {
            usize::try_from(
                pool.target(
                    snapshot,
                    CacheObservation {
                        hits: observed.hits,
                        misses: observed.misses,
                        evictions: observed.evictions,
                        // Reuse benefit is allocation work, not claimed avoided DATA bytes.
                        hit_bytes: observed.reused_bytes,
                        resident_bytes: fixed + observed.retained_bytes as u64,
                    },
                )
                .saturating_sub(fixed),
            )
            .unwrap_or(usize::MAX)
        } else if snapshot.swap_used_bytes() == 0 {
            usize::try_from(
                snapshot
                    .available_bytes()
                    .saturating_sub(shared_cache_reserve_bytes(snapshot.effective_limit_bytes())),
            )
            .unwrap_or(usize::MAX)
            .min(self.maximum)
        } else {
            0
        };
        self.buffers.set_limit(target);
        if let Some(pool) = &self.buffer_budget {
            pool.applied(
                target as u64 + fixed,
                self.buffers.status().retained_bytes as u64 + fixed,
            );
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
        self.admit_decoded_group_inner(payloads, false);
    }

    pub(crate) fn admit_decoded_group_writer(&self, payloads: Vec<VerifiedChunkPayload>) {
        self.admit_decoded_group_inner(payloads, true);
    }

    fn admit_decoded_group_inner(&self, payloads: Vec<VerifiedChunkPayload>, writer_warming: bool) {
        if payloads.is_empty() {
            return;
        }
        self.admit_location_proofs(&payloads);
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
            self.admit_group(
                payloads.into_iter().map(CachedPayload::Decoded).collect(),
                writer_warming,
            );
            return;
        }
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
                .map(|payload| payload.compress_for_cache_with_pool(&self.compression.buffers))
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
                let groups = compressed
                    .into_iter()
                    .map(|value| {
                        let bytes = value.resident_bytes() as u64;
                        let length = value.logical_length() as u64;
                        let key = crate::ReadCacheKey {
                            identity: value.chunk_id().bytes(),
                            ordinal: length,
                        };
                        (
                            vec![(key, Arc::new(CachedPayload::Compressed(value)), length)],
                            bytes,
                        )
                    })
                    .collect();
                let admitted = if writer_warming {
                    self.unified.insert_groups_writer_warming(groups)
                } else {
                    self.unified.insert_groups(groups)
                };
                self.compression
                    .admitted
                    .fetch_add(admitted, Ordering::Relaxed);
                return;
            }
        }
        self.compression.bypasses.fetch_add(1, Ordering::Relaxed);
        self.admit_group(
            payloads.into_iter().map(CachedPayload::Decoded).collect(),
            writer_warming,
        );
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
        let Some((payload, decoded)) = value.decompress_with_pool(&self.compression.buffers) else {
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
        self.unified.remove_if::<CachedPayload>(crate::ReadCacheKey {
            identity: value.chunk_id().bytes(), ordinal: value.logical_length() as u64,
        }, |entry| matches!(entry, CachedPayload::Compressed(current) if current.shares_backing_with(value)));
    }

    fn promote(&self, payload: &VerifiedChunkPayload) {
        // Hot compressed entries swap to their decoded representation
        // in place. `replace` charges only the delta and never evicts
        // another entry to save codec CPU.
        let key = crate::ReadCacheKey {
            identity: payload.chunk_id().bytes(),
            ordinal: payload.len() as u64,
        };
        if self.unified.replace(
            key,
            Arc::new(CachedPayload::Decoded(payload.clone())),
            payload.backing_allocation_bytes() as u64,
            payload.len() as u64,
        ) {
            self.compression.promotions.fetch_add(1, Ordering::Relaxed);
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
    fn pressure_closes_pool_retention_while_a_real_cache_hit_remains_readable() {
        let cache = VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(4 << 20, 0, NonZeroUsize::MIN).unwrap(),
            MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0),
        )
        .unwrap();
        let payload = crate::read_cache::tests::verified_payload(&vec![73; 65536]);
        let id = payload.chunk_id();
        cache.admit_decoded_group(vec![payload]);
        let reader = cache.get(id, 65536).unwrap();
        assert!(cache.status().buffer_pool().active_bytes > 0);
        cache.update_memory_pressure(MemoryPressureSnapshot::new(128 << 20, 0, 1));
        assert_eq!(reader.as_slice(), vec![73; 65536]);
        assert_eq!(cache.status().buffer_pool().retained_bytes, 0);
        drop(reader);
        assert_eq!(cache.status().buffer_pool().active_bytes, 0);
        assert_eq!(cache.status().buffer_pool().retained_bytes, 0);
        assert!(cache.status().resident_bytes() <= cache.status().target_bytes());
    }

    #[test]
    fn hot_admission_compares_decode_work_per_extra_byte() {
        let codec = Compression::new(MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0), false);
        assert!(!codec.prefer_decoded(100_000, 1024, 1));
        assert!(!codec.prefer_decoded(100, 65536, 2));
        assert!(codec.prefer_decoded(200_000, 1024, 2));
    }

    #[test]
    fn codec_workspace_is_bounded_and_released_on_all_paths() {
        let codec = Compression::new(MemoryPressureSnapshot::new(128 << 20, 128 << 20, 0), false);
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
    cache.admit_group(vec![CachedPayload::Compressed(old.clone())], false);
    cache.invalidate_compressed(&old);
    assert_eq!(cache.status().resident_bytes(), 0);
    assert_eq!(cache.status().entry_count(), 0);
    cache.admit_decoded_group(vec![value.clone()]);
    cache.invalidate_compressed(&old); // Late failure must not remove a replacement.
    assert_eq!(cache.get(value.chunk_id(), 65536).unwrap(), value);
    assert!(cache.status().resident_bytes() <= cache.status().target_bytes());
}
