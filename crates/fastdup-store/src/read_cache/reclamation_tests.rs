use super::*;
use fastdup_format::{ContainerId, RawRecord, SealedContainer};

fn payload(value: u8, length: usize) -> VerifiedChunkPayload {
    RawRecord::decode(&RawRecord::encode(&vec![value; length]).unwrap())
        .unwrap()
        .into_verified_payload()
}

fn cache(limit: usize) -> VerifiedReadCache {
    let cache = VerifiedReadCache::new_legacy_with_snapshot(
        VerifiedReadCacheConfig::new(limit, 0, NonZeroUsize::new(4).unwrap()).unwrap(),
        MemoryPressureSnapshot::new(128 * 1024 * 1024, 128 * 1024 * 1024, 0),
    )
    .unwrap();
    cache.compression.enabled.store(false, Ordering::Relaxed);
    cache
}

pub(super) fn shared_group() -> Vec<VerifiedChunkPayload> {
    let chunks: Vec<_> = (1..=16).map(|value| vec![value; 16384]).collect();
    let parts: Vec<_> = chunks.iter().map(Vec::as_slice).collect();
    let encoded =
        SealedContainer::encode_zstd_regions(ContainerId::new([0xd3; 16]).unwrap(), 1, &[&parts])
            .unwrap();
    let decoded = SealedContainer::decode(&encoded).unwrap();
    let group: Vec<_> = decoded
        .records()
        .iter()
        .map(RawRecord::verified_payload)
        .collect();
    assert!(group.iter().all(|item| item.shares_backing_with(&group[0])));
    group
}

pub(super) fn limit_payload(cache: &VerifiedReadCache, bytes: usize) {
    cache.update_memory_pressure(MemoryPressureSnapshot::new(
        128 * 1024 * 1024,
        u64::try_from(cache.metadata_bytes + bytes).unwrap(),
        0,
    ));
}

pub(super) fn assert_accounting(cache: &VerifiedReadCache) {
    let _admission = cache.admission.lock().unwrap();
    let mut backings = std::collections::BTreeMap::new();
    let mut entries = 0;
    for shard in &cache.shards {
        let state = shard.state.lock().unwrap();
        for entry in state.sets.iter().flat_map(|set| set.ways.iter().flatten()) {
            entries += 1;
            backings.insert(
                Arc::as_ptr(&entry.backing_charge).addr(),
                entry.backing_charge.bytes,
            );
        }
    }
    assert_eq!(cache.entry_count.load(Ordering::Acquire), entries);
    let resident = cache.resident_bytes.load(Ordering::Acquire);
    assert_eq!(resident, backings.values().sum::<usize>());
    let mut compressed = std::collections::BTreeMap::new();
    for shard in &cache.shards {
        let state = shard.state.lock().unwrap();
        for entry in state.sets.iter().flat_map(|set| set.ways.iter().flatten()) {
            if entry.backing_charge.compressed_logical != 0 {
                compressed.insert(
                    Arc::as_ptr(&entry.backing_charge).addr(),
                    (
                        entry.backing_charge.bytes,
                        entry.backing_charge.compressed_logical,
                    ),
                );
            }
        }
    }
    assert_eq!(
        cache.compression.resident.load(Ordering::Acquire),
        compressed.values().map(|v| v.0).sum::<usize>()
    );
    assert_eq!(
        cache.compression.logical.load(Ordering::Acquire),
        compressed.values().map(|v| v.1).sum::<usize>()
    );
    assert!(resident <= cache.target_bytes.load(Ordering::Acquire));
    assert!(resident + cache.metadata_bytes <= cache.config.hard_limit_bytes);
}

#[test]
fn shared_backing_is_charged_until_its_last_cache_view_is_evicted() {
    let cache = cache(4 * 1024 * 1024);
    let group = shared_group();
    let bytes = group[0].backing_allocation_bytes();
    limit_payload(&cache, bytes);
    cache.admit_decoded_group(group.clone());
    assert!(cache.status().entry_count() > 1);
    let protected = Arc::new(CacheBackingCharge {
        bytes: 0,
        compressed_logical: 0,
    });
    for _ in 0..1024 {
        {
            let mut admission = cache.admission.lock().unwrap();
            let mut steps = 1;
            cache.reclaim_locked(&mut admission, &mut steps, 0, Some(&protected));
            assert_eq!(steps, 0);
        }
        assert_accounting(&cache);
        let status = cache.status();
        if status.entry_count() == 0 {
            assert_eq!(status.resident_bytes(), 0);
            // An outstanding verified reader retains its own immutable bytes.
            assert_eq!(group[0].as_slice(), &vec![1; 16384]);
            return;
        }
        assert_eq!(status.resident_bytes(), bytes);
    }
    panic!("bounded sweeps must eventually retire the shared backing");
}

#[test]
fn large_sparse_geometry_makes_progress_with_bounded_admission_work() {
    let cache = cache(64 * 1024 * 1024);
    let group = shared_group();
    let bytes = group[0].backing_allocation_bytes();
    limit_payload(&cache, bytes);
    cache.admit_decoded_group(group);
    let new = payload(93, 262_144);
    assert_eq!(new.backing_allocation_bytes(), bytes);
    let slots = cache.shards.len() * cache.shards[0].state.lock().unwrap().sets.len() * CACHE_WAYS;
    assert!(slots > MAX_RECLAIM_STEPS_PER_GROUP);
    for _ in 0..=slots.div_ceil(MAX_RECLAIM_STEPS_PER_GROUP) {
        let before = cache.admission.lock().unwrap().reclaim_cursor;
        cache.admit_decoded_group(vec![new.clone()]);
        let after = cache.admission.lock().unwrap().reclaim_cursor;
        assert!((after + slots - before) % slots <= MAX_RECLAIM_STEPS_PER_GROUP);
        assert_accounting(&cache);
        if let Some(hit) = cache.get(new.chunk_id(), 262_144) {
            assert_eq!(hit, new);
            assert!(cache.status().evictions() > 0);
            return;
        }
    }
    panic!("the cursor must progress across admissions, including shared views");
}

#[test]
fn concurrent_admission_hits_and_pressure_keep_exact_accounting() {
    let cache = Arc::new(cache(4 * 1024 * 1024));
    limit_payload(&cache, 262_144);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let cache = Arc::clone(&cache);
            scope.spawn(move || {
                let values: Vec<_> = (1..=8)
                    .map(|value| payload(worker * 8 + value, 65536))
                    .collect();
                for ordinal in 0..256 {
                    let value = &values[ordinal % values.len()];
                    cache.admit_decoded_group(vec![value.clone()]);
                    if let Some(hit) = cache.get(value.chunk_id(), 65536) {
                        assert_eq!(hit, *value);
                    }
                }
            });
        }
        scope.spawn(|| {
            for _ in 0..64 {
                limit_payload(&cache, 0);
                limit_payload(&cache, 262_144);
            }
        });
    });
    assert_accounting(&cache);
    limit_payload(&cache, 0);
    assert_eq!(cache.status().resident_bytes(), 0);
    assert_eq!(cache.status().entry_count(), 0);
}

#[test]
#[ignore = "manual optimized uncontended cache-hit A/B benchmark"]
fn cache_hit_benchmark() {
    let cache = cache(4 * 1024 * 1024);
    let value = payload(17, 65536);
    let id = value.chunk_id();
    cache.admit_decoded_group(vec![value]);
    for round in 0..7 {
        let started = Instant::now();
        for _ in 0..2_000_000 {
            std::hint::black_box(cache.get(std::hint::black_box(id), 65536).unwrap());
        }
        println!(
            "cache_hit round={round} requests=2000000 elapsed_ns={}",
            started.elapsed().as_nanos()
        );
    }
}
