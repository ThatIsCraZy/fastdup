use super::*;
use fastdup_format::{ContainerId, RawRecord, SealedContainer};

fn payload(value: u8, length: usize) -> VerifiedChunkPayload {
    RawRecord::decode(&RawRecord::encode(&vec![value; length]).unwrap())
        .unwrap()
        .into_verified_payload()
}

fn cache(limit: usize) -> VerifiedReadCache {
    let cache = VerifiedReadCache::new_with_snapshot(
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
        u64::try_from(bytes).unwrap(),
        0,
    ));
}

/// Recomputes resident accounting from the unified directory itself:
/// one charge per shared decoded backing, one per compressed entry,
/// plus the unified per-entry directory charge.
pub(super) fn assert_accounting(cache: &VerifiedReadCache) {
    let status = cache.status();
    let mut entries = 0_usize;
    let mut backings = std::collections::HashMap::new();
    let mut compressed_bytes = 0_usize;
    let mut compressed_logical = 0_usize;
    cache.unified.visit::<CachedPayload>(|payload| {
        entries += 1;
        match payload {
            CachedPayload::Decoded(payload) => {
                backings.insert(payload.backing_id(), payload.backing_allocation_bytes());
            }
            CachedPayload::Compressed(value) => {
                compressed_bytes += value.resident_bytes();
                compressed_logical += value.logical_length();
            }
        }
    });
    assert_eq!(status.entry_count(), entries);
    let expected = backings.values().sum::<usize>() + compressed_bytes + entries * 512;
    assert_eq!(status.resident_bytes(), expected);
    assert_eq!(status.compressed_resident_bytes(), compressed_bytes);
    assert_eq!(status.compressed_logical_bytes(), compressed_logical);
    assert!(status.resident_bytes() <= status.target_bytes());
}

#[test]
fn shared_backing_is_charged_once_and_released_when_pressure_evicts_it() {
    let cache = cache(4 * 1024 * 1024);
    let group = shared_group();
    let bytes = group[0].backing_allocation_bytes();
    cache.admit_decoded_group(group.clone());
    assert!(cache.status().entry_count() > 1);
    assert_eq!(
        cache.status().resident_bytes(),
        bytes + group.len() * 512,
        "one shared backing is charged once for all its cache views"
    );
    assert_accounting(&cache);
    limit_payload(&cache, 0);
    assert_eq!(cache.status().entry_count(), 0);
    assert_eq!(cache.status().resident_bytes(), 0);
    // An outstanding verified reader retains its own immutable bytes.
    assert_eq!(group[0].as_slice(), &vec![1; 16384]);
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
