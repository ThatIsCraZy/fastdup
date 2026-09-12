use super::*;
use fastdup_format::RawRecord;

fn payload(bytes: &[u8]) -> VerifiedChunkPayload {
    RawRecord::decode(&RawRecord::encode(bytes).unwrap())
        .unwrap()
        .into_verified_payload()
}

fn cache() -> VerifiedReadCache {
    VerifiedReadCache::new_legacy_with_snapshot(
        VerifiedReadCacheConfig::new(4 * 1024 * 1024, 0, NonZeroUsize::new(4).unwrap()).unwrap(),
        MemoryPressureSnapshot::new(128 * 1024 * 1024, 128 * 1024 * 1024, 0),
    )
    .unwrap()
}

fn noise(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn compressed_entries_are_self_contained_and_survive_cache_purge_as_owned_views() {
    let cache = cache();
    let original = payload(&vec![31; 65536]);
    cache.admit_decoded_group(vec![original.clone()]);
    let status = cache.status();
    assert!(status.compressed_resident_bytes() < 65536 / 4);
    assert_eq!(status.compressed_logical_bytes(), 65536);
    assert_eq!(status.resident_bytes(), status.compressed_resident_bytes());
    let id = original.chunk_id();
    drop(original);
    let hit = cache.get(id, 65536).unwrap();
    assert_eq!(hit.as_slice(), &vec![31; 65536]);
    assert_eq!(cache.status().compressed_hits(), 1);
    assert_eq!(cache.status().decompressions(), 1);
    cache.update_memory_pressure(MemoryPressureSnapshot::new(128 * 1024 * 1024, 0, 0));
    assert_eq!(cache.status().compressed_resident_bytes(), 0);
    assert_eq!(cache.status().compressed_logical_bytes(), 0);
    assert_eq!(cache.status().resident_bytes(), 0);
    assert_eq!(hit.as_slice(), &vec![31; 65536]);
    assert_eq!(cache.status().codec_working_bytes(), 0);
}

#[test]
fn repeated_expensive_decodes_promote_without_a_fixed_hot_byte_reservation() {
    let cache = cache();
    let original = payload(&vec![37; 65536]);
    cache.admit_decoded_group(vec![original.clone()]);
    let id = original.chunk_id();
    drop(original);
    for _ in 0..128 {
        assert_eq!(cache.get(id, 65536).unwrap().as_slice(), &vec![37; 65536]);
        if cache.status().promotions() != 0 {
            break;
        }
    }
    assert_eq!(cache.status().promotions(), 1);
    assert_eq!(cache.status().compressed_resident_bytes(), 0);
    let decoded = cache.status().decompressions();
    for _ in 0..32 {
        assert_eq!(cache.get(id, 65536).unwrap().as_slice(), &vec![37; 65536]);
    }
    assert_eq!(cache.status().decompressions(), decoded);
    super::reclamation_tests::assert_accounting(&cache);
}

#[test]
fn incompressible_payloads_keep_the_original_shared_owner() {
    let cache = cache();
    let original = payload(&noise(65536, 773));
    cache.admit_decoded_group(vec![original.clone()]);
    let hit = cache.get(original.chunk_id(), 65536).unwrap();
    assert!(hit.shares_backing_with(&original));
    assert_eq!(cache.status().compressed_admissions(), 0);
    assert_eq!(cache.status().compression_bypasses(), 1);
}

#[test]
fn shared_record_views_are_compacted_without_retaining_the_large_decoded_owner() {
    let cache = cache();
    let group = super::reclamation_tests::shared_group();
    cache.admit_decoded_group(group.clone());
    assert!(cache.status().resident_bytes() < group[0].backing_allocation_bytes() / 4);
    for value in &group {
        assert_eq!(
            cache.get(value.chunk_id(), value.len() as u64).unwrap(),
            *value
        );
    }
    super::reclamation_tests::assert_accounting(&cache);
}

#[test]
fn concurrent_compaction_promotion_and_pressure_account_both_representations() {
    let cache = Arc::new(cache());
    super::reclamation_tests::limit_payload(&cache, 128 * 1024);
    std::thread::scope(|scope| {
        for worker in 1..=8_u8 {
            let cache = Arc::clone(&cache);
            scope.spawn(move || {
                let values = [
                    payload(&vec![worker; 65536]),
                    payload(&noise(32768, u64::from(worker))),
                ];
                for n in 0..128 {
                    let value = &values[n % 2];
                    cache.admit_decoded_group(vec![value.clone()]);
                    for _ in 0..3 {
                        if let Some(hit) = cache.get(value.chunk_id(), value.len() as u64) {
                            assert_eq!(hit, *value);
                        }
                    }
                }
            });
        }
        scope.spawn(|| {
            for _ in 0..128 {
                super::reclamation_tests::limit_payload(&cache, 0);
                super::reclamation_tests::limit_payload(&cache, 128 * 1024);
            }
        });
    });
    super::reclamation_tests::assert_accounting(&cache);
    let status = cache.status();
    assert!(status.compressed_resident_bytes() <= status.resident_bytes());
    assert!(status.codec_peak_working_bytes() <= status.codec_max_working_bytes());
    assert_eq!(status.codec_working_bytes(), 0);
}

#[test]
#[ignore = "manual release-mode compressed cache A/B, same byte budget and corpus"]
fn compressed_cache_workload_benchmark() {
    use std::hint::black_box;
    let corpus: Vec<_> = (0..48_u64)
        .map(|seed| {
            let pattern = noise(8192, seed + 1);
            pattern.repeat(8)
        })
        .collect();
    for round in 0..5 {
        for enabled in [round % 2 == 0, round % 2 != 0] {
            let cache = cache();
            cache.compression.enabled.store(enabled, Ordering::Relaxed);
            super::reclamation_tests::limit_payload(&cache, 512 * 1024);
            let start = Instant::now();
            for _ in 0..32 {
                for value in &corpus {
                    if let Some(hit) = cache.get(ChunkId::of(value), value.len() as u64) {
                        black_box(hit);
                    } else {
                        cache.admit_decoded_group(vec![payload(value)]);
                    }
                }
            }
            let elapsed = start.elapsed();
            let status = cache.status();
            println!(
                "compressed_cache round={round} enabled={enabled} requests=1536 hits={} misses={} elapsed_ns={} resident={} compressed={} represented={} promotions={} peak_codec={}",
                status.hits(),
                status.misses(),
                elapsed.as_nanos(),
                status.resident_bytes(),
                status.compressed_resident_bytes(),
                status.compressed_logical_bytes(),
                status.promotions(),
                status.codec_peak_working_bytes()
            );
        }
    }
}

#[test]
fn hot_promotion_never_evicts_other_entries_to_save_codec_cpu() {
    let cache = cache();
    let ids: Vec<_> = (91..=94)
        .map(|byte| {
            let value = payload(&vec![byte; 65536]);
            let id = value.chunk_id();
            cache.admit_decoded_group(vec![value]);
            id
        })
        .collect();
    let resident = cache.status().resident_bytes();
    super::reclamation_tests::limit_payload(&cache, resident);
    let before = cache.status().evictions();
    for _ in 0..64 {
        for &id in &ids {
            assert!(cache.get(id, 65536).is_some());
        }
    }
    assert_eq!(cache.status().promotions(), 0);
    assert_eq!(cache.status().evictions(), before);
    assert_eq!(cache.status().resident_bytes(), resident);
    assert_eq!(cache.status().misses(), 0);
}
