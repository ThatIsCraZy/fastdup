use std::num::NonZeroUsize;
use std::sync::Arc;

use fastdup_format::{ChunkId, ContainerId, ManifestExtent, ManifestLeaf};
use fastdup_store::{
    ContainerRepository, MemoryPressureSnapshot, VerifiedManifestFile, VerifiedReadCache,
    VerifiedReadCacheConfig,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

#[test]
fn verified_manifest_reads_share_one_bounded_cache_without_second_storage_read() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let payload = vec![0xA5; 64 * 1_024];
    containers
        .publish_raw(
            ContainerId::new([0x71; 16]).expect("container ID is nonzero"),
            1,
            &[&payload],
        )
        .expect("publish cache fixture");
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(
                512 * 1_024,
                128 * 1_024,
                NonZeroUsize::new(4).expect("four shards"),
            )
            .expect("valid cache geometry"),
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 4 * 1_024 * 1_024, 0),
        )
        .expect("construct bounded cache"),
    );
    let manifest = ManifestLeaf::new(
        u64::try_from(payload.len()).expect("fixture length fits u64"),
        vec![ManifestExtent::Data {
            logical_length: u64::try_from(payload.len()).expect("fixture length fits u64"),
            chunk_id: ChunkId::of(&payload),
        }],
    )
    .expect("construct cache fixture Manifest");
    let file = VerifiedManifestFile::new(manifest, containers)
        .expect("verify cache fixture")
        .with_verified_read_cache(Arc::clone(&cache));
    let baseline = storage.operation_count();

    assert_eq!(
        file.read_at(
            0,
            u32::try_from(payload.len()).expect("fixture length fits u32")
        )
        .expect("first read verifies DATA"),
        payload
    );
    let after_first = storage.operation_count();
    assert!(after_first > baseline);
    assert_eq!(
        file.clone()
            .read_at(
                0,
                u32::try_from(payload.len()).expect("fixture length fits u32")
            )
            .expect("second reader reuses verified DATA"),
        payload
    );
    assert_eq!(storage.operation_count(), after_first);

    let status = cache.status();
    assert_eq!(status.hits(), 1);
    assert_eq!(status.misses(), 1);
    assert_eq!(status.admissions(), 1);
    assert_eq!(status.entry_count(), 1);
    assert!(status.resident_bytes() <= status.target_bytes());
}

#[test]
fn swap_or_lost_headroom_purges_cache_and_refuses_new_admissions() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let payload = vec![0x3C; 32 * 1_024];
    containers
        .publish_raw(
            ContainerId::new([0x72; 16]).expect("container ID is nonzero"),
            2,
            &[&payload],
        )
        .expect("publish pressure fixture");
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(
                512 * 1_024,
                128 * 1_024,
                NonZeroUsize::new(2).expect("two shards"),
            )
            .expect("valid cache geometry"),
            MemoryPressureSnapshot::new(8 * 1_024 * 1_024, 4 * 1_024 * 1_024, 0),
        )
        .expect("construct bounded cache"),
    );
    let logical_length = u64::try_from(payload.len()).expect("fixture length fits u64");
    let manifest = ManifestLeaf::new(
        logical_length,
        vec![ManifestExtent::Data {
            logical_length,
            chunk_id: ChunkId::of(&payload),
        }],
    )
    .expect("construct pressure fixture Manifest");
    let file = VerifiedManifestFile::new(manifest, containers)
        .expect("verify pressure fixture")
        .with_verified_read_cache(Arc::clone(&cache));

    file.read_at(
        0,
        u32::try_from(payload.len()).expect("fixture length fits u32"),
    )
    .expect("warm cache");
    assert_eq!(cache.status().entry_count(), 1);
    cache.update_memory_pressure(MemoryPressureSnapshot::new(
        8 * 1_024 * 1_024,
        4 * 1_024 * 1_024,
        4 * 1_024,
    ));
    let pressured = cache.status();
    assert_eq!(pressured.target_bytes(), 0);
    assert_eq!(pressured.resident_bytes(), 0);
    assert_eq!(pressured.entry_count(), 0);

    let baseline = storage.operation_count();
    file.read_at(
        0,
        u32::try_from(payload.len()).expect("fixture length fits u32"),
    )
    .expect("pressure does not make durable DATA unreadable");
    let operations = &storage.operations()[baseline..];
    assert!(operations.contains(&StorageOperation::Read));
    let status = cache.status();
    assert_eq!(status.entry_count(), 0);
    assert_eq!(status.pressure_rejections(), 1);
}
