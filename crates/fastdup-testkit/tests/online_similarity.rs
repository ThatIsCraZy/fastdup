use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId,
    SimilarityIndexEntry,
};
use fastdup_store::{
    ExactIndexRunRepository, OnlineSimilarityRepository, SimilarityIndexRepository, StorageIo,
    similarity_index_entry_v1,
};
use fastdup_testkit::MemoryStorageIo;

fn exact() -> ExactIndexRunRepository<MemoryStorageIo> {
    let exact = ExactIndexRunRepository::new(MemoryStorageIo::new());
    let location =
        ExactIndexLocation::raw(ContainerId::new([1; 16]).unwrap(), 1, 4096, 65728, 1).unwrap();
    exact
        .append_level_zero(
            ExactIndexProfileId::new([1; 32]).unwrap(),
            vec![ExactIndexEntry::active(ChunkId::of(b"anchor"), 65536, location).unwrap()],
        )
        .unwrap();
    exact
}

fn target() -> Vec<u8> {
    (0..65536_u32)
        .map(|i| (i.wrapping_mul(197).wrapping_add(i / 256) % 251) as u8)
        .collect()
}

fn entries(ordinals: std::ops::Range<u8>) -> Vec<SimilarityIndexEntry> {
    let template = similarity_index_entry_v1(&target()).unwrap();
    ordinals
        .map(|n| {
            SimilarityIndexEntry::new(
                ChunkId::from_bytes([n; 32]),
                template.logical_length(),
                template.fingerprint_profile(),
                template.superfeatures(),
                template.sketch(),
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn complete_bucket_replacements_survive_compaction_and_restart() {
    let storage = MemoryStorageIo::new();
    let exact = exact();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let online = OnlineSimilarityRepository::open(repository.clone(), &exact).unwrap();
    let data = target();
    for ordinal in (0..80_u8).rev() {
        online
            .append(
                &exact.pin_active_generation().unwrap(),
                &entries(ordinal..ordinal + 1),
            )
            .unwrap();
        assert!(online.status().active_families <= 24);
    }
    let ids: Vec<_> = online
        .candidates_prehashed(ChunkId::of(&data), &data)
        .unwrap()
        .iter()
        .map(|c| c.chunk_id())
        .collect();
    assert_eq!(
        ids,
        (0..16_u8)
            .map(|n| ChunkId::from_bytes([n; 32]))
            .collect::<Vec<_>>()
    );
    assert!(online.status().compactions >= 20);
    OnlineSimilarityRepository::audit(&repository).unwrap();
    drop(online);
    storage.crash();
    let recovered = OnlineSimilarityRepository::open(repository.clone(), &exact).unwrap();
    assert_eq!(
        ids,
        recovered
            .candidates_prehashed(ChunkId::of(&data), &data)
            .unwrap()
            .iter()
            .map(|c| c.chunk_id())
            .collect::<Vec<_>>()
    );
    OnlineSimilarityRepository::audit(&repository).unwrap();
    // Live retirement removes obsolete physical families; heads protect their
    // immediate fallback state, not every historical compaction generation.
    assert!(
        storage
            .list_names()
            .unwrap()
            .iter()
            .filter(|n| n.starts_with("similarity-family."))
            .count()
            < 20
    );
}

#[test]
fn filesystem_compaction_preserves_a_leased_old_mapping_until_release() {
    use fastdup_store::FsStorageIo;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.artifacts/tmp")
        .join(format!(
            "online-similarity-lease-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let storage = FsStorageIo::open(root.join("similarity")).unwrap();
    let exact = ExactIndexRunRepository::new(FsStorageIo::open(root.join("exact")).unwrap());
    let location =
        ExactIndexLocation::raw(ContainerId::new([1; 16]).unwrap(), 1, 4096, 65728, 1).unwrap();
    exact
        .append_level_zero(
            ExactIndexProfileId::new([1; 32]).unwrap(),
            vec![ExactIndexEntry::active(ChunkId::of(b"anchor"), 65536, location).unwrap()],
        )
        .unwrap();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let online = OnlineSimilarityRepository::open(repository.clone(), &exact).unwrap();
    online.append_current(&entries(0..1)).unwrap();
    let old = repository.recover_latest().unwrap().unwrap();
    let original_names = storage.list_names().unwrap();
    let partition = original_names
        .iter()
        .find(|n| n.starts_with("similarity-part."))
        .unwrap();
    for n in 1..6_u8 {
        online.append_current(&entries(n..n + 1)).unwrap();
    }
    assert!(storage.exists(partition).unwrap());
    assert_eq!(old.candidates(&target()).unwrap().len(), 1);
    assert_eq!(
        storage.remove_file(partition).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    drop(old);
    online.append_current(&entries(6..7)).unwrap();
    assert!(!storage.exists(partition).unwrap());
    OnlineSimilarityRepository::audit(&repository).unwrap();
}

fn publish_four(storage: &MemoryStorageIo) -> (usize, usize) {
    let exact = exact();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let Ok(online) = OnlineSimilarityRepository::open(repository, &exact) else {
        return (0, storage.operation_count());
    };
    let start = storage.operation_count();
    for n in 0..4_u8 {
        if online
            .append(&exact.pin_active_generation().unwrap(), &entries(n..n + 1))
            .is_err()
        {
            break;
        }
    }
    (start, storage.operation_count())
}

#[test]
fn every_publication_and_compaction_io_fault_recovers_a_complete_prefix() {
    let (start, end) = publish_four(&MemoryStorageIo::new());
    for after in [false, true] {
        for position in start..end {
            let storage = if after {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            publish_four(&storage);
            storage.crash();
            let repository = SimilarityIndexRepository::new(storage);
            let recovered = OnlineSimilarityRepository::open(repository.clone(), &exact())
                .unwrap_or_else(|e| panic!("fault {position}, after={after}: {e}"));
            let data = target();
            let ids: Vec<_> = recovered
                .candidates_prehashed(ChunkId::of(&data), &data)
                .unwrap()
                .iter()
                .map(|c| c.chunk_id())
                .collect();
            assert!(ids.len() <= 4);
            assert_eq!(
                ids,
                (0..u8::try_from(ids.len()).unwrap())
                    .map(|n| ChunkId::from_bytes([n; 32]))
                    .collect::<Vec<_>>(),
                "fault {position}, after={after}"
            );
            OnlineSimilarityRepository::audit(&repository).unwrap();
        }
    }
}

#[test]
fn torn_new_head_keeps_the_previous_complete_generation() {
    let storage = MemoryStorageIo::new();
    let exact = exact();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let online = OnlineSimilarityRepository::open(repository.clone(), &exact).unwrap();
    online
        .append(&exact.pin_active_generation().unwrap(), &entries(0..1))
        .unwrap();
    online
        .append(&exact.pin_active_generation().unwrap(), &entries(1..2))
        .unwrap();
    drop(online);
    storage
        .inject_durable_torn_write("reduction-head.1.fds", 128)
        .unwrap();
    storage.crash();
    let recovered = OnlineSimilarityRepository::open(repository, &exact).unwrap();
    let data = target();
    assert_eq!(
        recovered
            .candidates_prehashed(ChunkId::of(&data), &data)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn corrupt_selected_partition_fails_recovery_and_scrub() {
    let storage = MemoryStorageIo::new();
    let exact = exact();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let online = OnlineSimilarityRepository::open(repository.clone(), &exact).unwrap();
    online
        .append(&exact.pin_active_generation().unwrap(), &entries(0..1))
        .unwrap();
    drop(online);
    let name = storage
        .list_names()
        .unwrap()
        .into_iter()
        .find(|n| n.starts_with("similarity-part."))
        .unwrap();
    storage.write_at(&name, 4200, &[0xff; 32]).unwrap();
    assert!(OnlineSimilarityRepository::open(repository.clone(), &exact).is_err());
    assert!(OnlineSimilarityRepository::audit(&repository).is_err());
}
