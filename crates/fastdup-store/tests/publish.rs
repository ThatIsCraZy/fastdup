use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use fastdup_format::{ChunkId, ContainerId, FormatError, MAX_CONTAINER_BYTES, PrehashedChunk};
use fastdup_store::{ContainerRepository, ContainerStore, FsStorageIo, StoreError};

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}

#[test]
fn published_container_is_immediately_reopenable_by_id() {
    let root = test_root("publish-reopen");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    let id = ContainerId::new([0x91; 16]).expect("nonzero container id");

    store
        .publish_raw(
            id,
            1,
            &[b"first chunk".as_slice(), b"second chunk".as_slice()],
        )
        .expect("publish durable container");

    let reopened = store.read(id).expect("reopen published container");
    assert_eq!(reopened.header().container_id(), id);
    assert_eq!(reopened.header().container_generation(), 1);
    assert_eq!(
        reopened.chunk(ChunkId::of(b"second chunk")),
        Some(b"second chunk".as_slice())
    );
    assert_eq!(
        std::fs::read_dir(&root)
            .expect("list test store")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("building"))
            .count(),
        0
    );
}

#[test]
fn wrong_prehashed_identity_never_becomes_a_published_container() {
    let root = test_root("publish-wrong-prehash");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let repository = ContainerRepository::new(
        FsStorageIo::open(&root).expect("create workspace-local repository"),
    );
    let id = ContainerId::new([0x9A; 16]).expect("nonzero container id");
    let payload = vec![b'R'; 192 * 1_024];
    let chunk = PrehashedChunk::new(ChunkId::from_bytes([0x66; 32]), &payload);
    let region = [chunk];
    let prepared = ContainerRepository::<FsStorageIo>::prepare_prehashed_adaptive_regions_parallel(
        id,
        1,
        &[&region],
        NonZeroUsize::MIN,
    )
    .expect("construct non-authoritative writer image");

    assert!(matches!(
        repository.publish_prepared_adaptive_profiled(prepared),
        Err(StoreError::Format(FormatError::ChunkHashMismatch))
    ));
    match repository.read(id) {
        Err(StoreError::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
        result => panic!("invalid writer evidence became visible: {result:?}"),
    }

    std::fs::remove_dir_all(&root).expect("remove only this test repository");
}

#[test]
fn oversized_published_file_is_rejected_before_it_is_read_into_memory() {
    let root = test_root("oversized-published");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    let id = ContainerId::new([0x92; 16]).expect("nonzero container id");
    let path = root.join(format!("{}.fdc", "92".repeat(16)));
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .expect("create sparse oversized published fixture");
    file.set_len(MAX_CONTAINER_BYTES + 4_096)
        .expect("extend sparse fixture");

    match store.read(id) {
        Err(StoreError::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidData),
        result => panic!("oversized file reached the format decoder: {result:?}"),
    }
}

#[test]
fn publishing_an_existing_id_never_replaces_the_first_container() {
    let root = test_root("publish-no-replace");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    let id = ContainerId::new([0x93; 16]).expect("nonzero container id");
    store
        .publish_raw(id, 1, &[b"original".as_slice()])
        .expect("publish original container");

    match store.publish_raw(id, 2, &[b"replacement".as_slice()]) {
        Err(StoreError::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists),
        result => panic!("duplicate publication did not fail with AlreadyExists: {result:?}"),
    }

    let reopened = store.read(id).expect("original remains readable");
    assert_eq!(reopened.header().container_generation(), 1);
    assert_eq!(
        reopened.chunk(ChunkId::of(b"original")),
        Some(b"original".as_slice())
    );
    assert_eq!(reopened.chunk(ChunkId::of(b"replacement")), None);
}

#[test]
fn adaptive_region_publication_uses_zstd_only_when_the_complete_record_wins() {
    let root = test_root("publish-adaptive-region");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let repository = ContainerRepository::new(
        FsStorageIo::open(&root).expect("create workspace-local repository"),
    );
    let compressible_a = vec![b'A'; 256 * 1_024];
    let compressible_b = vec![b'B'; 256 * 1_024];
    let region = [compressible_a.as_slice(), compressible_b.as_slice()];
    let id = ContainerId::new([0xD1; 16]).expect("container identity is nonzero");

    let (_published, metrics) = repository
        .publish_adaptive_regions_parallel_profiled(id, 1, &[&region], NonZeroUsize::MIN)
        .expect("publish one adaptive Compression Region durably");
    assert_eq!(metrics.incompressibility_gate().disabled_regions(), 1);
    assert_eq!(metrics.incompressibility_gate().eligible_regions(), 0);
    let reopened = repository
        .read(id)
        .expect("reopen through the production Container verifier");

    assert_eq!(reopened.zstd_record_count(), 1);
    assert_eq!(reopened.raw_record_count(), 0);
    assert_eq!(reopened.chunk_count(), 2);
    assert_eq!(
        reopened.chunk(ChunkId::of(&compressible_a)),
        Some(compressible_a.as_slice())
    );
    assert_eq!(
        reopened.chunk(ChunkId::of(&compressible_b)),
        Some(compressible_b.as_slice())
    );
}

#[test]
fn adaptive_region_publication_retains_raw_for_incompressible_bytes() {
    let root = test_root("publish-adaptive-incompressible");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    let mut state = 0xD1B5_4A32_D192_ED03_u64;
    let mut bytes = vec![0_u8; 512 * 1_024];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    let region = [&bytes[..256 * 1_024], &bytes[256 * 1_024..]];
    let id = ContainerId::new([0xD2; 16]).expect("container identity is nonzero");

    store
        .publish_adaptive_regions(id, 1, &[&region])
        .expect("publish the incompressible region through the adaptive writer");
    let reopened = store
        .read(id)
        .expect("reopen through the production Container verifier");

    assert_eq!(reopened.zstd_record_count(), 0);
    assert_eq!(reopened.raw_record_count(), 2);
    assert_eq!(reopened.chunk_count(), 2);
}
