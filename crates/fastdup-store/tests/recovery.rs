use std::path::{Path, PathBuf};

use fastdup_format::{ChunkId, ContainerId};
use fastdup_store::{ContainerStore, StoreError};

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}

fn id(byte: u8) -> ContainerId {
    ContainerId::new([byte; 16]).expect("fixture ID is nonzero")
}

fn encoded_id(byte: u8) -> String {
    format!("{byte:02x}").repeat(16)
}

#[test]
fn recovery_discovers_verified_containers_in_id_order_and_ignores_temporary_files() {
    let root = test_root("recover-published");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    store
        .publish_raw(id(0xb2), 2, &[b"second".as_slice()])
        .expect("publish second ID first");
    store
        .publish_raw(id(0xa1), 1, &[b"first".as_slice()])
        .expect("publish first ID second");
    std::fs::write(
        root.join(format!(".{}.building", encoded_id(0xc3))),
        b"torn",
    )
    .expect("create ignored crash remnant");
    std::fs::write(root.join("operator-note"), b"not a container").expect("create unrelated file");

    let recovered = store
        .recover_published()
        .expect("discover and fully verify published containers");

    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0].header().container_id(), id(0xa1));
    assert_eq!(recovered[1].header().container_id(), id(0xb2));
    assert_eq!(
        recovered[0].chunk(ChunkId::of(b"first")),
        Some(b"first".as_slice())
    );
    assert_eq!(
        recovered[1].chunk(ChunkId::of(b"second")),
        Some(b"second".as_slice())
    );
}

#[test]
fn verification_returns_compact_metadata_without_retaining_container_payloads() {
    let root = test_root("verify-published");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    store
        .publish_raw(id(0xb2), 22, &[b"second".as_slice()])
        .expect("publish second ID first");
    store
        .publish_raw(id(0xa1), 21, &[b"first".as_slice()])
        .expect("publish first ID second");

    let verified = store
        .verify_published()
        .expect("verify without retaining decoded payloads");

    assert_eq!(verified.len(), 2);
    assert_eq!(verified[0].container_id(), id(0xa1));
    assert_eq!(verified[0].container_generation(), 21);
    assert_eq!(verified[0].chunk_count(), 1);
    assert_eq!(verified[0].raw_record_count(), 1);
    assert_eq!(verified[0].zstd_record_count(), 0);
    assert_eq!(verified[0].file_length(), 12_288);
    assert_eq!(verified[1].container_id(), id(0xb2));
    assert_eq!(verified[1].container_generation(), 22);
}

#[test]
fn recovery_rejects_a_filename_that_disagrees_with_the_embedded_id() {
    let root = test_root("recover-id-mismatch");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    store
        .publish_raw(id(0xd4), 4, &[b"identity-bound".as_slice()])
        .expect("publish fixture");
    std::fs::rename(
        root.join(format!("{}.fdc", encoded_id(0xd4))),
        root.join(format!("{}.fdc", encoded_id(0xe5))),
    )
    .expect("simulate namespace corruption");

    assert!(matches!(
        store.read(id(0xe5)),
        Err(StoreError::PublishedIdentityMismatch { .. })
    ));
    assert!(matches!(
        store.recover_published(),
        Err(StoreError::PublishedIdentityMismatch { .. })
    ));
}

#[test]
fn recovery_rejects_a_malformed_published_name() {
    let root = test_root("recover-malformed-name");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let store = ContainerStore::open(&root).expect("create workspace-local store");
    std::fs::write(root.join("not-hex.fdc"), b"not a container")
        .expect("create malformed published entry");

    assert!(matches!(
        store.recover_published(),
        Err(StoreError::InvalidPublishedName(_))
    ));
}
