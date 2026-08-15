use std::path::{Path, PathBuf};

use fastdup_format::{ChunkId, ContainerId, MAX_CONTAINER_BYTES};
use fastdup_store::{ContainerStore, StoreError};

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
