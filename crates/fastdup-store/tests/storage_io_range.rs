use std::path::{Path, PathBuf};

use fastdup_store::{FsStorageIo, MAX_STORAGE_RANGE_BYTES, StorageIo};

fn test_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("storage-range-{}", std::process::id()))
}

#[test]
fn filesystem_adapter_reads_only_one_bounded_exact_range() {
    let root = test_root();
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let storage = FsStorageIo::open(&root).expect("open workspace-local storage adapter");
    let bytes = (0_u16..8_192)
        .map(|value| value.to_le_bytes()[0])
        .collect::<Vec<_>>();
    storage.create_new("range.fixture").expect("create fixture");
    storage
        .write_at("range.fixture", 0, &bytes)
        .expect("write fixture");
    storage
        .set_len("range.fixture", 8_192)
        .expect("finalize fixture length");

    assert_eq!(
        storage
            .object_len("range.fixture")
            .expect("fixture metadata is readable"),
        8_192
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 4_090, 32)
            .expect("in-range exact read succeeds"),
        bytes[4_090..4_122]
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 8_190, 4)
            .expect_err("short reads must fail instead of returning partial evidence")
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 0, MAX_STORAGE_RANGE_BYTES + 1)
            .expect_err("adapter must reject an attacker-sized allocation before reading")
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}
