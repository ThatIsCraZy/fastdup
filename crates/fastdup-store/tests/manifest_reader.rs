use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{ChunkId, ContainerId, ManifestExtent, ManifestLeaf};
use fastdup_store::{
    ContainerRepository, FsStorageIo, ManifestReadError, StoreError, VerifiedManifestFile,
};

fn unique_test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

#[test]
fn huge_fill_hole_and_verified_data_are_read_byte_exactly_without_file_materialization() {
    let root = unique_test_root("manifest-reader");
    let containers = ContainerRepository::new(
        FsStorageIo::open(&root).expect("create workspace-local container repository"),
    );
    let payload = b"abcdefgh";
    containers
        .publish_raw(
            ContainerId::new([0x81; 16]).expect("container identity is nonzero"),
            1,
            &[payload.as_slice()],
        )
        .expect("publish DATA location");

    let fill_length = 1_u64 << 40;
    let manifest = ManifestLeaf::new(
        fill_length + payload.len() as u64 + 9,
        vec![
            ManifestExtent::Fill {
                logical_length: fill_length,
                value: b'a',
            },
            ManifestExtent::Data {
                logical_length: payload.len() as u64,
                chunk_id: ChunkId::of(payload),
            },
            ManifestExtent::Hole { logical_length: 9 },
        ],
    )
    .expect("mixed manifest is valid");
    let file = VerifiedManifestFile::new(manifest, containers)
        .expect("all DATA dependencies verify before reads");

    let bytes = file
        .read_at(fill_length - 2, 14)
        .expect("bounded read crosses FILL, DATA, and HOLE");
    assert_eq!(bytes, b"aaabcdefgh\0\0\0\0");
    assert_eq!(file.logical_size(), fill_length + 17);
    assert_eq!(file.read_at(file.logical_size(), 1).unwrap(), b"");

    let container_path = root.join(format!("{}.fdc", "81".repeat(16)));
    let mut container_bytes = std::fs::read(&container_path).expect("read bounded test container");
    container_bytes[4_288] ^= 1;
    std::fs::write(&container_path, container_bytes).expect("inject durable test corruption");
    assert!(matches!(
        file.read_at(
            fill_length,
            u32::try_from(payload.len()).expect("test payload length fits u32"),
        ),
        Err(ManifestReadError::Store(StoreError::Format(_)))
    ));
}
