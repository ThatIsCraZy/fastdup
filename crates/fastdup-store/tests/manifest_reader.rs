use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexProfileId, ManifestExtent, ManifestLeaf,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, ManifestReadError, StoreError,
    VerifiedManifestFile,
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

#[test]
fn a_long_lived_manifest_reader_pins_exact_only_for_each_bounded_read() {
    let root = unique_test_root("manifest-reader-exact-pin-drain");
    let storage = FsStorageIo::open(&root).expect("open shared test repository");
    let containers = ContainerRepository::new(storage.clone());
    let first_id = ContainerId::new([0x82; 16]).expect("container identity is nonzero");
    let first_payload = b"a manifest reader may outlive its Exact generation";
    containers
        .publish_raw(first_id, 1, &[first_payload])
        .expect("publish the manifest DATA");
    let first = containers
        .read(first_id)
        .expect("recover the first verified Container");
    let first_entry = ExactIndexEntry::from_verified_raw(first.raw_locations()[0])
        .expect("derive the first Exact entry from verified evidence");

    let indexes = ExactIndexRunRepository::new(storage);
    let profile = ExactIndexProfileId::new([0x83; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, vec![first_entry])
        .expect("activate the first Exact generation");
    let active = indexes
        .pin_active_generation()
        .expect("pin the first Exact generation");
    let manifest = ManifestLeaf::new(
        first_payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: first_payload.len() as u64,
            chunk_id: ChunkId::of(first_payload),
        }],
    )
    .expect("one-extent manifest is valid");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify the manifest DATA")
        .with_active_index(&active);
    drop(active);

    let second_id = ContainerId::new([0x84; 16]).expect("container identity is nonzero");
    let second_payload = b"an unrelated Exact location advances the generation";
    containers
        .publish_raw(second_id, 2, &[second_payload])
        .expect("publish the second DATA Container");
    let second = containers
        .read(second_id)
        .expect("recover the second verified Container");
    let second_entry = ExactIndexEntry::from_verified_raw(second.raw_locations()[0])
        .expect("derive the second Exact entry from verified evidence");
    let transition = indexes
        .append_level_zero(profile, vec![second_entry])
        .expect("activate the successor Exact generation");
    let drain = transition
        .into_retired()
        .expect("the prior generation needs a drain token");

    assert!(
        drain.is_drained(),
        "a dormant Manifest reader cannot retain a generation pin"
    );
    assert_eq!(
        file.read_at(0, u32::try_from(first_payload.len()).unwrap())
            .expect("a post-retirement read falls back to verified Container discovery"),
        first_payload
    );
}
