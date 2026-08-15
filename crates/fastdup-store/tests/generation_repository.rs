use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    DurableInode, ManifestExtent, ManifestLeaf, NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_store::{FsStorageIo, GenerationRepository, WalTail};

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

fn root_for_manifest(
    manifest: fastdup_format::MetadataObjectId,
    logical_size: u64,
    mutation_sequence: u64,
) -> NamespaceRoot {
    NamespaceRoot::new(
        4_096,
        3,
        mutation_sequence,
        vec![
            DurableInode::new(
                2,
                0o640,
                1_000,
                1_001,
                2,
                mutation_sequence,
                logical_size,
                manifest,
            )
            .expect("regular inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, vec![b'v', b'm', b'-', 0xff])
                .expect("non-UTF-8 POSIX name is valid"),
            NamespaceEntry::new(1, 2, b"latest-backup".to_vec())
                .expect("second hardlink name is valid"),
        ],
    )
    .expect("namespace graph and link count agree")
}

#[test]
fn filesystem_repository_reopens_the_latest_complete_generation() {
    let root = unique_test_root("generation-reopen");
    let policy = PolicySetId::new([0x61; 32]).expect("policy identity is nonzero");
    let storage = FsStorageIo::open(&root).expect("create workspace-local repository");
    let repository = GenerationRepository::new(storage, policy);
    let reservation = NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
        .expect("empty reservation root is valid");
    assert_eq!(
        repository
            .commit_namespace(&reservation)
            .expect("reserve Inode IDs before visibility")
            .generation(),
        1
    );

    let first_manifest = ManifestLeaf::new(8, vec![ManifestExtent::Hole { logical_length: 8 }])
        .expect("hole manifest is valid");
    let first_manifest_id = repository
        .publish_manifest(&first_manifest)
        .expect("publish generation-one manifest");
    let first_record = repository
        .commit_namespace(&root_for_manifest(first_manifest_id, 8, 1))
        .expect("commit generation one");
    assert_eq!(first_record.generation(), 2);

    let second_manifest = ManifestLeaf::new(
        5,
        vec![ManifestExtent::Fill {
            logical_length: 5,
            value: 0xA5,
        }],
    )
    .expect("fill manifest is valid");
    let second_manifest_id = repository
        .publish_manifest(&second_manifest)
        .expect("publish generation-two manifest");
    let expected_root = root_for_manifest(second_manifest_id, 5, 3);
    let second_record = repository
        .commit_namespace(&expected_root)
        .expect("commit generation two");
    assert_eq!(second_record.generation(), 3);

    drop(repository);
    let reopened_repository = GenerationRepository::new(
        FsStorageIo::open(&root).expect("reopen workspace-local repository"),
        policy,
    );
    let recovered = reopened_repository
        .recover_latest()
        .expect("verify commit chain and reachable metadata")
        .expect("two generations were committed");

    assert_eq!(recovered.record(), second_record);
    assert_eq!(recovered.namespace_root(), &expected_root);
    assert_eq!(recovered.wal_tail(), &WalTail::Clean);
    assert_eq!(recovered.rejected_newer_generations(), 0);
    assert_eq!(
        reopened_repository
            .read_manifest(recovered.namespace_root().inodes()[0].manifest_root())
            .expect("mount path loads the verified committed Manifest"),
        second_manifest
    );
}
