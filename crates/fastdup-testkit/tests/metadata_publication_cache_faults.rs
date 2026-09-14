use fastdup_format::{ManifestExtent, ManifestLeaf, MetadataObjectId, PolicySetId};
use fastdup_store::{GenerationRepository, ReadIntent, ReadIntentScope};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

fn fixture() -> (ManifestLeaf, u64) {
    let logical_size = 4_096 + u64::from(std::process::id());
    let manifest = ManifestLeaf::new(
        logical_size,
        vec![ManifestExtent::Fill {
            logical_length: logical_size,
            value: 0xE7,
        }],
    )
    .expect("fixture manifest is valid");
    (manifest, logical_size)
}

fn publication_operation_position() -> (ManifestLeaf, u64, MetadataObjectId, usize) {
    let (manifest, logical_size) = fixture();
    let object_id = MetadataObjectId::from_encoded(&manifest.encode().unwrap())
        .expect("fixture identity is valid");
    let storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xD1; 32]).expect("policy identity is nonzero");
    let position = {
        // Keep the probe from leaving a shared cache entry that could hide a
        // backend read in the fault cases below.
        let _independent = ReadIntentScope::enter(ReadIntent::Independent);
        let repository = GenerationRepository::new(storage.clone(), policy);
        repository
            .publish_manifest(&manifest)
            .expect("probe publication succeeds");
        storage
            .operations()
            .iter()
            .position(|operation| *operation == StorageOperation::PublishNoreplace)
            .expect("probe contains one metadata publication")
    };
    (manifest, logical_size, object_id, position)
}

#[test]
fn metadata_publication_failures_do_not_admit_unsuccessful_images() {
    let (manifest, logical_size, object_id, publication_position) =
        publication_operation_position();
    let policy = PolicySetId::new([0xD1; 32]).expect("policy identity is nonzero");

    let before = MemoryStorageIo::with_fail_before(publication_position);
    let before_repository = GenerationRepository::new(before.clone(), policy);
    before_repository
        .publish_manifest(&manifest)
        .expect_err("failure before rename must fail publication");
    assert!(
        before_repository
            .read_manifest_range(object_id, logical_size, 0..logical_size)
            .is_err(),
        "an unsuccessful rename must not leave a readable cache admission"
    );

    let after = MemoryStorageIo::with_fail_after(publication_position);
    let after_repository = GenerationRepository::new(after.clone(), policy);
    after_repository
        .publish_manifest(&manifest)
        .expect_err("failure after rename must remain ambiguous to the caller");
    let before_read = after.operation_count();
    after_repository
        .read_manifest_range(object_id, logical_size, 0..logical_size)
        .expect("the durable renamed image remains readable from storage");
    assert!(
        after.operations()[before_read..].contains(&StorageOperation::Read),
        "an ambiguous rename must not be satisfied by a cache admission"
    );
}
