//! Storage-operation budget of Metadata publication, reads and reclamation.
use fastdup_format::{
    ExactIndexProfileId, ManifestExtent, ManifestLeaf, NamespaceRoot, PolicySetId,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationRepository, MaintenanceRepository,
    ReadIntent, ReadIntentScope, StorageIo,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const WINDOW: u64 = 64 * 1_024 * 1_024;

fn repository(storage: MemoryStorageIo) -> GenerationRepository<MemoryStorageIo> {
    GenerationRepository::new(
        storage,
        PolicySetId::new([0x71; 32]).expect("fixture policy set"),
    )
}

fn layout(leaves: u8) -> ManifestLeaf {
    ManifestLeaf::new(
        WINDOW * u64::from(leaves),
        (0..leaves)
            .map(|ordinal| ManifestExtent::Fill {
                logical_length: WINDOW,
                value: ordinal,
            })
            .collect(),
    )
    .expect("bounded Fill windows are a valid layout")
}

fn count(operations: &[StorageOperation], wanted: StorageOperation) -> usize {
    operations
        .iter()
        .filter(|operation| **operation == wanted)
        .count()
}

fn metadata_object_count(storage: &MemoryStorageIo) -> usize {
    storage
        .list_names()
        .expect("list the Metadata pool")
        .iter()
        .filter(|name| name.ends_with(".fdm"))
        .count()
}

/// Restaging an object that is already published must not reread its payload.
/// The name is that payload's content address and only this appliance writes the
/// pool, so a full reread would reprove what the durable header already states.
#[test]
fn restaging_a_published_tree_reads_only_object_headers() {
    let storage = MemoryStorageIo::new();
    let repository = repository(storage.clone());
    let layout = layout(33);
    repository
        .publish_manifest(&layout)
        .expect("publish one multi-level Manifest tree");
    let objects = metadata_object_count(&storage);
    assert!(objects > 32, "fixture must publish many Metadata Objects");
    let baseline = storage.operation_count();

    repository
        .publish_manifest(&layout)
        .expect("republish the identical tree");

    let operations = &storage.operations()[baseline..];
    assert_eq!(count(operations, StorageOperation::Read), 0);
    assert_eq!(count(operations, StorageOperation::CreateNew), 0);
    assert_eq!(count(operations, StorageOperation::PublishNoreplace), 0);
    assert_eq!(count(operations, StorageOperation::Exists), objects);
    assert_eq!(count(operations, StorageOperation::ReadExactAt), objects);
}

/// Damage to the durable header of a published object must still fail the
/// restage. Only the payload below that header is left to scrub.
#[test]
fn restaging_rejects_a_damaged_published_header() {
    let storage = MemoryStorageIo::new();
    let repository = repository(storage.clone());
    let layout = layout(1);
    repository
        .publish_manifest(&layout)
        .expect("publish one Manifest Object");
    let name = storage
        .list_names()
        .expect("list the Metadata pool")
        .into_iter()
        .find(|name| name.ends_with(".fdm"))
        .expect("the fixture published one Metadata Object");
    storage
        .write_at(&name, 0, &[0x00])
        .expect("damage the durable header");

    assert!(
        repository.publish_manifest(&layout).is_err(),
        "a damaged durable header must not be accepted as this encoding"
    );
}

/// A cold Metadata read costs exactly one storage operation. Measuring the
/// object first only repeats what the read itself reports.
#[test]
fn a_cold_metadata_read_costs_one_storage_operation() {
    let storage = MemoryStorageIo::new();
    let repository = repository(storage.clone());
    let root = repository
        .publish_manifest(&layout(1))
        .expect("publish one Manifest Object");
    let baseline = storage.operation_count();

    {
        let _cold = ReadIntentScope::enter(ReadIntent::Independent);
        repository
            .read_manifest(root)
            .expect("read the published Manifest without any cache");
    }

    let operations = &storage.operations()[baseline..];
    assert_eq!(count(operations, StorageOperation::Read), 1);
    assert_eq!(count(operations, StorageOperation::ObjectLen), 0);
}

/// Reclaiming Metadata only needs the size of each unreachable object. Reading
/// them back would reprove the content addressing of bytes that are about to be
/// unlinked, so the read cost of a collection must not grow with its candidates.
#[test]
fn metadata_reclamation_sizes_candidates_without_reading_them() {
    let mut measurements = Vec::new();
    for orphans in [1_u8, 9] {
        let storage = MemoryStorageIo::new();
        let generations = repository(storage.clone());
        generations
            .commit_namespace(
                &NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
                    .expect("empty reservation generation"),
            )
            .expect("commit the fixture generation");
        for ordinal in 0..orphans {
            generations
                .publish_manifest(
                    &ManifestLeaf::new(
                        4_096,
                        vec![ManifestExtent::Fill {
                            logical_length: 4_096,
                            value: ordinal,
                        }],
                    )
                    .expect("orphan Manifest is valid"),
                )
                .expect("publish an unreachable Metadata Object");
        }
        let maintenance = MaintenanceRepository::new(
            generations,
            ContainerRepository::new(MemoryStorageIo::new()),
            ExactIndexRunRepository::new(MemoryStorageIo::new()),
            ExactIndexProfileId::new([0x72; 32]).expect("fixture profile"),
        );
        let baseline = storage.operation_count();
        let report = maintenance
            .garbage_collect_metadata()
            .expect("collect unreachable Metadata");
        assert_eq!(u64::from(orphans), report.objects_removed());
        let operations = &storage.operations()[baseline..];
        assert!(
            count(operations, StorageOperation::ObjectLen) >= usize::from(orphans),
            "every candidate is sized before it is unlinked"
        );
        measurements.push((
            count(operations, StorageOperation::Read),
            count(operations, StorageOperation::RemoveFile),
        ));
    }

    // The mark phase reads the committed graph and may reuse process-local
    // residency across both fixtures, so only its direction is asserted: a
    // collection with eight more candidates must not read more.
    assert!(
        measurements[1].0 <= measurements[0].0,
        "eight more candidates cost {} more Metadata reads",
        measurements[1].0 - measurements[0].0
    );
    assert_eq!(measurements[1].1 - measurements[0].1, 8);
}
