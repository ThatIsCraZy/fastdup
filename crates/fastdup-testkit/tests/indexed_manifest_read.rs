use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef,
    ExactIndexRunSet, ManifestExtent, ManifestLeaf,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, IndexedRequiredChunkVerifier,
    MemoryPressureSnapshot, RequiredChunkVerifier, StorageIo, VerifiedManifestFile,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

#[test]
fn manifest_data_extent_uses_the_active_index_and_bounded_container_reader() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let payload = b"manifest read through persistent exact index";
    let container_id = ContainerId::new([0xB1; 16]).expect("container identity is nonzero");
    containers
        .publish_raw(container_id, 12, &[payload])
        .expect("publish one worked Container");
    let container = containers
        .read(container_id)
        .expect("obtain complete rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one Exact Index entry");

    let profile = ExactIndexProfileId::new([0xB2; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0),
    );
    let descriptor = indexes
        .publish(
            &ExactIndexRun::new(profile, 1, vec![entry])
                .expect("construct one immutable sorted Run"),
        )
        .expect("publish the Run");
    let run_set = ExactIndexRunSet::new(
        profile,
        1,
        vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
    )
    .expect("construct one immutable Run Set");
    indexes.activate(&run_set).expect("activate the Run Set");
    let active = indexes
        .recover_active()
        .expect("recover the complete index graph")
        .expect("one Run Set is active");

    let payload_length = u64::try_from(payload.len()).expect("worked payload length fits u64");
    let manifest = ManifestLeaf::new(
        payload_length,
        vec![ManifestExtent::Data {
            logical_length: payload_length,
            chunk_id: ChunkId::of(payload),
        }],
    )
    .expect("construct one complete Manifest");
    let file = VerifiedManifestFile::new(manifest, containers)
        .expect("verify Manifest dependencies before demand reads");
    let baseline = storage.operation_count();

    let bytes = file
        .read_at_with_index(
            &active,
            0,
            u32::try_from(payload.len()).expect("worked payload length fits u32"),
        )
        .expect("read the Manifest extent through the active index");

    assert_eq!(bytes, payload);
    let operations = &storage.operations()[baseline..];
    assert!(!operations.contains(&StorageOperation::Read));
    assert!(!operations.contains(&StorageOperation::ListNames));
}

#[test]
fn indexed_graph_verification_is_bounded_and_corruption_falls_back_to_one_complete_scan() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let payload = b"complete graph proof through one pinned exact location";
    let container_id = ContainerId::new([0xB3; 16]).expect("container identity is nonzero");
    containers
        .publish_raw(container_id, 13, &[payload])
        .expect("publish one worked Container");
    let container = containers
        .read(container_id)
        .expect("obtain complete rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one Exact Index entry");

    let profile = ExactIndexProfileId::new([0xB4; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 1),
    );
    let descriptor = indexes
        .publish(
            &ExactIndexRun::new(profile, 1, vec![entry])
                .expect("construct one immutable sorted Run"),
        )
        .expect("publish the Run");
    indexes
        .activate(
            &ExactIndexRunSet::new(
                profile,
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
            )
            .expect("construct one immutable Run Set"),
        )
        .expect("activate the Run Set");
    let active = Arc::new(
        indexes
            .recover_active()
            .expect("recover the complete index graph")
            .expect("one Run Set is active"),
    );
    let verifier = IndexedRequiredChunkVerifier::new(containers, active);
    let required = BTreeMap::from([(entry.chunk_id(), u64::from(entry.logical_length()))]);

    let healthy_baseline = storage.operation_count();
    verifier
        .verify_required_chunks(&required)
        .expect("bounded index and Container verification prove the graph");
    let healthy_operations = &storage.operations()[healthy_baseline..];
    assert!(!healthy_operations.contains(&StorageOperation::Read));
    assert!(!healthy_operations.contains(&StorageOperation::ListNames));

    let run_name = format!("{}.{:016x}.fdx", "b4".repeat(32), 1);
    storage
        .write_at(&run_name, 4_096 + 128, &[0xFF])
        .expect("inject one live Exact-Index page corruption");
    let corrupt_baseline = storage.operation_count();
    verifier
        .verify_required_chunks(&required)
        .expect("the verified Container scan remains correctness authority");
    let corrupt_operations = &storage.operations()[corrupt_baseline..];
    assert!(corrupt_operations.contains(&StorageOperation::Read));
    assert!(corrupt_operations.contains(&StorageOperation::ListNames));
}
use std::collections::BTreeMap;
use std::sync::Arc;
