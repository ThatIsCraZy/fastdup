use fastdup_format::{
    ContainerId, ExactIndexEntry, ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef,
    ExactIndexRunSet,
};
use fastdup_store::{ContainerRepository, ExactIndexRunRepository, StorageIo, StoreError};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

#[test]
fn container_generation_discovery_reads_only_paired_envelopes() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let first_id = ContainerId::new([0xA2; 16]).expect("container identity is nonzero");
    let second_id = ContainerId::new([0xA3; 16]).expect("container identity is nonzero");
    let payload = vec![0x5A; 256 * 1_024];
    repository
        .publish_raw(first_id, 41, &[&payload])
        .expect("publish first Container");
    repository
        .publish_raw(second_id, 43, &[&payload])
        .expect("publish second Container");
    let baseline = storage.operation_count();

    let high_water = repository
        .discover_container_generation_high_water()
        .expect("paired Container envelopes establish the allocator high-water");

    assert_eq!(high_water, Some(43));
    let operations = &storage.operations()[baseline..];
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ListNames)
            .count(),
        1
    );
    assert!(!operations.contains(&StorageOperation::Read));
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ObjectLen)
            .count(),
        2
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        4
    );

    let second_name = format!("{}.fdc", "a3".repeat(16));
    let second_length = storage
        .object_len(&second_name)
        .expect("published Container length exists");
    storage
        .write_at(&second_name, second_length - 4_096 + 100, &[0xFF])
        .expect("inject Footer corruption");
    assert!(matches!(
        repository.discover_container_generation_high_water(),
        Err(StoreError::Format(_))
    ));
}

#[test]
fn exact_location_read_uses_only_bounded_ranges_and_returns_verified_bytes() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xA4; 16]).expect("container identity is nonzero");
    let first = vec![0x11; 256 * 1_024];
    let requested = b"bounded reads still verify exact bytes";
    let third = vec![0x33; 256 * 1_024];
    repository
        .publish_raw(container_id, 9, &[&first, requested, &third])
        .expect("publish one worked multi-record Container");
    let container = repository
        .read(container_id)
        .expect("obtain rebuild evidence before measuring the demand read");
    let candidate = ExactIndexEntry::from_verified_raw(container.raw_locations()[1])
        .expect("construct one index candidate from verified rebuild evidence");
    let baseline = storage.operation_count();

    let bytes = repository
        .read_verified_location(candidate)
        .expect("bounded demand verification succeeds");

    assert_eq!(bytes, requested);
    let operations = &storage.operations()[baseline..];
    assert!(!operations.contains(&StorageOperation::Read));
    assert!(!operations.contains(&StorageOperation::ListNames));
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ObjectLen)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        3
    );
}

#[test]
fn bounded_location_read_rejects_record_corruption_without_returning_bytes() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xA5; 16]).expect("container identity is nonzero");
    let payload = b"persistent corruption must fail closed";
    repository
        .publish_raw(container_id, 10, &[payload])
        .expect("publish one worked Container");
    let container = repository
        .read(container_id)
        .expect("obtain rebuild evidence before injecting corruption");
    let candidate = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one index candidate from verified rebuild evidence");
    let location = candidate.location();
    let published_name = format!("{}.fdc", "a5".repeat(16));
    storage
        .write_at(&published_name, location.record_offset() + 192, &[0xFF])
        .expect("inject one live payload corruption through the storage seam");

    let error = repository
        .read_verified_location(candidate)
        .expect_err("corrupt stored bytes must never be returned");
    assert!(matches!(error, StoreError::Format(_)));
}

#[test]
fn active_exact_index_resolves_a_chunk_without_a_container_directory_scan() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xA6; 16]).expect("container identity is nonzero");
    let payload = b"active persistent index to bounded verified bytes";
    containers
        .publish_raw(container_id, 11, &[payload])
        .expect("publish one worked Container");
    let container = containers
        .read(container_id)
        .expect("obtain full rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one index entry from rebuild evidence");

    let profile = ExactIndexProfileId::new([0xA7; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new(storage.clone());
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
    let baseline = storage.operation_count();

    let bytes = containers
        .read_verified_chunk_with_index(
            &active,
            entry.chunk_id(),
            u64::from(entry.logical_length()),
        )
        .expect("index lookup and bounded Container verification succeed");

    assert_eq!(bytes, payload);
    let operations = &storage.operations()[baseline..];
    assert!(!operations.contains(&StorageOperation::Read));
    assert!(!operations.contains(&StorageOperation::ListNames));
}

#[test]
fn corrupt_exact_index_page_cannot_make_verified_container_data_unreadable() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xA8; 16]).expect("container identity is nonzero");
    let payload = b"the index is acceleration and never content authority";
    containers
        .publish_raw(container_id, 13, &[payload])
        .expect("publish one worked Container");
    let container = containers
        .read(container_id)
        .expect("obtain complete rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one index entry from rebuild evidence");

    let profile = ExactIndexProfileId::new([0xA9; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new(storage.clone());
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
    let run_name = format!("{}.{:016x}.fdx", "a9".repeat(32), 1);
    storage
        .write_at(&run_name, 4_096 + 128, &[0xFF])
        .expect("inject one live page corruption after activation");

    let bytes = containers
        .read_verified_chunk_with_index(
            &active,
            entry.chunk_id(),
            u64::from(entry.logical_length()),
        )
        .expect("verified slow path preserves readable committed content");

    assert_eq!(bytes, payload);
}
