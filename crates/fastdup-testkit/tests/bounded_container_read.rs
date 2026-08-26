use fastdup_format::{
    ContainerId, ExactIndexEntry, ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef,
    ExactIndexRunSet,
};
use fastdup_store::{
    CONTAINER_GENERATION_HIGH_WATER_SLOT_0, ContainerRepository, ExactIndexRunRepository,
    MemoryPressureSnapshot, StorageIo, StoreError,
};
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
fn durable_container_generation_high_water_reopens_without_a_directory_scan() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let allocator = repository
        .open_generation_allocator(4)
        .expect("initialize the durable generation allocator");
    let generation = allocator
        .reserve_generation()
        .expect("reserve the first generation from a durable range");
    repository
        .publish_raw(
            ContainerId::new([0xA7; 16]).expect("container identity is nonzero"),
            generation,
            &[b"durable generation reservation"],
        )
        .expect("publish a Container inside the reserved range");
    storage.crash();
    let baseline = storage.operation_count();

    let reopened = ContainerRepository::new(storage.clone())
        .open_generation_allocator(4)
        .expect("reopen from the durable high-water records");
    let next = reopened
        .reserve_generation()
        .expect("reserve beyond every pre-crash generation");

    assert!(next > generation);
    assert!(
        !storage.operations()[baseline..].contains(&StorageOperation::ListNames),
        "a healthy durable high-water must eliminate Container directory discovery"
    );
}

#[test]
fn offline_audit_rejects_a_corrupt_container_generation_high_water() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let allocator = repository
        .open_generation_allocator(4)
        .expect("initialize allocator records");
    let generation = allocator
        .reserve_generation()
        .expect("durably reserve one generation");
    assert_eq!(
        repository
            .audit_generation_high_water(Some(generation))
            .expect("healthy allocator covers every observed Container"),
        Some(4)
    );

    storage
        .write_at(CONTAINER_GENERATION_HIGH_WATER_SLOT_0, 100, &[0xFF])
        .expect("inject allocator-record corruption");
    assert!(matches!(
        repository.audit_generation_high_water(Some(generation)),
        Err(StoreError::ContainerGenerationHighWaterFormat(_))
    ));
}

#[test]
fn offline_audit_rejects_a_container_above_the_durable_generation_high_water() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let allocator = repository
        .open_generation_allocator(4)
        .expect("initialize allocator records");
    assert_eq!(
        allocator
            .reserve_generation()
            .expect("reserve the first generation"),
        1
    );
    repository
        .publish_raw(
            ContainerId::new([0xA8; 16]).expect("container identity is nonzero"),
            5,
            &[b"generation beyond the durable reservation"],
        )
        .expect("publish the inconsistent fixture Container");

    assert!(matches!(
        repository.audit_generation_high_water(Some(5)),
        Err(StoreError::ContainerGenerationHighWaterBehind {
            reserved_through: 4,
            observed: 5
        })
    ));
}

#[test]
fn frontend_and_maintenance_views_share_one_generation_allocator_state() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());
    let frontend = repository
        .open_generation_allocator(4)
        .expect("open frontend allocator");
    assert_eq!(frontend.reserve_generation().expect("frontend reserve"), 1);
    let baseline = storage.operation_count();

    let maintenance = repository
        .with_maintenance_storage(storage.clone())
        .open_generation_allocator(4)
        .expect("open maintenance allocator over the shared lifecycle");
    assert_eq!(
        maintenance
            .reserve_generation()
            .expect("maintenance reserve"),
        2
    );
    assert_eq!(frontend.reserve_generation().expect("frontend reserve"), 3);

    assert_eq!(frontend.durable_reserved_through(), 4);
    assert_eq!(maintenance.durable_reserved_through(), 4);
    assert_eq!(storage.operation_count(), baseline);
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
fn repeated_location_reads_reuse_the_verified_container_envelope() {
    let storage = MemoryStorageIo::new();
    let gib = 1_024_u64.pow(3);
    let repository = ContainerRepository::new_with_descriptor_cache_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 * gib, 96 * gib, 0),
    );
    let container_id = ContainerId::new([0xB4; 16]).expect("container identity is nonzero");
    let first = b"first record in one immutable container";
    let second = b"second record reuses the verified envelope";
    repository
        .publish_raw(container_id, 19, &[first, second])
        .expect("publish one worked Container");
    let container = repository
        .read(container_id)
        .expect("obtain rebuild evidence before measuring bounded reads");
    let first_entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct first exact location");
    let second_entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[1])
        .expect("construct second exact location");
    let baseline = storage.operation_count();

    assert_eq!(
        repository
            .read_verified_location(first_entry)
            .expect("first record verifies"),
        first
    );
    assert_eq!(
        repository
            .read_verified_location(second_entry)
            .expect("second record verifies"),
        second
    );

    let operations = &storage.operations()[baseline..];
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ObjectLen)
            .count(),
        1,
        "one immutable Container envelope must be measured only once"
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        4,
        "the cold read needs Header+Footer+Record; the hot read only its Record"
    );
    let cache = repository.descriptor_cache_status();
    assert_eq!(cache.misses(), 1);
    assert_eq!(cache.hits(), 1);
    assert_eq!(cache.evictions(), 0);
    assert!(
        cache.capacity() >= 16_777_216,
        "the descriptor cache must address at least 512 TiB of 32-MiB Containers"
    );
    assert!(cache.metadata_bytes() < 1_024 * 1_024);
}

#[test]
fn swap_pressure_disables_envelope_admission_without_changing_verified_reads() {
    let storage = MemoryStorageIo::new();
    let gib = 1_024_u64.pow(3);
    let repository = ContainerRepository::new_with_descriptor_cache_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 * gib, 96 * gib, 1),
    );
    let container_id = ContainerId::new([0xB5; 16]).expect("container identity is nonzero");
    let payload = b"cache pressure may cost IO but cannot change verified bytes";
    repository
        .publish_raw(container_id, 20, &[payload])
        .expect("publish fixture Container");
    let container = repository
        .read(container_id)
        .expect("obtain rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct exact location");
    let baseline = storage.operation_count();

    for _ in 0..2 {
        assert_eq!(
            repository
                .read_verified_location(entry)
                .expect("pressure fallback still verifies the record"),
            payload
        );
    }

    let operations = &storage.operations()[baseline..];
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
        6
    );
    let status = repository.descriptor_cache_status();
    assert_eq!(status.target_entries(), 0);
    assert_eq!(status.entry_count(), 0);
    assert_eq!(status.hits(), 0);
    assert_eq!(status.misses(), 2);
    assert_eq!(status.pressure_rejections(), 2);
    assert_eq!(status.swap_used_bytes(), 1);
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
    assert_eq!(
        repository
            .read_verified_location(candidate)
            .expect("warm only the immutable envelope proof"),
        payload
    );
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
fn repeated_exact_lookup_reuses_one_verified_hot_index_page() {
    let data = MemoryStorageIo::new();
    let containers = ContainerRepository::new(data);
    let container_id = ContainerId::new([0xBA; 16]).expect("container identity is nonzero");
    let payload = b"one hot Exact Index page should remain bounded in RAM";
    containers
        .publish_raw(container_id, 17, &[payload])
        .expect("publish one worked Container");
    let container = containers
        .read(container_id)
        .expect("obtain verified rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct one exact entry");

    let metadata = MemoryStorageIo::new();
    let profile = ExactIndexProfileId::new([0xBB; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new_with_memory_snapshot(
        metadata.clone(),
        MemoryPressureSnapshot::new(128 * 1_024 * 1_024 * 1_024, 96 * 1_024 * 1_024 * 1_024, 0),
    );
    let descriptor = indexes
        .publish(&ExactIndexRun::new(profile, 1, vec![entry]).expect("construct one Run"))
        .expect("publish one Run");
    indexes
        .activate(
            &ExactIndexRunSet::new(
                profile,
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin one Run")],
            )
            .expect("construct one Run Set"),
        )
        .expect("activate one Run Set");
    let active = indexes
        .recover_active()
        .expect("recover active Exact Index")
        .expect("one Exact Index is active");
    let baseline = metadata.operation_count();

    let first = active
        .lookup_transitions(entry.chunk_id(), entry.logical_length())
        .expect("first hot lookup succeeds");
    let after_first = metadata.operation_count();
    let second = active
        .lookup_transitions(entry.chunk_id(), entry.logical_length())
        .expect("second hot lookup succeeds");
    let after_second = metadata.operation_count();

    assert_eq!(first, second);
    assert_eq!(first.candidates(), &[entry]);
    assert_eq!(
        metadata.operations()[baseline..after_first]
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        1,
        "binary search and candidate collection must share one verified page"
    );
    assert_eq!(
        metadata.operations()[after_first..after_second]
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        0,
        "a repeated hot-key lookup must not issue another Metadata page read"
    );
    let cache = indexes.page_cache_status();
    assert_eq!(cache.misses(), 1);
    assert_eq!(cache.hits(), 3);
    assert_eq!(cache.resident_pages(), 1);
    assert!(cache.target_pages() >= cache.resident_pages());
    assert!(cache.capacity_pages() >= cache.target_pages());
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
