use fastdup_format::{ChunkId, ContainerId, SealedContainer};
use fastdup_store::{ContainerRepository, StoreError};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CONTAINER_GENERATION: u64 = 7;
const FIRST_CHUNK: &[u8] = b"fault-injection first chunk";
const SECOND_CHUNK: &[u8] = b"fault-injection second chunk";

fn container_id() -> ContainerId {
    ContainerId::new([0x5a; 16]).expect("fixture container ID is nonzero")
}

fn assert_fixture(container: &SealedContainer) {
    assert_eq!(container.header().container_id(), container_id());
    assert_eq!(
        container.header().container_generation(),
        CONTAINER_GENERATION
    );
    assert_eq!(container.chunk(ChunkId::of(FIRST_CHUNK)), Some(FIRST_CHUNK));
    assert_eq!(
        container.chunk(ChunkId::of(SECOND_CHUNK)),
        Some(SECOND_CHUNK)
    );
}

fn publish(repository: &ContainerRepository<MemoryStorageIo>) -> Result<(), StoreError> {
    repository.publish_raw(
        container_id(),
        CONTAINER_GENERATION,
        &[FIRST_CHUNK, SECOND_CHUNK],
    )
}

fn recover(repository: &ContainerRepository<MemoryStorageIo>) -> Vec<SealedContainer> {
    repository
        .recover_published()
        .expect("recovery never exposes an invalid published container")
}

#[test]
fn successful_publish_survives_a_crash_as_a_verified_container() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());

    publish(&repository).expect("publish succeeds without an injected fault");
    storage.crash();

    let recovered = recover(&repository);
    assert_eq!(recovered.len(), 1);
    assert_fixture(&recovered[0]);
}

#[test]
fn every_publish_failpoint_recovers_to_absent_or_fully_verified() {
    let probe_storage = MemoryStorageIo::new();
    let probe_repository = ContainerRepository::new(probe_storage.clone());
    publish(&probe_repository).expect("probe publish succeeds");
    let operation_count = probe_storage.operation_count();
    assert!(
        operation_count > 0,
        "publish must exercise the storage seam"
    );

    for fail_before in 0..operation_count {
        let storage = MemoryStorageIo::with_fail_before(fail_before);
        let repository = ContainerRepository::new(storage.clone());
        let result = publish(&repository);
        assert!(
            result.is_err(),
            "operation {fail_before} was not interrupted"
        );

        storage.crash();
        let recovered = recover(&repository);
        assert!(
            recovered.len() <= 1,
            "operation {fail_before} recovered duplicate IDs"
        );
        if let Some(container) = recovered.first() {
            assert_fixture(container);
        }
    }
}

#[test]
fn every_after_effect_failpoint_recovers_to_absent_or_fully_verified() {
    let probe_storage = MemoryStorageIo::new();
    let probe_repository = ContainerRepository::new(probe_storage.clone());
    publish(&probe_repository).expect("probe publish succeeds");
    let operations = probe_storage.operations();

    for (fail_after, operation) in operations.iter().copied().enumerate() {
        let storage = MemoryStorageIo::with_fail_after(fail_after);
        let repository = ContainerRepository::new(storage.clone());
        let result = publish(&repository);
        assert!(
            result.is_err(),
            "operation {fail_after} ({operation:?}) was not interrupted after taking effect"
        );

        storage.crash();
        let recovered = recover(&repository);
        assert!(
            recovered.len() <= 1,
            "operation {fail_after} ({operation:?}) recovered duplicate IDs"
        );
        if let Some(container) = recovered.first() {
            assert_fixture(container);
        } else {
            assert_ne!(
                operation,
                StorageOperation::SyncRoot,
                "an effective root sync must preserve the published name"
            );
        }
    }
}

#[test]
fn exact_length_is_finalized_before_verification_and_durability() {
    let storage = MemoryStorageIo::new();
    let repository = ContainerRepository::new(storage.clone());

    publish(&repository).expect("publish succeeds");

    assert_eq!(
        storage.operations(),
        vec![
            StorageOperation::CreateNew,
            StorageOperation::WriteAt,
            StorageOperation::WriteAt,
            StorageOperation::WriteAt,
            StorageOperation::SetLen,
            StorageOperation::ObjectLen,
            StorageOperation::ReadExactAt,
            StorageOperation::ReadExactAt,
            StorageOperation::ReadExactAt,
            StorageOperation::SyncFile,
            StorageOperation::PublishNoreplace,
            StorageOperation::SyncRoot,
        ]
    );
}

#[test]
fn publish_rejects_a_valid_but_unexpected_writer_reread() {
    let substitute = SealedContainer::encode(
        ContainerId::new([0x6b; 16]).expect("substitute ID is nonzero"),
        8,
        &[b"different valid bytes".as_slice()],
    )
    .expect("substitute is independently valid");
    let storage = MemoryStorageIo::with_read_substitution(substitute);
    let repository = ContainerRepository::new(storage.clone());

    assert!(matches!(
        publish(&repository),
        Err(StoreError::PublishVerificationMismatch)
    ));
    storage.crash();
    assert!(recover(&repository).is_empty());
}

#[test]
fn every_high_water_extension_fault_skips_all_precrash_generations() {
    let probe_storage = MemoryStorageIo::new();
    let probe_repository = ContainerRepository::new(probe_storage.clone());
    let probe_allocator = probe_repository
        .open_generation_allocator(2)
        .expect("initialize probe allocator");
    assert_eq!(
        probe_allocator.reserve_generation().expect("reserve one"),
        1
    );
    assert_eq!(
        probe_allocator.reserve_generation().expect("reserve two"),
        2
    );
    let baseline = probe_storage.operation_count();
    assert_eq!(
        probe_allocator.reserve_generation().expect("extend probe"),
        3
    );
    let extension_operations = probe_storage.operation_count() - baseline;
    assert!(extension_operations > 0);

    for fail_after_effect in [false, true] {
        for relative in 0..extension_operations {
            let position = baseline + relative;
            let storage = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let repository = ContainerRepository::new(storage.clone());
            let allocator = repository
                .open_generation_allocator(2)
                .expect("fixture allocator initializes before the injected extension fault");
            assert_eq!(allocator.reserve_generation().expect("reserve one"), 1);
            assert_eq!(allocator.reserve_generation().expect("reserve two"), 2);
            assert!(
                allocator.reserve_generation().is_err(),
                "fault position {relative} must interrupt range extension"
            );

            storage.crash();
            let reopened_repository = ContainerRepository::new(storage.clone());
            reopened_repository
                .audit_generation_high_water(None)
                .expect("an interrupted extension retains one valid old or new pair");
            let reopened = reopened_repository
                .open_generation_allocator(2)
                .expect("recover the selected durable reservation");
            assert!(
                reopened
                    .reserve_generation()
                    .expect("reserve after recovery")
                    > 2,
                "recovery must never reuse a generation returned before the crash"
            );
        }
    }
}

#[test]
fn every_empty_high_water_initialization_fault_recovers_to_empty_or_a_valid_pair() {
    let probe_storage = MemoryStorageIo::new();
    let probe_repository = ContainerRepository::new(probe_storage.clone());
    let probe_allocator = probe_repository
        .open_generation_allocator(2)
        .expect("initialize probe allocator");
    probe_allocator
        .reserve_generation()
        .expect("publish the first probe reservation");
    let operation_count = probe_storage.operation_count();
    assert!(operation_count > 0);

    for fail_after_effect in [false, true] {
        for position in 0..operation_count {
            let storage = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let repository = ContainerRepository::new(storage.clone());
            let attempt = repository
                .open_generation_allocator(2)
                .and_then(|allocator| allocator.reserve_generation());
            assert!(
                attempt.is_err(),
                "fault position {position} must interrupt allocator initialization"
            );

            storage.crash();
            let reopened_repository = ContainerRepository::new(storage.clone());
            reopened_repository
                .audit_generation_high_water(None)
                .expect("an interrupted empty initialization is absent or a valid pair");
            reopened_repository
                .open_generation_allocator(2)
                .expect("reopen the allocator after the crash")
                .reserve_generation()
                .expect("reserve from the recovered or newly initialized allocator");
        }
    }
}
