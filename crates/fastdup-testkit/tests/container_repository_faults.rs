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
            StorageOperation::WriteAt,
            StorageOperation::SetLen,
            StorageOperation::Read,
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
