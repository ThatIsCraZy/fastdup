use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet,
};
use fastdup_store::{ExactIndexRunRepository, ExactIndexStoreError};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const RUN_GENERATION: u64 = 11;

fn profile() -> ExactIndexProfileId {
    ExactIndexProfileId::new([0x71; 32]).expect("profile identity is nonzero")
}

fn run() -> ExactIndexRun {
    run_at(RUN_GENERATION, 0)
}

fn run_at(run_generation: u64, first_ordinal: u8) -> ExactIndexRun {
    let entries = (first_ordinal..first_ordinal + 40)
        .rev()
        .map(|ordinal| {
            let logical_length = 16_384 + u32::from(ordinal);
            let record_length = (logical_length + 255) / 64 * 64;
            let location = ExactIndexLocation::raw(
                ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
                u64::from(ordinal) + 1,
                4_096 + u64::from(ordinal) * 64,
                record_length,
                0xCC00_0000 + u32::from(ordinal),
            )
            .expect("worked RAW location is valid");
            ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
                .expect("worked active entry is valid")
        })
        .collect();
    ExactIndexRun::new(profile(), run_generation, entries).expect("worked run is canonicalizable")
}

fn high_fanout_run() -> ExactIndexRun {
    let chunk_id = ChunkId::from_bytes([0xD7; 32]);
    let logical_length = 32_768;
    let entries = (0_u8..70)
        .map(|ordinal| {
            let location = ExactIndexLocation::raw(
                ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
                u64::from(ordinal) + 1,
                4_096,
                32_960,
                0xDD00_0000 + u32::from(ordinal),
            )
            .expect("worked RAW location is valid");
            ExactIndexEntry::active(chunk_id, logical_length, location)
                .expect("worked active entry is valid")
        })
        .collect();
    ExactIndexRun::new(profile(), RUN_GENERATION, entries)
        .expect("high-fanout run is canonicalizable")
}

fn assert_absent_or_complete(repository: &ExactIndexRunRepository<MemoryStorageIo>) -> bool {
    match repository.open(profile(), RUN_GENERATION) {
        Ok(reader) => {
            let lookup = reader
                .lookup(ChunkId::from_bytes([17; 32]), 16_401)
                .expect("a recovered published run remains page-valid");
            assert!(lookup.complete());
            assert_eq!(lookup.candidates().len(), 1);
            repository
                .audit(profile(), RUN_GENERATION)
                .expect("a recovered published run remains fully auditable");
            true
        }
        Err(ExactIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            false
        }
        Err(error) => panic!("recovery exposed neither absence nor a complete run: {error}"),
    }
}

#[test]
fn every_publish_fault_recovers_only_absence_or_the_complete_run() {
    let probe_storage = MemoryStorageIo::new();
    let probe = ExactIndexRunRepository::new(probe_storage.clone());
    probe.publish(&run()).expect("probe publish succeeds");
    let operations = probe_storage.operations();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncRoot));

    for (position, operation) in operations.iter().copied().enumerate() {
        let storage = MemoryStorageIo::with_fail_before(position);
        let repository = ExactIndexRunRepository::new(storage.clone());
        assert!(
            repository.publish(&run()).is_err(),
            "fail-before {position} ({operation:?}) was not observed"
        );
        storage.crash();
        assert!(
            !assert_absent_or_complete(&repository),
            "fail-before {position} ({operation:?}) made an unacknowledged run durable"
        );

        let storage = MemoryStorageIo::with_fail_after(position);
        let repository = ExactIndexRunRepository::new(storage.clone());
        assert!(
            repository.publish(&run()).is_err(),
            "fail-after {position} ({operation:?}) was not observed"
        );
        storage.crash();
        let recovered = assert_absent_or_complete(&repository);
        assert_eq!(
            recovered,
            operation == StorageOperation::SyncRoot,
            "only an effective final directory sync may make the run crash-durable"
        );
    }
}

#[test]
fn lookup_never_materializes_the_run_or_an_unbounded_location_set() {
    let storage = MemoryStorageIo::new();
    let repository = ExactIndexRunRepository::new(storage.clone());
    repository
        .publish(&high_fanout_run())
        .expect("publish high-fanout run");
    let baseline = storage.operation_count();

    let reader = repository
        .open(profile(), RUN_GENERATION)
        .expect("open verified run envelope");
    let lookup = reader
        .lookup(ChunkId::from_bytes([0xD7; 32]), 32_768)
        .expect("bounded lookup succeeds");

    assert_eq!(lookup.candidates().len(), 64);
    assert!(!lookup.complete());
    let lookup_operations = &storage.operations()[baseline..];
    assert!(!lookup_operations.contains(&StorageOperation::Read));
    assert!(!lookup_operations.contains(&StorageOperation::ListNames));
    assert!(
        lookup_operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count()
            <= 7,
        "open plus binary lookup must perform only bounded 4-KiB reads"
    );
}

#[test]
fn every_first_activation_fault_recovers_only_absence_or_the_complete_run_set() {
    let probe_storage = MemoryStorageIo::new();
    let probe = ExactIndexRunRepository::new(probe_storage.clone());
    let descriptor = probe.publish(&run()).expect("publish durable probe Run");
    let run_set = ExactIndexRunSet::new(
        profile(),
        1,
        vec![ExactIndexRunRef::new(0, descriptor).expect("probe Run reference is valid")],
    )
    .expect("probe Run Set is valid");
    let baseline = probe_storage.operation_count();
    probe.activate(&run_set).expect("probe activation succeeds");
    let activation_operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(
        activation_operations.last(),
        Some(&StorageOperation::SyncFile),
        "the activation WAL sync must remain the final fallible commit operation"
    );

    for (relative_position, operation) in activation_operations.iter().copied().enumerate() {
        let absolute_position = baseline + relative_position;
        let storage = MemoryStorageIo::with_fail_before(absolute_position);
        let repository = ExactIndexRunRepository::new(storage.clone());
        let descriptor = repository
            .publish(&run())
            .expect("seed durable Run before the selected activation fault");
        let run_set = ExactIndexRunSet::new(
            profile(),
            1,
            vec![ExactIndexRunRef::new(0, descriptor).expect("seed Run reference is valid")],
        )
        .expect("seed Run Set is valid");
        assert!(
            repository.activate(&run_set).is_err(),
            "fail-before {relative_position} ({operation:?}) was not observed"
        );
        storage.crash();
        assert!(
            repository
                .recover_active()
                .expect("recovery remains structurally valid")
                .is_none(),
            "fail-before {relative_position} ({operation:?}) exposed an uncommitted Run Set"
        );

        let storage = MemoryStorageIo::with_fail_after(absolute_position);
        let repository = ExactIndexRunRepository::new(storage.clone());
        let descriptor = repository
            .publish(&run())
            .expect("seed durable Run before the selected activation fault");
        let run_set = ExactIndexRunSet::new(
            profile(),
            1,
            vec![ExactIndexRunRef::new(0, descriptor).expect("seed Run reference is valid")],
        )
        .expect("seed Run Set is valid");
        assert!(
            repository.activate(&run_set).is_err(),
            "fail-after {relative_position} ({operation:?}) was not observed"
        );
        storage.crash();
        let recovered = repository
            .recover_active()
            .expect("recovery remains structurally valid");
        assert_eq!(
            recovered.is_some(),
            relative_position + 1 == activation_operations.len(),
            "only an effective final activation-WAL sync may expose the new Run Set"
        );
    }
}

#[test]
fn replacement_activation_fault_recovers_only_the_previous_or_complete_new_run_set() {
    fn prepare(
        storage: &MemoryStorageIo,
    ) -> (
        ExactIndexRunRepository<MemoryStorageIo>,
        ExactIndexRunSet,
        usize,
    ) {
        let repository = ExactIndexRunRepository::new(storage.clone());
        let first = repository
            .publish(&run())
            .expect("publish first durable Run");
        let first_set = ExactIndexRunSet::new(
            profile(),
            1,
            vec![ExactIndexRunRef::new(0, first).expect("first Run reference is valid")],
        )
        .expect("first Run Set is valid");
        repository
            .activate(&first_set)
            .expect("commit first activation");
        let replacement = repository
            .publish(&run_at(RUN_GENERATION + 1, 80))
            .expect("publish replacement durable Run");
        let replacement_set = ExactIndexRunSet::new(
            profile(),
            2,
            vec![
                ExactIndexRunRef::new(0, replacement).expect("replacement Run reference is valid"),
                ExactIndexRunRef::new(1, first).expect("retained Run reference is valid"),
            ],
        )
        .expect("replacement Run Set is valid");
        let baseline = storage.operation_count();
        (repository, replacement_set, baseline)
    }

    let probe_storage = MemoryStorageIo::new();
    let (probe, replacement_set, baseline) = prepare(&probe_storage);
    probe
        .activate(&replacement_set)
        .expect("probe replacement activation succeeds");
    let operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));

    for (relative_position, operation) in operations.iter().copied().enumerate() {
        let absolute_position = baseline + relative_position;
        let storage = MemoryStorageIo::with_fail_before(absolute_position);
        let (repository, replacement_set, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(repository.activate(&replacement_set).is_err());
        storage.crash();
        let recovered = repository
            .recover_active()
            .expect("old activation remains recoverable")
            .expect("first Run Set remains active");
        assert_eq!(
            recovered.run_set().generation(),
            1,
            "fail-before {relative_position} ({operation:?}) exposed a mixed/new Run Set"
        );

        let storage = MemoryStorageIo::with_fail_after(absolute_position);
        let (repository, replacement_set, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(repository.activate(&replacement_set).is_err());
        storage.crash();
        let recovered = repository
            .recover_active()
            .expect("one whole activation remains recoverable")
            .expect("at least the first Run Set remains active");
        let expected_generation = if relative_position + 1 == operations.len() {
            2
        } else {
            1
        };
        assert_eq!(
            recovered.run_set().generation(),
            expected_generation,
            "fail-after {relative_position} ({operation:?}) recovered a mixed generation"
        );
    }
}
