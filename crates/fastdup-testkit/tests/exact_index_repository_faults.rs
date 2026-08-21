use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet,
};
use fastdup_store::{ExactIndexRunRepository, ExactIndexStoreError, MemoryPressureSnapshot};
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

fn single_entry_run(run_generation: u64, ordinal: u8) -> ExactIndexRun {
    let logical_length = 32_768 + u32::from(ordinal);
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        run_generation,
        4_096,
        record_length,
        0xEE00_0000 + u32::from(ordinal),
    )
    .expect("worked RAW location is valid");
    let entry =
        ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
            .expect("worked active entry is valid");
    ExactIndexRun::new(profile(), run_generation, vec![entry]).expect("one-entry run is canonical")
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
    let repository = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0),
    );
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
fn one_large_run_family_consumes_one_lookup_slot_and_reads_only_its_key_partition() {
    const FAMILY_GENERATION: u64 = 100;
    const PARTITION_COUNT: u16 = 65;

    let storage = MemoryStorageIo::new();
    let repository = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0),
    );
    let mut refs = Vec::new();
    for ordinal in 0..PARTITION_COUNT {
        let descriptor = repository
            .publish(&single_entry_run(
                FAMILY_GENERATION + u64::from(ordinal),
                u8::try_from(ordinal).expect("worked partition ordinal fits u8"),
            ))
            .expect("publish one durable family partition");
        refs.push(
            ExactIndexRunRef::family_partition(
                1,
                FAMILY_GENERATION,
                ordinal,
                PARTITION_COUNT,
                descriptor,
            )
            .expect("partition reference is valid"),
        );
    }
    let run_set = ExactIndexRunSet::new(profile(), 1, refs)
        .expect("one complete partitioned family is canonical");
    let active = repository
        .activate(&run_set)
        .expect("a family may contain more than 64 physical Runs");
    assert_eq!(active.family_count(), 1);
    assert_eq!(active.run_count(), usize::from(PARTITION_COUNT));

    let baseline = storage.operation_count();
    let lookup = active
        .lookup_transitions(ChunkId::from_bytes([42; 32]), 32_810)
        .expect("range-aware family lookup succeeds");
    assert!(lookup.complete());
    assert_eq!(lookup.candidates().len(), 1);
    let operations = &storage.operations()[baseline..];
    assert!(!operations.contains(&StorageOperation::Read));
    assert!(!operations.contains(&StorageOperation::ListNames));
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        1,
        "binary search and candidate extraction reuse the one verified hot page"
    );
}

#[test]
fn active_run_membership_skips_every_exact_page_for_an_absent_key() {
    let storage = MemoryStorageIo::new();
    let repository = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0),
    );
    let mut run_refs = Vec::new();
    for generation in 201..=204 {
        let descriptor = repository
            .publish(&run_at(generation, 0))
            .expect("publish one immutable overlapping Run");
        run_refs
            .push(ExactIndexRunRef::new(0, descriptor).expect("pin one verified Run descriptor"));
    }
    let active = repository
        .activate(
            &ExactIndexRunSet::new(profile(), 1, run_refs)
                .expect("construct one active overlapping Run Set"),
        )
        .expect("activate every complete Run dependency");
    let baseline = storage.operation_count();

    let lookup = active
        .lookup_transitions(ChunkId::from_bytes([20; 32]), 99_999)
        .expect("an absent key remains a complete non-authoritative lookup");

    assert!(lookup.complete());
    assert!(lookup.candidates().is_empty());
    let membership = active.membership_status();
    assert_eq!(membership.filter_count(), 4);
    assert!(membership.allocated_bytes() >= 4 * 64);
    assert_eq!(membership.probes(), 4);
    assert_eq!(membership.definitely_absent(), 4);
    assert_eq!(membership.requires_exact_lookup(), 0);
    let operations = &storage.operations()[baseline..];
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ReadExactAt)
            .count(),
        0,
        "complete Run membership should reject the absent key before page lookup: {operations:?}"
    );
}

#[test]
fn active_run_membership_never_authorizes_a_present_key() {
    let storage = MemoryStorageIo::new();
    let repository = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0),
    );
    let descriptor = repository
        .publish(&run_at(205, 0))
        .expect("publish one immutable Run");
    let active = repository
        .activate(
            &ExactIndexRunSet::new(
                profile(),
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
            )
            .expect("construct one active Run Set"),
        )
        .expect("activate the complete Run dependency");
    let baseline = storage.operation_count();

    let lookup = active
        .lookup_transitions(ChunkId::from_bytes([20; 32]), 16_404)
        .expect("the Bloom positive still runs the complete Exact lookup");

    assert_eq!(lookup.candidates().len(), 1);
    assert!(storage.operations()[baseline..].contains(&StorageOperation::ReadExactAt));
    let membership = active.membership_status();
    assert_eq!(membership.probes(), 1);
    assert_eq!(membership.definitely_absent(), 0);
    assert_eq!(membership.requires_exact_lookup(), 1);
}

#[test]
fn swap_pressure_disables_run_membership_without_changing_lookup_results() {
    let storage = MemoryStorageIo::new();
    let repository = ExactIndexRunRepository::new_with_memory_snapshot(
        storage.clone(),
        MemoryPressureSnapshot::new(128 << 30, 96 << 30, 1),
    );
    let descriptor = repository
        .publish(&run_at(206, 0))
        .expect("publish one immutable Run");
    let active = repository
        .activate(
            &ExactIndexRunSet::new(
                profile(),
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
            )
            .expect("construct one active Run Set"),
        )
        .expect("activate without optional membership acceleration");
    let baseline = storage.operation_count();

    let lookup = active
        .lookup_transitions(ChunkId::from_bytes([20; 32]), 99_999)
        .expect("an unfiltered absent key uses the normal Exact path");

    assert!(lookup.candidates().is_empty());
    assert!(storage.operations()[baseline..].contains(&StorageOperation::ReadExactAt));
    assert_eq!(active.membership_status().filter_count(), 0);
    assert_eq!(active.membership_status().probes(), 0);
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
        "the selected activation-slot sync must remain the final fallible commit operation"
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
        let replacement_first = repository
            .publish(&run_at(RUN_GENERATION + 1, 80))
            .expect("publish first replacement family partition");
        let replacement_second = repository
            .publish(&run_at(RUN_GENERATION + 2, 120))
            .expect("publish second replacement family partition");
        let replacement_set = ExactIndexRunSet::new(
            profile(),
            2,
            vec![
                ExactIndexRunRef::family_partition(0, RUN_GENERATION + 1, 0, 2, replacement_first)
                    .expect("first replacement partition reference is valid"),
                ExactIndexRunRef::family_partition(0, RUN_GENERATION + 1, 1, 2, replacement_second)
                    .expect("second replacement partition reference is valid"),
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

#[test]
fn every_activation_rotation_fault_recovers_only_the_previous_or_complete_new_run_set() {
    const SLOT_RECORDS: u64 = 64;

    fn prepare(
        storage: &MemoryStorageIo,
    ) -> (
        ExactIndexRunRepository<MemoryStorageIo>,
        ExactIndexRunSet,
        usize,
    ) {
        let repository = ExactIndexRunRepository::new(storage.clone());
        let descriptor = repository
            .publish(&run())
            .expect("publish one durable Run shared by every activation");
        let run_ref = ExactIndexRunRef::new(0, descriptor).expect("Run reference is valid");
        for generation in 1..=SLOT_RECORDS {
            let run_set = ExactIndexRunSet::new(profile(), generation, vec![run_ref])
                .expect("worked Run Set generation is valid");
            repository
                .activate(&run_set)
                .expect("seed one complete bounded-slot activation");
        }
        let successor = ExactIndexRunSet::new(profile(), SLOT_RECORDS + 1, vec![run_ref])
            .expect("rotation successor Run Set is valid");
        let baseline = storage.operation_count();
        (repository, successor, baseline)
    }

    let probe_storage = MemoryStorageIo::new();
    let (probe, successor, baseline) = prepare(&probe_storage);
    probe
        .activate(&successor)
        .expect("probe activation rotation succeeds");
    let operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(
        operations.last(),
        Some(&StorageOperation::SyncFile),
        "the rotated slot sync must remain the final fallible commit operation"
    );

    for (relative_position, operation) in operations.iter().copied().enumerate() {
        let absolute_position = baseline + relative_position;
        let storage = MemoryStorageIo::with_fail_before(absolute_position);
        let (repository, successor, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(
            repository.activate(&successor).is_err(),
            "fail-before {relative_position} ({operation:?}) was not observed"
        );
        storage.crash();
        let recovered = repository
            .recover_active()
            .expect("the previous activation remains recoverable")
            .expect("one complete Run Set remains active");
        assert_eq!(
            recovered.run_set().generation(),
            SLOT_RECORDS,
            "fail-before {relative_position} ({operation:?}) exposed a mixed/new Run Set"
        );

        let storage = MemoryStorageIo::with_fail_after(absolute_position);
        let (repository, successor, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(
            repository.activate(&successor).is_err(),
            "fail-after {relative_position} ({operation:?}) was not observed"
        );
        storage.crash();
        let recovered = repository
            .recover_active()
            .expect("one whole activation remains recoverable")
            .expect("at least the previous Run Set remains active");
        let expected_generation = if relative_position + 1 == operations.len() {
            SLOT_RECORDS + 1
        } else {
            SLOT_RECORDS
        };
        assert_eq!(
            recovered.run_set().generation(),
            expected_generation,
            "fail-after {relative_position} ({operation:?}) recovered a mixed generation"
        );
    }
}

#[test]
fn every_compaction_fault_publishes_only_absence_or_one_complete_canonical_run() {
    const TARGET_GENERATION: u64 = RUN_GENERATION + 4;

    fn prepare(
        storage: &MemoryStorageIo,
    ) -> (
        ExactIndexRunRepository<MemoryStorageIo>,
        Vec<ExactIndexRunRef>,
        usize,
    ) {
        let repository = ExactIndexRunRepository::new(storage.clone());
        let mut inputs = Vec::new();
        for (generation, first_ordinal) in [(RUN_GENERATION, 0), (12, 40), (13, 80), (14, 120)] {
            let descriptor = repository
                .publish(&run_at(generation, first_ordinal))
                .expect("publish one durable compaction source");
            inputs.push(
                ExactIndexRunRef::new(0, descriptor).expect("compaction source reference is valid"),
            );
        }
        let baseline = storage.operation_count();
        (repository, inputs, baseline)
    }

    fn assert_absent_or_complete(repository: &ExactIndexRunRepository<MemoryStorageIo>) -> bool {
        match repository.open(profile(), TARGET_GENERATION) {
            Ok(reader) => {
                let lookup = reader
                    .lookup(ChunkId::from_bytes([97; 32]), 16_481)
                    .expect("a published compacted Run remains page-valid");
                assert!(lookup.complete());
                assert_eq!(lookup.candidates().len(), 1);
                repository
                    .audit(profile(), TARGET_GENERATION)
                    .expect("a published compacted Run remains fully auditable");
                true
            }
            Err(ExactIndexStoreError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                false
            }
            Err(error) => panic!("compaction exposed neither absence nor a complete Run: {error}"),
        }
    }

    let probe_storage = MemoryStorageIo::new();
    let (probe, inputs, baseline) = prepare(&probe_storage);
    probe
        .compact(&inputs, TARGET_GENERATION)
        .expect("probe compaction succeeds");
    let operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(
        operations.last(),
        Some(&StorageOperation::SyncRoot),
        "the compacted Run directory sync must be the final publication operation"
    );

    for (relative_position, operation) in operations.iter().copied().enumerate() {
        let absolute_position = baseline + relative_position;
        let storage = MemoryStorageIo::with_fail_before(absolute_position);
        let (repository, inputs, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(repository.compact(&inputs, TARGET_GENERATION).is_err());
        storage.crash();
        assert!(
            !assert_absent_or_complete(&repository),
            "fail-before {relative_position} ({operation:?}) made compaction durable"
        );

        let storage = MemoryStorageIo::with_fail_after(absolute_position);
        let (repository, inputs, observed_baseline) = prepare(&storage);
        assert_eq!(observed_baseline, baseline);
        assert!(repository.compact(&inputs, TARGET_GENERATION).is_err());
        storage.crash();
        let recovered = assert_absent_or_complete(&repository);
        assert_eq!(
            recovered,
            relative_position + 1 == operations.len(),
            "only an effective final SyncRoot may publish compacted Run bytes after an error"
        );
    }
}
