use fastdup_appliance::{DurableNamespace, checkpoint_policy_set};
use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ManifestExtent, ManifestLeaf, NamespaceEntry, NamespaceRoot,
};
use fastdup_posix::{
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, RequestContext,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationRepository,
    RecoveryCheckpointRepository, SimilarityIndexRepository, StorageIo,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

fn id(byte: u8) -> ContainerId {
    ContainerId::new([byte; 16]).unwrap()
}
fn name(byte: u8) -> String {
    format!("{}.fdc", format!("{byte:02x}").repeat(16))
}

fn fixture(dependent: bool) -> (MemoryStorageIo, MemoryStorageIo, Vec<u8>) {
    fixture_using(MemoryStorageIo::new(), MemoryStorageIo::new(), dependent)
}

fn fixture_using(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
    dependent: bool,
) -> (MemoryStorageIo, MemoryStorageIo, Vec<u8>) {
    let containers = ContainerRepository::new(data.clone());
    let allocator = containers.open_generation_allocator(1024).unwrap();
    assert_eq!(allocator.reserve_generation().unwrap(), 1);
    let mut state = 919_u64;
    let base: Vec<u8> = (0..65536)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    containers.publish_raw(id(31), 1, &[&base]).unwrap();
    let mut payload = base.clone();
    if dependent {
        assert_eq!(allocator.reserve_generation().unwrap(), 2);
        payload[777] ^= 1;
        containers
            .publish_zstd_prefix_pairs_verified(id(32), 2, &[(&base, &payload)])
            .unwrap();
        let structure = containers.read_structure(id(32)).unwrap();
        assert_eq!(
            structure.chunks()[0].dependency,
            Some((ChunkId::of(&base), base.len() as u32))
        );
    }
    let generations = GenerationRepository::new(metadata.clone(), checkpoint_policy_set());
    generations
        .commit_namespace(&NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let manifest = generations
        .publish_manifest(
            &ManifestLeaf::new(
                payload.len() as u64,
                vec![ManifestExtent::Data {
                    logical_length: payload.len() as u64,
                    chunk_id: ChunkId::of(&payload),
                }],
            )
            .unwrap(),
        )
        .unwrap();
    let root = NamespaceRoot::new(
        1024,
        3,
        1,
        vec![DurableInode::new(2, 0o600, 0, 0, 1, 1, payload.len() as u64, manifest).unwrap()],
        vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
    )
    .unwrap();
    generations
        .commit_namespace_with_data(&root, &containers)
        .unwrap();
    (metadata, data, payload)
}

#[test]
fn structural_graph_recovery_reads_no_container_payload_and_keeps_demand_verification() {
    let (metadata, data, payload) = fixture(false);
    let generations = GenerationRepository::new(metadata, checkpoint_policy_set());
    let containers = ContainerRepository::new(data.clone());
    let before = data.operation_count();
    let recovered = generations
        .recover_latest_with_structural_files(&containers)
        .unwrap()
        .unwrap();
    assert!(!data.operations()[before..].contains(&StorageOperation::Read));
    let (_, mut files) = recovered.into_parts();
    assert_eq!(
        files
            .pop()
            .unwrap()
            .into_file()
            .read_at(0, payload.len() as u32)
            .unwrap(),
        payload
    );
}

#[test]
fn writable_structural_open_defers_bitrot_to_demand_read_and_scrub() {
    let (metadata, data, payload) = fixture(false);
    data.write_at(&name(31), 4096 + 192 + 100, &[payload[100] ^ 1])
        .unwrap();
    let containers = ContainerRepository::new(data.clone());
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    let similarities = SimilarityIndexRepository::new(metadata.clone());
    let appliance = DurableNamespace::open_with_structural_recovery(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        containers.clone(),
        &indexes,
        &similarities,
        1024,
    )
    .unwrap();
    assert!(appliance.namespace().mutation_admission_open());
    assert!(
        containers
            .scrub_container::<MemoryStorageIo>(id(31), None)
            .is_err()
    );
    // A later checkpoint resume must never clear an integrity failure.
    appliance.namespace().fail_integrity();
    appliance.namespace().resume_mutation_admission();
    assert!(!appliance.namespace().mutation_admission_open());
    assert!(matches!(
        appliance.namespace().dispatch(
            RequestContext {
                uid: 0,
                gid: 0,
                pid: 1
            },
            Operation::Create {
                parent: ROOT_INODE,
                name: b"blocked",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            }
        ),
        Err(PosixError::Io)
    ));
    let recovered = GenerationRepository::new(metadata, checkpoint_policy_set())
        .recover_latest_with_structural_files(&containers)
        .unwrap()
        .unwrap();
    let (_, mut files) = recovered.into_parts();
    assert!(files.pop().unwrap().into_file().read_at(0, 1024).is_err());
}

#[test]
fn missing_independent_base_prevents_structural_mount() {
    let (metadata, data, _) = fixture(true);
    let generations = GenerationRepository::new(metadata, checkpoint_policy_set());
    let containers = ContainerRepository::new(data.clone());
    assert!(
        generations
            .recover_latest_with_structural_files(&containers)
            .unwrap()
            .is_some()
    );
    data.remove_file(&name(31)).unwrap();
    assert!(
        generations
            .recover_latest_with_structural_files(&containers)
            .is_err()
    );
}

#[test]
fn torn_seal_or_record_structure_prevents_structural_mount() {
    for offset in [40, 4096 + 12, 4096 + 128] {
        let (metadata, data, _) = fixture(false);
        let old = data.read_exact_at(&name(31), offset, 1).unwrap()[0];
        data.write_at(&name(31), offset, &[old ^ 1]).unwrap();
        assert!(
            GenerationRepository::new(metadata, checkpoint_policy_set())
                .recover_latest_with_structural_files(&ContainerRepository::new(data))
                .is_err()
        );
    }
}

#[test]
fn recovery_checkpoint_copy_uses_commit_durability_and_restore_still_checks_data() {
    let (metadata, data, payload) = fixture(false);
    let generations = GenerationRepository::new(metadata, checkpoint_policy_set());
    let checkpoints = RecoveryCheckpointRepository::new(data.clone());
    let summary = checkpoints
        .publish_committed(&generations)
        .unwrap()
        .unwrap();
    assert_eq!(summary.generation(), 2);
    data.write_at(&name(31), 4096 + 192, &[payload[0] ^ 1])
        .unwrap();
    let replacement = GenerationRepository::new(MemoryStorageIo::new(), checkpoint_policy_set());
    assert!(
        checkpoints
            .recover_latest(&replacement, &ContainerRepository::new(data))
            .is_err()
    );
}

#[test]
fn committed_recovery_does_not_visit_container_inventory() {
    let (metadata, data, _) = fixture(false);
    let containers = ContainerRepository::new(data.clone());
    for byte in 80..112 {
        containers
            .publish_raw(id(byte), u64::from(byte), &[&[byte; 4096]])
            .unwrap();
    }
    let before = data.operation_count();
    let (recovered, pending) = GenerationRepository::new(metadata, checkpoint_policy_set())
        .recover_committed_for_mount(&containers)
        .unwrap();
    assert!(recovered.is_some());
    assert_eq!(pending.remaining_chunks(), 1);
    assert_eq!(
        data.operation_count() - before,
        0,
        "normal recovery must not visit DATA inventory"
    );
}

fn scrub_required(
    containers: &ContainerRepository<MemoryStorageIo>,
    mut pending: fastdup_store::PendingDataVerification,
) -> Result<(), fastdup_store::StoreError> {
    for id in containers.recovery_container_snapshot()? {
        containers.scrub_container_for_recovery::<MemoryStorageIo>(id, None, &mut pending)?;
    }
    pending.finish()
}

#[test]
fn committed_mount_defers_missing_containers_seals_payloads_and_bases_to_scrub_and_reads() {
    for damage in 0..4 {
        let (metadata, data, _) = fixture(damage == 3);
        if damage == 0 || damage == 3 {
            data.remove_file(&name(31)).unwrap();
        } else {
            let offset = if damage == 1 { 40 } else { 4096 + 192 };
            let byte = data.read_exact_at(&name(31), offset, 1).unwrap()[0];
            data.write_at(&name(31), offset, &[byte ^ 1]).unwrap();
        }
        let containers = ContainerRepository::new(data);
        let (recovered, pending) = GenerationRepository::new(metadata, checkpoint_policy_set())
            .recover_committed_for_mount(&containers)
            .unwrap();
        let (_, mut files) = recovered.unwrap().into_parts();
        assert!(
            files.pop().unwrap().into_file().read_at(0, 1024).is_err(),
            "damage={damage}"
        );
        assert!(
            scrub_required(&containers, pending).is_err(),
            "damage={damage}"
        );
    }
}

#[test]
fn initial_scrub_discharges_required_chunks_only_after_full_dependent_verification() {
    let (metadata, data, payload) = fixture(true);
    let containers = ContainerRepository::new(data);
    let (recovered, mut pending) = GenerationRepository::new(metadata, checkpoint_policy_set())
        .recover_committed_for_mount(&containers)
        .unwrap();
    assert_eq!(pending.remaining_chunks(), 1);
    containers
        .scrub_container_for_recovery::<MemoryStorageIo>(id(31), None, &mut pending)
        .unwrap();
    assert_eq!(
        pending.remaining_chunks(),
        1,
        "unrequired Base alone cannot satisfy the dependent Chunk"
    );
    containers
        .scrub_container_for_recovery::<MemoryStorageIo>(id(32), None, &mut pending)
        .unwrap();
    assert_eq!(pending.remaining_chunks(), 0);
    pending.finish().unwrap();
    let (_, mut files) = recovered.unwrap().into_parts();
    assert_eq!(
        files
            .pop()
            .unwrap()
            .into_file()
            .read_at(0, payload.len() as u32)
            .unwrap(),
        payload
    );
}

#[test]
fn writable_committed_open_skips_inventory_and_keeps_checkpoint_durability() {
    let (metadata, data, _) = fixture(false);
    let before = data.operation_count();
    let mut appliance = DurableNamespace::open_with_committed_recovery(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        ContainerRepository::new(data.clone()),
        &ExactIndexRunRepository::new(metadata.clone()),
        &SimilarityIndexRepository::new(metadata),
        1024,
    )
    .unwrap();
    assert!(appliance.namespace().mutation_admission_open());
    assert!(!data.operations()[before..].contains(&StorageOperation::ListNames));
    scrub_required(
        &ContainerRepository::new(data),
        appliance.take_startup_data_verification().unwrap(),
    )
    .unwrap();
}

fn add_torn_tail(metadata: &MemoryStorageIo) {
    let length = metadata.object_len("commit.wal").unwrap();
    metadata
        .write_at("commit.wal", length, &[0x79; 11])
        .unwrap();
    metadata.sync_file("commit.wal").unwrap();
}

#[test]
fn every_committed_mount_recovery_fault_preserves_the_selected_commit() {
    let (metadata, data, _) = fixture(false);
    add_torn_tail(&metadata);
    let baseline = metadata.operation_count();
    let (_, pending) = GenerationRepository::new(metadata.clone(), checkpoint_policy_set())
        .recover_committed_for_mount(&ContainerRepository::new(data))
        .unwrap();
    drop(pending);
    let operations = metadata.operations()[baseline..].to_vec();
    assert!(operations.contains(&StorageOperation::SetLen));
    assert!(operations.contains(&StorageOperation::SyncFile));
    for relative in 0..operations.len() {
        for after in [false, true] {
            let metadata = if after {
                MemoryStorageIo::with_fail_after(baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(baseline + relative)
            };
            let (metadata, data, payload) = fixture_using(metadata, MemoryStorageIo::new(), false);
            add_torn_tail(&metadata);
            let generations = GenerationRepository::new(metadata.clone(), checkpoint_policy_set());
            let containers = ContainerRepository::new(data.clone());
            assert!(
                generations
                    .recover_committed_for_mount(&containers)
                    .is_err(),
                "relative={relative} after={after}"
            );
            metadata.crash();
            data.crash();
            let (recovered, pending) = generations
                .recover_committed_for_mount(&containers)
                .unwrap();
            let recovered = recovered.unwrap();
            assert_eq!(recovered.generation().record().generation(), 2);
            assert_eq!(
                recovered.generation().wal_tail(),
                &fastdup_store::WalTail::Clean
            );
            let (_, mut files) = recovered.into_parts();
            assert_eq!(
                files
                    .pop()
                    .unwrap()
                    .into_file()
                    .read_at(0, payload.len() as u32)
                    .unwrap(),
                payload
            );
            scrub_required(&containers, pending).unwrap();
        }
    }
}

#[test]
fn missing_newest_metadata_fails_before_wal_tail_repair_or_rollback() {
    let (metadata, data, _) = fixture(false);
    let generations = GenerationRepository::new(metadata.clone(), checkpoint_policy_set());
    let containers = ContainerRepository::new(data);
    let recovered = generations
        .recover_latest_with_structural_files(&containers)
        .unwrap()
        .unwrap();
    let object = recovered.generation().record().namespace_root();
    let name = format!(
        "{}.fdm",
        object
            .bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    metadata.remove_file(&name).unwrap();
    add_torn_tail(&metadata);
    let before = metadata.operation_count();
    assert!(
        generations
            .recover_committed_for_mount(&containers)
            .is_err()
    );
    assert!(!metadata.operations()[before..].contains(&StorageOperation::SetLen));
}

#[test]
fn retiring_locations_cannot_discharge_startup_requirements() {
    let (metadata, data, _) = fixture(false);
    let containers = ContainerRepository::new(data);
    let (_, mut pending) = GenerationRepository::new(metadata, checkpoint_policy_set())
        .recover_committed_for_mount(&containers)
        .unwrap();
    containers.install_retiring_selection_barrier(&std::collections::BTreeMap::from([(
        id(31).bytes(),
        id(31),
    )]));
    containers
        .scrub_container_for_recovery::<MemoryStorageIo>(id(31), None, &mut pending)
        .unwrap();
    assert_eq!(pending.remaining_chunks(), 1);
    assert!(pending.finish().is_err());
}

#[test]
fn invalid_and_broken_chain_tails_preserve_the_valid_commit_prefix() {
    for broken_chain in [false, true] {
        let (metadata, data, _) = fixture(false);
        let before = metadata.read("commit.wal").unwrap();
        let suffix = if broken_chain {
            before[before.len() - fastdup_format::COMMIT_RECORD_BYTES..].to_vec()
        } else {
            vec![0; fastdup_format::COMMIT_RECORD_BYTES]
        };
        metadata
            .write_at("commit.wal", before.len() as u64, &suffix)
            .unwrap();
        metadata.sync_file("commit.wal").unwrap();
        let (recovered, pending) =
            GenerationRepository::new(metadata.clone(), checkpoint_policy_set())
                .recover_committed_for_mount(&ContainerRepository::new(data))
                .unwrap();
        assert_eq!(recovered.unwrap().generation().record().generation(), 2);
        assert_eq!(metadata.read("commit.wal").unwrap(), before);
        drop(pending);
    }
}
