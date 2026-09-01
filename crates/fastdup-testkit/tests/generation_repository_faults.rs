use fastdup_format::{
    COMMIT_RECORD_BYTES, ChunkId, ContainerId, DurableInode, ManifestExtent, ManifestInnerNode,
    ManifestLeaf, MetadataObjectId, MetadataObjectKind, NamespaceEntry, NamespaceGraphRoot,
    NamespaceRoot, PolicySetId, metadata_object_kind,
};
use fastdup_store::{
    ContainerRepository, GenerationError, GenerationRepository, RequiredChunkVerifier, StorageIo,
    StoreError, SuccessorPredecessor, WalTail,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug)]
struct AcceptAllRequiredChunks;

impl RequiredChunkVerifier for AcceptAllRequiredChunks {
    fn verify_required_chunks(&self, _required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingRequiredChunks {
    calls: Mutex<Vec<Vec<ChunkId>>>,
    rejected: Vec<ChunkId>,
}

impl RecordingRequiredChunks {
    fn rejecting(rejected: Vec<ChunkId>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            rejected,
        }
    }

    fn calls(&self) -> Vec<Vec<ChunkId>> {
        self.calls
            .lock()
            .expect("ASSERT: recording verifier lock poisoned")
            .clone()
    }
}

impl RequiredChunkVerifier for RecordingRequiredChunks {
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        self.calls
            .lock()
            .expect("ASSERT: recording verifier lock poisoned")
            .push(required.keys().copied().collect());
        if let Some(chunk_id) = required
            .keys()
            .find(|chunk_id| self.rejected.contains(chunk_id))
        {
            return Err(StoreError::MissingVerifiedChunk {
                chunk_id: *chunk_id,
                logical_length: required[chunk_id],
            });
        }
        Ok(())
    }
}

fn metadata_name(object_id: MetadataObjectId) -> String {
    let mut name = String::with_capacity(68);
    for byte in object_id.bytes() {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing to a String is infallible");
    }
    name.push_str(".fdm");
    name
}

fn is_metadata_name(name: &str) -> bool {
    name.len() == 68 && name.as_bytes().get(64..) == Some(b".fdm")
}

fn empty_file_root(manifest_root: fastdup_format::MetadataObjectId) -> NamespaceRoot {
    NamespaceRoot::new(
        1_024,
        3,
        1,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_001, 1, 0, 0, manifest_root)
                .expect("empty regular inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, vec![b'v', b'm', b'-', 0xff])
                .expect("raw byte name is valid"),
        ],
    )
    .expect("root graph is valid")
}

fn reservation_root(inode_reservation_end: u64) -> NamespaceRoot {
    NamespaceRoot::new(inode_reservation_end, 2, 0, Vec::new(), Vec::new())
        .expect("empty reservation generation is valid")
}

fn nested_directory_root() -> NamespaceRoot {
    NamespaceRoot::new(
        1_024,
        4,
        2,
        vec![
            DurableInode::new_directory(2, 0o750, 1_000, 1_001, 3, 1)
                .expect("parent directory is valid"),
            DurableInode::new_directory(3, 0o700, 1_000, 1_001, 2, 1)
                .expect("child directory is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"parent".to_vec()).expect("root entry is valid"),
            NamespaceEntry::new(2, 3, b"child".to_vec()).expect("nested entry is valid"),
        ],
    )
    .expect("nested directory root is valid")
}

fn bootstrap_inode_reservation(repository: &GenerationRepository<MemoryStorageIo>) {
    let record = repository
        .commit_namespace(&reservation_root(1_024))
        .expect("inode range must be durable before an inode becomes visible");
    assert_eq!(record.generation(), 1);
}

fn hole_file_root(
    manifest_root: fastdup_format::MetadataObjectId,
    logical_size: u64,
    mutation_sequence: u64,
) -> NamespaceRoot {
    hole_file_root_with_reservation(manifest_root, logical_size, mutation_sequence, 1_024)
}

fn hole_file_root_with_reservation(
    manifest_root: fastdup_format::MetadataObjectId,
    logical_size: u64,
    mutation_sequence: u64,
    inode_reservation_end: u64,
) -> NamespaceRoot {
    NamespaceRoot::new(
        inode_reservation_end,
        3,
        mutation_sequence,
        vec![
            DurableInode::new(
                2,
                0o640,
                1_000,
                1_001,
                1,
                mutation_sequence,
                logical_size,
                manifest_root,
            )
            .expect("regular inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, vec![b'v', b'm', b'-', 0xff])
                .expect("raw byte name is valid"),
        ],
    )
    .expect("root graph is valid")
}

fn commit_hole_generation(
    repository: &GenerationRepository<MemoryStorageIo>,
    logical_size: u64,
    mutation_sequence: u64,
) -> Result<(NamespaceRoot, fastdup_format::CommitRecord), GenerationError> {
    let manifest = ManifestLeaf::new(
        logical_size,
        vec![ManifestExtent::Hole {
            logical_length: logical_size,
        }],
    )
    .expect("nonempty hole manifest is valid");
    let manifest_id = repository.publish_manifest(&manifest)?;
    let root = hole_file_root(manifest_id, logical_size, mutation_sequence);
    let record = repository.commit_namespace(&root)?;
    Ok((root, record))
}

fn seed_first_generation(
    storage: &MemoryStorageIo,
    policy: PolicySetId,
) -> (GenerationRepository<MemoryStorageIo>, NamespaceRoot) {
    let repository = GenerationRepository::new(storage.clone(), policy);
    bootstrap_inode_reservation(&repository);
    let (root, record) = commit_hole_generation(&repository, 8, 1)
        .expect("first visible-file generation must commit before injected position");
    assert_eq!(record.generation(), 2);
    (repository, root)
}

#[test]
fn committed_namespace_root_recovers_only_after_the_wal_is_durable() {
    let storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x51; 32]).expect("policy identity is nonzero");
    let repository = GenerationRepository::new(storage.clone(), policy);
    let manifest =
        ManifestLeaf::new(0, Vec::<ManifestExtent>::new()).expect("empty manifest is valid");
    let manifest_id = repository
        .publish_manifest(&manifest)
        .expect("manifest publication must succeed");
    let root = empty_file_root(manifest_id);
    bootstrap_inode_reservation(&repository);

    let committed = repository
        .commit_namespace(&root)
        .expect("generation commit must succeed");
    assert_eq!(committed.generation(), 2);

    storage.crash();
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("recovery must verify the committed graph")
        .expect("one committed generation must exist");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &root);
}

#[test]
fn stale_successor_proof_cannot_advance_a_newer_installed_generation() {
    let metadata = MemoryStorageIo::new();
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let policy = PolicySetId::new([0x79; 32]).expect("policy identity is nonzero");
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let verifier = RecordingRequiredChunks::default();
    let reservation = repository
        .commit_namespace(&reservation_root(1_024))
        .expect("publish predecessor reservation");
    let predecessor = SuccessorPredecessor::from_committed_record(reservation);

    let first_manifest = ManifestLeaf::new(8, vec![ManifestExtent::Hole { logical_length: 8 }])
        .expect("first successor Manifest is valid");
    let first_proof = repository
        .publish_manifest_successor(predecessor, &first_manifest)
        .expect("publish first successor Manifest");
    let first_root = hole_file_root(first_proof.summary().root(), 8, 1);
    let first_commit = repository
        .commit_namespace_with_successor_proofs_using(
            &first_root,
            &containers,
            predecessor,
            &[first_proof],
            &verifier,
        )
        .expect("the proof matches its installed predecessor");
    assert_eq!(first_commit.record().generation(), 2);

    let stale_manifest = ManifestLeaf::new(5, vec![ManifestExtent::Hole { logical_length: 5 }])
        .expect("stale successor Manifest is locally valid");
    let stale_proof = repository
        .publish_manifest_successor(predecessor, &stale_manifest)
        .expect("immutable Metadata publication may precede stale-proof rejection");
    let stale_root = hole_file_root(stale_proof.summary().root(), 5, 2);
    let error = repository
        .commit_namespace_with_successor_proofs_using(
            &stale_root,
            &containers,
            predecessor,
            &[stale_proof],
            &verifier,
        )
        .expect_err("a proof bound to generation one must not advance generation two");
    assert!(matches!(
        error,
        GenerationError::StaleSuccessorPredecessor {
            proof_generation: 1,
            installed_generation: Some(2),
        }
    ));
    assert_eq!(
        verifier.calls(),
        vec![Vec::<ChunkId>::new()],
        "a stale predecessor must be rejected before any dependency verifier work"
    );

    metadata.crash();
    let recovered = GenerationRepository::new(metadata, policy)
        .recover_latest()
        .expect("stale rejection leaves the installed generation recoverable")
        .expect("generation two remains installed");
    assert_eq!(recovered.record(), first_commit.record());
    assert_eq!(recovered.namespace_root(), &first_root);
}

#[test]
fn every_generation_two_failpoint_recovers_only_the_previous_or_complete_next_root() {
    let policy = PolicySetId::new([0x52; 32]).expect("policy identity is nonzero");
    let probe_storage = MemoryStorageIo::new();
    let (probe_repository, old_root) = seed_first_generation(&probe_storage, policy);
    let baseline_operations = probe_storage.operation_count();
    let (new_root, new_record) =
        commit_hole_generation(&probe_repository, 5, 3).expect("probe generation must commit");
    assert_eq!(new_record.generation(), 3);
    let generation_two_operations = probe_storage.operations()[baseline_operations..].to_vec();
    assert_eq!(
        generation_two_operations.last(),
        Some(&StorageOperation::SyncFile),
        "the Commit WAL sync must be the final fallible storage operation"
    );
    assert!(!generation_two_operations.is_empty());

    for relative_position in 0..generation_two_operations.len() {
        let storage = MemoryStorageIo::with_fail_before(
            baseline_operations
                .checked_add(relative_position)
                .expect("bounded operation position"),
        );
        let (repository, seeded_root) = seed_first_generation(&storage, policy);
        assert_eq!(seeded_root, old_root);
        assert!(
            commit_hole_generation(&repository, 5, 3).is_err(),
            "fail-before position {relative_position} unexpectedly committed"
        );
        storage.crash();
        let recovered = GenerationRepository::new(storage, policy)
            .recover_latest()
            .expect("previous complete generation must recover")
            .expect("generation one must remain committed");
        assert_eq!(
            recovered.namespace_root(),
            &old_root,
            "fail-before position {relative_position} exposed a mixed generation"
        );
    }

    let final_sync_position = generation_two_operations.len() - 1;
    for relative_position in 0..generation_two_operations.len() {
        let storage = MemoryStorageIo::with_fail_after(
            baseline_operations
                .checked_add(relative_position)
                .expect("bounded operation position"),
        );
        let (repository, seeded_root) = seed_first_generation(&storage, policy);
        assert_eq!(seeded_root, old_root);
        assert!(
            commit_hole_generation(&repository, 5, 3).is_err(),
            "fail-after position {relative_position} must report its injected error"
        );
        storage.crash();
        let recovered = GenerationRepository::new(storage, policy)
            .recover_latest()
            .expect("one complete generation must recover")
            .expect("at least generation one must remain committed");
        let expected = if relative_position == final_sync_position {
            &new_root
        } else {
            &old_root
        };
        assert_eq!(
            recovered.namespace_root(),
            expected,
            "fail-after position {relative_position} exposed a mixed generation"
        );
    }
}

#[test]
fn every_nested_directory_failpoint_recovers_only_the_reservation_or_complete_tree() {
    let policy = PolicySetId::new([0x7B; 32]).expect("policy identity is nonzero");
    let old_root = reservation_root(1_024);
    let new_root = nested_directory_root();
    let probe_storage = MemoryStorageIo::new();
    let probe = GenerationRepository::new(probe_storage.clone(), policy);
    bootstrap_inode_reservation(&probe);
    let baseline = probe_storage.operation_count();
    probe
        .commit_namespace(&new_root)
        .expect("nested directory probe commits");
    let operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));

    for relative in 0..operations.len() {
        for fail_after in [false, true] {
            let storage = if fail_after {
                MemoryStorageIo::with_fail_after(baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(baseline + relative)
            };
            let repository = GenerationRepository::new(storage.clone(), policy);
            bootstrap_inode_reservation(&repository);
            assert!(repository.commit_namespace(&new_root).is_err());
            storage.crash();
            let recovered = GenerationRepository::new(storage, policy)
                .recover_latest()
                .expect("one whole directory generation recovers")
                .expect("reservation generation remains committed");
            let expected = if fail_after && relative + 1 == operations.len() {
                &new_root
            } else {
                &old_root
            };
            assert_eq!(
                recovered.namespace_root(),
                expected,
                "directory failpoint relative={relative} after={fail_after} exposed a mixed tree"
            );
        }
    }
}

#[test]
fn torn_and_invalid_wal_tails_recover_the_last_complete_generation() {
    let policy = PolicySetId::new([0x53; 32]).expect("policy identity is nonzero");

    let torn_storage = MemoryStorageIo::new();
    let (_, expected_root) = seed_first_generation(&torn_storage, policy);
    let committed_prefix_bytes = 2 * COMMIT_RECORD_BYTES;
    torn_storage
        .write_at("commit.wal", committed_prefix_bytes as u64, &[0xA5; 37])
        .expect("torn suffix write is admitted");
    torn_storage
        .sync_file("commit.wal")
        .expect("torn suffix is made durable for the recovery test");
    torn_storage.crash();
    let recovered = GenerationRepository::new(torn_storage, policy)
        .recover_latest()
        .expect("valid WAL prefix must recover")
        .expect("generation one is complete");
    assert_eq!(recovered.namespace_root(), &expected_root);
    assert_eq!(
        recovered.wal_tail(),
        &WalTail::Torn {
            valid_bytes: committed_prefix_bytes,
            tail_bytes: 37,
        }
    );

    let invalid_storage = MemoryStorageIo::new();
    let (_, expected_root) = seed_first_generation(&invalid_storage, policy);
    invalid_storage
        .write_at(
            "commit.wal",
            committed_prefix_bytes as u64,
            &[0; COMMIT_RECORD_BYTES],
        )
        .expect("invalid complete record write is admitted");
    invalid_storage
        .sync_file("commit.wal")
        .expect("invalid record is made durable for the recovery test");
    invalid_storage.crash();
    let recovered = GenerationRepository::new(invalid_storage, policy)
        .recover_latest()
        .expect("valid WAL prefix must recover")
        .expect("generation one is complete");
    assert_eq!(recovered.namespace_root(), &expected_root);
    assert_eq!(
        recovered.wal_tail(),
        &WalTail::InvalidRecord {
            offset: committed_prefix_bytes,
        }
    );
}

#[test]
fn corrupt_newest_namespace_object_falls_back_as_one_whole_generation() {
    let policy = PolicySetId::new([0x54; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let (_, new_record) =
        commit_hole_generation(&repository, 5, 3).expect("generation two must commit");
    let newest_root_name = metadata_name(new_record.namespace_root());
    let mut newest_root_bytes = storage
        .read(&newest_root_name)
        .expect("newest root object exists");
    newest_root_bytes[100] ^= 1;
    storage
        .write_at(&newest_root_name, 100, &newest_root_bytes[100..101])
        .expect("corruption is injected into the newest root");
    storage
        .sync_file(&newest_root_name)
        .expect("corruption is durable for the recovery test");
    storage.crash();

    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("the earlier complete graph must recover")
        .expect("generation one remains reachable");
    assert_eq!(recovered.record().generation(), 2);
    assert_eq!(recovered.namespace_root(), &old_root);
    assert_eq!(recovered.rejected_newer_generations(), 1);
    assert_eq!(recovered.wal_tail(), &WalTail::Clean);
}

#[test]
fn missing_newest_namespace_shard_falls_back_as_one_whole_generation() {
    let policy = PolicySetId::new([0x56; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let (_, new_record) =
        commit_hole_generation(&repository, 5, 3).expect("generation two must commit");
    let descriptor = NamespaceGraphRoot::decode(
        &storage
            .read(&metadata_name(new_record.namespace_root()))
            .expect("newest descriptor exists"),
    )
    .expect("newest descriptor verifies");
    let shard_name = metadata_name(descriptor.shards()[0].object_id());
    storage
        .remove_file(&shard_name)
        .expect("remove one durable child shard");
    storage.sync_root().expect("make missing shard durable");
    storage.crash();

    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("the earlier complete graph must recover")
        .expect("generation one remains reachable");
    assert_eq!(recovered.record().generation(), 2);
    assert_eq!(recovered.namespace_root(), &old_root);
    assert_eq!(recovered.rejected_newer_generations(), 1);
}

#[test]
fn fallback_keeps_the_newest_valid_inode_reservation_high_water() {
    let policy = PolicySetId::new([0x5C; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let new_root = hole_file_root_with_reservation(manifest_id, 8, 3, 2_048);
    let new_record = repository
        .commit_namespace(&new_root)
        .expect("generation two advances the reservation");
    let newest_root_name = metadata_name(new_record.namespace_root());
    let mut newest_root_bytes = storage
        .read(&newest_root_name)
        .expect("newest root object exists");
    newest_root_bytes[100] ^= 1;
    storage
        .write_at(&newest_root_name, 100, &newest_root_bytes[100..101])
        .expect("corrupt newest root");
    storage
        .sync_file(&newest_root_name)
        .expect("make corruption durable");
    storage.crash();

    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("earlier root recovers while WAL high-water remains authoritative")
        .expect("generation one is complete");
    assert_eq!(recovered.namespace_root(), &old_root);
    assert_eq!(recovered.inode_reservation_end_high_water(), 2_048);
}

#[test]
fn data_manifest_commits_and_recovers_only_through_a_verified_container_source() {
    let policy = PolicySetId::new([0x55; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let payload = b"abcdefgh";
    let chunk_id = ChunkId::of(payload);
    containers
        .publish_raw(
            ContainerId::new([0x71; 16]).expect("container identity is nonzero"),
            1,
            &[payload.as_slice()],
        )
        .expect("publish durable DATA location");

    let repository = GenerationRepository::new(storage.clone(), policy);
    let manifest = ManifestLeaf::new(
        payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: payload.len() as u64,
            chunk_id,
        }],
    )
    .expect("DATA manifest is valid");
    let manifest_id = repository
        .publish_manifest(&manifest)
        .expect("publish DATA manifest");
    let root = hole_file_root(manifest_id, payload.len() as u64, 1);

    assert!(matches!(
        repository.commit_namespace(&root),
        Err(GenerationError::DataLocationsNotConnected)
    ));
    bootstrap_inode_reservation(&repository);
    let committed = repository
        .commit_namespace_with_data(&root, &containers)
        .expect("verified DATA location authorizes the generation");
    assert_eq!(committed.generation(), 2);

    storage.crash();
    let reopened_containers = ContainerRepository::new(storage.clone());
    assert_eq!(
        reopened_containers
            .read_verified_chunk(chunk_id, payload.len() as u64)
            .expect("demand read re-verifies the container and Chunk ID"),
        payload
    );
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest_with_data(&reopened_containers)
        .expect("recovery re-verifies the reachable DATA location")
        .expect("one complete DATA generation exists");
    assert_eq!(recovered.namespace_root(), &root);
}

#[test]
fn healthy_recovery_verifies_only_the_newest_structurally_valid_data_graph() {
    let policy = PolicySetId::new([0x75; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(container_storage.clone());
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let accepting_verifier = AcceptAllRequiredChunks;
    bootstrap_inode_reservation(&repository);

    let mut historical_manifest_id = None;
    let mut latest_root = None;
    let mut latest_chunk_id = None;
    for (mutation_sequence, payload) in [
        (1, b"generation-two".as_slice()),
        (2, b"generation-three".as_slice()),
        (3, b"generation-four".as_slice()),
    ] {
        let chunk_id = ChunkId::of(payload);
        let manifest = ManifestLeaf::new(
            payload.len() as u64,
            vec![ManifestExtent::Data {
                logical_length: payload.len() as u64,
                chunk_id,
            }],
        )
        .expect("DATA manifest is valid");
        let manifest_id = repository
            .publish_manifest(&manifest)
            .expect("publish DATA manifest");
        historical_manifest_id.get_or_insert(manifest_id);
        let root = hole_file_root(manifest_id, payload.len() as u64, mutation_sequence);
        repository
            .commit_namespace_with_verified_files_using(&root, &containers, &accepting_verifier)
            .expect("complete dependency proof authorizes the generation");
        latest_root = Some(root);
        latest_chunk_id = Some(chunk_id);
    }
    let historical_manifest_name =
        metadata_name(historical_manifest_id.expect("historical Manifest ID was recorded"));
    let mut historical_manifest = metadata
        .read(&historical_manifest_name)
        .expect("historical Manifest exists");
    historical_manifest[100] ^= 1;
    metadata
        .write_at(
            &historical_manifest_name,
            100,
            &historical_manifest[100..101],
        )
        .expect("inject historical Manifest corruption");
    metadata
        .sync_file(&historical_manifest_name)
        .expect("make historical Manifest corruption durable");
    metadata.crash();
    container_storage.crash();

    let recording_verifier = RecordingRequiredChunks::default();
    let recovered = GenerationRepository::new(metadata, policy)
        .recover_latest_with_verified_files_using(&containers, &recording_verifier)
        .expect("healthy latest generation recovers")
        .expect("one committed generation exists");

    assert_eq!(recovered.generation().record().generation(), 4);
    assert_eq!(
        recovered.generation().namespace_root(),
        latest_root.as_ref().expect("latest root was recorded")
    );
    assert_eq!(
        recording_verifier.calls(),
        vec![vec![latest_chunk_id.expect("latest Chunk ID was recorded")]],
        "even a corrupt historical DATA graph must not block or be re-proven before a healthy current graph"
    );
}

#[test]
fn recovery_never_exposes_an_unpinned_historical_generation() {
    let policy = PolicySetId::new([0x76; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(container_storage.clone());
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let accepting_verifier = AcceptAllRequiredChunks;
    bootstrap_inode_reservation(&repository);

    let mut chunk_ids = Vec::new();
    for (mutation_sequence, payload) in [
        (1, b"unpinned-old".as_slice()),
        (2, b"pinned-previous".as_slice()),
        (3, b"pinned-current".as_slice()),
    ] {
        let chunk_id = ChunkId::of(payload);
        let manifest = ManifestLeaf::new(
            payload.len() as u64,
            vec![ManifestExtent::Data {
                logical_length: payload.len() as u64,
                chunk_id,
            }],
        )
        .expect("DATA manifest is valid");
        let manifest_id = repository
            .publish_manifest(&manifest)
            .expect("publish DATA manifest");
        let root = hole_file_root(manifest_id, payload.len() as u64, mutation_sequence);
        repository
            .commit_namespace_with_verified_files_using(&root, &containers, &accepting_verifier)
            .expect("complete dependency proof authorizes the generation");
        chunk_ids.push(chunk_id);
    }
    metadata.crash();
    container_storage.crash();

    let recording_verifier = RecordingRequiredChunks::rejecting(vec![chunk_ids[1], chunk_ids[2]]);
    let error = GenerationRepository::new(metadata, policy)
        .recover_latest_with_verified_files_using(&containers, &recording_verifier)
        .expect_err("current and previous failure must not expose older history");

    assert!(matches!(error, GenerationError::NoRecoverableGeneration));
    assert_eq!(
        recording_verifier.calls(),
        vec![vec![chunk_ids[2]], vec![chunk_ids[1]]],
        "only the current and immediately previous pinned graphs are candidates"
    );
}

#[test]
fn missing_data_location_prevents_commit_and_corrupt_newest_data_falls_back() {
    let policy = PolicySetId::new([0x56; 32]).expect("policy identity is nonzero");
    let missing_storage = MemoryStorageIo::new();
    let missing_containers = ContainerRepository::new(missing_storage.clone());
    let missing_repository = GenerationRepository::new(missing_storage, policy);
    let missing_payload = b"missing";
    let missing_manifest = ManifestLeaf::new(
        missing_payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: missing_payload.len() as u64,
            chunk_id: ChunkId::of(missing_payload),
        }],
    )
    .expect("DATA manifest is valid");
    let missing_manifest_id = missing_repository
        .publish_manifest(&missing_manifest)
        .expect("manifest object may precede its DATA location");
    let missing_root = hole_file_root(missing_manifest_id, missing_payload.len() as u64, 1);
    assert!(matches!(
        missing_repository.commit_namespace_with_data(&missing_root, &missing_containers),
        Err(GenerationError::Store(
            fastdup_store::StoreError::MissingVerifiedChunk { .. }
        ))
    ));

    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let containers = ContainerRepository::new(storage.clone());
    let payload = b"generation-two-data";
    let container_id = ContainerId::new([0x72; 16]).expect("container identity is nonzero");
    containers
        .publish_raw(container_id, 1, &[payload.as_slice()])
        .expect("publish generation-two DATA");
    let manifest = ManifestLeaf::new(
        payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: payload.len() as u64,
            chunk_id: ChunkId::of(payload),
        }],
    )
    .expect("DATA manifest is valid");
    let manifest_id = repository
        .publish_manifest(&manifest)
        .expect("publish generation-two manifest");
    let data_root = hole_file_root(manifest_id, payload.len() as u64, 3);
    let record = repository
        .commit_namespace_with_data(&data_root, &containers)
        .expect("commit verified DATA generation");
    assert_eq!(record.generation(), 3);
    assert!(matches!(
        repository.recover_latest(),
        Err(GenerationError::DataLocationsNotConnected)
    ));

    let container_name = format!("{}.fdc", "72".repeat(16));
    let mut bytes = storage
        .read(&container_name)
        .expect("published DATA container exists");
    bytes[5_000] ^= 1;
    storage
        .write_at(&container_name, 5_000, &bytes[5_000..5_001])
        .expect("inject DATA corruption");
    storage
        .sync_file(&container_name)
        .expect("make DATA corruption durable");
    storage.crash();

    let reopened_containers = ContainerRepository::new(storage.clone());
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest_with_data(&reopened_containers)
        .expect("invalid newest DATA graph falls back atomically")
        .expect("generation one remains complete");
    assert_eq!(recovered.record().generation(), 2);
    assert_eq!(recovered.namespace_root(), &old_root);
    assert_eq!(recovered.rejected_newer_generations(), 1);
}

#[test]
fn retry_makes_an_existing_live_wal_name_durable_before_acknowledging_commit() {
    let policy = PolicySetId::new([0x57; 32]).expect("policy identity is nonzero");
    let probe_storage = MemoryStorageIo::new();
    let probe_repository = GenerationRepository::new(probe_storage.clone(), policy);
    let root = reservation_root(1_024);
    let baseline = probe_storage.operation_count();
    probe_repository
        .commit_namespace(&root)
        .expect("probe commit succeeds");
    let relative_wal_root_sync = probe_storage.operations()[baseline..]
        .iter()
        .rposition(|operation| *operation == StorageOperation::SyncRoot)
        .expect("initial WAL creation has a directory sync");

    let storage = MemoryStorageIo::with_fail_before(baseline + relative_wal_root_sync);
    let repository = GenerationRepository::new(storage.clone(), policy);
    assert!(repository.commit_namespace(&root).is_err());
    let committed = repository
        .commit_namespace(&root)
        .expect("retry must durably publish the already-live WAL name");
    storage.crash();
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("retry-acknowledged generation must recover")
        .expect("generation one must not disappear with its WAL name");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &root);
}

#[test]
fn retry_makes_an_existing_live_root_name_durable_before_wal_commit() {
    let policy = PolicySetId::new([0x58; 32]).expect("policy identity is nonzero");
    let probe_storage = MemoryStorageIo::new();
    let (probe_repository, old_root) = seed_first_generation(&probe_storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let new_root = hole_file_root(manifest_id, 8, 3);
    let baseline = probe_storage.operation_count();
    probe_repository
        .commit_namespace(&new_root)
        .expect("probe generation two succeeds");
    let relative_root_sync = probe_storage.operations()[baseline..]
        .iter()
        .position(|operation| *operation == StorageOperation::SyncRoot)
        .expect("new Namespace Root publication has a directory sync");

    let storage = MemoryStorageIo::with_fail_before(baseline + relative_root_sync);
    let (repository, seeded_root) = seed_first_generation(&storage, policy);
    assert_eq!(seeded_root, old_root);
    assert!(repository.commit_namespace(&new_root).is_err());
    let committed = repository
        .commit_namespace(&new_root)
        .expect("retry must durably publish the already-live root name");
    storage.crash();
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("retry-acknowledged root must recover")
        .expect("generation two must remain reachable");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &new_root);
}

#[test]
fn recovery_propagates_transient_graph_io_instead_of_rolling_back() {
    let policy = PolicySetId::new([0x59; 32]).expect("policy identity is nonzero");
    let probe = MemoryStorageIo::new();
    seed_first_generation(&probe, policy);
    let recovery_root_read_position = probe.operation_count() + 1;

    let storage = MemoryStorageIo::with_fail_before(recovery_root_read_position);
    seed_first_generation(&storage, policy);
    let error = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect_err("transient metadata I/O must not be classified as corruption");
    match error {
        GenerationError::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::Interrupted),
        other => panic!("transient graph I/O was hidden by fallback: {other:?}"),
    }
}

#[test]
fn recovery_refuses_to_roll_back_past_an_unknown_newest_policy() {
    let old_policy = PolicySetId::new([0x5A; 32]).expect("policy identity is nonzero");
    let new_policy = PolicySetId::new([0x5B; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (_, old_root) = seed_first_generation(&storage, old_policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let new_root = hole_file_root(manifest_id, 8, 3);
    let new_record = GenerationRepository::new(storage.clone(), new_policy)
        .commit_namespace(&new_root)
        .expect("a later writer policy creates generation two");
    assert_eq!(new_record.generation(), 3);
    storage.crash();

    assert!(matches!(
        GenerationRepository::new(storage, old_policy).recover_latest(),
        Err(GenerationError::UnsupportedPolicySet {
            generation: 3,
            policy_set,
        }) if policy_set == new_policy
    ));
}

#[test]
fn writer_rejects_decreasing_commit_cutoff_or_inode_reservation() {
    let policy = PolicySetId::new([0x5D; 32]).expect("policy identity is nonzero");

    let cutoff_storage = MemoryStorageIo::new();
    let (cutoff_repository, old_root) = seed_first_generation(&cutoff_storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let decreasing_cutoff = hole_file_root_with_reservation(manifest_id, 8, 0, 1_024);
    assert!(matches!(
        cutoff_repository.commit_namespace(&decreasing_cutoff),
        Err(GenerationError::NonMonotonicNamespaceMutation {
            previous: 1,
            proposed: 0,
        })
    ));

    let reservation_storage = MemoryStorageIo::new();
    let (reservation_repository, old_root) = seed_first_generation(&reservation_storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let decreasing_reservation = hole_file_root_with_reservation(manifest_id, 8, 3, 512);
    assert!(matches!(
        reservation_repository.commit_namespace(&decreasing_reservation),
        Err(GenerationError::NonMonotonicInodeReservation {
            previous: 1_024,
            proposed: 512,
        })
    ));

    let inode_storage = MemoryStorageIo::new();
    let (inode_repository, old_root) = seed_first_generation(&inode_storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let decreasing_inode = NamespaceRoot::new(
        1_024,
        3,
        3,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_001, 1, 0, 8, manifest_id)
                .expect("locally valid inode version"),
        ],
        vec![
            NamespaceEntry::new(1, 2, vec![b'v', b'm', b'-', 0xff])
                .expect("raw byte name is valid"),
        ],
    )
    .expect("root is locally valid before transition verification");
    assert!(matches!(
        inode_repository.commit_namespace(&decreasing_inode),
        Err(GenerationError::NonMonotonicInodeMutation {
            inode: 2,
            previous: 1,
            proposed: 0,
        })
    ));
}

#[test]
fn canonical_metadata_and_wal_lookups_do_not_scan_the_publication_directory() {
    let policy = PolicySetId::new([0x5E; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    seed_first_generation(&storage, policy);
    assert!(
        !storage.operations().contains(&StorageOperation::ListNames),
        "canonical metadata publication must not grow linearly with directory size"
    );
}

#[test]
fn commit_log_rotates_with_bounded_segments_and_recovers_the_latest_generation() {
    const COMMIT_COUNT: u64 = 130;
    const MAX_SEGMENT_BYTES: u64 = 64 * COMMIT_RECORD_BYTES as u64;

    let policy = PolicySetId::new([0x61; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let root = reservation_root(1_024);

    let mut latest = None;
    for expected_generation in 1..=COMMIT_COUNT {
        let committed = repository
            .commit_namespace(&root)
            .expect("bounded Generation Log must not stop at a segment boundary");
        assert_eq!(committed.generation(), expected_generation);
        latest = Some(committed);
    }

    for name in storage
        .list_names()
        .expect("durable object names are enumerable in the test adapter")
        .into_iter()
        .filter(|name| name == "commit.wal" || name == "commit.1.wal")
    {
        assert!(
            storage
                .object_len(&name)
                .expect("Generation Log object has a length")
                <= MAX_SEGMENT_BYTES,
            "Generation Log segment {name} exceeded its bounded on-disk size"
        );
    }

    storage.crash();
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("bounded Generation Log recovers after repeated rotation")
        .expect("at least one committed generation exists");
    assert_eq!(recovered.record(), latest.expect("one commit was recorded"));
    assert_eq!(recovered.namespace_root(), &root);
}

fn seed_generation_log_rotation_boundary(
    storage: &MemoryStorageIo,
    policy: PolicySetId,
) -> (
    GenerationRepository<MemoryStorageIo>,
    fastdup_format::CommitRecord,
) {
    let repository = GenerationRepository::new(storage.clone(), policy);
    let root = reservation_root(1_024);
    let mut latest = None;
    for _ in 0..64 {
        latest = Some(
            repository
                .commit_namespace(&root)
                .expect("seed Commit must remain below the rotation boundary"),
        );
    }
    (
        repository,
        latest.expect("the boundary contains Commit Records"),
    )
}

#[test]
fn every_rotation_failpoint_recovers_only_the_previous_or_complete_next_generation() {
    let policy = PolicySetId::new([0x62; 32]).expect("policy identity is nonzero");
    let previous_root = reservation_root(1_024);
    let next_root = reservation_root(2_048);

    let probe_storage = MemoryStorageIo::new();
    let (probe_repository, previous_record) =
        seed_generation_log_rotation_boundary(&probe_storage, policy);
    assert_eq!(previous_record.generation(), 64);
    let baseline = probe_storage.operation_count();
    let next_record = probe_repository
        .commit_namespace(&next_root)
        .expect("probe rotation Commit succeeds");
    assert_eq!(next_record.generation(), 65);
    let operations = probe_storage.operations()[baseline..].to_vec();
    assert_eq!(
        operations.last(),
        Some(&StorageOperation::SyncFile),
        "the rotated-slot sync remains the sole Commit point"
    );

    for relative_position in 0..operations.len() {
        let storage = MemoryStorageIo::with_fail_before(baseline + relative_position);
        let (repository, seeded_record) = seed_generation_log_rotation_boundary(&storage, policy);
        assert_eq!(seeded_record, previous_record);
        assert!(repository.commit_namespace(&next_root).is_err());
        storage.crash();
        let recovered = GenerationRepository::new(storage, policy)
            .recover_latest()
            .expect("the previous rotation slot remains recoverable")
            .expect("the previous generation remains committed");
        assert_eq!(
            recovered.record(),
            previous_record,
            "fail-before position {relative_position} exposed the rotating slot"
        );
        assert_eq!(recovered.namespace_root(), &previous_root);
    }

    let commit_point = operations.len() - 1;
    for relative_position in 0..operations.len() {
        let storage = MemoryStorageIo::with_fail_after(baseline + relative_position);
        let (repository, seeded_record) = seed_generation_log_rotation_boundary(&storage, policy);
        assert_eq!(seeded_record, previous_record);
        assert!(repository.commit_namespace(&next_root).is_err());
        storage.crash();
        let recovered = GenerationRepository::new(storage, policy)
            .recover_latest()
            .expect("one complete rotation slot remains recoverable")
            .expect("at least the previous generation remains committed");
        let expected = if relative_position == commit_point {
            next_record
        } else {
            previous_record
        };
        assert_eq!(
            recovered.record(),
            expected,
            "fail-after position {relative_position} exposed a mixed rotation"
        );
        assert_eq!(
            recovered.namespace_root(),
            if relative_position == commit_point {
                &next_root
            } else {
                &previous_root
            }
        );
    }
}

#[test]
#[ignore = "manual lifetime gate crosses sixteen thousand Commit generations"]
fn commit_log_crosses_sixteen_thousand_generations() {
    const COMMIT_COUNT: u64 = 16_400;

    let policy = PolicySetId::new([0x63; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let root = reservation_root(1_024);
    let mut latest = None;
    for expected_generation in 1..=COMMIT_COUNT {
        let record = repository
            .commit_namespace(&root)
            .expect("slot rotation must remain lifetime-bounded");
        assert_eq!(record.generation(), expected_generation);
        latest = Some(record);
    }

    storage.crash();
    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("the long-running Generation Log recovers")
        .expect("the lifetime gate committed at least one generation");
    assert_eq!(recovered.record(), latest.expect("one commit was recorded"));
    assert_eq!(recovered.namespace_root(), &root);
}

#[test]
fn manifest_tree_reuses_unchanged_leaves_and_recovers_through_its_root() {
    const EXTENT_COUNT: usize = 2_500;
    const EXTENT_BYTES: u64 = 64 * 1_024;

    let policy = PolicySetId::new([0x64; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let build_manifest = |changed_fill: u8| {
        let extents = (0..EXTENT_COUNT)
            .map(|ordinal| {
                if ordinal % 2 == 0 {
                    ManifestExtent::Hole {
                        logical_length: EXTENT_BYTES,
                    }
                } else {
                    ManifestExtent::Fill {
                        logical_length: EXTENT_BYTES,
                        value: if ordinal == 1_201 { changed_fill } else { 0x5A },
                    }
                }
            })
            .collect();
        ManifestLeaf::new(
            u64::try_from(EXTENT_COUNT).expect("fixture count fits u64") * EXTENT_BYTES,
            extents,
        )
        .expect("worked large Manifest is valid")
    };

    let first = build_manifest(0x5A);
    let first_root = repository
        .publish_manifest(&first)
        .expect("large Manifest tree publishes child-first");
    let first_root_bytes = storage
        .read(&metadata_name(first_root))
        .expect("published Manifest root exists");
    assert_eq!(
        metadata_object_kind(&first_root_bytes).expect("root envelope verifies"),
        MetadataObjectKind::ManifestInnerNode
    );
    let first_inner = ManifestInnerNode::decode(&first_root_bytes).expect("root node verifies");
    assert_eq!(first_inner.level(), 1);
    assert_eq!(first_inner.children().len(), 3);
    assert_eq!(
        repository
            .read_manifest(first_root)
            .expect("tree flattens through its public read seam"),
        first
    );
    let objects_after_first = storage
        .list_names()
        .expect("test storage names are enumerable")
        .into_iter()
        .filter(|name| is_metadata_name(name))
        .count();
    assert_eq!(
        objects_after_first, 3,
        "two byte-identical full leaves share one object; the tail leaf and root are distinct"
    );

    let second = build_manifest(0xA5);
    let second_root = repository
        .publish_manifest(&second)
        .expect("one changed range publishes a successor tree");
    assert_ne!(second_root, first_root);
    let objects_after_second = storage
        .list_names()
        .expect("test storage names are enumerable")
        .into_iter()
        .filter(|name| is_metadata_name(name))
        .count();
    assert_eq!(
        objects_after_second - objects_after_first,
        2,
        "only one changed leaf and its root path are new"
    );

    bootstrap_inode_reservation(&repository);
    let root = hole_file_root(
        second_root,
        u64::try_from(EXTENT_COUNT).expect("fixture count fits u64") * EXTENT_BYTES,
        1,
    );
    let committed = repository
        .commit_namespace(&root)
        .expect("generation verification traverses the Manifest tree");
    storage.crash();
    let reopened = GenerationRepository::new(storage, policy);
    let recovered = reopened
        .recover_latest()
        .expect("recovery traverses the complete Manifest tree")
        .expect("tree-bearing generation is committed");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &root);
    assert_eq!(
        reopened
            .read_manifest(second_root)
            .expect("reopened tree remains byte-exact"),
        second
    );
}

#[test]
fn manifest_tree_adds_levels_without_materializing_logical_file_bytes() {
    const LEAF_COUNT: usize = 1_025;
    const LEAF_BYTES: u64 = 64 * 1_024 * 1_024;

    let policy = PolicySetId::new([0x65; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let manifest = ManifestLeaf::new(
        u64::try_from(LEAF_COUNT).expect("fixture count fits u64") * LEAF_BYTES,
        vec![
            ManifestExtent::Fill {
                logical_length: LEAF_BYTES,
                value: 0xC3,
            };
            LEAF_COUNT
        ],
    )
    .expect("large logical Manifest is structurally valid");

    let root = repository
        .publish_manifest(&manifest)
        .expect("multi-level Manifest publishes");
    let root_bytes = storage
        .read(&metadata_name(root))
        .expect("multi-level root exists");
    let root_node = ManifestInnerNode::decode(&root_bytes).expect("multi-level root verifies");
    assert_eq!(root_node.level(), 2);
    assert_eq!(root_node.children().len(), 2);
    assert_eq!(root_node.file_length(), manifest.file_length());
    assert_eq!(
        repository
            .read_manifest(root)
            .expect("bounded traversal reconstructs the logical recipe"),
        manifest
    );
}

#[test]
fn corrupt_nonempty_rotation_slot_never_rolls_back_beyond_the_live_pair() {
    let policy = PolicySetId::new([0x66; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let root = reservation_root(1_024);
    for expected_generation in 1..=67 {
        assert_eq!(
            repository
                .commit_namespace(&root)
                .expect("rotation seed commits")
                .generation(),
            expected_generation
        );
    }
    storage
        .write_at("commit.1.wal", 0, b"X")
        .expect("inject corruption into the active nonempty slot");
    storage
        .sync_file("commit.1.wal")
        .expect("make active-slot corruption durable");
    storage.crash();

    assert!(matches!(
        GenerationRepository::new(storage, policy).recover_latest(),
        Err(GenerationError::NoRecoverableGeneration)
    ));
}

#[test]
fn rotation_fallback_keeps_the_new_high_water_and_never_exposes_older_history() {
    let policy = PolicySetId::new([0x67; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let old_root = reservation_root(1_024);
    let mut record_64 = None;
    for _ in 0..64 {
        record_64 = Some(
            repository
                .commit_namespace(&old_root)
                .expect("seed reaches the rotation boundary"),
        );
    }
    let root_65 = reservation_root(2_048);
    let record_65 = repository
        .commit_namespace(&root_65)
        .expect("rotation advances the durable reservation high-water");
    let root_66 = reservation_root(4_096);
    let record_66 = repository
        .commit_namespace(&root_66)
        .expect("successor advances the durable reservation high-water again");
    assert_eq!(record_65.generation(), 65);
    assert_eq!(record_66.generation(), 66);

    let corrupt_root = |record: fastdup_format::CommitRecord| {
        let name = metadata_name(record.namespace_root());
        let mut bytes = storage.read(&name).expect("Namespace Root exists");
        bytes[100] ^= 1;
        storage
            .write_at(&name, 100, &bytes[100..101])
            .expect("inject Namespace Root corruption");
        storage
            .sync_file(&name)
            .expect("make Namespace Root corruption durable");
    };
    corrupt_root(record_66);
    storage.crash();
    let recovered = GenerationRepository::new(storage.clone(), policy)
        .recover_latest()
        .expect("the immediate predecessor remains a recovery candidate")
        .expect("generation 65 remains live");
    assert_eq!(recovered.record(), record_65);
    assert_eq!(recovered.namespace_root(), &root_65);
    assert_eq!(recovered.inode_reservation_end_high_water(), 4_096);
    assert_eq!(recovered.rejected_newer_generations(), 1);

    corrupt_root(record_65);
    storage.crash();
    assert!(matches!(
        GenerationRepository::new(storage, policy).recover_latest(),
        Err(GenerationError::NoRecoverableGeneration)
    ));
    assert_eq!(record_64.expect("boundary seed exists").generation(), 64);
}

#[test]
fn corrupt_changed_manifest_leaf_falls_back_as_one_complete_tree_generation() {
    const EXTENT_COUNT: usize = 1_100;
    const EXTENT_BYTES: u64 = 64 * 1_024;

    let policy = PolicySetId::new([0x68; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let repository = GenerationRepository::new(storage.clone(), policy);
    let manifest = |changed_value: u8| {
        ManifestLeaf::new(
            u64::try_from(EXTENT_COUNT).expect("fixture count fits u64") * EXTENT_BYTES,
            (0..EXTENT_COUNT)
                .map(|ordinal| ManifestExtent::Fill {
                    logical_length: EXTENT_BYTES,
                    value: if ordinal == 1_050 {
                        changed_value
                    } else {
                        0x31
                    },
                })
                .collect(),
        )
        .expect("tree fallback fixture is valid")
    };
    bootstrap_inode_reservation(&repository);

    let first_manifest = manifest(0x31);
    let first_root_id = repository
        .publish_manifest(&first_manifest)
        .expect("publish first tree");
    let logical_size = first_manifest.file_length();
    let first_root = hole_file_root(first_root_id, logical_size, 1);
    let first_record = repository
        .commit_namespace(&first_root)
        .expect("commit first complete tree");

    let second_manifest = manifest(0x32);
    let second_root_id = repository
        .publish_manifest(&second_manifest)
        .expect("publish changed tree");
    let second_root = hole_file_root(second_root_id, logical_size, 2);
    repository
        .commit_namespace(&second_root)
        .expect("commit second complete tree");

    let first_node = ManifestInnerNode::decode(
        &storage
            .read(&metadata_name(first_root_id))
            .expect("first root exists"),
    )
    .expect("first root verifies");
    let second_node = ManifestInnerNode::decode(
        &storage
            .read(&metadata_name(second_root_id))
            .expect("second root exists"),
    )
    .expect("second root verifies");
    let changed_child = second_node
        .children()
        .iter()
        .map(|child| child.child())
        .find(|child| {
            !first_node
                .children()
                .iter()
                .any(|prior| prior.child() == *child)
        })
        .expect("one changed leaf has a new identity");
    let changed_name = metadata_name(changed_child);
    let mut bytes = storage.read(&changed_name).expect("changed leaf exists");
    bytes[100] ^= 1;
    storage
        .write_at(&changed_name, 100, &bytes[100..101])
        .expect("inject changed-leaf corruption");
    storage
        .sync_file(&changed_name)
        .expect("make changed-leaf corruption durable");
    storage.crash();

    let recovered = GenerationRepository::new(storage, policy)
        .recover_latest()
        .expect("tree corruption permits exactly one atomic fallback")
        .expect("the prior complete tree remains live");
    assert_eq!(recovered.record(), first_record);
    assert_eq!(recovered.namespace_root(), &first_root);
    assert_eq!(recovered.rejected_newer_generations(), 1);
}

#[test]
fn a_removed_inode_id_cannot_reappear_below_the_allocation_cursor() {
    let policy = PolicySetId::new([0x5F; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let without_inode = NamespaceRoot::new(1_024, 3, 3, Vec::new(), Vec::new())
        .expect("generation two durably unlinks the only inode");
    repository
        .commit_namespace(&without_inode)
        .expect("removing the inode is a valid transition");

    let reused_inode = hole_file_root_with_reservation(manifest_id, 8, 5, 1_024);
    assert!(matches!(
        repository.commit_namespace(&reused_inode),
        Err(GenerationError::ReusedInodeId {
            inode: 2,
            previous_allocation_cursor: 3,
        })
    ));
}

#[test]
fn a_new_inode_range_must_be_durable_one_generation_before_use() {
    let policy = PolicySetId::new([0x60; 32]).expect("policy identity is nonzero");
    let storage = MemoryStorageIo::new();
    let (repository, old_root) = seed_first_generation(&storage, policy);
    let manifest_id = old_root.inodes()[0].manifest_root();
    let premature = NamespaceRoot::new(
        2_048,
        1_025,
        3,
        vec![
            DurableInode::new(1_024, 0o640, 1_000, 1_001, 1, 3, 8, manifest_id)
                .expect("locally valid inode in the newly proposed range"),
        ],
        vec![NamespaceEntry::new(1, 1_024, b"too-early".to_vec()).expect("valid root entry")],
    )
    .expect("root is locally valid before transition verification");
    assert!(matches!(
        repository.commit_namespace(&premature),
        Err(
            GenerationError::AllocationExceededPreviouslyDurableReservation {
                previous_reservation_end: 1_024,
                proposed_allocation_cursor: 1_025,
            }
        )
    ));
}
