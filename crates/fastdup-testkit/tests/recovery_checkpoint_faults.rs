use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use fastdup_format::{
    ChunkId, DurableInode, DurableXattr, ManifestExtent, ManifestLeaf, NamespaceEntry,
    NamespaceRoot, PolicySetId,
};
use fastdup_store::{
    ContainerRepository, GenerationRepository, RecoveryCheckpointRepository, RequiredChunkVerifier,
    StorageIo, StoreError,
};
use fastdup_testkit::{MemoryStorageIo, PausedStorageIo, StorageOperation};

#[derive(Debug)]
struct AcceptAllRequiredChunks;

impl RequiredChunkVerifier for AcceptAllRequiredChunks {
    fn verify_required_chunks(&self, _required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug)]
struct RejectingRequiredChunks {
    rejected: ChunkId,
    calls: Mutex<u64>,
}

impl RejectingRequiredChunks {
    fn new(rejected: ChunkId) -> Self {
        Self {
            rejected,
            calls: Mutex::new(0),
        }
    }

    fn calls(&self) -> u64 {
        *self.calls.lock().expect("recording verifier lock is valid")
    }
}

impl RequiredChunkVerifier for RejectingRequiredChunks {
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        *self.calls.lock().expect("recording verifier lock is valid") += 1;
        if let Some(logical_length) = required.get(&self.rejected) {
            return Err(StoreError::MissingVerifiedChunk {
                chunk_id: self.rejected,
                logical_length: *logical_length,
            });
        }
        Ok(())
    }
}

fn policy() -> PolicySetId {
    PolicySetId::new([0xd1; 32]).expect("fixture policy is nonzero")
}

fn reservation_root() -> NamespaceRoot {
    NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("empty reservation root is valid")
}

fn visible_root(manifest_root: fastdup_format::MetadataObjectId) -> NamespaceRoot {
    visible_root_at(manifest_root, 64 * 1_024, 1)
}

fn visible_root_at(
    manifest_root: fastdup_format::MetadataObjectId,
    logical_size: u64,
    mutation_sequence: u64,
) -> NamespaceRoot {
    NamespaceRoot::new(
        1_024,
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
        vec![NamespaceEntry::new(1, 2, b"backup.img".to_vec()).expect("namespace entry is valid")],
    )
    .expect("namespace graph is valid")
}

fn seed_source() -> (
    GenerationRepository<MemoryStorageIo>,
    fastdup_format::CommitRecord,
    NamespaceRoot,
) {
    let metadata = MemoryStorageIo::new();
    let source = GenerationRepository::new(metadata, policy());
    source
        .commit_namespace(&reservation_root())
        .expect("reserve inode identities before visibility");
    let manifest = ManifestLeaf::new(
        64 * 1_024,
        vec![ManifestExtent::Hole {
            logical_length: 64 * 1_024,
        }],
    )
    .expect("hole Manifest is valid");
    let manifest_root = source
        .publish_manifest(&manifest)
        .expect("publish source Manifest");
    let namespace = visible_root(manifest_root);
    let committed = source
        .commit_namespace(&namespace)
        .expect("commit source generation");
    (source, committed, namespace)
}

#[test]
fn metadata_tier_loss_recovers_the_latest_self_contained_checkpoint() {
    let (source, committed, namespace) = seed_source();

    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data.clone());
    let published = checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish one self-contained DATA-tier checkpoint")
        .expect("one committed source generation exists");
    assert_eq!(published.generation(), committed.generation());
    assert_eq!(published.metadata_object_count(), 3);
    data.crash();

    let replacement_metadata = MemoryStorageIo::new();
    let replacement = GenerationRepository::new(replacement_metadata.clone(), policy());
    let recovered = checkpoints
        .recover_latest(&replacement, &AcceptAllRequiredChunks)
        .expect("recover without the source Metadata tier")
        .expect("one complete DATA-tier checkpoint exists");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &namespace);

    replacement_metadata.crash();
    let reopened = GenerationRepository::new(replacement_metadata, policy())
        .recover_latest()
        .expect("the restored Metadata tier reopens")
        .expect("the restored Commit anchor is durable");
    assert_eq!(reopened.record(), committed);
    assert_eq!(reopened.namespace_root(), &namespace);
}

#[test]
fn metadata_tier_loss_recovers_namespace_spanning_many_metadata_objects() {
    let source = GenerationRepository::new(MemoryStorageIo::new(), policy());
    source
        .commit_namespace(&reservation_root())
        .expect("reserve inode identities before visibility");
    let empty_manifest = source
        .publish_manifest(&ManifestLeaf::new(0, Vec::new()).expect("empty Manifest"))
        .expect("publish empty Manifest");
    let value = vec![0x5A; 60 * 1_024];
    let mut inodes = Vec::new();
    let mut entries = Vec::new();
    for ordinal in 0_u64..280 {
        let inode = ordinal + 2;
        inodes.push(
            DurableInode::new_with_metadata(
                inode,
                0o600,
                1_000,
                1_000,
                1,
                ordinal + 1,
                0,
                empty_manifest,
                0,
                vec![DurableXattr::new(b"user.large".to_vec(), value.clone()).unwrap()],
            )
            .expect("large namespace inode"),
        );
        entries.push(
            NamespaceEntry::new(1, inode, format!("file-{ordinal:04}").into_bytes())
                .expect("large namespace entry"),
        );
    }
    let namespace = NamespaceRoot::new(1_024, 282, 280, inodes, entries)
        .expect("namespace larger than one object");
    let committed = source
        .commit_namespace(&namespace)
        .expect("commit sharded namespace");

    let checkpoint_storage = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(checkpoint_storage.clone());
    let summary = checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish complete sharded graph")
        .expect("committed generation exists");
    assert!(summary.metadata_object_count() > 3);
    checkpoint_storage.crash();

    let replacement_storage = MemoryStorageIo::new();
    let replacement = GenerationRepository::new(replacement_storage.clone(), policy());
    let recovered = checkpoints
        .recover_latest(&replacement, &AcceptAllRequiredChunks)
        .expect("install all Namespace Shards")
        .expect("complete checkpoint exists");
    assert_eq!(recovered.record(), committed);
    assert_eq!(recovered.namespace_root(), &namespace);
    replacement_storage.crash();
    assert_eq!(
        GenerationRepository::new(replacement_storage, policy())
            .recover_latest()
            .expect("reopen installed graph")
            .expect("installed generation exists")
            .namespace_root(),
        &namespace
    );
}

#[test]
fn corrupt_newest_checkpoint_is_rejected_by_scrub_and_recovery_selects_the_previous_one() {
    let (source, first_record, first_namespace) = seed_source();
    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data.clone());
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish first checkpoint");

    let second_size = 128 * 1_024;
    let second_manifest = ManifestLeaf::new(
        second_size,
        vec![ManifestExtent::Hole {
            logical_length: second_size,
        }],
    )
    .expect("second Manifest is valid");
    let second_manifest_root = source
        .publish_manifest(&second_manifest)
        .expect("publish second Manifest");
    let second_namespace = visible_root_at(second_manifest_root, second_size, 2);
    let second_record = source
        .commit_namespace(&second_namespace)
        .expect("commit second visible generation");
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish second checkpoint");

    let newest_name = format!(
        "recovery-checkpoint.{:016x}.fdrc",
        second_record.generation()
    );
    let mut first_header_byte = data
        .read_exact_at(&newest_name, 104, 1)
        .expect("read one authenticated header byte");
    first_header_byte[0] ^= 0x80;
    data.write_at(&newest_name, 104, &first_header_byte)
        .expect("inject durable checkpoint corruption");
    data.sync_file(&newest_name)
        .expect("make corruption durable");
    data.sync_root().expect("retain corrupted published name");
    data.crash();

    assert!(checkpoints.scrub(&AcceptAllRequiredChunks).is_err());
    let replacement = GenerationRepository::new(MemoryStorageIo::new(), policy());
    let recovered = checkpoints
        .recover_latest(&replacement, &AcceptAllRequiredChunks)
        .expect("recovery falls back by whole checkpoint")
        .expect("the previous checkpoint remains complete");
    assert_eq!(recovered.record(), first_record);
    assert_eq!(recovered.namespace_root(), &first_namespace);
}

#[test]
fn torn_inactive_head_is_repaired_by_the_next_checkpoint_publication() {
    let (source, _, _) = seed_source();
    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data.clone());
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish first checkpoint");

    let second_size = 128 * 1_024;
    let second_manifest = ManifestLeaf::new(
        second_size,
        vec![ManifestExtent::Hole {
            logical_length: second_size,
        }],
    )
    .expect("second Manifest is valid");
    let second_manifest_root = source
        .publish_manifest(&second_manifest)
        .expect("publish second Manifest");
    let second_namespace = visible_root_at(second_manifest_root, second_size, 2);
    let second_record = source
        .commit_namespace(&second_namespace)
        .expect("commit second visible generation");
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish second checkpoint");

    let inactive_head = "recovery-checkpoint.0.head";
    let head_length = data
        .object_len(inactive_head)
        .expect("inactive Head has a durable fixed length");
    let mut corrupted_byte = data
        .read_exact_at(inactive_head, 128, 1)
        .expect("read one inactive Head byte");
    corrupted_byte[0] ^= 0x80;
    data.write_at(inactive_head, 128, &corrupted_byte)
        .expect("tear the inactive Head without changing its length");
    data.sync_file(inactive_head)
        .expect("make the inactive-Head tear durable");
    assert_eq!(
        data.object_len(inactive_head)
            .expect("torn inactive Head remains present"),
        head_length,
        "the fault must preserve the valid-looking Head length"
    );
    data.crash();

    let before_repair = GenerationRepository::new(MemoryStorageIo::new(), policy());
    let recovered = checkpoints
        .recover_latest(&before_repair, &AcceptAllRequiredChunks)
        .expect("the valid active Head remains recoverable")
        .expect("second checkpoint remains selected");
    assert_eq!(recovered.record(), second_record);
    assert_eq!(recovered.namespace_root(), &second_namespace);

    let third_size = 192 * 1_024;
    let third_manifest = ManifestLeaf::new(
        third_size,
        vec![ManifestExtent::Hole {
            logical_length: third_size,
        }],
    )
    .expect("third Manifest is valid");
    let third_manifest_root = source
        .publish_manifest(&third_manifest)
        .expect("publish third Manifest");
    let third_namespace = visible_root_at(third_manifest_root, third_size, 3);
    let third_record = source
        .commit_namespace(&third_namespace)
        .expect("commit third visible generation");
    let published = checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("a torn inactive Head must be reusable")
        .expect("third checkpoint is newer");
    assert_eq!(published.generation(), third_record.generation());
    checkpoints
        .scrub(&AcceptAllRequiredChunks)
        .expect("publication replaces the torn inactive Head with a valid selector");

    data.crash();
    let replacement = GenerationRepository::new(MemoryStorageIo::new(), policy());
    let recovered = checkpoints
        .recover_latest(&replacement, &AcceptAllRequiredChunks)
        .expect("recover after repairing the inactive Head")
        .expect("third checkpoint remains selected");
    assert_eq!(recovered.record(), third_record);
    assert_eq!(recovered.namespace_root(), &third_namespace);
}

#[test]
fn every_checkpoint_publication_fault_recovers_only_absence_or_the_complete_checkpoint() {
    let (source, committed, namespace) = seed_source();
    let probe_storage = MemoryStorageIo::new();
    RecoveryCheckpointRepository::new(probe_storage.clone())
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("probe publication succeeds");
    let operation_count = probe_storage.operation_count();
    assert!(operation_count > 0);

    for fail_before in 0..operation_count {
        let data = MemoryStorageIo::with_fail_before(fail_before);
        let checkpoints = RecoveryCheckpointRepository::new(data.clone());
        assert!(
            checkpoints
                .publish(&source, &AcceptAllRequiredChunks)
                .is_err(),
            "operation {fail_before} must be interrupted"
        );
        data.crash();
        let replacement = GenerationRepository::new(MemoryStorageIo::new(), policy());
        let recovered = checkpoints
            .recover_latest(&replacement, &AcceptAllRequiredChunks)
            .unwrap_or_else(|error| panic!("operation {fail_before} recovery failed: {error}"));
        if let Some(recovered) = recovered {
            assert_eq!(recovered.record(), committed);
            assert_eq!(recovered.namespace_root(), &namespace);
        }
    }

    for fail_after in 0..operation_count {
        let data = MemoryStorageIo::with_fail_after(fail_after);
        let checkpoints = RecoveryCheckpointRepository::new(data.clone());
        let result = checkpoints.publish(&source, &AcceptAllRequiredChunks);
        assert!(
            result.is_err(),
            "operation {fail_after} must report failure"
        );
        data.crash();
        let replacement = GenerationRepository::new(MemoryStorageIo::new(), policy());
        let recovered = checkpoints
            .recover_latest(&replacement, &AcceptAllRequiredChunks)
            .unwrap_or_else(|error| panic!("operation {fail_after} recovery failed: {error}"));
        if let Some(recovered) = recovered {
            assert_eq!(recovered.record(), committed);
            assert_eq!(recovered.namespace_root(), &namespace);
        }
    }
}

#[test]
fn every_metadata_installation_fault_is_idempotently_retryable() {
    let (source, committed, namespace) = seed_source();
    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data);
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish stable checkpoint");

    let probe_target = MemoryStorageIo::new();
    let probe_repository = GenerationRepository::new(probe_target.clone(), policy());
    checkpoints
        .recover_latest(&probe_repository, &AcceptAllRequiredChunks)
        .expect("probe installation succeeds");
    let operation_count = probe_target.operation_count();
    assert!(operation_count > 0);

    for fail_before in 0..operation_count {
        let metadata = MemoryStorageIo::with_fail_before(fail_before);
        let target = GenerationRepository::new(metadata.clone(), policy());
        assert!(
            checkpoints
                .recover_latest(&target, &AcceptAllRequiredChunks)
                .is_err(),
            "operation {fail_before} must be interrupted"
        );
        metadata.crash();
        let retry = GenerationRepository::new(metadata, policy());
        let recovered = checkpoints
            .recover_latest(&retry, &AcceptAllRequiredChunks)
            .unwrap_or_else(|error| panic!("operation {fail_before} retry failed: {error}"))
            .expect("retry installs or reuses the complete anchor");
        assert_eq!(recovered.record(), committed);
        assert_eq!(recovered.namespace_root(), &namespace);
    }

    for fail_after in 0..operation_count {
        let metadata = MemoryStorageIo::with_fail_after(fail_after);
        let target = GenerationRepository::new(metadata.clone(), policy());
        let result = checkpoints.recover_latest(&target, &AcceptAllRequiredChunks);
        if result.is_ok() {
            continue;
        }
        metadata.crash();
        let retry = GenerationRepository::new(metadata, policy());
        let recovered = checkpoints
            .recover_latest(&retry, &AcceptAllRequiredChunks)
            .unwrap_or_else(|error| panic!("operation {fail_after} retry failed: {error}"))
            .expect("retry installs or reuses the complete anchor");
        assert_eq!(recovered.record(), committed);
        assert_eq!(recovered.namespace_root(), &namespace);
    }

    assert!(
        probe_target
            .operations()
            .contains(&StorageOperation::SyncRoot),
        "Metadata objects become directory-durable before the Commit anchor"
    );
}

#[test]
fn missing_data_dependency_blocks_metadata_installation_before_any_target_mutation() {
    let metadata = MemoryStorageIo::new();
    let source = GenerationRepository::new(metadata, policy());
    source
        .commit_namespace(&reservation_root())
        .expect("reserve inode identities before visibility");
    let chunk_id = ChunkId::of(b"required recovery payload");
    let manifest = ManifestLeaf::new(
        25,
        vec![ManifestExtent::Data {
            logical_length: 25,
            chunk_id,
        }],
    )
    .expect("DATA Manifest is valid");
    let manifest_root = source
        .publish_manifest(&manifest)
        .expect("publish source Manifest");
    source
        .commit_namespace(&visible_root_at(manifest_root, 25, 1))
        .expect_err("ordinary commit refuses an unverified DATA dependency");
    let committed = source
        .commit_namespace_with_verified_files_using(
            &visible_root_at(manifest_root, 25, 1),
            &ContainerRepository::new(MemoryStorageIo::new()),
            &AcceptAllRequiredChunks,
        )
        .expect("fixture verifier establishes source DATA")
        .record();
    assert_eq!(committed.generation(), 2);

    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data);
    let published = checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish DATA-bearing checkpoint")
        .expect("source generation exists");
    assert_eq!(published.required_chunk_count(), 1);

    let target_storage = MemoryStorageIo::new();
    let target = GenerationRepository::new(target_storage.clone(), policy());
    let rejecting = RejectingRequiredChunks::new(chunk_id);
    assert!(checkpoints.recover_latest(&target, &rejecting).is_err());
    assert!(rejecting.calls() > 0);
    assert_eq!(
        target_storage.operation_count(),
        0,
        "checkpoint and DATA verification precede every Metadata-tier mutation"
    );
}

#[test]
fn healthy_publication_retains_only_the_current_and_previous_complete_checkpoints() {
    let (source, _, _) = seed_source();
    let data = MemoryStorageIo::new();
    let checkpoints = RecoveryCheckpointRepository::new(data.clone());
    checkpoints
        .publish(&source, &AcceptAllRequiredChunks)
        .expect("publish first checkpoint");

    for mutation_sequence in 2..=3 {
        let logical_size = mutation_sequence * 64 * 1_024;
        let manifest = ManifestLeaf::new(
            logical_size,
            vec![ManifestExtent::Hole {
                logical_length: logical_size,
            }],
        )
        .expect("successor Manifest is valid");
        let manifest_root = source
            .publish_manifest(&manifest)
            .expect("publish successor Manifest");
        source
            .commit_namespace(&visible_root_at(
                manifest_root,
                logical_size,
                mutation_sequence,
            ))
            .expect("commit successor Namespace");
        checkpoints
            .publish(&source, &AcceptAllRequiredChunks)
            .expect("publish successor checkpoint");
    }

    let mut names = data
        .list_names()
        .expect("list retained checkpoint publications")
        .into_iter()
        .filter(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("fdrc"))
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "recovery-checkpoint.0000000000000003.fdrc".to_owned(),
            "recovery-checkpoint.0000000000000004.fdrc".to_owned(),
        ]
    );
    let scrub = checkpoints
        .scrub(&AcceptAllRequiredChunks)
        .expect("both retained checkpoints are complete");
    assert_eq!(scrub.checkpoint_count(), 2);
    assert_eq!(scrub.first_generation(), Some(3));
    assert_eq!(scrub.latest_generation(), Some(4));
}

#[test]
fn blocked_hdd_checkpoint_publication_does_not_hold_the_commit_lock() {
    let (source, selected, _) = seed_source();
    let successor_manifest = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x5a,
        }],
    )
    .expect("successor Manifest is valid");
    let successor_manifest_root = source
        .publish_manifest(&successor_manifest)
        .expect("stage successor Manifest before the scheduling probe");
    let successor = visible_root_at(successor_manifest_root, 4_096, 2);

    let data = MemoryStorageIo::new();
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        data,
        StorageOperation::WriteAt,
        ".recovery-checkpoint.",
    );
    let checkpoints = RecoveryCheckpointRepository::new(paused.clone());
    paused.arm();
    let publishing_source = source.clone();
    let publisher_thread =
        thread::spawn(move || checkpoints.publish(&publishing_source, &AcceptAllRequiredChunks));
    assert!(paused.wait_until_reached(Duration::from_secs(1)));

    let (committed_tx, committed_rx) = mpsc::sync_channel(1);
    let committing_source = source.clone();
    let commit_thread = thread::spawn(move || {
        committed_tx
            .send(committing_source.commit_namespace(&successor))
            .expect("commit observer remains alive");
    });
    let received_commit = committed_rx.recv_timeout(Duration::from_secs(1));
    paused.resume();
    let commit = received_commit
        .expect("ordinary Commit must not wait for Recovery-Checkpoint DATA I/O")
        .expect("successor Commit succeeds while DATA publication is paused");
    commit_thread.join().expect("Commit thread does not panic");
    let checkpoint = publisher_thread
        .join()
        .expect("Recovery-Checkpoint thread does not panic")
        .expect("Recovery Checkpoint completes after DATA resumes")
        .expect("selected generation exists");

    assert_eq!(checkpoint.generation(), selected.generation());
    assert_eq!(commit.generation(), selected.generation() + 1);
}
