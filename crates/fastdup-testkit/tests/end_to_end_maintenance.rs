use std::collections::BTreeMap;

use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ExactIndexEntry, ExactIndexFormatError, ExactIndexLocation,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, GcCandidateCatalogRow,
    ManifestExtent, ManifestLeaf, NamespaceEntry, NamespaceRoot, PolicySetId, SealedContainer,
};
use fastdup_store::{
    ContainerRepository, DataPoolUsage, ExactIndexRunRepository, ExactIndexStoreError,
    GcCandidateCatalogRepository, GcCandidateSelectionMode, GenerationRepository, MaintenanceError,
    MaintenanceExecutionMode, MaintenancePriority, MaintenanceRepository, MetadataGcExactReason,
    MetadataGcMarkMode, OnlineGcCycleOutcome, OnlineGcRunMode, SimilarityIndexRepository,
    StorageIo, StoreError,
};
use fastdup_testkit::{MemoryStorageIo, PausedStorageIo, StorageOperation};
use std::time::Duration;

fn seeded_repositories() -> (
    GenerationRepository<MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
    ExactIndexProfileId,
) {
    seeded_repositories_using(MemoryStorageIo::new(), MemoryStorageIo::new())
}

fn seeded_repositories_using(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> (
    GenerationRepository<MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
    ExactIndexProfileId,
) {
    let policy = PolicySetId::new([0x81; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0x82; 32]).expect("profile ID is nonzero");
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(metadata);

    let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("initial reservation root is valid");
    generations
        .commit_namespace(&reservation)
        .expect("reserve inode range before visibility");

    let first = b"maintenance-first-chunk";
    let second = b"maintenance-second-chunk";
    containers
        .publish_raw(
            ContainerId::new([0x83; 16]).expect("container ID is nonzero"),
            1,
            &[first, second],
        )
        .expect("publish fixture Container");
    let manifest = ManifestLeaf::new(
        u64::try_from(first.len()).expect("fixture length fits u64"),
        vec![ManifestExtent::Data {
            logical_length: u64::try_from(first.len()).expect("fixture length fits u64"),
            chunk_id: ChunkId::of(first),
        }],
    )
    .expect("fixture Manifest is valid");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish fixture Manifest");
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        1,
        vec![
            DurableInode::new(
                2,
                0o640,
                1_000,
                1_000,
                1,
                1,
                u64::try_from(first.len()).expect("fixture length fits u64"),
                manifest_root,
            )
            .expect("fixture inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("fixture name is valid")],
    )
    .expect("fixture Namespace Root is valid");
    generations
        .commit_namespace_with_data(&namespace, &containers)
        .expect("commit fixture DATA generation");

    (generations, containers, indexes, profile)
}

#[test]
fn metadata_gc_removes_an_uncommitted_manifest_without_touching_the_committed_graph() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let orphan = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x5a,
        }],
    )
    .expect("orphan Manifest is valid");
    let orphan_root = generations
        .publish_manifest(&orphan)
        .expect("publish an uncommitted Metadata graph");
    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);

    let report = maintenance
        .garbage_collect_metadata()
        .expect("collect unreachable Metadata Objects");

    assert_eq!(report.objects_removed(), 1);
    assert!(report.bytes_removed() != 0);
    assert!(generations.read_manifest(orphan_root).is_err());
    maintenance
        .scrub()
        .expect("Metadata GC retains the complete committed graph");
    assert_eq!(
        metadata
            .list_names()
            .expect("list retained Metadata names")
            .iter()
            .filter(|name| name.strip_suffix(".fdm").is_some())
            .count(),
        3,
        "reservation root, committed Namespace root, and committed Manifest remain"
    );
}

#[test]
fn unchanged_metadata_gc_cycle_uses_the_clean_mark_catalog() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata, MemoryStorageIo::new());
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let first = maintenance
        .garbage_collect_metadata()
        .expect("establish the exact Metadata mark catalog");
    assert!(first.exact_mark_performed());
    assert_eq!(first.mark_mode(), MetadataGcMarkMode::ExactSnapshot);
    assert_eq!(
        first.exact_reason(),
        Some(MetadataGcExactReason::ProcessStart)
    );
    assert!(first.metrics().object_graph_read_bytes() > 0);
    assert!(first.metrics().catalog_write_bytes() > 0);
    assert_eq!(first.metrics().catalog_chain_runs(), 1);

    let unchanged = maintenance
        .garbage_collect_metadata()
        .expect("reuse the unchanged clean Metadata mark catalog");
    assert!(!unchanged.exact_mark_performed());
    assert_eq!(unchanged.mark_mode(), MetadataGcMarkMode::Reused);
    assert_eq!(unchanged.exact_reason(), None);
    assert_eq!(unchanged.metrics().catalog_write_bytes(), 0);
    assert_eq!(unchanged.objects_removed(), 0);
    assert_eq!(unchanged.objects_retained(), first.objects_retained());
    assert_eq!(first.catalog_generation(), Some(1));
    assert_eq!(unchanged.catalog_generation(), Some(1));
}

#[test]
fn committed_successor_advances_metadata_catalog_with_an_additive_delta_run() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    let exact = maintenance
        .garbage_collect_metadata()
        .expect("establish the exact Metadata mark catalog");
    assert_eq!(exact.catalog_generation(), Some(1));

    let installed = generations
        .recover_latest_with_data(&containers)
        .expect("recover installed predecessor")
        .expect("fixture has an installed predecessor");
    let predecessor =
        fastdup_store::SuccessorPredecessor::from_committed_record(installed.record());
    let manifest = ManifestLeaf::new(
        8_192,
        vec![ManifestExtent::Fill {
            logical_length: 8_192,
            value: 0x4d,
        }],
    )
    .expect("successor Manifest is valid");
    let proof = generations
        .publish_manifest_successor(predecessor, &manifest)
        .expect("publish proof-bearing successor Metadata");
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        2,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 2, 8_192, proof.summary().root())
                .expect("successor inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("successor name is valid")],
    )
    .expect("successor Namespace Root is valid");
    let _committed = generations
        .commit_namespace_with_successor_proofs_using(
            &namespace,
            &containers,
            predecessor,
            &[proof],
            &containers,
        )
        .expect("commit proof-bearing successor");
    assert_eq!(
        metadata
            .list_names()
            .expect("list catalog names before maintenance")
            .iter()
            .filter(|name| name.starts_with("metadata-mark-catalog-"))
            .count(),
        1,
        "the frontend commit path journals only in RAM and publishes no catalog file"
    );

    let delta = maintenance
        .garbage_collect_metadata()
        .expect("publish an additive Metadata root delta");

    assert!(!delta.exact_mark_performed());
    assert_eq!(delta.mark_mode(), MetadataGcMarkMode::AdditionDelta);
    assert_eq!(delta.exact_reason(), None);
    assert!(delta.metrics().catalog_write_bytes() > 0);
    assert_eq!(delta.metrics().catalog_chain_runs(), 2);
    assert_eq!(delta.objects_removed(), 0);
    assert_eq!(delta.catalog_generation(), Some(2));
    assert_eq!(delta.objects_retained(), exact.objects_retained() + 2);
    maintenance
        .scrub()
        .expect("snapshot plus additive Metadata delta remains scrub-valid");
    assert_eq!(
        metadata
            .list_names()
            .expect("list Metadata catalog runs")
            .iter()
            .filter(|name| name.starts_with("metadata-mark-catalog-"))
            .count(),
        2,
        "the exact snapshot and additive delta are both retained"
    );
}

#[test]
fn path_local_successors_classify_every_new_metadata_node_as_additive() {
    #[derive(Clone, Copy, Debug)]
    enum PathEdit {
        Replacement,
        Truncate,
        Splice,
    }

    for edit in [PathEdit::Replacement, PathEdit::Truncate, PathEdit::Splice] {
        let (generations, containers, indexes, profile) = seeded_repositories();
        let maintenance =
            MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
        maintenance
            .garbage_collect_metadata()
            .expect("establish exact Metadata mark state");
        let installed = generations
            .recover_latest_with_data(&containers)
            .expect("recover installed predecessor")
            .expect("fixture has an installed predecessor");
        let predecessor =
            fastdup_store::SuccessorPredecessor::from_committed_record(installed.record());
        let inode = installed
            .namespace_root()
            .file_inodes()
            .next()
            .expect("fixture contains one file inode");
        let original = generations
            .read_manifest(inode.manifest_root())
            .expect("read installed Manifest");
        let proof = generations
            .publish_manifest_successor(predecessor, &original)
            .expect("reopen installed graph as a successor proof");
        let logical_size = original.file_length();
        let proof = match edit {
            PathEdit::Replacement => generations
                .publish_manifest_replacement_successor(
                    proof,
                    0..logical_size,
                    &[ManifestExtent::Fill {
                        logical_length: logical_size,
                        value: 0x44,
                    }],
                )
                .expect("publish path-local replacement nodes"),
            PathEdit::Truncate => generations
                .publish_manifest_truncate_successor(proof, logical_size - 1)
                .expect("publish truncate-local nodes"),
            PathEdit::Splice => generations
                .publish_manifest_splice_successor(
                    proof,
                    0..1,
                    &[ManifestExtent::Fill {
                        logical_length: 2,
                        value: 0x55,
                    }],
                )
                .expect("publish splice-local nodes"),
        };
        let namespace = NamespaceRoot::new(
            1_024,
            3,
            2,
            vec![
                DurableInode::new(
                    2,
                    0o640,
                    1_000,
                    1_000,
                    1,
                    2,
                    proof.summary().logical_size(),
                    proof.summary().root(),
                )
                .expect("successor inode is valid"),
            ],
            vec![
                NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("successor name is valid"),
            ],
        )
        .expect("successor Namespace Root is valid");
        generations
            .commit_namespace_with_successor_proofs_using(
                &namespace,
                &containers,
                predecessor,
                &[proof],
                &containers,
            )
            .expect("commit path-local proof-bearing successor");

        let delta = maintenance
            .garbage_collect_metadata()
            .expect("persist classified path-local additions");
        assert_eq!(
            delta.mark_mode(),
            MetadataGcMarkMode::AdditionDelta,
            "{edit:?} must remain additive"
        );
        assert!(!delta.exact_mark_performed());
        maintenance
            .scrub()
            .expect("path-local Metadata delta remains scrub-valid");
    }
}

#[test]
fn blocked_metadata_delta_io_does_not_block_a_frontend_commit() {
    let metadata = MemoryStorageIo::new();
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        metadata,
        StorageOperation::WriteAt,
        ".metadata-mark-catalog-",
    );
    let policy = PolicySetId::new([0xd1; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0xd2; 32]).expect("profile ID is nonzero");
    let generations = GenerationRepository::new(paused.clone(), policy);
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let indexes = ExactIndexRunRepository::new(paused.clone());
    let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("initial reservation is valid");
    let first = generations
        .commit_namespace(&reservation)
        .expect("commit initial reservation");
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .garbage_collect_metadata()
        .expect("establish exact Metadata catalog before arming the pause");

    let predecessor = fastdup_store::SuccessorPredecessor::from_committed_record(first);
    let manifest = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x6d,
        }],
    )
    .expect("delta Manifest is valid");
    let proof = generations
        .publish_manifest_successor(predecessor, &manifest)
        .expect("publish proof-bearing successor");
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        2,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 2, 4_096, proof.summary().root())
                .expect("delta inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"delta.vbk".to_vec()).expect("name is valid")],
    )
    .expect("delta Namespace is valid");
    generations
        .commit_namespace_with_successor_proofs_using(
            &namespace,
            &containers,
            predecessor,
            &[proof],
            &containers,
        )
        .expect("commit proof-bearing successor");

    paused.arm();
    let collecting = maintenance.clone();
    let collector = std::thread::spawn(move || collecting.garbage_collect_metadata());
    assert!(
        paused.wait_until_reached(Duration::from_secs(2)),
        "Metadata delta publication reaches its deliberately blocked catalog write"
    );

    let committing = generations.clone();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let frontend = std::thread::spawn(move || {
        let empty = NamespaceRoot::new(1_024, 3, 3, Vec::new(), Vec::new())
            .expect("empty frontend successor is valid");
        completed_tx
            .send(committing.commit_namespace(&empty))
            .expect("frontend completion receiver remains available");
    });
    let frontend_result = completed_rx.recv_timeout(Duration::from_secs(1));
    paused.resume();

    frontend_result
        .expect("frontend Commit must not wait for Metadata delta file I/O")
        .expect("concurrent frontend Commit succeeds");
    frontend.join().expect("frontend thread remains healthy");
    let delta = collector
        .join()
        .expect("Metadata-GC thread remains healthy")
        .expect("blocked delta publication resumes safely");
    assert_eq!(delta.mark_mode(), MetadataGcMarkMode::AdditionDelta);
    let exact = maintenance
        .garbage_collect_metadata()
        .expect("concurrent legacy Commit forces an exact follow-up");
    assert_eq!(exact.mark_mode(), MetadataGcMarkMode::ExactSnapshot);
    assert_eq!(
        exact.exact_reason(),
        Some(MetadataGcExactReason::LegacyCommit)
    );
    maintenance
        .scrub()
        .expect("concurrent delta and frontend Commit remain scrub-clean");
}

#[test]
#[allow(clippy::too_many_lines)]
fn commit_log_rotation_forces_an_exact_metadata_mark_after_additive_deltas() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata, MemoryStorageIo::new());
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .garbage_collect_metadata()
        .expect("establish exact Metadata catalog");
    let installed = generations
        .recover_latest_with_data(&containers)
        .expect("recover installed predecessor")
        .expect("fixture has an installed predecessor");
    let mut predecessor =
        fastdup_store::SuccessorPredecessor::from_committed_record(installed.record());
    let manifest = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x72,
        }],
    )
    .expect("rotating successor Manifest is valid");
    let first_proof = generations
        .publish_manifest_successor(predecessor, &manifest)
        .expect("publish first rotating successor");
    let summary = first_proof.summary();

    for mutation_sequence in 2..=63 {
        let proof = if mutation_sequence == 2 {
            first_proof.clone()
        } else {
            generations.reuse_manifest_successor(predecessor, summary)
        };
        let namespace = NamespaceRoot::new(
            1_024,
            3,
            mutation_sequence,
            vec![
                DurableInode::new(
                    2,
                    0o640,
                    1_000,
                    1_000,
                    1,
                    mutation_sequence,
                    4_096,
                    summary.root(),
                )
                .expect("rotating successor inode is valid"),
            ],
            vec![
                NamespaceEntry::new(1, 2, b"backup.vbk".to_vec())
                    .expect("rotating successor name is valid"),
            ],
        )
        .expect("rotating successor Namespace is valid");
        let committed = generations
            .commit_namespace_with_successor_proofs_using(
                &namespace,
                &containers,
                predecessor,
                &[proof],
                &containers,
            )
            .expect("commit nonrotating successor");
        predecessor =
            fastdup_store::SuccessorPredecessor::from_committed_record(committed.record());
        if mutation_sequence <= 34 {
            let catalog = maintenance
                .garbage_collect_metadata()
                .expect("advance the bounded Metadata delta chain");
            if mutation_sequence == 34 {
                assert!(
                    catalog.exact_mark_performed(),
                    "the 32-run delta chain limit starts a fresh exact Snapshot"
                );
                assert_eq!(
                    catalog.exact_reason(),
                    Some(MetadataGcExactReason::DeltaChainLimit)
                );
            } else {
                assert!(!catalog.exact_mark_performed());
            }
        }
    }
    drop(first_proof);
    let delta = maintenance
        .garbage_collect_metadata()
        .expect("collapse nonrotating root additions into one delta run");
    assert!(!delta.exact_mark_performed());

    let proof = generations.reuse_manifest_successor(predecessor, summary);
    let rotating_namespace = NamespaceRoot::new(
        1_024,
        3,
        64,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 64, 4_096, summary.root())
                .expect("WAL-rotating inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("WAL-rotating name is valid"),
        ],
    )
    .expect("WAL-rotating Namespace is valid");
    generations
        .commit_namespace_with_successor_proofs_using(
            &rotating_namespace,
            &containers,
            predecessor,
            &[proof],
            &containers,
        )
        .expect("rotate the bounded Commit WAL");

    let exact = maintenance
        .garbage_collect_metadata()
        .expect("rotation re-establishes exact Metadata deletion authority");

    assert!(exact.exact_mark_performed());
    assert_eq!(
        exact.exact_reason(),
        Some(MetadataGcExactReason::WalRotation)
    );
    assert!(exact.objects_removed() > 0);
    maintenance
        .scrub()
        .expect("rotation collection retains both protected Commit graphs");
}

#[test]
fn recovered_metadata_mark_catalog_forces_one_exact_refresh_before_reuse() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data.clone());
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let first = maintenance
        .garbage_collect_metadata()
        .expect("publish the first durable Metadata mark catalog");
    assert_eq!(first.catalog_generation(), Some(1));
    drop(maintenance);

    let reopened = MaintenanceRepository::new(
        GenerationRepository::new(
            metadata.clone(),
            PolicySetId::new([0x81; 32]).expect("policy ID is nonzero"),
        ),
        ContainerRepository::new(data),
        ExactIndexRunRepository::new(metadata),
        profile,
    );
    let recovered = reopened
        .garbage_collect_metadata()
        .expect("audit and refresh the recovered Metadata mark catalog");

    assert!(recovered.exact_mark_performed());
    assert_eq!(recovered.catalog_generation(), Some(2));
    let unchanged = reopened
        .garbage_collect_metadata()
        .expect("reuse the refreshed catalog in the reopened process");
    assert!(!unchanged.exact_mark_performed());
    assert_eq!(unchanged.catalog_generation(), Some(2));
}

#[test]
fn corrupt_metadata_mark_catalog_is_rebuilt_without_becoming_deletion_authority() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data.clone());
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    maintenance
        .garbage_collect_metadata()
        .expect("publish the first Metadata mark catalog");
    let catalog_name = metadata
        .list_names()
        .expect("list Metadata names")
        .into_iter()
        .find(|name| {
            name.starts_with("metadata-mark-catalog-") && name.strip_suffix(".run").is_some()
        })
        .expect("one published Metadata mark catalog exists");
    let row_byte = metadata
        .read_exact_at(&catalog_name, 4_096, 1)
        .expect("read one catalog row byte")[0];
    metadata
        .write_at(&catalog_name, 4_096, &[row_byte ^ 0xFF])
        .expect("inject catalog-row corruption");
    drop(maintenance);

    let reopened = MaintenanceRepository::new(
        GenerationRepository::new(
            metadata.clone(),
            PolicySetId::new([0x81; 32]).expect("policy ID is nonzero"),
        ),
        ContainerRepository::new(data),
        ExactIndexRunRepository::new(metadata.clone()),
        profile,
    );
    assert!(
        reopened.scrub().is_err(),
        "offline scrub must surface the damaged durable catalog"
    );
    let rebuilt = reopened
        .garbage_collect_metadata()
        .expect("ignore the damaged hint and rebuild from Metadata authority");

    assert!(rebuilt.exact_mark_performed());
    assert_eq!(rebuilt.catalog_generation(), Some(2));
    assert!(
        !metadata
            .exists(&catalog_name)
            .expect("check old catalog name")
    );
    reopened
        .scrub()
        .expect("catalog corruption never damaged the live Metadata graph");
}

#[test]
fn every_metadata_mark_catalog_publication_fault_retries_from_durable_authority() {
    fn fixture(
        metadata: MemoryStorageIo,
    ) -> MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo> {
        let (generations, containers, indexes, profile) =
            seeded_repositories_using(metadata, MemoryStorageIo::new());
        MaintenanceRepository::new(generations, containers, indexes, profile)
    }

    let probe_metadata = MemoryStorageIo::new();
    let probe = fixture(probe_metadata.clone());
    let baseline = probe_metadata.operation_count();
    probe
        .garbage_collect_metadata()
        .expect("probe Metadata mark catalog publication succeeds");
    let publication_operations = probe_metadata.operations()[baseline..].len();
    assert!(publication_operations > 5);

    for fail_after_effect in [false, true] {
        for relative in 0..publication_operations {
            let absolute = baseline + relative;
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let maintenance = fixture(metadata.clone());
            assert!(
                maintenance.garbage_collect_metadata().is_err(),
                "fault point {relative} must interrupt the first exact catalog publication"
            );
            metadata.crash();

            let retried = maintenance
                .garbage_collect_metadata()
                .expect("retry rebuilds the catalog from durable Commit authority");
            assert!(retried.exact_mark_performed());
            maintenance
                .scrub()
                .expect("every catalog publication interruption preserves the live graph");
        }
    }
}

#[test]
fn every_metadata_mark_delta_publication_fault_retries_without_exact_rebuild() {
    fn fixture(
        metadata: MemoryStorageIo,
    ) -> MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo> {
        let (generations, containers, indexes, profile) =
            seeded_repositories_using(metadata, MemoryStorageIo::new());
        let maintenance =
            MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
        maintenance
            .garbage_collect_metadata()
            .expect("establish exact Metadata catalog before fault injection");
        let installed = generations
            .recover_latest_with_data(&containers)
            .expect("recover installed predecessor")
            .expect("fixture has an installed predecessor");
        let predecessor =
            fastdup_store::SuccessorPredecessor::from_committed_record(installed.record());
        let manifest = ManifestLeaf::new(
            2_048,
            vec![ManifestExtent::Fill {
                logical_length: 2_048,
                value: 0x37,
            }],
        )
        .expect("delta-fault Manifest is valid");
        let proof = generations
            .publish_manifest_successor(predecessor, &manifest)
            .expect("publish delta-fault successor");
        let namespace = NamespaceRoot::new(
            1_024,
            3,
            2,
            vec![
                DurableInode::new(2, 0o640, 1_000, 1_000, 1, 2, 2_048, proof.summary().root())
                    .expect("delta-fault inode is valid"),
            ],
            vec![
                NamespaceEntry::new(1, 2, b"backup.vbk".to_vec())
                    .expect("delta-fault name is valid"),
            ],
        )
        .expect("delta-fault Namespace is valid");
        generations
            .commit_namespace_with_successor_proofs_using(
                &namespace,
                &containers,
                predecessor,
                &[proof],
                &containers,
            )
            .expect("commit delta-fault successor");
        maintenance
    }

    let probe_metadata = MemoryStorageIo::new();
    let probe = fixture(probe_metadata.clone());
    let baseline = probe_metadata.operation_count();
    probe
        .garbage_collect_metadata()
        .expect("probe additive delta publication succeeds");
    let publication_operations = probe_metadata.operation_count() - baseline;
    assert!(publication_operations > 5);

    for fail_after_effect in [false, true] {
        for relative in 0..publication_operations {
            let absolute = baseline + relative;
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let maintenance = fixture(metadata);
            assert!(
                maintenance.garbage_collect_metadata().is_err(),
                "fault point {relative} must interrupt additive delta publication"
            );
            let retried = maintenance
                .garbage_collect_metadata()
                .expect("retry resumes the same additive delta without a graph rebuild");
            assert!(!retried.exact_mark_performed());
            assert_eq!(retried.catalog_generation(), Some(2));
            maintenance
                .scrub()
                .expect("every additive delta publication interruption preserves authority");
        }
    }
}

#[test]
fn metadata_publication_invalidates_the_clean_mark_catalog() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata, MemoryStorageIo::new());
    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);
    maintenance
        .garbage_collect_metadata()
        .expect("establish the clean Metadata mark catalog");
    let orphan = generations
        .publish_manifest(
            &ManifestLeaf::new(
                8_192,
                vec![ManifestExtent::Fill {
                    logical_length: 8_192,
                    value: 0x3c,
                }],
            )
            .expect("orphan Manifest is valid"),
        )
        .expect("publish Metadata after the clean catalog");

    let refreshed = maintenance
        .garbage_collect_metadata()
        .expect("refresh the invalidated Metadata mark catalog");

    assert!(refreshed.exact_mark_performed());
    assert_eq!(
        refreshed.exact_reason(),
        Some(MetadataGcExactReason::MetadataRootPinDrain)
    );
    assert_eq!(refreshed.objects_removed(), 1);
    assert!(generations.read_manifest(orphan).is_err());
}

#[test]
fn dropping_an_uncommitted_successor_proof_forces_exact_metadata_collection() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata, MemoryStorageIo::new());
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .garbage_collect_metadata()
        .expect("establish clean Metadata catalog");
    let installed = generations
        .recover_latest_with_data(&containers)
        .expect("recover installed predecessor")
        .expect("fixture has an installed predecessor");
    let predecessor =
        fastdup_store::SuccessorPredecessor::from_committed_record(installed.record());
    let proof = generations
        .publish_manifest_successor(
            predecessor,
            &ManifestLeaf::new(
                12_288,
                vec![ManifestExtent::Fill {
                    logical_length: 12_288,
                    value: 0x63,
                }],
            )
            .expect("abandoned successor Manifest is valid"),
        )
        .expect("publish abandoned successor proof");
    let abandoned_root = proof.summary().root();
    drop(proof);

    let collected = maintenance
        .garbage_collect_metadata()
        .expect("collect Metadata after the final unpublished root pin drains");

    assert!(collected.exact_mark_performed());
    assert!(collected.objects_removed() > 0);
    assert!(generations.read_manifest(abandoned_root).is_err());
}

#[test]
fn adaptive_online_gc_collects_metadata_in_the_idle_io_worker() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let orphan = ManifestLeaf::new(
        4_097,
        vec![ManifestExtent::Fill {
            logical_length: 4_097,
            value: 0x6b,
        }],
    )
    .expect("online orphan Manifest is valid");
    let orphan_root = generations
        .publish_manifest(&orphan)
        .expect("publish online Metadata garbage");
    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("online scheduler requires active Exact state");
    let catalog = GcCandidateCatalogRepository::new(metadata);

    let cycle = maintenance
        .run_adaptive_online_gc_cycle(
            &catalog,
            DataPoolUsage::new(50, 100).expect("fixture pool usage is valid"),
            OnlineGcRunMode::Background,
        )
        .expect("adaptive cycle includes Metadata GC");

    assert_eq!(cycle.metadata_gc().objects_removed(), 1);
    assert!(cycle.metadata_gc().bytes_removed() != 0);
    assert!(generations.read_manifest(orphan_root).is_err());
    maintenance
        .scrub()
        .expect("online Metadata collection retains the committed graph");
}

#[test]
fn metadata_gc_retains_a_manifest_pinned_by_a_long_lived_reader() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata, MemoryStorageIo::new());
    let recovered = generations
        .recover_latest_with_verified_files(&containers)
        .expect("recover the committed DATA graph")
        .expect("fixture has a committed generation");
    let (_, mut files) = recovered.into_parts();
    let file = files.remove(0).into_file();

    for mutation_sequence in 2..=65 {
        let empty = NamespaceRoot::new(1_024, 3, mutation_sequence, Vec::new(), Vec::new())
            .expect("empty successor Namespace is valid");
        generations
            .commit_namespace_with_data(&empty, &containers)
            .expect("rotate the old file generation out of the bounded WAL");
    }
    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);

    maintenance
        .garbage_collect_metadata()
        .expect("collect Metadata outside retained WAL and reader pins");
    assert_eq!(
        file.read_at(0, 8).expect("pinned reader remains usable"),
        b"maintena"
    );

    drop(file);
    let drained = maintenance
        .garbage_collect_metadata()
        .expect("collect the graph after its final reader pin drains");
    assert!(drained.objects_removed() != 0);
    maintenance
        .scrub()
        .expect("pin drain leaves the current Metadata graph valid");
}

#[test]
fn online_gc_retains_data_pinned_by_a_long_lived_reader() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data);
    let recovered = generations
        .recover_latest_with_verified_files(&containers)
        .expect("recover the committed DATA graph")
        .expect("fixture has a committed generation");
    let (_, mut files) = recovered.into_parts();
    let file = files.remove(0).into_file();

    let empty = NamespaceRoot::new(1_024, 3, 2, Vec::new(), Vec::new())
        .expect("empty successor Namespace is valid");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("first empty generation commits");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("second empty generation drains the durable DATA predecessor");
    let maintenance = MaintenanceRepository::new(generations, containers.clone(), indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("Online GC requires active Exact coverage");
    let catalog = GcCandidateCatalogRepository::new(metadata);

    let cycle = maintenance
        .run_adaptive_online_gc_cycle(
            &catalog,
            DataPoolUsage::new(50, 100).expect("fixture pool usage is valid"),
            OnlineGcRunMode::Urgent,
        )
        .expect("pinned-reader Online-GC quantum succeeds conservatively");

    if let OnlineGcCycleOutcome::Collected(collected) = cycle.outcome() {
        assert_eq!(collected.replacement_containers(), 1);
        assert_eq!(collected.chunks_relocated(), 1);
    }
    assert_eq!(
        file.read_at(0, 8)
            .expect("pinned DATA reader remains usable"),
        b"maintena"
    );
}

#[test]
fn metadata_gc_waits_for_inflight_manifest_publication_and_retains_its_proof() {
    let metadata = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x91; 32]).expect("policy ID is nonzero");
    let bootstrap = GenerationRepository::new(metadata.clone(), policy);
    let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("initial reservation is valid");
    let predecessor = bootstrap
        .commit_namespace(&reservation)
        .expect("publish initial generation");

    let paused = PausedStorageIo::before(metadata, StorageOperation::SyncRoot);
    let generations = GenerationRepository::new(paused.clone(), policy);
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let indexes = ExactIndexRunRepository::new(paused.clone());
    let profile = ExactIndexProfileId::new([0x92; 32]).expect("profile ID is nonzero");
    let publishing = generations.clone();
    let publisher = std::thread::spawn(move || {
        let manifest = ManifestLeaf::new(
            8_192,
            vec![ManifestExtent::Fill {
                logical_length: 8_192,
                value: 0xa5,
            }],
        )
        .expect("inflight Manifest is valid");
        publishing.publish_manifest_successor(
            fastdup_store::SuccessorPredecessor::from_committed_record(predecessor),
            &manifest,
        )
    });
    assert!(paused.wait_until_reached(Duration::from_secs(2)));

    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);
    let collecting = std::thread::spawn(move || maintenance.garbage_collect_metadata());
    assert!(
        !paused.wait_until_reached_count(2, Duration::from_millis(100)),
        "Metadata GC cannot enter deletion while a Metadata publication is inflight"
    );
    paused.resume();
    let proof = publisher
        .join()
        .expect("publisher thread remains healthy")
        .expect("publication completes");
    let report = collecting
        .join()
        .expect("Metadata GC thread remains healthy")
        .expect("Metadata GC completes after publication");
    assert_eq!(report.objects_removed(), 0);
    generations
        .read_manifest(proof.summary().root())
        .expect("successor proof keeps its complete graph readable");
}

#[test]
#[allow(clippy::too_many_lines)]
fn staged_path_publication_holds_the_gc_barrier_until_its_new_root_is_pinned() {
    #[derive(Clone, Copy, Debug)]
    enum PathEdit {
        Replacement,
        Truncate,
    }

    for edit in [PathEdit::Replacement, PathEdit::Truncate] {
        let metadata = MemoryStorageIo::new();
        let paused =
            PausedStorageIo::disarmed_before_name_prefix(metadata, StorageOperation::WriteAt, ".");
        let policy = PolicySetId::new([0x93; 32]).expect("policy ID is nonzero");
        let profile = ExactIndexProfileId::new([0x94; 32]).expect("profile ID is nonzero");
        let generations = GenerationRepository::new(paused.clone(), policy);
        let containers = ContainerRepository::new(MemoryStorageIo::new());
        let indexes = ExactIndexRunRepository::new(paused.clone());
        let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
            .expect("initial reservation is valid");
        let first = generations
            .commit_namespace(&reservation)
            .expect("publish initial generation");
        let predecessor = fastdup_store::SuccessorPredecessor::from_committed_record(first);
        let manifest = ManifestLeaf::new(
            8_192,
            vec![
                ManifestExtent::Fill {
                    logical_length: 4_096,
                    value: 0x31,
                },
                ManifestExtent::Fill {
                    logical_length: 4_096,
                    value: 0x32,
                },
            ],
        )
        .expect("installed Manifest is valid");
        let installed_proof = generations
            .publish_manifest_successor(predecessor, &manifest)
            .expect("publish installed Manifest");
        let installed_summary = installed_proof.summary();
        let namespace = NamespaceRoot::new(
            1_024,
            3,
            1,
            vec![
                DurableInode::new(
                    2,
                    0o640,
                    1_000,
                    1_000,
                    1,
                    1,
                    8_192,
                    installed_summary.root(),
                )
                .expect("installed inode is valid"),
            ],
            vec![NamespaceEntry::new(1, 2, b"staged.vbk".to_vec()).expect("name is valid")],
        )
        .expect("installed Namespace Root is valid");
        let committed = generations
            .commit_namespace_with_successor_proofs_using(
                &namespace,
                &containers,
                predecessor,
                &[installed_proof],
                &containers,
            )
            .expect("commit installed Manifest");
        let committed_predecessor =
            fastdup_store::SuccessorPredecessor::from_committed_record(committed.record());
        let maintenance =
            MaintenanceRepository::new(generations.clone(), containers, indexes, profile);
        maintenance
            .garbage_collect_metadata()
            .expect("establish clean Metadata mark state");
        generations
            .publish_manifest(
                &ManifestLeaf::new(
                    1,
                    vec![ManifestExtent::Fill {
                        logical_length: 1,
                        value: 0xee,
                    }],
                )
                .expect("orphan Manifest is valid"),
            )
            .expect("publish orphan to require an exact collection");
        let proof = generations.reuse_manifest_successor(committed_predecessor, installed_summary);

        paused.arm();
        let publishing = generations.clone();
        let publisher = std::thread::spawn(move || match edit {
            PathEdit::Replacement => publishing.stage_manifest_replacement_successor(
                proof,
                0..4_096,
                &[ManifestExtent::Fill {
                    logical_length: 4_096,
                    value: 0x41,
                }],
            ),
            PathEdit::Truncate => publishing.stage_manifest_truncate_successor(proof, 4_096),
        });
        assert!(
            paused.wait_until_reached(Duration::from_secs(2)),
            "{edit:?} reaches its deliberately blocked staged Metadata write"
        );

        let collecting = maintenance.clone();
        let collector = std::thread::spawn(move || collecting.garbage_collect_metadata());
        assert!(
            !paused.wait_until_reached_count(2, Duration::from_millis(100)),
            "Metadata GC cannot enter deletion while {edit:?} is staged before root-pin acquisition"
        );

        paused.resume();
        let staged = publisher
            .join()
            .expect("staged publisher thread remains healthy")
            .expect("staged path publication completes");
        collector
            .join()
            .expect("Metadata-GC thread remains healthy")
            .expect("Metadata GC completes after staged root-pin acquisition");
        generations
            .read_manifest(staged.summary().root())
            .expect("staged proof keeps its complete graph readable");
    }
}

#[test]
fn reused_successor_pin_waits_for_an_exclusive_metadata_gc_batch() {
    let metadata = MemoryStorageIo::new();
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        metadata,
        StorageOperation::WriteAt,
        ".metadata-mark-catalog-",
    );
    let policy = PolicySetId::new([0x95; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0x96; 32]).expect("profile ID is nonzero");
    let generations = GenerationRepository::new(paused.clone(), policy);
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let indexes = ExactIndexRunRepository::new(paused.clone());
    let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("initial reservation is valid");
    let predecessor = generations
        .commit_namespace(&reservation)
        .expect("publish initial generation");
    let predecessor = fastdup_store::SuccessorPredecessor::from_committed_record(predecessor);
    let manifest = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x51,
        }],
    )
    .expect("successor Manifest is valid");
    let first_proof = generations
        .publish_manifest_successor(predecessor, &manifest)
        .expect("publish successor root before GC");
    let summary = first_proof.summary();
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        1,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 1, 4_096, summary.root())
                .expect("installed inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"reused.vbk".to_vec()).expect("name is valid")],
    )
    .expect("installed Namespace Root is valid");
    let committed = generations
        .commit_namespace_with_successor_proofs_using(
            &namespace,
            &containers,
            predecessor,
            &[first_proof],
            &containers,
        )
        .expect("commit the root that will be reused");
    let predecessor =
        fastdup_store::SuccessorPredecessor::from_committed_record(committed.record());
    let maintenance = MaintenanceRepository::new(generations.clone(), containers, indexes, profile);

    paused.arm();
    let collecting = maintenance.clone();
    let collector = std::thread::spawn(move || collecting.garbage_collect_metadata());
    assert!(
        paused.wait_until_reached(Duration::from_secs(2)),
        "exact Metadata GC reaches its blocked catalog publication"
    );

    let reusing = generations.clone();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let publisher = std::thread::spawn(move || {
        let proof = reusing.reuse_manifest_successor(predecessor, summary);
        completed_tx
            .send(proof)
            .expect("reused-proof receiver remains available");
    });
    assert!(
        completed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "root-pin acquisition cannot pass an exclusive Metadata-GC batch"
    );

    paused.resume();
    collector
        .join()
        .expect("Metadata-GC thread remains healthy")
        .expect("Metadata GC completes after its catalog publication resumes");
    let proof = completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("root-pin acquisition proceeds after Metadata GC releases its barrier");
    publisher.join().expect("reuse thread remains healthy");
    generations
        .read_manifest(proof.summary().root())
        .expect("reused proof names a readable Manifest root");
}

#[test]
fn every_metadata_gc_delete_fault_recovers_a_complete_live_graph() {
    fn fixture(
        metadata: MemoryStorageIo,
    ) -> MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo> {
        let (generations, containers, indexes, profile) =
            seeded_repositories_using(metadata, MemoryStorageIo::new());
        let orphan = ManifestLeaf::new(
            16_384,
            vec![ManifestExtent::Fill {
                logical_length: 16_384,
                value: 0x33,
            }],
        )
        .expect("fault-matrix orphan is valid");
        generations
            .publish_manifest(&orphan)
            .expect("publish fault-matrix orphan");
        MaintenanceRepository::new(generations, containers, indexes, profile)
    }

    let probe_metadata = MemoryStorageIo::new();
    let probe = fixture(probe_metadata.clone());
    let baseline = probe_metadata.operation_count();
    probe
        .garbage_collect_metadata()
        .expect("probe Metadata GC succeeds");
    let destructive = probe_metadata.operations()[baseline..]
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, operation)| {
            matches!(
                operation,
                StorageOperation::RemoveFile | StorageOperation::SyncRoot
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        destructive.len(),
        2,
        "one orphan requires one unlink and one directory durability point"
    );

    for fail_after_effect in [false, true] {
        for (relative, _) in &destructive {
            let absolute = baseline + relative;
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let maintenance = fixture(metadata.clone());
            assert!(maintenance.garbage_collect_metadata().is_err());
            metadata.crash();
            maintenance
                .scrub()
                .expect("post-crash scrub accepts retained or durably deleted Metadata garbage");
        }
    }
}

fn recoverable_retiring_fixture(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> (
    MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
) {
    let policy = PolicySetId::new([0xC1; 32]).expect("fixture policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0xC2; 32]).expect("fixture profile ID is nonzero");
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(metadata);
    for (id, generation, chunks) in [
        (
            [0xC3; 16],
            1_u64,
            [
                b"recovery-victim-one-a".as_slice(),
                b"recovery-victim-one-b".as_slice(),
            ],
        ),
        (
            [0xC4; 16],
            2_u64,
            [
                b"recovery-victim-two-a".as_slice(),
                b"recovery-victim-two-b".as_slice(),
            ],
        ),
    ] {
        containers
            .publish_raw(
                ContainerId::new(id).expect("fixture Container ID is nonzero"),
                generation,
                &chunks,
            )
            .expect("publish recovery victim");
    }
    let maintenance =
        MaintenanceRepository::new(generations, containers.clone(), indexes.clone(), profile);
    maintenance
        .rebuild_exact_index()
        .expect("publish initial ACTIVE Exact generation");
    let mut retiring = Vec::new();
    for id in [[0xC3; 16], [0xC4; 16]] {
        let container = containers
            .read(ContainerId::new(id).expect("fixture Container ID is nonzero"))
            .expect("reread recovery victim");
        for location in container.locations().iter().copied() {
            let active = ExactIndexEntry::from_verified(location)
                .expect("verified Container Location is an Exact Location");
            retiring.push(
                ExactIndexEntry::retiring(active).expect("ACTIVE fixture Location may retire"),
            );
        }
    }
    indexes
        .append_level_zero(profile, retiring)
        .expect("activate durable RETIRING fixture");
    (maintenance, containers, indexes)
}

fn recoverable_repositories_for_existing_retirement(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> (
    MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
) {
    let policy = PolicySetId::new([0xC1; 32]).expect("fixture policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0xC2; 32]).expect("fixture profile ID is nonzero");
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    let maintenance = MaintenanceRepository::new(
        GenerationRepository::new(metadata, policy),
        containers.clone(),
        indexes.clone(),
        profile,
    );
    (maintenance, containers, indexes)
}

fn seeded_mixed_repositories_using(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> (
    GenerationRepository<MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
    ExactIndexProfileId,
) {
    let (generations, containers, indexes, profile) = seeded_repositories_using(metadata, data);
    let first = b"maintenance-first-chunk";
    let third = b"maintenance-third-live-chunk";
    containers
        .publish_raw(
            ContainerId::new([0x88; 16]).expect("fixture ID is nonzero"),
            2,
            &[third, b"maintenance-fourth-dead-chunk"],
        )
        .expect("publish second partially live Container");
    let logical_length =
        u64::try_from(first.len() + third.len()).expect("fixture logical length fits u64");
    let manifest = ManifestLeaf::new(
        logical_length,
        vec![
            ManifestExtent::Data {
                logical_length: u64::try_from(first.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(first),
            },
            ManifestExtent::Data {
                logical_length: u64::try_from(third.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(third),
            },
        ],
    )
    .expect("successor Manifest is valid");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish successor Manifest");
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        2,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 2, logical_length, manifest_root)
                .expect("successor inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("fixture name is valid")],
    )
    .expect("successor Namespace Root is valid");
    generations
        .commit_namespace_with_data(&namespace, &containers)
        .expect("commit both live Chunks without changing their logical identities");
    (generations, containers, indexes, profile)
}

fn seeded_replaced_generation_repositories(
    generations_count: u8,
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> (
    GenerationRepository<MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    ExactIndexRunRepository<MemoryStorageIo>,
    ExactIndexProfileId,
) {
    assert!(
        generations_count >= 2,
        "ASSERT: the performance fixture needs current and previous DATA generations"
    );
    let policy = PolicySetId::new([0xB1; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0xB2; 32]).expect("profile ID is nonzero");
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(metadata);

    let reservation = NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("initial reservation root is valid");
    generations
        .commit_namespace(&reservation)
        .expect("reserve inode range before visibility");

    for ordinal in 1..=generations_count {
        let mut payload = vec![ordinal; 16 * 1_024];
        payload[..8].copy_from_slice(&u64::from(ordinal).to_le_bytes());
        containers
            .publish_raw(
                ContainerId::new([ordinal; 16]).expect("fixture Container ID is nonzero"),
                u64::from(ordinal),
                &[&payload],
            )
            .expect("publish replacement-generation Container");
        let logical_length = u64::try_from(payload.len()).expect("fixture payload length fits u64");
        let manifest = ManifestLeaf::new(
            logical_length,
            vec![ManifestExtent::Data {
                logical_length,
                chunk_id: ChunkId::of(&payload),
            }],
        )
        .expect("replacement-generation Manifest is valid");
        let manifest_root = generations
            .publish_manifest(&manifest)
            .expect("publish replacement-generation Manifest");
        let namespace = NamespaceRoot::new(
            1_024,
            3,
            u64::from(ordinal),
            vec![
                DurableInode::new(
                    2,
                    0o640,
                    1_000,
                    1_000,
                    1,
                    u64::from(ordinal),
                    logical_length,
                    manifest_root,
                )
                .expect("replacement-generation inode is valid"),
            ],
            vec![
                NamespaceEntry::new(1, 2, b"rolling.vbk".to_vec()).expect("fixture name is valid"),
            ],
        )
        .expect("replacement-generation Namespace Root is valid");
        generations
            .commit_namespace_with_data(&namespace, &containers)
            .expect("commit replacement DATA generation");
    }

    (generations, containers, indexes, profile)
}

#[test]
fn scrub_verifies_the_live_generation_every_container_and_optional_exact_index() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let report = maintenance.scrub().expect("end-to-end scrub succeeds");

    assert_eq!(report.commit_generations_verified(), 2);
    assert_eq!(report.commit_generation(), Some(2));
    assert_eq!(report.namespace_inodes(), 1);
    assert_eq!(report.manifest_files(), 1);
    assert_eq!(report.containers(), 1);
    assert_eq!(report.container_chunks(), 2);
    assert_eq!(report.container_generation_high_water(), Some(1));
    assert_eq!(report.exact_activation_generation(), None);
    assert_eq!(report.exact_active_locations_verified(), 0);
}

#[test]
fn gc_scrub_data_io_scales_with_container_count_not_commit_history() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let container_count = 24_usize;
    let (generations, containers, indexes, profile) = seeded_replaced_generation_repositories(
        u8::try_from(container_count).expect("fixture count fits u8"),
        metadata,
        data.clone(),
    );
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let baseline = data.operation_count();

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("generation-bound scrub succeeds");

    assert_eq!(plan.reclaimable_containers(), container_count - 2);
    let operations = &data.operations()[baseline..];
    let whole_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    let directory_scans = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ListNames)
        .count();
    assert!(
        whole_reads <= container_count * 2,
        "Scrub may verify online DATA once and classify the Container inventory once; observed {whole_reads} whole reads for {container_count} Containers"
    );
    assert!(
        directory_scans <= 2,
        "Scrub must not rescan the DATA directory once per historical Commit; observed {directory_scans} scans"
    );

    let collected = maintenance
        .garbage_collect(plan)
        .expect("only the current and previous DATA generations remain pinned");
    assert_eq!(collected.containers_removed(), 22);
    maintenance
        .scrub()
        .expect("obsolete WAL history does not become an implicit DATA snapshot");
}

#[test]
fn mixed_gc_planning_does_not_rescan_the_complete_container_store() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(metadata, data.clone());
    for ordinal in 1_u8..=20 {
        let payload = vec![ordinal; 4 * 1_024];
        containers
            .publish_raw(
                ContainerId::new([ordinal; 16]).expect("dead fixture ID is nonzero"),
                u64::from(ordinal) + 10,
                &[&payload],
            )
            .expect("publish fully unreachable planning fixture");
    }
    let container_count = 22_usize;
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let baseline = data.operation_count();

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("mixed generation-bound scrub succeeds");

    assert_eq!(plan.compaction_victim_containers(), 2);
    assert_eq!(plan.reclaimable_containers(), 20);
    let operations = &data.operations()[baseline..];
    let whole_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    let directory_scans = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ListNames)
        .count();
    assert!(
        whole_reads <= container_count * 2,
        "online DATA proof plus one GC inventory may read each Container twice; observed {whole_reads} whole reads for {container_count} Containers"
    );
    assert!(
        directory_scans <= 2,
        "mixed-victim selection must reuse first-pass Chunk identities; observed {directory_scans} directory scans"
    );
}

#[test]
fn scrub_plan_collects_only_fully_unreachable_containers_and_rebuilds_the_index_first() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let retained_container = ContainerId::new([0x83; 16]).expect("fixture ID is nonzero");
    let unreachable_container = ContainerId::new([0x84; 16]).expect("fixture ID is nonzero");
    let unreachable = b"fully-unreachable-maintenance-chunk";
    containers
        .publish_raw(unreachable_container, 2, &[unreachable])
        .expect("publish fully unreachable Container");
    let inspect_containers = containers.clone();
    let inspect_indexes = indexes.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("successful scrub produces a GC capability");

    assert_eq!(plan.scrub_priority(), MaintenancePriority::Background);
    assert_eq!(plan.gc_priority(), MaintenancePriority::Normal);
    assert_eq!(plan.reclaimable_containers(), 1);
    assert_eq!(plan.partially_live_containers(), 1);
    assert!(plan.reclaimable_bytes() * 5 >= plan.container_bytes());

    let collected = maintenance
        .garbage_collect(plan)
        .expect("generation-bound plan safely collects unreachable DATA");
    assert_eq!(collected.containers_removed(), 1);
    assert!(collected.bytes_removed() != 0);
    assert!(matches!(
        inspect_containers.read(unreachable_container),
        Err(StoreError::Io(ref error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    inspect_containers
        .read(retained_container)
        .expect("partially live Container remains complete");

    let active = inspect_indexes
        .recover_active()
        .expect("replacement Exact Index is structurally valid")
        .expect("GC activates a replacement Exact Index");
    assert!(
        inspect_containers
            .find_verified_chunk_with_index(
                &active,
                ChunkId::of(unreachable),
                u64::try_from(unreachable.len()).expect("fixture length fits u64"),
            )
            .expect("removed garbage degrades to an ordinary Exact miss")
            .is_none()
    );
    maintenance
        .scrub()
        .expect("post-GC writer, recovery, and scrub invariants remain paired");
}

#[test]
fn scrub_gc_merges_two_partially_live_containers_without_rewriting_manifests() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let first_container = ContainerId::new([0x83; 16]).expect("fixture ID is nonzero");
    let second_container = ContainerId::new([0x88; 16]).expect("fixture ID is nonzero");
    let first = b"maintenance-first-chunk";
    let third = b"maintenance-third-live-chunk";
    containers
        .publish_raw(
            second_container,
            2,
            &[third, b"maintenance-fourth-dead-chunk"],
        )
        .expect("publish second partially live Container");
    let logical_length =
        u64::try_from(first.len() + third.len()).expect("fixture logical length fits u64");
    let manifest = ManifestLeaf::new(
        logical_length,
        vec![
            ManifestExtent::Data {
                logical_length: u64::try_from(first.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(first),
            },
            ManifestExtent::Data {
                logical_length: u64::try_from(third.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(third),
            },
        ],
    )
    .expect("successor Manifest is valid");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish successor Manifest");
    let namespace = NamespaceRoot::new(
        1_024,
        3,
        2,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_000, 1, 2, logical_length, manifest_root)
                .expect("successor inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("fixture name is valid")],
    )
    .expect("successor Namespace Root is valid");
    generations
        .commit_namespace_with_data(&namespace, &containers)
        .expect("commit both live Chunks without changing their logical identities");
    let inspect = containers.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("successful scrub proves both mixed Containers");

    assert_eq!(plan.partially_live_containers(), 2);
    assert_eq!(plan.compaction_victim_containers(), 2);
    assert_eq!(plan.replacement_chunks(), 2);
    assert_eq!(plan.gc_priority(), MaintenancePriority::Normal);
    assert!(plan.estimated_reclaimable_bytes() * 5 > plan.container_bytes());
    let report = maintenance
        .garbage_collect(plan)
        .expect("GC rewrites live Chunks before collecting both victims");
    assert_eq!(report.containers_removed(), 2);
    assert_eq!(report.replacement_containers(), 1);
    assert_eq!(report.chunks_relocated(), 2);
    assert!(report.bytes_reclaimed() > 0);
    for container_id in [first_container, second_container] {
        assert!(matches!(
            inspect.read(container_id),
            Err(StoreError::Io(ref error)) if error.kind() == std::io::ErrorKind::NotFound
        ));
    }
    assert_eq!(
        inspect
            .read_verified_chunk(
                ChunkId::of(first),
                u64::try_from(first.len()).expect("fixture length fits u64"),
            )
            .expect("first relocated Chunk remains readable"),
        first
    );
    assert_eq!(
        inspect
            .read_verified_chunk(
                ChunkId::of(third),
                u64::try_from(third.len()).expect("fixture length fits u64"),
            )
            .expect("second relocated Chunk remains readable"),
        third
    );
    assert_eq!(
        inspect
            .audit_published()
            .expect("replacement Container inventory is valid")
            .containers(),
        1
    );
    maintenance
        .scrub()
        .expect("post-compaction writer, recovery, and scrub invariants remain paired");
}

#[test]
fn every_mixed_container_gc_data_fault_preserves_complete_live_coverage() {
    let probe_data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(MemoryStorageIo::new(), probe_data.clone());
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let plan = probe
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("probe scrub succeeds");
    let baseline = probe_data.operation_count();
    probe
        .garbage_collect(plan)
        .expect("probe compaction succeeds");
    let operation_count = probe_data.operation_count() - baseline;
    assert!(
        operation_count > 10,
        "probe includes rewrite and deletion I/O"
    );

    for relative in 0..operation_count {
        for fail_after in [false, true] {
            let absolute = baseline + relative;
            let data = if fail_after {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let (generations, containers, indexes, profile) =
                seeded_mixed_repositories_using(MemoryStorageIo::new(), data.clone());
            let inspect = containers.clone();
            let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
            let plan = maintenance
                .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
                .expect("fault case scrub succeeds before compaction");
            assert!(
                maintenance.garbage_collect(plan).is_err(),
                "configured data fault at relative position {relative}, fail_after={fail_after} must be observed"
            );
            data.crash();
            for payload in [
                b"maintenance-first-chunk".as_slice(),
                b"maintenance-third-live-chunk".as_slice(),
            ] {
                assert_eq!(
                    inspect
                        .read_verified_chunk(
                            ChunkId::of(payload),
                            u64::try_from(payload.len()).expect("fixture length fits u64"),
                        )
                        .expect("old or replacement coverage remains readable after crash"),
                    payload,
                );
            }
            maintenance
                .scrub()
                .expect("post-crash complete scrub accepts old, duplicate, or compacted coverage");
        }
    }
}

#[test]
fn mixed_container_gc_retry_resumes_a_durable_replacement_temporary() {
    let probe_data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(MemoryStorageIo::new(), probe_data.clone());
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let plan = probe
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("probe scrub succeeds");
    let baseline = probe_data.operation_count();
    probe
        .garbage_collect(plan)
        .expect("probe compaction succeeds");
    let operations = probe_data.operations();
    let replacement_sync = operations[baseline..]
        .iter()
        .position(|operation| *operation == StorageOperation::SyncFile)
        .expect("replacement publication synchronizes its file");

    let data = MemoryStorageIo::with_fail_after(baseline + replacement_sync);
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(MemoryStorageIo::new(), data);
    let inspect = containers.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let first = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("first scrub succeeds");
    assert!(maintenance.garbage_collect(first).is_err());

    let retry = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("worked pool usage is valid"))
        .expect("retry scrub ignores the non-authoritative temporary");
    let report = maintenance
        .garbage_collect(retry)
        .expect("retry resumes, verifies, and publishes the replacement");
    assert_eq!(report.containers_removed(), 2);
    assert_eq!(report.replacement_containers(), 1);
    assert_eq!(
        inspect
            .audit_published()
            .expect("retry leaves one valid replacement")
            .containers(),
        1
    );
    maintenance
        .scrub()
        .expect("resumed writer and scrub invariants remain paired");
}

#[test]
fn maintenance_priority_thresholds_are_inclusive() {
    let below = DataPoolUsage::new(899, 1_000).expect("worked pool usage is valid");
    let at = DataPoolUsage::new(900, 1_000).expect("worked pool usage is valid");

    assert_eq!(below.scrub_priority(), MaintenancePriority::Background);
    assert_eq!(at.scrub_priority(), MaintenancePriority::Normal);
    assert_eq!(
        below.gc_priority(199, 1_000),
        MaintenancePriority::Background
    );
    assert_eq!(
        below.gc_priority(200, 1_000),
        MaintenancePriority::Background
    );
    assert_eq!(below.gc_priority(201, 1_000), MaintenancePriority::Normal);
}

#[test]
#[allow(clippy::too_many_lines)]
fn metadata_liveness_delta_updates_catalog_and_local_proof_compacts_without_scrub() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let restart_metadata = metadata.clone();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(metadata.clone(), data.clone());

    let initial = generations
        .liveness_delta_since(None)
        .expect("initial delta scans protected Metadata only");
    assert_eq!(initial.base_generation(), None);
    assert_eq!(initial.latest_generation(), Some(3));
    assert_eq!(initial.protected_chunk_count(), 2);
    assert_eq!(initial.added().len(), 2);
    assert!(initial.removed().is_empty());
    let from_first_data = generations
        .liveness_delta_since(Some(2))
        .expect("retained WAL generation supplies an incremental base");
    assert_eq!(
        from_first_data.added(),
        &BTreeMap::from([(
            ChunkId::of(b"maintenance-third-live-chunk"),
            u64::try_from(b"maintenance-third-live-chunk".len()).expect("fixture length fits u64"),
        )])
    );
    assert!(from_first_data.removed().is_empty());

    let maintenance_containers = containers.with_maintenance_storage(data.clone());
    let maintenance = MaintenanceRepository::new(
        generations.clone(),
        maintenance_containers,
        indexes.clone(),
        profile,
    );
    maintenance
        .rebuild_exact_index()
        .expect("activate complete Exact generation for delta attribution");
    let catalog = GcCandidateCatalogRepository::new(metadata);
    let rows = [
        candidate_row(
            [0x83; 16],
            1,
            &[
                b"maintenance-first-chunk".as_slice(),
                b"maintenance-second-chunk".as_slice(),
            ],
        ),
        candidate_row(
            [0x88; 16],
            2,
            &[
                b"maintenance-third-live-chunk".as_slice(),
                b"maintenance-fourth-dead-chunk".as_slice(),
            ],
        ),
    ];
    catalog
        .publish_rows(1, 0, 0, 2, rows)
        .expect("publish publication-only catalog");
    maintenance
        .refresh_gc_candidate_catalog(&catalog, 2)
        .expect("derive and publish Metadata delta successor");
    let current = catalog
        .recover_latest()
        .expect("recover liveness successor")
        .expect("liveness successor exists");
    for row in &rows {
        let observed = current
            .find_row(row.container_id())
            .expect("binary catalog lookup succeeds")
            .expect("fixture row remains present");
        assert!(observed.estimate_known());
        assert_eq!(observed.reachable_target_count(), 1);
    }
    let shortlist = current
        .shortlist(GcCandidateSelectionMode::Background, 2, 3)
        .expect("rank bounded victims");

    let baseline = data.operation_count();
    let proof = maintenance
        .prove_gc_candidates(
            &shortlist,
            DataPoolUsage::new(50, 100).expect("worked pool usage is valid"),
        )
        .expect("candidate-local proof preserves only live generation requirements");
    assert_eq!(proof.victim_containers(), 2);
    assert_eq!(proof.replacement_chunks(), 2);
    assert_eq!(proof.reachable_victim_chunks(), 2);
    assert!(proof.replacement_upper_bound() < proof.victim_bytes());
    let proof_operations = &data.operations()[baseline..];
    assert_eq!(
        proof_operations
            .iter()
            .filter(|operation| **operation == StorageOperation::Read)
            .count(),
        2,
        "proof reads only the two selected victim Containers"
    );
    assert_eq!(
        proof_operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ListNames)
            .count(),
        0,
        "candidate-local proof never scans the DATA pool directory"
    );

    let current_generation = generations
        .recover_latest_with_data(&containers)
        .expect("recover current generation")
        .expect("fixture generation exists");
    generations
        .commit_namespace_with_data(current_generation.namespace_root(), &containers)
        .expect("advance protected Commit pair after proof construction");
    assert!(matches!(
        maintenance.garbage_collect_proved_candidates(proof),
        Err(MaintenanceError::StaleGcPlan)
    ));
    let current_shortlist = current
        .shortlist(GcCandidateSelectionMode::Background, 2, 4)
        .expect("stale hints remain safe proof inputs");
    let exact_stale_proof = maintenance
        .prove_gc_candidates(
            &current_shortlist,
            DataPoolUsage::new(50, 100).expect("worked pool usage is valid"),
        )
        .expect("fresh proof binds the advanced Commit pair");
    maintenance
        .rebuild_exact_index()
        .expect("advance the active Exact generation after proof construction");
    assert!(matches!(
        maintenance.garbage_collect_proved_candidates(exact_stale_proof),
        Err(MaintenanceError::StaleGcPlan)
    ));
    let current_proof = maintenance
        .prove_gc_candidates(
            &current_shortlist,
            DataPoolUsage::new(50, 100).expect("worked pool usage is valid"),
        )
        .expect("final proof binds Commit pair and new Exact generation");
    let held_generation = indexes
        .recover_active_generation()
        .expect("install the proof's Exact generation")
        .expect("the proof has an active Exact generation");
    let retirement = maintenance
        .begin_online_gc_retirement(current_proof)
        .expect("replacement and RETIRING transition activate atomically");
    assert_eq!(retirement.victim_containers(), 2);
    assert!(!retirement.pins_drained());
    let restarted_indexes = ExactIndexRunRepository::new(restart_metadata);
    let restarted_generation = restarted_indexes
        .recover_active_generation()
        .expect("restart recovers the complete RETIRING activation")
        .expect("RETIRING activation exists after restart");
    let restarted_retiring = restarted_indexes
        .retiring_containers(&restarted_generation)
        .expect("restart derives the effective retiring selection set");
    assert_eq!(restarted_retiring.len(), 2);
    let restarted_containers = ContainerRepository::new(data.clone());
    restarted_containers.install_retiring_selection_barrier(&restarted_retiring);
    let replacement = restarted_containers
        .find_verified_location_with_index(
            &restarted_generation,
            ChunkId::of(b"maintenance-first-chunk"),
            u64::try_from(b"maintenance-first-chunk".len()).expect("fixture length fits u64"),
        )
        .expect("restart Exact lookup remains available")
        .expect("replacement ACTIVE Location is selected");
    assert!(
        !restarted_retiring.contains_key(&replacement.location().container_id().bytes()),
        "restart never selects a shadowed RETIRING victim"
    );
    drop(restarted_generation);
    drop(held_generation);
    let report = maintenance
        .finish_online_gc_retirement(retirement)
        .expect("pin drain precedes victim deletion and REMOVED transition");
    assert_eq!(report.containers_removed(), 2);
    assert_eq!(report.replacement_containers(), 1);
    assert_eq!(report.chunks_relocated(), 2);
    assert_eq!(
        containers
            .audit_published()
            .expect("one complete replacement remains")
            .containers(),
        1
    );
    maintenance
        .scrub()
        .expect("post-collection full audit remains clean");
}

#[test]
fn recovered_online_gc_finalizer_is_idempotent_after_durable_partial_unlink() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let restart_metadata = metadata.clone();
    let restart_data = data.clone();
    let (_maintenance, containers, _indexes) = recoverable_retiring_fixture(metadata, data.clone());

    let mut published = data.list_names().expect("list recovery victim names");
    published.sort_unstable();
    assert_eq!(published.len(), 2);
    data.remove_file(&published[0])
        .expect("model an earlier victim unlink");
    data.sync_root()
        .expect("model its DATA directory sync before process loss");
    drop(containers);

    let (restarted, restarted_containers, restarted_indexes) =
        recoverable_repositories_for_existing_retirement(restart_metadata, restart_data);
    let report = restarted
        .finalize_recovered_online_gc()
        .expect("restart finalizes the durable RETIRING generation");
    assert_eq!(report.retiring_containers(), 2);
    assert_eq!(report.containers_removed(), 1);
    assert_eq!(report.containers_already_absent(), 1);
    assert_eq!(report.retiring_locations_finalized(), 4);
    assert!(report.bytes_removed() != 0);
    assert!(report.activation_generation().is_some());
    assert_eq!(
        restarted_containers
            .audit_published()
            .expect("all victims are gone")
            .containers(),
        0
    );
    let active = restarted_indexes
        .recover_active_generation()
        .expect("recover finalized Exact generation")
        .expect("finalized Exact generation exists");
    assert!(
        restarted_indexes
            .retiring_entries(&active)
            .expect("read effective transitions")
            .is_empty()
    );
    assert_eq!(
        restarted
            .finalize_recovered_online_gc()
            .expect("a repeated finalizer is a no-op"),
        fastdup_store::OnlineGcRecoveryReport::default()
    );
}

#[test]
fn recovered_online_gc_retries_after_crash_during_unlink_batch() {
    let control_data = MemoryStorageIo::new();
    let (control, _containers, _indexes) =
        recoverable_retiring_fixture(MemoryStorageIo::new(), control_data.clone());
    let control_baseline = control_data.operation_count();
    control
        .finalize_recovered_online_gc()
        .expect("control finalizer succeeds");
    let first_remove = control_data.operations()[control_baseline..]
        .iter()
        .position(|operation| *operation == StorageOperation::RemoveFile)
        .and_then(|relative| control_baseline.checked_add(relative))
        .expect("control finalizer unlinks at least one victim");

    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::with_fail_after(first_remove);
    let restart_metadata = metadata.clone();
    let restart_data = data.clone();
    let (maintenance, _containers, _indexes) = recoverable_retiring_fixture(metadata, data.clone());
    assert!(matches!(
        maintenance.finalize_recovered_online_gc(),
        Err(MaintenanceError::Store(StoreError::Io(_)))
    ));
    data.crash();

    let (restarted, restarted_containers, restarted_indexes) =
        recoverable_repositories_for_existing_retirement(restart_metadata, restart_data);
    let report = restarted
        .finalize_recovered_online_gc()
        .expect("restart safely retries the interrupted unlink batch");
    assert_eq!(report.containers_removed(), 2);
    assert_eq!(report.containers_already_absent(), 0);
    assert_eq!(
        restarted_containers
            .audit_published()
            .expect("retry removes both victims")
            .containers(),
        0
    );
    let active = restarted_indexes
        .recover_active_generation()
        .expect("recover retry result")
        .expect("retry publishes an Exact generation");
    assert!(
        restarted_indexes
            .retiring_entries(&active)
            .expect("read retry transitions")
            .is_empty()
    );
}

#[test]
fn adaptive_online_gc_cycle_bootstraps_hints_and_collects_one_bounded_quantum() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(metadata.clone(), data.clone());
    let maintenance = MaintenanceRepository::new(generations, containers.clone(), indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("online scheduler requires one active Exact generation");
    let catalog = GcCandidateCatalogRepository::new(metadata);
    let baseline = data.operation_count();

    let cycle = maintenance
        .run_adaptive_online_gc_cycle_with_workers(
            &catalog,
            DataPoolUsage::new(50, 100).expect("fixture pool usage is valid"),
            OnlineGcRunMode::Idle,
            std::num::NonZeroUsize::new(2).expect("two relocation workers are nonzero"),
        )
        .expect("idle scheduler quantum succeeds");
    let OnlineGcCycleOutcome::Collected(collected) = cycle.outcome() else {
        panic!("ASSERT: fixture must produce one profitable Online-GC cycle");
    };
    assert_eq!(collected.containers_removed(), 2);
    assert_eq!(collected.replacement_containers(), 1);
    assert_eq!(
        containers
            .audit_published()
            .expect("replacement is the only remaining Container")
            .containers(),
        1
    );
    assert_eq!(cycle.catalog().row_count(), 1);
    let metrics = cycle.metrics();
    assert_eq!(metrics.shortlisted_candidates(), 2);
    assert_eq!(metrics.proved_victims(), 2);
    assert_eq!(metrics.relocation_workers(), 2);
    assert!(metrics.candidate_proof_read_bytes() > 0);
    assert!(metrics.relocation_read_bytes() > 0);
    assert_eq!(
        metrics.relocation_write_bytes(),
        collected.replacement_bytes()
    );
    assert_eq!(metrics.unlinked_bytes(), collected.bytes_removed());
    assert!(metrics.total_wall() >= metrics.candidate_proof_wall());
    assert!(metrics.relocation_wall() >= metrics.retiring_activation_wall());
    assert!(metrics.relocation_wall() >= metrics.pin_drain_wall());
    assert!(metrics.relocation_wall() >= metrics.victim_verify_wall());
    assert!(metrics.relocation_wall() >= metrics.unlink_wall());
    assert!(metrics.relocation_wall() >= metrics.data_sync_wall());
    assert!(metrics.relocation_wall() >= metrics.removed_activation_wall());
    assert!(
        data.operations()[baseline..].contains(&StorageOperation::ReadExactAt),
        "catalog bootstrap reads only bounded Header/Footer ranges"
    );
}

#[test]
fn adaptive_online_gc_reclaims_victims_without_relocation_when_protected_data_is_empty() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_mixed_repositories_using(metadata.clone(), data);
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("online scheduler requires one active Exact generation");
    let empty = NamespaceRoot::new(1_024, 3, 3, Vec::new(), Vec::new())
        .expect("empty successor Namespace is valid");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("first empty generation commits");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("second empty generation drains the last DATA predecessor");
    let catalog = GcCandidateCatalogRepository::new(metadata);

    let cycle = maintenance
        .run_adaptive_online_gc_cycle(
            &catalog,
            DataPoolUsage::new(50, 100).expect("fixture pool usage is valid"),
            OnlineGcRunMode::Urgent,
        )
        .expect("zero-live Online-GC quantum succeeds");
    let OnlineGcCycleOutcome::Collected(collected) = cycle.outcome() else {
        panic!("ASSERT: an empty protected DATA set makes every shortlisted victim reclaimable");
    };

    assert_eq!(collected.containers_removed(), 2);
    assert_eq!(collected.replacement_containers(), 0);
    assert_eq!(collected.chunks_relocated(), 0);
    assert_eq!(cycle.catalog().row_count(), 0);
    assert_eq!(
        containers
            .audit_published()
            .expect("Online GC leaves an auditable empty DATA pool")
            .containers(),
        0
    );
    let scrub = maintenance
        .scrub()
        .expect("empty DATA and zero-active Exact state remain scrub-clean");
    assert_eq!(scrub.containers(), 0);
    assert_eq!(scrub.exact_active_locations_verified(), 0);
}

#[test]
fn online_gc_holds_commit_binding_through_retiring_activation() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (_bootstrap_generations, _bootstrap_containers, _bootstrap_indexes, profile) =
        seeded_mixed_repositories_using(metadata.clone(), data.clone());
    let exact_prefix = format!(".{}", "82".repeat(32));
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        metadata,
        StorageOperation::WriteAt,
        exact_prefix,
    );
    let policy = PolicySetId::new([0x81; 32]).expect("policy ID is nonzero");
    let generations = GenerationRepository::new(paused.clone(), policy);
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(paused.clone());
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("Online GC requires active Exact coverage");
    let empty = NamespaceRoot::new(1_024, 3, 3, Vec::new(), Vec::new())
        .expect("empty successor Namespace is valid");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("first empty generation commits");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("second empty generation drains the DATA predecessor");
    let catalog = GcCandidateCatalogRepository::new(paused.clone());

    paused.arm();
    let collecting = std::thread::spawn(move || {
        maintenance.run_adaptive_online_gc_cycle(
            &catalog,
            DataPoolUsage::new(50, 100).expect("fixture pool usage is valid"),
            OnlineGcRunMode::Urgent,
        )
    });
    assert!(
        paused.wait_until_reached(Duration::from_secs(2)),
        "Online GC reaches Exact RETIRING publication"
    );

    let committing_generations = generations.clone();
    let committing_containers = containers.clone();
    let committing = std::thread::spawn(move || {
        committing_generations.commit_namespace_with_data(&empty, &committing_containers)
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !committing.is_finished(),
        "a Namespace commit cannot pass the proof binding before RETIRING activates"
    );

    paused.resume();
    let cycle = collecting
        .join()
        .expect("Online-GC worker remains healthy")
        .expect("Online-GC retirement succeeds");
    assert!(matches!(
        cycle.outcome(),
        OnlineGcCycleOutcome::Collected(_)
    ));
    committing
        .join()
        .expect("frontend commit worker remains healthy")
        .expect("frontend commit proceeds after RETIRING activation");
}

#[test]
fn urgent_zero_live_gc_drains_more_than_one_candidate_quantum_and_becomes_idempotent() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_replaced_generation_repositories(70, metadata.clone(), MemoryStorageIo::new());
    let maintenance =
        MaintenanceRepository::new(generations.clone(), containers.clone(), indexes, profile);
    maintenance
        .rebuild_exact_index()
        .expect("build Exact coverage for all seventy Containers");
    let empty = NamespaceRoot::new(1_024, 3, 71, Vec::new(), Vec::new())
        .expect("empty successor Namespace is valid");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("first empty generation commits");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("second empty generation drains the last DATA predecessor");
    let catalog = GcCandidateCatalogRepository::new(metadata);
    let usage = DataPoolUsage::new(50, 100).expect("fixture pool usage is valid");

    let first = maintenance
        .run_adaptive_online_gc_cycle(&catalog, usage, OnlineGcRunMode::Urgent)
        .expect("first bounded zero-live quantum succeeds");
    let OnlineGcCycleOutcome::Collected(first) = first.outcome() else {
        panic!("ASSERT: first urgent quantum must collect its bounded shortlist");
    };
    assert_eq!(first.containers_removed(), 64);
    assert_eq!(first.replacement_containers(), 0);

    let second = maintenance
        .run_adaptive_online_gc_cycle(&catalog, usage, OnlineGcRunMode::Urgent)
        .expect("second bounded zero-live quantum succeeds");
    let OnlineGcCycleOutcome::Collected(second) = second.outcome() else {
        panic!("ASSERT: second urgent quantum must collect the remaining shortlist");
    };
    assert_eq!(second.containers_removed(), 6);
    assert_eq!(second.replacement_containers(), 0);

    let stable = maintenance
        .run_adaptive_online_gc_cycle(&catalog, usage, OnlineGcRunMode::Urgent)
        .expect("empty follow-up quantum is a successful no-op");
    assert!(matches!(
        stable.outcome(),
        OnlineGcCycleOutcome::NoCandidates
    ));
    assert_eq!(
        containers
            .audit_published()
            .expect("audit empty DATA")
            .containers(),
        0
    );
    maintenance
        .scrub()
        .expect("multi-quantum empty pool remains scrub-clean");
}

#[test]
fn liveness_delta_waits_for_previous_generation_drain_before_removal() {
    let (generations, containers, _indexes, _profile) = seeded_repositories();
    let empty = NamespaceRoot::new(1_024, 3, 2, Vec::new(), Vec::new())
        .expect("empty successor Namespace is valid");
    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("first empty generation commits");
    let still_pinned = generations
        .liveness_delta_since(Some(2))
        .expect("base generation remains retained");
    assert!(still_pinned.added().is_empty());
    assert!(
        still_pinned.removed().is_empty(),
        "generation two remains protected as the previous online generation"
    );

    generations
        .commit_namespace_with_data(&empty, &containers)
        .expect("second empty generation drains the old predecessor");
    let drained = generations
        .liveness_delta_since(Some(2))
        .expect("retained base compares against the new protected pair");
    assert_eq!(
        drained.removed(),
        &BTreeMap::from([(
            ChunkId::of(b"maintenance-first-chunk"),
            u64::try_from(b"maintenance-first-chunk".len()).expect("fixture length fits u64"),
        )])
    );
}

fn candidate_row(id: [u8; 16], generation: u64, chunks: &[&[u8]]) -> GcCandidateCatalogRow {
    let id = ContainerId::new(id).expect("fixture identity is nonzero");
    let (image, publication) = SealedContainer::encode_with_writer_evidence(id, generation, chunks)
        .expect("fixture Container encodes")
        .into_publication_parts();
    GcCandidateCatalogRow::from_intrinsic_summary(
        id,
        generation,
        u64::try_from(image.len()).expect("fixture length fits u64"),
        publication
            .intrinsic_summary()
            .expect("publication reconstructs intrinsic summary"),
    )
    .expect("candidate row is valid")
}

#[test]
#[allow(clippy::too_many_lines)]
fn generation_bound_reverse_dependencies_preserve_live_base_without_copying_dead_chunks() {
    let metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let base = b"maintenance-first-chunk";
    let mut target = base.to_vec();
    target[7] ^= 0x5a;
    let dependent_id = ContainerId::new([0xa3; 16]).expect("dependent ID is nonzero");
    let _dependent = containers
        .publish_zstd_prefix_pairs_verified(dependent_id, 2, &[(base, target.as_slice())])
        .expect("publish dependent target");
    let dead_id = ContainerId::new([0xa4; 16]).expect("dead ID is nonzero");
    containers
        .publish_raw(dead_id, 3, &[b"unrelated-dead-gc-padding"])
        .expect("publish unrelated dead padding Container");
    let target_manifest = ManifestLeaf::new(
        u64::try_from(target.len()).expect("target length fits u64"),
        vec![ManifestExtent::Data {
            logical_length: u64::try_from(target.len()).expect("target length fits u64"),
            chunk_id: ChunkId::of(&target),
        }],
    )
    .expect("target Manifest is valid");
    let target_root = generations
        .publish_manifest(&target_manifest)
        .expect("publish target Manifest");
    let target_namespace = NamespaceRoot::new(
        1_024,
        3,
        2,
        vec![
            DurableInode::new(
                2,
                0o640,
                1_000,
                1_000,
                1,
                2,
                u64::try_from(target.len()).expect("target length fits u64"),
                target_root,
            )
            .expect("target inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("target name is valid")],
    )
    .expect("target Namespace Root is valid");
    generations
        .commit_namespace_with_data(&target_namespace, &containers)
        .expect("commit live PREFIX target while previous generation retains its Base");
    let maintenance =
        MaintenanceRepository::new(generations, containers.clone(), indexes.clone(), profile);
    maintenance
        .rebuild_exact_index()
        .expect("active Exact generation resolves the dependent victim");

    let catalog = GcCandidateCatalogRepository::new(metadata);
    catalog
        .publish_rows(
            1,
            0,
            0,
            2,
            [
                candidate_row(
                    [0x83; 16],
                    1,
                    &[base.as_slice(), b"maintenance-second-chunk".as_slice()],
                ),
                candidate_row([0xa4; 16], 3, &[b"unrelated-dead-gc-padding"]),
            ],
        )
        .expect("publish dependency test catalog");
    let snapshot = catalog
        .recover_latest()
        .expect("recover dependency catalog")
        .expect("dependency catalog exists");
    let shortlist = snapshot
        .shortlist(GcCandidateSelectionMode::Urgent, 2, 4)
        .expect("select Base plus dead padding; the live dependent stays outside the victim set");
    let proof = maintenance
        .prove_gc_candidates(
            &shortlist,
            DataPoolUsage::new(50, 100).expect("worked pool usage is valid"),
        )
        .expect("proof resolves the live reverse dependency generation");
    assert_eq!(proof.victim_containers(), 2);
    assert_eq!(proof.reverse_dependency_edges(), 1);
    assert_eq!(proof.reachable_victim_chunks(), 1);
    assert_eq!(
        proof.replacement_chunks(),
        1,
        "the incoming live Base edge requires replacement; the unrelated dead Chunk does not"
    );
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("replacement closes unknown dependency before deletion");

    let active = indexes
        .recover_active()
        .expect("recover replacement Exact generation")
        .expect("replacement Exact generation exists");
    assert_eq!(
        containers
            .read_verified_chunk_with_index(
                &active,
                ChunkId::of(&target),
                u64::try_from(target.len()).expect("fixture length fits u64"),
            )
            .expect("formerly dependent target remains byte exact"),
        target
    );
    maintenance
        .scrub()
        .expect("post-replacement scrub verifies the dependency closure");
}

#[test]
fn asynchronous_job_runs_scrub_in_background_and_escalates_large_gc() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    containers
        .publish_raw(
            ContainerId::new([0x85; 16]).expect("fixture ID is nonzero"),
            2,
            &[b"asynchronous-unreachable-container"],
        )
        .expect("publish asynchronous GC candidate");
    let inspect = containers.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let job = maintenance
        .start_scrub_and_gc(DataPoolUsage::new(10, 100).expect("worked pool usage is valid"))
        .expect("maintenance coordinator starts without blocking the caller");
    assert_eq!(job.scrub_priority(), MaintenancePriority::Background);
    let report = job
        .wait()
        .expect("background scrub and escalated GC complete");

    assert_eq!(report.scrub_priority(), MaintenancePriority::Background);
    assert_eq!(report.gc().priority(), MaintenancePriority::Normal);
    assert_eq!(report.gc().containers_removed(), 1);
    assert_eq!(
        inspect
            .audit_published()
            .expect("remaining Container Store is valid")
            .containers(),
        1
    );
}

#[test]
fn full_speed_gc_now_overrides_background_cpu_and_io_scheduling() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let job = maintenance
        .start_scrub_and_gc_with_mode(
            DataPoolUsage::new(10, 100).expect("worked pool usage is valid"),
            MaintenanceExecutionMode::FullSpeed,
        )
        .expect("full-speed maintenance coordinator starts");
    assert_eq!(job.scrub_priority(), MaintenancePriority::Normal);

    let report = job.wait().expect("full-speed scrub and GC complete");
    assert_eq!(report.scrub_priority(), MaintenancePriority::Normal);
    assert_eq!(report.gc().priority(), MaintenancePriority::Normal);
}

#[test]
fn gc_rejects_a_scrub_plan_after_the_online_generation_pair_changes() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    containers
        .publish_raw(
            ContainerId::new([0x86; 16]).expect("fixture ID is nonzero"),
            2,
            &[b"stale-plan-unreachable-container"],
        )
        .expect("publish stale-plan candidate");
    let advance = generations.clone();
    let advance_containers = containers.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(10, 100).expect("worked pool usage is valid"))
        .expect("scrub plan is initially current");
    let current = advance
        .recover_latest_with_data(&advance_containers)
        .expect("recover current generation")
        .expect("fixture generation exists");
    advance
        .commit_namespace_with_data(current.namespace_root(), &advance_containers)
        .expect("advance the online generation pair after scrub");

    assert!(matches!(
        maintenance.garbage_collect(plan),
        Err(MaintenanceError::StaleGcPlan)
    ));
    assert_eq!(
        advance_containers
            .audit_published()
            .expect("stale-plan rejection leaves DATA untouched")
            .containers(),
        2
    );
}

#[test]
fn every_gc_delete_fault_keeps_the_live_graph_and_post_crash_scrub_valid() {
    let probe_data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(MemoryStorageIo::new(), probe_data.clone());
    containers
        .publish_raw(
            ContainerId::new([0x87; 16]).expect("fixture ID is nonzero"),
            2,
            &[b"fault-matrix-unreachable-container"],
        )
        .expect("publish GC fault candidate");
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let plan = probe
        .scrub_for_gc(DataPoolUsage::new(10, 100).expect("worked pool usage is valid"))
        .expect("probe scrub succeeds");
    let baseline = probe_data.operation_count();
    probe.garbage_collect(plan).expect("probe GC succeeds");
    let operations = probe_data.operations();
    let fault_positions: Vec<_> = operations[baseline..]
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, operation)| {
            matches!(
                operation,
                StorageOperation::RemoveFile | StorageOperation::SyncRoot
            )
        })
        .collect();
    assert_eq!(
        fault_positions.len(),
        2,
        "GC has one exact unlink and one directory durability point"
    );

    for (relative, operation) in fault_positions {
        for fail_after in [false, true] {
            let absolute = baseline + relative;
            let data = if fail_after {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let metadata = MemoryStorageIo::new();
            let (generations, containers, indexes, profile) =
                seeded_repositories_using(metadata, data.clone());
            containers
                .publish_raw(
                    ContainerId::new([0x87; 16]).expect("fixture ID is nonzero"),
                    2,
                    &[b"fault-matrix-unreachable-container"],
                )
                .expect("publish fault candidate before configured position");
            let inspect = containers.clone();
            let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
            let plan = maintenance
                .scrub_for_gc(DataPoolUsage::new(10, 100).expect("worked pool usage is valid"))
                .expect("fault case scrub succeeds before deletion");
            assert!(
                maintenance.garbage_collect(plan).is_err(),
                "configured {operation:?} fault must be observed"
            );
            data.crash();
            let live = b"maintenance-first-chunk";
            assert_eq!(
                inspect
                    .read_verified_chunk(
                        ChunkId::of(live),
                        u64::try_from(live.len()).expect("fixture length fits u64"),
                    )
                    .expect("live graph remains readable after GC fault and crash"),
                live,
            );
            maintenance
                .scrub()
                .expect("post-crash scrub accepts retained or durably deleted garbage");
        }
    }
}

#[test]
fn rebuild_publishes_and_activates_one_new_exact_index_generation() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let lookup_indexes = indexes.clone();
    let lookup_containers = containers.clone();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let rebuilt = maintenance
        .rebuild_exact_index()
        .expect("rebuild fully verifies and activates replacement Runs");

    assert_eq!(rebuilt.containers_scanned(), 1);
    assert_eq!(rebuilt.entries_rebuilt(), 2);
    assert_eq!(rebuilt.run_families(), 1);
    assert_eq!(rebuilt.physical_runs(), 1);
    assert_eq!(rebuilt.run_set_generation(), 1);
    assert_eq!(rebuilt.activation_generation(), 1);

    let active = lookup_indexes
        .recover_active()
        .expect("reopen rebuilt Exact Index")
        .expect("rebuilt Exact Index is active");
    let expected = b"maintenance-first-chunk";
    assert_eq!(
        lookup_containers
            .read_verified_chunk_with_index(
                &active,
                ChunkId::of(expected),
                u64::try_from(expected.len()).expect("fixture length fits u64"),
            )
            .expect("ordinary indexed lookup remains byte exact"),
        expected
    );

    let scrubbed = maintenance
        .scrub()
        .expect("post-rebuild end-to-end scrub succeeds");
    assert_eq!(scrubbed.exact_activation_generation(), Some(1));
    assert_eq!(scrubbed.exact_active_locations_verified(), 2);
}

#[test]
fn paired_rebuild_scans_data_once_and_binds_similarity_to_exact() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data.clone());
    let exact_lookup = indexes.clone();
    let similarities = SimilarityIndexRepository::new(metadata);
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    let data_baseline = data.operation_count();

    let rebuilt = maintenance
        .rebuild_pool_indexes(&similarities)
        .expect("paired pool-index rebuild succeeds");

    assert_eq!(rebuilt.exact().containers_scanned(), 1);
    assert_eq!(rebuilt.exact().entries_rebuilt(), 2);
    assert_eq!(rebuilt.similarity_entries(), 2);
    assert_eq!(rebuilt.similarity_partitions(), 1);
    assert_eq!(
        data.operations()[data_baseline..]
            .iter()
            .filter(|operation| **operation == StorageOperation::Read)
            .count(),
        1,
        "one verified Container read feeds both index builders"
    );

    let active = exact_lookup
        .recover_active()
        .expect("recover paired Exact index")
        .expect("paired Exact index is active");
    let exact_id = active
        .run_set()
        .id()
        .expect("identify active Exact Run Set");
    let recovered = similarities
        .recover_latest_for_exact(exact_id)
        .expect("recover paired Similarity index")
        .expect("paired Similarity index is active");
    assert_eq!(recovered.status().source_exact_run_set_id(), Some(exact_id));
    assert_eq!(
        similarities
            .audit_latest_for_exact(exact_id)
            .expect("offline audit accepts the paired identity")
            .expect("paired Similarity audit exists")
            .entries_verified(),
        2
    );
}

#[test]
fn paired_rebuild_keeps_dependent_targets_out_of_similarity_bases() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data);
    let base = b"maintenance-first-chunk";
    let mut target = base.to_vec();
    target[8] ^= 0x31;
    containers
        .publish_zstd_prefix_pairs_verified(
            ContainerId::new([0xa3; 16]).expect("dependent Container ID is nonzero"),
            2,
            &[(base, target.as_slice())],
        )
        .expect("publish dependent target fixture");
    let similarities = SimilarityIndexRepository::new(metadata);
    let maintenance = MaintenanceRepository::new(generations, containers, indexes.clone(), profile);

    let rebuilt = maintenance
        .rebuild_pool_indexes(&similarities)
        .expect("rebuild indexes with one dependent target");

    assert_eq!(rebuilt.exact().entries_rebuilt(), 3);
    assert_eq!(
        rebuilt.similarity_entries(),
        2,
        "only the two independently decodable fixture Chunks may become Bases"
    );
    let active = indexes
        .recover_active()
        .expect("recover rebuilt Exact")
        .expect("rebuilt Exact exists");
    let lookup = active
        .lookup_transitions(
            ChunkId::of(&target),
            u32::try_from(target.len()).expect("fixture length fits u32"),
        )
        .expect("dependent target remains exactly addressable");
    assert!(
        lookup
            .candidates()
            .iter()
            .any(|entry| entry.location().dependency_id() == ChunkId::of(base).bytes())
    );
}

#[test]
fn paired_rebuild_publishes_an_empty_tombstone_for_an_empty_pool() {
    let metadata = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x91; 32]).expect("nonzero fixture policy");
    let profile = ExactIndexProfileId::new([0x92; 32]).expect("nonzero fixture profile");
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    let similarities = SimilarityIndexRepository::new(metadata.clone());
    let maintenance = MaintenanceRepository::new(
        GenerationRepository::new(metadata, policy),
        ContainerRepository::new(MemoryStorageIo::new()),
        indexes.clone(),
        profile,
    );

    let rebuilt = maintenance
        .rebuild_pool_indexes(&similarities)
        .expect("empty pool rebuild succeeds");
    assert_eq!(rebuilt.exact().entries_rebuilt(), 0);
    assert_eq!(rebuilt.similarity_entries(), 0);
    assert_eq!(rebuilt.similarity_partitions(), 0);

    let active = indexes
        .recover_active()
        .expect("recover empty Exact Run Set")
        .expect("empty Exact Run Set is active");
    let recovered = similarities
        .recover_latest_for_exact(active.run_set().id().expect("identify empty Exact Run Set"))
        .expect("recover bound empty Similarity family")
        .expect("empty Similarity family is active");
    assert_eq!(recovered.status().entries_streamed(), 0);
    assert_eq!(recovered.status().buckets(), 0);
}

#[test]
fn paired_rebuild_skips_orphan_similarity_partition_generations() {
    let metadata = MemoryStorageIo::new();
    let orphan = "similarity-part.0001.0001.0000000000000009.0000.fds";
    metadata
        .create_new(orphan)
        .expect("create orphan partition name");
    metadata.sync_root().expect("persist orphan partition name");
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let similarities = SimilarityIndexRepository::new(metadata);
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    assert_eq!(
        maintenance
            .rebuild_pool_indexes(&similarities)
            .expect("rebuild allocates after orphan")
            .similarity_generation(),
        10
    );
}

#[test]
fn every_paired_rebuild_fault_exposes_only_a_bound_similarity_family() {
    let probe_metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(probe_metadata.clone(), MemoryStorageIo::new());
    let probe_similarity = SimilarityIndexRepository::new(probe_metadata.clone());
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let baseline = probe_metadata.operation_count();
    probe
        .rebuild_pool_indexes(&probe_similarity)
        .expect("probe paired rebuild succeeds");
    let operations = probe_metadata.operations()[baseline..].to_vec();

    for (relative, operation) in operations.iter().copied().enumerate() {
        for fail_after in [false, true] {
            let absolute = baseline + relative;
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let (generations, containers, indexes, profile) =
                seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
            let similarities = SimilarityIndexRepository::new(metadata.clone());
            let maintenance =
                MaintenanceRepository::new(generations, containers, indexes.clone(), profile);
            let outcome = maintenance.rebuild_pool_indexes(&similarities);
            metadata.crash();

            let active = indexes
                .recover_active()
                .expect("Exact recovery remains structurally valid");
            let recovered_similarity = similarities
                .recover_latest()
                .expect("Similarity recovery remains structurally valid");
            if let Some(similarity) = recovered_similarity {
                let active = active.as_ref().expect(
                    "a visible Similarity family always has its Exact Run Set active first",
                );
                assert_eq!(
                    similarity.status().source_exact_run_set_id(),
                    Some(
                        active
                            .run_set()
                            .id()
                            .expect("identify recovered Exact Run Set")
                    ),
                    "fault at {relative} ({operation:?}), fail_after={fail_after}"
                );
            }
            if outcome.is_ok() {
                assert!(active.is_some());
                assert!(
                    similarities
                        .recover_latest()
                        .expect("acknowledged Similarity recovery succeeds")
                        .is_some()
                );
            }
        }
    }
}

#[test]
fn replacement_fault_never_selects_the_old_similarity_for_the_new_exact_set() {
    let probe_metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(probe_metadata.clone(), MemoryStorageIo::new());
    let probe_similarity = SimilarityIndexRepository::new(probe_metadata.clone());
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    probe
        .rebuild_pool_indexes(&probe_similarity)
        .expect("seed probe pair");
    let second_baseline = probe_metadata.operation_count();
    probe
        .rebuild_pool_indexes(&probe_similarity)
        .expect("probe replacement pair succeeds");
    let operations = probe_metadata.operations()[second_baseline..].to_vec();

    for (relative, operation) in operations.iter().copied().enumerate() {
        for fail_after in [false, true] {
            let absolute = second_baseline + relative;
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(absolute)
            } else {
                MemoryStorageIo::with_fail_before(absolute)
            };
            let (generations, containers, indexes, profile) =
                seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
            let similarities = SimilarityIndexRepository::new(metadata.clone());
            let maintenance =
                MaintenanceRepository::new(generations, containers, indexes.clone(), profile);
            maintenance
                .rebuild_pool_indexes(&similarities)
                .expect("fault is positioned in the replacement rebuild");
            let outcome = maintenance.rebuild_pool_indexes(&similarities);
            metadata.crash();

            let active = indexes
                .recover_active()
                .expect("replacement Exact recovery is valid")
                .expect("the initial Exact pair remains available");
            let exact_id = active
                .run_set()
                .id()
                .expect("identify selected replacement Exact Run Set");
            let paired = similarities
                .recover_latest_for_exact(exact_id)
                .expect("paired selection treats an older family as unavailable");
            if let Some(index) = paired.as_ref() {
                assert_eq!(index.status().source_exact_run_set_id(), Some(exact_id));
            }
            if outcome.is_ok() {
                assert!(
                    paired.is_some(),
                    "acknowledged replacement at {relative} ({operation:?}), fail_after={fail_after} must expose its pair"
                );
            }
        }
    }
}

#[test]
fn scrub_rejects_corrupt_container_and_corrupt_active_index_page() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), data.clone());
    let maintenance = MaintenanceRepository::new(
        generations.clone(),
        containers.clone(),
        indexes.clone(),
        profile,
    );
    maintenance
        .rebuild_exact_index()
        .expect("seed a complete active Exact Index");

    let run_name = format!("{}.0000000000000001.fdx", "82".repeat(32));
    metadata
        .write_at(&run_name, 4_096, &[0xFF])
        .expect("corrupt one active Run page");
    metadata
        .sync_file(&run_name)
        .expect("make active Run corruption durable");
    assert!(matches!(
        maintenance.scrub(),
        Err(MaintenanceError::ExactIndex(_))
    ));

    let corrupt_data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(MemoryStorageIo::new(), corrupt_data.clone());
    let container_name = format!("{}.fdc", "83".repeat(16));
    corrupt_data
        .write_at(&container_name, 0, &[0xFF])
        .expect("corrupt the published Container header");
    corrupt_data
        .sync_file(&container_name)
        .expect("make Container corruption durable");
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);
    assert!(matches!(
        maintenance.scrub(),
        Err(MaintenanceError::Store(StoreError::Format(_)))
    ));
}

#[test]
fn scrub_rejects_cross_run_chunk_length_conflict_without_a_full_chunk_map() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xA1; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0xA2; 32]).expect("profile ID is nonzero");
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    let chunk_id = ChunkId::from_bytes([0xA3; 32]);
    let mut refs = Vec::new();
    for (generation, logical_length) in [(1_u64, 16_384_u32), (2, 32_768)] {
        let record_length = (logical_length + 255) / 64 * 64;
        let location = ExactIndexLocation::raw(
            ContainerId::new([u8::try_from(generation).expect("fixture generation fits u8"); 16])
                .expect("container ID is nonzero"),
            generation,
            4_096,
            record_length,
            u32::try_from(generation).expect("fixture generation fits u32"),
        )
        .expect("worked RAW location is valid");
        let entry = ExactIndexEntry::active(chunk_id, logical_length, location)
            .expect("worked active entry is valid");
        let run = ExactIndexRun::new(profile, generation, vec![entry])
            .expect("individual Run is locally valid");
        let descriptor = indexes.publish(&run).expect("publish conflicting Run");
        refs.push(ExactIndexRunRef::new(0, descriptor).expect("Run reference is valid"));
    }
    let run_set = ExactIndexRunSet::new(profile, 1, refs).expect("Run Set is locally valid");
    indexes
        .activate(&run_set)
        .expect("legacy activation accepts locally valid Runs");
    let maintenance = MaintenanceRepository::new(
        GenerationRepository::new(metadata, policy),
        ContainerRepository::new(data),
        indexes,
        profile,
    );

    assert!(matches!(
        maintenance.scrub(),
        Err(MaintenanceError::ExactIndex(ExactIndexStoreError::Format(
            ExactIndexFormatError::ChunkLengthConflict
        )))
    ));
}

#[test]
fn repeated_rebuild_advances_generations_without_reusing_run_names() {
    let (generations, containers, indexes, profile) = seeded_repositories();
    let maintenance = MaintenanceRepository::new(generations, containers, indexes, profile);

    let first = maintenance
        .rebuild_exact_index()
        .expect("first rebuild succeeds");
    let second = maintenance
        .rebuild_exact_index()
        .expect("second rebuild succeeds");

    assert_eq!(first.run_set_generation(), 1);
    assert_eq!(first.activation_generation(), 1);
    assert_eq!(second.run_set_generation(), 2);
    assert_eq!(second.activation_generation(), 2);
    assert_eq!(second.entries_rebuilt(), first.entries_rebuilt());
    let scrubbed = maintenance
        .scrub()
        .expect("newest rebuild is fully scrubbed");
    assert_eq!(scrubbed.exact_activation_generation(), Some(2));
    assert_eq!(scrubbed.exact_active_locations_verified(), 2);
}

#[test]
fn rebuild_streams_many_containers_through_bounded_fanin_compaction() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x91; 32]).expect("policy ID is nonzero");
    let profile = ExactIndexProfileId::new([0x92; 32]).expect("profile ID is nonzero");
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(data);
    let indexes = ExactIndexRunRepository::new(metadata);
    let repeated = b"same-logical-chunk-in-seventeen-containers";
    for ordinal in 1_u8..=17 {
        containers
            .publish_raw(
                ContainerId::new([ordinal; 16]).expect("container ID is nonzero"),
                u64::from(ordinal),
                &[repeated],
            )
            .expect("publish rebuild input Container");
    }
    let maintenance =
        MaintenanceRepository::new(generations, containers.clone(), indexes.clone(), profile);

    let rebuilt = maintenance
        .rebuild_exact_index()
        .expect("bounded-fanin rebuild succeeds");

    assert_eq!(rebuilt.containers_scanned(), 17);
    assert_eq!(rebuilt.entries_rebuilt(), 17);
    assert_eq!(rebuilt.run_families(), 2);
    assert_eq!(rebuilt.physical_runs(), 2);
    let active = indexes
        .recover_active()
        .expect("reopen compacted index")
        .expect("compacted index is active");
    let lookup = active
        .lookup_transitions(
            ChunkId::of(repeated),
            u32::try_from(repeated.len()).expect("fixture length fits u32"),
        )
        .expect("lookup across compacted families succeeds");
    assert!(lookup.complete());
    assert_eq!(lookup.candidates().len(), 17);
    assert_eq!(
        indexes
            .audit_active_locations(&containers)
            .expect("all compacted locations pair with DATA")
            .expect("index is active")
            .active_locations(),
        17
    );
}

#[test]
fn retry_after_orphaned_run_allocates_a_fresh_run_generation() {
    let probe_metadata = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(probe_metadata.clone(), MemoryStorageIo::new());
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let baseline = probe_metadata.operation_count();
    probe.rebuild_exact_index().expect("probe rebuild succeeds");
    let operations = probe_metadata.operations()[baseline..].to_vec();
    let run_publish_sync = operations
        .iter()
        .position(|operation| *operation == StorageOperation::SyncRoot)
        .expect("Run publication contains a directory sync");

    let metadata = MemoryStorageIo::with_fail_after(baseline + run_publish_sync);
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(metadata.clone(), MemoryStorageIo::new());
    let maintenance = MaintenanceRepository::new(generations, containers, indexes.clone(), profile);
    assert!(maintenance.rebuild_exact_index().is_err());
    metadata.crash();
    assert!(
        indexes
            .recover_active()
            .expect("activation remains absent")
            .is_none()
    );

    let rebuilt = maintenance
        .rebuild_exact_index()
        .expect("retry skips orphaned immutable Run generation");
    assert_eq!(rebuilt.activation_generation(), 1);
    let names = metadata.list_names().expect("list durable metadata names");
    let run_names = names
        .iter()
        .filter(|name| name.strip_suffix(".fdx").is_some())
        .count();
    assert_eq!(run_names, 2, "orphan and retry Run use distinct names");
    assert_eq!(
        maintenance
            .scrub()
            .expect("retried index is fully scrubbed")
            .exact_active_locations_verified(),
        2
    );
}

#[test]
fn every_rebuild_metadata_fault_recovers_only_no_index_or_the_complete_new_index() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_data = MemoryStorageIo::new();
    let (generations, containers, indexes, profile) =
        seeded_repositories_using(probe_metadata.clone(), probe_data);
    let probe = MaintenanceRepository::new(generations, containers, indexes, profile);
    let baseline = probe_metadata.operation_count();
    probe.rebuild_exact_index().expect("probe rebuild succeeds");
    let operations = probe_metadata.operations()[baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));

    for (relative, operation) in operations.iter().copied().enumerate() {
        let absolute = baseline + relative;
        let metadata = MemoryStorageIo::with_fail_before(absolute);
        let data = MemoryStorageIo::new();
        let (generations, containers, indexes, profile) =
            seeded_repositories_using(metadata.clone(), data);
        let maintenance =
            MaintenanceRepository::new(generations, containers.clone(), indexes.clone(), profile);
        let outcome = maintenance.rebuild_exact_index();
        metadata.crash();
        let active = indexes
            .recover_active()
            .expect("activation recovery remains structurally valid");
        assert_eq!(
            active.is_some(),
            outcome.is_ok(),
            "fail-before {relative} ({operation:?}) must recover no index or the acknowledged complete index"
        );
        if active.is_some() {
            assert_eq!(
                indexes
                    .audit_active_locations(&containers)
                    .expect("acknowledged index remains fully paired")
                    .expect("acknowledged index is selected")
                    .active_locations(),
                2
            );
        }

        let metadata = MemoryStorageIo::with_fail_after(absolute);
        let data = MemoryStorageIo::new();
        let (generations, containers, indexes, profile) =
            seeded_repositories_using(metadata.clone(), data);
        let maintenance =
            MaintenanceRepository::new(generations, containers.clone(), indexes.clone(), profile);
        let outcome = maintenance.rebuild_exact_index();
        metadata.crash();
        let active = indexes
            .recover_active()
            .expect("activation recovery remains structurally valid");
        if outcome.is_ok() {
            assert!(
                active.is_some(),
                "acknowledged fail-after {relative} ({operation:?}) must remain active"
            );
        } else {
            assert_eq!(
                active.is_some(),
                relative + 1 == operations.len(),
                "only an effective final activation sync may expose an unacknowledged rebuilt index"
            );
        }
        if active.is_some() {
            assert_eq!(
                indexes
                    .audit_active_locations(&containers)
                    .expect("recovered index remains fully paired")
                    .expect("recovered index is selected")
                    .active_locations(),
                2
            );
        }
    }
}
