use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ExactIndexEntry, ExactIndexFormatError, ExactIndexLocation,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, ManifestExtent,
    ManifestLeaf, NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_store::{
    ContainerRepository, DataPoolUsage, ExactIndexRunRepository, ExactIndexStoreError,
    GenerationRepository, MaintenanceError, MaintenancePriority, MaintenanceRepository, StorageIo,
    StoreError,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

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
