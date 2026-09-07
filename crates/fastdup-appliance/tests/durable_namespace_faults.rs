use fastdup_appliance::{DurableNamespace, HistoricalProofCacheStatus, recover_mount};
use fastdup_format::PolicySetId;
use fastdup_posix::{
    FS_IMMUTABLE_FL, FallocateMode, HandleId, InodeId, Namespace, NamespaceConfig, OpenOptions,
    Operation, PosixError, ROOT_INODE, Reply, RequestContext, XattrSetMode,
};
use fastdup_store::{ContainerRepository, GenerationRepository};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 61,
};
const NAME: &[u8] = b"vm-\xff";
const PAYLOAD: &[u8] = b"durable-prefix";
const ROOT_CALLER: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};

#[test]
fn every_inode_metadata_checkpoint_fault_recovers_the_previous_or_complete_image() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers);
    let probe_inode = seed_metadata_predecessor(&probe);
    probe
        .checkpoint()
        .expect("checkpoint metadata predecessor")
        .expect("predecessor is dirty");
    let metadata_baseline = probe_metadata.operation_count();
    install_metadata_successor(&probe, probe_inode);
    probe
        .checkpoint()
        .expect("probe metadata checkpoint")
        .expect("metadata successor is dirty");
    let operations = probe_metadata.operations()[metadata_baseline..].to_vec();
    let final_sync = operations.len() - 1;
    assert_eq!(operations[final_sync], StorageOperation::SyncFile);

    for relative in 0..operations.len() {
        for fail_after in [false, true] {
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(metadata_baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(metadata_baseline + relative)
            };
            let containers = MemoryStorageIo::new();
            let appliance = open(metadata.clone(), containers.clone());
            let inode = seed_metadata_predecessor(&appliance);
            appliance
                .checkpoint()
                .expect("checkpoint metadata predecessor under injection")
                .expect("predecessor is dirty");
            install_metadata_successor(&appliance, inode);
            assert!(
                appliance.checkpoint().is_err(),
                "metadata fault relative={relative} after={fail_after} unexpectedly committed"
            );
            metadata.crash();
            containers.crash();
            let generations = GenerationRepository::new(metadata, policy());
            let container_repository = ContainerRepository::new(containers);
            let recovered = recover_mount(
                NamespaceConfig::default(),
                &generations,
                &container_repository,
            )
            .expect("recover one whole metadata image")
            .expect("metadata predecessor exists");
            let complete = fail_after && relative == final_sync;
            assert_metadata_image(&recovered, inode, complete);
        }
    }
}

fn seed_metadata_predecessor(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
) -> InodeId {
    let Reply::Created { entry, .. } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"metadata-fault",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create metadata fault fixture")
    else {
        panic!("ASSERT: create metadata fixture reply");
    };
    entry.attr.inode
}

fn install_metadata_successor(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
    inode: InodeId,
) {
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetXattr {
                inode,
                name: b"user.immutable.until",
                value: b"2038-01-19T03:14:07Z",
                mode: XattrSetMode::Upsert,
            },
        )
        .expect("set retention metadata");
    appliance
        .namespace()
        .dispatch(
            ROOT_CALLER,
            Operation::SetFileFlags {
                inode,
                flags: FS_IMMUTABLE_FL,
            },
        )
        .expect("set immutable metadata");
}

fn assert_metadata_image(namespace: &Namespace, inode: InodeId, complete: bool) {
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetXattr {
                inode,
                name: b"user.immutable.until",
            },
        ),
        if complete {
            Ok(Reply::Xattr(b"2038-01-19T03:14:07Z".to_vec()))
        } else {
            Err(PosixError::NoData)
        }
    );
    assert_eq!(
        namespace.dispatch(CALLER, Operation::GetFileFlags { inode }),
        Ok(Reply::FileFlags(if complete { FS_IMMUTABLE_FL } else { 0 }))
    );
}

#[test]
fn every_checkpoint_fault_recovers_only_the_previous_or_complete_generation() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers.clone());
    let metadata_baseline = probe_metadata.operation_count();
    let container_baseline = probe_containers.operation_count();
    write_fixture(&probe);
    probe
        .checkpoint()
        .expect("probe checkpoint succeeds")
        .expect("probe writes one generation");
    let metadata_operations = probe_metadata.operations()[metadata_baseline..].to_vec();
    let container_operations = probe_containers.operations()[container_baseline..].to_vec();
    assert_eq!(
        metadata_operations.last(),
        Some(&StorageOperation::SyncFile),
        "Commit WAL sync must remain the final metadata operation"
    );
    assert!(
        container_operations.contains(&StorageOperation::SyncRoot),
        "DATA publication must include a durable container-directory sync"
    );

    for relative in 0..container_operations.len() {
        for fail_after in [false, true] {
            let metadata = MemoryStorageIo::new();
            let containers = if fail_after {
                MemoryStorageIo::with_fail_after(container_baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(container_baseline + relative)
            };
            let appliance = open(metadata.clone(), containers.clone());
            write_fixture(&appliance);
            assert!(
                appliance.checkpoint().is_err(),
                "container fault relative={relative} after={fail_after} unexpectedly committed"
            );
            crash_and_assert(&metadata, &containers, RecoveryOracle::Previous);
        }
    }

    let final_metadata_sync = metadata_operations.len() - 1;
    for relative in 0..metadata_operations.len() {
        for fail_after in [false, true] {
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(metadata_baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(metadata_baseline + relative)
            };
            let containers = MemoryStorageIo::new();
            let appliance = open(metadata.clone(), containers.clone());
            write_fixture(&appliance);
            assert!(
                appliance.checkpoint().is_err(),
                "metadata fault relative={relative} after={fail_after} unexpectedly returned success"
            );
            let oracle = if fail_after && relative == final_metadata_sync {
                RecoveryOracle::Complete
            } else {
                RecoveryOracle::Previous
            };
            crash_and_assert(&metadata, &containers, oracle);
        }
    }
}

#[test]
fn namespace_only_checkpoint_reuses_the_installed_data_proof_without_a_scan() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let appliance = open(metadata, containers.clone());
    write_fixture(&appliance);
    appliance
        .checkpoint()
        .expect("commit fixture DATA")
        .expect("fixture requires a generation");
    let Reply::Created { .. } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"metadata-only",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create an empty file")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let baseline = containers.operation_count();
    appliance
        .checkpoint()
        .expect("commit namespace-only generation")
        .expect("new name requires a generation");
    let list_operations = containers.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::ListNames)
        .count();
    assert_eq!(
        list_operations, 0,
        "an unchanged installed DATA graph must not scan Containers again"
    );
}

#[test]
fn retry_after_ambiguous_container_publish_consumes_a_fresh_generation() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata, probe_containers.clone());
    let baseline = probe_containers.operation_count();
    write_fixture(&probe);
    probe
        .checkpoint()
        .expect("probe checkpoint succeeds")
        .expect("probe publishes one generation");
    let relative_sync_root = probe_containers.operations()[baseline..]
        .iter()
        .position(|operation| *operation == StorageOperation::SyncRoot)
        .expect("container publication ends with a directory sync");

    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::with_fail_after(baseline + relative_sync_root);
    let appliance = open(metadata, containers.clone());
    write_fixture(&appliance);
    assert!(
        appliance.checkpoint().is_err(),
        "fail-after directory sync must remain an ambiguous publication error"
    );
    assert_eq!(
        appliance.historical_proof_cache_status().entry_count(),
        0,
        "a failed commit must keep its Frozen proofs pinned outside Historical S3-FIFO"
    );
    appliance
        .checkpoint()
        .expect("retry the same frozen commit cut")
        .expect("retry publishes a complete generation");
    assert_historical_demotion_or_memory_pressure(appliance.historical_proof_cache_status());
    assert_eq!(
        appliance.generation_proof_set_status().frozen_proofs(),
        0,
        "a successful retry must release its Frozen proof ownership"
    );
    let mut generations = ContainerRepository::new(containers)
        .verify_published()
        .expect("both immutable publications verify")
        .into_iter()
        .map(fastdup_store::PublishedContainerSummary::container_generation)
        .collect::<Vec<_>>();
    generations.sort_unstable();
    assert_eq!(
        generations,
        vec![1, 2],
        "an ambiguous durable container must not cause generation reuse"
    );
}

#[test]
fn metadata_failure_keeps_verified_data_proofs_frozen_until_retry_commits() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers);
    let baseline = probe_metadata.operation_count();
    write_fixture(&probe);
    probe
        .checkpoint()
        .expect("probe checkpoint succeeds")
        .expect("probe writes one generation");
    assert!(
        !probe_metadata.operations()[baseline..].is_empty(),
        "checkpoint must publish metadata after DATA verification"
    );

    let metadata = MemoryStorageIo::with_fail_before(baseline);
    let containers = MemoryStorageIo::new();
    let appliance = open(metadata, containers);
    write_fixture(&appliance);
    assert!(
        appliance.checkpoint().is_err(),
        "metadata failure after DATA verification must fail the commit"
    );
    assert_eq!(
        appliance.historical_proof_cache_status().entry_count(),
        0,
        "failed metadata visibility must not demote Frozen proofs"
    );
    assert!(
        appliance.generation_proof_set_status().frozen_proofs() > 0,
        "verified DATA must remain pinned for the Frozen retry"
    );

    appliance
        .checkpoint()
        .expect("retry Frozen metadata commit")
        .expect("retry commits the generation");
    assert_eq!(appliance.generation_proof_set_status().frozen_proofs(), 0);
    assert_historical_demotion_or_memory_pressure(appliance.historical_proof_cache_status());
}

fn assert_historical_demotion_or_memory_pressure(status: HistoricalProofCacheStatus) {
    if status.entry_count() > 0 {
        assert!(status.admissions() > 0);
        return;
    }
    assert!(
        status.admission_rejections() > 0
            && (status.swap_used_bytes() > 0 || status.available_bytes() <= status.reserve_bytes()),
        "a successful commit may omit Historical proofs only under observed memory pressure"
    );
}

#[test]
fn retry_resumes_an_ambiguous_commit_cut_lane_drain_byte_exactly() {
    let payload = (0..1_048_576_usize)
        .map(|index| u8::try_from(index % 251).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata, probe_containers.clone());
    let baseline = probe_containers.operation_count();
    write_named(&probe, b"partial-drain", &payload);
    probe
        .checkpoint()
        .expect("probe checkpoint succeeds")
        .expect("probe publishes one generation");
    let relative_sync_root = probe_containers.operations()[baseline..]
        .iter()
        .position(|operation| *operation == StorageOperation::SyncRoot)
        .expect("commit-cut lane drain publishes one durable Container");

    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::with_fail_after(baseline + relative_sync_root);
    let appliance = open(metadata.clone(), containers.clone());
    write_named(&appliance, b"partial-drain", &payload);
    assert!(
        appliance.checkpoint().is_err(),
        "ambiguous lane-drain publication must abort metadata visibility"
    );
    appliance
        .checkpoint()
        .expect("retry the same Frozen Commit Cut")
        .expect("retry publishes the complete generation");
    drop(appliance);

    metadata.crash();
    containers.crash();
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy()),
        &ContainerRepository::new(containers),
    )
    .expect("recover retried lane-drain generation")
    .expect("retried generation exists");
    let Reply::Entry(entry) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"partial-drain",
            },
        )
        .expect("recover drained file")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    let Reply::Opened(handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open drained file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        recovered.dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: 1_048_576,
            },
        ),
        Ok(Reply::Data(payload))
    );
}

#[test]
fn recovery_mount_installs_the_verified_graph_without_a_duplicate_data_scan() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let appliance = open(metadata.clone(), containers.clone());
    write_fixture(&appliance);
    appliance
        .checkpoint()
        .expect("commit fixture DATA")
        .expect("fixture requires a generation");
    drop(appliance);

    let baseline = containers.operation_count();
    recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy()),
        &ContainerRepository::new(containers.clone()),
    )
    .expect("recover complete DATA graph")
    .expect("committed namespace exists");
    let list_operations = containers.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::ListNames)
        .count();
    assert_eq!(
        list_operations, 1,
        "one recovery graph proof must feed reader installation without rescanning DATA"
    );
}

#[test]
fn every_path_local_truncate_fault_recovers_the_previous_or_exact_cut() {
    check_truncate_faults(false);
}

#[test]
fn every_mixed_shrink_fault_recovers_the_previous_or_complete_header_and_cut() {
    check_truncate_faults(true);
}

fn check_truncate_faults(rewrite_header: bool) {
    const PREVIOUS_SIZE: u64 = 1_048_576;
    let truncated_size: u64 = if rewrite_header { 8 } else { 128 };
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers);
    let (probe_inode, probe_handle) = seed_truncate_predecessor(&probe, PREVIOUS_SIZE);
    probe
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: probe_inode,
                handle: Some(probe_handle),
                length: truncated_size,
            },
        )
        .expect("truncate probe predecessor");
    if rewrite_header {
        probe
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: probe_inode,
                    handle: probe_handle,
                    offset: 0,
                    data: b"HEAD",
                },
            )
            .unwrap();
    }
    let baseline = probe_metadata.operation_count();
    probe
        .checkpoint()
        .expect("checkpoint probe truncate")
        .expect("probe truncate needs one generation");
    let operations = probe_metadata.operations()[baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));
    let final_sync = operations.len() - 1;

    for relative in 0..operations.len() {
        for fail_after in [false, true] {
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(baseline + relative)
            };
            let containers = MemoryStorageIo::new();
            let appliance = open(metadata.clone(), containers.clone());
            let (inode, handle) = seed_truncate_predecessor(&appliance, PREVIOUS_SIZE);
            appliance
                .namespace()
                .dispatch(
                    CALLER,
                    Operation::SetLength {
                        inode,
                        handle: Some(handle),
                        length: truncated_size,
                    },
                )
                .expect("truncate injected predecessor");
            if rewrite_header {
                appliance
                    .namespace()
                    .dispatch(
                        CALLER,
                        Operation::Write {
                            inode,
                            handle,
                            offset: 0,
                            data: b"HEAD",
                        },
                    )
                    .unwrap();
            }
            assert!(
                appliance.checkpoint().is_err(),
                "truncate fault relative={relative} after={fail_after} unexpectedly returned success"
            );
            drop(appliance);
            metadata.crash();
            containers.crash();
            let recovered = recover_mount(
                NamespaceConfig::default(),
                &GenerationRepository::new(metadata, policy()),
                &ContainerRepository::new(containers),
            )
            .expect("one atomic truncate generation remains recoverable")
            .expect("truncate predecessor exists");
            let Reply::Attr(attr) = recovered
                .dispatch(CALLER, Operation::GetAttr { inode })
                .expect("stat recovered truncate fixture")
            else {
                panic!("ASSERT: getattr returned the wrong reply variant");
            };
            let expected = if fail_after && relative == final_sync {
                truncated_size
            } else {
                PREVIOUS_SIZE
            };
            assert_eq!(
                attr.size, expected,
                "fault relative={relative} after={fail_after} exposed a mixed truncate"
            );
            let Reply::Opened(handle) = recovered
                .dispatch(
                    CALLER,
                    Operation::Open {
                        inode,
                        options: OpenOptions::READ_ONLY,
                        truncate: false,
                    },
                )
                .unwrap()
            else {
                panic!("open recovered shrink")
            };
            let header = if rewrite_header && expected == truncated_size {
                b"HEAD".to_vec()
            } else {
                PAYLOAD[..4].to_vec()
            };
            assert_eq!(
                recovered
                    .dispatch(
                        CALLER,
                        Operation::Read {
                            inode,
                            handle,
                            offset: 0,
                            length: 4
                        }
                    )
                    .unwrap(),
                Reply::Data(header)
            );
        }
    }
}

#[test]
fn every_sparse_splice_fault_recovers_the_previous_or_complete_layout() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers.clone());
    let (probe_inode, probe_handle, previous) = seed_sparse_splice_predecessor(&probe);
    let complete = apply_sparse_splice(&probe, probe_inode, probe_handle, previous.clone());
    let metadata_baseline = probe_metadata.operation_count();
    let container_baseline = probe_containers.operation_count();
    probe
        .checkpoint()
        .expect("checkpoint sparse splice probe")
        .expect("sparse splice needs one generation");
    let operations = probe_metadata.operations()[metadata_baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));
    assert_eq!(
        probe_containers.operation_count(),
        container_baseline,
        "sparse structural edits must not read or publish DATA containers"
    );
    let final_sync = operations.len() - 1;

    for relative in 0..operations.len() {
        for fail_after in [false, true] {
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(metadata_baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(metadata_baseline + relative)
            };
            let containers = MemoryStorageIo::new();
            let appliance = open(metadata.clone(), containers.clone());
            let (inode, handle, candidate_previous) = seed_sparse_splice_predecessor(&appliance);
            assert_eq!(candidate_previous, previous);
            assert_eq!(
                apply_sparse_splice(&appliance, inode, handle, candidate_previous),
                complete
            );
            assert!(
                appliance.checkpoint().is_err(),
                "sparse splice fault relative={relative} after={fail_after} unexpectedly committed"
            );
            drop(appliance);
            metadata.crash();
            containers.crash();
            let recovered = recover_mount(
                NamespaceConfig::default(),
                &GenerationRepository::new(metadata, policy()),
                &ContainerRepository::new(containers),
            )
            .expect("recover one whole sparse splice generation")
            .expect("sparse predecessor exists");
            let Reply::Opened(recovered_handle) = recovered
                .dispatch(
                    CALLER,
                    Operation::Open {
                        inode,
                        options: OpenOptions::READ_ONLY,
                        truncate: false,
                    },
                )
                .expect("open recovered sparse splice file")
            else {
                panic!("ASSERT: open returned the wrong reply");
            };
            let expected = if fail_after && relative == final_sync {
                &complete
            } else {
                &previous
            };
            assert_eq!(
                read_range(
                    &recovered,
                    inode,
                    recovered_handle,
                    0,
                    u32::try_from(expected.len()).expect("fixture length fits u32"),
                ),
                *expected,
                "fault relative={relative} after={fail_after} exposed a mixed sparse splice"
            );
        }
    }
}

fn seed_sparse_splice_predecessor(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
) -> (InodeId, HandleId, Vec<u8>) {
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"sparse-splice-fault",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse splice predecessor")
    else {
        panic!("ASSERT: create returned the wrong reply");
    };
    let inode = entry.attr.inode;
    for (offset, bytes) in [(0_u64, b"abcdefgh".as_slice()), (16, b"XYZ".as_slice())] {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset,
                    data: bytes,
                },
            )
            .expect("write sparse splice predecessor");
    }
    appliance
        .checkpoint()
        .expect("commit sparse splice predecessor")
        .expect("predecessor needs one generation");
    let mut previous = vec![0; 19];
    previous[..8].copy_from_slice(b"abcdefgh");
    previous[16..].copy_from_slice(b"XYZ");
    (inode, handle, previous)
}

fn apply_sparse_splice(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
    inode: InodeId,
    handle: HandleId,
    mut bytes: Vec<u8>,
) -> Vec<u8> {
    for (offset, length, mode) in [
        (3_u64, 5_u64, FallocateMode::ZeroRange { keep_size: true }),
        (5, 4, FallocateMode::InsertRange),
        (12, 3, FallocateMode::CollapseRange),
        (1, 2, FallocateMode::PunchHole),
    ] {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Fallocate {
                    inode,
                    handle,
                    offset,
                    length,
                    mode,
                },
            )
            .expect("apply sparse splice mutation");
        let start = usize::try_from(offset).expect("fixture offset fits usize");
        let length = usize::try_from(length).expect("fixture length fits usize");
        match mode {
            FallocateMode::ZeroRange { .. } | FallocateMode::PunchHole => {
                bytes[start..start + length].fill(0);
            }
            FallocateMode::InsertRange => {
                bytes.splice(start..start, std::iter::repeat_n(0, length));
            }
            FallocateMode::CollapseRange => {
                bytes.drain(start..start + length);
            }
            FallocateMode::Allocate { .. } => unreachable!(),
        }
    }
    bytes
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_metadata_clone_fault_recovers_the_previous_or_complete_range() {
    const SOURCE_OFFSET: u64 = 4_096;
    const TARGET_OFFSET: u64 = 64 * 1_024;
    const CLONE_LENGTH: u64 = 96 * 1_024;
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe = open(probe_metadata.clone(), probe_containers.clone());
    let (source_inode, source_handle, target_inode, target_handle, payload) =
        seed_clone_predecessor(&probe);
    clone_fixture(
        &probe,
        source_inode,
        source_handle,
        target_inode,
        target_handle,
        SOURCE_OFFSET,
        TARGET_OFFSET,
        CLONE_LENGTH,
    );
    let metadata_baseline = probe_metadata.operation_count();
    let container_baseline = probe_containers.operation_count();
    probe
        .checkpoint()
        .expect("checkpoint probe clone")
        .expect("clone needs one generation");
    let operations = probe_metadata.operations()[metadata_baseline..].to_vec();
    assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));
    assert_eq!(
        probe_containers.operation_count(),
        container_baseline,
        "clone checkpoint must not access or publish DATA containers"
    );
    let final_sync = operations.len() - 1;

    for relative in 0..operations.len() {
        for fail_after in [false, true] {
            let metadata = if fail_after {
                MemoryStorageIo::with_fail_after(metadata_baseline + relative)
            } else {
                MemoryStorageIo::with_fail_before(metadata_baseline + relative)
            };
            let containers = MemoryStorageIo::new();
            let appliance = open(metadata.clone(), containers.clone());
            let (source_inode, source_handle, target_inode, target_handle, candidate_payload) =
                seed_clone_predecessor(&appliance);
            assert_eq!(candidate_payload, payload);
            clone_fixture(
                &appliance,
                source_inode,
                source_handle,
                target_inode,
                target_handle,
                SOURCE_OFFSET,
                TARGET_OFFSET,
                CLONE_LENGTH,
            );
            assert!(
                appliance.checkpoint().is_err(),
                "clone fault relative={relative} after={fail_after} unexpectedly returned success"
            );
            drop(appliance);
            metadata.crash();
            containers.crash();
            let recovered = recover_mount(
                NamespaceConfig::default(),
                &GenerationRepository::new(metadata, policy()),
                &ContainerRepository::new(containers),
            )
            .expect("one atomic clone generation remains recoverable")
            .expect("clone predecessor exists");
            let Reply::Opened(handle) = recovered
                .dispatch(
                    CALLER,
                    Operation::Open {
                        inode: target_inode,
                        options: OpenOptions::READ_ONLY,
                        truncate: false,
                    },
                )
                .expect("open recovered clone target")
            else {
                panic!("ASSERT: recovered target open reply");
            };
            let observed = read_range(
                &recovered,
                target_inode,
                handle,
                TARGET_OFFSET,
                u32::try_from(CLONE_LENGTH).expect("clone length fits u32"),
            );
            let expected = if fail_after && relative == final_sync {
                payload[usize::try_from(SOURCE_OFFSET).expect("source offset fits")
                    ..usize::try_from(SOURCE_OFFSET + CLONE_LENGTH).expect("source end fits")]
                    .to_vec()
            } else {
                vec![0; usize::try_from(CLONE_LENGTH).expect("clone length fits usize")]
            };
            assert_eq!(
                observed, expected,
                "fault relative={relative} after={fail_after} exposed a mixed clone"
            );
        }
    }
}

#[derive(Clone, Copy)]
enum RecoveryOracle {
    Previous,
    Complete,
}

fn open(
    metadata: MemoryStorageIo,
    containers: MemoryStorageIo,
) -> DurableNamespace<MemoryStorageIo, MemoryStorageIo> {
    DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, policy()),
        ContainerRepository::new(containers),
        16,
    )
    .expect("initial reservation generation is outside injected range")
}

fn write_fixture(appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>) {
    write_named(appliance, NAME, PAYLOAD);
}

fn seed_truncate_predecessor(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
    logical_size: u64,
) -> (fastdup_posix::InodeId, fastdup_posix::HandleId) {
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"truncate-fault",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create truncate predecessor")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: PAYLOAD,
            },
        )
        .expect("write truncate predecessor prefix");
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: logical_size,
            },
        )
        .expect("extend truncate predecessor with a sparse suffix");
    appliance
        .checkpoint()
        .expect("checkpoint truncate predecessor")
        .expect("truncate predecessor needs one generation");
    (entry.attr.inode, handle)
}

fn seed_clone_predecessor(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
) -> (
    fastdup_posix::InodeId,
    fastdup_posix::HandleId,
    fastdup_posix::InodeId,
    fastdup_posix::HandleId,
    Vec<u8>,
) {
    let payload = (0..3 * 256 * 1_024_usize)
        .map(|index| u8::try_from((index * 131 + index / 97) % 251).expect("fixture byte"))
        .collect::<Vec<_>>();
    let Reply::Created {
        entry: source,
        handle: source_handle,
    } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"clone-source",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create clone source")
    else {
        panic!("ASSERT: source create reply");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: source.attr.inode,
                handle: source_handle,
                offset: 0,
                data: &payload,
            },
        )
        .expect("write clone source");
    let Reply::Created {
        entry: target,
        handle: target_handle,
    } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"clone-target",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create clone target")
    else {
        panic!("ASSERT: target create reply");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: target.attr.inode,
                handle: Some(target_handle),
                length: u64::try_from(payload.len()).expect("fixture length fits u64"),
            },
        )
        .expect("pre-size clone target");
    appliance
        .checkpoint()
        .expect("checkpoint clone predecessor")
        .expect("clone predecessor needs one generation");
    (
        source.attr.inode,
        source_handle,
        target.attr.inode,
        target_handle,
        payload,
    )
}

#[allow(clippy::too_many_arguments)]
fn clone_fixture(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
    source_inode: fastdup_posix::InodeId,
    source_handle: fastdup_posix::HandleId,
    target_inode: fastdup_posix::InodeId,
    target_handle: fastdup_posix::HandleId,
    source_offset: u64,
    target_offset: u64,
    length: u64,
) {
    assert!(matches!(
        appliance.namespace().dispatch(
            CALLER,
            Operation::CloneRange {
                source_inode,
                source_handle,
                source_offset,
                target_inode,
                target_handle,
                target_offset,
                length,
            },
        ),
        Ok(Reply::Cloned { bytes, .. }) if bytes == length
    ));
}

fn read_range(
    namespace: &Namespace,
    inode: fastdup_posix::InodeId,
    handle: fastdup_posix::HandleId,
    offset: u64,
    length: u32,
) -> Vec<u8> {
    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset,
                length,
            },
        )
        .expect("read recovered clone range")
    else {
        panic!("ASSERT: clone range read reply");
    };
    bytes
}

fn write_named(
    appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
    name: &[u8],
    payload: &[u8],
) {
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name,
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: payload,
            },
        )
        .expect("write fixture payload");
}

fn crash_and_assert(
    metadata: &MemoryStorageIo,
    containers: &MemoryStorageIo,
    oracle: RecoveryOracle,
) {
    metadata.crash();
    containers.crash();
    let generations = GenerationRepository::new(metadata.clone(), policy());
    let container_repository = ContainerRepository::new(containers.clone());
    let namespace = recover_mount(
        NamespaceConfig::default(),
        &generations,
        &container_repository,
    )
    .expect("one whole generation remains recoverable")
    .expect("initial reservation generation exists");
    match oracle {
        RecoveryOracle::Previous => assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Lookup {
                    parent: ROOT_INODE,
                    name: NAME,
                },
            ),
            Err(PosixError::NoEntry)
        ),
        RecoveryOracle::Complete => assert_complete(&namespace),
    }
    let mut fast = DurableNamespace::open_with_committed_recovery(
        NamespaceConfig::default(),
        generations,
        container_repository,
        &fastdup_store::ExactIndexRunRepository::new(metadata.clone()),
        &fastdup_store::SimilarityIndexRepository::new(metadata.clone()),
        1024,
    )
    .expect("committed writable recovery preserves the crash oracle");
    match oracle {
        RecoveryOracle::Previous => assert_eq!(
            fast.namespace().dispatch(
                CALLER,
                Operation::Lookup {
                    parent: ROOT_INODE,
                    name: NAME
                }
            ),
            Err(PosixError::NoEntry)
        ),
        RecoveryOracle::Complete => assert_complete(fast.namespace()),
    }
    let mut pending = fast.take_startup_data_verification().unwrap();
    let repository = ContainerRepository::new(containers.clone());
    for id in repository.recovery_container_snapshot().unwrap() {
        repository
            .scrub_container_for_recovery::<MemoryStorageIo>(id, None, &mut pending)
            .unwrap();
    }
    pending.finish().unwrap();
}

fn assert_complete(namespace: &Namespace) {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: NAME,
            },
        )
        .expect("complete generation contains the file")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: 64,
            },
        ),
        Ok(Reply::Data(PAYLOAD.to_vec()))
    );
}

#[test]
fn every_frozen_replace_fault_excludes_later_truncate_and_unlink() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_data = MemoryStorageIo::new();
    let probe = frozen_replace_fixture(probe_metadata.clone(), probe_data.clone());
    let baselines = [
        probe_metadata.operation_count(),
        probe_data.operation_count(),
    ];
    probe.checkpoint().unwrap().unwrap();
    let operations = [
        probe_metadata.operations()[baselines[0]..].to_vec(),
        probe_data.operations()[baselines[1]..].to_vec(),
    ];
    let commit_sync = operations[0]
        .iter()
        .rposition(|operation| *operation == StorageOperation::SyncFile)
        .expect("the WAL Commit is synchronized");
    assert!(operations[1].contains(&StorageOperation::SyncRoot));
    drop(probe);
    for tier in 0..2 {
        for relative in 0..operations[tier].len() {
            for after in [false, true] {
                let failing = if after {
                    MemoryStorageIo::with_fail_after(baselines[tier] + relative)
                } else {
                    MemoryStorageIo::with_fail_before(baselines[tier] + relative)
                };
                let (metadata, data) = if tier == 0 {
                    (failing, MemoryStorageIo::new())
                } else {
                    (MemoryStorageIo::new(), failing)
                };
                let appliance = frozen_replace_fixture(metadata.clone(), data.clone());
                let result = appliance.checkpoint();
                if tier == 1 || relative <= commit_sync {
                    assert!(
                        result.is_err(),
                        "tier={tier} relative={relative} after={after} must hit the fault"
                    );
                }
                assert_eq!(
                    named_image(appliance.namespace(), b"future").unwrap().2,
                    b"ACTIVE!"
                );
                drop(appliance);
                metadata.crash();
                data.crash();
                let recovered = recover_mount(
                    NamespaceConfig::default(),
                    &GenerationRepository::new(metadata, policy()),
                    &ContainerRepository::new(data),
                )
                .unwrap()
                .unwrap();
                let committed =
                    tier == 0 && (relative > commit_sync || (after && relative == commit_sync));
                assert_replace_image(&recovered, committed);
            }
        }
    }
    eprintln!(
        "frozen_replace_fault_cases={} metadata_operations={} data_operations={}",
        2 * (operations[0].len() + operations[1].len()),
        operations[0].len(),
        operations[1].len()
    );
}

#[allow(clippy::too_many_lines)]
fn frozen_replace_fixture(
    metadata: MemoryStorageIo,
    data: MemoryStorageIo,
) -> DurableNamespace<MemoryStorageIo, MemoryStorageIo> {
    let appliance = open(metadata, data);
    write_named(&appliance, b"source", b"original-source");
    write_named(&appliance, b"target", b"original-target");
    let namespace = appliance.namespace();
    let (inode, _, _) = named_image(namespace, b"source").unwrap();
    let (old_target, _, _) = named_image(namespace, b"target").unwrap();
    let open_writer = |inode| {
        let Reply::Opened(handle) = namespace
            .dispatch(
                CALLER,
                Operation::Open {
                    inode,
                    options: OpenOptions::READ_WRITE,
                    truncate: false,
                },
            )
            .unwrap()
        else {
            panic!("open writer reply")
        };
        handle
    };
    let handle = open_writer(inode);
    let orphan_handle = open_writer(old_target);
    namespace
        .dispatch(
            CALLER,
            Operation::Link {
                inode,
                new_parent: ROOT_INODE,
                new_name: b"alias",
            },
        )
        .unwrap();
    appliance.checkpoint().unwrap().unwrap();
    for operation in [
        Operation::Write {
            inode,
            handle,
            offset: 1,
            data: b"unaligned",
        },
        Operation::SetLength {
            inode,
            handle: Some(handle),
            length: 3,
        },
        Operation::SetLength {
            inode,
            handle: Some(handle),
            length: 8_193,
        },
        Operation::Write {
            inode,
            handle,
            offset: 16_391,
            data: b"TAIL",
        },
        Operation::Rename {
            parent: ROOT_INODE,
            name: b"source",
            new_parent: ROOT_INODE,
            new_name: b"target",
            no_replace: false,
        },
        Operation::Write {
            inode: old_target,
            handle: orphan_handle,
            offset: 0,
            data: b"ORPHAN!",
        },
    ] {
        namespace.dispatch(CALLER, operation).unwrap();
    }
    assert_eq!(
        read_range(namespace, old_target, orphan_handle, 0, 64),
        b"ORPHAN!l-target"
    );
    namespace.begin_commit().unwrap().unwrap();
    for operation in [
        Operation::SetLength {
            inode,
            handle: Some(handle),
            length: 7,
        },
        Operation::Write {
            inode,
            handle,
            offset: 0,
            data: b"ACTIVE!",
        },
        Operation::Rename {
            parent: ROOT_INODE,
            name: b"target",
            new_parent: ROOT_INODE,
            new_name: b"future",
            no_replace: false,
        },
        Operation::Unlink {
            parent: ROOT_INODE,
            name: b"alias",
        },
    ] {
        namespace.dispatch(CALLER, operation).unwrap();
    }
    appliance
}

fn named_image(namespace: &Namespace, name: &[u8]) -> Option<(InodeId, u32, Vec<u8>)> {
    let entry = match namespace.dispatch(
        CALLER,
        Operation::Lookup {
            parent: ROOT_INODE,
            name,
        },
    ) {
        Ok(Reply::Entry(entry)) => entry,
        Err(PosixError::NoEntry) => return None,
        other => panic!("lookup image: {other:?}"),
    };
    let inode = entry.attr.inode;
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("open image reply")
    };
    let bytes = read_range(namespace, inode, handle, 0, 32_768);
    assert_eq!(bytes.len() as u64, entry.attr.size);
    namespace
        .dispatch(CALLER, Operation::Release { inode, handle })
        .unwrap();
    Some((inode, entry.attr.link_count, bytes))
}

fn assert_replace_image(namespace: &Namespace, committed: bool) {
    assert!(named_image(namespace, b"future").is_none());
    let alias = named_image(namespace, b"alias").unwrap();
    let target = named_image(namespace, b"target").unwrap();
    assert_eq!(alias.1, 2);
    if committed {
        assert!(named_image(namespace, b"source").is_none());
        assert_eq!(target, alias);
        let mut expected = vec![0; 16_395];
        expected[..3].copy_from_slice(b"oun");
        expected[16_391..].copy_from_slice(b"TAIL");
        assert_eq!(
            target.2, expected,
            "truncate/extend must not resurrect old bytes"
        );
    } else {
        assert_eq!(named_image(namespace, b"source").unwrap(), alias);
        assert_eq!(alias.2, b"original-source");
        assert_ne!(alias.0, target.0);
        assert_eq!(target.1, 1);
        assert_eq!(target.2, b"original-target");
    }
}

fn policy() -> PolicySetId {
    PolicySetId::new([0x6D; 32]).expect("fixture Policy Set ID is nonzero")
}

#[test]
fn every_mixed_growth_checkpoint_fault_recovers_one_complete_layout() {
    fn change(
        appliance: &DurableNamespace<MemoryStorageIo, MemoryStorageIo>,
        inode: InodeId,
        handle: HandleId,
        tail_offset: u64,
    ) {
        for (offset, bytes) in [(0, b"HEAD"), (tail_offset, b"TAIL")] {
            appliance
                .namespace()
                .dispatch(
                    CALLER,
                    Operation::Write {
                        inode,
                        handle,
                        offset,
                        data: bytes,
                    },
                )
                .unwrap();
        }
    }
    for tail_offset in [4094, 4096, 8192] {
        let probe_metadata = MemoryStorageIo::new();
        let probe_data = MemoryStorageIo::new();
        let probe = open(probe_metadata.clone(), probe_data);
        let (inode, handle) = seed_truncate_predecessor(&probe, 4096);
        change(&probe, inode, handle, tail_offset);
        let baseline = probe_metadata.operation_count();
        probe.checkpoint().unwrap().unwrap();
        let operations = probe_metadata.operations()[baseline..].to_vec();
        assert_eq!(operations.last(), Some(&StorageOperation::SyncFile));
        for relative in 0..operations.len() {
            for after in [false, true] {
                let metadata = if after {
                    MemoryStorageIo::with_fail_after(baseline + relative)
                } else {
                    MemoryStorageIo::with_fail_before(baseline + relative)
                };
                let data = MemoryStorageIo::new();
                let appliance = open(metadata.clone(), data.clone());
                let (inode, handle) = seed_truncate_predecessor(&appliance, 4096);
                change(&appliance, inode, handle, tail_offset);
                assert!(
                    appliance.checkpoint().is_err(),
                    "tail_offset={tail_offset} fault={relative} after={after}"
                );
                drop(appliance);
                metadata.crash();
                data.crash();
                let recovered = recover_mount(
                    NamespaceConfig::default(),
                    &GenerationRepository::new(metadata, policy()),
                    &ContainerRepository::new(data),
                )
                .unwrap()
                .unwrap();
                let Reply::Opened(handle) = recovered
                    .dispatch(
                        CALLER,
                        Operation::Open {
                            inode,
                            options: OpenOptions::READ_ONLY,
                            truncate: false,
                        },
                    )
                    .unwrap()
                else {
                    panic!("reopen")
                };
                let complete = after && relative + 1 == operations.len();
                let mut expected = vec![0; 4096];
                expected[..PAYLOAD.len()].copy_from_slice(PAYLOAD);
                if complete {
                    expected[..4].copy_from_slice(b"HEAD");
                    expected.resize(tail_offset as usize + 4, 0);
                    expected[tail_offset as usize..].copy_from_slice(b"TAIL");
                }
                assert_eq!(
                    read_range(&recovered, inode, handle, 0, tail_offset as u32 + 4),
                    expected,
                    "tail_offset={tail_offset} fault={relative} after={after}"
                );
            }
        }
    }
}
