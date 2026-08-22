use fastdup_appliance::{DurableNamespace, recover_mount};
use fastdup_format::PolicySetId;
use fastdup_posix::{
    Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply,
    RequestContext,
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
    assert!(
        appliance.historical_proof_cache_status().entry_count() > 0,
        "only the successful retry may demote Frozen proofs into Historical S3-FIFO"
    );
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
    assert!(appliance.historical_proof_cache_status().entry_count() > 0);
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
    const PREVIOUS_SIZE: u64 = 1_048_576;
    const TRUNCATED_SIZE: u64 = 128;
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
                length: TRUNCATED_SIZE,
            },
        )
        .expect("truncate probe predecessor");
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
                        length: TRUNCATED_SIZE,
                    },
                )
                .expect("truncate injected predecessor");
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
                TRUNCATED_SIZE
            } else {
                PREVIOUS_SIZE
            };
            assert_eq!(
                attr.size, expected,
                "fault relative={relative} after={fail_after} exposed a mixed truncate"
            );
        }
    }
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

fn policy() -> PolicySetId {
    PolicySetId::new([0x6D; 32]).expect("fixture Policy Set ID is nonzero")
}
