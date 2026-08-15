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
fn namespace_only_checkpoint_does_not_rescan_containers_for_generation_allocation() {
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
        list_operations, 1,
        "one graph proof must feed reader installation without a duplicate DATA scan"
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
    appliance
        .checkpoint()
        .expect("retry the same frozen commit cut")
        .expect("retry publishes a complete generation");
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
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: NAME,
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
                data: PAYLOAD,
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
