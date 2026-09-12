use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{DurableNamespace, recover_mount, recover_mount_with_index};
use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ExactIndexEntry, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet, ManifestExtent, ManifestLeaf, NamespaceEntry,
    NamespaceRoot, PolicySetId,
};
use fastdup_posix::{
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository, StorageIo,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 42,
};

fn unique_test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

#[test]
#[allow(clippy::too_many_lines)]
fn recovered_mixed_manifest_mounts_byte_exactly_and_keeps_create_closed() {
    let root = unique_test_root("appliance-recover-mount");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x71; 32]).expect("policy identity is nonzero");
    let payload = b"DATA\0\xff";
    let raw_name = b"vm-\xff";

    let containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("create workspace-local container repository"),
    );
    let container_generation = containers
        .open_generation_allocator(1_024)
        .expect("initialize current Container generation state")
        .reserve_generation()
        .expect("reserve the first Container generation");
    containers
        .publish_raw(
            ContainerId::new([0x91; 16]).expect("container identity is nonzero"),
            container_generation,
            &[payload.as_slice()],
        )
        .expect("publish durable DATA dependency");

    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("create workspace-local metadata repository"),
        policy,
    );
    generations
        .commit_namespace(
            &NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
                .expect("initial durable inode reservation is valid"),
        )
        .expect("publish initial inode reservation");

    let payload_length = u64::try_from(payload.len()).expect("test payload length fits u64");
    let logical_size = payload_length + 3 + 4;
    let manifest = ManifestLeaf::new(
        logical_size,
        vec![
            ManifestExtent::Data {
                logical_length: payload_length,
                chunk_id: ChunkId::of(payload),
            },
            ManifestExtent::Fill {
                logical_length: 3,
                value: b'Z',
            },
            ManifestExtent::Hole { logical_length: 4 },
        ],
    )
    .expect("mixed DATA/FILL/HOLE manifest is valid");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish immutable manifest");
    let namespace_root = NamespaceRoot::new(
        4_096,
        3,
        1,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_001, 1, 7, logical_size, manifest_root)
                .expect("durable regular inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, raw_name.to_vec())
                .expect("byte-exact non-UTF-8 name is valid"),
        ],
    )
    .expect("durable namespace graph is valid");
    generations
        .commit_namespace_with_data(&namespace_root, &containers)
        .expect("publish DATA-bearing namespace generation");
    drop(generations);
    drop(containers);

    let reopened_generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
        policy,
    );
    let reopened_containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("reopen container repository"),
    );
    let namespace = recover_mount(
        NamespaceConfig::default(),
        &reopened_generations,
        &reopened_containers,
    )
    .expect("recover verified durable namespace")
    .expect("committed generation exists");

    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: raw_name,
            },
        )
        .expect("lookup byte-exact committed name")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    assert_eq!(entry.attr.size, logical_size);
    assert_eq!(entry.attr.allocated_bytes, payload_length + 3);
    assert_eq!(entry.attr.mode, 0o640);
    assert_eq!(entry.attr.mutation_sequence, 7);

    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
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
                inode,
                handle,
                offset: 0,
                length: u32::try_from(logical_size).expect("test file length fits u32"),
            },
        ),
        Ok(Reply::Data(b"DATA\0\xffZZZ\0\0\0\0".to_vec()))
    );
    assert_eq!(
        namespace.dispatch(CALLER, Operation::GetAttr { inode }),
        Ok(Reply::Attr(entry.attr))
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"must-stay-closed",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        ),
        Err(PosixError::ReadOnly)
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn recovered_posix_reads_pin_the_active_exact_index() {
    let metadata_storage = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let index_storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xA1; 32]).expect("policy identity is nonzero");
    let payload = b"bounded POSIX read through the pinned exact index";

    let containers = ContainerRepository::new(container_storage.clone());
    let container_id = ContainerId::new([0xA2; 16]).expect("container identity is nonzero");
    let container_generation = containers
        .open_generation_allocator(1_024)
        .expect("initialize current Container generation state")
        .reserve_generation()
        .expect("reserve the first Container generation");
    containers
        .publish_raw(container_id, container_generation, &[payload.as_slice()])
        .expect("publish the durable DATA dependency");
    let container = containers
        .read(container_id)
        .expect("obtain complete rebuild evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct the exact location from verified evidence");

    let profile = ExactIndexProfileId::new([0xA3; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new(index_storage.clone());
    let descriptor = indexes
        .publish(
            &ExactIndexRun::new(profile, 1, vec![entry])
                .expect("construct one immutable sorted Run"),
        )
        .expect("publish the immutable Run");
    indexes
        .activate(
            &ExactIndexRunSet::new(
                profile,
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
            )
            .expect("construct the active Run Set"),
        )
        .expect("activate the complete Run Set");

    let generations = GenerationRepository::new(metadata_storage, policy);
    generations
        .commit_namespace(
            &NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
                .expect("initial durable inode reservation is valid"),
        )
        .expect("publish the initial inode reservation");
    let payload_length = u64::try_from(payload.len()).expect("test payload length fits u64");
    let manifest = ManifestLeaf::new(
        payload_length,
        vec![ManifestExtent::Data {
            logical_length: payload_length,
            chunk_id: ChunkId::of(payload),
        }],
    )
    .expect("construct the DATA Manifest");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish the immutable Manifest");
    let root = NamespaceRoot::new(
        4_096,
        3,
        1,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_001, 1, 1, payload_length, manifest_root)
                .expect("durable regular inode is valid"),
        ],
        vec![NamespaceEntry::new(1, 2, b"indexed".to_vec()).expect("directory entry is valid")],
    )
    .expect("durable Namespace Root is valid");
    generations
        .commit_namespace_with_data(&root, &containers)
        .expect("commit the DATA-bearing generation");

    let recovery_baseline = container_storage.operation_count();
    let namespace = recover_mount_with_index(
        NamespaceConfig::default(),
        &generations,
        &containers,
        &indexes,
    )
    .expect("recover the verified generation and optional acceleration state")
    .expect("one committed generation exists");
    let recovery_operations = &container_storage.operations()[recovery_baseline..];
    assert!(
        !recovery_operations.contains(&StorageOperation::ListNames),
        "healthy indexed recovery must not scan the Container directory"
    );
    assert!(
        !recovery_operations.contains(&StorageOperation::Read),
        "healthy indexed recovery must use bounded Location verification"
    );
    // The recovered file must follow ordinary index activation before its first
    // cold read, instead of retaining the generation installed at mount time.
    indexes
        .activate(
            &ExactIndexRunSet::new(
                profile,
                2,
                vec![ExactIndexRunRef::new(0, descriptor).unwrap()],
            )
            .unwrap(),
        )
        .expect("activate a successor while the recovered file remains open");
    let baseline = container_storage.operation_count();

    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"indexed",
            },
        )
        .expect("lookup the recovered inode")
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
        .expect("open the recovered file")
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
                length: u32::try_from(payload.len()).expect("test payload length fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec()))
    );
    let operations = &container_storage.operations()[baseline..];
    assert!(!operations.contains(&StorageOperation::Read));
    assert!(!operations.contains(&StorageOperation::ListNames));

    let writable_recovery_baseline = container_storage.operation_count();
    let writable = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        &indexes,
        16,
    )
    .expect("recover a writable Namespace with the same pinned Run Set");
    assert_eq!(writable.exact_index_run_count(), 1);
    let writable_recovery_operations =
        &container_storage.operations()[writable_recovery_baseline..];
    assert_eq!(
        writable_recovery_operations
            .iter()
            .filter(|operation| **operation == StorageOperation::ListNames)
            .count(),
        0,
        "healthy fixed high-water recovery must not scan the DATA directory: {writable_recovery_operations:?}"
    );
    assert_eq!(
        writable_recovery_operations
            .iter()
            .filter(|operation| **operation == StorageOperation::Read)
            .count(),
        0,
        "fixed high-water recovery must not read whole Container payloads: {writable_recovery_operations:?}"
    );
    let object_lengths = writable_recovery_operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ObjectLen)
        .count();
    let bounded_reads = writable_recovery_operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count();
    assert_eq!(
        object_lengths, 1,
        "independent recovery rechecks the Container envelope"
    );
    assert_eq!(
        bounded_reads, 3,
        "writable recovery verifies the DATA record once and reuses that proof for inode reservation: {writable_recovery_operations:?}"
    );
    let writable_baseline = container_storage.operation_count();
    let Reply::Entry(writable_entry) = writable
        .namespace()
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"indexed",
            },
        )
        .expect("lookup the writable recovered inode")
    else {
        panic!("ASSERT: writable lookup returned the wrong reply variant");
    };
    let Reply::Opened(writable_handle) = writable
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode: writable_entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the writable recovered file")
    else {
        panic!("ASSERT: writable open returned the wrong reply variant");
    };
    assert_eq!(
        writable.namespace().dispatch(
            CALLER,
            Operation::Read {
                inode: writable_entry.attr.inode,
                handle: writable_handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("test payload length fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec()))
    );
    let writable_operations = &container_storage.operations()[writable_baseline..];
    assert!(!writable_operations.contains(&StorageOperation::Read));
    assert!(!writable_operations.contains(&StorageOperation::ListNames));

    let Reply::Opened(write_handle) = writable
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode: writable_entry.attr.inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("open the indexed file for a same-content mutation")
    else {
        panic!("ASSERT: writable open returned the wrong reply variant");
    };
    writable
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: writable_entry.attr.inode,
                handle: write_handle,
                offset: 0,
                data: &payload[..1],
            },
        )
        .expect("accept a same-content mutation into a new commit epoch");
    let checkpoint_baseline = container_storage.operation_count();
    assert!(
        writable
            .checkpoint()
            .expect("commit and install a later Manifest reader")
            .is_some()
    );
    let checkpoint_operations = &container_storage.operations()[checkpoint_baseline..];
    assert!(
        !checkpoint_operations.contains(&StorageOperation::ListNames),
        "healthy indexed checkpoint graph verification must not scan the Container directory"
    );
    assert!(
        !checkpoint_operations.contains(&StorageOperation::Read),
        "healthy indexed checkpoint graph verification must use bounded Location reads"
    );
    let installed_baseline = container_storage.operation_count();
    assert_eq!(
        writable.namespace().dispatch(
            CALLER,
            Operation::Read {
                inode: writable_entry.attr.inode,
                handle: writable_handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("test payload length fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec()))
    );
    let installed_operations = &container_storage.operations()[installed_baseline..];
    assert!(!installed_operations.contains(&StorageOperation::Read));
    assert!(!installed_operations.contains(&StorageOperation::ListNames));
    drop(writable);

    index_storage
        .write_at("exact-index.activation.wal", 0, &[0])
        .expect("inject one activation-record integrity fault");
    assert!(
        indexes.recover_active().is_err(),
        "VERIFY: the damaged activation must not be accepted"
    );
    let fallback_namespace = recover_mount_with_index(
        NamespaceConfig::default(),
        &generations,
        &containers,
        &indexes,
    )
    .expect("index corruption must not make verified Namespace DATA unavailable")
    .expect("one committed generation still exists");
    let fallback_baseline = container_storage.operation_count();
    let Reply::Entry(fallback_entry) = fallback_namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"indexed",
            },
        )
        .expect("lookup the scan-backed recovered inode")
    else {
        panic!("ASSERT: fallback lookup returned the wrong reply variant");
    };
    let Reply::Opened(fallback_handle) = fallback_namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: fallback_entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the scan-backed recovered file")
    else {
        panic!("ASSERT: fallback open returned the wrong reply variant");
    };
    assert_eq!(
        fallback_namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: fallback_entry.attr.inode,
                handle: fallback_handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("test payload length fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec()))
    );
    let fallback_operations = &container_storage.operations()[fallback_baseline..];
    assert!(fallback_operations.contains(&StorageOperation::ReadExactAt));
    assert!(fallback_operations.contains(&StorageOperation::ListNames));
}

#[test]
fn current_index_reader_keeps_inflight_generation_pinned_until_data_read_finishes() {
    use fastdup_store::VerifiedManifestFile;
    use fastdup_testkit::PausedStorageIo;
    use std::time::Duration;

    let data = PausedStorageIo::disarmed_before_name_prefix(
        MemoryStorageIo::new(),
        StorageOperation::ReadExactAt,
        "a4",
    );
    let containers = ContainerRepository::new(data.clone());
    let id = ContainerId::new([0xa4; 16]).unwrap();
    let payload = b"a bounded cold read must protect its selected physical location";
    containers.publish_raw(id, 1, &[payload]).unwrap();
    let verified = containers.read(id).unwrap();
    let entry = ExactIndexEntry::from_verified_raw(verified.raw_locations()[0]).unwrap();
    let indexes = ExactIndexRunRepository::new(MemoryStorageIo::new());
    let profile = ExactIndexProfileId::new([0xa5; 32]).unwrap();
    indexes.append_level_zero(profile, vec![entry]).unwrap();
    let manifest = ManifestLeaf::new(
        payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: payload.len() as u64,
            chunk_id: ChunkId::of(payload),
        }],
    )
    .unwrap();
    let file = VerifiedManifestFile::new(manifest, containers)
        .unwrap()
        .with_index_repository(&indexes);
    data.arm();
    let reader = std::thread::spawn(move || file.read_at(0, 4096));
    let reached = data.wait_until_reached(Duration::from_secs(10));
    if !reached {
        data.resume();
        reader.join().unwrap().unwrap();
        panic!("bounded DATA read did not reach the storage pause");
    }
    let drain = indexes
        .append_level_zero(profile, vec![entry])
        .unwrap()
        .into_retired()
        .unwrap();
    let protected = !drain.is_drained();
    data.resume();
    assert_eq!(reader.join().unwrap().unwrap(), payload);
    assert!(
        protected,
        "retirement must wait for the in-flight DATA read"
    );
    assert!(
        drain.is_drained(),
        "completed read must release its operation pin"
    );
}

#[test]
fn writable_restart_proves_data_once_before_reserving_new_inode_ids() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xe7; 32]).unwrap();
    let payload = b"a fresh recovery proof survives its inode-only successor";
    {
        let containers = ContainerRepository::new(data.clone());
        let generation = containers
            .open_generation_allocator(1024)
            .unwrap()
            .reserve_generation()
            .unwrap();
        containers
            .publish_raw(
                ContainerId::new([0xe8; 16]).unwrap(),
                generation,
                &[payload.as_slice()],
            )
            .unwrap();
        let generations = GenerationRepository::new(metadata.clone(), policy);
        generations
            .commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
            .unwrap();
        let length = u64::try_from(payload.len()).unwrap();
        let manifest = generations
            .publish_manifest(
                &ManifestLeaf::new(
                    length,
                    vec![ManifestExtent::Data {
                        logical_length: length,
                        chunk_id: ChunkId::of(payload),
                    }],
                )
                .unwrap(),
            )
            .unwrap();
        let root = NamespaceRoot::new(
            4096,
            3,
            1,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, 1, length, manifest).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"recovered".to_vec()).unwrap()],
        )
        .unwrap();
        generations
            .commit_namespace_with_data(&root, &containers)
            .unwrap();
    }
    metadata.crash();
    data.crash();
    let baseline = data.operation_count();
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, policy),
        ContainerRepository::new(data.clone()),
        16,
    )
    .unwrap();
    let reads = data.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    assert_eq!(
        reads,
        0,
        "recovery uses bounded DATA reads: {:?}",
        &data.operations()[baseline..]
    );
    let bounded_reads = data.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count();
    assert_eq!(
        bounded_reads,
        5,
        "generation discovery, envelope and one DATA record proof: {:?}",
        &data.operations()[baseline..]
    );
    let Reply::Created { entry, .. } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"after-restart",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("create returns an entry");
    };
    assert!(
        entry.attr.inode.get() >= 4096,
        "restart skips the previous inode reservation"
    );
}
