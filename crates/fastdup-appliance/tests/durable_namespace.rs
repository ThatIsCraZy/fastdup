use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{DurableNamespace, recover_mount, recover_mount_with_index};
use fastdup_format::PolicySetId;
use fastdup_posix::{NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 52,
};
const CHUNK_BYTES: usize = 256 * 1_024;

#[test]
#[allow(clippy::too_many_lines)]
fn checkpoint_recovers_byte_exact_sparse_file_and_skips_old_inode_reservation() {
    let root = unique_test_root("durable-namespace");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x63; 32]).expect("policy identity is nonzero");

    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("create metadata repository"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("create container repository"),
        ),
        16,
    )
    .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"vm-\xff",
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create byte-exact name")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"abcdefgh",
            },
        )
        .expect("write prefix");
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 1_048_576,
                data: b"tail",
            },
        )
        .expect("write beyond EOF without materializing hole");
    assert!(
        appliance
            .checkpoint()
            .expect("durably commit first generation")
            .is_some()
    );
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 2,
                data: b"XY",
            },
        )
        .expect("post-checkpoint write is live but still inside loss window");
    assert_eq!(
        read(appliance.namespace(), inode, handle, 0, 8),
        b"abXYefgh"
    );
    drop(appliance);

    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
        policy,
    );
    let containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("reopen container repository"),
    );
    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("recover latest complete generation")
        .expect("committed namespace exists");
    let Reply::Entry(recovered_entry) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"vm-\xff",
            },
        )
        .expect("recover raw name")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    assert_eq!(recovered_entry.attr.inode, inode);
    assert_eq!(recovered_entry.attr.size, 1_048_580);
    assert_eq!(recovered_entry.attr.allocated_bytes, 12);
    let Reply::Opened(recovered_handle) = recovered
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
    assert_eq!(read(&recovered, inode, recovered_handle, 0, 8), b"abcdefgh");
    assert_eq!(
        read(&recovered, inode, recovered_handle, 1_048_572, 8),
        b"\0\0\0\0tail"
    );
    drop(recovered);
    drop(generations);
    drop(containers);

    let reopened = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("open metadata for writable recovery"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("open containers for writable recovery"),
        ),
        16,
    )
    .expect("reserve fresh inode range before writable recovery");
    let Reply::Created { entry: fresh, .. } = reopened
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"after-crash",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create from fresh durable reservation")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    assert!(
        fresh.attr.inode.get() >= 18,
        "recovery must skip every unused ID from the old durable range"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn compressible_checkpoint_publishes_zstd_regions_and_recovers_byte_exactly() {
    let root = unique_test_root("durable-zstd-checkpoint");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x6A; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("create metadata repository"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("create container repository"),
        ),
        16,
    )
    .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"compressed-region",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create the compressible file")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let payload = (0..4 * CHUNK_BYTES)
        .map(|index| b'a' + u8::try_from(index % 23).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &payload,
            },
        )
        .expect("write the complete compression fixture");
    let profiled = appliance
        .checkpoint_profiled()
        .expect("durably checkpoint the compression fixture")
        .expect("the DATA mutation needs a generation");
    let metrics = profiled.metrics();
    let payload_bytes = u64::try_from(payload.len()).expect("fixture length fits u64");
    assert_eq!(metrics.logical_chunk_bytes(), payload_bytes);
    assert_eq!(metrics.exact_hit_chunks(), 0);
    assert_eq!(metrics.new_chunk_bytes(), payload_bytes);
    assert!(metrics.logical_chunks() > 1);
    assert!(metrics.zstd_records() > 0);
    assert_eq!(metrics.raw_records(), 0);
    assert!(metrics.containers() > 0);
    assert!(metrics.container_file_bytes() < payload_bytes);
    assert!(metrics.peak_buffered_chunk_bytes() <= 32 * 1_024 * 1_024);
    assert!(metrics.peak_buffered_chunks() > 0);
    assert!(metrics.total().wall() >= metrics.manifest_plan().wall());
    assert!(metrics.manifest_plan().wall() >= metrics.fastcdc().wall());
    drop(appliance);

    let containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("reopen Container repository"),
    );
    let published = containers
        .recover_published()
        .expect("fully verify every published Container");
    assert!(!published.is_empty());
    assert_eq!(
        published
            .iter()
            .map(fastdup_format::SealedContainer::raw_record_count)
            .sum::<usize>(),
        0
    );
    assert!(
        published
            .iter()
            .map(fastdup_format::SealedContainer::zstd_record_count)
            .sum::<usize>()
            > 0
    );

    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
        policy,
    );
    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("recover the complete Zstd-backed generation")
        .expect("one committed generation exists");
    let Reply::Entry(recovered_entry) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"compressed-region",
            },
        )
        .expect("lookup the recovered file")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: recovered_entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the recovered file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(
            &recovered,
            recovered_entry.attr.inode,
            recovered_handle,
            0,
            u32::try_from(payload.len()).expect("fixture length fits u32"),
        ),
        payload
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn checkpoint_uses_bounded_content_defined_chunks() {
    let root = unique_test_root("durable-fastcdc-checkpoint");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x6B; 32]).expect("policy identity is nonzero");
    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("create metadata repository"),
        policy,
    );
    let containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("create Container repository"),
    );
    let appliance = DurableNamespace::open(NamespaceConfig::default(), generations, containers, 16)
        .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"fastcdc-stream",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create the FastCDC fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let mut state = 0xD1B5_4A32_D192_ED03_u64;
    let payload = (0..4 * CHUNK_BYTES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect::<Vec<_>>();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &payload,
            },
        )
        .expect("write the FastCDC fixture");
    appliance
        .checkpoint()
        .expect("checkpoint the FastCDC fixture")
        .expect("the DATA mutation needs a generation");
    drop(appliance);

    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
        policy,
    );
    let containers = ContainerRepository::new(
        FsStorageIo::open(&container_root).expect("reopen Container repository"),
    );
    let recovered_generation = generations
        .recover_latest_with_data(&containers)
        .expect("recover the FastCDC generation")
        .expect("one committed generation exists");
    let inode = recovered_generation
        .namespace_root()
        .inodes()
        .first()
        .expect("fixture inode is durable");
    let manifest = generations
        .read_manifest(inode.manifest_root())
        .expect("read the durable FastCDC Manifest");
    let data_lengths = manifest
        .extents()
        .iter()
        .filter_map(|extent| match extent {
            fastdup_format::ManifestExtent::Data { logical_length, .. } => Some(*logical_length),
            fastdup_format::ManifestExtent::Hole { .. }
            | fastdup_format::ManifestExtent::Fill { .. } => None,
        })
        .collect::<Vec<_>>();
    assert!(
        data_lengths.len() > 8,
        "FastCDC-v1 should produce roughly 64-KiB chunks, not four fixed 256-KiB cells"
    );
    assert!(
        data_lengths
            .iter()
            .all(|length| *length > 0 && *length <= CHUNK_BYTES as u64),
        "every durable logical Chunk must obey the FastCDC-v1 maximum"
    );
    assert_eq!(data_lengths.iter().sum::<u64>(), payload.len() as u64);

    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("mount the complete FastCDC generation")
        .expect("one committed Namespace exists");
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the FastCDC-backed file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            0,
            u32::try_from(payload.len()).expect("fixture length fits u32"),
        ),
        payload
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn later_checkpoint_reuses_zstd_chunks_through_the_persistent_exact_index() {
    let metadata_storage = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let index_storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x6C; 32]).expect("policy identity is nonzero");
    let generations = GenerationRepository::new(metadata_storage, policy);
    let containers = ContainerRepository::new(container_storage);
    let indexes = ExactIndexRunRepository::new(index_storage);
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        &indexes,
        16,
    )
    .expect("bootstrap the indexed writable Namespace");
    let payload = (0..3 * CHUNK_BYTES)
        .map(|index| b'a' + u8::try_from(index % 23).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();

    let first = create_and_write(&appliance, b"first-copy", &payload);
    appliance
        .checkpoint()
        .expect("checkpoint the first copy")
        .expect("the first copy needs a generation");
    assert_eq!(appliance.exact_index_run_count(), 1);
    assert!(!appliance.exact_index_degraded());
    let first_containers = containers
        .verify_published()
        .expect("verify the first published Container set");
    assert_eq!(first_containers.len(), 1);
    assert!(first_containers[0].zstd_record_count() > 0);

    let second = create_and_write(&appliance, b"second-copy", &payload);
    appliance
        .checkpoint()
        .expect("checkpoint the duplicate copy")
        .expect("the duplicate Namespace entry needs a generation");
    assert_eq!(
        containers
            .verify_published()
            .expect("verify the post-dedup Container set")
            .len(),
        1,
        "a verified Exact Hit must not publish another physical Container"
    );
    assert_eq!(
        appliance.exact_index_run_count(),
        1,
        "a duplicate-only checkpoint must not publish an empty L0 Run"
    );
    assert!(!appliance.exact_index_degraded());
    drop(appliance);

    let recovered = recover_mount_with_index(
        NamespaceConfig::default(),
        &generations,
        &containers,
        &indexes,
    )
    .expect("recover the Namespace and activated Exact Index")
    .expect("one committed Namespace exists");
    for inode in [first, second] {
        let Reply::Opened(handle) = recovered
            .dispatch(
                CALLER,
                Operation::Open {
                    inode,
                    options: OpenOptions::READ_ONLY,
                    truncate: false,
                },
            )
            .expect("open one recovered duplicate")
        else {
            panic!("ASSERT: open returned the wrong reply variant");
        };
        assert_eq!(
            read(
                &recovered,
                inode,
                handle,
                0,
                u32::try_from(payload.len()).expect("fixture length fits u32"),
            ),
            payload
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn exact_index_compaction_keeps_more_than_sixty_four_checkpoints_deduplicating() {
    const CHECKPOINTS: usize = 70;
    const PAYLOAD_BYTES: usize = 20 * 1_024;

    let metadata_storage = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let index_storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x6E; 32]).expect("policy identity is nonzero");
    let generations = GenerationRepository::new(metadata_storage, policy);
    let containers = ContainerRepository::new(container_storage);
    let indexes = ExactIndexRunRepository::new(index_storage);
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        &indexes,
        128,
    )
    .expect("bootstrap the indexed writable Namespace");

    let mut first_payload = Vec::new();
    let mut first_inode = None;
    for ordinal in 0..CHECKPOINTS {
        let payload = pseudorandom_payload(
            PAYLOAD_BYTES,
            u64::try_from(ordinal).expect("fixture ordinal fits u64") + 1,
        );
        let name = format!("unique-{ordinal:03}");
        let inode = create_and_write(&appliance, name.as_bytes(), &payload);
        appliance
            .checkpoint()
            .expect("checkpoint one unique payload")
            .expect("one new file needs a generation");
        if ordinal == 0 {
            first_payload = payload;
            first_inode = Some(inode);
        }
    }

    assert!(
        appliance.exact_index_run_count() < fastdup_store::MAX_ACTIVE_EXACT_INDEX_RUNS,
        "bounded compaction must keep the active set below its hard reader limit"
    );
    assert!(
        !appliance.exact_index_degraded(),
        "ordinary level-zero pressure must not disable Exact Dedup"
    );
    let containers_before_duplicate = containers
        .verify_published()
        .expect("verify every unique Container")
        .len();
    assert_eq!(containers_before_duplicate, CHECKPOINTS);

    let duplicate_inode = create_and_write(&appliance, b"duplicate-of-first", &first_payload);
    appliance
        .checkpoint()
        .expect("checkpoint the late duplicate")
        .expect("the duplicate name needs a generation");
    assert_eq!(
        containers
            .verify_published()
            .expect("verify Containers after the Exact Hit")
            .len(),
        containers_before_duplicate,
        "the compacted Exact Index must retain the first checkpoint's Location"
    );
    drop(appliance);

    let recovered = recover_mount_with_index(
        NamespaceConfig::default(),
        &generations,
        &containers,
        &indexes,
    )
    .expect("recover the compacted Exact Index and Namespace")
    .expect("one committed Namespace exists");
    for inode in [
        first_inode.expect("the first fixture inode was recorded"),
        duplicate_inode,
    ] {
        let Reply::Opened(handle) = recovered
            .dispatch(
                CALLER,
                Operation::Open {
                    inode,
                    options: OpenOptions::READ_ONLY,
                    truncate: false,
                },
            )
            .expect("open one file backed by the compacted index")
        else {
            panic!("ASSERT: open returned the wrong reply variant");
        };
        assert_eq!(
            read(
                &recovered,
                inode,
                handle,
                0,
                u32::try_from(first_payload.len()).expect("fixture length fits u32"),
            ),
            first_payload
        );
    }
}

#[test]
fn exact_index_publication_failure_degrades_without_blocking_namespace_durability() {
    let probe_metadata = MemoryStorageIo::new();
    let probe_containers = MemoryStorageIo::new();
    let probe_indexes = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x6D; 32]).expect("policy identity is nonzero");
    let probe = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(probe_metadata, policy),
        ContainerRepository::new(probe_containers),
        &ExactIndexRunRepository::new(probe_indexes.clone()),
        16,
    )
    .expect("open the healthy probe");
    let index_baseline = probe_indexes.operation_count();
    create_and_write(&probe, b"probe", b"non-uniform durable index probe");
    probe
        .checkpoint()
        .expect("healthy probe checkpoint succeeds")
        .expect("healthy probe has one mutation");
    assert!(probe_indexes.operation_count() > index_baseline);

    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::with_fail_before(index_baseline);
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        &ExactIndexRunRepository::new(indexes),
        16,
    )
    .expect("initial index recovery happens before the injected publication fault");
    let inode = create_and_write(
        &appliance,
        b"index-degraded",
        b"non-uniform durable index probe",
    );
    appliance
        .checkpoint()
        .expect("the non-authoritative index failure must not fail the Namespace checkpoint")
        .expect("the DATA mutation needs a Namespace generation");
    assert!(appliance.exact_index_degraded());
    drop(appliance);
    metadata.crash();
    containers.crash();

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(containers),
    )
    .expect("recover without any Exact Index authority")
    .expect("the complete Namespace generation remains durable");
    let Reply::Opened(handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the recovered file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(&recovered, inode, handle, 0, 31),
        b"non-uniform durable index probe"
    );
}

fn create_and_write<M, C>(
    appliance: &DurableNamespace<M, C>,
    name: &[u8],
    payload: &[u8],
) -> fastdup_posix::InodeId
where
    M: fastdup_store::StorageIo,
    C: Clone + Send + Sync + fastdup_store::StorageIo + 'static,
{
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name,
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create one duplicate fixture")
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
        .expect("write one duplicate fixture");
    entry.attr.inode
}

fn pseudorandom_payload(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)]
fn one_byte_update_publishes_only_one_new_chunk_and_recovers_byte_exact() {
    let root = unique_test_root("bounded-one-byte-checkpoint");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x71; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("create metadata repository"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("create container repository"),
        ),
        16,
    )
    .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"bounded-update",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create test file")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    let mut expected = vec![0_u8; 4 * CHUNK_BYTES];
    for (index, byte) in expected.iter_mut().enumerate() {
        let mixed = index ^ index.rotate_left(7) ^ index.rotate_left(17);
        *byte = u8::try_from(mixed & 0xff).expect("masked fixture byte fits u8");
    }
    for chunk in 0_u8..4 {
        expected[usize::from(chunk) * CHUNK_BYTES] = 0x40 + chunk;
    }
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: &expected,
            },
        )
        .expect("write one MiB fixture");
    appliance
        .checkpoint()
        .expect("commit initial file")
        .expect("initial checkpoint must publish a generation");
    assert_eq!(published_chunk_count(&container_root), 4);

    let changed_offset = CHUNK_BYTES + 37;
    expected[changed_offset] ^= 0xff;
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: u64::try_from(changed_offset).expect("fixture offset fits u64"),
                data: &expected[changed_offset..=changed_offset],
            },
        )
        .expect("change one byte in the second chunk");
    appliance
        .checkpoint()
        .expect("commit bounded update")
        .expect("changed file must publish a generation");
    assert_eq!(
        published_chunk_count(&container_root),
        5,
        "one changed 256-KiB cell must not republish three unchanged chunks"
    );
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
        .expect("create an empty file for a namespace-only generation")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .checkpoint()
        .expect("commit namespace-only update")
        .expect("new name must publish a generation");
    assert_eq!(
        published_chunk_count(&container_root),
        5,
        "namespace-only generations must not republish unchanged file data"
    );
    drop(appliance);

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
            policy,
        ),
        &ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("reopen container repository"),
        ),
    )
    .expect("recover newest complete generation")
    .expect("committed namespace exists");
    let Reply::Opened(recovered_handle) = recovered
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
        read(
            &recovered,
            inode,
            recovered_handle,
            0,
            u32::try_from(expected.len()).expect("fixture length fits u32"),
        ),
        expected
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn append_checkpoint_rechunks_only_bounded_suffix_and_recovers_byte_exact() {
    let root = unique_test_root("bounded-append-checkpoint");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x73; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("create metadata repository"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("create container repository"),
        ),
        16,
    )
    .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"growing-backup-stream",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create growing file")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    let prefix = pseudorandom_payload(4 * 1_024 * 1_024, 0x45e9_23a1_89cb_670d);
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: &prefix,
            },
        )
        .expect("write initial stream prefix");
    appliance
        .checkpoint()
        .expect("commit initial stream prefix")
        .expect("initial stream prefix is dirty");

    let appended = pseudorandom_payload(512 * 1_024, 0xb7a4_61d3_05ef_298c);
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: u64::try_from(prefix.len()).expect("prefix length fits u64"),
                data: &appended,
            },
        )
        .expect("append the next stream segment");
    let metrics = appliance
        .checkpoint_profiled()
        .expect("commit appended stream segment")
        .expect("appended segment is dirty")
        .metrics();
    let appended_bytes = u64::try_from(appended.len()).expect("append length fits u64");
    assert!(
        metrics.logical_chunk_bytes() >= appended_bytes,
        "the appended bytes must all pass through FastCDC"
    );
    assert!(
        metrics.logical_chunk_bytes() <= appended_bytes + u64::try_from(CHUNK_BYTES).unwrap(),
        "append checkpoint must process only the suffix and at most one prior maximum-size Chunk"
    );
    drop(appliance);

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
            policy,
        ),
        &ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("reopen container repository"),
        ),
    )
    .expect("recover appended stream")
    .expect("committed namespace exists");
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered growing file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    let mut expected = prefix;
    expected.extend_from_slice(&appended);
    let mut recovered_bytes = Vec::with_capacity(expected.len());
    for offset in (0..expected.len()).step_by(1_024 * 1_024) {
        let length = (expected.len() - offset).min(1_024 * 1_024);
        recovered_bytes.extend_from_slice(&read(
            &recovered,
            inode,
            recovered_handle,
            u64::try_from(offset).expect("read offset fits u64"),
            u32::try_from(length).expect("bounded read length fits u32"),
        ));
    }
    assert_eq!(recovered_bytes, expected);
}

#[test]
fn append_graph_proof_reads_are_bounded_by_changed_suffix_not_prior_file_size() {
    let small_prefix_reads = append_checkpoint_container_range_reads(2 * 1_024 * 1_024);
    let large_prefix_reads = append_checkpoint_container_range_reads(12 * 1_024 * 1_024);
    assert!(
        large_prefix_reads <= small_prefix_reads + 32,
        "verified DATA reads must remain suffix-bounded: small={small_prefix_reads}, large={large_prefix_reads}"
    );
}

fn append_checkpoint_container_range_reads(prefix_bytes: usize) -> usize {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x74; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, policy),
        ContainerRepository::new(containers.clone()),
        &ExactIndexRunRepository::new(indexes),
        16,
    )
    .expect("bootstrap indexed durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"proof-bounded-append",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create proof fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let prefix = pseudorandom_payload(prefix_bytes, 0x1fd2_a563_c840_7e9b);
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &prefix,
            },
        )
        .expect("write proof prefix");
    appliance
        .checkpoint()
        .expect("commit proof prefix")
        .expect("proof prefix is dirty");

    let baseline = containers.operation_count();
    let appended = pseudorandom_payload(512 * 1_024, 0xd0b4_7c6a_915e_382f);
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: u64::try_from(prefix_bytes).expect("prefix size fits u64"),
                data: &appended,
            },
        )
        .expect("append proof suffix");
    appliance
        .checkpoint()
        .expect("commit proof suffix")
        .expect("proof suffix is dirty");
    containers.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count()
}

#[test]
#[allow(clippy::too_many_lines)]
fn sparse_update_reuses_unchanged_data_and_preserves_holes() {
    let root = unique_test_root("bounded-sparse-checkpoint");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let policy = PolicySetId::new([0x72; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("create metadata repository"),
            policy,
        ),
        ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("create container repository"),
        ),
        16,
    )
    .expect("bootstrap writable durable namespace");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"sparse-update",
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    for (offset, bytes) in [(0_u64, b"nonzero!".as_slice()), (1_048_576, b"tail")] {
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
            .expect("write sparse extent");
    }
    appliance
        .checkpoint()
        .expect("commit sparse fixture")
        .expect("sparse fixture is dirty");
    assert_eq!(published_chunk_count(&container_root), 2);

    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 3,
                data: b"X",
            },
        )
        .expect("change one sparse prefix byte");
    appliance
        .checkpoint()
        .expect("commit sparse update")
        .expect("sparse update is dirty");
    assert_eq!(
        published_chunk_count(&container_root),
        3,
        "the distant tail DATA extent must be reused"
    );
    drop(appliance);

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(
            FsStorageIo::open(&metadata_root).expect("reopen metadata repository"),
            policy,
        ),
        &ContainerRepository::new(
            FsStorageIo::open(&container_root).expect("reopen container repository"),
        ),
    )
    .expect("recover sparse update")
    .expect("committed namespace exists");
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered sparse file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(read(&recovered, inode, recovered_handle, 0, 8), b"nonXero!");
    assert_eq!(
        read(&recovered, inode, recovered_handle, 1_048_568, 12),
        b"\0\0\0\0\0\0\0\0tail"
    );
}

fn published_chunk_count(container_root: &Path) -> usize {
    ContainerRepository::new(
        FsStorageIo::open(container_root).expect("open container repository for verification"),
    )
    .verify_published()
    .expect("verify every published container")
    .into_iter()
    .try_fold(0_usize, |total, summary| {
        total.checked_add(summary.chunk_count())
    })
    .expect("fixture chunk count cannot overflow")
}

fn read(
    namespace: &fastdup_posix::Namespace,
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
        .expect("read fixture range")
    else {
        panic!("ASSERT: read returned the wrong reply variant");
    };
    bytes
}

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
