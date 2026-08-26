use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{
    DurableNamespace, checkpoint_policy_set_v1, recover_mount, recover_mount_with_index,
};
use fastdup_format::PolicySetId;
use fastdup_posix::{
    FS_IMMUTABLE_FL, FallocateMode, InodeAttributesUpdate, NamespaceConfig, OpenOptions, Operation,
    PosixTimestamp, ROOT_INODE, Reply, RequestContext, SeekKind, XattrSetMode,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 52,
};
const ROOT_CALLER: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};
const CHUNK_BYTES: usize = 256 * 1_024;

#[test]
#[allow(clippy::too_many_lines)]
fn hardlinks_symlinks_ownership_and_times_survive_checkpoint_recovery() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x5A; 32]).unwrap();
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .unwrap();
    let Reply::Created { entry, .. } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"data",
                mode: 0o660,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("create reply")
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Link {
                inode: entry.attr.inode,
                new_parent: ROOT_INODE,
                new_name: b"data-alias",
            },
        )
        .unwrap();
    let Reply::Entry(symlink) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Symlink {
                parent: ROOT_INODE,
                name: b"latest",
                target: b"data-alias",
            },
        )
        .unwrap()
    else {
        panic!("symlink reply")
    };
    let mtime = PosixTimestamp::new(1_234, 567);
    appliance
        .namespace()
        .dispatch(
            ROOT_CALLER,
            Operation::SetAttributes {
                inode: entry.attr.inode,
                update: InodeAttributesUpdate {
                    uid: Some(2_000),
                    gid: Some(3_000),
                    mtime: Some(mtime),
                    ..Default::default()
                },
            },
        )
        .unwrap();
    appliance.checkpoint().unwrap().expect("checkpoint");
    metadata.crash();
    containers.crash();
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(containers),
    )
    .unwrap()
    .expect("recovered namespace");
    let Reply::Entry(alias) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"data-alias",
            },
        )
        .unwrap()
    else {
        panic!("lookup reply")
    };
    assert_eq!(alias.attr.link_count, 2);
    assert_eq!((alias.attr.uid, alias.attr.gid), (2_000, 3_000));
    assert_eq!(alias.attr.times.mtime, mtime);
    assert_eq!(
        recovered.dispatch(
            CALLER,
            Operation::Readlink {
                inode: symlink.attr.inode
            }
        ),
        Ok(Reply::LinkTarget(b"data-alias".to_vec()))
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn xattrs_posix_acl_and_immutable_flag_survive_checkpoint_recovery_and_scrub() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), PolicySetId::new([0x4D; 32]).unwrap()),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("open metadata durability fixture");
    let Reply::Created { entry, .. } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"veeam-backup",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create metadata durability fixture")
    else {
        panic!("ASSERT: create reply");
    };
    let inode = entry.attr.inode;
    let retention = b"2038-01-19T03:14:07Z";
    let access_acl = posix_acl(&[
        (0x01, 7, u32::MAX),
        (0x04, 4, u32::MAX),
        (0x20, 1, u32::MAX),
    ]);
    for (name, value) in [
        (b"user.immutable.until".as_slice(), retention.as_slice()),
        (b"system.posix_acl_access".as_slice(), access_acl.as_slice()),
    ] {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::SetXattr {
                    inode,
                    name,
                    value,
                    mode: XattrSetMode::Upsert,
                },
            )
            .expect("set durable inode metadata");
    }
    appliance
        .namespace()
        .dispatch(
            ROOT_CALLER,
            Operation::SetFileFlags {
                inode,
                flags: FS_IMMUTABLE_FL,
            },
        )
        .expect("set immutable flag");
    appliance
        .checkpoint()
        .expect("checkpoint inode metadata")
        .expect("metadata mutation produces a generation");
    metadata.crash();
    containers.crash();

    let generations =
        GenerationRepository::new(metadata.clone(), PolicySetId::new([0x4D; 32]).unwrap());
    let container_repository = ContainerRepository::new(containers.clone());
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &generations,
        &container_repository,
    )
    .expect("recover metadata generation")
    .expect("metadata generation exists");
    assert_eq!(
        recovered.dispatch(
            CALLER,
            Operation::GetXattr {
                inode,
                name: b"user.immutable.until",
            },
        ),
        Ok(Reply::Xattr(retention.to_vec()))
    );
    assert_eq!(
        recovered.dispatch(CALLER, Operation::GetFileFlags { inode }),
        Ok(Reply::FileFlags(FS_IMMUTABLE_FL))
    );
    let Reply::Attr(attr) = recovered
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get recovered ACL-projected mode")
    else {
        panic!("ASSERT: getattr reply");
    };
    assert_eq!(attr.mode & 0o777, 0o741);
    generations
        .scrub_all_with_data(&container_repository)
        .expect("offline scrub accepts durable xattrs, ACL, and immutable flag");
    drop(recovered);
    let reopened = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, PolicySetId::new([0x4D; 32]).unwrap()),
        ContainerRepository::new(containers),
        16,
    )
    .expect("reopen metadata generation for mutation");
    assert_eq!(
        reopened.namespace().dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"veeam-backup",
            },
        ),
        Err(fastdup_posix::PosixError::PermissionDenied)
    );
}

fn posix_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut bytes = 2_u32.to_le_bytes().to_vec();
    for &(tag, permissions, id) in entries {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&permissions.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

#[test]
#[allow(clippy::too_many_lines)]
fn sparse_allocation_and_structural_splices_recover_without_data_reingest() {
    let root = unique_test_root("durable-sparse-allocation");
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
                name: b"sparse-splice",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse splice file")
    else {
        panic!("ASSERT: create returned the wrong reply");
    };
    let inode = entry.attr.inode;
    let mut oracle = vec![None; 19];
    for (offset, bytes) in [(0_u64, b"abcdef".as_slice()), (16, b"XYZ".as_slice())] {
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
            .expect("write sparse fixture");
        for (index, byte) in bytes.iter().copied().enumerate() {
            oracle[usize::try_from(offset).unwrap() + index] = Some(byte);
        }
    }
    appliance
        .checkpoint()
        .expect("commit source generation")
        .expect("source generation is dirty");

    for (offset, length, mode) in [
        (4_u64, 6_u64, FallocateMode::ZeroRange { keep_size: true }),
        (1, 2, FallocateMode::PunchHole),
        (5, 4, FallocateMode::InsertRange),
        (12, 3, FallocateMode::CollapseRange),
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
            .expect("apply metadata-only sparse mutation");
        match mode {
            FallocateMode::ZeroRange { .. } => {
                for slot in oracle
                    .iter_mut()
                    .skip(usize::try_from(offset).unwrap())
                    .take(usize::try_from(length).unwrap())
                {
                    *slot = Some(0);
                }
            }
            FallocateMode::PunchHole => {
                for slot in oracle
                    .iter_mut()
                    .skip(usize::try_from(offset).unwrap())
                    .take(usize::try_from(length).unwrap())
                {
                    *slot = None;
                }
            }
            FallocateMode::InsertRange => {
                oracle.splice(
                    usize::try_from(offset).unwrap()..usize::try_from(offset).unwrap(),
                    std::iter::repeat_n(None, usize::try_from(length).unwrap()),
                );
            }
            FallocateMode::CollapseRange => {
                let start = usize::try_from(offset).unwrap();
                oracle.drain(start..start + usize::try_from(length).unwrap());
            }
            FallocateMode::Allocate { .. } => unreachable!(),
        }
    }
    let checkpoint = appliance
        .checkpoint_profiled()
        .expect("commit sparse metadata successor")
        .expect("sparse successor is dirty");
    assert_eq!(
        checkpoint.metrics().checkpoint_rechunk_bytes(),
        0,
        "structural sparse edits must reuse committed DATA and FILL recipes"
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
        .expect("recover sparse successor")
        .expect("committed namespace exists");
    let Reply::Opened(handle) = recovered
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
        panic!("ASSERT: open returned the wrong reply");
    };
    let expected = oracle
        .iter()
        .map(|byte| byte.unwrap_or(0))
        .collect::<Vec<_>>();
    assert_eq!(
        read(
            &recovered,
            inode,
            handle,
            0,
            u32::try_from(expected.len()).unwrap(),
        ),
        expected
    );
    let Reply::Attr(attr) = recovered
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get recovered attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!(attr.size, oracle.len() as u64);
    assert_eq!(
        attr.allocated_bytes,
        oracle.iter().filter(|byte| byte.is_some()).count() as u64
    );
    let expected_hole = oracle
        .iter()
        .position(Option::is_none)
        .map_or(oracle.len() as u64, |offset| offset as u64);
    assert_eq!(
        recovered.dispatch(
            CALLER,
            Operation::Seek {
                inode,
                handle,
                offset: 0,
                kind: SeekKind::Hole,
            },
        ),
        Ok(Reply::Offset(expected_hole))
    );
    let scrub = generations
        .scrub_all_with_data(&containers)
        .expect("offline scrub accepts the sparse splice successor");
    assert_eq!(scrub.latest_manifest_files(), 1);
}

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
    assert_eq!(
        metrics
            .new_chunk_bytes()
            .checked_add(metrics.recipe_reuse_bytes())
            .and_then(|bytes| bytes.checked_add(metrics.exact_hit_bytes()))
            .expect("fixture accounting is bounded"),
        payload_bytes,
        "pre-cut publication plus checkpoint fallback must cover the complete payload"
    );
    assert!(metrics.logical_chunks() > 1);
    assert!(metrics.zstd_records() > 0);
    assert!(metrics.containers() > 0);
    assert!(metrics.container_file_bytes() < payload_bytes);
    assert!(metrics.peak_buffered_chunk_bytes() <= 32 * 1_024 * 1_024);
    assert!(metrics.peak_buffered_chunks() > 0);
    assert!(metrics.total().wall() >= metrics.manifest_plan().wall());
    assert!(metrics.manifest_plan().wall() >= metrics.cdc().wall());
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
    let root = unique_test_root("durable-seqcdc-checkpoint");
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
                name: b"seqcdc-stream",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create the SeqCDC fixture")
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
        .expect("write the SeqCDC fixture");
    appliance
        .checkpoint()
        .expect("checkpoint the SeqCDC fixture")
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
        .expect("recover the SeqCDC generation")
        .expect("one committed generation exists");
    let inode = recovered_generation
        .namespace_root()
        .inodes()
        .first()
        .expect("fixture inode is durable");
    let manifest = generations
        .read_manifest(inode.manifest_root())
        .expect("read the durable SeqCDC Manifest");
    let data_lengths = manifest
        .extents()
        .iter()
        .filter_map(|extent| match extent {
            fastdup_format::ManifestExtent::Data { logical_length, .. }
            | fastdup_format::ManifestExtent::DataSlice { logical_length, .. } => {
                Some(*logical_length)
            }
            fastdup_format::ManifestExtent::Hole { .. }
            | fastdup_format::ManifestExtent::Fill { .. } => None,
        })
        .collect::<Vec<_>>();
    assert!(
        data_lengths.len() > 8,
        "SeqCDC-v1 should produce roughly 64-KiB chunks, not four fixed 256-KiB cells"
    );
    assert!(
        data_lengths
            .iter()
            .all(|length| *length > 0 && *length <= CHUNK_BYTES as u64),
        "every durable logical Chunk must obey the SeqCDC-v1 maximum"
    );
    assert_eq!(data_lengths.iter().sum::<u64>(), payload.len() as u64);

    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("mount the complete SeqCDC generation")
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
        .expect("open the SeqCDC-backed file")
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
fn partial_seqcdc_range_clone_publishes_only_metadata_and_recovers_byte_exact() {
    let metadata = MemoryStorageIo::new();
    let containers_storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x8c; 32]).expect("policy identity is nonzero");
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(containers_storage.clone());
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        16,
    )
    .expect("open durable namespace");
    let payload = (0..3 * CHUNK_BYTES)
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
                name: b"source-full",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create source")
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
        .expect("write source");
    appliance.checkpoint().expect("checkpoint source");
    let data_objects_before = containers
        .recover_published()
        .expect("recover source containers")
        .len();

    let Reply::Created {
        entry: target,
        handle: target_handle,
    } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"synthetic-full.tmp",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create target")
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
                length: u64::try_from(payload.len()).expect("fixture length"),
            },
        )
        .expect("pre-size sparse target");
    let source_offset = 4_096_u64;
    let target_offset = 64 * 1_024_u64;
    let clone_length = 96 * 1_024_u64;
    assert_eq!(
        appliance.namespace().dispatch(
            CALLER,
            Operation::CloneRange {
                source_inode: source.attr.inode,
                source_handle,
                source_offset,
                target_inode: target.attr.inode,
                target_handle,
                target_offset,
                length: clone_length,
            },
        ),
        Ok(Reply::Cloned {
            bytes: clone_length,
            mutation_sequence: 2,
        })
    );
    assert_eq!(
        appliance.namespace().checkpointable_dirty_payload_bytes(),
        0,
        "range clone must not create frontend payload pages"
    );
    assert_eq!(
        appliance.namespace().dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"synthetic-full.tmp",
                new_parent: ROOT_INODE,
                new_name: b"synthetic-full.vbk",
                no_replace: false,
            },
        ),
        Ok(Reply::Empty)
    );
    appliance.checkpoint().expect("checkpoint metadata clone");
    assert_eq!(
        containers
            .recover_published()
            .expect("recover clone containers")
            .len(),
        data_objects_before,
        "synthetic clone must publish no DATA container"
    );
    let selected = generations
        .recover_latest_with_data(&containers)
        .expect("verify cloned generation")
        .expect("cloned generation is present");
    let durable_target = selected
        .namespace_root()
        .inodes()
        .iter()
        .find(|inode| inode.inode() == target.attr.inode.get())
        .expect("target inode is durable");
    let scrubbed = generations
        .scrub_manifest_tree_metadata(durable_target.manifest_root())
        .expect("offline scrub accepts the cloned Manifest");
    assert_eq!(
        scrubbed.logical_size(),
        u64::try_from(payload.len()).expect("fixture length fits u64")
    );
    let cloned_manifest = generations
        .read_manifest(durable_target.manifest_root())
        .expect("read cloned Manifest recipe");
    assert!(
        cloned_manifest
            .extents()
            .iter()
            .any(|extent| matches!(extent, fastdup_format::ManifestExtent::DataSlice { .. })),
        "misaligned clone boundaries must be represented by v2 Chunk slices"
    );

    metadata.crash();
    containers_storage.crash();
    let recovered_generations = GenerationRepository::new(metadata, policy);
    let recovered_containers = ContainerRepository::new(containers_storage);
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &recovered_generations,
        &recovered_containers,
    )
    .expect("recover cloned generation")
    .expect("cloned generation exists");
    let Reply::Entry(recovered_entry) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"synthetic-full.vbk",
            },
        )
        .expect("final synthetic-full name is durable")
    else {
        panic!("ASSERT: recovered synthetic-full lookup reply");
    };
    assert_eq!(recovered_entry.attr.inode, target.attr.inode);
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: target.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered target")
    else {
        panic!("ASSERT: recovered target open reply");
    };
    assert_eq!(
        read(
            &recovered,
            target.attr.inode,
            recovered_handle,
            target_offset,
            u32::try_from(clone_length).expect("clone length fits u32"),
        ),
        payload[usize::try_from(source_offset).expect("source offset fits")
            ..usize::try_from(source_offset + clone_length).expect("source end fits")]
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
    let first_run_count = appliance.exact_index_run_count();
    assert!(first_run_count >= 1);
    assert!(!appliance.exact_index_degraded());
    let first_containers = containers
        .verify_published()
        .expect("verify the first published Container set");
    let first_container_count = first_containers.len();
    assert!(first_container_count >= 1);
    assert!(
        first_containers
            .iter()
            .any(|container| container.zstd_record_count() > 0)
    );

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
        first_container_count,
        "a verified Exact Hit must not publish another physical Container"
    );
    assert_eq!(
        appliance.exact_index_run_count(),
        first_run_count,
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
    M: Clone + Send + Sync + fastdup_store::StorageIo + 'static,
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

#[test]
fn one_checkpoint_shares_one_barrier_across_all_new_metadata_objects() {
    let metadata = MemoryStorageIo::new();
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1()),
        ContainerRepository::new(MemoryStorageIo::new()),
        16,
    )
    .expect("open metadata batching fixture");
    create_and_write(&appliance, b"metadata-a", b"first manifest payload");
    create_and_write(&appliance, b"metadata-b", b"second manifest payload");
    let baseline = metadata.operations().len();

    appliance
        .checkpoint()
        .expect("checkpoint both manifests")
        .expect("fixture has one dirty generation");

    let root_syncs = metadata.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::SyncRoot)
        .count();
    assert_eq!(
        root_syncs, 2,
        "all immutable Manifest and Namespace objects share one barrier; the second sync preserves the independently retryable WAL-slot topology"
    );
}

#[test]
fn writable_appliance_installs_one_shared_pressure_bounded_verified_read_cache() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x9C; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("open cached appliance");
    let payload: Vec<u8> = (0..96 * 1_024)
        .map(|offset| {
            u8::try_from(offset % 251)
                .expect("fixture modulus fits u8")
                .wrapping_mul(31)
        })
        .collect();
    let inode = create_and_write(&appliance, b"cache-fixture", &payload);
    appliance
        .checkpoint()
        .expect("commit cache fixture")
        .expect("cache fixture is dirty");
    let Reply::Opened(handle) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open committed cache fixture")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };

    let baseline = containers.operation_count();
    assert_eq!(
        read(
            appliance.namespace(),
            inode,
            handle,
            0,
            u32::try_from(payload.len()).expect("fixture length fits u32"),
        ),
        payload
    );
    let after_first = containers.operation_count();
    assert!(after_first > baseline);
    assert_eq!(
        read(
            appliance.namespace(),
            inode,
            handle,
            0,
            u32::try_from(payload.len()).expect("fixture length fits u32"),
        ),
        payload
    );
    let after_second = containers.operation_count();
    let cache = appliance.verified_read_cache_status();
    assert!(cache.misses() > 0);
    if after_second == after_first {
        assert!(cache.hits() > 0);
        assert!(cache.target_bytes() > 0);
    } else {
        assert_eq!(cache.target_bytes(), 0);
        assert_eq!(cache.resident_bytes(), 0);
        assert!(
            cache.swap_used_bytes() > 0 || cache.available_bytes() <= cache.reserve_bytes(),
            "verified DATA cache may repeat I/O only after its live headroom disappears"
        );
    }
    assert!(cache.resident_bytes() <= cache.target_bytes());
    assert!(
        cache
            .resident_bytes()
            .checked_add(cache.metadata_bytes())
            .is_some_and(|bytes| bytes <= cache.hard_limit_bytes())
    );
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
        "the appended bytes must all pass through SeqCDC"
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

#[test]
fn sparse_tree_append_reads_only_the_right_manifest_path() {
    const PREFIX_BYTES: u64 = 68 * 1_024 * 1_024 * 1_024;
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x75; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("bootstrap sparse-tree fixture");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"huge-sparse-append",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse-tree fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: PREFIX_BYTES,
            },
        )
        .expect("create the large sparse prefix");
    for ordinal in 0..1_088_u64 {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: entry.attr.inode,
                    handle,
                    offset: ordinal * 64 * 1_024 * 1_024,
                    data: &[u8::try_from(ordinal % 251).expect("fixture byte fits u8")],
                },
            )
            .expect("materialize one sparse Manifest boundary");
    }
    appliance
        .checkpoint()
        .expect("checkpoint sparse prefix")
        .expect("sparse prefix needs one generation");

    let baseline = metadata.operation_count();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: PREFIX_BYTES,
                data: b"path-local append",
            },
        )
        .expect("append one small DATA extent");
    appliance
        .checkpoint()
        .expect("checkpoint sparse append")
        .expect("sparse append needs one generation");
    let metadata_reads = metadata.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    assert!(
        metadata_reads <= 64,
        "an append must read only the right Manifest path and bounded commit metadata, observed {metadata_reads} reads"
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one public-seam fixture keeps construction, bounded-I/O oracle, crash, and recovery together"
)]
fn sparse_tree_replacement_reads_only_the_touched_manifest_path() {
    const PREFIX_BYTES: u64 = 68 * 1_024 * 1_024 * 1_024;
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x76; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("bootstrap sparse-tree fixture");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"huge-sparse-replacement",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse-tree fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: PREFIX_BYTES,
            },
        )
        .expect("create the large sparse prefix");
    for ordinal in 0..1_088_u64 {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: entry.attr.inode,
                    handle,
                    offset: ordinal * 64 * 1_024 * 1_024,
                    data: &[u8::try_from(ordinal % 251).expect("fixture byte fits u8")],
                },
            )
            .expect("materialize one sparse Manifest boundary");
    }
    appliance
        .checkpoint()
        .expect("checkpoint sparse prefix")
        .expect("sparse prefix needs one generation");

    let baseline = metadata.operation_count();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: PREFIX_BYTES - 1,
                data: b"R",
            },
        )
        .expect("replace one byte near the sparse-tree tail");
    appliance
        .checkpoint()
        .expect("checkpoint sparse replacement")
        .expect("sparse replacement needs one generation");
    let metadata_reads = metadata.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    assert!(
        metadata_reads <= 256,
        "a replacement must read only touched Manifest paths and bounded commit metadata, observed {metadata_reads} reads"
    );
    drop(appliance);
    metadata.crash();
    containers.crash();

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(containers),
    )
    .expect("recover sparse replacement generation")
    .expect("sparse replacement generation exists");
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered sparse replacement")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            PREFIX_BYTES - 1,
            1,
        ),
        b"R"
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one public-seam tracer keeps the large-tree cut, bounded-I/O oracle, crash, and recovery together"
)]
fn sparse_tree_truncate_reuses_left_subtrees_and_recovers_the_exact_cut() {
    const ORIGINAL_BYTES: u64 = 68 * 1_024 * 1_024 * 1_024;
    const TRUNCATED_BYTES: u64 = 32 * 1_024 * 1_024 * 1_024 + 17;
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x79; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("bootstrap sparse-tree truncate fixture");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"huge-sparse-truncate",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse-tree truncate fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: ORIGINAL_BYTES,
            },
        )
        .expect("create the large sparse predecessor");
    for ordinal in 0..1_088_u64 {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: entry.attr.inode,
                    handle,
                    offset: ordinal * 64 * 1_024 * 1_024,
                    data: &[u8::try_from(ordinal % 251).expect("fixture byte fits u8")],
                },
            )
            .expect("materialize one sparse Manifest boundary");
    }
    appliance
        .checkpoint()
        .expect("checkpoint sparse predecessor")
        .expect("sparse predecessor needs one generation");

    let baseline = metadata.operation_count();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: TRUNCATED_BYTES,
            },
        )
        .expect("truncate the large sparse file");
    appliance
        .checkpoint()
        .expect("checkpoint path-local truncate")
        .expect("truncate needs one generation");
    let metadata_reads = metadata.operations()[baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    assert!(
        metadata_reads <= 256,
        "truncate must read only the cutoff Manifest path and bounded commit metadata, observed {metadata_reads} reads"
    );
    drop(appliance);
    metadata.crash();
    containers.crash();

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(containers),
    )
    .expect("recover sparse truncate generation")
    .expect("sparse truncate generation exists");
    let Reply::Attr(recovered_attr) = recovered
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("stat recovered sparse truncate")
    else {
        panic!("ASSERT: getattr returned the wrong reply variant");
    };
    assert_eq!(recovered_attr.size, TRUNCATED_BYTES);
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered sparse truncate")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            32 * 1_024 * 1_024 * 1_024,
            18,
        ),
        [u8::try_from(512 % 251).expect("fixture byte fits u8")]
            .into_iter()
            .chain([0; 16])
            .collect::<Vec<_>>()
    );
    assert!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            TRUNCATED_BYTES,
            1,
        )
        .is_empty(),
        "the exact truncate cut must be EOF after recovery"
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one public-seam tracer keeps DATA-boundary reduction, crash, and byte-exact recovery together"
)]
fn truncate_inside_data_reencodes_only_the_boundary_prefix_and_recovers_byte_exactly() {
    const ORIGINAL_BYTES: usize = 1_048_576;
    const TRUNCATED_BYTES: usize = 333_333;
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let policy = PolicySetId::new([0x7a; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(containers.clone()),
        16,
    )
    .expect("bootstrap DATA-boundary truncate fixture");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"truncate-inside-data",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create DATA-boundary fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let payload = pseudorandom_payload(ORIGINAL_BYTES, 0x713d_6b2f_98a0_4ce1);
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
        .expect("write DATA-boundary fixture");
    appliance
        .checkpoint()
        .expect("checkpoint complete DATA predecessor")
        .expect("DATA predecessor needs one generation");
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: u64::try_from(TRUNCATED_BYTES).expect("fixture length fits u64"),
            },
        )
        .expect("truncate inside one DATA Chunk");
    appliance
        .checkpoint()
        .expect("checkpoint DATA-boundary truncate")
        .expect("DATA-boundary truncate needs one generation");
    drop(appliance);
    metadata.crash();
    containers.crash();

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(containers),
    )
    .expect("recover DATA-boundary truncate")
    .expect("DATA-boundary generation exists");
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered DATA-boundary file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            0,
            u32::try_from(TRUNCATED_BYTES).expect("fixture length fits u32"),
        ),
        payload[..TRUNCATED_BYTES]
    );
    assert!(
        read(
            &recovered,
            entry.attr.inode,
            recovered_handle,
            u64::try_from(TRUNCATED_BYTES).expect("fixture length fits u64"),
            1,
        )
        .is_empty()
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
fn checkpoint_consumes_writer_verified_dependencies_without_record_reread() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        ContainerRepository::new(containers.clone()),
        &ExactIndexRunRepository::new(indexes),
        16,
    )
    .expect("open proof-carrying appliance");
    let payload = pseudorandom_payload(2 * 1_024 * 1_024, 0x3e62_1f8b_94ad_c507);
    create_and_write(&appliance, b"verified-dependency", &payload);

    let baseline = containers.operation_count();
    appliance
        .checkpoint()
        .expect("commit writer-verified DATA")
        .expect("new DATA requires one generation");
    let operations = containers.operations();
    let operations = &operations[baseline..];
    let publication_sample_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::CreateNew)
        .count()
        * 3;
    let data_range_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count();
    assert_eq!(
        data_range_reads,
        publication_sample_reads + 1,
        "the first durable generation range adds one fixed allocator-record reread, while the online successor commit must not reread DATA Records"
    );
}

#[test]
fn second_identical_file_reuses_online_proofs_or_reverifies_under_memory_pressure() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        ContainerRepository::new(containers.clone()),
        &ExactIndexRunRepository::new(indexes),
        16,
    )
    .expect("open online-proof appliance");
    let payload = pseudorandom_payload(2 * 1_024 * 1_024, 0x197e_38a4_cd52_f06b);
    create_and_write(&appliance, b"first-copy", &payload);
    appliance
        .checkpoint()
        .expect("commit first copy")
        .expect("first copy requires one generation");
    let historical_after_first = appliance.historical_proof_cache_status();
    let generation_after_first = appliance.generation_proof_set_status();
    assert!(
        historical_after_first.entry_count() > 0
            || generation_after_first.active_proofs() > 0
            || generation_after_first.frozen_proofs() > 0,
        "the first stream must retain verified online dependency evidence"
    );

    let baseline = containers.operation_count();
    let historical_hits = historical_after_first.hits();
    create_and_write(&appliance, b"second-copy", &payload);
    appliance
        .checkpoint()
        .expect("commit second copy")
        .expect("second copy requires one generation");
    let operations = containers.operations();
    let operations = &operations[baseline..];
    let publication_sample_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::CreateNew)
        .count()
        * 3;
    let data_range_reads = operations
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count();
    let record_rereads = data_range_reads.saturating_sub(publication_sample_reads);
    let after_second = appliance.historical_proof_cache_status();
    if historical_after_first.admission_rejections() == 0 {
        assert_eq!(
            record_rereads, 0,
            "an admitted immutable proof must avoid rereading identical DATA"
        );
        assert!(
            after_second.hits() > historical_hits,
            "the second stream must consume admitted committed history"
        );
    } else {
        assert!(historical_after_first.swap_used_bytes() > 0);
        assert!(
            record_rereads > 0,
            "pressure-rejected acceleration must fall back to DATA verification"
        );
    }
}

#[test]
fn actual_ingest_emits_replayable_online_proof_events() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        ContainerRepository::new(containers),
        &ExactIndexRunRepository::new(indexes),
        16,
    )
    .expect("open traceable appliance");
    appliance
        .start_online_proof_trace(16_384)
        .expect("start bounded trace");
    let payload = pseudorandom_payload(2 * 1_024 * 1_024, 0x9f41_37bd_82a6_c501);
    create_and_write(&appliance, b"trace-first", &payload);
    appliance
        .checkpoint()
        .expect("commit first traced copy")
        .expect("first traced copy changes namespace");
    create_and_write(&appliance, b"trace-second", &payload);
    appliance
        .checkpoint()
        .expect("commit second traced copy")
        .expect("second traced copy changes namespace");
    let trace = appliance
        .finish_online_proof_trace()
        .expect("finish bounded trace");

    assert!(trace.events().iter().any(|event| matches!(
        event,
        fastdup_appliance::ProofCacheEvent::AdmitPublished { .. }
    )));
    assert!(
        trace
            .events()
            .iter()
            .any(|event| matches!(event, fastdup_appliance::ProofCacheEvent::Lookup { .. }))
    );
    assert!(trace.events().iter().any(|event| matches!(
        event,
        fastdup_appliance::ProofCacheEvent::AdmitExactReuse { .. }
    )));
    let budget = 64 * 1_024 * 1_024;
    for policy in [
        fastdup_appliance::ProofCachePolicy::S3Fifo,
        fastdup_appliance::ProofCachePolicy::Sieve,
    ] {
        let report = fastdup_appliance::replay_proof_cache_trace(&trace, policy, budget)
            .expect("replay actual ingest trace");
        assert!(report.lookups() > 0);
        assert_eq!(report.byte_budget(), budget);
    }
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

#[test]
#[allow(clippy::too_many_lines)]
fn nested_directories_checkpoint_recover_and_scrub_with_one_file_manifest() {
    let metadata = MemoryStorageIo::new();
    let container_storage = MemoryStorageIo::new();
    let generations = GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1());
    let containers = ContainerRepository::new(container_storage.clone());
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1()),
        ContainerRepository::new(container_storage.clone()),
        16,
    )
    .expect("open writable namespace");
    let Reply::Entry(parent) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"parent",
                mode: 0o750,
            },
        )
        .expect("create durable parent directory")
    else {
        panic!("mkdir returned the wrong reply");
    };
    let Reply::Entry(child) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: parent.attr.inode,
                name: b"child",
                mode: 0o700,
            },
        )
        .expect("create durable child directory")
    else {
        panic!("mkdir returned the wrong reply");
    };
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: child.attr.inode,
                name: b"payload",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create file inside nested directory")
    else {
        panic!("create returned the wrong reply");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"nested durable bytes",
            },
        )
        .expect("write nested file");
    appliance
        .checkpoint()
        .expect("checkpoint nested namespace")
        .expect("nested namespace is dirty");
    drop(appliance);

    let scrub = generations
        .scrub_all_with_data(&containers)
        .expect("offline scrub accepts nested namespace");
    assert_eq!(scrub.latest_namespace_inodes(), 3);
    assert_eq!(scrub.latest_manifest_files(), 1);
    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("recover nested namespace")
        .expect("committed nested namespace exists");
    let Reply::Entry(recovered_parent) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"parent",
            },
        )
        .expect("recover parent directory")
    else {
        panic!("lookup returned the wrong reply");
    };
    let Reply::Entry(recovered_child) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: recovered_parent.attr.inode,
                name: b"child",
            },
        )
        .expect("recover child directory")
    else {
        panic!("lookup returned the wrong reply");
    };
    let Reply::Entry(recovered_file) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: recovered_child.attr.inode,
                name: b"payload",
            },
        )
        .expect("recover nested file")
    else {
        panic!("lookup returned the wrong reply");
    };
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: recovered_file.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered nested file")
    else {
        panic!("open returned the wrong reply");
    };
    assert_eq!(
        read(
            &recovered,
            recovered_file.attr.inode,
            recovered_handle,
            0,
            64,
        ),
        b"nested durable bytes"
    );
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
