use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{DurableNamespace, recover_mount};
use fastdup_format::PolicySetId;
use fastdup_posix::{NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_store::{ContainerRepository, FsStorageIo, GenerationRepository};

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
