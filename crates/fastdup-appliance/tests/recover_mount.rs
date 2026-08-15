use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::recover_mount;
use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ManifestExtent, ManifestLeaf, NamespaceEntry,
    NamespaceRoot, PolicySetId,
};
use fastdup_posix::{
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};
use fastdup_store::{ContainerRepository, FsStorageIo, GenerationRepository};

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
    containers
        .publish_raw(
            ContainerId::new([0x91; 16]).expect("container identity is nonzero"),
            1,
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
