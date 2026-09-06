//! Checkpoint snapshots must retain the same lane across truncate barriers.
use super::*;
use fastdup_posix::{HandleId, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_testkit::MemoryStorageIo;

const MIB: usize = 1_048_576;
const CALLER: RequestContext = RequestContext {
    uid: 1000,
    gid: 1000,
    pid: 7,
};
type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

fn write(appliance: &Appliance, inode: InodeId, handle: HandleId, bytes: &[u8]) {
    for (ordinal, block) in bytes.chunks(MIB).enumerate() {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: u64::try_from(ordinal * MIB).unwrap(),
                    data: block,
                },
            )
            .unwrap();
    }
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Sync {
                inode,
                handle,
                data_only: true,
            },
        )
        .unwrap();
}

fn assert_file(namespace: &Namespace, inode: InodeId, expected: &[u8]) {
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
        panic!("expected read handle")
    };
    for (ordinal, block) in expected.chunks(MIB).enumerate() {
        let Reply::Data(bytes) = namespace
            .dispatch(
                CALLER,
                Operation::Read {
                    inode,
                    handle,
                    offset: u64::try_from(ordinal * MIB).unwrap(),
                    length: u32::try_from(block.len()).unwrap(),
                },
            )
            .unwrap()
        else {
            panic!("expected file bytes")
        };
        assert_eq!(bytes, block);
    }
    let Reply::Data(eof) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: u64::try_from(expected.len()).unwrap(),
                length: 1,
            },
        )
        .unwrap()
    else {
        panic!("expected EOF")
    };
    assert!(eof.is_empty());
}

fn assert_recovered(
    metadata: &MemoryStorageIo,
    data: &MemoryStorageIo,
    indexes: &MemoryStorageIo,
    inode: InodeId,
    expected: &[u8],
) {
    metadata.crash();
    data.crash();
    indexes.crash();
    let recovered = crate::recover_mount_with_index(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        &ContainerRepository::new(data.clone()),
        &ExactIndexRunRepository::new(indexes.clone()),
    )
    .unwrap()
    .unwrap();
    assert_file(&recovered, inode, expected);
}

#[test]
fn checkpoint_snapshot_survives_truncate_and_new_container_publication() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = Arc::new(
        DurableNamespace::open_with_index(
            NamespaceConfig::default(),
            GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
            ContainerRepository::new(data.clone()),
            &ExactIndexRunRepository::new(indexes.clone()),
            32,
        )
        .unwrap(),
    );
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"truncate-race",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("expected new file")
    };
    let inode = entry.attr.inode;
    let mut state = 0x8f31_a7c5_19d2_4e6b_u64;
    let bytes: Vec<u8> = (0..44 * MIB)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    let (old, new) = bytes.split_at(8 * MIB);
    write(&appliance, inode, handle, old);
    let old_lane = appliance.write_through.lane_for(inode);
    // ROOT sorts before the file. Hold its empty lane so the checkpoint has
    // frozen its Namespace cut and cloned the file lane, but cannot drain it.
    let blocker = appliance.write_through.lane_for(ROOT_INODE);
    let guard = blocker.lock().unwrap();
    let checkpoint_appliance = Arc::clone(&appliance);
    let checkpoint = std::thread::spawn(move || checkpoint_appliance.checkpoint());
    let deadline = Instant::now() + Duration::from_secs(30);
    while Arc::strong_count(&old_lane) < 3 {
        assert!(
            Instant::now() < deadline,
            "checkpoint must capture the lane"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let Reply::Opened(writer) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_WRITE,
                truncate: true,
            },
        )
        .unwrap()
    else {
        panic!("expected truncated file handle")
    };
    write(&appliance, inode, writer, new);
    drop(guard);
    checkpoint
        .join()
        .expect("checkpoint must preserve publication order")
        .expect("checkpoint succeeds")
        .expect("frozen cut exists");
    assert_file(appliance.namespace(), inode, new);
    assert_recovered(&metadata, &data, &indexes, inode, old);
    appliance.checkpoint().unwrap().unwrap();
    drop(appliance);
    assert_recovered(&metadata, &data, &indexes, inode, new);
}
