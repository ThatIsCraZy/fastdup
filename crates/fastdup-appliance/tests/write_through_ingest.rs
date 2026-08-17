use fastdup_appliance::{DurableNamespace, checkpoint_policy_set_v1, recover_mount};
use fastdup_posix::{
    HandleId, InodeId, Namespace, NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply,
    RequestContext,
};
use fastdup_store::{ContainerRepository, ExactIndexRunRepository, GenerationRepository};
use fastdup_testkit::MemoryStorageIo;
use std::sync::Barrier;

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 7,
};

type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

fn open_appliance() -> Appliance {
    open_appliance_on(
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
    )
}

fn open_appliance_on(
    metadata: MemoryStorageIo,
    containers: MemoryStorageIo,
    indexes: MemoryStorageIo,
) -> Appliance {
    DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        ContainerRepository::new(containers),
        &ExactIndexRunRepository::new(indexes),
        32,
    )
    .expect("open write-through appliance")
}

fn create_file(appliance: &Appliance, name: &[u8]) -> (InodeId, HandleId) {
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
        .expect("create ingest file")
    else {
        panic!("create returned the wrong reply");
    };
    (entry.attr.inode, handle)
}

fn fixture_block() -> Vec<u8> {
    (0_usize..1_048_576)
        .map(|index| {
            u8::try_from((index.wrapping_mul(131) ^ (index >> 9)) % 251)
                .expect("fixture byte is bounded")
        })
        .collect()
}

fn write_fixture(appliance: &Appliance, inode: InodeId, handle: HandleId) -> Vec<u8> {
    write_salted_fixture(appliance, inode, handle, 0)
}

fn write_salted_fixture(
    appliance: &Appliance,
    inode: InodeId,
    handle: HandleId,
    salt: u8,
) -> Vec<u8> {
    let mut block = fixture_block();
    for byte in &mut block {
        *byte = byte.wrapping_add(salt);
    }
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal + u64::from(salt)).expect("fixture ordinal is bounded");
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: ordinal * 1_048_576,
                    data: &block,
                },
            )
            .expect("append one sequential block");
    }
    block[0] = 4_u8.wrapping_add(salt);
    block[..4_096].to_vec()
}

fn read_fixture(appliance: &Appliance, inode: InodeId, handle: HandleId) -> Vec<u8> {
    let Reply::Data(bytes) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: 4 * 1_048_576,
                length: 4_096,
            },
        )
        .expect("read externalized data")
    else {
        panic!("read returned the wrong reply");
    };
    bytes
}

fn write_one_mebibyte(
    appliance: &Appliance,
    inode: InodeId,
    handle: HandleId,
    ordinal: u64,
    block: &[u8],
) {
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: ordinal * 1_048_576,
                data: block,
            },
        )
        .expect("append one interleaved block");
}

#[test]
fn sequential_writes_publish_reduced_data_before_the_namespace_commit() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"long-stream");
    let expected = write_fixture(&appliance, inode, handle);

    let staged = appliance.write_through_status();
    assert_eq!(staged.sealed_uncommitted_containers(), 1);
    assert!(
        staged.buffered_bytes() <= 32 * 1_024 * 1_024 + 256 * 1_024,
        "the next Container plus one FastCDC tail is the hard staging bound: {staged:?}"
    );
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 4 * 1_024 * 1_024,
        "durable verified chunks must replace the resident POSIX dirty copies"
    );
    assert!(!staged.degraded());
    assert_eq!(read_fixture(&appliance, inode, handle), expected);

    let committed = appliance
        .checkpoint_profiled()
        .expect("commit the pre-published stream")
        .expect("stream has one dirty generation");
    assert!(
        committed.metrics().exact_hit_bytes() >= 32 * 1_024 * 1_024,
        "checkpoint must adopt pre-published exact locations instead of rewriting their bytes"
    );
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        0
    );
    assert_eq!(read_fixture(&appliance, inode, handle), expected);

    let (duplicate_inode, duplicate_handle) = create_file(&appliance, b"second-long-stream");
    assert_eq!(
        write_fixture(&appliance, duplicate_inode, duplicate_handle),
        expected
    );
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 4 * 1_024 * 1_024,
        "checkpoint-spanning Exact hits must also release resident POSIX dirty copies"
    );
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        0,
        "an Exact duplicate stream must not publish another Container"
    );
    assert_eq!(
        read_fixture(&appliance, duplicate_inode, duplicate_handle),
        expected
    );
}

#[test]
fn interleaved_files_keep_independent_bounded_ingest_lanes() {
    let appliance = open_appliance();
    let (inode_a, handle_a) = create_file(&appliance, b"stream-a");
    let (inode_b, handle_b) = create_file(&appliance, b"stream-b");
    let mut block_a = fixture_block();
    let mut block_b = fixture_block();
    for byte in &mut block_b {
        *byte = byte.wrapping_add(17);
    }

    for ordinal in 0_u64..34 {
        block_a[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        block_b[0] = u8::try_from(ordinal + 67).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode_a, handle_a, ordinal, &block_a);
        write_one_mebibyte(&appliance, inode_b, handle_b, ordinal, &block_b);
    }

    let staged = appliance.write_through_status();
    assert_eq!(staged.active_lanes(), 2);
    assert_eq!(staged.sealed_uncommitted_containers(), 2);
    assert!(
        staged.buffered_bytes() <= 2 * (32 * 1_024 * 1_024 + 256 * 1_024),
        "two lanes must each retain at most one Container target plus one CDC suffix: {staged:?}"
    );
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 8 * 1_024 * 1_024,
        "both durable lanes must release their resident POSIX dirty copies"
    );
    block_a[0] = 4;
    block_b[0] = 71;
    assert_eq!(
        read_fixture(&appliance, inode_a, handle_a),
        block_a[..4_096]
    );
    assert_eq!(
        read_fixture(&appliance, inode_b, handle_b),
        block_b[..4_096]
    );
}

#[test]
fn parallel_lanes_publish_one_complete_exact_index_history() {
    let indexes = MemoryStorageIo::new();
    let appliance = open_appliance_on(
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
        indexes.clone(),
    );
    let (inode_a, handle_a) = create_file(&appliance, b"parallel-a");
    let (inode_b, handle_b) = create_file(&appliance, b"parallel-b");

    let barrier = Barrier::new(2);
    let (expected_a, expected_b) = std::thread::scope(|scope| {
        let writer_a = scope
            .spawn(|| write_salted_fixture_in_lockstep(&appliance, inode_a, handle_a, 0, &barrier));
        let writer_b = scope.spawn(|| {
            write_salted_fixture_in_lockstep(&appliance, inode_b, handle_b, 67, &barrier)
        });
        (
            writer_a.join().expect("parallel writer A must not panic"),
            writer_b.join().expect("parallel writer B must not panic"),
        )
    });

    assert_eq!(
        appliance.exact_index_run_count(),
        2,
        "both L0 publications must survive one serialized activation history"
    );
    assert_eq!(read_fixture(&appliance, inode_a, handle_a), expected_a);
    assert_eq!(read_fixture(&appliance, inode_b, handle_b), expected_b);
    assert!(!appliance.exact_index_degraded());
    let recovered = ExactIndexRunRepository::new(indexes)
        .recover_active()
        .expect("recover the serialized activation history")
        .expect("parallel publishers activated one durable Run Set");
    assert_eq!(recovered.run_count(), 2);
}

#[test]
fn frozen_cut_commits_while_the_next_epoch_stays_live_and_recovers_in_order() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = open_appliance_on(metadata.clone(), containers.clone(), indexes);
    let (inode, handle) = create_file(&appliance, b"epochs");
    write_at(&appliance, inode, handle, 0, b"generation-one");

    let frozen = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the first epoch")
        .expect("the first epoch is dirty");
    write_at(&appliance, inode, handle, 0, b"generation-two");

    let first = appliance
        .checkpoint()
        .expect("commit the already-frozen epoch")
        .expect("the frozen epoch needs one generation");
    assert_eq!(
        read_named(appliance.namespace(), b"epochs"),
        b"generation-two"
    );
    assert_eq!(
        appliance.namespace().checkpointable_dirty_payload_bytes(),
        14,
        "the post-cut write remains the active dirty epoch"
    );
    let recovered_first = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1()),
        &ContainerRepository::new(containers.clone()),
    )
    .expect("recover first committed epoch")
    .expect("the first generation exists");
    assert_eq!(read_named(&recovered_first, b"epochs"), b"generation-one");

    let successor = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the successor epoch")
        .expect("the successor epoch is dirty");
    assert_ne!(successor.token(), frozen.token());
    let second = appliance
        .checkpoint()
        .expect("commit the active successor epoch")
        .expect("the successor epoch needs one generation");
    assert_eq!(second.generation(), first.generation() + 1);
    metadata.crash();
    containers.crash();
    let recovered_second = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        &ContainerRepository::new(containers),
    )
    .expect("recover successor epoch after crash")
    .expect("the successor generation exists");
    assert_eq!(read_named(&recovered_second, b"epochs"), b"generation-two");
}

#[test]
fn ninth_interleaved_stream_does_not_fall_back_to_the_resident_dirty_guard() {
    let appliance = open_appliance();
    let mut streams = Vec::new();
    let mut blocks = Vec::new();
    for ordinal in 0_u8..9 {
        let name = format!("stream-{ordinal}");
        streams.push(create_file(&appliance, name.as_bytes()));
        let mut block = fixture_block();
        for byte in &mut block {
            *byte = byte.wrapping_add(ordinal.wrapping_mul(17));
        }
        blocks.push(block);
    }

    for write_ordinal in 0_u64..33 {
        for ((inode, handle), block) in streams.iter().copied().zip(&mut blocks) {
            block[0] = u8::try_from(write_ordinal)
                .expect("fixture ordinal is bounded")
                .wrapping_add(u8::try_from(inode.get()).expect("fixture inode fits u8"));
            write_one_mebibyte(&appliance, inode, handle, write_ordinal, block);
        }
    }

    let dirty = appliance.namespace().checkpointable_dirty_payload_bytes();
    assert!(
        dirty < 32 * 1_024 * 1_024,
        "the ninth hot stream must release stable chunks instead of accumulating {dirty} resident bytes"
    );
}

#[test]
fn container_crossing_a_frozen_cut_releases_the_valid_active_suffix() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"cross-cut");
    let mut block = fixture_block();
    for ordinal in 0_u64..16 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    let _frozen = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the partial Container")
        .expect("the prefix is dirty");

    for ordinal in 16_u64..50 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }

    let dirty = appliance.namespace().checkpointable_dirty_payload_bytes();
    assert!(
        dirty < 20 * 1_024 * 1_024,
        "valid post-cut chunks must externalize even when the same Container batch begins in the frozen epoch: {dirty} resident bytes"
    );
}

fn write_at(appliance: &Appliance, inode: InodeId, handle: HandleId, offset: u64, data: &[u8]) {
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset,
                data,
            },
        )
        .expect("write epoch bytes");
}

fn read_named(namespace: &Namespace, name: &[u8]) -> Vec<u8> {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name,
            },
        )
        .expect("lookup epoch file")
    else {
        panic!("lookup returned the wrong reply");
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
        .expect("open epoch file")
    else {
        panic!("open returned the wrong reply");
    };
    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(entry.attr.size).expect("fixture length fits u32"),
            },
        )
        .expect("read epoch file")
    else {
        panic!("read returned the wrong reply");
    };
    bytes
}

fn write_salted_fixture_in_lockstep(
    appliance: &Appliance,
    inode: InodeId,
    handle: HandleId,
    salt: u8,
    barrier: &Barrier,
) -> Vec<u8> {
    let mut block = fixture_block();
    for byte in &mut block {
        *byte = byte.wrapping_add(salt);
    }
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal + u64::from(salt)).expect("fixture ordinal is bounded");
        barrier.wait();
        write_one_mebibyte(appliance, inode, handle, ordinal, &block);
        barrier.wait();
    }
    block[0] = 4_u8.wrapping_add(salt);
    block[..4_096].to_vec()
}
