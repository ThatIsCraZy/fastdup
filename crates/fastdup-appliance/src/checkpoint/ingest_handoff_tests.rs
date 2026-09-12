//! Exercise the real queued writer while pausing its post-staging handoff.
use super::*;
use fastdup_posix::{HandleId, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_testkit::MemoryStorageIo;
use std::sync::mpsc;

const MIB: usize = 1_048_576;
const CALLER: RequestContext = RequestContext {
    uid: 1000,
    gid: 1000,
    pid: 7,
};
type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

fn create(appliance: &Appliance, name: &[u8]) -> (InodeId, HandleId) {
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
        .unwrap()
    else {
        panic!("expected created file")
    };
    (entry.attr.inode, handle)
}

fn write(appliance: &Appliance, inode: InodeId, handle: HandleId, offset: usize, bytes: &[u8]) {
    for (ordinal, block) in bytes.chunks(MIB).enumerate() {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: u64::try_from(offset + ordinal * MIB).unwrap(),
                    data: block,
                },
            )
            .unwrap();
    }
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
            panic!("expected data")
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
    assert!(
        eof.is_empty(),
        "Active suffix must not enter the recovered Frozen cut"
    );
}

fn sync(appliance: &Appliance, inode: InodeId, handle: HandleId) {
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

fn check_handoff(fill: bool) {
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
    let mut bytes = vec![73; 36 * MIB];
    if !fill {
        let mut state = 0x8f31_a7c5_19d2_4e6b_u64;
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let (base, handle) = create(&appliance, b"base");
        write(&appliance, base, handle, 0, &bytes);
        appliance.checkpoint().unwrap().unwrap();
    }
    let (inode, handle) = create(&appliance, b"handoff");
    write(&appliance, inode, handle, 0, &bytes[..28 * MIB]);
    sync(&appliance, inode, handle);
    appliance.namespace().begin_commit().unwrap().unwrap();

    let (reached_tx, reached_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    *appliance.write_through.after_inline_stage.lock().unwrap() = Some(Box::new(move || {
        reached_tx.send(()).unwrap();
        // Dropping the sender also releases this worker if the test unwinds.
        let _ = resume_rx.recv();
    }));
    // A post-cut job consumes the pre-cut Tail. Pause after releasing its Lane,
    // before process_job returns and before its Ingest sequence is retired.
    write(&appliance, inode, handle, 28 * MIB, &bytes[28 * MIB..]);
    reached_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let worker = Arc::clone(&appliance);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let checkpoint = std::thread::spawn(move || {
        let _ = result_tx.send(worker.checkpoint_profiled());
    });
    let result = result_rx.recv_timeout(Duration::from_secs(10));
    resume_tx.send(()).unwrap();
    checkpoint.join().unwrap();
    let metrics = result
        .expect("Frozen planning must not wait for post-cut job retirement")
        .unwrap()
        .unwrap()
        .metrics();
    assert!(
        metrics.checkpoint_rechunk_bytes() <= u64::try_from(MIB).unwrap(),
        "inline recipe handoff lost a stable prefix: {} bytes",
        metrics.checkpoint_rechunk_bytes()
    );
    assert_file(appliance.namespace(), inode, &bytes);
    sync(&appliance, inode, handle);
    drop(appliance);
    metadata.crash();
    data.crash();
    indexes.crash();
    let recovered = crate::recover_mount_with_index(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, checkpoint_policy_set()),
        &ContainerRepository::new(data),
        &ExactIndexRunRepository::new(indexes),
    )
    .unwrap()
    .unwrap();
    assert_file(&recovered, inode, &bytes[..28 * MIB]);
}

#[test]
fn frozen_cut_keeps_exact_recipes_before_post_cut_job_retirement() {
    check_handoff(false);
}

#[test]
fn frozen_cut_keeps_fill_recipes_before_post_cut_job_retirement() {
    check_handoff(true);
}
