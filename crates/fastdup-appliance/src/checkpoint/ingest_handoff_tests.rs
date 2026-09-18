//! Exercise the real queued writer while pausing its post-staging handoff.
use super::*;
use fastdup_posix::{
    AdmissionPauseReason, HandleId, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext,
};
use fastdup_testkit::MemoryStorageIo;
use std::sync::mpsc;

const MIB: usize = 1_048_576;
const CALLER: RequestContext = RequestContext {
    uid: 1000,
    gid: 1000,
    pid: 7,
};
type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

fn open_appliance() -> Arc<Appliance> {
    Arc::new(
        DurableNamespace::open_with_index(
            NamespaceConfig::default(),
            GenerationRepository::new(MemoryStorageIo::new(), checkpoint_policy_set()),
            ContainerRepository::new(MemoryStorageIo::new()),
            &ExactIndexRunRepository::new(MemoryStorageIo::new()),
            32,
        )
        .unwrap(),
    )
}

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

#[test]
fn transient_checkpoint_pause_releases_a_saturated_staging_gate() {
    let appliance = open_appliance();
    let gate_bytes = appliance.write_through.saturate_pending_gate_for_tests();
    let (inode, handle) = create(&appliance, b"staging");
    write(&appliance, inode, handle, 0, &[91; MIB]);

    let worker = Arc::clone(&appliance);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let checkpoint = std::thread::spawn(move || {
        let _ = result_tx.send(worker.checkpoint_profiled());
    });
    while !appliance.checkpoint_lock_is_held() {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        !appliance.checkpoint_staging_gate_open(),
        "a normal checkpoint must not bypass the staging gate"
    );
    let stuck_deadline = std::time::Instant::now() + Duration::from_millis(250);
    while std::time::Instant::now() < stuck_deadline {
        assert!(
            result_rx.try_recv().is_err(),
            "the Frozen cut must remain blocked while the staging gate is saturated"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    appliance
        .namespace()
        .pause_mutation_admission_for(AdmissionPauseReason::CheckpointTimeout);
    assert!(appliance.force_checkpoint_staging_gate_for_transient_pause());
    assert!(appliance.checkpoint_staging_gate_open());

    let result = result_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the forced staging gate must release the Frozen cut");
    checkpoint.join().unwrap();
    let metrics = result
        .expect("forced staging must preserve checkpoint integrity")
        .expect("a dirty checkpoint must commit")
        .metrics();
    assert!(
        metrics.checkpoint_rechunk_bytes() <= u64::try_from(MIB).unwrap(),
        "forced staging lost stable bytes: {} bytes",
        metrics.checkpoint_rechunk_bytes()
    );
    assert_eq!(
        appliance.forced_staging_batches(),
        1,
        "exactly one saturated staging batch may bypass the gate"
    );
    assert!(
        !appliance.checkpoint_staging_gate_open(),
        "a successful commit must clear the forced staging gate"
    );
    assert_file(appliance.namespace(), inode, &[91; MIB]);
    appliance
        .write_through
        .release_pending_gate_for_tests(gate_bytes);
    drop(appliance);
}

#[test]
fn checkpoint_staging_force_policy_only_opens_for_transient_pauses() {
    let appliance = open_appliance();
    for reason in [
        AdmissionPauseReason::CheckpointTimeout,
        AdmissionPauseReason::DirtyPressure,
        AdmissionPauseReason::DurabilityLag,
        AdmissionPauseReason::ProgressFailure,
    ] {
        appliance.namespace().pause_mutation_admission_for(reason);
        assert!(
            appliance.force_checkpoint_staging_gate_for_transient_pause(),
            "{reason:?} must permit one bounded staging batch"
        );
        assert!(appliance.checkpoint_staging_gate_open());
        appliance.clear_checkpoint_staging_gate();
    }

    for reason in [
        AdmissionPauseReason::Unspecified,
        AdmissionPauseReason::IntegrityFailure,
        AdmissionPauseReason::Shutdown,
    ] {
        appliance.namespace().pause_mutation_admission_for(reason);
        assert!(
            !appliance.force_checkpoint_staging_gate_for_transient_pause(),
            "{reason:?} must not bypass the staging gate"
        );
        assert!(!appliance.checkpoint_staging_gate_open());
    }
}

/// The staging escape hatch is scoped to one generation, so a supervisor that
/// keeps checkpointing while admission stays closed has to re-apply the force
/// policy for every attempt. A catch-up checkpoint that inherits the cleared
/// gate freezes its commit cut and then waits in the Ingest Queue for a batch
/// that only that same cut could release.
#[test]
fn a_committed_generation_closes_the_staging_gate_for_the_next_checkpoint() {
    let appliance = open_appliance();
    let gate_bytes = appliance.write_through.saturate_pending_gate_for_tests();
    let first_block = vec![91_u8; MIB].into_boxed_slice();
    let second_block = vec![92_u8; MIB].into_boxed_slice();
    let (first, first_handle) = create(&appliance, b"catch-up-first");
    write(&appliance, first, first_handle, 0, &first_block);
    appliance
        .namespace()
        .pause_mutation_admission_for(AdmissionPauseReason::CheckpointTimeout);
    assert!(appliance.force_checkpoint_staging_gate_for_transient_pause());
    appliance
        .checkpoint_profiled()
        .expect("forced staging must preserve checkpoint integrity")
        .expect("a dirty checkpoint must commit");
    assert!(
        !appliance.checkpoint_staging_gate_open(),
        "a committed generation must clear the forced staging gate"
    );

    // Distinct content: repeating the first block would let Exact Dedup answer
    // the write without new staging pressure, so the gate would never engage.
    appliance.namespace().resume_mutation_admission();
    let (second, second_handle) = create(&appliance, b"catch-up-second");
    write(&appliance, second, second_handle, 0, &second_block);
    appliance
        .namespace()
        .pause_mutation_admission_for(AdmissionPauseReason::CheckpointTimeout);

    let worker = Arc::clone(&appliance);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let catch_up = std::thread::spawn(move || {
        let _ = result_tx.send(worker.checkpoint_profiled());
    });
    let stuck_deadline = std::time::Instant::now() + Duration::from_millis(250);
    while std::time::Instant::now() < stuck_deadline {
        assert!(
            result_rx.try_recv().is_err(),
            "the catch-up cut must remain blocked until the gate is forced again"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(appliance.force_checkpoint_staging_gate_for_transient_pause());
    result_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("re-forcing the staging gate must release the catch-up cut")
        .expect("forced staging must preserve checkpoint integrity")
        .expect("a dirty catch-up checkpoint must commit");
    catch_up.join().unwrap();
    assert_eq!(
        appliance.forced_staging_batches(),
        2,
        "each generation may admit exactly one saturated staging batch"
    );
    assert_file(appliance.namespace(), first, &first_block);
    assert_file(appliance.namespace(), second, &second_block);
    appliance
        .write_through
        .release_pending_gate_for_tests(gate_bytes);
    drop(appliance);
}
