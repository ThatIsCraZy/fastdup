//! Exercise the real queued writer while pausing its post-staging handoff.
use super::*;
use fastdup_posix::{
    AdmissionPauseReason, HandleId, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext,
};
use fastdup_testkit::MemoryStorageIo;
use std::sync::mpsc;

const MIB: usize = 1_048_576;
/// An order of magnitude below the five-second supervisor threshold, so a cut
/// that waits on its own backlog fails here instead of in the field.
const COMMIT_CUT_BUDGET: Duration = Duration::from_millis(500);
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

/// A checkpoint that is already running must never wait on pending-region
/// capacity, because the Lane drain that releases that capacity is the
/// statement after the wait. Before ADR 0097 the cycle was broken by the
/// five-second supervisor timeout, which turned every multi-stream ingest into
/// a sequence of five-second stalls.
#[test]
fn a_saturated_staging_gate_never_blocks_the_frozen_commit_cut() {
    let appliance = open_appliance();
    let gate_bytes = appliance.write_through.saturate_pending_gate_for_tests();
    let (inode, handle) = create(&appliance, b"staging");
    write(&appliance, inode, handle, 0, &[91; MIB]);

    let worker = Arc::clone(&appliance);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let checkpoint = std::thread::spawn(move || {
        let _ = result_tx.send(worker.checkpoint_profiled());
    });
    let result = result_rx
        .recv_timeout(COMMIT_CUT_BUDGET)
        .expect("the commit cut must admit the backlog it is waiting for");
    checkpoint.join().unwrap();

    assert!(
        appliance.namespace().mutation_admission_open(),
        "releasing the cut must not require an admission pause"
    );
    assert!(
        !appliance.checkpoint_staging_gate_open(),
        "the cut must not borrow the one-generation watchdog hatch"
    );
    assert!(
        !appliance.write_through.commit_cut_staging_open(),
        "the commit-cut hatch is scoped to the cut wait"
    );

    let metrics = result
        .expect("hatched staging must preserve checkpoint integrity")
        .expect("a dirty checkpoint must commit")
        .metrics();
    assert!(
        metrics.checkpoint_rechunk_bytes() <= u64::try_from(MIB).unwrap(),
        "hatched staging lost stable bytes: {} bytes",
        metrics.checkpoint_rechunk_bytes()
    );
    assert!(
        appliance.forced_staging_batches() >= 1,
        "the blocked batch must be accounted as a hatched admission"
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

/// The commit-cut hatch must close with the cut that opened it. A hatch that
/// leaked across generations would turn the pending-region gate into an
/// advisory number and let Lane payload grow past its budget between
/// checkpoints.
#[test]
fn consecutive_commit_cuts_each_close_their_staging_hatch() {
    let appliance = open_appliance();
    let gate_bytes = appliance.write_through.saturate_pending_gate_for_tests();
    // Distinct content: repeating a block would let Exact Dedup answer the
    // second write without new staging pressure, so the gate would never engage.
    let first_block = vec![91_u8; MIB].into_boxed_slice();
    let second_block = vec![92_u8; MIB].into_boxed_slice();
    let (first, first_handle) = create(&appliance, b"catch-up-first");
    let (second, second_handle) = create(&appliance, b"catch-up-second");

    for (inode, handle, block) in [
        (first, first_handle, &first_block),
        (second, second_handle, &second_block),
    ] {
        write(&appliance, inode, handle, 0, block);
        let worker = Arc::clone(&appliance);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let checkpoint = std::thread::spawn(move || {
            let _ = result_tx.send(worker.checkpoint_profiled());
        });
        result_rx
            .recv_timeout(COMMIT_CUT_BUDGET)
            .expect("every generation must admit its own backlog")
            .expect("hatched staging must preserve checkpoint integrity")
            .expect("a dirty checkpoint must commit");
        checkpoint.join().unwrap();
        assert!(
            !appliance.write_through.commit_cut_staging_open(),
            "a committed generation must close its commit-cut hatch"
        );
        assert!(
            !appliance.checkpoint_staging_gate_open(),
            "a committed generation must leave the watchdog hatch closed"
        );
    }

    assert_file(appliance.namespace(), first, &first_block);
    assert_file(appliance.namespace(), second, &second_block);
    appliance
        .write_through
        .release_pending_gate_for_tests(gate_bytes);
    drop(appliance);
}

/// The pending-region gate must charge exactly what the Ingest Lanes hold.
///
/// Two paths detach Lane payload without a staging reservation to settle it:
/// evicting a Lane frees its buffers, and the commit-cut drain publishes Lane
/// payload into Containers outside `stage_write_batch`. Bytes left charged for
/// either retire gate capacity for the process lifetime. On the test appliance
/// the ledger reached 3.1 GiB against a 304 MiB gate after 10 GiB of ingest,
/// at which point every staging reservation blocked, the Ingest workers parked
/// instead of staging, and the writers behind them spent 91 % of their wall
/// time waiting for a checkpoint to wake them.
#[test]
fn the_pending_region_gate_charges_only_what_the_lanes_hold() {
    let appliance = open_appliance();
    let assert_exact = |stage: &str| {
        let charged = appliance.write_through.charged_region_bytes();
        let live = appliance.write_through.live_lane_region_bytes();
        assert_eq!(
            charged, live,
            "{stage}: the gate charges {charged} bytes for {live} bytes of Lane payload"
        );
    };

    // Enough payload for the commit-cut drain to publish a Container, which is
    // the detachment that `stage_write_batch` never settles.
    let (inode, handle) = create(&appliance, b"drain-publication");
    let payload = vec![37_u8; 40 * MIB];
    write(&appliance, inode, handle, 0, &payload);
    appliance
        .checkpoint()
        .expect("a drained checkpoint commits");
    assert_exact("after the commit-cut drain");

    // Enough inodes to pass the Registry's eviction grace, so Lanes are
    // actually evicted while they still hold payload rather than overflowing.
    for ordinal in 0..(super::write_through::MAX_ACTIVE_INGEST_LANES_V1 * 3) {
        let (evicting, evicting_handle) =
            create(&appliance, format!("lane-{ordinal}").as_bytes());
        write(&appliance, evicting, evicting_handle, 0, &vec![91_u8; 65_536]);
    }
    appliance
        .checkpoint()
        .expect("a checkpoint over evicted Lanes commits");
    assert_exact("after Lane eviction");
    assert!(
        appliance.write_through.charged_region_bytes()
            < super::write_through::INGEST_PENDING_GATE_BYTES_V1,
        "the ledger must stay inside its own gate"
    );
    assert_file(appliance.namespace(), inode, &payload);
    drop(appliance);
}
