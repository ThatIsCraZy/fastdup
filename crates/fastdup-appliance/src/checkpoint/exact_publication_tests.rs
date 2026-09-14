use super::*;
use fastdup_format::{ExactIndexLocation, ExactLocationTransition};
use fastdup_testkit::MemoryStorageIo;

fn entry(ordinal: u64) -> ExactIndexEntry {
    ExactIndexEntry::active(
        ChunkId::of(&ordinal.to_le_bytes()),
        32,
        ExactIndexLocation::raw(
            ContainerId::new([51; 16]).unwrap(),
            1,
            4096 + ordinal * 256,
            256,
            0,
        )
        .unwrap(),
    )
    .unwrap()
}

fn core(storage: MemoryStorageIo) -> Arc<ExactPublisherCore<MemoryStorageIo>> {
    Arc::new(ExactPublisherCore {
        repository: ExactIndexRunRepository::new(storage),
        profile: checkpoint_exact_index_profile_v1(),
        degraded: AtomicBool::new(false),
        recent: RwLock::new(BTreeMap::new()),
        similarity: None,
        failed_reduction_guard: Mutex::new(None),
    })
}

fn publish(entries: Vec<ExactIndexEntry>) -> ExactPublicationCommand {
    ExactPublicationCommand::Publish(entries, Vec::new(), None)
}

fn run(
    core: Arc<ExactPublisherCore<MemoryStorageIo>>,
    commands: Vec<ExactPublicationCommand>,
) -> ExactQueueTimings {
    let (sender, receiver) = mpsc::sync_channel(commands.len() + 1);
    for command in commands {
        sender.send((command, None)).unwrap();
    }
    sender
        .send((ExactPublicationCommand::Shutdown, None))
        .unwrap();
    let timings = ExactQueueTimings::default();
    ExactPublicationQueue::run(&core, &receiver, &timings);
    timings
}

#[test]
fn publisher_batches_additions_but_preserves_flush_and_transition_boundaries() {
    let core = core(MemoryStorageIo::new());
    let (sender, receiver) = mpsc::sync_channel(8);
    let (reply, flushed) = mpsc::sync_channel(0);
    for command in [
        publish(vec![entry(1)]),
        publish(vec![entry(2), entry(1)]),
        ExactPublicationCommand::Flush(reply),
        publish(vec![entry(3)]),
        publish(vec![ExactIndexEntry::retiring(entry(1)).unwrap()]),
        ExactPublicationCommand::Shutdown,
    ] {
        sender.send((command, None)).unwrap();
    }
    let worker_core = Arc::clone(&core);
    let timings = ExactQueueTimings::default();
    let worker_timings = timings.clone();
    let worker = std::thread::spawn(move || {
        ExactPublicationQueue::run(&worker_core, &receiver, &worker_timings)
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while timings.batch.snapshot("batch").completed == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    // The zero-capacity reply parks the worker at the real Flush boundary.
    let before = core.repository.pin_active_generation();
    let observed = before.as_ref().map(|index| {
        (
            index.run_set().generation(),
            index
                .lookup_transitions(entry(3).chunk_id(), 32)
                .unwrap()
                .candidates()
                .len(),
        )
    });
    let flush_result = flushed.recv_timeout(Duration::from_secs(3));
    worker.join().unwrap();
    flush_result.unwrap();
    assert_eq!(observed, Some((1, 0)));
    let active = core.repository.pin_active_generation().unwrap();
    assert_eq!(active.run_set().generation(), 3);
    assert_eq!(
        active
            .lookup_transitions(entry(1).chunk_id(), 32)
            .unwrap()
            .candidates()[0]
            .transition(),
        ExactLocationTransition::Retiring
    );
    assert_eq!(timings.publish.snapshot("commands").completed, 4);
    assert_eq!(timings.batch.snapshot("batches").completed, 3);
    assert_eq!(
        core.repository.audit_activation_log().unwrap(),
        Some(active.record())
    );
}

#[test]
fn publisher_batch_entry_and_command_limits_leave_remaining_work_ordered() {
    let core = core(MemoryStorageIo::new());
    let timings = run(
        Arc::clone(&core),
        vec![
            publish(
                (0..EXACT_PUBLICATION_BATCH_ENTRIES as u64 - 1)
                    .map(entry)
                    .collect(),
            ),
            publish(vec![entry(20_000), entry(20_001)]),
        ],
    );
    assert_eq!(timings.batch.snapshot("batches").completed, 2);
    assert!(!core.degraded.load(Ordering::Acquire));
    let timings = run(
        Arc::clone(&core),
        (30_000..30_017)
            .map(|ordinal| publish(vec![entry(ordinal)]))
            .collect(),
    );
    assert_eq!(timings.batch.snapshot("batches").completed, 3); // 8 + 8 + 1
    assert_eq!(timings.publish.snapshot("commands").completed, 17);
    let recovered = core.repository.recover_active().unwrap().unwrap();
    for ordinal in [0, 16_382, 20_000, 20_001, 30_000, 30_016] {
        assert!(
            !recovered
                .lookup_transitions(entry(ordinal).chunk_id(), 32)
                .unwrap()
                .candidates()
                .is_empty()
        );
    }
}

#[test]
fn failed_combined_publication_keeps_gc_admission_and_flush_completes() {
    let core = core(MemoryStorageIo::with_fail_before(0));
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let mut commands = Vec::new();
    for ordinal in 0..3 {
        core.remember_recent(&[entry(ordinal)]);
        commands.push(ExactPublicationCommand::Publish(
            vec![entry(ordinal)],
            Vec::new(),
            containers.try_pin_data_reference(),
        ));
    }
    let (reply, flushed) = mpsc::sync_channel(1);
    commands.push(ExactPublicationCommand::Flush(reply));
    let timings = run(Arc::clone(&core), commands);
    flushed.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(timings.batch.snapshot("batches").completed, 1);
    assert!(core.degraded.load(Ordering::Acquire));
    assert!(core.failed_reduction_guard.lock().unwrap().is_some());
    assert!(core.recent.read().unwrap().is_empty());
    run(Arc::clone(&core), vec![publish(vec![entry(4)])]);
    assert!(!core.degraded.load(Ordering::Acquire));
    assert!(core.failed_reduction_guard.lock().unwrap().is_some());
}

#[test]
fn batching_reduces_durable_operations_with_identical_recovered_entries() {
    let mut counts = Vec::new();
    for batched in [false, true] {
        let storage = MemoryStorageIo::new();
        let core = core(storage.clone());
        let mut commands = Vec::new();
        for ordinal in 0..32 {
            commands.push(publish(vec![entry(ordinal)]));
            if !batched {
                let (reply, _) = mpsc::sync_channel(1);
                commands.push(ExactPublicationCommand::Flush(reply));
            }
        }
        let timings = run(Arc::clone(&core), commands);
        let operations = storage.operation_count();
        let count = |op| {
            storage
                .operations()
                .iter()
                .filter(|observed| **observed == op)
                .count()
        };
        let writes = count(fastdup_testkit::StorageOperation::WriteAt);
        let file_syncs = count(fastdup_testkit::StorageOperation::SyncFile);
        let root_syncs = count(fastdup_testkit::StorageOperation::SyncRoot);
        let active = core.repository.recover_active().unwrap().unwrap();
        for ordinal in 0..32 {
            assert_eq!(
                active
                    .active_reference(entry(ordinal).chunk_id(), 32, None)
                    .unwrap(),
                Some(entry(ordinal))
            );
        }
        assert_eq!(
            core.repository.audit_activation_log().unwrap(),
            Some(active.record())
        );
        let batches = timings.batch.snapshot("batch").completed;
        println!(
            "batched={batched} commands=32 activations={batches} storage_operations={operations} writes={writes} file_syncs={file_syncs} root_syncs={root_syncs}"
        );
        counts.push(operations);
    }
    assert!(
        counts[1] * 2 < counts[0],
        "batching must remove durable work, not just move it"
    );
}
