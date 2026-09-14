use super::*;
use fastdup_testkit::MemoryStorageIo;

#[test]
fn pipeline_telemetry_remains_available_while_checkpoint_lock_is_held() {
    let storage = MemoryStorageIo::default();
    let appliance = Arc::new(
        DurableNamespace::open(
            NamespaceConfig::default(),
            GenerationRepository::new(storage.clone(), checkpoint_policy_set()),
            ContainerRepository::new(storage),
            32,
        )
        .unwrap(),
    );
    let lock = appliance.checkpoint_lock.lock().unwrap();
    let writer = Arc::clone(&appliance);
    let worker = std::thread::spawn(move || writer.checkpoint_profiled());
    let observer = Arc::clone(&appliance);
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshots = observer.pipeline_timings();
            if let Some(wait) = snapshots
                .into_iter()
                .find(|row| row.id == "checkpointCheckpointLock" && row.active == 1)
            {
                let _ = send.send(wait);
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let observed = receive.recv_timeout(Duration::from_millis(400));
    // Release the actual lock and join both workers even if observation failed.
    drop(lock);
    worker.join().unwrap().unwrap();
    reader.join().unwrap();
    let wait =
        observed.expect("telemetry must meet the live sampler deadline while checkpoint waits");
    assert_eq!(wait.completed, 0);
    assert!(wait.busy > Duration::ZERO);
    assert!(
        appliance
            .pipeline_timings()
            .iter()
            .all(|row| row.active == 0)
    );
}

#[test]
fn full_exact_queue_exposes_waiters_before_they_finish() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let queue = Arc::new(ExactPublicationQueue {
        sender,
        timings: ExactQueueTimings::default(),
        worker: Mutex::new(None),
    });
    let (reply, _response) = mpsc::sync_channel(1);
    queue.send(ExactPublicationCommand::Flush(reply));
    let writer = Arc::clone(&queue);
    let blocked = std::thread::spawn(move || writer.flush());
    let deadline = Instant::now() + Duration::from_millis(400);
    let observed = loop {
        let rows = queue.snapshots();
        if rows
            .iter()
            .any(|row| row.id == "exactQueueWait" && row.active == 2)
        {
            break Some(rows);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    // Drain the real bounded channel, preserving the worker's flush reply.
    for _ in 0..2 {
        let (command, waiting) = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(waiting);
        if let ExactPublicationCommand::Flush(reply) = command {
            let _ = reply.send(());
        }
    }
    blocked.join().unwrap();
    let rows = observed.expect("queue saturation must be observable before publication resumes");
    assert!(
        rows.iter()
            .any(|row| row.id == "exactEnqueue" && row.active == 1)
    );
    assert!(
        rows.iter()
            .any(|row| row.id == "exactFlush" && row.active == 1)
    );
    assert!(queue.snapshots().iter().all(|row| row.active == 0));
}
