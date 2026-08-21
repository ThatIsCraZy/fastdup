use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use fastdup_format::{ContainerId, SealedContainer};
use fastdup_store::StoreError;

use super::{Counters, verify_owned_reread};

pub(super) struct VerificationRequest {
    ordinal: usize,
    bytes: Vec<u8>,
    expected_container_hash: [u8; 32],
    container_id: ContainerId,
    container_generation: u64,
}

impl VerificationRequest {
    pub(super) fn new(
        ordinal: usize,
        bytes: Vec<u8>,
        expected_container_hash: [u8; 32],
        container_id: ContainerId,
        container_generation: u64,
    ) -> Self {
        Self {
            ordinal,
            bytes,
            expected_container_hash,
            container_id,
            container_generation,
        }
    }
}

pub(super) struct VerificationResult {
    ordinal: usize,
    verified: Result<SealedContainer, StoreError>,
}

impl VerificationResult {
    pub(super) const fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub(super) fn into_verified(self) -> Result<SealedContainer, StoreError> {
        self.verified
    }
}

enum VerificationCommand {
    Verify(VerificationRequest),
    Shutdown,
}

pub(super) struct VerificationPool {
    commands: mpsc::SyncSender<VerificationCommand>,
    results: mpsc::Receiver<VerificationResult>,
    workers: Vec<JoinHandle<()>>,
}

impl VerificationPool {
    pub(super) fn start(
        worker_count: NonZeroUsize,
        queue_capacity: usize,
        counters: &Arc<Counters>,
    ) -> io::Result<Self> {
        assert!(
            queue_capacity > 0,
            "ASSERT: verifier queue is bounded and nonzero"
        );
        let (commands, receive_commands) = mpsc::sync_channel(queue_capacity);
        let receive_commands = Arc::new(Mutex::new(receive_commands));
        let (send_results, results) = mpsc::channel();
        let mut workers = Vec::with_capacity(worker_count.get());
        let worker_limit = u64::try_from(worker_count.get())
            .expect("ASSERT: verifier worker count fits telemetry");
        for ordinal in 0..worker_count.get() {
            let receive_commands = Arc::clone(&receive_commands);
            let send_results = send_results.clone();
            let counters = Arc::clone(counters);
            match thread::Builder::new()
                .name(format!("fastdup-verify-{ordinal}"))
                .spawn(move || {
                    verifier_loop(&receive_commands, &send_results, &counters, worker_limit);
                }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    stop_workers(&commands, workers);
                    return Err(error);
                }
            }
        }
        drop(send_results);
        Ok(Self {
            commands,
            results,
            workers,
        })
    }

    pub(super) fn verify_batch(
        &self,
        requests: Vec<VerificationRequest>,
    ) -> Vec<VerificationResult> {
        let count = requests.len();
        for request in requests {
            self.commands
                .send(VerificationCommand::Verify(request))
                .expect("ASSERT: permanent verifier workers remain alive during publication");
        }
        (0..count)
            .map(|_| {
                self.results
                    .recv()
                    .expect("ASSERT: every submitted verification returns one result")
            })
            .collect()
    }
}

impl Drop for VerificationPool {
    fn drop(&mut self) {
        let workers = std::mem::take(&mut self.workers);
        stop_workers(&self.commands, workers);
    }
}

fn stop_workers(commands: &mpsc::SyncSender<VerificationCommand>, workers: Vec<JoinHandle<()>>) {
    for _ in 0..workers.len() {
        let _ = commands.send(VerificationCommand::Shutdown);
    }
    for worker in workers {
        assert!(worker.join().is_ok(), "ASSERT: Container verifier panicked");
    }
}

fn verifier_loop(
    commands: &Mutex<mpsc::Receiver<VerificationCommand>>,
    results: &mpsc::Sender<VerificationResult>,
    counters: &Counters,
    worker_limit: u64,
) {
    loop {
        let command = commands
            .lock()
            .expect("ASSERT: verifier queue lock poisoned")
            .recv();
        let Ok(command) = command else {
            return;
        };
        let VerificationCommand::Verify(request) = command else {
            return;
        };
        let active = counters
            .verifier
            .active
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("ASSERT: active verifier count cannot overflow");
        assert!(
            active <= worker_limit,
            "ASSERT: active verifications cannot exceed the fixed worker pool"
        );
        counters
            .verifier
            .peak_active
            .fetch_max(active, Ordering::Relaxed);
        counters
            .verifier
            .jobs_started
            .fetch_add(1, Ordering::Relaxed);

        let verified = verify_owned_reread(
            &request.bytes,
            request.expected_container_hash,
            request.container_id,
            request.container_generation,
        );
        if verified.is_err() {
            counters
                .verifier
                .jobs_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        counters
            .verifier
            .jobs_completed
            .fetch_add(1, Ordering::Relaxed);
        let previous = counters.verifier.active.fetch_sub(1, Ordering::Relaxed);
        assert!(previous > 0, "ASSERT: verifier active accounting is paired");
        if results
            .send(VerificationResult {
                ordinal: request.ordinal,
                verified,
            })
            .is_err()
        {
            return;
        }
    }
}
