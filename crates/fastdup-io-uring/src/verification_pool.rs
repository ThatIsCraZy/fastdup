use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

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

pub(super) struct VerificationPool {
    worker_count: NonZeroUsize,
    counters: Arc<Counters>,
}

impl VerificationPool {
    pub(super) fn start(
        worker_count: NonZeroUsize,
        queue_capacity: usize,
        counters: &Arc<Counters>,
    ) -> Self {
        assert!(
            queue_capacity > 0,
            "ASSERT: verifier queue is bounded and nonzero"
        );
        Self {
            worker_count,
            counters: Arc::clone(counters),
        }
    }

    pub(super) fn verify_batch(
        &self,
        requests: Vec<VerificationRequest>,
    ) -> Vec<VerificationResult> {
        if requests.is_empty() {
            return Vec::new();
        }
        let request_count = requests.len();
        let shard_count = self.worker_count.get().min(request_count);
        let mut shards = (0..shard_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<VerificationRequest>>>();
        for (ordinal, request) in requests.into_iter().enumerate() {
            shards[ordinal % shard_count].push(request);
        }
        let results = Mutex::new(Vec::with_capacity(request_count));
        let hash_workers = if request_count == 1 {
            self.worker_count
        } else {
            NonZeroUsize::MIN
        };
        rayon::scope_fifo(|scope| {
            for shard in shards {
                let results = &results;
                let counters = &self.counters;
                scope.spawn_fifo(move |_| {
                    for request in shard {
                        let result =
                            verify_request(&request, hash_workers, self.worker_count, counters);
                        results
                            .lock()
                            .expect("ASSERT: verifier result lock poisoned")
                            .push(result);
                    }
                });
            }
        });
        let results = results
            .into_inner()
            .expect("ASSERT: verifier result lock poisoned after completion");
        assert_eq!(
            results.len(),
            request_count,
            "ASSERT: every submitted verification returns one result"
        );
        results
    }
}

fn verify_request(
    request: &VerificationRequest,
    hash_workers: NonZeroUsize,
    worker_limit: NonZeroUsize,
    counters: &Counters,
) -> VerificationResult {
    let active = counters
        .verifier
        .active
        .fetch_add(1, Ordering::Relaxed)
        .checked_add(1)
        .expect("ASSERT: active verifier count cannot overflow");
    assert!(
        active <= u64::try_from(worker_limit.get()).expect("ASSERT: verifier limit fits u64"),
        "ASSERT: active verifications cannot exceed the CPU-worker budget"
    );
    counters
        .verifier
        .peak_active
        .fetch_max(active, Ordering::Relaxed);
    counters
        .verifier
        .jobs_started
        .fetch_add(1, Ordering::Relaxed);
    if SealedContainer::container_hash_worker_count(request.bytes.len(), hash_workers).get() > 1 {
        counters
            .verifier
            .parallel_hashes
            .fetch_add(1, Ordering::Relaxed);
    }

    let verified = verify_owned_reread(
        &request.bytes,
        request.expected_container_hash,
        request.container_id,
        request.container_generation,
        hash_workers,
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
    VerificationResult {
        ordinal: request.ordinal,
        verified,
    }
}
