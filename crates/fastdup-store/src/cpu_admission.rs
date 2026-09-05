use rayon::prelude::*;
use std::num::NonZeroUsize;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Shared CPU admission for ingest and demand Record decoding.
#[derive(Debug)]
pub struct WorkerPermits {
    total: NonZeroUsize,
    available: Mutex<usize>,
    changed: Condvar,
}

impl WorkerPermits {
    /// Runs a bounded CPU batch with at most one permit per actual job.
    /// Empty batches do not acquire; partial grants preserve input order.
    /// Callers must not hold another lease while waiting for this batch.
    ///
    /// # Panics
    /// Panics on poisoned admission/job locks or a missing worker result.
    pub fn map<T: Send, R: Send>(
        &self,
        inputs: Vec<T>,
        desired: NonZeroUsize,
        apply: impl Fn(T) -> R + Sync,
    ) -> Vec<R> {
        use std::collections::VecDeque;
        use std::sync::Mutex;
        if inputs.is_empty() {
            return Vec::new();
        }
        let desired = NonZeroUsize::new(desired.get().min(inputs.len()))
            .expect("ASSERT: a nonempty CPU batch requests at least one worker");
        let lease = self.acquire(desired);
        if lease.workers().get() == 1 {
            return inputs.into_iter().map(apply).collect();
        }
        let length = inputs.len();
        let queue = Mutex::new(inputs.into_iter().enumerate().collect::<VecDeque<_>>());
        let completed = (0..lease.workers().get().min(length))
            .into_par_iter()
            .map(|_| {
                let mut completed = Vec::new();
                loop {
                    let job = queue
                        .lock()
                        .expect("ASSERT: bounded CPU queue lock poisoned")
                        .pop_front();
                    let Some((ordinal, input)) = job else {
                        break;
                    };
                    completed.push((ordinal, apply(input)));
                }
                completed
            })
            .collect::<Vec<_>>();
        let mut ordered = std::iter::repeat_with(|| None)
            .take(length)
            .collect::<Vec<_>>();
        for (ordinal, result) in completed.into_iter().flatten() {
            ordered[ordinal] = Some(result);
        }
        ordered
            .into_iter()
            .map(|result| result.expect("ASSERT: every admitted CPU job completed"))
            .collect()
    }

    #[must_use]
    pub fn new(total: NonZeroUsize) -> Self {
        Self {
            total,
            available: Mutex::new(total.get()),
            changed: Condvar::new(),
        }
    }

    /// Returns available permits without waiting for other stages.
    #[must_use]
    /// # Panics
    /// Panics if the admission lock is poisoned.
    pub fn available(&self) -> usize {
        *self
            .available
            .lock()
            .expect("ASSERT: CPU permit lock poisoned")
    }

    /// Demand reads never block while owning Singleflight leaders. Nested
    /// Rayon callers keep their existing permit and decode synchronously.
    #[must_use]
    /// # Panics
    /// Panics if the admission lock is poisoned.
    pub fn try_acquire(&self, desired: NonZeroUsize) -> Option<WorkerPermitLease<'_>> {
        if rayon::current_thread_index().is_some() {
            return None;
        }
        let mut available = self
            .available
            .lock()
            .expect("ASSERT: CPU permit lock poisoned");
        let acquired = NonZeroUsize::new(desired.get().min(*available))?;
        *available -= acquired.get();
        Some(WorkerPermitLease {
            pool: self,
            acquired,
            requested: desired,
            wait_ns: 0,
            blocked: false,
        })
    }

    /// Waits for at least one worker and accepts a partial grant.
    ///
    /// # Panics
    /// Panics on a poisoned lock or a request above the configured budget.
    pub fn acquire(&self, desired: NonZeroUsize) -> WorkerPermitLease<'_> {
        assert!(
            desired.get() <= self.total.get(),
            "ASSERT: requested encode workers exceed the write-through worker budget"
        );
        let wait_started = Instant::now();
        let mut blocked = false;
        let mut available = self
            .available
            .lock()
            .expect("ASSERT: encode worker permit lock poisoned");
        while *available == 0 {
            blocked = true;
            available = self
                .changed
                .wait(available)
                .expect("ASSERT: encode worker permit lock poisoned while waiting");
        }
        assert!(
            *available <= self.total.get(),
            "ASSERT: available encode workers exceed the write-through worker budget"
        );
        let acquired = desired.get().min(*available);
        assert!(acquired != 0, "ASSERT: a granted worker lease is nonempty");
        *available -= acquired;
        WorkerPermitLease {
            pool: self,
            acquired: NonZeroUsize::new(acquired)
                .expect("ASSERT: a granted worker lease is nonempty"),
            requested: desired,
            wait_ns: u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            blocked,
        }
    }
}

/// RAII ownership of a bounded number of CPU worker jobs.
#[derive(Debug)]
pub struct WorkerPermitLease<'a> {
    pool: &'a WorkerPermits,
    acquired: NonZeroUsize,
    requested: NonZeroUsize,
    wait_ns: u64,
    blocked: bool,
}

impl WorkerPermitLease<'_> {
    #[must_use]
    pub const fn workers(&self) -> NonZeroUsize {
        self.acquired
    }

    #[must_use]
    pub const fn requested_workers(&self) -> NonZeroUsize {
        self.requested
    }

    #[must_use]
    pub const fn wait_ns(&self) -> u64 {
        self.wait_ns
    }

    #[must_use]
    pub const fn blocked(&self) -> bool {
        self.blocked
    }
}

impl Drop for WorkerPermitLease<'_> {
    fn drop(&mut self) {
        let mut available = self
            .pool
            .available
            .lock()
            .expect("ASSERT: encode worker permit lock poisoned during retirement");
        *available = available
            .checked_add(self.acquired.get())
            .expect("ASSERT: encode worker permit accounting cannot overflow");
        assert!(
            *available <= self.pool.total.get(),
            "ASSERT: encode worker retirement exceeded the write-through worker budget"
        );
        self.pool.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "manual release-mode competing CPU admission A/B"]
    fn competing_small_batch_admission_microbenchmark() {
        use std::hint::black_box;
        use std::sync::Barrier;
        let bytes = vec![0x71; 65_536];
        let cpu = || {
            for _ in 0..1024 {
                black_box(blake3::hash(black_box(&bytes)));
            }
        };
        let measure = |bounded| {
            let admission = WorkerPermits::new(NonZeroUsize::new(10).unwrap());
            let ready = Barrier::new(2);
            let start = Instant::now();
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    // Compare the former ten-permit request for one job with
                    // the new one-permit request, using identical CPU work.
                    let _lease =
                        admission.acquire(NonZeroUsize::new(if bounded { 1 } else { 10 }).unwrap());
                    ready.wait();
                    cpu();
                });
                scope.spawn(|| {
                    ready.wait();
                    admission.map(vec![(); 9], NonZeroUsize::new(9).unwrap(), |()| cpu());
                });
            });
            start.elapsed()
        };
        let mut samples = [Vec::new(), Vec::new()];
        for round in 0..11 {
            for side in 0..2 {
                let side = (side + round) % 2;
                samples[side].push(measure(side == 1));
            }
        }
        for samples in &mut samples {
            samples.sort_unstable();
        }
        println!(
            "competing_admission reserved_ten_ms={:.3} reserved_one_ms={:.3} speedup={:.3}",
            samples[0][5].as_secs_f64() * 1000.0,
            samples[1][5].as_secs_f64() * 1000.0,
            samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
        );
    }
}
