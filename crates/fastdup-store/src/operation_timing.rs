//! Bounded timing observations, independent of storage and scheduling locks.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One operation family. Clones observe the same counters without retaining work.
#[derive(Clone, Debug, Default)]
pub struct OperationTiming(Arc<Mutex<State>>);

#[derive(Debug, Default)]
struct State {
    active: u64,
    completed: u64,
    total: Duration,
    maximum: Duration,
    busy_since: Option<Instant>,
}

/// Completed durations are cumulative; active duration is the current continuous
/// busy period, not the age of an arbitrarily selected concurrent operation.
#[derive(Clone, Debug)]
pub struct OperationTimingSnapshot {
    pub id: &'static str,
    pub active: u64,
    pub completed: u64,
    pub total: Duration,
    pub maximum: Duration,
    pub busy: Duration,
}

/// Ends observation on success, error or unwind. Never holds a lock across work.
#[derive(Debug)]
pub struct OperationTimer {
    timing: OperationTiming,
    started: Instant,
}

impl OperationTiming {
    /// Begins an observation without retaining a lock over the operation.
    ///
    /// # Panics
    /// Panics if an earlier invariant failure poisoned the telemetry lock.
    #[must_use]
    pub fn begin(&self) -> OperationTimer {
        let started = Instant::now();
        let mut state = self.0.lock().expect("operation timing lock");
        if state.active == 0 {
            state.busy_since = Some(started);
        }
        state.active += 1;
        OperationTimer {
            timing: self.clone(),
            started,
        }
    }

    /// Returns counters without acquiring any storage or queue lock.
    ///
    /// # Panics
    /// Panics if an earlier invariant failure poisoned the telemetry lock.
    #[must_use]
    pub fn snapshot(&self, id: &'static str) -> OperationTimingSnapshot {
        let state = self.0.lock().expect("operation timing lock");
        OperationTimingSnapshot {
            id,
            active: state.active,
            completed: state.completed,
            total: state.total,
            maximum: state.maximum,
            busy: state
                .busy_since
                .map_or(Duration::ZERO, |start| start.elapsed()),
        }
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        let mut state = self.timing.0.lock().expect("operation timing lock");
        state.active -= 1;
        state.completed = state.completed.saturating_add(1);
        state.total = state.total.saturating_add(elapsed);
        state.maximum = state.maximum.max(elapsed);
        if state.active == 0 {
            state.busy_since = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unfinished_and_overlapping_operations_remain_observable() {
        let timing = OperationTiming::default();
        let first = timing.begin();
        let second = timing.begin();
        let snapshot = timing.clone().snapshot("test");
        assert_eq!((snapshot.active, snapshot.completed), (2, 0));
        assert!(snapshot.busy > Duration::ZERO);
        drop(first);
        let snapshot = timing.snapshot("test");
        assert_eq!((snapshot.active, snapshot.completed), (1, 1));
        assert!(snapshot.busy > Duration::ZERO);
        drop(second);
        let snapshot = timing.snapshot("test");
        assert_eq!((snapshot.active, snapshot.completed), (0, 2));
        assert_eq!(snapshot.busy, Duration::ZERO);
        assert!(snapshot.total >= snapshot.maximum);
    }
}
