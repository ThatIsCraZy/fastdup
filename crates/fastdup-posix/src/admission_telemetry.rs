//! Observations of explicit admission transitions, never admission authority.
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionPauseReason {
    Unspecified,
    CheckpointTimeout,
    DirtyPressure,
    DurabilityLag,
    ProgressFailure,
    IntegrityFailure,
    Shutdown,
}

impl AdmissionPauseReason {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::CheckpointTimeout => "checkpointTimeout",
            Self::DirtyPressure => "dirtyPressure",
            Self::DurabilityLag => "durabilityLag",
            Self::ProgressFailure => "progressFailure",
            Self::IntegrityFailure => "integrityFailure",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AdmissionStatus {
    pub open: bool,
    pub reason: Option<AdmissionPauseReason>,
    pub closures: u64,
    pub closed: Duration,
    pub current_closed: Duration,
    pub maximum_closed: Duration,
}

#[derive(Debug)]
pub(crate) struct AdmissionTelemetry(Mutex<State>);

#[derive(Debug)]
struct State {
    open: bool,
    reason: Option<AdmissionPauseReason>,
    closures: u64,
    closed: Duration,
    maximum: Duration,
    since: Option<Instant>,
}

impl AdmissionTelemetry {
    pub(crate) fn new(open: bool) -> Self {
        Self(Mutex::new(State {
            open,
            reason: None,
            closures: 0,
            closed: Duration::ZERO,
            maximum: Duration::ZERO,
            since: None,
        }))
    }

    // Called under the existing admission write lock, so transitions cannot
    // overtake one another. Observers never acquire that authority lock.
    pub(crate) fn set(&self, open: bool, reason: AdmissionPauseReason) {
        self.set_at(open, reason, Instant::now());
    }

    fn set_at(&self, open: bool, reason: AdmissionPauseReason, now: Instant) {
        let mut state = self.0.lock().expect("admission telemetry lock");
        if state.open != open {
            if open {
                if let Some(since) = state.since.take() {
                    let elapsed = now.duration_since(since);
                    state.closed = state.closed.saturating_add(elapsed);
                    state.maximum = state.maximum.max(elapsed);
                }
            } else {
                state.since = Some(now);
                state.closures = state.closures.saturating_add(1);
            }
        }
        state.open = open;
        state.reason = (!open).then_some(reason);
    }

    pub(crate) fn snapshot(&self) -> AdmissionStatus {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> AdmissionStatus {
        let state = self.0.lock().expect("admission telemetry lock");
        let current = state
            .since
            .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        AdmissionStatus {
            open: state.open,
            reason: state.reason,
            closures: state.closures,
            closed: state.closed.saturating_add(current),
            current_closed: current,
            maximum_closed: state.maximum.max(current),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_pause_does_not_restart_the_clock_or_double_count() {
        let telemetry = AdmissionTelemetry::new(true);
        let start = Instant::now();
        telemetry.set_at(false, AdmissionPauseReason::CheckpointTimeout, start);
        telemetry.set_at(
            false,
            AdmissionPauseReason::ProgressFailure,
            start + Duration::from_secs(3),
        );
        let running = telemetry.snapshot_at(start + Duration::from_secs(7));
        assert_eq!(running.closures, 1);
        assert_eq!(running.current_closed, Duration::from_secs(7));
        assert_eq!(running.closed, running.current_closed);
        telemetry.set_at(
            true,
            AdmissionPauseReason::Unspecified,
            start + Duration::from_secs(8),
        );
        let finished = telemetry.snapshot_at(start + Duration::from_secs(10));
        assert!(finished.open);
        assert_eq!(finished.current_closed, Duration::ZERO);
        assert_eq!(finished.closed, Duration::from_secs(8));
        assert_eq!(finished.maximum_closed, Duration::from_secs(8));
        assert!(finished.reason.is_none());
    }
}
