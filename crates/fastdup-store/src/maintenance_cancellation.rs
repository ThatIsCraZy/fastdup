//! Cooperative stop requests for maintenance-only repository views.
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

const SHUTDOWN: u32 = 1;
const CONTENTION: u32 = 2;

#[derive(Clone, Debug, Default)]
pub struct MaintenanceCancellation(Arc<AtomicU32>);

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceCancelled;

impl fmt::Display for MaintenanceCancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("maintenance stopped at a safe boundary")
    }
}
impl std::error::Error for MaintenanceCancelled {}

impl MaintenanceCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.fetch_or(SHUTDOWN, Ordering::Release);
    }

    /// Requests cancellation of maintenance work that contends with durable progress.
    /// Unlike shutdown, the request is cleared before the next admitted quantum.
    pub fn cancel_contention(&self) {
        self.0.fetch_or(CONTENTION, Ordering::Release);
    }

    /// Clears a contention request while preserving shutdown cancellation.
    pub fn clear_contention(&self) {
        self.0.fetch_and(!CONTENTION, Ordering::Release);
    }

    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.0.load(Ordering::Acquire) & SHUTDOWN != 0
    }

    #[must_use]
    pub fn is_contention(&self) -> bool {
        self.0.load(Ordering::Acquire) & CONTENTION != 0
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) != 0
    }

    /// Checks a safe boundary; never interrupts an in-progress publication.
    ///
    /// # Errors
    /// Returns the distinct stop outcome after cancellation was requested.
    pub fn check(&self) -> Result<(), MaintenanceCancelled> {
        if self.0.load(Ordering::Acquire) != 0 {
            Err(MaintenanceCancelled)
        } else {
            Ok(())
        }
    }
}

pub(crate) fn check_io(token: Option<&MaintenanceCancellation>) -> io::Result<()> {
    token
        .map_or(Ok(()), MaintenanceCancellation::check)
        .map_err(|error| io::Error::new(io::ErrorKind::Interrupted, error))
}

pub(crate) fn is_cancelled_io(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<MaintenanceCancelled>)
}

#[cfg(test)]
mod tests {
    use super::MaintenanceCancellation;

    #[test]
    fn contention_cancellation_is_clearable_and_shutdown_is_sticky() {
        let token = MaintenanceCancellation::new();
        assert!(token.check().is_ok());

        token.cancel_contention();
        assert!(token.check().is_err());
        token.clear_contention();
        assert!(token.check().is_ok());

        token.cancel();
        token.cancel_contention();
        assert!(token.check().is_err());
        token.clear_contention();
        assert!(token.check().is_err());
    }
}
