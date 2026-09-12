//! Cooperative stop requests for maintenance-only repository views.
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Debug, Default)]
pub struct MaintenanceCancellation(Arc<AtomicBool>);

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
        self.0.store(true, Ordering::Release);
    }

    /// Checks a safe boundary; never interrupts an in-progress publication.
    ///
    /// # Errors
    /// Returns the distinct stop outcome after cancellation was requested.
    pub fn check(&self) -> Result<(), MaintenanceCancelled> {
        if self.0.load(Ordering::Acquire) {
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
