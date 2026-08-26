use std::fs;
use std::io::{self, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_store::{DataPoolUsage, MaintenancePriority, OnlineGcRunMode};

pub const ONLINE_GC_CONTROL_SOCKET_NAME: &str = ".fastdup-online-gc.sock";
pub const ONLINE_GC_CONTROL_REQUEST: &[u8] = b"GC NOW\n";

const FRONTEND_IDLE_AFTER: Duration = Duration::from_secs(30);
const ACTIVE_INTERVAL: Duration = Duration::from_mins(15);
const IDLE_INTERVAL: Duration = Duration::from_mins(1);
const PRESSURE_INTERVAL: Duration = Duration::from_secs(30);

#[must_use]
pub fn online_gc_control_path(metadata_root: &Path) -> PathBuf {
    metadata_root.join(ONLINE_GC_CONTROL_SOCKET_NAME)
}

/// Sends one immediate Online-GC request to the writable appliance.
///
/// Filesystem permissions on the daemon-owned mode-0600 Unix socket authorize
/// the local caller. The response is one bounded UTF-8 status line.
///
/// # Errors
///
/// Returns connection, timeout, protocol, response-bound, or socket I/O
/// failures.
pub fn request_online_gc_now(metadata_root: &Path) -> io::Result<String> {
    let mut stream = UnixStream::connect(online_gc_control_path(metadata_root))?;
    stream.set_read_timeout(Some(Duration::from_hours(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(ONLINE_GC_CONTROL_REQUEST)?;
    stream.flush()?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    BufReader::new(stream)
        .take(4_097)
        .read_to_string(&mut response)?;
    if response.len() > 4_096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Online-GC control response exceeds 4096 bytes",
        ));
    }
    if response.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Online-GC control socket closed without a response",
        ));
    }
    if !response.ends_with('\n') || response[..response.len() - 1].contains(['\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Online-GC control response is not exactly one line",
        ));
    }
    Ok(response)
}

/// Removes a disconnected prior-daemon socket without replacing a live owner.
///
/// # Errors
///
/// Returns metadata, type, live-owner, connection, or unlink failures.
pub fn remove_stale_online_gc_socket(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "Online-GC control path exists and is not a Unix socket",
        ));
    }
    match UnixStream::connect(path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another writable appliance owns the Online-GC control socket",
        )),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(path)
        }
        Err(error) => Err(error),
    }
}

/// Pure admission policy for bounded adaptive Online-GC quanta.
#[derive(Clone, Debug)]
pub struct OnlineGcScheduler {
    frontend_operations: u64,
    frontend_activity_at: Instant,
    started_at: Instant,
}

impl OnlineGcScheduler {
    #[must_use]
    pub fn new(now: Instant, frontend_operations: u64) -> Self {
        Self {
            frontend_operations,
            frontend_activity_at: now,
            started_at: now,
        }
    }

    /// Selects at most one quantum without performing I/O or mutating frontend
    /// accounting. A changed pre-existing `io_uring` submission counter is the
    /// only frontend activity signal.
    #[must_use]
    pub fn poll(
        &mut self,
        now: Instant,
        frontend_operations: u64,
        usage: DataPoolUsage,
    ) -> Option<OnlineGcRunMode> {
        if frontend_operations != self.frontend_operations {
            self.frontend_operations = frontend_operations;
            self.frontend_activity_at = now;
        }
        let pressure = usage.scrub_priority() == MaintenancePriority::Normal;
        let quiet = now.saturating_duration_since(self.frontend_activity_at) >= FRONTEND_IDLE_AFTER;
        let interval = if pressure {
            PRESSURE_INTERVAL
        } else if quiet {
            IDLE_INTERVAL
        } else {
            ACTIVE_INTERVAL
        };
        if now.saturating_duration_since(self.started_at) < interval {
            return None;
        }
        self.started_at = now;
        Some(if pressure {
            OnlineGcRunMode::Urgent
        } else if quiet {
            OnlineGcRunMode::Idle
        } else {
            OnlineGcRunMode::Background
        })
    }

    pub fn record_immediate_start(&mut self, now: Instant) {
        self.started_at = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_is_slow_under_load_fast_when_idle_and_urgent_under_pressure() {
        let started = Instant::now();
        let low = DataPoolUsage::new(50, 100).expect("low pressure is valid");
        let high = DataPoolUsage::new(90, 100).expect("high pressure is valid");
        let mut scheduler = OnlineGcScheduler::new(started, 10);

        assert_eq!(
            scheduler.poll(started + Duration::from_secs(31), 11, low),
            None,
            "new frontend I/O restarts the quiet interval"
        );
        assert_eq!(
            scheduler.poll(started + ACTIVE_INTERVAL, 12, low),
            Some(OnlineGcRunMode::Background)
        );
        assert_eq!(
            scheduler.poll(started + ACTIVE_INTERVAL + IDLE_INTERVAL, 12, low),
            Some(OnlineGcRunMode::Idle)
        );
        assert_eq!(
            scheduler.poll(
                started + ACTIVE_INTERVAL + IDLE_INTERVAL + PRESSURE_INTERVAL,
                12,
                high,
            ),
            Some(OnlineGcRunMode::Urgent)
        );
    }
}
