use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::net::Shutdown;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_store::{DataPoolUsage, OnlineGcRunMode};

pub const ONLINE_GC_ACTIVE_INTERVAL_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_ACTIVE_INTERVAL_SECONDS";
pub const ONLINE_GC_IDLE_AFTER_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_IDLE_AFTER_SECONDS";
pub const ONLINE_GC_IDLE_INTERVAL_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_IDLE_INTERVAL_SECONDS";
pub const ONLINE_GC_URGENT_INTERVAL_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_URGENT_INTERVAL_SECONDS";
pub const ONLINE_GC_PRESSURE_LOW_BASIS_POINTS_ENV: &str =
    "FASTDUP_ONLINE_GC_PRESSURE_LOW_BASIS_POINTS";
pub const ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS_ENV: &str =
    "FASTDUP_ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS";
pub const ONLINE_GC_DAILY_WINDOW_UTC_ENV: &str = "FASTDUP_ONLINE_GC_DAILY_WINDOW_UTC";
pub const ONLINE_GC_WINDOW_INTERVAL_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_WINDOW_INTERVAL_SECONDS";
pub const ONLINE_GC_MAX_RELOCATION_WORKERS_ENV: &str = "FASTDUP_ONLINE_GC_MAX_RELOCATION_WORKERS";

pub const ONLINE_GC_CONTROL_SOCKET_NAME: &str = ".fastdup-online-gc.sock";
pub const ONLINE_GC_CONTROL_REQUEST: &[u8] = b"GC NOW\n";

const FRONTEND_IDLE_AFTER: Duration = Duration::from_secs(30);
const ACTIVE_INTERVAL: Duration = Duration::from_mins(15);
const IDLE_INTERVAL: Duration = Duration::from_mins(1);
const PRESSURE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_PRESSURE_LOW_BASIS_POINTS: u16 = 8_500;
const DEFAULT_PRESSURE_HIGH_BASIS_POINTS: u16 = 9_000;

/// One daily UTC maintenance window. A start later than the end wraps across
/// midnight; `00:00..24:00` covers the full day.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DailyGcWindow {
    start_minute: u16,
    end_minute: u16,
}

impl DailyGcWindow {
    /// Constructs a half-open UTC minute range.
    ///
    /// # Errors
    ///
    /// Rejects an empty range, a start outside the day, or an end after 24:00.
    pub const fn new(start_minute: u16, end_minute: u16) -> Result<Self, OnlineGcPolicyError> {
        if start_minute >= 1_440 || end_minute > 1_440 || start_minute == end_minute {
            return Err(OnlineGcPolicyError::InvalidDailyWindow);
        }
        Ok(Self {
            start_minute,
            end_minute,
        })
    }

    const fn contains(self, minute: u16) -> bool {
        debug_assert!(minute < 1_440);
        if self.start_minute < self.end_minute {
            minute >= self.start_minute && minute < self.end_minute
        } else {
            minute >= self.start_minute || minute < self.end_minute
        }
    }

    /// Parses `HH:MM-HH:MM` in UTC. `24:00` is accepted only as the end.
    ///
    /// # Errors
    ///
    /// Rejects malformed clocks or an invalid/empty window.
    pub fn parse_utc(value: &str) -> Result<Self, OnlineGcPolicyError> {
        let (start, end) = value
            .split_once('-')
            .ok_or(OnlineGcPolicyError::InvalidDailyWindow)?;
        Self::new(
            parse_utc_minute(start, false)?,
            parse_utc_minute(end, true)?,
        )
    }
}

fn parse_utc_minute(value: &str, allow_day_end: bool) -> Result<u16, OnlineGcPolicyError> {
    let (hour, minute) = value
        .split_once(':')
        .ok_or(OnlineGcPolicyError::InvalidDailyWindow)?;
    let hour = hour
        .parse::<u16>()
        .map_err(|_| OnlineGcPolicyError::InvalidDailyWindow)?;
    let minute = minute
        .parse::<u16>()
        .map_err(|_| OnlineGcPolicyError::InvalidDailyWindow)?;
    if minute >= 60 || hour > 24 || (hour == 24 && (!allow_day_end || minute != 0)) {
        return Err(OnlineGcPolicyError::InvalidDailyWindow);
    }
    hour.checked_mul(60)
        .and_then(|minutes| minutes.checked_add(minute))
        .ok_or(OnlineGcPolicyError::InvalidDailyWindow)
}

/// Operator policy for adaptive Online-GC admission and relocation CPU use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineGcPolicy {
    active_interval: Duration,
    idle_after: Duration,
    idle_interval: Duration,
    urgent_interval: Duration,
    pressure_low_basis_points: u16,
    pressure_high_basis_points: u16,
    daily_window: Option<(DailyGcWindow, Duration)>,
    maximum_relocation_workers: NonZeroUsize,
}

impl Default for OnlineGcPolicy {
    fn default() -> Self {
        Self {
            active_interval: ACTIVE_INTERVAL,
            idle_after: FRONTEND_IDLE_AFTER,
            idle_interval: IDLE_INTERVAL,
            urgent_interval: PRESSURE_INTERVAL,
            pressure_low_basis_points: DEFAULT_PRESSURE_LOW_BASIS_POINTS,
            pressure_high_basis_points: DEFAULT_PRESSURE_HIGH_BASIS_POINTS,
            daily_window: None,
            maximum_relocation_workers: thread_parallelism(),
        }
    }
}

impl OnlineGcPolicy {
    /// Loads optional operator overrides from `FASTDUP_ONLINE_GC_*` variables.
    /// Missing variables retain the safe defaults.
    ///
    /// # Errors
    ///
    /// Rejects non-UTF-8, nonnumeric, zero, inconsistent-watermark, malformed
    /// window, or zero-worker configuration before the daemon opens storage.
    pub fn from_environment() -> Result<Self, OnlineGcPolicyConfigurationError> {
        let mut policy = Self::default();
        let active = environment_seconds(ONLINE_GC_ACTIVE_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.active_interval);
        let idle_after =
            environment_seconds(ONLINE_GC_IDLE_AFTER_SECONDS_ENV)?.unwrap_or(policy.idle_after);
        let idle = environment_seconds(ONLINE_GC_IDLE_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.idle_interval);
        let urgent = environment_seconds(ONLINE_GC_URGENT_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.urgent_interval);
        policy = policy
            .with_intervals(active, idle_after, idle, urgent)
            .map_err(OnlineGcPolicyConfigurationError::Policy)?;

        let low = environment_number::<u16>(ONLINE_GC_PRESSURE_LOW_BASIS_POINTS_ENV)?
            .unwrap_or(policy.pressure_low_basis_points);
        let high = environment_number::<u16>(ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS_ENV)?
            .unwrap_or(policy.pressure_high_basis_points);
        policy = policy
            .with_pressure_watermarks(low, high)
            .map_err(OnlineGcPolicyConfigurationError::Policy)?;

        let window = environment_utf8(ONLINE_GC_DAILY_WINDOW_UTC_ENV)?;
        let window_interval = environment_seconds(ONLINE_GC_WINDOW_INTERVAL_SECONDS_ENV)?;
        match (window, window_interval) {
            (Some(window), interval) => {
                policy = policy
                    .with_daily_utc_window(
                        DailyGcWindow::parse_utc(&window)
                            .map_err(OnlineGcPolicyConfigurationError::Policy)?,
                        interval.unwrap_or(policy.idle_interval),
                    )
                    .map_err(OnlineGcPolicyConfigurationError::Policy)?;
            }
            (None, Some(_)) => {
                return Err(OnlineGcPolicyConfigurationError::MissingWindow);
            }
            (None, None) => {}
        }
        if let Some(workers) = environment_number::<usize>(ONLINE_GC_MAX_RELOCATION_WORKERS_ENV)? {
            let workers = NonZeroUsize::new(workers).ok_or(
                OnlineGcPolicyConfigurationError::InvalidEnvironmentValue(
                    ONLINE_GC_MAX_RELOCATION_WORKERS_ENV,
                ),
            )?;
            policy = policy.with_maximum_relocation_workers(workers);
        }
        Ok(policy)
    }

    /// Replaces the active, quiet threshold, idle, and pressure intervals.
    ///
    /// # Errors
    ///
    /// Rejects any zero duration.
    pub const fn with_intervals(
        mut self,
        active_interval: Duration,
        idle_after: Duration,
        idle_interval: Duration,
        urgent_interval: Duration,
    ) -> Result<Self, OnlineGcPolicyError> {
        if active_interval.is_zero()
            || idle_after.is_zero()
            || idle_interval.is_zero()
            || urgent_interval.is_zero()
        {
            return Err(OnlineGcPolicyError::ZeroDuration);
        }
        self.active_interval = active_interval;
        self.idle_after = idle_after;
        self.idle_interval = idle_interval;
        self.urgent_interval = urgent_interval;
        Ok(self)
    }

    /// Replaces the inclusive pressure exit and entry watermarks.
    ///
    /// # Errors
    ///
    /// Rejects values above 100%, or a low watermark not below the high one.
    pub const fn with_pressure_watermarks(
        mut self,
        low_basis_points: u16,
        high_basis_points: u16,
    ) -> Result<Self, OnlineGcPolicyError> {
        if high_basis_points > 10_000 || low_basis_points >= high_basis_points {
            return Err(OnlineGcPolicyError::InvalidPressureWatermarks);
        }
        self.pressure_low_basis_points = low_basis_points;
        self.pressure_high_basis_points = high_basis_points;
        Ok(self)
    }

    /// Enables one daily UTC window with its own admission interval.
    ///
    /// # Errors
    ///
    /// Rejects a zero admission interval.
    pub const fn with_daily_utc_window(
        mut self,
        window: DailyGcWindow,
        interval: Duration,
    ) -> Result<Self, OnlineGcPolicyError> {
        if interval.is_zero() {
            return Err(OnlineGcPolicyError::ZeroDuration);
        }
        self.daily_window = Some((window, interval));
        Ok(self)
    }

    #[must_use]
    pub const fn with_maximum_relocation_workers(mut self, workers: NonZeroUsize) -> Self {
        self.maximum_relocation_workers = workers;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnlineGcPolicyError {
    ZeroDuration,
    InvalidPressureWatermarks,
    InvalidDailyWindow,
}

impl std::fmt::Display for OnlineGcPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ZeroDuration => "Online-GC policy durations must be nonzero",
            Self::InvalidPressureWatermarks => {
                "Online-GC pressure requires 0 <= low < high <= 10000 basis points"
            }
            Self::InvalidDailyWindow => {
                "Online-GC daily UTC window requires distinct minutes within 00:00..24:00"
            }
        })
    }
}

impl std::error::Error for OnlineGcPolicyError {}

pub enum OnlineGcPolicyConfigurationError {
    InvalidEnvironmentValue(&'static str),
    MissingWindow,
    Policy(OnlineGcPolicyError),
}

impl std::fmt::Display for OnlineGcPolicyConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEnvironmentValue(name) => {
                write!(formatter, "invalid Online-GC environment value in {name}")
            }
            Self::MissingWindow => write!(
                formatter,
                "{ONLINE_GC_WINDOW_INTERVAL_SECONDS_ENV} requires {ONLINE_GC_DAILY_WINDOW_UTC_ENV}"
            ),
            Self::Policy(error) => error.fmt(formatter),
        }
    }
}

impl std::fmt::Debug for OnlineGcPolicyConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for OnlineGcPolicyConfigurationError {}

fn environment_utf8(
    name: &'static str,
) -> Result<Option<String>, OnlineGcPolicyConfigurationError> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| OnlineGcPolicyConfigurationError::InvalidEnvironmentValue(name))
        })
        .transpose()
}

fn environment_number<T>(name: &'static str) -> Result<Option<T>, OnlineGcPolicyConfigurationError>
where
    T: std::str::FromStr,
{
    environment_utf8(name)?
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|_| OnlineGcPolicyConfigurationError::InvalidEnvironmentValue(name))
        })
        .transpose()
}

fn environment_seconds(
    name: &'static str,
) -> Result<Option<Duration>, OnlineGcPolicyConfigurationError> {
    environment_number::<u64>(name).map(|seconds| seconds.map(Duration::from_secs))
}

fn thread_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

fn utc_minute_of_day() -> u16 {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    u16::try_from((seconds / 60) % 1_440).expect("ASSERT: UTC minute of day fits u16")
}

#[must_use]
pub fn online_gc_control_path(metadata_root: &Path) -> PathBuf {
    metadata_root.join(ONLINE_GC_CONTROL_SOCKET_NAME)
}

struct OnlineGcSocketAccess {
    _directory: File,
    path: PathBuf,
}

impl OnlineGcSocketAccess {
    fn open(socket_path: &Path) -> io::Result<Self> {
        let directory_path = socket_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Online-GC control socket requires a parent directory",
            )
        })?;
        let file_name = socket_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Online-GC control socket requires a file name",
            )
        })?;
        let directory = File::open(directory_path)?;
        let path =
            PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(file_name);
        Ok(Self {
            _directory: directory,
            path,
        })
    }
}

/// Binds the daemon-owned filesystem socket without placing the complete
/// Metadata-root pathname in `sockaddr_un.sun_path`.
///
/// # Errors
///
/// Returns stale-owner, directory, bind, permission, or `/proc/self/fd`
/// access failures.
pub fn bind_online_gc_control_socket(metadata_root: &Path) -> io::Result<UnixListener> {
    let socket_path = online_gc_control_path(metadata_root);
    remove_stale_online_gc_socket(&socket_path)?;
    let access = OnlineGcSocketAccess::open(&socket_path)?;
    let listener = UnixListener::bind(&access.path)?;
    fs::set_permissions(&access.path, fs::Permissions::from_mode(0o600))?;
    Ok(listener)
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
    let socket_path = online_gc_control_path(metadata_root);
    let access = OnlineGcSocketAccess::open(&socket_path)?;
    let mut stream = UnixStream::connect(&access.path)?;
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
    let access = OnlineGcSocketAccess::open(path)?;
    match UnixStream::connect(&access.path) {
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
    policy: OnlineGcPolicy,
    pressure_latched: bool,
    status: OnlineGcSchedulerStatus,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OnlineGcSchedulerStatus {
    polls: u64,
    deferred_polls: u64,
    frontend_activity_changes: u64,
    background_admissions: u64,
    idle_admissions: u64,
    urgent_admissions: u64,
    scheduled_admissions: u64,
    immediate_requests: u64,
}

impl OnlineGcSchedulerStatus {
    #[must_use]
    pub const fn polls(self) -> u64 {
        self.polls
    }
    #[must_use]
    pub const fn deferred_polls(self) -> u64 {
        self.deferred_polls
    }
    #[must_use]
    pub const fn frontend_activity_changes(self) -> u64 {
        self.frontend_activity_changes
    }
    #[must_use]
    pub const fn background_admissions(self) -> u64 {
        self.background_admissions
    }
    #[must_use]
    pub const fn idle_admissions(self) -> u64 {
        self.idle_admissions
    }
    #[must_use]
    pub const fn urgent_admissions(self) -> u64 {
        self.urgent_admissions
    }
    #[must_use]
    pub const fn scheduled_admissions(self) -> u64 {
        self.scheduled_admissions
    }
    #[must_use]
    pub const fn immediate_requests(self) -> u64 {
        self.immediate_requests
    }
}

impl OnlineGcScheduler {
    #[must_use]
    pub fn new(now: Instant, frontend_operations: u64) -> Self {
        Self::with_policy(now, frontend_operations, OnlineGcPolicy::default())
    }

    #[must_use]
    pub fn with_policy(now: Instant, frontend_operations: u64, policy: OnlineGcPolicy) -> Self {
        Self {
            frontend_operations,
            frontend_activity_at: now,
            started_at: now,
            policy,
            pressure_latched: false,
            status: OnlineGcSchedulerStatus::default(),
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
        self.status.polls = self.status.polls.saturating_add(1);
        if frontend_operations != self.frontend_operations {
            self.frontend_operations = frontend_operations;
            self.frontend_activity_at = now;
            self.status.frontend_activity_changes =
                self.status.frontend_activity_changes.saturating_add(1);
        }
        if usage_at_least(usage, self.policy.pressure_high_basis_points) {
            self.pressure_latched = true;
        } else if usage_at_most(usage, self.policy.pressure_low_basis_points) {
            self.pressure_latched = false;
        }
        let pressure = self.pressure_latched;
        let quiet =
            now.saturating_duration_since(self.frontend_activity_at) >= self.policy.idle_after;
        let scheduled_interval = self
            .policy
            .daily_window
            .filter(|(window, _)| window.contains(utc_minute_of_day()))
            .map(|(_, interval)| interval);
        let scheduled = !pressure && scheduled_interval.is_some();
        let interval = if pressure {
            self.policy.urgent_interval
        } else if let Some(interval) = scheduled_interval {
            interval
        } else if quiet {
            self.policy.idle_interval
        } else {
            self.policy.active_interval
        };
        if now.saturating_duration_since(self.started_at) < interval {
            self.status.deferred_polls = self.status.deferred_polls.saturating_add(1);
            return None;
        }
        self.started_at = now;
        let mode = if pressure {
            OnlineGcRunMode::Urgent
        } else if scheduled || quiet {
            OnlineGcRunMode::Idle
        } else {
            OnlineGcRunMode::Background
        };
        match mode {
            OnlineGcRunMode::Background => {
                self.status.background_admissions =
                    self.status.background_admissions.saturating_add(1);
            }
            OnlineGcRunMode::Idle => {
                self.status.idle_admissions = self.status.idle_admissions.saturating_add(1);
                if scheduled {
                    self.status.scheduled_admissions =
                        self.status.scheduled_admissions.saturating_add(1);
                }
            }
            OnlineGcRunMode::Urgent => {
                self.status.urgent_admissions = self.status.urgent_admissions.saturating_add(1);
            }
        }
        Some(mode)
    }

    pub fn record_immediate_start(&mut self, now: Instant) {
        self.started_at = now;
        self.status.immediate_requests = self.status.immediate_requests.saturating_add(1);
    }

    #[must_use]
    pub const fn status(&self) -> OnlineGcSchedulerStatus {
        self.status
    }

    #[must_use]
    pub fn relocation_workers(&self, mode: OnlineGcRunMode) -> NonZeroUsize {
        if mode == OnlineGcRunMode::Background {
            NonZeroUsize::MIN
        } else {
            self.policy
                .maximum_relocation_workers
                .min(thread_parallelism())
        }
    }
}

fn usage_at_least(usage: DataPoolUsage, basis_points: u16) -> bool {
    u128::from(usage.used_bytes()) * 10_000
        >= u128::from(usage.capacity_bytes()) * u128::from(basis_points)
}

fn usage_at_most(usage: DataPoolUsage, basis_points: u16) -> bool {
    u128::from(usage.used_bytes()) * 10_000
        <= u128::from(usage.capacity_bytes()) * u128::from(basis_points)
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
        let status = scheduler.status();
        assert_eq!(status.polls(), 4);
        assert_eq!(status.deferred_polls(), 1);
        assert_eq!(status.frontend_activity_changes(), 2);
        assert_eq!(status.background_admissions(), 1);
        assert_eq!(status.idle_admissions(), 1);
        assert_eq!(status.urgent_admissions(), 1);
        assert_eq!(status.immediate_requests(), 0);
    }

    #[test]
    fn policy_hysteresis_does_not_oscillate_between_pressure_samples() {
        let started = Instant::now();
        let policy = OnlineGcPolicy::default()
            .with_intervals(
                Duration::from_secs(10),
                Duration::from_secs(30),
                Duration::from_secs(5),
                Duration::from_secs(2),
            )
            .expect("nonzero intervals are valid")
            .with_pressure_watermarks(8_000, 9_000)
            .expect("ordered basis-point watermarks are valid");
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, policy);
        let high = DataPoolUsage::new(90, 100).expect("high usage is valid");
        let between = DataPoolUsage::new(85, 100).expect("middle usage is valid");
        let low = DataPoolUsage::new(80, 100).expect("low usage is valid");

        assert_eq!(
            scheduler.poll(started + Duration::from_secs(2), 2, high),
            Some(OnlineGcRunMode::Urgent)
        );
        assert_eq!(
            scheduler.poll(started + Duration::from_secs(4), 3, between),
            Some(OnlineGcRunMode::Urgent),
            "pressure remains latched above the low watermark"
        );
        assert_eq!(
            scheduler.poll(started + Duration::from_secs(6), 4, low),
            None,
            "reaching the low watermark exits pressure and restores the active interval"
        );
    }

    #[test]
    fn scheduled_window_admits_bounded_idle_work_despite_frontend_activity() {
        let started = Instant::now();
        let policy = OnlineGcPolicy::default()
            .with_intervals(
                Duration::from_mins(1),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(5),
            )
            .expect("nonzero intervals are valid")
            .with_daily_utc_window(
                DailyGcWindow::new(0, 1_440).expect("all-day window is valid"),
                Duration::from_secs(5),
            )
            .expect("nonzero scheduled interval is valid")
            .with_maximum_relocation_workers(
                std::num::NonZeroUsize::new(2).expect("two is nonzero"),
            );
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, policy);
        let low = DataPoolUsage::new(50, 100).expect("low usage is valid");

        assert_eq!(
            scheduler.poll(started + Duration::from_secs(5), 2, low),
            Some(OnlineGcRunMode::Idle)
        );
        assert_eq!(scheduler.status().scheduled_admissions(), 1);
        assert_eq!(
            scheduler.relocation_workers(OnlineGcRunMode::Background),
            std::num::NonZeroUsize::MIN
        );
        assert_eq!(
            scheduler.relocation_workers(OnlineGcRunMode::Idle),
            std::num::NonZeroUsize::new(2).expect("two is nonzero")
        );
    }
}
