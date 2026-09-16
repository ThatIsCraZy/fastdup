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

use fastdup_store::{DataPoolUsage, GcPhaseRequest, OnlineGcQuantum, OnlineGcRunMode};

pub const ONLINE_GC_METADATA_INTERVAL_SECONDS_ENV: &str =
    "FASTDUP_ONLINE_GC_METADATA_INTERVAL_SECONDS";
pub const ONLINE_GC_METADATA_MAX_INTERVAL_SECONDS_ENV: &str =
    "FASTDUP_ONLINE_GC_METADATA_MAX_INTERVAL_SECONDS";
pub const ONLINE_GC_DATA_INTERVAL_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_DATA_INTERVAL_SECONDS";
pub const ONLINE_GC_DATA_MAX_INTERVAL_SECONDS_ENV: &str =
    "FASTDUP_ONLINE_GC_DATA_MAX_INTERVAL_SECONDS";
pub const ONLINE_GC_DATA_DELETE_INTERVAL_SECONDS_ENV: &str =
    "FASTDUP_ONLINE_GC_DATA_DELETE_INTERVAL_SECONDS";
pub const ONLINE_GC_MIN_SAVINGS_BASIS_POINTS_ENV: &str =
    "FASTDUP_ONLINE_GC_MIN_SAVINGS_BASIS_POINTS";
pub const ONLINE_GC_IDLE_AFTER_SECONDS_ENV: &str = "FASTDUP_ONLINE_GC_IDLE_AFTER_SECONDS";
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
const METADATA_BASE_INTERVAL: Duration = Duration::from_mins(1);
const METADATA_MAX_INTERVAL: Duration = Duration::from_mins(15);
const DATA_BASE_INTERVAL: Duration = Duration::from_hours(1);
const DATA_MAX_INTERVAL: Duration = Duration::from_hours(24 * 7);
const DATA_DELETE_INTERVAL: Duration = Duration::from_hours(1);
const PRESSURE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MINIMUM_SAVINGS_BASIS_POINTS: u64 = 100;
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
    metadata_base_interval: Duration,
    metadata_max_interval: Duration,
    data_base_interval: Duration,
    data_max_interval: Duration,
    data_delete_interval: Duration,
    minimum_savings_basis_points: u64,
    idle_after: Duration,
    urgent_interval: Duration,
    pressure_low_basis_points: u16,
    pressure_high_basis_points: u16,
    daily_window: Option<(DailyGcWindow, Duration)>,
    maximum_relocation_workers: NonZeroUsize,
}

impl Default for OnlineGcPolicy {
    fn default() -> Self {
        Self {
            metadata_base_interval: METADATA_BASE_INTERVAL,
            metadata_max_interval: METADATA_MAX_INTERVAL,
            data_base_interval: DATA_BASE_INTERVAL,
            data_max_interval: DATA_MAX_INTERVAL,
            data_delete_interval: DATA_DELETE_INTERVAL,
            minimum_savings_basis_points: DEFAULT_MINIMUM_SAVINGS_BASIS_POINTS,
            idle_after: FRONTEND_IDLE_AFTER,
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
        let idle_after =
            environment_seconds(ONLINE_GC_IDLE_AFTER_SECONDS_ENV)?.unwrap_or(policy.idle_after);
        let urgent = environment_seconds(ONLINE_GC_URGENT_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.urgent_interval);
        let metadata_base = environment_seconds(ONLINE_GC_METADATA_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.metadata_base_interval);
        let metadata_max = environment_seconds(ONLINE_GC_METADATA_MAX_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.metadata_max_interval);
        let data_base = environment_seconds(ONLINE_GC_DATA_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.data_base_interval);
        let data_max = environment_seconds(ONLINE_GC_DATA_MAX_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.data_max_interval);
        let data_delete = environment_seconds(ONLINE_GC_DATA_DELETE_INTERVAL_SECONDS_ENV)?
            .unwrap_or(policy.data_delete_interval);
        let minimum_savings = environment_number::<u64>(ONLINE_GC_MIN_SAVINGS_BASIS_POINTS_ENV)?
            .unwrap_or(policy.minimum_savings_basis_points);
        policy = policy
            .with_intervals(idle_after, urgent)
            .and_then(|policy| {
                policy.with_adaptive_intervals(
                    metadata_base,
                    metadata_max,
                    data_base,
                    data_max,
                    data_delete,
                )
            })
            .map_err(OnlineGcPolicyConfigurationError::Policy)?;
        if minimum_savings > 10_000 {
            return Err(OnlineGcPolicyConfigurationError::InvalidEnvironmentValue(
                ONLINE_GC_MIN_SAVINGS_BASIS_POINTS_ENV,
            ));
        }
        policy.minimum_savings_basis_points = minimum_savings;

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
                        interval.unwrap_or(policy.metadata_base_interval),
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

    /// Replaces the quiet threshold and the pressure admission interval.
    ///
    /// # Errors
    ///
    /// Rejects any zero duration.
    pub const fn with_intervals(
        mut self,
        idle_after: Duration,
        urgent_interval: Duration,
    ) -> Result<Self, OnlineGcPolicyError> {
        if idle_after.is_zero() || urgent_interval.is_zero() {
            return Err(OnlineGcPolicyError::ZeroDuration);
        }
        self.idle_after = idle_after;
        self.urgent_interval = urgent_interval;
        Ok(self)
    }

    /// Replaces the two adaptive phase intervals. A phase starts at its base
    /// interval and doubles its wait after a quantum that saved less than the
    /// minimum savings, capped at the phase maximum.
    ///
    /// # Errors
    ///
    /// Rejects zero durations or a maximum below the base.
    pub fn with_adaptive_intervals(
        mut self,
        metadata_base_interval: Duration,
        metadata_max_interval: Duration,
        data_base_interval: Duration,
        data_max_interval: Duration,
        data_delete_interval: Duration,
    ) -> Result<Self, OnlineGcPolicyError> {
        if metadata_base_interval.is_zero()
            || metadata_max_interval < metadata_base_interval
            || data_base_interval.is_zero()
            || data_max_interval < data_base_interval
            || data_delete_interval.is_zero()
        {
            return Err(OnlineGcPolicyError::ZeroDuration);
        }
        self.metadata_base_interval = metadata_base_interval;
        self.metadata_max_interval = metadata_max_interval;
        self.data_base_interval = data_base_interval;
        self.data_max_interval = data_max_interval;
        self.data_delete_interval = data_delete_interval;
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

/// One adaptive interval: doubles its wait after an unprofitable quantum and
/// resets to the base interval after a quantum that saved enough.
#[derive(Clone, Copy, Debug)]
struct AdaptivePhase {
    base: Duration,
    maximum: Duration,
    wait: Duration,
    next_due: Instant,
    last_saved_basis_points: u64,
}

impl AdaptivePhase {
    fn new(now: Instant, base: Duration, maximum: Duration) -> Self {
        Self {
            base,
            maximum,
            wait: base,
            next_due: now.checked_add(base).unwrap_or(now),
            last_saved_basis_points: 0,
        }
    }

    fn record(
        &mut self,
        now: Instant,
        saved_basis_points: u64,
        minimum_savings_basis_points: u64,
        forced: bool,
    ) {
        self.last_saved_basis_points = saved_basis_points;
        if saved_basis_points >= minimum_savings_basis_points {
            self.wait = self.base;
        } else if !forced {
            self.wait = self.wait.saturating_mul(2).min(self.maximum);
        }
        self.next_due = now.checked_add(self.wait).unwrap_or(now);
    }
}

/// Pure admission policy for bounded adaptive Online-GC quanta.
#[derive(Clone, Debug)]
pub struct OnlineGcScheduler {
    frontend_operations: u64,
    frontend_deletes: u64,
    frontend_activity_at: Instant,
    last_usage: DataPoolUsage,
    metadata_phase: AdaptivePhase,
    data_phase: AdaptivePhase,
    data_delete_since_run: bool,
    urgent_started_at: Instant,
    window_next_due: Option<Instant>,
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
    metadata_wait_seconds: u64,
    data_wait_seconds: u64,
    metadata_saved_basis_points: u64,
    data_saved_basis_points: u64,
    data_delete_resets: u64,
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
    #[must_use]
    pub const fn metadata_wait_seconds(self) -> u64 {
        self.metadata_wait_seconds
    }
    #[must_use]
    pub const fn data_wait_seconds(self) -> u64 {
        self.data_wait_seconds
    }
    #[must_use]
    pub const fn metadata_saved_basis_points(self) -> u64 {
        self.metadata_saved_basis_points
    }
    #[must_use]
    pub const fn data_saved_basis_points(self) -> u64 {
        self.data_saved_basis_points
    }
    #[must_use]
    pub const fn data_delete_resets(self) -> u64 {
        self.data_delete_resets
    }
}

impl OnlineGcScheduler {
    /// Errors for a zero-usage pool only during tests; production callers pass
    /// a measured pool view.
    fn new_with_usage(
        now: Instant,
        frontend_operations: u64,
        usage: DataPoolUsage,
        policy: OnlineGcPolicy,
    ) -> Self {
        let metadata_phase = AdaptivePhase::new(
            now,
            policy.metadata_base_interval,
            policy.metadata_max_interval,
        );
        let data_phase =
            AdaptivePhase::new(now, policy.data_base_interval, policy.data_max_interval);
        let status = OnlineGcSchedulerStatus {
            metadata_wait_seconds: whole_seconds(policy.metadata_base_interval),
            data_wait_seconds: whole_seconds(policy.data_base_interval),
            ..OnlineGcSchedulerStatus::default()
        };
        Self {
            frontend_operations,
            frontend_deletes: 0,
            frontend_activity_at: now,
            last_usage: usage,
            metadata_phase,
            data_phase,
            data_delete_since_run: false,
            urgent_started_at: now,
            window_next_due: None,
            policy,
            pressure_latched: false,
            status,
        }
    }

    #[must_use]
    pub fn new(now: Instant, frontend_operations: u64) -> Self {
        Self::with_policy(now, frontend_operations, OnlineGcPolicy::default())
    }

    /// # Panics
    ///
    /// Never; the placeholder pool usage is a checked constant.
    #[must_use]
    pub fn with_policy(now: Instant, frontend_operations: u64, policy: OnlineGcPolicy) -> Self {
        // A neutral placeholder usage until the first poll observes the pool.
        let usage = DataPoolUsage::new(1, 100).expect("ASSERT: 1 of 100 is a valid usage");
        Self::new_with_usage(now, frontend_operations, usage, policy)
    }

    /// Selects at most one quantum without performing I/O or mutating frontend
    /// accounting. A changed pre-existing `io_uring` submission counter is the
    /// frontend activity signal; a changed frontend delete counter clamps the
    /// DATA phase back to the delete interval.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn poll(
        &mut self,
        now: Instant,
        frontend_operations: u64,
        frontend_deletes: u64,
        usage: DataPoolUsage,
    ) -> Option<OnlineGcQuantum> {
        self.status.polls = self.status.polls.saturating_add(1);
        self.last_usage = usage;
        if frontend_operations != self.frontend_operations {
            self.frontend_operations = frontend_operations;
            self.frontend_activity_at = now;
            self.status.frontend_activity_changes =
                self.status.frontend_activity_changes.saturating_add(1);
        }
        if frontend_deletes > self.frontend_deletes {
            self.frontend_deletes = frontend_deletes;
            self.data_delete_since_run = true;
            if self.data_phase.wait > self.policy.data_delete_interval {
                self.data_phase.wait = self.policy.data_delete_interval;
                let due = now
                    .checked_add(self.data_phase.wait)
                    .unwrap_or(self.data_phase.next_due);
                if due < self.data_phase.next_due {
                    self.data_phase.next_due = due;
                }
                self.status.data_wait_seconds = whole_seconds(self.data_phase.wait);
                self.status.data_delete_resets = self.status.data_delete_resets.saturating_add(1);
            }
        } else {
            self.frontend_deletes = frontend_deletes;
        }
        if usage_at_least(usage, self.policy.pressure_high_basis_points) {
            self.pressure_latched = true;
        } else if usage_at_most(usage, self.policy.pressure_low_basis_points) {
            self.pressure_latched = false;
        }
        let pressure = self.pressure_latched;
        let quiet =
            now.saturating_duration_since(self.frontend_activity_at) >= self.policy.idle_after;
        let window = self
            .policy
            .daily_window
            .filter(|(window, _)| window.contains(utc_minute_of_day()));
        if pressure {
            if now.saturating_duration_since(self.urgent_started_at) < self.policy.urgent_interval {
                self.status.deferred_polls = self.status.deferred_polls.saturating_add(1);
                return None;
            }
            self.urgent_started_at = now;
            let mode = OnlineGcRunMode::Urgent;
            self.status.urgent_admissions = self.status.urgent_admissions.saturating_add(1);
            return Some(OnlineGcQuantum {
                mode,
                phases: GcPhaseRequest::both(),
            });
        }
        let window_allows_data = window.is_some() || self.policy.daily_window.is_none();
        let mut data_due = now >= self.data_phase.next_due && window_allows_data;
        if window.is_some() {
            // The window interval caps how often the DATA phase may start
            // while the window is open.
            let capped = self.window_next_due.unwrap_or(now);
            data_due &= now >= capped;
        }
        let metadata_due = now >= self.metadata_phase.next_due;
        if !metadata_due && !data_due {
            self.status.deferred_polls = self.status.deferred_polls.saturating_add(1);
            return None;
        }
        let mode = if quiet {
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
                if window.is_some() {
                    self.status.scheduled_admissions =
                        self.status.scheduled_admissions.saturating_add(1);
                }
            }
            OnlineGcRunMode::Urgent => {
                unreachable!("ASSERT: pressure admission returned above; only quiet selects Idle")
            }
        }
        if data_due && let Some((_, interval)) = window {
            self.window_next_due = Some(now.checked_add(interval).unwrap_or(now));
        }
        Some(OnlineGcQuantum {
            mode,
            phases: GcPhaseRequest {
                metadata: metadata_due,
                data: data_due,
            },
        })
    }

    /// Applies the Metadata-phase outcome measured in basis points of the
    /// bytes the run actually inspected.
    pub fn record_metadata_run(&mut self, now: Instant, saved_basis_points: u64, forced: bool) {
        self.metadata_phase.record(
            now,
            saved_basis_points,
            self.policy.minimum_savings_basis_points,
            forced,
        );
        self.status.metadata_wait_seconds = whole_seconds(self.metadata_phase.wait);
        self.status.metadata_saved_basis_points = saved_basis_points;
    }

    /// Applies the DATA-phase outcome from bytes freed against the pool's
    /// used bytes observed at admission.
    pub fn record_data_run(&mut self, now: Instant, freed_bytes: u64, forced: bool) {
        let total = self.last_usage.used_bytes().max(1);
        let saved =
            u64::try_from(u128::from(freed_bytes.saturating_mul(10_000)) / u128::from(total))
                .unwrap_or(u64::MAX);
        self.data_phase
            .record(now, saved, self.policy.minimum_savings_basis_points, forced);
        self.data_delete_since_run = false;
        self.status.data_wait_seconds = whole_seconds(self.data_phase.wait);
        self.status.data_saved_basis_points = saved;
    }

    pub fn record_immediate_start(&mut self, now: Instant) {
        self.urgent_started_at = now;
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

fn whole_seconds(duration: Duration) -> u64 {
    duration.as_secs()
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

    fn adaptive_policy() -> OnlineGcPolicy {
        OnlineGcPolicy::default()
            .with_intervals(Duration::from_secs(30), Duration::from_secs(2))
            .expect("nonzero intervals are valid")
            .with_adaptive_intervals(
                Duration::from_secs(10),
                Duration::from_secs(80),
                Duration::from_hours(1),
                Duration::from_hours(16),
                Duration::from_mins(10),
            )
            .expect("ordered adaptive intervals are valid")
            .with_pressure_watermarks(8_000, 9_000)
            .expect("ordered basis-point watermarks are valid")
    }

    #[test]
    fn metadata_phase_adapts_independently_from_the_data_phase() {
        let started = Instant::now();
        let low = DataPoolUsage::new(100, 1_000).expect("low usage is valid");
        let mut scheduler = OnlineGcScheduler::with_policy(started, 10, adaptive_policy());

        assert_eq!(
            scheduler.poll(started + Duration::from_secs(9), 10, 0, low),
            None,
            "neither phase timer is due at its base interval yet"
        );
        assert_eq!(
            scheduler.poll(started + Duration::from_secs(10), 10, 0, low),
            Some(OnlineGcQuantum {
                mode: OnlineGcRunMode::Background,
                phases: GcPhaseRequest {
                    metadata: true,
                    data: false,
                },
            }),
            "the Metadata phase admits alone at its short base interval"
        );
        let mut now = started + Duration::from_secs(10);
        for expected_wait in [20_u64, 40, 80, 80] {
            scheduler.record_metadata_run(now, 0, false);
            now += Duration::from_secs(expected_wait);
            assert_eq!(
                scheduler.poll(now, 10, 0, low),
                Some(OnlineGcQuantum {
                    mode: OnlineGcRunMode::Idle,
                    phases: GcPhaseRequest {
                        metadata: true,
                        data: false,
                    },
                }),
                "unprofitable Metadata quanta double the wait up to the maximum"
            );
            assert_eq!(scheduler.status().metadata_wait_seconds(), expected_wait);
        }
        scheduler.record_metadata_run(now, 150, false);
        assert_eq!(scheduler.status().metadata_wait_seconds(), 10);
        assert_eq!(
            scheduler.poll(started + Duration::from_hours(1), 10, 0, low),
            Some(OnlineGcQuantum {
                mode: OnlineGcRunMode::Idle,
                phases: GcPhaseRequest {
                    metadata: true,
                    data: true,
                },
            }),
            "a profitable Metadata run returns to the base and the Data hour arrived"
        );
    }

    #[test]
    fn unprofitable_data_quanta_back_off_doubling_to_their_maximum() {
        let started = Instant::now();
        let low = DataPoolUsage::new(500, 1_000).expect("low usage is valid");
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, adaptive_policy());
        assert!(
            scheduler
                .poll(started + Duration::from_hours(1), 1, 0, low)
                .is_some_and(|quantum| quantum.phases.data)
        );
        let mut now = started + Duration::from_hours(1);
        for expected_wait in [2 * 3600_u64, 4 * 3600, 8 * 3600] {
            scheduler.record_data_run(now, 0, false);
            assert_eq!(scheduler.status().data_wait_seconds(), expected_wait);
            now += Duration::from_secs(expected_wait);
            assert!(
                scheduler
                    .poll(now, 1, 0, low)
                    .is_none_or(|quantum| !quantum.phases.data
                        || now >= started + Duration::from_secs(expected_wait)),
                "the Data phase stays quiet until its doubled wait elapsed"
            );
        }
        scheduler.record_data_run(now, 999, false);
        assert_eq!(scheduler.status().data_wait_seconds(), 3600);
        assert_eq!(scheduler.status().data_saved_basis_points(), 19_980);
    }

    #[test]
    fn fuse_delete_clamps_data_backoff_to_the_delete_interval() {
        let started = Instant::now();
        let low = DataPoolUsage::new(500, 1_000).expect("low usage is valid");
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, adaptive_policy());
        let mut now = started + Duration::from_hours(1);
        assert!(scheduler.poll(now, 1, 0, low).is_some());
        scheduler.record_data_run(now, 0, false);
        now += Duration::from_hours(2);
        assert!(scheduler.poll(now, 1, 0, low).is_some());
        scheduler.record_data_run(now, 0, false);
        assert_eq!(scheduler.status().data_wait_seconds(), 4 * 3600);

        now += Duration::from_mins(1);
        assert!(
            scheduler
                .poll(now, 1, 7, low)
                .is_some_and(|quantum| !quantum.phases.data),
            "the delete signal admits Metadata only"
        );
        assert_eq!(scheduler.status().data_wait_seconds(), 600);
        assert_eq!(scheduler.status().data_delete_resets(), 1);
        scheduler.record_metadata_run(now, 100, false);
        assert!(
            scheduler
                .poll(now + Duration::from_mins(10), 1, 7, low)
                .is_some_and(|quantum| quantum.phases.data),
            "the clamped Data timer fires one delete interval later"
        );
    }

    #[test]
    fn pressure_latches_urgent_both_phase_quanta_until_the_low_watermark() {
        let started = Instant::now();
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, adaptive_policy());
        let high = DataPoolUsage::new(900, 1_000).expect("high usage is valid");
        let between = DataPoolUsage::new(850, 1_000).expect("middle usage is valid");
        let low = DataPoolUsage::new(800, 1_000).expect("low usage is valid");

        let urgent = scheduler
            .poll(started + Duration::from_secs(2), 2, 0, high)
            .expect("pressure admits after the urgent interval");
        assert_eq!(urgent.mode, OnlineGcRunMode::Urgent);
        assert!(urgent.phases.metadata && urgent.phases.data);
        assert_eq!(
            scheduler.poll(started + Duration::from_secs(3), 3, 0, between),
            None,
            "urgent admission is capped by the urgent interval"
        );
        assert_eq!(
            scheduler
                .poll(started + Duration::from_secs(4), 4, 0, between)
                .expect("pressure stays latched above the low watermark")
                .mode,
            OnlineGcRunMode::Urgent
        );
        scheduler.record_metadata_run(started + Duration::from_secs(4), 0, true);
        scheduler.record_data_run(started + Duration::from_secs(4), 0, true);
        assert_eq!(
            scheduler.status().metadata_wait_seconds(),
            10,
            "forced pressure quanta never extend the adaptive chain"
        );
        assert_eq!(
            scheduler
                .poll(started + Duration::from_secs(15), 5, 0, low)
                .expect("exiting pressure falls back to the Metadata base interval")
                .phases,
            GcPhaseRequest {
                metadata: true,
                data: false,
            }
        );
    }

    #[test]
    fn scheduled_window_caps_only_the_data_phase_start_frequency() {
        let started = Instant::now();
        let policy = adaptive_policy()
            .with_daily_utc_window(
                DailyGcWindow::new(0, 1_440).expect("all-day window is valid"),
                Duration::from_secs(5),
            )
            .expect("nonzero scheduled interval is valid")
            .with_maximum_relocation_workers(
                std::num::NonZeroUsize::new(2).expect("two is nonzero"),
            );
        let low = DataPoolUsage::new(50, 100).expect("low usage is valid");
        let mut scheduler = OnlineGcScheduler::with_policy(started, 1, policy);
        assert_eq!(
            scheduler.poll(started + Duration::from_secs(10), 2, 0, low),
            Some(OnlineGcQuantum {
                mode: OnlineGcRunMode::Background,
                phases: GcPhaseRequest {
                    metadata: true,
                    data: false,
                },
            })
        );
        assert_eq!(scheduler.status().scheduled_admissions(), 0);
        scheduler.record_metadata_run(started + Duration::from_secs(10), 100, false);
        assert_eq!(
            scheduler
                .poll(started + Duration::from_hours(1), 2, 0, low)
                .expect("the data base interval arrived inside the open window")
                .phases,
            GcPhaseRequest {
                metadata: true,
                data: true,
            }
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
