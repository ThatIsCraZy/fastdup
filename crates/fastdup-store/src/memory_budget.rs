use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub(crate) const SYSTEM_REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const UNBOUNDED_SWAP: u64 = u64::MAX;

/// One conservative view of memory available to this process.
///
/// Process Swap, current-cgroup Swap, and Host Swap are separate signals. Only
/// Swap attributed to this process closes cache admission. A production
/// no-Swap promise additionally requires a dedicated cgroup with
/// `memory.swap.max=0`, exposed by [`Self::swap_protection_enforced`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPressureSnapshot {
    effective_limit: u64,
    available: u64,
    process_swap_used: u64,
    host_swap_used: u64,
    cgroup_swap_used: u64,
    cgroup_swap_limit: u64,
}

impl MemoryPressureSnapshot {
    /// Constructs a deterministic process-pressure snapshot.
    #[must_use]
    pub const fn new(
        effective_limit_bytes: u64,
        available_bytes: u64,
        swap_used_bytes: u64,
    ) -> Self {
        Self {
            effective_limit: effective_limit_bytes,
            available: available_bytes,
            process_swap_used: swap_used_bytes,
            host_swap_used: 0,
            cgroup_swap_used: 0,
            cgroup_swap_limit: 0,
        }
    }

    /// Constructs a complete deterministic host/cgroup pressure snapshot.
    #[must_use]
    pub const fn with_swap_state(
        effective_limit_bytes: u64,
        available_bytes: u64,
        process_swap_used_bytes: u64,
        host_swap_used_bytes: u64,
        cgroup_swap_used_bytes: u64,
        cgroup_swap_limit_bytes: Option<u64>,
    ) -> Self {
        Self {
            effective_limit: effective_limit_bytes,
            available: available_bytes,
            process_swap_used: process_swap_used_bytes,
            host_swap_used: host_swap_used_bytes,
            cgroup_swap_used: cgroup_swap_used_bytes,
            cgroup_swap_limit: match cgroup_swap_limit_bytes {
                Some(limit) => limit,
                None => UNBOUNDED_SWAP,
            },
        }
    }

    /// Returns the process-wide governed system snapshot.
    ///
    /// All caches share this sampler. Calls inside the refresh window only
    /// load atomics; at most one caller performs procfs/cgroup I/O per process
    /// refresh interval.
    ///
    /// # Errors
    ///
    /// Returns an error after a failed sample. Callers must fail closed rather
    /// than keep admitting from a stale budget.
    pub fn read_system() -> io::Result<Self> {
        system_memory_budget_governor().snapshot()
    }

    #[must_use]
    pub const fn effective_limit_bytes(self) -> u64 {
        self.effective_limit
    }

    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available
    }

    /// Returns Swap currently charged to this fastdup process.
    #[must_use]
    pub const fn swap_used_bytes(self) -> u64 {
        self.process_swap_used
    }

    #[must_use]
    pub const fn process_swap_used_bytes(self) -> u64 {
        self.process_swap_used
    }

    #[must_use]
    pub const fn host_swap_used_bytes(self) -> u64 {
        self.host_swap_used
    }

    #[must_use]
    pub const fn cgroup_swap_used_bytes(self) -> u64 {
        self.cgroup_swap_used
    }

    /// Returns the finite cgroup Swap limit, or `None` when unbounded/absent.
    #[must_use]
    pub const fn cgroup_swap_limit_bytes(self) -> Option<u64> {
        if self.cgroup_swap_limit == UNBOUNDED_SWAP {
            None
        } else {
            Some(self.cgroup_swap_limit)
        }
    }

    #[must_use]
    pub const fn swap_protection_enforced(self) -> bool {
        self.cgroup_swap_limit == 0
    }
}

trait MemoryBudgetSource: Send + Sync {
    fn sample(&self) -> io::Result<MemoryPressureSnapshot>;
}

#[derive(Debug)]
struct LinuxMemoryBudgetSource;

impl MemoryBudgetSource for LinuxMemoryBudgetSource {
    fn sample(&self) -> io::Result<MemoryPressureSnapshot> {
        sample_linux_memory()
    }
}

/// Process-wide memory-pressure authority shared by all rebuildable caches.
pub struct MemoryBudgetGovernor {
    source: Box<dyn MemoryBudgetSource>,
    refresh_interval: Duration,
    started: Instant,
    last_refresh_millis: AtomicU64,
    refresh_gate: Mutex<()>,
    attempted: AtomicBool,
    valid: AtomicBool,
    effective_limit: AtomicU64,
    available: AtomicU64,
    process_swap_used: AtomicU64,
    host_swap_used: AtomicU64,
    cgroup_swap_used: AtomicU64,
    cgroup_swap_limit: AtomicU64,
    samples: AtomicU64,
    sample_failures: AtomicU64,
}

impl std::fmt::Debug for MemoryBudgetGovernor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryBudgetGovernor")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl MemoryBudgetGovernor {
    fn system() -> Self {
        let governor =
            Self::with_source(Box::new(LinuxMemoryBudgetSource), SYSTEM_REFRESH_INTERVAL);
        {
            let _guard = governor
                .refresh_gate
                .lock()
                .expect("ASSERT: initial memory-governor lock cannot be poisoned");
            governor.refresh_locked();
        }
        governor
    }

    fn with_source(source: Box<dyn MemoryBudgetSource>, refresh_interval: Duration) -> Self {
        Self {
            source,
            refresh_interval,
            started: Instant::now(),
            last_refresh_millis: AtomicU64::new(0),
            refresh_gate: Mutex::new(()),
            attempted: AtomicBool::new(false),
            valid: AtomicBool::new(false),
            effective_limit: AtomicU64::new(0),
            available: AtomicU64::new(0),
            process_swap_used: AtomicU64::new(1),
            host_swap_used: AtomicU64::new(0),
            cgroup_swap_used: AtomicU64::new(1),
            cgroup_swap_limit: AtomicU64::new(UNBOUNDED_SWAP),
            samples: AtomicU64::new(0),
            sample_failures: AtomicU64::new(0),
        }
    }

    /// Returns the latest complete snapshot, refreshing it when due.
    ///
    /// # Errors
    ///
    /// Returns an error when the latest sampling attempt failed. Stale values
    /// are never presented as a current admission budget.
    pub fn snapshot(&self) -> io::Result<MemoryPressureSnapshot> {
        self.refresh_if_due();
        if !self.valid.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "MemoryBudgetGovernor has no current complete system sample",
            ));
        }
        Ok(MemoryPressureSnapshot::with_swap_state(
            self.effective_limit.load(Ordering::Acquire),
            self.available.load(Ordering::Acquire),
            self.process_swap_used.load(Ordering::Acquire),
            self.host_swap_used.load(Ordering::Acquire),
            self.cgroup_swap_used.load(Ordering::Acquire),
            decode_swap_limit(self.cgroup_swap_limit.load(Ordering::Acquire)),
        ))
    }

    /// Rejects a no-Swap promise unless cgroup v2 enforces it.
    ///
    /// # Errors
    ///
    /// Returns an error after a failed sample, when `memory.swap.max` is absent
    /// or unbounded, or when the current cgroup already has charged Swap.
    pub fn require_no_swap(&self) -> io::Result<MemoryPressureSnapshot> {
        let snapshot = self.snapshot()?;
        if !snapshot.swap_protection_enforced() {
            return Err(io::Error::other(
                "fastdup no-Swap policy requires cgroup v2 memory.swap.max=0 (systemd MemorySwapMax=0)",
            ));
        }
        if snapshot.cgroup_swap_used_bytes() != 0 {
            return Err(io::Error::other(
                "fastdup cgroup already has nonzero memory.swap.current",
            ));
        }
        Ok(snapshot)
    }

    #[must_use]
    pub fn status(&self) -> MemoryBudgetGovernorStatus {
        MemoryBudgetGovernorStatus {
            snapshot: self.snapshot().ok(),
            samples: self.samples.load(Ordering::Relaxed),
            sample_failures: self.sample_failures.load(Ordering::Relaxed),
        }
    }

    fn refresh_if_due(&self) {
        let elapsed_millis = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let interval = u64::try_from(self.refresh_interval.as_millis()).unwrap_or(u64::MAX);
        let previous = self.last_refresh_millis.load(Ordering::Relaxed);
        if self.attempted.load(Ordering::Acquire)
            && elapsed_millis.saturating_sub(previous) < interval
        {
            return;
        }
        if self
            .last_refresh_millis
            .compare_exchange(
                previous,
                elapsed_millis,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        let _guard = self
            .refresh_gate
            .lock()
            .expect("ASSERT: memory-governor refresh lock poisoned");
        self.refresh_locked();
    }

    fn refresh_locked(&self) {
        self.attempted.store(true, Ordering::Release);
        if let Ok(snapshot) = self.source.sample() {
            self.effective_limit
                .store(snapshot.effective_limit_bytes(), Ordering::Release);
            self.available
                .store(snapshot.available_bytes(), Ordering::Release);
            self.process_swap_used
                .store(snapshot.process_swap_used_bytes(), Ordering::Release);
            self.host_swap_used
                .store(snapshot.host_swap_used_bytes(), Ordering::Release);
            self.cgroup_swap_used
                .store(snapshot.cgroup_swap_used_bytes(), Ordering::Release);
            self.cgroup_swap_limit.store(
                snapshot.cgroup_swap_limit_bytes().unwrap_or(UNBOUNDED_SWAP),
                Ordering::Release,
            );
            self.valid.store(true, Ordering::Release);
            self.samples.fetch_add(1, Ordering::Relaxed);
        } else {
            self.valid.store(false, Ordering::Release);
            self.sample_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Observable process-wide sampling state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryBudgetGovernorStatus {
    snapshot: Option<MemoryPressureSnapshot>,
    samples: u64,
    sample_failures: u64,
}

impl MemoryBudgetGovernorStatus {
    #[must_use]
    pub const fn snapshot(self) -> Option<MemoryPressureSnapshot> {
        self.snapshot
    }

    #[must_use]
    pub const fn samples(self) -> u64 {
        self.samples
    }

    #[must_use]
    pub const fn sample_failures(self) -> u64 {
        self.sample_failures
    }
}

/// Returns the one governor shared by every system-configured cache.
#[must_use]
pub fn system_memory_budget_governor() -> &'static MemoryBudgetGovernor {
    static GOVERNOR: OnceLock<MemoryBudgetGovernor> = OnceLock::new();
    GOVERNOR.get_or_init(MemoryBudgetGovernor::system)
}

fn decode_swap_limit(encoded: u64) -> Option<u64> {
    (encoded != UNBOUNDED_SWAP).then_some(encoded)
}

fn sample_linux_memory() -> io::Result<MemoryPressureSnapshot> {
    let meminfo = fs::read_to_string("/proc/meminfo")?;
    let process_status = fs::read_to_string("/proc/self/status")?;
    let host_total = meminfo_kib(&meminfo, "MemTotal")?;
    let host_available = meminfo_kib(&meminfo, "MemAvailable")?;
    let swap_total = meminfo_kib(&meminfo, "SwapTotal")?;
    let swap_free = meminfo_kib(&meminfo, "SwapFree")?;
    let mut effective_limit = host_total;
    let mut available = host_available;
    let host_swap_used = swap_total.saturating_sub(swap_free);
    let process_swap_used = status_kib(&process_status, "VmSwap")?;
    let mut cgroup_swap_used = 0;
    let mut cgroup_swap_limit = None;

    if let Some(cgroup) = current_cgroup_v2()? {
        let maximum = read_optional_limit(cgroup.join("memory.max"))?;
        let high = read_optional_limit(cgroup.join("memory.high"))?;
        let current = read_counter(cgroup.join("memory.current"))?;
        let cgroup_limit = [maximum, high].into_iter().flatten().min();
        if let Some(limit) = cgroup_limit {
            effective_limit = effective_limit.min(limit);
            available = available.min(limit.saturating_sub(current));
        }
        cgroup_swap_used = read_optional_counter(cgroup.join("memory.swap.current"))?;
        cgroup_swap_limit = read_optional_existing_limit(cgroup.join("memory.swap.max"))?;
    }

    Ok(MemoryPressureSnapshot::with_swap_state(
        effective_limit,
        available,
        process_swap_used,
        host_swap_used,
        cgroup_swap_used,
        cgroup_swap_limit,
    ))
}

fn status_kib(status: &str, key: &str) -> io::Result<u64> {
    meminfo_kib(status, key)
}

fn meminfo_kib(meminfo: &str, key: &str) -> io::Result<u64> {
    let value = meminfo
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name == key).then_some(value)
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing /proc/meminfo key"))?;
    let kib = value
        .split_ascii_whitespace()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty /proc/meminfo value"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    kib.checked_mul(1_024)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "meminfo byte overflow"))
}

fn current_cgroup_v2() -> io::Result<Option<PathBuf>> {
    let cgroup = fs::read_to_string("/proc/self/cgroup")?;
    let Some(path) = cgroup.lines().find_map(|line| line.strip_prefix("0::")) else {
        return Ok(None);
    };
    let path = PathBuf::from("/sys/fs/cgroup").join(path.trim_start_matches('/'));
    if !path.join("memory.max").is_file() {
        return Ok(None);
    }
    Ok(Some(path))
}

fn read_optional_limit(path: PathBuf) -> io::Result<Option<u64>> {
    let value = fs::read_to_string(path)?;
    let value = value.trim();
    if value == "max" {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn read_optional_existing_limit(path: PathBuf) -> io::Result<Option<u64>> {
    match read_optional_limit(path) {
        Ok(limit) => Ok(limit),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_counter(path: PathBuf) -> io::Result<u64> {
    fs::read_to_string(path)?
        .trim()
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn read_optional_counter(path: PathBuf) -> io::Result<u64> {
    match read_counter(path) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct SequenceSource {
        samples: Mutex<VecDeque<io::Result<MemoryPressureSnapshot>>>,
    }

    impl MemoryBudgetSource for SequenceSource {
        fn sample(&self) -> io::Result<MemoryPressureSnapshot> {
            self.samples
                .lock()
                .expect("fixture source lock")
                .pop_front()
                .expect("fixture supplies a sample")
        }
    }

    #[test]
    fn host_and_shared_cgroup_swap_do_not_impersonate_fastdup_process_swap() {
        let snapshot = MemoryPressureSnapshot::with_swap_state(
            128 << 30,
            96 << 30,
            0,
            12 << 30,
            31 << 20,
            Some(0),
        );

        assert_eq!(snapshot.host_swap_used_bytes(), 12 << 30);
        assert_eq!(snapshot.cgroup_swap_used_bytes(), 31 << 20);
        assert_eq!(snapshot.process_swap_used_bytes(), 0);
        assert_eq!(snapshot.swap_used_bytes(), 0);
        assert!(snapshot.swap_protection_enforced());
    }

    #[test]
    fn failed_refresh_invalidates_stale_admission_budget() {
        let source = SequenceSource {
            samples: Mutex::new(VecDeque::from([
                Ok(MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0)),
                Err(io::Error::other("fixture pressure read failed")),
            ])),
        };
        let governor = MemoryBudgetGovernor::with_source(Box::new(source), Duration::ZERO);

        assert_eq!(
            governor.snapshot().expect("first sample is complete"),
            MemoryPressureSnapshot::new(128 << 30, 96 << 30, 0)
        );
        assert!(governor.snapshot().is_err());
        assert_eq!(governor.sample_failures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn no_swap_promise_requires_a_zero_cgroup_limit() {
        let unprotected = SequenceSource {
            samples: Mutex::new(VecDeque::from([Ok(
                MemoryPressureSnapshot::with_swap_state(128 << 30, 96 << 30, 0, 0, 0, None),
            )])),
        };
        let governor =
            MemoryBudgetGovernor::with_source(Box::new(unprotected), Duration::from_mins(1));

        assert!(governor.require_no_swap().is_err());
    }

    #[test]
    fn failed_samples_are_rate_limited_away_from_cache_hot_paths() {
        let failed = SequenceSource {
            samples: Mutex::new(VecDeque::from([Err(io::Error::other(
                "fixture pressure read failed",
            ))])),
        };
        let governor = MemoryBudgetGovernor::with_source(Box::new(failed), Duration::from_mins(1));

        assert!(governor.snapshot().is_err());
        assert!(governor.snapshot().is_err());
        assert_eq!(governor.sample_failures.load(Ordering::Relaxed), 1);
    }
}
