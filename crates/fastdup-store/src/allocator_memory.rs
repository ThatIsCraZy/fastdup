//! glibc free-arena reclamation, sampled off the storage/cache hot paths.
//! Qualification: docs/benchmarks/allocator-reclaim-2026-09-12.md.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Allocator counters are not cache occupancy. Free arena blocks may already
/// have been discarded from RSS; allocated bytes include all malloc users.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllocatorMemoryStatus {
    pub arena_bytes: u64,
    pub allocated_bytes: u64,
    pub free_bytes: u64,
    pub anonymous_resident_bytes: u64,
    pub trim_attempts: u64,
    pub last_trim_micros: u64,
}
static STATUS: Mutex<Option<AllocatorMemoryStatus>> = Mutex::new(None);
static RUNNING: AtomicBool = AtomicBool::new(false);

/// Returns the last background observation without walking allocator lists.
#[must_use]
pub fn allocator_memory_status() -> Option<AllocatorMemoryStatus> {
    STATUS.lock().ok().and_then(|status| *status)
}

/// One daemon-owned worker. Drop wakes it and waits for the current probe/trim;
/// no callback survives daemon shutdown or touches a durable storage object.
pub struct AllocatorReclaimer {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl AllocatorReclaimer {
    /// Starts housekeeping on glibc/Linux. Other targets have no allocator hook.
    /// # Errors
    /// Returns thread creation failure or an already-running owner.
    pub fn start() -> std::io::Result<Self> {
        if RUNNING.swap(true, Ordering::AcqRel) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "allocator housekeeping already started",
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stop);
        let worker = std::thread::Builder::new()
            .name("allocator-reclaim".into())
            .spawn(move || {
                let mut last_trim: Option<(Instant, Duration)> = None;
                while !signal.load(Ordering::Acquire) {
                    let started = Instant::now();
                    if let Some(mut status) = observe() {
                        let previous = allocator_memory_status().unwrap_or_default();
                        status.trim_attempts = previous.trim_attempts;
                        status.last_trim_micros = previous.last_trim_micros;
                        let pressure = crate::MemoryPressureSnapshot::read_system().ok();
                        let reserve = pressure.map(|p| {
                            crate::cache_budget::cache_memory_reserve(p.effective_limit_bytes())
                        });
                        let ready =
                            last_trim.is_none_or(|(at, cost)| at.elapsed() >= cooldown(cost));
                        if ready && reserve.is_some_and(|reserve| should_reclaim(status, reserve)) {
                            let at = Instant::now();
                            reclaim();
                            let cost = at.elapsed();
                            last_trim = Some((Instant::now(), cost));
                            status = observe().unwrap_or(status);
                            status.trim_attempts = previous.trim_attempts.saturating_add(1);
                            status.last_trim_micros =
                                u64::try_from(cost.as_micros()).unwrap_or(u64::MAX);
                        }
                        if let Ok(mut latest) = STATUS.lock() {
                            *latest = Some(status);
                        }
                    }
                    // Probe/trim cost stretches the next interval: housekeeping does
                    // not busy-loop under allocator contention or memory pressure.
                    std::thread::park_timeout(cooldown(started.elapsed()));
                }
            });
        match worker {
            Ok(worker) => Ok(Self {
                stop,
                worker: Some(worker),
            }),
            Err(error) => {
                RUNNING.store(false, Ordering::Release);
                Err(error)
            }
        }
    }
}
impl Drop for AllocatorReclaimer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            let _ = worker.join();
        }
        RUNNING.store(false, Ordering::Release);
    }
}
fn cooldown(cost: Duration) -> Duration {
    Duration::from_secs(30).max(cost.saturating_mul(100))
}
fn should_reclaim(status: AllocatorMemoryStatus, reserve: u64) -> bool {
    reserve != 0
        && status.free_bytes >= reserve
        && status
            .anonymous_resident_bytes
            .saturating_sub(status.allocated_bytes)
            >= reserve
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
fn observe() -> Option<AllocatorMemoryStatus> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let anonymous = status
        .lines()
        .find_map(|line| line.strip_prefix("RssAnon:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)?;
    // SAFETY: glibc's thread-safe mallinfo2 accepts no pointers, acquires its
    // own arena locks, and returns counters by value. No Rust allocation is
    // borrowed or invalidated. Restrict the ABI to Linux GNU targets.
    let info = unsafe { libc::mallinfo2() };
    Some(AllocatorMemoryStatus {
        arena_bytes: info.arena as u64,
        allocated_bytes: (info.uordblks as u64).saturating_add(info.hblkhd as u64),
        free_bytes: info.fordblks as u64,
        anonymous_resident_bytes: anonymous,
        ..AllocatorMemoryStatus::default()
    })
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn observe() -> Option<AllocatorMemoryStatus> {
    None
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
fn reclaim() {
    // SAFETY: malloc_trim operates only on allocator-owned free pages while
    // holding its internal locks. Zero is a valid pad; live Rust allocations,
    // cache views and foreign malloc users retain their bytes and addresses.
    // No concurrent mallopt/allocator replacement is introduced by this module.
    unsafe {
        libc::malloc_trim(0);
    }
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn reclaim() {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reclaim_requires_free_resident_slack_not_just_a_large_heap() {
        let mut state = AllocatorMemoryStatus {
            allocated_bytes: 300,
            free_bytes: 600,
            anonymous_resident_bytes: 900,
            ..AllocatorMemoryStatus::default()
        };
        assert!(should_reclaim(state, 80));
        state.anonymous_resident_bytes = 320;
        assert!(
            !should_reclaim(state, 80),
            "already discarded free blocks do not trigger another trim"
        );
        state.anonymous_resident_bytes = 900;
        state.free_bytes = 40;
        assert!(
            !should_reclaim(state, 80),
            "live anonymous mappings are not allocator garbage"
        );
        assert!(!should_reclaim(state, 0));
    }
    #[test]
    fn costly_housekeeping_backs_off_and_shutdown_interrupts_the_wait() {
        assert_eq!(
            cooldown(Duration::from_millis(100)),
            Duration::from_secs(30)
        );
        assert_eq!(cooldown(Duration::from_secs(2)), Duration::from_secs(200));
        let started = Instant::now();
        let worker = AllocatorReclaimer::start().unwrap();
        assert!(AllocatorReclaimer::start().is_err());
        drop(worker);
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
