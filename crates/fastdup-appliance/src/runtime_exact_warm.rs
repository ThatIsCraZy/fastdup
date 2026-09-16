//! Bounded proactive warming of reclaimable Exact-acceleration cache entries.

use super::runtime_scrub;
use super::runtime_telemetry;
use fastdup_posix::Namespace;
use fastdup_store::{
    ExactCacheWarmPolicy, ExactIndexPageCacheStatus, ExactIndexRunRepository,
    ExactRunMembershipStatus, FsStorageIo, MaintenanceCancellation, MetadataReadReason,
    MetadataReadScope, set_background_io_priority,
};

type FsExactIndexRepository = ExactIndexRunRepository<FsStorageIo>;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const BASE_INTERVAL: Duration = Duration::from_secs(30);
const GATE_INTERVAL: Duration = Duration::from_secs(5);
const IDLE_AFTER: Duration = Duration::from_secs(5);
const SATURATED_INTERVAL: Duration = Duration::from_mins(5);
const REJECTION_INTERVAL: Duration = Duration::from_mins(10);
const EXACT_WARM_ENABLED_ENV: &str = "FASTDUP_EXACT_CACHE_WARM";
const EXACT_WARM_INTERVAL_SECONDS_ENV: &str = "FASTDUP_EXACT_CACHE_WARM_INTERVAL_SECONDS";
const EXACT_WARM_PAGE_BUDGET_ENV: &str = "FASTDUP_EXACT_CACHE_WARM_PAGE_BUDGET";
const EXACT_WARM_STRUCTURE_BUDGET_ENV: &str = "FASTDUP_EXACT_CACHE_WARM_STRUCTURE_BUDGET";
const EXACT_WARM_RUN_PAGE_BUDGET_ENV: &str = "FASTDUP_EXACT_CACHE_WARM_RUN_PAGE_BUDGET";
const EXACT_WARM_IDLE_AFTER_SECONDS_ENV: &str = "FASTDUP_EXACT_CACHE_WARM_IDLE_AFTER_SECONDS";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactCacheWarmRuntimePolicy {
    enabled: bool,
    base_interval: Duration,
    gate_interval: Duration,
    idle_after: Duration,
    warm_policy: ExactCacheWarmPolicy,
}

impl Default for ExactCacheWarmRuntimePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            base_interval: BASE_INTERVAL,
            gate_interval: GATE_INTERVAL,
            idle_after: IDLE_AFTER,
            warm_policy: ExactCacheWarmPolicy::default(),
        }
    }
}

impl ExactCacheWarmRuntimePolicy {
    #[must_use]
    pub const fn enabled(self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn base_interval(self) -> Duration {
        self.base_interval
    }

    #[must_use]
    pub const fn idle_after(self) -> Duration {
        self.idle_after
    }

    #[must_use]
    pub const fn warm_policy(self) -> ExactCacheWarmPolicy {
        self.warm_policy
    }

    pub fn from_environment() -> Result<Self, String> {
        let mut policy = Self::default();
        if let Some(enabled) = environment_bool(EXACT_WARM_ENABLED_ENV)? {
            policy.enabled = enabled;
        }
        if let Some(seconds) = environment_seconds(EXACT_WARM_INTERVAL_SECONDS_ENV)? {
            if seconds.is_zero() {
                return Err(format!("{EXACT_WARM_INTERVAL_SECONDS_ENV} must be nonzero"));
            }
            policy.base_interval = seconds;
        }
        if let Some(seconds) = environment_seconds(EXACT_WARM_IDLE_AFTER_SECONDS_ENV)? {
            if seconds.is_zero() {
                return Err(format!(
                    "{EXACT_WARM_IDLE_AFTER_SECONDS_ENV} must be nonzero"
                ));
            }
            policy.idle_after = seconds;
        }
        if let Some(pages) = environment_usize(EXACT_WARM_PAGE_BUDGET_ENV)? {
            policy.warm_policy.maximum_pages = pages;
        }
        if let Some(structures) = environment_usize(EXACT_WARM_STRUCTURE_BUDGET_ENV)? {
            policy.warm_policy.maximum_structures = structures;
        }
        if let Some(run_pages) = environment_usize(EXACT_WARM_RUN_PAGE_BUDGET_ENV)? {
            policy.warm_policy.maximum_run_pages = run_pages;
        }
        Ok(policy)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WarmAction {
    Skipped(&'static str),
    Warmed,
    Cancelled,
}

pub struct ExactWarmRuntimeHandle {
    shutdown: watch::Sender<bool>,
    cancellation: MaintenanceCancellation,
    worker: JoinHandle<Result<(), String>>,
}

impl ExactWarmRuntimeHandle {
    pub fn request_stop(&self) {
        self.cancellation.cancel();
        let _ = self.shutdown.send(true);
    }

    pub async fn stop(self) -> Result<(), String> {
        self.request_stop();
        self.worker
            .await
            .map_err(|error| format!("Exact cache warmer join failed: {error}"))?
    }
}

pub fn emit_policy(policy: ExactCacheWarmRuntimePolicy) {
    let warm = policy.warm_policy();
    eprintln!(
        concat!(
            "exact_cache_warm_policy enabled={} interval_seconds={} idle_after_seconds={} ",
            "page_budget={} structure_budget={} run_page_budget={}"
        ),
        policy.enabled(),
        policy.base_interval().as_secs(),
        policy.idle_after().as_secs(),
        warm.maximum_pages,
        warm.maximum_structures,
        warm.maximum_run_pages,
    );
}

#[allow(clippy::too_many_arguments)]
pub fn start(
    repository: FsExactIndexRepository,
    policy: ExactCacheWarmRuntimePolicy,
    frontend_operations: Arc<dyn Fn() -> u64 + Send + Sync>,
    namespace: Arc<Namespace>,
    scrub_gate: runtime_scrub::ScrubGate,
) -> ExactWarmRuntimeHandle {
    let (shutdown, shutdown_rx) = watch::channel(false);
    let cancellation = MaintenanceCancellation::new();
    let worker = tokio::spawn(run(
        repository,
        policy,
        shutdown_rx,
        cancellation.clone(),
        frontend_operations,
        namespace,
        scrub_gate,
    ));
    ExactWarmRuntimeHandle {
        shutdown,
        cancellation,
        worker,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    repository: FsExactIndexRepository,
    policy: ExactCacheWarmRuntimePolicy,
    mut shutdown: watch::Receiver<bool>,
    cancellation: MaintenanceCancellation,
    frontend_operations: Arc<dyn Fn() -> u64 + Send + Sync>,
    namespace: Arc<Namespace>,
    scrub_gate: runtime_scrub::ScrubGate,
) -> Result<(), String> {
    if !policy.enabled {
        return Ok(());
    }
    let mut observer = WarmObserver::new(policy, (frontend_operations)());
    let mut next = policy.base_interval;
    loop {
        let mut delay = Box::pin(sleep(next));
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            () = &mut delay => {
                let now = Instant::now();
                let operations = (frontend_operations)();
                let membership = repository
                    .pin_active_generation()
                    .map(|generation| generation.membership_status())
                    .unwrap_or_default();
                let cycle = observer.observe(
                    repository.page_cache_status(),
                    membership,
                    operations,
                    now,
                    &namespace,
                    scrub_gate.permits_gc(),
                );                let mut record_action = cycle.action;
                next = match cycle.action {
                    WarmAction::Skipped(_) => cycle.interval,
                    WarmAction::Warmed | WarmAction::Cancelled => {
                        if cancellation.check().is_err() {
                            return Ok(());
                        }
                        let repository = repository.clone();
                        let warm_policy = policy.warm_policy;
                        let cancellation = cancellation.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            let _background = set_background_io_priority();
                            let _reads = MetadataReadScope::enter(MetadataReadReason::IndexLookup);
                            repository.warm_active_generation(&warm_policy, Some(&cancellation))
                        })
                        .await;
                        let mut next_interval = cycle.interval;
                        match result {
                            Ok(Ok(progress)) => {
                                if progress.pages_rejected != 0 {
                                    next_interval = REJECTION_INTERVAL;
                                }
                                eprintln!(
                                    concat!(
                                        "exact_cache_warm ok=true active_runs={} total_pages={} ",
                                        "structures_requested={} structures_built={} ",
                                        "pages_skipped_resident={} pages_warmed={} pages_rejected={} ",
                                        "interval_seconds={}"
                                    ),
                                    progress.active_runs,
                                    progress.total_pages,
                                    progress.structures_requested,
                                    progress.structures_built,
                                    progress.pages_skipped_resident,
                                    progress.pages_warmed,
                                    progress.pages_rejected,
                                    next_interval.as_secs(),
                                );
                            }
                            Ok(Err(error)) if error.is_cancelled() => {
                                record_status(WarmAction::Cancelled);
                                return Ok(());
                            }
                            Ok(Err(error)) => {
                                eprintln!("exact_cache_warm_error={error}");
                                record_action = WarmAction::Skipped("warm-error");
                                next_interval = policy.gate_interval;
                            }
                            Err(error) => {
                                eprintln!("exact_cache_warm_worker_join_error={error}");
                                record_action = WarmAction::Skipped("warm-worker-error");
                                next_interval = policy.gate_interval;
                            }
                        }
                        next_interval
                    }
                };
                record_status(record_action);
            }
        }
    }
}

struct WarmObserver {
    policy: ExactCacheWarmRuntimePolicy,
    previous: Option<WarmSample>,
    frontend_operations: u64,
    frontend_activity_at: Instant,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WarmSample {
    misses: u64,
    evictions: u64,
    pressure_rejections: u64,
    hits: u64,
    target_pages: u64,
    protected_limit_bytes: u64,
    protected_resident_bytes: u64,
    missing_filters: u64,
    missing_bounds: u64,
}

impl WarmSample {
    fn observe(exact: ExactIndexPageCacheStatus, membership: ExactRunMembershipStatus) -> Self {
        Self {
            misses: exact.misses(),
            evictions: exact.evictions(),
            pressure_rejections: exact.pressure_rejections(),
            hits: exact.hits(),
            target_pages: exact.target_pages(),
            protected_limit_bytes: exact.protected_limit_bytes(),
            protected_resident_bytes: exact.protected_resident_bytes(),
            missing_filters: membership.missing_filter_count(),
            missing_bounds: membership.missing_page_bounds_count(),
        }
    }

    fn protected_budget_is_saturated(self) -> bool {
        self.protected_resident_bytes.saturating_mul(10)
            >= self.protected_limit_bytes.saturating_mul(9)
    }

    fn hit_rate_basis_points(self) -> u64 {
        let total = self.hits.saturating_add(self.misses);
        self.hits
            .saturating_mul(10_000)
            .checked_div(total)
            .unwrap_or(0)
    }

    const fn demand_activity(self, previous: Self) -> u64 {
        self.misses
            .saturating_sub(previous.misses)
            .saturating_add(self.evictions.saturating_sub(previous.evictions))
    }

    const fn pressure_rejection_delta(self, previous: Self) -> u64 {
        self.pressure_rejections
            .saturating_sub(previous.pressure_rejections)
    }
}

struct CycleDecision {
    action: WarmAction,
    interval: Duration,
}

impl WarmObserver {
    fn new(policy: ExactCacheWarmRuntimePolicy, frontend_operations: u64) -> Self {
        Self::new_at(policy, frontend_operations, Instant::now())
    }

    fn new_at(policy: ExactCacheWarmRuntimePolicy, frontend_operations: u64, now: Instant) -> Self {
        Self {
            policy,
            previous: None,
            frontend_operations,
            frontend_activity_at: now,
        }
    }

    fn observe(
        &mut self,
        exact: ExactIndexPageCacheStatus,
        membership: ExactRunMembershipStatus,
        operations: u64,
        now: Instant,
        namespace: &Namespace,
        scrub_complete: bool,
    ) -> CycleDecision {
        self.observe_sample(
            WarmSample::observe(exact, membership),
            operations,
            now,
            namespace.mutation_admission_open(),
            scrub_complete,
        )
    }

    fn observe_sample(
        &mut self,
        current: WarmSample,
        operations: u64,
        now: Instant,
        mutation_admission_open: bool,
        scrub_complete: bool,
    ) -> CycleDecision {
        if operations != self.frontend_operations {
            self.frontend_operations = operations;
            self.frontend_activity_at = now;
        }
        let action = if !scrub_complete {
            WarmAction::Skipped("scrub-incomplete")
        } else if !mutation_admission_open {
            WarmAction::Skipped("mutation-admission-closed")
        } else if now.saturating_duration_since(self.frontend_activity_at) < self.policy.idle_after
        {
            WarmAction::Skipped("frontend-active")
        } else if current.target_pages == 0 || current.protected_limit_bytes == 0 {
            WarmAction::Skipped("cache-budget-closed")
        } else if current.protected_budget_is_saturated()
            && current.missing_filters == 0
            && current.missing_bounds == 0
            && current.hit_rate_basis_points() >= 9_900
        {
            WarmAction::Skipped("acceleration-saturated")
        } else if self
            .previous
            .is_some_and(|previous| current.demand_activity(previous) == 0)
            && current.missing_filters == 0
            && current.missing_bounds == 0
        {
            WarmAction::Skipped("no-new-demand")
        } else {
            WarmAction::Warmed
        };
        let interval = if matches!(action, WarmAction::Warmed | WarmAction::Cancelled)
            && self
                .previous
                .is_some_and(|previous| current.pressure_rejection_delta(previous) > 0)
        {
            REJECTION_INTERVAL
        } else {
            match action {
                WarmAction::Skipped(reason) => match reason {
                    "frontend-active" => self.policy.idle_after,
                    "acceleration-saturated" => self.policy.base_interval.max(SATURATED_INTERVAL),
                    "no-new-demand" => self.policy.base_interval,
                    _ => self.policy.gate_interval,
                },
                WarmAction::Warmed | WarmAction::Cancelled => self.policy.base_interval,
            }
        };
        self.previous = Some(current);
        CycleDecision { action, interval }
    }
}

fn record_status(action: WarmAction) {
    runtime_telemetry::record_exact_warm(serde_json::json!({
        "state": match action {
            WarmAction::Skipped(reason) => reason,
            WarmAction::Warmed => "warmed",
            WarmAction::Cancelled => "cancelled",
        },
    }));
}

fn environment_bool(name: &str) -> Result<Option<bool>, String> {
    std::env::var_os(name)
        .map(|value| {
            let value = value
                .into_string()
                .map_err(|_| format!("{name} must be ASCII"))?;
            match value.as_str() {
                "0" | "off" | "false" => Ok(false),
                "1" | "on" | "true" => Ok(true),
                _ => Err(format!("{name} must be 0, 1, on, off, true, or false")),
            }
        })
        .transpose()
}

fn environment_seconds(name: &str) -> Result<Option<Duration>, String> {
    environment_u64(name).map(|seconds| seconds.map(Duration::from_secs))
}

fn environment_u64(name: &str) -> Result<Option<u64>, String> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("{name} must be ASCII"))?
                .parse()
                .map_err(|_| format!("{name} must be a number"))
        })
        .transpose()
}

fn environment_usize(name: &str) -> Result<Option<usize>, String> {
    environment_u64(name)?
        .map(|value| {
            usize::try_from(value).map_err(|_| format!("{name} exceeds the supported range"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WarmSample {
        WarmSample {
            misses: 1,
            evictions: 0,
            pressure_rejections: 0,
            hits: 99,
            target_pages: 200,
            protected_limit_bytes: 1 << 20,
            protected_resident_bytes: 1 << 18,
            missing_filters: 0,
            missing_bounds: 0,
        }
    }

    fn quiet_observer(now: Instant) -> WarmObserver {
        let policy = ExactCacheWarmRuntimePolicy::default();
        let quiet = now
            .checked_sub(policy.idle_after + Duration::from_millis(1))
            .expect("test instant is far enough from process start");
        WarmObserver::new_at(policy, 0, quiet)
    }

    #[test]
    fn closed_budget_skips_proactive_warming() {
        let now = Instant::now();
        let mut observer = quiet_observer(now);
        let decision = observer.observe_sample(WarmSample::default(), 0, now, true, true);
        assert_eq!(decision.action, WarmAction::Skipped("cache-budget-closed"));
    }

    #[test]
    fn frontend_activity_defers_proactive_warming() {
        let now = Instant::now();
        let mut observer = WarmObserver::new_at(ExactCacheWarmRuntimePolicy::default(), 1, now);
        let decision = observer.observe_sample(sample(), 2, now, true, true);
        assert_eq!(decision.action, WarmAction::Skipped("frontend-active"));
    }

    #[test]
    fn online_gc_does_not_prevent_proactive_reads() {
        let now = Instant::now();
        let mut observer = quiet_observer(now);
        let decision = observer.observe_sample(sample(), 0, now, true, true);
        assert_eq!(decision.action, WarmAction::Warmed);
    }

    #[test]
    fn miss_growth_accepts_a_warm_cycle() {
        let now = Instant::now();
        let mut observer = quiet_observer(now);
        let first = sample();
        observer.observe_sample(first, 0, now, true, true);
        let mut second = first;
        second.misses += 1;
        let decision = observer.observe_sample(second, 0, now, true, true);
        assert_eq!(decision.action, WarmAction::Warmed);
    }

    #[test]
    fn saturated_cache_with_no_new_demand_waits() {
        let now = Instant::now();
        let mut observer = quiet_observer(now);
        let first = WarmSample {
            misses: 100,
            hits: 9_900,
            protected_resident_bytes: (1 << 20) * 19 / 20,
            ..sample()
        };
        observer.observe_sample(first, 0, now, true, true);
        let decision = observer.observe_sample(first, 0, now, true, true);
        assert_eq!(
            decision.action,
            WarmAction::Skipped("acceleration-saturated")
        );
    }

    #[test]
    fn pressure_rejection_backs_off() {
        let now = Instant::now();
        let mut observer = quiet_observer(now);
        let first = sample();
        observer.observe_sample(first, 0, now, true, true);
        let mut second = first;
        second.misses += 1;
        second.pressure_rejections = 1;
        let decision = observer.observe_sample(second, 0, now, true, true);
        assert_eq!(decision.action, WarmAction::Warmed);
        assert_eq!(decision.interval, REJECTION_INTERVAL);
    }
}
