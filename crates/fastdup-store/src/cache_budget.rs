//! Shared byte leases for rebuildable caches. No controller lock is taken on hits.
use crate::MemoryPressureSnapshot;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const PERIOD: Duration = Duration::from_millis(250);

/// Work avoided by reuse: storage access or a work-buffer allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheFallback {
    Data,
    Metadata,
    /// Reusable work buffers avoid allocation, never storage reads.
    Memory,
}

/// Cumulative cache counters, sampled on the cold pressure-refresh path.
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheObservation {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    /// Bytes whose fallback read (or work-buffer allocation) was avoided by hits.
    pub hit_bytes: u64,
    /// Resident payload plus conservatively charged lookup metadata.
    pub resident_bytes: u64,
}

#[derive(Debug)]
struct PoolState {
    name: &'static str,
    fallback: CacheFallback,
    fixed: u64,
    maximum: u64,
    observed: CacheObservation,
    previous: CacheObservation,
    benefit: u128,
    demand: u64,
    desired: u64,
    leased: u64,
}

#[derive(Debug)]
struct State {
    pools: BTreeMap<u64, PoolState>,
    next_id: u64,
    budget: u64,
    refreshed: Instant,
    new_pool: bool,
    effective_limit: u64,
    available: u64,
}

#[derive(Debug)]
struct Broker(Mutex<State>);

/// One cache's participation in the shared memory budget.
///
/// A reduced lease remains charged until `applied` acknowledges local eviction.
/// Pool destruction returns its lease; keep this field after resident storage.
#[derive(Debug)]
pub struct CachePool {
    broker: Arc<Broker>,
    id: u64,
}

impl CachePool {
    /// Registers a system cache. `maximum` describes addressable geometry, not
    /// a preferred allocation. Actual shares follow observed reuse and pressure.
    #[must_use]
    pub fn system(name: &'static str, fallback: CacheFallback, fixed: u64, maximum: u64) -> Self {
        Self::register(Arc::clone(system_broker()), name, fallback, fixed, maximum)
    }

    fn register(
        broker: Arc<Broker>,
        name: &'static str,
        fallback: CacheFallback,
        fixed: u64,
        maximum: u64,
    ) -> Self {
        let id = {
            let mut state = broker.0.lock().expect("ASSERT: cache budget lock poisoned");
            let id = state.next_id;
            state.next_id = id.checked_add(1).expect("ASSERT: cache pool ID exhausted");
            state.new_pool = true;
            state.pools.insert(
                id,
                PoolState {
                    name,
                    fallback,
                    fixed,
                    maximum: maximum.max(fixed),
                    observed: CacheObservation {
                        resident_bytes: fixed,
                        ..CacheObservation::default()
                    },
                    previous: CacheObservation::default(),
                    benefit: 0,
                    demand: 0,
                    desired: fixed,
                    leased: fixed,
                },
            );
            id
        };
        Self { broker, id }
    }

    /// Reports measured reuse and requests a new total byte target. The caller
    /// serializes local admission, applies this target, then calls `applied`.
    ///
    /// # Panics
    /// Panics if an internal controller lock or registration invariant failed.
    #[must_use]
    pub fn target(&self, snapshot: MemoryPressureSnapshot, observation: CacheObservation) -> u64 {
        let mut state = self
            .broker
            .0
            .lock()
            .expect("ASSERT: cache budget lock poisoned");
        state
            .pools
            .get_mut(&self.id)
            .expect("ASSERT: registered cache pool exists")
            .observed = observation;
        if state.new_pool
            || state.refreshed.elapsed() >= PERIOD
            || state.budget == 0
            || snapshot.swap_used_bytes() != 0
            || snapshot.effective_limit_bytes() < state.effective_limit
            || snapshot.available_bytes() < cache_memory_reserve(snapshot.effective_limit_bytes())
        {
            state.rebalance(snapshot);
        }
        let leased = state
            .pools
            .values()
            .fold(0_u64, |sum, pool| sum.saturating_add(pool.leased));
        let free = state.budget.saturating_sub(leased);
        let pool = state
            .pools
            .get_mut(&self.id)
            .expect("ASSERT: registered cache pool exists");
        let target = pool
            .desired
            .min(pool.leased.saturating_add(free))
            .max(pool.fixed);
        pool.leased = pool.leased.max(target);
        target
    }

    /// Releases a donor's unused lease only after its admissions and evictions
    /// have completed under the cache's own lock. Fixed metadata stays charged.
    ///
    /// # Panics
    /// Panics if eviction has not reached the target, a lease was not granted,
    /// or an internal controller lock or registration invariant failed.
    pub fn applied(&self, target: u64, resident: u64) {
        let mut state = self
            .broker
            .0
            .lock()
            .expect("ASSERT: cache budget lock poisoned");
        let pool = state
            .pools
            .get_mut(&self.id)
            .expect("ASSERT: registered cache pool exists");
        assert!(
            resident <= target.max(pool.fixed),
            "ASSERT: cache applied its byte target before releasing the lease"
        );
        assert!(
            target <= pool.leased,
            "ASSERT: cache cannot acknowledge an ungranted lease"
        );
        pool.leased = target.max(pool.fixed);
        pool.observed.resident_bytes = resident;
    }
}

impl Drop for CachePool {
    fn drop(&mut self) {
        if let Ok(mut state) = self.broker.0.lock() {
            state.pools.remove(&self.id);
        }
    }
}

impl Broker {
    fn new() -> Self {
        Self(Mutex::new(State {
            pools: BTreeMap::new(),
            next_id: 0,
            budget: 0,
            refreshed: Instant::now(),
            new_pool: false,
            effective_limit: 0,
            available: 0,
        }))
    }
}

impl State {
    fn rebalance(&mut self, snapshot: MemoryPressureSnapshot) {
        self.refreshed = Instant::now();
        self.new_pool = false;
        self.refresh_headroom(snapshot);
        let fixed = self
            .pools
            .values()
            .fold(0_u64, |sum, pool| sum.saturating_add(pool.fixed));
        let distributable = self.budget.saturating_sub(fixed);
        let count = u64::try_from(
            self.pools
                .values()
                .filter(|pool| pool.fallback != CacheFallback::Memory)
                .count(),
        )
        .unwrap_or(u64::MAX)
        .max(1);
        // Bounded exploration prevents a cold or previously evicted workload
        // from remaining permanently invisible to the hit-based controller.
        let probe = distributable / 16 / count;
        let growth = distributable / 16;
        let mut scores = Vec::with_capacity(self.pools.len());
        for (&id, pool) in &mut self.pools {
            let hits = pool.observed.hits.saturating_sub(pool.previous.hits);
            let misses = pool.observed.misses.saturating_sub(pool.previous.misses);
            let evictions = pool
                .observed
                .evictions
                .saturating_sub(pool.previous.evictions);
            let bytes = pool
                .observed
                .hit_bytes
                .saturating_sub(pool.previous.hit_bytes);
            // EWMA ages idle workloads out; a saturated hit rate alone is not
            // evidence that allocating more RAM would produce further hits.
            pool.benefit = pool.benefit.saturating_mul(3) / 4 + u128::from(bytes);
            pool.demand =
                pool.demand.saturating_mul(3) / 4 + misses.saturating_add(evictions.min(misses));
            pool.previous = pool.observed;
            if pool.fallback == CacheFallback::Memory {
                // Idle scratch gets only capacity left after disk-saving demand.
                pool.desired = pool.fixed;
                continue;
            }
            let weight = match pool.fallback {
                CacheFallback::Data => 16_u128,
                CacheFallback::Metadata => 1,
                CacheFallback::Memory => unreachable!("scratch is served last"),
            };
            let density = pool.benefit.saturating_mul(weight).saturating_mul(1024)
                / u128::from(pool.observed.resident_bytes.max(probe).max(1));
            let score = density.saturating_add(weight * u128::from(hits > 0));
            let wanted = if pool.demand > 0 {
                pool.observed
                    .resident_bytes
                    .saturating_sub(pool.fixed)
                    .saturating_add(growth.max(probe))
            } else if pool.benefit > 0 {
                pool.observed.resident_bytes.saturating_sub(pool.fixed)
            } else {
                probe
            };
            let cap = wanted.min(pool.maximum.saturating_sub(pool.fixed));
            pool.desired = pool.fixed.saturating_add(probe.min(cap));
            scores.push((id, score, cap));
        }
        // Water fill: capped/fully served pools return the unused share, so a
        // small hot metadata cache cannot hoard idle capacity from DATA caches.
        let mut remaining =
            distributable.saturating_sub(self.pools.values().map(|p| p.desired - p.fixed).sum());
        for _ in 0..=self.pools.len() {
            let total = scores
                .iter()
                .filter(|(id, _, cap)| self.pools[id].desired - self.pools[id].fixed < *cap)
                .map(|(_, score, _)| (*score).max(1))
                .sum::<u128>();
            if remaining == 0 || total == 0 {
                break;
            }
            let before = remaining;
            for &(id, score, cap) in &scores {
                let pool = self
                    .pools
                    .get_mut(&id)
                    .expect("ASSERT: scoring pool exists");
                let room = cap.saturating_sub(pool.desired - pool.fixed);
                let share =
                    u64::try_from(u128::from(before) * score.max(1) / total).unwrap_or(u64::MAX);
                let added = room.min(share).min(remaining);
                pool.desired += added;
                remaining -= added;
            }
            if before == remaining {
                break;
            }
        }
        self.distribute_idle_buffers(remaining, probe);
    }

    fn refresh_headroom(&mut self, snapshot: MemoryPressureSnapshot) {
        self.effective_limit = snapshot.effective_limit_bytes();
        self.available = snapshot.available_bytes();
        let resident = self.pools.values().fold(0_u64, |sum, pool| {
            sum.saturating_add(pool.observed.resident_bytes)
        });
        let reserve = cache_memory_reserve(snapshot.effective_limit_bytes());
        self.budget = if snapshot.swap_used_bytes() != 0 {
            0
        } else {
            resident
                .saturating_add(
                    snapshot
                        .available_bytes()
                        .min(snapshot.effective_limit_bytes()),
                )
                .saturating_sub(reserve)
                .min(snapshot.effective_limit_bytes().saturating_sub(reserve))
        };
    }

    fn distribute_idle_buffers(&mut self, mut remaining: u64, probe: u64) {
        for pool in self
            .pools
            .values_mut()
            .filter(|pool| pool.fallback == CacheFallback::Memory)
        {
            let wanted = if pool.demand > 0 || pool.benefit > 0 {
                pool.observed
                    .resident_bytes
                    .saturating_sub(pool.fixed)
                    .saturating_add(probe)
            } else {
                0
            };
            let added = wanted.min(pool.maximum - pool.fixed).min(remaining);
            pool.desired += added;
            remaining -= added;
        }
    }
}

/// Eight percent of effective host/cgroup RAM stays outside rebuildable caches.
#[must_use]
pub const fn cache_memory_reserve(effective: u64) -> u64 {
    effective / 100 * 8 + (effective % 100 * 8).div_ceil(100)
}

fn system_broker() -> &'static Arc<Broker> {
    static BROKER: OnceLock<Arc<Broker>> = OnceLock::new();
    BROKER.get_or_init(|| Arc::new(Broker::new()))
}

/// One pool's last sampled accounting and reuse counters.
#[derive(Clone, Debug)]
pub struct CachePoolStatus {
    pub name: &'static str,
    pub fallback: CacheFallback,
    pub resident_bytes: u64,
    pub target_bytes: u64,
    pub leased_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Sampled memory headroom and all participants in the common budget.
#[derive(Clone, Debug)]
pub struct CacheBudgetStatus {
    pub effective_limit_bytes: u64,
    pub available_bytes: u64,
    pub budget_bytes: u64,
    pub pools: Vec<CachePoolStatus>,
}

/// Current shared-cache leases for diagnostics; never storage authority.
///
/// # Panics
/// Panics if an earlier controller invariant poisoned its lock.
#[must_use]
pub fn cache_budget_status() -> CacheBudgetStatus {
    let state = system_broker()
        .0
        .lock()
        .expect("ASSERT: cache budget lock poisoned");
    CacheBudgetStatus {
        effective_limit_bytes: state.effective_limit,
        available_bytes: state.available,
        budget_bytes: state.budget,
        pools: state
            .pools
            .values()
            .map(|pool| CachePoolStatus {
                name: pool.name,
                fallback: pool.fallback,
                resident_bytes: pool.observed.resident_bytes,
                target_bytes: pool.desired,
                leased_bytes: pool.leased,
                hits: pool.observed.hits,
                misses: pool.observed.misses,
                evictions: pool.observed.evictions,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pressure() -> MemoryPressureSnapshot {
        MemoryPressureSnapshot::new(100_000, 100_000, 0)
    }
    fn pool(broker: &Arc<Broker>, tier: CacheFallback) -> CachePool {
        CachePool::register(Arc::clone(broker), "test", tier, 0, 100_000)
    }
    fn tick(broker: &Broker, snapshot: MemoryPressureSnapshot) {
        broker.0.lock().unwrap().rebalance(snapshot);
    }

    #[test]
    fn idle_buffers_lose_to_data_and_metadata_then_release_their_lease() {
        for fallback in [CacheFallback::Data, CacheFallback::Metadata] {
            let broker = Arc::new(Broker::new());
            let disk = pool(&broker, fallback);
            let buffers = pool(&broker, CacheFallback::Memory);
            let _ = buffers.target(
                pressure(),
                CacheObservation {
                    hits: 10_000,
                    misses: 100,
                    hit_bytes: 10_000,
                    ..CacheObservation::default()
                },
            );
            tick(&broker, pressure());
            let target = buffers.target(
                pressure(),
                CacheObservation {
                    hits: 20_000,
                    misses: 200,
                    hit_bytes: 20_000,
                    ..CacheObservation::default()
                },
            );
            assert!(target > 0, "unused headroom can retain reusable buffers");
            buffers.applied(target, target);
            let _ = disk.target(
                pressure(),
                CacheObservation {
                    hits: 1,
                    misses: 1,
                    hit_bytes: 4096,
                    resident_bytes: 90_000,
                    ..CacheObservation::default()
                },
            );
            let low = MemoryPressureSnapshot::new(100_000, 10_000, 0);
            tick(&broker, low);
            assert_eq!(broker.0.lock().unwrap().pools[&buffers.id].desired, 0);
            assert_eq!(
                broker.0.lock().unwrap().pools[&buffers.id].leased,
                target,
                "donor remains charged until idle buffers are dropped"
            );
            assert_eq!(buffers.target(low, CacheObservation::default()), 0);
            buffers.applied(0, 0);
            assert_eq!(broker.0.lock().unwrap().pools[&buffers.id].leased, 0);
        }
    }
    #[test]
    fn data_reuse_wins_contested_memory_and_total_stays_below_ninety_two_percent() {
        let broker = Arc::new(Broker::new());
        let data = pool(&broker, CacheFallback::Data);
        let meta = pool(&broker, CacheFallback::Metadata);
        for pool in [&data, &meta] {
            let _ = pool.target(
                pressure(),
                CacheObservation {
                    hits: 100,
                    misses: 100,
                    hit_bytes: 1_000_000,
                    resident_bytes: 40_000,
                    ..CacheObservation::default()
                },
            );
        }
        tick(&broker, MemoryPressureSnapshot::new(100_000, 10_000, 0));
        let state = broker.0.lock().unwrap();
        assert!(state.pools[&data.id].desired > state.pools[&meta.id].desired);
        assert!(state.pools.values().map(|p| p.desired).sum::<u64>() <= 92_000);
    }
    #[test]
    fn a_donor_keeps_its_lease_until_eviction_is_acknowledged() {
        let broker = Arc::new(Broker::new());
        let a = pool(&broker, CacheFallback::Data);
        let b = pool(&broker, CacheFallback::Metadata);
        {
            let mut state = broker.0.lock().unwrap();
            state.new_pool = false;
            state.budget = 100;
            state.pools.get_mut(&a.id).unwrap().leased = 100;
            state.pools.get_mut(&a.id).unwrap().desired = 0;
            state.pools.get_mut(&b.id).unwrap().desired = 100;
        }
        assert_eq!(b.target(pressure(), CacheObservation::default()), 0);
        a.applied(0, 0);
        assert_eq!(b.target(pressure(), CacheObservation::default()), 100);
    }
    #[test]
    fn swap_and_external_pressure_revoke_payload_targets() {
        let broker = Arc::new(Broker::new());
        let a = pool(&broker, CacheFallback::Data);
        let t = a.target(pressure(), CacheObservation::default());
        assert!(t > 0);
        a.applied(t, t);
        // New pressure must revoke admission within the normal refresh period.
        let t = a.target(
            MemoryPressureSnapshot::new(100_000, 0, 0),
            CacheObservation {
                resident_bytes: t,
                ..CacheObservation::default()
            },
        );
        assert_eq!(t, 0);
        a.applied(0, 0);
        assert_eq!(
            a.target(
                MemoryPressureSnapshot::new(100_000, 100_000, 1),
                CacheObservation::default()
            ),
            0
        );
    }
    #[test]
    fn workload_turnover_moves_leases_between_pools_without_fixed_partitions() {
        let broker = Arc::new(Broker::new());
        let data = pool(&broker, CacheFallback::Data);
        let meta = pool(&broker, CacheFallback::Metadata);
        let mut first_data = 0;
        for round in 0..120 {
            {
                let mut state = broker.0.lock().unwrap();
                for (id, active) in [(data.id, round < 40), (meta.id, round >= 40)] {
                    let p = state.pools.get_mut(&id).unwrap();
                    if active {
                        p.observed.hits += 1000;
                        p.observed.hit_bytes += 4_096_000;
                        p.observed.misses += 100;
                    }
                }
                let used = state
                    .pools
                    .values()
                    .map(|p| p.observed.resident_bytes)
                    .sum::<u64>();
                state.rebalance(MemoryPressureSnapshot::new(
                    100_000,
                    80_000_u64.saturating_sub(used),
                    0,
                ));
            }
            for p in [&data, &meta] {
                let observation = broker.0.lock().unwrap().pools[&p.id].observed;
                let target = p.target(pressure(), observation);
                p.applied(target, target);
            }
            let state = broker.0.lock().unwrap();
            assert!(state.pools.values().map(|p| p.leased).sum::<u64>() <= 72_000);
            if round == 39 {
                first_data = state.pools[&data.id].leased;
                assert!(first_data > state.pools[&meta.id].leased);
            }
            if round == 119 {
                assert!(state.pools[&meta.id].leased > state.pools[&data.id].leased);
                assert!(state.pools[&data.id].leased < first_data / 2);
            }
        }
    }

    #[test]
    fn concurrent_borrowers_never_spend_the_same_lease() {
        let broker = Arc::new(Broker::new());
        let pools: Vec<_> = (0..8).map(|_| pool(&broker, CacheFallback::Data)).collect();
        let barrier = std::sync::Barrier::new(pools.len());
        std::thread::scope(|scope| {
            for pool in &pools {
                let barrier = &barrier;
                let broker = &broker;
                scope.spawn(move || {
                    let mut observation = CacheObservation::default();
                    for _ in 0..100 {
                        observation.hits += 100;
                        observation.misses += 10;
                        observation.hit_bytes += 100_000;
                        let target = pool.target(pressure(), observation);
                        barrier.wait();
                        {
                            let state = broker.0.lock().unwrap();
                            assert!(
                                state.pools.values().map(|p| p.leased).sum::<u64>() <= state.budget
                            );
                        }
                        pool.applied(target, target);
                        observation.resident_bytes = target;
                        barrier.wait();
                    }
                });
            }
        });
    }

    #[test]
    fn pool_destruction_and_cold_exploration_do_not_lose_or_duplicate_budget() {
        let broker = Arc::new(Broker::new());
        let first = pool(&broker, CacheFallback::Data);
        let target = first.target(pressure(), CacheObservation::default());
        assert!(target > 0);
        first.applied(target, target);
        let cold = pool(&broker, CacheFallback::Metadata);
        tick(&broker, pressure());
        let cold_target = cold.target(pressure(), CacheObservation::default());
        assert!(cold_target > 0, "a zero-hit pool can explore");
        drop(first);
        let state = broker.0.lock().unwrap();
        assert_eq!(state.pools.len(), 1);
        assert_eq!(state.pools[&cold.id].leased, cold_target);
    }
}
