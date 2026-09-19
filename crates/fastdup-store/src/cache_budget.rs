//! Shared byte leases for rebuildable caches. No controller lock is taken on hits.
use crate::MemoryPressureSnapshot;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const PERIOD: Duration = Duration::from_millis(250);
const MIB: u64 = 1 << 20;
/// A candidate outside its deadband must persist for this many periods before
/// stable targets move. This removes one-sample memory-headroom oscillation.
const TARGET_STREAK_TRIGGER: i8 = 4;

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
    pinned: u64,
    target_drift: i8,
}

#[derive(Debug)]
struct State {
    pools: BTreeMap<u64, PoolState>,
    next_id: u64,
    budget: u64,
    hard_pressure: bool,
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
                    pinned: 0,
                    target_drift: 0,
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
        self.target_with_pinned_floor(snapshot, observation, 0)
    }

    /// Requests an evictable byte target while charging a non-evictable resident
    /// floor against the shared headroom. The returned target excludes `pinned`
    /// so pressure cannot admit more evictable capacity merely to reserve room
    /// for a pinned resident set.
    ///
    /// # Panics
    /// Panics if an internal controller lock or registration invariant failed.
    #[must_use]
    pub fn target_with_pinned_floor(
        &self,
        snapshot: MemoryPressureSnapshot,
        observation: CacheObservation,
        pinned: u64,
    ) -> u64 {
        let mut state = self
            .broker
            .0
            .lock()
            .expect("ASSERT: cache budget lock poisoned");
        let prior_pinned = state
            .pools
            .get(&self.id)
            .expect("ASSERT: registered cache pool exists")
            .pinned;
        {
            let pool = state
                .pools
                .get_mut(&self.id)
                .expect("ASSERT: registered cache pool exists");
            pool.observed = observation;
            pool.pinned = pinned;
        }
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
        let leased = if pinned >= prior_pinned {
            leased.saturating_add(pinned - prior_pinned)
        } else {
            leased.saturating_sub(prior_pinned - pinned)
        };
        let free = state.budget.saturating_sub(leased);
        let pool = state
            .pools
            .get_mut(&self.id)
            .expect("ASSERT: registered cache pool exists");
        let desired = pool.desired.saturating_sub(pinned).max(pool.fixed);
        let granted = pool.leased.saturating_sub(prior_pinned);
        let target = desired.min(granted.saturating_add(free)).max(pool.fixed);
        pool.leased = pool.leased.max(target.saturating_add(pinned));
        target
    }

    /// Releases a donor's unused lease only after its admissions and evictions
    /// have completed under the cache's own lock. Fixed metadata stays charged.
    ///
    /// # Panics
    /// Panics if eviction has not reached the target, a lease was not granted,
    /// or an internal controller lock or registration invariant failed.
    pub fn applied(&self, target: u64, resident: u64) {
        self.applied_with_pinned_floor(target, resident, 0);
    }

    /// Acknowledges the evictable portion of a target and replaces the charged
    /// non-evictable resident floor. `resident` must exclude `pinned`; total
    /// headroom accounting adds it back after the lease is reduced.
    ///
    /// # Panics
    /// Panics if eviction has not reached the evictable target, a lease was not
    /// granted, or an internal controller lock or registration invariant failed.
    pub fn applied_with_pinned_floor(&self, target: u64, resident: u64, pinned: u64) {
        let mut state = self
            .broker
            .0
            .lock()
            .expect("ASSERT: cache budget lock poisoned");
        let pool = state
            .pools
            .get_mut(&self.id)
            .expect("ASSERT: registered cache pool exists");
        let prior_pinned = pool.pinned;
        assert!(
            resident <= target.max(pool.fixed),
            "ASSERT: cache applied its byte target before releasing the lease"
        );
        assert!(
            target <= pool.leased.saturating_sub(prior_pinned),
            "ASSERT: cache cannot acknowledge an ungranted lease"
        );
        pool.pinned = pinned;
        pool.leased = target.max(pool.fixed).saturating_add(pinned);
        pool.observed.resident_bytes = resident.saturating_add(pinned);
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
            hard_pressure: true,
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
        let hard_pressure = self.hard_pressure;
        let old: BTreeMap<u64, u64> = self
            .pools
            .iter()
            .map(|(id, pool)| (*id, pool.desired))
            .collect();
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
        let probe = bounded_share(distributable, 12, 32 * MIB, 128 * MIB)
            .saturating_div(count)
            .max(u64::from(distributable > 0));
        let growth = bounded_share(distributable, 25, 16 * MIB, 128 * MIB);
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
            // EWMA ages idle workloads out of competition for memory; a
            // saturated hit rate alone is not evidence for further growth.
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
        let remaining = self.water_fill_scores(&scores, distributable);
        let remaining = self.retain_uncontested_disk_targets(&old, remaining);
        self.distribute_idle_buffers(remaining, probe);
        let step = bounded_share(distributable, 30, 8 * MIB, 128 * MIB);
        let deadband = bounded_share(distributable, 20, 8 * MIB, 32 * MIB);
        self.damp_targets(&old, step, deadband, hard_pressure);
        self.retain_target_budget(fixed, distributable);
    }

    fn retain_uncontested_disk_targets(&mut self, old: &BTreeMap<u64, u64>, mut free: u64) -> u64 {
        if self.hard_pressure {
            return free;
        }
        let retention = self.pools.iter().fold(0_u64, |total, (id, pool)| {
            if pool.fallback == CacheFallback::Memory {
                return total;
            }
            let previous = old.get(id).copied().unwrap_or(pool.fixed).min(pool.maximum);
            total.saturating_add(previous.saturating_sub(pool.desired))
        });
        if retention > free {
            // Targets now compete: keep the ordinary score-based allocation
            // and damping, without bias toward a former workload's lease.
            return free;
        }
        // Serve measured disk-saving demand first, then retain already granted
        // disk-cache capacity from otherwise unused headroom. Shrinking merely
        // because hits/misses stopped destroys the next job's warm working set;
        // even a hit-only phase would shrink class ceilings below residency.
        // This neither grows an idle target nor protects it from a competing
        // pool or real memory pressure. Scratch buffers still receive leftovers.
        for (&id, pool) in &mut self.pools {
            if pool.fallback == CacheFallback::Memory {
                continue;
            }
            let previous = old
                .get(&id)
                .copied()
                .unwrap_or(pool.fixed)
                .min(pool.maximum);
            let retained = previous.saturating_sub(pool.desired).min(free);
            pool.desired += retained;
            free -= retained;
        }
        free
    }

    fn water_fill_scores(&mut self, scores: &[(u64, u128, u64)], distributable: u64) -> u64 {
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
            for &(id, score, cap) in scores {
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
        remaining
    }

    fn damp_targets(
        &mut self,
        old: &BTreeMap<u64, u64>,
        step: u64,
        deadband: u64,
        hard_pressure: bool,
    ) {
        let candidate: BTreeMap<u64, u64> = self
            .pools
            .iter()
            .map(|(id, pool)| (*id, pool.desired))
            .collect();
        for (&id, pool) in &mut self.pools {
            let old = old.get(&id).copied().unwrap_or(pool.fixed);
            let candidate = candidate.get(&id).copied().unwrap_or(pool.fixed);
            if hard_pressure {
                pool.desired = old.min(candidate);
                pool.target_drift = 0;
                continue;
            }
            if old == pool.fixed && candidate > pool.fixed {
                pool.desired = candidate;
                pool.target_drift = 0;
                continue;
            }
            let deadband = deadband.min(old.saturating_sub(pool.fixed) / 4).max(1);
            let direction = i32::from(candidate > old.saturating_add(deadband))
                - i32::from(candidate.saturating_add(deadband) <= old);
            let drift = if direction == 0 {
                0
            } else if direction > 0 {
                if pool.target_drift > 0 {
                    pool.target_drift.saturating_add(1)
                } else {
                    1
                }
            } else if pool.target_drift < 0 {
                pool.target_drift.saturating_sub(1)
            } else {
                -1
            };
            pool.target_drift = drift;
            if drift.abs() >= TARGET_STREAK_TRIGGER {
                let difference = candidate.abs_diff(old);
                let movement = step.max(difference / 8).min(difference);
                pool.desired = if candidate > old {
                    old.saturating_add(movement)
                } else {
                    old.saturating_sub(movement)
                };
            } else {
                pool.desired = old;
            }
        }
    }

    fn refresh_headroom(&mut self, snapshot: MemoryPressureSnapshot) {
        let previous_effective_limit = self.effective_limit;
        self.effective_limit = snapshot.effective_limit_bytes();
        self.available = snapshot.available_bytes();
        let resident = self.pools.values().fold(0_u64, |sum, pool| {
            sum.saturating_add(pool.observed.resident_bytes)
        });
        let reserve = cache_memory_reserve(snapshot.effective_limit_bytes());
        let raw = if snapshot.swap_used_bytes() != 0 {
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
        self.hard_pressure = snapshot.swap_used_bytes() != 0
            || snapshot.available_bytes() < reserve
            || (previous_effective_limit != 0
                && snapshot.effective_limit_bytes() < previous_effective_limit)
            || raw == 0;
        self.budget = if raw == 0 || self.hard_pressure || self.budget == 0 {
            raw
        } else {
            let smoothed = if raw < self.budget {
                self.budget.saturating_sub((self.budget - raw).div_ceil(4))
            } else {
                self.budget.saturating_add((raw - self.budget) / 8)
            };
            smoothed.min(self.effective_limit.saturating_sub(reserve))
        };
    }

    fn retain_target_budget(&mut self, fixed: u64, distributable: u64) {
        let total = self
            .pools
            .values()
            .fold(0_u64, |sum, pool| sum.saturating_add(pool.desired));
        if total <= distributable.saturating_add(fixed) {
            return;
        }
        let variable_total = total.saturating_sub(fixed);
        if variable_total == 0 {
            return;
        }
        let mut excess = total - distributable.saturating_add(fixed);
        let variables: Vec<(u64, u64)> = self
            .pools
            .iter()
            .map(|(id, pool)| (*id, pool.desired.saturating_sub(pool.fixed)))
            .collect();
        for (id, variable) in &variables {
            let reduction = u64::try_from(
                u128::from(excess) * u128::from(*variable) / u128::from(variable_total.max(1)),
            )
            .unwrap_or(u64::MAX)
            .min(*variable);
            if reduction == 0 {
                continue;
            }
            if let Some(pool) = self.pools.get_mut(id) {
                pool.desired = pool.desired.saturating_sub(reduction);
            }
            excess = excess.saturating_sub(reduction);
        }
        for pool in self.pools.values_mut() {
            if excess == 0 {
                break;
            }
            let reduction = excess.min(pool.desired.saturating_sub(pool.fixed));
            pool.desired = pool.desired.saturating_sub(reduction);
            excess = excess.saturating_sub(reduction);
        }
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

fn bounded_share(budget: u64, basis_points: u64, minimum: u64, maximum: u64) -> u64 {
    if budget == 0 {
        return 0;
    }
    let scaled = budget.saturating_mul(basis_points) / 10_000;
    let floor = minimum.min(scaled.max(1));
    let ceiling = maximum.min(budget);
    scaled.max(floor).min(ceiling)
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
    fn warm_disk_cache_keeps_its_target_while_headroom_is_uncontested() {
        for active in [false, true] {
            let broker = Arc::new(Broker::new());
            let cache = CachePool::register(
                Arc::clone(&broker),
                "unified",
                CacheFallback::Data,
                MIB,
                16_000 * MIB,
            );
            let snapshot = MemoryPressureSnapshot::new(24_000 * MIB, 20_000 * MIB, 0);
            let warm_target = 512 * MIB;
            let resident = 300 * MIB;
            {
                let mut state = broker.0.lock().unwrap();
                let pool = state.pools.get_mut(&cache.id).unwrap();
                pool.desired = warm_target;
                pool.leased = warm_target;
                pool.observed.resident_bytes = resident;
                pool.observed.hits = 1_000;
                pool.observed.hit_bytes = 100 * MIB;
            }
            // Status sampling continues between jobs. Hit-only phases also
            // must not shrink the target down to residency: class ceilings
            // would then evict useful entries and manufacture fresh misses.
            for _ in 0..200 {
                let mut observation = broker.0.lock().unwrap().pools[&cache.id].observed;
                if active {
                    observation.hits += 100;
                    observation.hit_bytes += MIB;
                }
                let _ = cache.target(snapshot, observation);
                tick(&broker, snapshot);
                let target = cache.target(snapshot, observation);
                assert!(
                    target >= warm_target,
                    "uncontested warm target shrank: {target}"
                );
                cache.applied(target, resident);
            }
            let target = cache.target(
                MemoryPressureSnapshot::new(24_000 * MIB, 0, 0),
                CacheObservation {
                    resident_bytes: resident,
                    ..CacheObservation::default()
                },
            );
            assert_eq!(target, MIB, "real pressure must still reclaim the payload");
        }
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
            for _ in 0..50 {
                tick(&broker, low);
                if broker.0.lock().unwrap().pools[&buffers.id].desired == 0 {
                    break;
                }
            }
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
    fn pinned_floor_consumes_headroom_without_entering_the_evictable_target() {
        let broker = Arc::new(Broker::new());
        let unified = CachePool::register(
            Arc::clone(&broker),
            "unified",
            CacheFallback::Data,
            10,
            100_000,
        );
        {
            let mut state = broker.0.lock().unwrap();
            state.new_pool = false;
            state.budget = 50;
            let pool = state.pools.get_mut(&unified.id).unwrap();
            pool.desired = 100;
            pool.leased = 50;
            pool.pinned = 40;
            pool.observed.resident_bytes = 50;
        }
        let target = unified.target_with_pinned_floor(
            pressure(),
            CacheObservation {
                resident_bytes: 50,
                ..CacheObservation::default()
            },
            40,
        );
        assert_eq!(target, 10, "the requested target covers only fixed state");
        unified.applied_with_pinned_floor(target, target, 40);
        let state = broker.0.lock().unwrap();
        let pool = state.pools.get(&unified.id).unwrap();
        assert_eq!(pool.leased, 50, "the pinned floor stays charged");
        assert_eq!(pool.observed.resident_bytes, 50);
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
        // Fill the shared budget before turning the workload over. A short
        // underfilled run has uncontested headroom and should retain idle data.
        for round in 0..1600 {
            {
                let mut state = broker.0.lock().unwrap();
                for (id, active) in [(data.id, round < 600), (meta.id, round >= 600)] {
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
            if round == 599 {
                first_data = state.pools[&data.id].leased;
                assert!(first_data > state.pools[&meta.id].leased);
            }
            if round == 1599 {
                assert!(state.pools[&meta.id].leased > state.pools[&data.id].leased);
                assert!(
                    state.pools[&data.id].leased < first_data / 2,
                    "idle={} active={} initial={first_data}",
                    state.pools[&data.id].leased,
                    state.pools[&meta.id].leased,
                );
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

    #[test]
    fn volatile_candidate_oscillation_does_not_move_a_stable_target() {
        let broker = Arc::new(Broker::new());
        let data = CachePool::register(Arc::clone(&broker), "test", CacheFallback::Data, 0, 100);
        let mut state = broker.0.lock().unwrap();
        state.pools.get_mut(&data.id).unwrap().desired = 50;
        let old: BTreeMap<u64, u64> = [(data.id, 50_u64)].into_iter().collect();
        for _ in 0..20 {
            for candidate in [80, 30] {
                state.pools.get_mut(&data.id).unwrap().desired = candidate;
                state.damp_targets(&old, 10, 4, false);
                assert_eq!(state.pools[&data.id].desired, 50);
            }
        }
        assert!(state.pools[&data.id].target_drift.abs() < TARGET_STREAK_TRIGGER);
    }

    #[test]
    fn a_sustained_candidate_shift_moves_the_target_by_one_damped_step() {
        let broker = Arc::new(Broker::new());
        let data = CachePool::register(Arc::clone(&broker), "test", CacheFallback::Data, 0, 100);
        let mut state = broker.0.lock().unwrap();
        state.pools.get_mut(&data.id).unwrap().desired = 50;
        let old: BTreeMap<u64, u64> = [(data.id, 50_u64)].into_iter().collect();
        for _ in 0..3 {
            state.pools.get_mut(&data.id).unwrap().desired = 80;
            state.damp_targets(&old, 10, 4, false);
            assert_eq!(state.pools[&data.id].desired, 50);
        }
        state.pools.get_mut(&data.id).unwrap().desired = 80;
        state.damp_targets(&old, 10, 4, false);
        assert_eq!(state.pools[&data.id].desired, 60);
        assert_eq!(state.pools[&data.id].target_drift, TARGET_STREAK_TRIGGER);
    }

    #[test]
    fn hard_pressure_rejects_growth_immediately_but_preserves_admission_shrink() {
        let broker = Arc::new(Broker::new());
        let data = CachePool::register(Arc::clone(&broker), "test", CacheFallback::Data, 0, 100);
        let mut state = broker.0.lock().unwrap();
        state.pools.get_mut(&data.id).unwrap().desired = 50;
        let old: BTreeMap<u64, u64> = [(data.id, 50_u64)].into_iter().collect();
        state.pools.get_mut(&data.id).unwrap().desired = 80;
        state.damp_targets(&old, 10, 4, true);
        assert_eq!(state.pools[&data.id].desired, 50);
        state.pools.get_mut(&data.id).unwrap().desired = 30;
        state.damp_targets(&old, 10, 4, true);
        assert_eq!(state.pools[&data.id].desired, 30);
        assert_eq!(state.pools[&data.id].target_drift, 0);
    }
}
