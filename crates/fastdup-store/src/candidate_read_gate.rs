//! Volatile writer-only admission for speculative cold Base reads.
//! No fingerprint or learned observation is content/integrity authority.
use std::sync::Mutex;

const BUCKETS: usize = 16 * 3 * 4 * 2;
const WARMUP: u8 = 8;
const EXPLORATION_INTERVAL: u64 = 32;

#[derive(Clone, Copy, Debug)]
pub(crate) struct GateKey(usize);
impl GateKey {
    pub(crate) fn new(
        distance: u16,
        length: u32,
        independent_bytes: usize,
        has_best: bool,
    ) -> Self {
        let distance = usize::from(distance / 32).min(15);
        let size = if length <= 16 * 1024 {
            0
        } else if length <= 64 * 1024 {
            1
        } else {
            2
        };
        let compression = independent_bytes
            .saturating_mul(4)
            .checked_div(length as usize)
            .unwrap_or(0)
            .min(3);
        Self(((distance * 3 + size) * 4 + compression) * 2 + usize::from(has_best))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GateDecision {
    Allow,
    Explore,
    Skip,
}

#[derive(Clone, Copy, Debug, Default)]
struct Evidence {
    samples: u8,
    wins: u32,
    saved: u64,
    cost_ns: u64,
    rejected: u64,
}
impl Evidence {
    fn observe(&mut self, saved: u64, cost_ns: u64) {
        // A rolling 32-result success history plus an EWMA (~32 observations).
        self.wins = (self.wins << 1) | u32::from(saved != 0);
        self.samples = self.samples.saturating_add(1);
        if self.samples == 1 {
            self.saved = saved;
            self.cost_ns = cost_ns.max(1);
        } else {
            self.saved = self.saved - self.saved / 32 + saved / 32;
            self.cost_ns = (self.cost_ns - self.cost_ns / 32 + cost_ns / 32).max(1);
        }
    }
}
struct State {
    buckets: [Evidence; BUCKETS],
    portfolio: Evidence,
}
impl Default for State {
    fn default() -> Self {
        Self {
            buckets: [Evidence::default(); BUCKETS],
            portfolio: Evidence::default(),
        }
    }
}
#[derive(Default)]
pub(crate) struct CandidateReadGate(Mutex<State>);
impl CandidateReadGate {
    pub(crate) fn decide(&self, key: GateKey) -> GateDecision {
        // Admission is an optimization. Never queue writers behind the learner.
        let Ok(mut state) = self.0.try_lock() else {
            return GateDecision::Allow;
        };
        let portfolio = state.portfolio;
        let bucket = &mut state.buckets[key.0];
        if bucket.samples < WARMUP {
            return GateDecision::Allow;
        }
        // Compare marginal bytes saved per measured read+codec nanosecond.
        // Retain a 4x tolerance to avoid chasing small timing fluctuations.
        let inefficient = (u128::from(bucket.saved) * u128::from(portfolio.cost_ns))
            .saturating_mul(4)
            < u128::from(portfolio.saved) * u128::from(bucket.cost_ns);
        if bucket.wins != 0 && !inefficient {
            return GateDecision::Allow;
        }
        bucket.rejected = bucket.rejected.wrapping_add(1);
        if bucket.rejected.is_multiple_of(EXPLORATION_INTERVAL) {
            GateDecision::Explore
        } else {
            GateDecision::Skip
        }
    }

    pub(crate) fn observe(&self, key: GateKey, saved_bytes: u64, cost_ns: u64) {
        if let Ok(mut state) = self.0.try_lock() {
            state.buckets[key.0].observe(saved_bytes, cost_ns);
            state.portfolio.observe(saved_bytes, cost_ns);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn learns_unprofitable_reads_and_probes_changed_workloads() {
        let gate = CandidateReadGate::default();
        let key = GateKey::new(180, 65536, 60000, false);
        for _ in 0..WARMUP {
            assert_eq!(gate.decide(key), GateDecision::Allow);
            gate.observe(key, 0, 1_000_000);
        }
        for _ in 1..EXPLORATION_INTERVAL {
            assert_eq!(gate.decide(key), GateDecision::Skip);
        }
        assert_eq!(gate.decide(key), GateDecision::Explore);
        gate.observe(key, 20_000, 1_000_000);
        assert_eq!(gate.decide(key), GateDecision::Allow);
        for _ in 0..32 {
            gate.observe(key, 0, 1_000_000);
        }
        assert_eq!(gate.decide(key), GateDecision::Skip);
    }
    #[test]
    fn measured_cost_and_gain_matter_not_a_fixed_sketch_cutoff() {
        let gate = CandidateReadGate::default();
        let good = GateKey::new(220, 65536, 60000, false);
        let expensive = GateKey::new(20, 65536, 60000, false);
        for _ in 0..32 {
            gate.observe(good, 30_000, 1000);
        }
        for _ in 0..WARMUP {
            gate.observe(expensive, 100, 10_000_000);
        }
        assert_eq!(gate.decide(good), GateDecision::Allow);
        assert_eq!(gate.decide(expensive), GateDecision::Skip);
    }
    #[test]
    fn buckets_separate_size_compression_and_marginal_trials_and_contention_fails_open() {
        let gate = CandidateReadGate::default();
        let key = GateKey::new(512, 262144, 262144, false);
        for _ in 0..WARMUP {
            gate.observe(key, 0, 1000);
        }
        assert_eq!(gate.decide(key), GateDecision::Skip);
        for key in [
            GateKey::new(512, 16000, 16000, false),
            GateKey::new(512, 262144, 9000, false),
            GateKey::new(512, 262144, 262144, true),
        ] {
            assert_eq!(gate.decide(key), GateDecision::Allow);
        }
        let _held = gate.0.lock().unwrap();
        assert_eq!(gate.decide(key), GateDecision::Allow);
        gate.observe(key, 0, 1); // Nonblocking feedback too.
    }
}
