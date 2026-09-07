//! Agent-side counter differences, carried into live and historical telemetry.
use crate::detail_telemetry::{CacheWindowCounters, CacheWindowTelemetry, RuntimeDetails};
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub(crate) struct CacheWindow {
    runtime: String,
    samples: VecDeque<(u64, Vec<CacheWindowCounters>)>,
}
impl CacheWindow {
    pub(crate) fn observe(&mut self, now: u64, runtime: &mut RuntimeDetails) {
        let counters: Vec<_> = runtime.cache_budget.as_ref().map_or_else(
            || {
                runtime
                    .caches
                    .iter()
                    .map(|c| CacheWindowCounters {
                        id: c.id.clone(),
                        hits: c.hits,
                        misses: c.misses,
                        evictions: c.evictions,
                    })
                    .collect()
            },
            |b| {
                b.pools
                    .iter()
                    .map(|c| CacheWindowCounters {
                        id: c.id.clone(),
                        hits: c.hits,
                        misses: c.misses,
                        evictions: c.evictions,
                    })
                    .collect()
            },
        );
        let reset = self.runtime != runtime.runtime_id
            || self.samples.back().is_some_and(|(time, previous)| {
                now < *time
                    || previous.len() != counters.len()
                    || counters.iter().any(|c| {
                        !previous.iter().any(|p| {
                            c.id == p.id
                                && c.hits >= p.hits
                                && c.misses >= p.misses
                                && c.evictions >= p.evictions
                        })
                    })
            });
        if reset {
            self.samples.clear();
            self.runtime.clone_from(&runtime.runtime_id);
        }
        if self.samples.back().is_some_and(|(time, _)| *time == now) {
            self.samples.pop_back();
        }
        self.samples.push_back((now, counters.clone()));
        // Keep only observations within the requested five-minute window.
        while self
            .samples
            .front()
            .is_some_and(|(time, _)| now.saturating_sub(*time) > 300)
        {
            self.samples.pop_front();
        }
        let (start, baseline) = self.samples.front().expect("current observation exists");
        runtime.cache_window = Some(CacheWindowTelemetry {
            seconds: now - *start,
            pools: counters
                .into_iter()
                .filter_map(|c| {
                    let previous = baseline.iter().find(|p| p.id == c.id)?;
                    Some(CacheWindowCounters {
                        id: c.id,
                        hits: c.hits - previous.hits,
                        misses: c.misses - previous.misses,
                        evictions: c.evictions - previous.evictions,
                    })
                })
                .collect(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn runtime(id: &str, hits: u64, misses: u64) -> RuntimeDetails {
        serde_json::from_value(serde_json::json!({
            "runtimeId":id,"caches":[{"id":"verifiedRead","hits":hits,"misses":misses,"evictions":0}],
            "ioUring":{"ringEntries":1,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "reduction":{"enabled":false,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0}
        })).unwrap()
    }
    #[test]
    fn uses_differences_not_lifetime_percentages_and_retains_five_minutes() {
        let mut window = CacheWindow::default();
        for second in 0..=600 {
            let mut r = runtime("one", 10_000 + second * 9, 20_000 + second);
            window.observe(second, &mut r);
            let stats = r.cache_window.unwrap();
            assert_eq!(stats.seconds, second.min(300));
            assert_eq!(stats.pools[0].hits, second.min(300) * 9);
            assert_eq!(stats.pools[0].misses, second.min(300));
        }
        assert_eq!(window.samples.len(), 301);
    }
    #[test]
    fn restart_counter_reset_gap_and_clock_reversal_never_mix_epochs() {
        let mut window = CacheWindow::default();
        for (time, id, hits) in [
            (100, "one", 100),
            (200, "two", 200),
            (250, "two", 10),
            (1000, "two", 20),
            (999, "two", 30),
        ] {
            let mut r = runtime(id, hits, 0);
            window.observe(time, &mut r);
            assert_eq!(r.cache_window.unwrap().seconds, 0);
        }
    }
    #[test]
    fn cache_budget_counters_drive_window_and_serialize_into_history() {
        let mut window = CacheWindow::default();
        for (time, hits) in [(0, 100), (30, 110)] {
            let mut r = runtime("one", 900_000, 100);
            r.cache_budget = Some(serde_json::from_value(serde_json::json!({
                "maximumMemoryUsedBasisPoints":9200,"effectiveLimitBytes":1000,"availableBytes":80,"budgetBytes":800,
                "pools":[{"id":"verifiedRead","fallbackTier":"data","residentBytes":20,"targetBytes":30,"leasedBytes":30,"hits":hits,"misses":4,"evictions":0}]
            })).unwrap());
            window.observe(time, &mut r);
            let saved = serde_json::to_value(&r).unwrap();
            assert_eq!(
                saved["cacheWindow"]["pools"][0]["hits"],
                if time == 0 { 0 } else { 10 }
            );
            assert_eq!(saved["cacheBudget"]["pools"][0]["residentBytes"], 20);
        }
    }
}
