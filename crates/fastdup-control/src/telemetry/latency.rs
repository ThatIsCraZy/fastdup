//! Counter deltas, not differences or averages of percentiles. Reset and
//! unavailable observations break the window instead of inventing zeroes.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyHistogram {
    pub upper_micros: Vec<Option<u64>>,
    pub counts: Vec<u64>,
    pub errors: u64,
}
impl LatencyHistogram {
    pub(crate) fn valid(&self) -> bool {
        self.counts.len() == self.upper_micros.len()
            && (2..=64).contains(&self.counts.len())
            && self.upper_micros.last() == Some(&None)
            && self.upper_micros[..self.upper_micros.len() - 1]
                .iter()
                .all(Option::is_some)
            && self.upper_micros[..self.upper_micros.len() - 1]
                .windows(2)
                .all(|w| w[0] < w[1])
    }
    fn delta(&self, previous: &Self) -> Option<Self> {
        if !self.valid() || !previous.valid() || self.upper_micros != previous.upper_micros {
            return None;
        }
        Some(Self {
            upper_micros: self.upper_micros.clone(),
            errors: self.errors.checked_sub(previous.errors)?,
            counts: self
                .counts
                .iter()
                .zip(&previous.counts)
                .map(|(a, b)| a.checked_sub(*b))
                .collect::<Option<_>>()?,
        })
    }
    fn merge(mut self, other: Self) -> Option<Self> {
        if !self.valid() || !other.valid() || self.upper_micros != other.upper_micros {
            return None;
        }
        for (a, b) in self.counts.iter_mut().zip(other.counts) {
            *a = a.checked_add(b)?;
        }
        self.errors = self.errors.checked_add(other.errors)?;
        Some(self)
    }
    pub(crate) fn highest_bucket(&self) -> u64 {
        self.counts
            .iter()
            .rposition(|c| *c > 0)
            .map_or(0, |i| self.upper_micros[i].unwrap_or(u64::MAX))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyWindow {
    pub from: u64,
    pub to: u64,
    /// Sum of observed intervals, excluding gaps.
    pub covered_seconds: u64,
    pub read: LatencyHistogram,
    pub write: LatencyHistogram,
}
impl LatencyWindow {
    pub(crate) fn merge(self, other: Self) -> Option<Self> {
        if self.to > other.from {
            return None;
        }
        Some(Self {
            from: self.from,
            to: other.to,
            covered_seconds: self.covered_seconds.checked_add(other.covered_seconds)?,
            read: self.read.merge(other.read)?,
            write: self.write.merge(other.write)?,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct LatencySampler {
    previous: Option<(String, u64, LatencyHistogram, LatencyHistogram)>,
}
impl LatencySampler {
    pub(crate) fn observe(&mut self, now: u64, details: Option<&mut crate::DetailTelemetry>) {
        let Some(details) = details else {
            self.previous = None;
            return;
        };
        details.latency_window = None;
        let Some((read, write)) = details.latency_histograms.take() else {
            self.previous = None;
            return;
        };
        let Some(runtime) = &details.runtime else {
            self.previous = None;
            return;
        };
        if runtime.runtime_id.is_empty() {
            self.previous = None;
            return;
        }
        // Two observations in one wall-clock second share the old baseline;
        // advancing it here would silently discard the intervening requests.
        if self
            .previous
            .as_ref()
            .is_some_and(|(id, time, _, _)| id == &runtime.runtime_id && *time == now)
        {
            return;
        }
        if let Some((id, from, old_read, old_write)) = &self.previous
            && id == &runtime.runtime_id
            && now > *from
            && now - *from <= 30
            && let Some((read, write)) = read.delta(old_read).zip(write.delta(old_write))
        {
            details.latency_window = Some(LatencyWindow {
                from: *from,
                to: now,
                covered_seconds: now - *from,
                read,
                write,
            });
        }
        self.previous = Some((runtime.runtime_id.clone(), now, read, write));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram(counts: [u64; 3]) -> LatencyHistogram {
        LatencyHistogram {
            upper_micros: vec![Some(100), Some(5_000_000), None],
            counts: counts.to_vec(),
            errors: 0,
        }
    }
    fn details(runtime: &str, counts: [u64; 3]) -> crate::DetailTelemetry {
        let runtime = serde_json::from_value(serde_json::json!({
            "runtimeId":runtime, "ioUring":{"ringEntries":1,"inflightBytes":0,"maxInflightBytes":1,"peakInflightBytes":0,"submitted":0,"completed":0},
            "caches":[],"reduction":{"enabled":false,"queries":0,"candidates":0,"acceptedPrefixes":0,"acceptedSparseXor":0,"savedPayloadBytes":0,"fallbacks":0,"errors":0}
        })).unwrap();
        crate::DetailTelemetry {
            runtime: Some(runtime),
            latency_histograms: Some((histogram(counts), histogram(counts))),
            ..crate::DetailTelemetry::default()
        }
    }
    #[test]
    fn deltas_retain_a_single_slow_request_without_lifetime_dilution() {
        let mut sampler = LatencySampler::default();
        let mut first = details("a", [10_000, 0, 0]);
        sampler.observe(100, Some(&mut first));
        assert!(first.latency_window.is_none());
        let mut next = details("a", [10_001, 1, 0]);
        sampler.observe(101, Some(&mut next));
        let window = next.latency_window.unwrap();
        assert_eq!(window.read.counts, [1, 1, 0]);
        assert_eq!(window.covered_seconds, 1);
        assert_eq!(window.read.highest_bucket(), 5_000_000);
        let mut quiet = details("a", [10_003, 1, 0]);
        sampler.observe(102, Some(&mut quiet));
        let combined = window.merge(quiet.latency_window.unwrap()).unwrap();
        assert_eq!(combined.read.counts, [3, 1, 0]);
        assert_eq!(combined.covered_seconds, 2);
    }
    #[test]
    fn resets_missing_runtime_clock_reversal_and_long_gaps_break_windows() {
        let mut sampler = LatencySampler::default();
        for (id, time, counts) in [
            ("a", 100, [10, 0, 0]),
            ("b", 101, [20, 0, 0]),
            ("b", 102, [1, 0, 0]),
            ("b", 99, [2, 0, 0]),
            ("b", 200, [3, 0, 0]),
        ] {
            let mut sample = details(id, counts);
            sampler.observe(time, Some(&mut sample));
            assert!(sample.latency_window.is_none());
        }
        sampler.observe(201, None);
        let mut next = details("b", [4, 0, 0]);
        sampler.observe(202, Some(&mut next));
        assert!(next.latency_window.is_none());
    }
    #[test]
    fn incompatible_or_malformed_histograms_are_not_merged() {
        let mut bad = histogram([1, 2, 3]);
        bad.upper_micros.pop();
        assert!(!bad.valid());
        assert!(bad.delta(&histogram([0, 0, 0])).is_none());
        assert!(histogram([1, 0, 0]).delta(&histogram([2, 0, 0])).is_none());
        let serialized = serde_json::to_string(&histogram([1, 2, 3])).unwrap();
        assert_eq!(
            serde_json::from_str::<LatencyHistogram>(&serialized).unwrap(),
            histogram([1, 2, 3])
        );
    }

    #[test]
    fn same_second_observations_do_not_discard_intervening_requests() {
        let mut sampler = LatencySampler::default();
        sampler.observe(100, Some(&mut details("a", [10, 0, 0])));
        sampler.observe(100, Some(&mut details("a", [11, 1, 0])));
        let mut next = details("a", [12, 1, 0]);
        sampler.observe(101, Some(&mut next));
        assert_eq!(next.latency_window.unwrap().read.counts, [2, 1, 0]);
    }
}
