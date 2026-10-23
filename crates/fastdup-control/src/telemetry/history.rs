//! Bounded, mergeable history. Details belong to an actual observation; ranges
//! and histograms describe the interval. Neither is storage authority.
use serde::{Deserialize, Serialize};

use crate::TelemetrySnapshot;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum HistoryVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricRange {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySummary {
    pub version: HistoryVersion,
    pub from: i64,
    pub to: i64,
    pub resolution_seconds: u64,
    /// None for legacy rollups: their source count and extrema were discarded.
    pub sample_count: Option<u64>,
    pub extrema_complete: bool,
    pub read: MetricRange,
    pub write: MetricRange,
    pub cpu: MetricRange,
    pub ram: MetricRange,
    pub latency: Option<super::latency::LatencyWindow>,
    /// One coherent original observation, selected by admission closure, then
    /// window latency, then lowest combined throughput. Never nested history.
    pub incident: Option<Box<TelemetrySnapshot>>,
}

pub(crate) struct TelemetryAverage {
    latest: TelemetrySnapshot,
    summary: Option<HistorySummary>,
    sums: [f64; 6],
    weight: u64,
    reduction_weight: u64,
}

impl TelemetryAverage {
    pub(crate) fn new() -> Self {
        Self {
            latest: TelemetrySnapshot::default(),
            summary: None,
            sums: [0.0; 6],
            weight: 0,
            reduction_weight: 0,
        }
    }

    pub(crate) fn push(
        &mut self,
        observed_at: i64,
        resolution: u64,
        mut sample: TelemetrySnapshot,
    ) {
        sample.series.clear();
        let previous = sample.history.take();
        let range = |value| MetricRange {
            min: value,
            max: value,
            mean: value,
        };
        let mut incoming = previous.map_or_else(
            || HistorySummary {
                version: HistoryVersion::V1,
                from: observed_at,
                to: observed_at.saturating_add(
                    i64::try_from(resolution.saturating_sub(1)).unwrap_or(i64::MAX),
                ),
                resolution_seconds: resolution,
                sample_count: (resolution == 1).then_some(1),
                extrema_complete: resolution == 1,
                read: range(sample.frontend_read_mbps),
                write: range(sample.frontend_write_mbps),
                cpu: range(sample.cpu_percent),
                ram: range(sample.ram_percent),
                latency: sample
                    .details
                    .as_ref()
                    .and_then(|d| d.latency_window.clone()),
                incident: None,
            },
            |value| *value,
        );
        let weight = incoming.sample_count.unwrap_or(1).max(1);
        incoming.resolution_seconds = incoming.resolution_seconds.max(resolution);
        for (sum, value) in self.sums.iter_mut().zip([
            incoming.read.mean,
            incoming.write.mean,
            sample.dedup_rate,
            sample.reduction_ratio.unwrap_or(0.0),
            incoming.cpu.mean,
            incoming.ram.mean,
        ]) {
            *sum += value * weight as f64;
        }
        self.weight += weight;
        if sample.reduction_ratio.is_some() {
            self.reduction_weight += weight;
        }
        // Legacy rollups contain averaged main fields, so they cannot supply
        // a new supposedly original incident observation.
        if incoming.extrema_complete && incoming.sample_count == Some(1) {
            incoming.incident = Some(Box::new(sample.clone()));
        }
        if let Some(summary) = &mut self.summary {
            summary.from = summary.from.min(incoming.from);
            summary.to = summary.to.max(incoming.to);
            summary.resolution_seconds =
                summary.resolution_seconds.max(incoming.resolution_seconds);
            summary.sample_count = summary
                .sample_count
                .zip(incoming.sample_count)
                .and_then(|(a, b)| a.checked_add(b));
            summary.extrema_complete &= incoming.extrema_complete;
            for (a, b) in [
                &mut summary.read,
                &mut summary.write,
                &mut summary.cpu,
                &mut summary.ram,
            ]
            .into_iter()
            .zip([incoming.read, incoming.write, incoming.cpu, incoming.ram])
            {
                a.min = a.min.min(b.min);
                a.max = a.max.max(b.max);
            }
            summary.latency = match (summary.latency.take(), incoming.latency) {
                (Some(a), Some(b)) => a.merge(b),
                (a, b) => a.or(b),
            };
            if let Some(candidate) = incoming.incident
                && summary
                    .incident
                    .as_ref()
                    .is_none_or(|current| prefer_incident(&candidate, current))
            {
                summary.incident = Some(candidate);
            }
        } else {
            self.summary = Some(incoming);
        }
        self.latest = sample;
    }

    pub(crate) fn finish(mut self) -> TelemetrySnapshot {
        let weight = self.weight.max(1) as f64;
        let mut summary = self.summary.expect("nonempty history bucket");
        summary.read.mean = self.sums[0] / weight;
        summary.write.mean = self.sums[1] / weight;
        summary.cpu.mean = self.sums[4] / weight;
        summary.ram.mean = self.sums[5] / weight;
        // Keep top-level means compatible with existing clients. The original
        // incident retains its actual throughput, details, workload and time.
        self.latest.frontend_read_mbps = summary.read.mean;
        self.latest.frontend_write_mbps = summary.write.mean;
        self.latest.cpu_percent = summary.cpu.mean;
        self.latest.ram_percent = summary.ram.mean;
        self.latest.dedup_rate = self.sums[2] / weight;
        self.latest.reduction_ratio =
            (self.reduction_weight > 0).then(|| self.sums[3] / self.reduction_weight as f64);
        if summary.sample_count == Some(1) {
            summary.incident = None;
        }
        self.latest.history = Some(Box::new(summary));
        self.latest
    }
}

fn prefer_incident(candidate: &TelemetrySnapshot, current: &TelemetrySnapshot) -> bool {
    let score = |sample: &TelemetrySnapshot| {
        let details = sample.details.as_ref();
        let admission = details
            .and_then(|d| d.runtime.as_ref())
            .and_then(|r| r.pipeline.as_ref())
            .map(|p| &p.admission);
        let blocked = admission.is_some_and(|a| !a.open);
        let closed_ms = admission
            .filter(|_| blocked)
            .map_or(0.0, |a| a.current_closed_ms);
        let latency = details
            .and_then(|d| d.latency_window.as_ref())
            .map_or(0, |w| w.read.highest_bucket().max(w.write.highest_bucket()));
        (
            blocked,
            closed_ms,
            latency,
            sample.frontend_read_mbps + sample.frontend_write_mbps,
        )
    };
    let a = score(candidate);
    let b = score(current);
    a.0.cmp(&b.0)
        .then_with(|| a.1.total_cmp(&b.1))
        .then_with(|| a.2.cmp(&b.2))
        .then_with(|| b.3.total_cmp(&a.3))
        .is_gt()
}
