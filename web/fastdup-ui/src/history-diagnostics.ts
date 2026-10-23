import type { TelemetrySnapshot } from "./types";

export interface HistoryWindow { from: number; to: number }
export interface Histogram { upperMicros: (number | null)[]; counts: number[]; errors: number }
export interface LatencyWindow extends HistoryWindow { coveredSeconds: number; read: Histogram; write: Histogram }
export interface MetricRange { min: number; max: number; mean: number }
export interface HistorySummary extends HistoryWindow {
  version: "1"; resolutionSeconds: number; sampleCount: number | null; extremaComplete: boolean;
  read: MetricRange; write: MetricRange; cpu: MetricRange; ram: MetricRange;
  latency?: LatencyWindow | null; incident?: TelemetrySnapshot | null;
}
export interface WorkloadContext {
  observedAt: number; smbStatus: string; truncated: boolean; repository?: string | null;
  shares: {id: string; name: string; path: string; protocol: string}[];
  connections: {share: string; client: string; smbSessionId?: string | null; connectedAt?: string | null}[];
  backupSessions?: string[] | null;
}
export interface DiagnosticEvent { id: string; timestamp: number; source: string; action: string; outcome: string; detail: string }
export interface EventWindow { events: DiagnosticEvent[]; truncated: boolean }
export interface Finding {
  rule: string; title: string; explanation: string; observedAt: string; runtimeId?: string;
  evidence: {field: string; value: string | number | boolean; unit?: string}[];
}

export function histogramPercentile(histogram: Histogram, percentile: number): {value: number; overflow: boolean} | undefined {
  const {counts, upperMicros} = histogram;
  if (!counts.length || counts.length !== upperMicros.length) return;
  const total = counts.reduce((a,b) => a + b, 0);
  if (!total) return;
  const threshold = Math.ceil(total * percentile / 100);
  let sum = 0;
  for (let i = 0; i < counts.length; i++) {
    sum += counts[i];
    if (sum >= threshold) return {value: upperMicros[i] ?? upperMicros[i - 1] ?? 0, overflow: upperMicros[i] == null};
  }
}

/** Rules use observations in one sample. Lifetime maxima never establish a current wait. */
export function diagnose(sample: TelemetrySnapshot): Finding[] {
  const runtime = sample.details?.runtime;
  const pipeline = runtime?.pipeline;
  const admission = pipeline?.admission;
  const findings: Finding[] = [];
  const add = (rule: string, title: string, explanation: string, evidence: Finding["evidence"]) =>
    findings.push({rule, title, explanation, evidence, observedAt: sample.observedAt, runtimeId: runtime?.runtimeId});
  if (admission && !admission.open) {
    const publication = pipeline?.operations.find(operation => operation.id === "publicationWait");
    const correlated = admission.reason === "durabilityLag" && publication && publication.active > 0;
    add(correlated ? "durability-publication-v1" : "admission-closed-v1",
      correlated ? "Durability-Lag und aktive Publication-Waits" : "Schreibannahme geschlossen",
      correlated ? "Beide Zustände wurden gleichzeitig gemessen. Publication-Waits können den dauerhaften Fortschritt verzögern; die Korrelation beweist keine Ursache." : "Die Runtime nimmt zum Messzeitpunkt keine neuen Schreibmutationen an. Der gemeldete Grund und die aktuelle Schließdauer sind die Belege.",
      [{field: "pipeline.admission.open", value: false}, {field: "pipeline.admission.reason", value: admission.reason ?? "unknown"},
        {field: "pipeline.admission.currentClosedMs", value: admission.currentClosedMs, unit: "ms"},
        ...(correlated ? [{field: "pipeline.publicationWait.active", value: publication.active}] : [])]);
  }
  const latency = sample.details?.latencyWindow;
  if (latency) {
    for (const name of ["read", "write"] as const) {
      const histogram = latency[name];
      const peak = histogramPercentile(histogram, 100);
      if (peak && (peak.value >= 1_000_000 || peak.overflow)) add(`latency-${name}-v1`,
        "Lange Dateizugriffe im Messintervall", "Mindestens ein Request liegt in einem Latenz-Bucket mit mindestens einer Sekunde Obergrenze. Das ist eine Bucket-Grenze, keine exakte Dauer.",
        [{field: `latencyWindow.${name}.highestBucketMicros`, value: peak.value, unit: "µs"},
          {field: "latencyWindow.from", value: latency.from, unit: "unix s"}, {field: "latencyWindow.to", value: latency.to, unit: "unix s"}]);
      if (histogram.errors > 0) add(`errors-${name}-v1`, "Fehlgeschlagene Dateizugriffe im Messintervall",
        "Der Fehlerzähler ist in diesem Messintervall gestiegen.", [{field: `latencyWindow.${name}.errors`, value: histogram.errors}]);
    }
  }
  if (runtime?.gc?.state === "running") add("gc-active-v1", "GC zum Messzeitpunkt aktiv",
    "GC lief gleichzeitig mit den angezeigten Zugriffen. Daraus folgt noch keine Verursachung eines Durchsatzeinbruchs.",
    [{field: "gc.state", value: "running"}, {field: "gc.observedAt", value: runtime.gc.observedAt, unit: "unix s"}]);
  return findings;
}

export function nearestSample(samples: TelemetrySnapshot[], seconds: number): number {
  return samples.reduce((best, sample, index) => Math.abs(Date.parse(sample.observedAt) / 1000 - seconds) < Math.abs(Date.parse(samples[best].observedAt) / 1000 - seconds) ? index : best, 0);
}

export function sampleWindow(sample: TelemetrySnapshot): HistoryWindow {
  const time = Math.floor(Date.parse(sample.observedAt) / 1000);
  return sample.history ?? {from: time, to: time};
}

export function diagnosticExport(window: HistoryWindow, samples: TelemetrySnapshot[], selected: TelemetrySnapshot | undefined, events: EventWindow | null) {
  const observations = samples.flatMap(sample => sample.history?.incident ? [sample, sample.history.incident] : [sample]);
  const findings = observations.flatMap(diagnose).filter((finding, index, all) => all.findIndex(other => other.rule === finding.rule && other.observedAt === finding.observedAt && other.runtimeId === finding.runtimeId) === index);
  const sourceWindow = samples.length ? {from: Math.min(...samples.map(sample => sampleWindow(sample).from)), to: Math.max(...samples.map(sample => sampleWindow(sample).to))} : null;
  return {schemaVersion: 1, generatedAt: new Date().toISOString(), window, sourceWindow,
    selected: selected ? {observedAt: selected.observedAt, sequence: selected.sequence} : null,
    coverage: {sampleLimit: 1500, samples: samples.length, eventsAvailable: events !== null, eventsTruncated: events?.truncated ?? false,
      extremaComplete: samples.length > 0 && samples.every(sample => sample.history?.extremaComplete),
      detailPolicy: "Last snapshot per bucket plus one retained incident (admission closure, latency, lowest combined throughput). Workload connections are context, not per-client performance. Backup session IDs may be unavailable."},
    samples, events: events?.events ?? [], findings};
}

export function historicalChart(samples: TelemetrySnapshot[], selected: number, kind: "throughput" | "resources", locale: string, selectedAt?: string) {
  const first = samples.length ? Math.min(...samples.map(sample => sampleWindow(sample).from * 1000)) : 0;
  const last = samples.length ? Math.max(...samples.map(sample => sampleWindow(sample).to * 1000)) : 0;
  const metrics = kind === "throughput" ? [["POSIX Read", "read", "frontendReadMbps", "#22d3ee"], ["POSIX Write", "write", "frontendWriteMbps", "#b69aff"]] as const
    : [["CPU", "cpu", "cpuPercent", "#f5b84b"], ["RAM", "ram", "ramPercent", "#3ddc97"]] as const;
  const series = metrics.flatMap(([name, range, field, color]) => ["mean", "min", "max"].map(stat => ({
    name: `${name} · ${stat === "mean" ? "Ø" : stat}`, type: "line", showSymbol: true, symbol: "circle", symbolSize: stat === "mean" ? 4 : 2, triggerLineEvent: true,
    data: samples.map(sample => [Date.parse(sample.observedAt), stat === "mean" ? sample[field] : sample.history?.extremaComplete ? sample.history[range][stat as "min" | "max"] : null]),
    lineStyle: {color, width: stat === "mean" ? 2 : 1, type: stat === "mean" ? "solid" : "dashed", opacity: stat === "mean" ? 1 : 0.65}, itemStyle: {color},
    markLine: stat === "mean" && samples[selected] ? {silent: true, symbol: "none", label: {show: false}, lineStyle: {color: "#fff"}, data: [{xAxis: Date.parse(selectedAt ?? samples[selected].observedAt)}]} : undefined,
  })));
  return {animation: false, grid: {left: 48, right: 20, top: 50, bottom: 30},
    tooltip: {trigger: "axis", backgroundColor: "#101c24", borderColor: "#375361", textStyle: {color: "#edf5fa"}},
    legend: {type: "scroll", top: 0, textStyle: {color: "#afbecb"}, pageTextStyle: {color: "#afbecb"}},
    xAxis: {type: "time", ...(last > first ? {min: first, max: last} : {}), axisLabel: {color: "#afbecb", hideOverlap: true, formatter: (value: number) => new Date(value).toLocaleString(locale, {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit"})}},
    yAxis: {type: "value", min: 0, ...(kind === "resources" ? {max: 100} : {}), name: kind === "throughput" ? "MB/s" : "%", axisLabel: {color: "#afbecb"}, splitLine: {lineStyle: {color: "#263947"}}}, series};
}
