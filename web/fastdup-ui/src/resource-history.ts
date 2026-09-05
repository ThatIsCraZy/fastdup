import type { TelemetrySnapshot } from "./types";

export type ResourceSample = Pick<TelemetrySnapshot, "observedAt" | "cpuPercent" | "ramPercent">;
export function appendResourceSample(history: ResourceSample[], sample: ResourceSample): ResourceSample[] {
  const time = Date.parse(sample.observedAt);
  if (!Number.isFinite(time)) return history;
  const last = history[history.length - 1];
  if (last && time < Date.parse(last.observedAt)) return history;
  const point = { observedAt: sample.observedAt, cpuPercent: sample.cpuPercent, ramPercent: sample.ramPercent };
  const previous = last?.observedAt === sample.observedAt ? history.slice(0, -1) : history;
  return [...previous.filter(item => Date.parse(item.observedAt) >= time - 15 * 60_000), point].slice(-900);
}

export function resourceChartOption(samples: ResourceSample[], locale: string) {
  return {
    animation: false,
    textStyle: { fontFamily: 'Inter, "Segoe UI", sans-serif' },
    grid: { left: 45, right: 24, top: 42, bottom: 35 },
    legend: { top: 2, textStyle: { color: "#afbecb" } },
    tooltip: { trigger: "axis", valueFormatter: (value: number) => `${value.toLocaleString(locale, { maximumFractionDigits: 1 })} %`, backgroundColor: "#101c24", borderColor: "#375361", textStyle: { color: "#edf5fa" } },
    xAxis: {
      type: "time",
      axisLabel: { color: "#afbecb", hideOverlap: true, formatter: (value: number) => new Date(value).toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit", second: "2-digit" }) },
      axisLine: { lineStyle: { color: "#324650" } },
    },
    yAxis: { type: "value", min: 0, max: 100, name: "%", nameTextStyle: { color: "#afbecb" }, axisLabel: { color: "#afbecb" }, splitLine: { lineStyle: { color: "rgba(68,94,108,.24)" } } },
    series: ([['CPU', 'cpuPercent', '#f5b84b'], ['RAM', 'ramPercent', '#3ddc97']] as const).map(([name, key, color]) => ({
      name, type: "line", showSymbol: samples.length < 2, symbolSize: 7,
      data: samples.map(sample => [Date.parse(sample.observedAt), sample[key]]),
      lineStyle: { color, width: 2 }, itemStyle: { color },
    })),
  };
}
