import { describe, expect, it } from "vitest";
import { appendResourceSample, resourceChartOption, type ResourceSample } from "./resource-history";
const point = (second: number, cpu = second, ram = 40): ResourceSample => ({ observedAt: new Date(1_700_000_000_000 + second * 1000).toISOString(), cpuPercent: cpu, ramPercent: ram });
describe("live resource history", () => {
  it("plots distinct CPU and RAM values over time, including a visible first sample", () => {
    const first = appendResourceSample([], point(1, 20, 65));
    expect(resourceChartOption(first, "de-DE").series[0].showSymbol).toBe(true);
    const history = appendResourceSample(first, point(2, 45, 66));
    const chart = resourceChartOption(history, "en-US");
    expect(chart.xAxis.type).toBe("time");
    expect(chart.series[0].data.map(p => p[1])).toEqual([20, 45]);
    expect(chart.series[1].data.map(p => p[1])).toEqual([65, 66]);
    expect(chart.yAxis).toMatchObject({ min: 0, max: 100 });
  });
  it("deduplicates refreshes, ignores out-of-order samples, and bounds memory", () => {
    let history: ResourceSample[] = [];
    for (let second = 0; second < 2000; second++) history = appendResourceSample(history, point(second));
    expect(history).toHaveLength(900);
    history = appendResourceSample(history, point(1999, 1));
    expect(history).toHaveLength(900);
    expect(history.at(-1)?.cpuPercent).toBe(1);
    expect(appendResourceSample(history, point(1))).toBe(history);
    expect(appendResourceSample(history, { ...point(2000), observedAt: "invalid" })).toBe(history);
    expect(appendResourceSample(history, point(4000))).toHaveLength(1);
    expect(Object.keys(history[0]).sort()).toEqual(["cpuPercent", "observedAt", "ramPercent"]);
  });
});
