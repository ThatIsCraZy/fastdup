import { expect, it } from "vitest";
import { formatBytes, formatCount, formatDuration, formatPercent, hitRate } from "./format";

it("scales durations to the readable unit and keeps sub-millisecond precision", () => {
  expect(formatDuration(0.05, "de-DE")).toBe("50 µs");
  expect(formatDuration(0.5, "de-DE")).toBe("500 µs");
  expect(formatDuration(2, "de-DE")).toBe("2 ms");
  expect(formatDuration(12_000, "de-DE")).toBe("12 s");
  expect(formatDuration(120_000, "de-DE")).toBe("2 min");
  expect(formatDuration(7_200_000, "de-DE")).toBe("2 h");
});

it("never turns a missing sample into a zero", () => {
  for (const missing of [null, undefined, Number.NaN]) {
    expect(formatDuration(missing, "de-DE")).toBe("—");
    expect(formatBytes(missing, "de-DE")).toBe("—");
    expect(formatCount(missing, "de-DE")).toBe("—");
    expect(formatPercent(missing, "de-DE")).toBe("—");
  }
  expect(formatDuration(0, "de-DE")).toBe("0 ms");
  expect(formatBytes(0, "de-DE")).toBe("0 B");
});

it("uses one decimal byte vocabulary across every surface", () => {
  expect(formatBytes(1024, "de-DE")).toBe("1,02 KB");
  expect(formatBytes(6e9, "de-DE")).toBe("6 GB");
  expect(formatBytes(80e12, "de-DE")).toBe("80 TB");
});

it("reports no hit rate without observations instead of a hundred percent", () => {
  expect(hitRate(0, 0)).toBeNull();
  expect(hitRate(undefined, 5)).toBeNull();
  expect(hitRate(75, 25)).toBe(75);
});
