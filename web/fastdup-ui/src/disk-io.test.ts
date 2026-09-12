import { expect, it } from "vitest";
import { formatQueueDepth } from "./disk-io";

it("distinguishes missing, idle, small and concurrent queue depth", () => {
  expect(formatQueueDepth(undefined, "de-DE")).toBe("—");
  expect(formatQueueDepth(null, "de-DE")).toBe("—");
  expect(formatQueueDepth(NaN, "de-DE")).toBe("—");
  expect(formatQueueDepth(0, "de-DE")).toBe("0");
  expect(formatQueueDepth(0.001, "de-DE")).toBe("< 0,01");
  expect(formatQueueDepth(0.42, "de-DE")).toBe("0,42");
  expect(formatQueueDepth(31.75, "en-US")).toBe("31.75");
});
