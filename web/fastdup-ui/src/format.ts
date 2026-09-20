/**
 * One numeric vocabulary for every telemetry surface.
 * Missing samples render as an em dash and never as a zero, so an unavailable
 * counter stays distinguishable from a counter that really is zero.
 */
const MISSING = "—";

export function formatCount(value: number | null | undefined, locale: string): string {
  return value == null || !Number.isFinite(value)
    ? MISSING
    : value.toLocaleString(locale, { maximumFractionDigits: 2 });
}

export function formatBytes(value: number | null | undefined, locale: string): string {
  if (value == null || !Number.isFinite(value)) return MISSING;
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let current = Math.abs(value);
  let unit = 0;
  while (current >= 1000 && unit < units.length - 1) {
    current /= 1000;
    unit += 1;
  }
  const signed = value < 0 ? -current : current;
  return `${signed.toLocaleString(locale, { maximumFractionDigits: 2 })} ${units[unit]}`;
}

/** Scales to the unit that keeps the magnitude readable; every value carries its own unit. */
export function formatDuration(milliseconds: number | null | undefined, locale: string): string {
  if (milliseconds == null || !Number.isFinite(milliseconds)) return MISSING;
  const scaled = (value: number) => value.toLocaleString(locale, { maximumFractionDigits: 2 });
  const magnitude = Math.abs(milliseconds);
  if (magnitude === 0) return "0 ms";
  if (magnitude < 1) return `${scaled(milliseconds * 1000)} µs`;
  if (magnitude < 1000) return `${scaled(milliseconds)} ms`;
  if (magnitude < 60_000) return `${scaled(milliseconds / 1000)} s`;
  if (magnitude < 3_600_000) return `${scaled(milliseconds / 60_000)} min`;
  return `${scaled(milliseconds / 3_600_000)} h`;
}

export function formatPercent(value: number | null | undefined, locale: string, digits = 1): string {
  return value == null || !Number.isFinite(value)
    ? MISSING
    : `${value.toLocaleString(locale, { maximumFractionDigits: digits })} %`;
}

export function formatRate(value: number | null | undefined, unit: string, locale: string): string {
  return value == null || !Number.isFinite(value)
    ? MISSING
    : `${value.toLocaleString(locale, { maximumFractionDigits: 2 })} ${unit}`;
}

/** Share of successful lookups, or null when the counter pair carries no observation. */
export function hitRate(hits?: number | null, misses?: number | null): number | null {
  if (hits == null || misses == null) return null;
  const total = hits + misses;
  return total > 0 ? (hits * 100) / total : null;
}
