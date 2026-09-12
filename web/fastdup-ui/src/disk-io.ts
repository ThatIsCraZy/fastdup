/** Keep small nonzero interval averages distinct from an idle device. */
export function formatQueueDepth(value: number | null | undefined, locale: string): string {
  if (value == null || !Number.isFinite(value) || value < 0) return "—";
  if (value > 0 && value < 0.01) return `< ${Number(0.01).toLocaleString(locale)}`;
  return value.toLocaleString(locale, { maximumFractionDigits: 2 });
}
