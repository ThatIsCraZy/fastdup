import type { CSSProperties, ReactNode } from "react";
import ReactECharts from "echarts-for-react";
import { useI18n } from "../i18n";
import { formatDuration } from "../format";

/**
 * Presentation primitives for every telemetry surface. One rule per value kind:
 *
 * - a value measured against a limit, or a share of a whole -> `Meter`
 * - a handful of headline scalars                           -> `StatGrid`
 * - repeating records with several dimensions               -> `DataTable`
 * - named parts of a single duration                        -> horizontal bar chart
 *
 * Primary values are always visible. Secondary, forensic values live in a
 * `Disclosure` and are always collapsed. Scope switches use `Segmented`,
 * optional extra table columns use `ColumnToggle` — never a bare checkbox.
 *
 * All labels are German source strings and are translated here, so call sites
 * pass plain text and pre-formatted values.
 */

export interface StatItem {
  label: string;
  value: ReactNode;
  hint?: string;
}

export function StatGrid({ items, columns = 3 }: { items: (StatItem | false | null | undefined)[]; columns?: number }) {
  const { t } = useI18n();
  const visible = items.filter((item): item is StatItem => Boolean(item));
  if (!visible.length) return null;
  return (
    <dl className="telemetry-values" style={{ "--stat-columns": columns } as CSSProperties}>
      {visible.map((item) => (
        <div key={item.label}>
          <dt>{t(item.label)}</dt>
          <dd>{item.value}</dd>
          {item.hint && <small>{t(item.hint)}</small>}
        </div>
      ))}
    </dl>
  );
}

/** A value against its limit. Renders no bar when either side is unavailable. */
export function Meter({
  label,
  ariaLabel,
  value,
  max,
  valueText,
  limitLabel = "von",
  limitText,
  hint,
  size = "default",
}: {
  label: string;
  ariaLabel?: string;
  value?: number | null;
  max?: number | null;
  valueText: string;
  limitLabel?: string;
  limitText?: string;
  hint?: string;
  size?: "default" | "large";
}) {
  const { t } = useI18n();
  return (
    <div className={`meter meter-${size}`}>
      <div className="meter-head">
        <span className="meter-label">{t(label)}</span>
        <strong className="meter-value">{valueText}</strong>
        {limitText && (
          <span className="meter-limit">
            {t(limitLabel)} <b>{limitText}</b>
          </span>
        )}
      </div>
      {value != null && max != null && (
        <progress aria-label={t(ariaLabel ?? label)} value={value} max={Math.max(1, max)} />
      )}
      {hint && <small className="meter-hint">{t(hint)}</small>}
    </div>
  );
}

/** A bounded share rendered inside a table cell, so rates stay comparable across rows. */
export function InlineMeter({ percent, text }: { percent: number | null; text: string }) {
  return (
    <span className="cache-hit-rate">
      {text}
      {percent != null && (
        <span className="cache-hit-track" aria-hidden="true">
          <span style={{ width: `${Math.min(100, Math.max(0, percent))}%` }} />
        </span>
      )}
    </span>
  );
}

export interface TelemetryColumn<T> {
  key: string;
  label: string;
  numeric?: boolean;
  render: (row: T) => ReactNode;
}

export function DataTable<T>({
  label,
  columns,
  rows,
  rowKey,
  empty,
  note,
}: {
  label?: string;
  columns: (TelemetryColumn<T> | false | null | undefined)[];
  rows: T[];
  rowKey: (row: T) => string;
  empty?: string;
  note?: string;
}) {
  const { t } = useI18n();
  const visible = columns.filter((column): column is TelemetryColumn<T> => Boolean(column));
  if (!rows.length) return empty ? <p className="detail-empty">{t(empty)}</p> : null;
  return (
    <>
      <div className="telemetry-table-scroll">
        <table aria-label={label ? t(label) : undefined}>
          <thead>
            <tr>
              {visible.map((column) => (
                <th key={column.key} scope="col" className={column.numeric ? "numeric" : undefined}>
                  {t(column.label)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={rowKey(row)}>
                {visible.map((column, index) =>
                  index === 0 ? (
                    <th key={column.key} scope="row">
                      {column.render(row)}
                    </th>
                  ) : (
                    <td key={column.key} className={column.numeric ? "numeric" : undefined}>
                      {column.render(row)}
                    </td>
                  ),
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {note && <p className="detail-note">{t(note)}</p>}
    </>
  );
}

export function Disclosure({ summary, note, children }: { summary: string; note?: string; children: ReactNode }) {
  const { t } = useI18n();
  return (
    <details className="telemetry-disclosure">
      <summary>{t(summary)}</summary>
      {note && <p className="detail-note">{t(note)}</p>}
      {children}
    </details>
  );
}

export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: { value: T; label: string }[];
  onChange: (value: T) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="segmented" role="group" aria-label={t(label)}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          onClick={() => onChange(option.value)}
        >
          {t(option.label)}
        </button>
      ))}
    </div>
  );
}

export function ColumnToggle({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  const { t } = useI18n();
  return (
    <label className="column-toggle">
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
      {t(label)}
    </label>
  );
}

export function PanelSection({
  title,
  description,
  aside,
  children,
}: {
  title?: string;
  description?: string;
  aside?: ReactNode;
  children: ReactNode;
}) {
  const { t } = useI18n();
  return (
    <section className="telemetry-section" aria-label={title ? t(title) : undefined}>
      {(title || description || aside) && (
        <div className="telemetry-section-head">
          <div>
            {title && <h3>{t(title)}</h3>}
            {description && <p className="detail-note">{t(description)}</p>}
          </div>
          {aside}
        </div>
      )}
      {children}
    </section>
  );
}

/** Named parts of one duration. Both checkpoint and collection phases use this. */
export function PhaseBars({ phases }: { phases: { id: string; label: string; ms: number }[] }) {
  const { t, locale } = useI18n();
  return (
    <ReactECharts
      style={{ height: Math.max(280, phases.length * 28) }}
      option={{
        animation: false,
        textStyle: { fontFamily: 'Inter, "Segoe UI", sans-serif' },
        grid: { left: 230, right: 30, top: 15, bottom: 35 },
        tooltip: { trigger: "axis", valueFormatter: (value: number) => formatDuration(value, locale) },
        xAxis: { type: "value", name: "ms", axisLabel: { color: "#afbecb" }, splitLine: { lineStyle: { color: "#253945" } } },
        yAxis: { type: "category", inverse: true, data: phases.map((phase) => t(phase.label)), axisLabel: { color: "#afbecb" } },
        series: [{ type: "bar", data: phases.map((phase) => phase.ms), itemStyle: { color: "#63c4d5" }, barMaxWidth: 16 }],
      }}
    />
  );
}
