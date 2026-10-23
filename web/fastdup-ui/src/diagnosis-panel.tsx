import {useState} from "react";
import {Button} from "./components/ui/button";
import {useI18n} from "./i18n";
import {diagnose, histogramPercentile, sampleWindow, type EventWindow, type LatencyWindow} from "./history-diagnostics";
import type {TelemetrySnapshot} from "./types";

export function WindowLatencies({window}: {window?: LatencyWindow | null}) {
  const {t, locale} = useI18n();
  const time = (seconds: number) => new Date(seconds * 1000).toLocaleString(locale);
  return <section className="diagnostic-section">
    <h3>{t("Latenzen im Messfenster")}</h3>
    {!window ? <p>{t("Keine Intervallhistogramme vorhanden. Mount-Perzentile lassen sich nicht in Fenster-Perzentile umrechnen.")}</p> : <>
      <p>{time(window.from)} – {time(window.to)} · {window.coveredSeconds} s {t("erfasst")}</p>
      <p>{t("Perzentile aus zusammengeführten Histogrammen; Angaben sind Bucket-Obergrenzen. Spitzen bleiben auch bei wenigen langsamen Requests sichtbar.")}</p>
      <div className="diagnostic-table-scroll"><table><thead><tr><th>{t("Zugriff")}</th><th>p50</th><th>p95</th><th>p99</th><th>{t("Spitzen-Bucket")}</th><th>{t("Requests")}</th><th>{t("Fehler")}</th></tr></thead>
        <tbody>{(["read", "write"] as const).map(name => <tr key={name}><th>{name === "read" ? "Read" : "Write"}</th>
          {[50,95,99,100].map(percentile => {const value = histogramPercentile(window[name], percentile); return <td key={percentile}>{value ? `${value.overflow ? "> " : "≤ "}${(value.value / 1000).toLocaleString(locale)} ms` : "—"}</td>;})}
          <td>{window[name].counts.reduce((a,b) => a+b, 0).toLocaleString(locale)}</td><td>{window[name].errors}</td></tr>)}</tbody></table></div>
    </>}
  </section>;
}

export function DiagnosisPanel({sample, bucket, samples, events, onSelect, historical}: {
  sample?: TelemetrySnapshot; bucket?: TelemetrySnapshot; samples: TelemetrySnapshot[]; events: EventWindow | null;
  onSelect: (sample: TelemetrySnapshot) => void;
  historical: boolean;
}) {
  const {t, locale} = useI18n();
  const [workload, setWorkload] = useState("");
  const findings = sample ? diagnose(sample) : [];
  const observations = samples.flatMap(item => item.history?.incident ? [item, item.history.incident] : [item]);
  const options = [...new Set(observations.flatMap(item => [
    ...(item.workloads?.shares.map(s => `share:${s.name}`) ?? []),
    ...(item.workloads?.connections.flatMap(c => [`share:${c.share}`, `client:${c.client}`]) ?? []),
  ]))].sort();
  const matches = workload ? observations.filter(item => item.workloads?.connections.some(c => workload === `share:${c.share}` || workload === `client:${c.client}`)) : [];
  const context = sample?.workloads;
  const stale = context && sample && Math.abs(Date.parse(sample.observedAt) / 1000 - context.observedAt) > 20;
  const window = bucket && sampleWindow(bucket);
  const nearby = events?.events.filter(event => window && event.timestamp >= window.from - 30 && event.timestamp <= window.to + 30) ?? [];
  return <div className="diagnosis-panel">
    <WindowLatencies window={bucket?.history?.latency ?? sample?.details?.latencyWindow}/>
    <section className="diagnostic-section">
      <h3>{t("Begründete Diagnosehinweise")}</h3>
      <p>{t("Hinweise beziehen sich auf den ausgewählten Snapshot. Fehlende Hinweise sind kein Nachweis für störungsfreien Betrieb.")}</p>
      {!findings.length && <p>{t("Keine belegbaren Hinweise in diesem Messpunkt.")}</p>}
      {findings.map(finding => <article className="diagnostic-finding" key={finding.rule}>
        <h4>{t(finding.title)}</h4><p>{t(finding.explanation)}</p>
        <p>{new Date(finding.observedAt).toLocaleString(locale)} · {finding.runtimeId ?? "—"}</p>
        <ul>{finding.evidence.map(evidence => <li key={evidence.field}><code>{evidence.field}</code>: {String(evidence.value)} {evidence.unit}</li>)}</ul>
      </article>)}
    </section>
    <section className="diagnostic-section">
      <h3>{t("Workload-Kontext")}</h3>
      <p>{t("Engine-Messwerte gelten für die gesamte Appliance. Verbindungen zeigen zeitliche Überschneidungen, keine verursachergerechte Leistungszuordnung.")}</p>
      {context ? <>
        <p>{t("Messpunkt")}: {new Date(context.observedAt * 1000).toLocaleString(locale)} · Repository: {context.repository ?? "—"}</p>
        {stale && <p role="status">{t("Workload-Beobachtung ist veraltet.")}</p>}
        {context.smbStatus !== "available" && <p>{t("SMB-Verbindungen konnten nicht erfasst werden.")}</p>}
        {context.truncated && <p>{t("Verbindungsliste ist auf 256 Einträge begrenzt.")}</p>}
        <div className="diagnostic-table-scroll"><table><thead><tr><th>Share / {t("Speicherbereich")}</th><th>{t("Pfad")}</th><th>{t("Client")}</th><th>SMB Session</th></tr></thead>
          <tbody>{context.shares.map(share => <tr key={share.id}><td>{share.name} ({share.protocol})</td><td>{share.path}</td><td>—</td><td>—</td></tr>)}
            {context.connections.map((connection, index) => <tr key={index}><td>{connection.share}</td><td>{context.shares.find(share => share.name === connection.share)?.path ?? "—"}</td><td>{connection.client}</td><td>{connection.smbSessionId ?? "—"}</td></tr>)}</tbody></table></div>
        <p>Backup Sessions: {context.backupSessions?.join(", ") || t("Nicht verfügbar; SMB-Sessions sind keine Backup-Sessions.")}</p>
      </> : <p>{t("Für diesen Messpunkt wurde kein Workload-Kontext aufgezeichnet.")}</p>}
      <label>{t("Verbindungen im Zeitfenster finden")} <select value={workload} onChange={event => setWorkload(event.target.value)}><option value="">{t("Alle")}</option>{options.map(option => <option key={option} value={option}>{option}</option>)}</select></label>
      {workload && <div className="diagnostic-matches"><p>{matches.length} {t("gespeicherte Beobachtungen mit dieser Verbindung")}</p>
        {matches.slice(0, 50).map((item,index) => <Button key={`${item.observedAt}-${index}`} onClick={() => onSelect(item)}>{new Date(item.observedAt).toLocaleString(locale)}</Button>)}
        {matches.length > 50 && <p>{t("Erste 50 Treffer; für weitere Treffer das Zeitfenster eingrenzen.")}</p>}
      </div>}
    </section>
    <section className="diagnostic-section">
      <h3>{t("Ereignisse auf der Zeitachse")}</h3>
      <p>{t("Audit-Ereignisse und letzte Job-Aktualisierungen. Markierte Einträge liegen im ausgewählten Intervall oder höchstens 30 Sekunden daneben.")}</p>
      {!historical ? <p>{t("Für Ereignisse und Export einen historischen Zeitraum auswählen.")}</p> : !events ? <p>{t("Ereignisse konnten nicht geladen werden.")}</p> : <>
        {events.truncated && <p role="status">{t("Mehr als 1.000 Ereignisse: Zeitfenster eingrenzen. Der Export kennzeichnet die Begrenzung.")}</p>}
        {!events.events.length && <p>{t("Keine Ereignisse im Zeitfenster.")}</p>}
        <div className="diagnostic-event-list">{events.events.map(event => <button key={event.id} className={nearby.includes(event) ? "selected" : ""} onClick={() => {
          const nearest = observations.reduce<TelemetrySnapshot | undefined>((best, item) => !best || Math.abs(Date.parse(item.observedAt)/1000 - event.timestamp) < Math.abs(Date.parse(best.observedAt)/1000 - event.timestamp) ? item : best, undefined);
          if (nearest) onSelect(nearest);
        }}><time>{new Date(event.timestamp * 1000).toLocaleString(locale)}</time><span>{event.action} · {event.outcome}</span><small>{event.detail}</small></button>)}</div>
      </>}
    </section>
  </div>;
}
