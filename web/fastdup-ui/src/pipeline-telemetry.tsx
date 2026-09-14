import { useI18n } from "./i18n";

export interface PipelineTelemetry {
  admission: { open: boolean; reason?: string | null; closures: number; closedMs: number; currentClosedMs: number; maximumClosedMs: number };
  operations: { id: string; active: number; completed: number; totalMs: number; maximumMs: number; busyMs: number }[];
}

export const checkpointPhaseLabels: Record<string, string> = {
  checkpointLock: "Checkpoint-Sperre", proofFreeze: "Nachweise einfrieren", cutCapture: "Commit-Grenze erfassen",
  freeze: "Freeze", ingestWait: "Auf Ingest warten", publicationWait: "Auf Publikation am Cut warten",
  laneLock: "Ingest-Lane übernehmen", stableExtract: "Stabile Chunks abarbeiten",
  publicationEnqueue: "Auf Publikationsplatz warten", publicationRetire: "Auf Publikationsabschluss warten",
  recipeAttach: "Referenzen übernehmen", writerSetup: "Writer vorbereiten", manifestPlan: "Manifest planen · gesamt",
  cdc: "CDC", hashFill: "Hash / FILL", exactLookup: "Exact Lookup", encode: "Encoding",
  containerPublish: "Container Publish", indexPublish: "Index Publish", metadataCommit: "Metadata Commit",
  unattributed: "Sonstige Verwaltung",
};
const operationLabels: Record<string, string> = {
  checkpointTotal: "Checkpoint · gesamt", checkpointCheckpointLock: checkpointPhaseLabels.checkpointLock,
  ...Object.fromEntries(Object.entries(checkpointPhaseLabels).map(([id, label]) => [`checkpoint${id[0].toUpperCase()}${id.slice(1)}`, label])),
  exactEnqueue: "Exact · Queue-Platz", exactQueueWait: "Exact · bis Verarbeitung",
  exactPublish: "Exact · Verarbeitung gesamt", exactFlush: "Exact · Abschluss abwarten",
  exactGenerationLock: "Exact · Generation-Sperre", exactRecover: "Exact · Vorgänger auswählen",
  exactValidate: "Exact · Übergänge prüfen", exactRunPublish: "Exact · Run schreiben",
  exactPublishBatch: "Exact · Gemeinsame Publikation",
  exactGenerationDiscovery: "Exact · Run-Generationen rekonstruieren",
  exactCompaction: "Exact · Kompaktierung", exactActivation: "Exact · Aktivierung",
};
const reasonLabels: Record<string, string> = {
  checkpointTimeout: "Checkpoint über fünf Sekunden", dirtyPressure: "Dirty-DATA-Grenze erreicht",
  durabilityLag: "Dauerhafter Fortschritt verzögert", progressFailure: "Dauerhafter Fortschritt fehlgeschlagen",
  integrityFailure: "Integritätsfehler", shutdown: "Dienst wird beendet", unspecified: "Ohne Zuordnung",
};

export function PipelineTelemetryPanel({ pipeline }: { pipeline: PipelineTelemetry }) {
  const { t, locale } = useI18n();
  const number = (value: number) => value.toLocaleString(locale, { maximumFractionDigits: 2 });
  const seconds = (ms: number) => `${number(ms / 1000)} s`;
  const admission = pipeline.admission;
  const active = pipeline.operations.filter(operation => operation.active > 0);
  return <div aria-label={t("Checkpoint und Schreibannahme")}>
    <h3>{t("Schreibannahme zum Messpunkt")}: {t(admission.open ? "Offen" : "Gesperrt")}</h3>
    {!admission.open && admission.reason && <p>{t(reasonLabels[admission.reason] ?? admission.reason)}</p>}
    <dl className="telemetry-values">
      {[["Aktuelle Sperre", seconds(admission.currentClosedMs)], ["Sperren seit Mount", number(admission.closures)],
        ["Gesperrt seit Mount", seconds(admission.closedMs)], ["Längste Sperre", seconds(admission.maximumClosedMs)]].map(([label, value]) => <div key={label}><dt>{t(label)}</dt><dd>{value}</dd></div>)}
    </dl>
    <h3>{t("Laufende Arbeit und Wartezeiten")}</h3>
    <p className="detail-note">{t("Aktiv seit misst die ununterbrochene Belegung zum Messpunkt. Übergeordnete und untergeordnete Phasen können gleichzeitig aktiv sein.")}</p>
    {active.length ? <div className="telemetry-table-scroll"><table aria-label={t("Laufende Pipeline-Phasen")}>
      <thead><tr><th>{t("Phase")}</th><th>{t("Laufende Vorgänge")}</th><th>{t("Aktiv seit")}</th></tr></thead>
      <tbody>{active.map(operation => <tr key={operation.id}><th>{t(operationLabels[operation.id] ?? operation.id)}</th><td>{number(operation.active)}</td><td>{seconds(operation.busyMs)}</td></tr>)}</tbody>
    </table></div> : <p>{t("Zum Messpunkt keine aktive Pipeline-Phase.")}</p>}
    <details className="telemetry-disclosure">
      <summary>{t("Pipeline-Zeiten seit Mount")}</summary>
      <p className="detail-note">{t("Dauern zählen beendete Vorgänge einschließlich Fehlerpfaden. Parallele und verschachtelte Zeiten nicht addieren. Exact · bis Verarbeitung enthält auch das Warten auf Queue-Platz; Exact-Speicherphasen enthalten Online GC.")}</p>
      <div className="telemetry-table-scroll"><table aria-label={t("Kumulative Pipeline-Zeiten")}>
        <thead><tr><th>{t("Phase")}</th><th>{t("Beendete Vorgänge")}</th><th>{t("Gesamtdauer")}</th><th>{t("Mittlere Dauer")}</th><th>{t("Maximale Dauer")}</th></tr></thead>
        <tbody>{pipeline.operations.map(operation => <tr key={operation.id}>
          <th>{t(operationLabels[operation.id] ?? operation.id)}</th><td>{number(operation.completed)}</td>
          <td>{seconds(operation.totalMs)}</td><td>{operation.completed ? seconds(operation.totalMs / operation.completed) : "—"}</td>
          <td>{operation.completed ? seconds(operation.maximumMs) : "—"}</td>
        </tr>)}</tbody>
      </table></div>
    </details>
  </div>;
}
