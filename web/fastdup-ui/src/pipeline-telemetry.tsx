import { useI18n } from "./i18n";
import { formatCount, formatDuration } from "./format";
import { DataTable, Disclosure, PanelSection, StatGrid, type TelemetryColumn } from "./components/telemetry";

export interface PipelineTelemetry {
  admission: { open: boolean; reason?: string | null; closures: number; closedMs: number; currentClosedMs: number; maximumClosedMs: number };
  operations: { id: string; active: number; completed: number; totalMs: number; maximumMs: number; busyMs: number }[];
}

export const checkpointPhaseLabels: Record<string, string> = {
  checkpointLock: "Checkpoint-Sperre", proofFreeze: "Nachweise einfrieren", cutCapture: "Commit-Grenze erfassen",
  freeze: "Freeze", ingestWait: "Auf Ingest warten", publicationWait: "Auf Publikation am Cut warten",
  laneLock: "Ingest-Lane übernehmen", stableExtract: "Stabile Chunks abarbeiten",
  drainResidue: "Restmenge abarbeiten",
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

type Operation = PipelineTelemetry["operations"][number];

export function PipelineTelemetryPanel({ pipeline }: { pipeline: PipelineTelemetry }) {
  const { t, locale } = useI18n();
  const count = (value: number) => formatCount(value, locale);
  const duration = (value: number) => formatDuration(value, locale);
  const admission = pipeline.admission;
  const active = pipeline.operations.filter(operation => operation.active > 0);
  const label = (operation: Operation) => t(operationLabels[operation.id] ?? operation.id);

  const activeColumns: TelemetryColumn<Operation>[] = [
    { key: "phase", label: "Phase", render: label },
    { key: "active", label: "Laufende Vorgänge", numeric: true, render: operation => count(operation.active) },
    { key: "busy", label: "Aktiv seit", numeric: true, render: operation => duration(operation.busyMs) },
  ];
  const cumulativeColumns: TelemetryColumn<Operation>[] = [
    { key: "phase", label: "Phase", render: label },
    { key: "completed", label: "Beendete Vorgänge", numeric: true, render: operation => count(operation.completed) },
    { key: "total", label: "Gesamtdauer", numeric: true, render: operation => duration(operation.totalMs) },
    { key: "average", label: "Mittlere Dauer", numeric: true, render: operation => operation.completed ? duration(operation.totalMs / operation.completed) : "—" },
    { key: "maximum", label: "Maximale Dauer", numeric: true, render: operation => operation.completed ? duration(operation.maximumMs) : "—" },
  ];

  return <div aria-label={t("Checkpoint und Schreibannahme")}>
    <PanelSection
      title="Schreibannahme zum Messpunkt"
      description={admission.open
        ? "Neue Mutationen werden angenommen."
        : "Neue Mutationen warten, bis der Auslöser abgearbeitet ist."}
      aside={<span className={`status-pill ${admission.open ? "open" : "closed"}`}>{t(admission.open ? "Offen" : "Gesperrt")}</span>}
    >
      {!admission.open && admission.reason && <p className="detail-note">{t(reasonLabels[admission.reason] ?? admission.reason)}</p>}
      <StatGrid columns={4} items={[
        { label: "Aktuelle Sperre", value: duration(admission.currentClosedMs) },
        { label: "Sperren seit Mount", value: count(admission.closures) },
        { label: "Gesperrt seit Mount", value: duration(admission.closedMs) },
        { label: "Längste Sperre", value: duration(admission.maximumClosedMs) },
      ]} />
    </PanelSection>
    <PanelSection
      title="Laufende Arbeit und Wartezeiten"
      description="Aktiv seit misst die ununterbrochene Belegung zum Messpunkt. Übergeordnete und untergeordnete Phasen können gleichzeitig aktiv sein."
    >
      <DataTable
        label="Laufende Pipeline-Phasen"
        columns={activeColumns}
        rows={active}
        rowKey={operation => operation.id}
        empty="Zum Messpunkt keine aktive Pipeline-Phase."
      />
      <Disclosure
        summary="Pipeline-Zeiten seit Mount"
        note="Dauern zählen beendete Vorgänge einschließlich Fehlerpfaden. Parallele und verschachtelte Zeiten nicht addieren. Exact · bis Verarbeitung enthält auch das Warten auf Queue-Platz; Exact-Speicherphasen enthalten Online GC."
      >
        <DataTable
          label="Kumulative Pipeline-Zeiten"
          columns={cumulativeColumns}
          rows={pipeline.operations}
          rowKey={operation => operation.id}
          empty="Seit dem Mount wurden noch keine Pipeline-Phasen gemessen."
        />
      </Disclosure>
    </PanelSection>
  </div>;
}
