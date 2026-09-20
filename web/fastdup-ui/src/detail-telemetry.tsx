import { useState } from "react";
import { PipelineTelemetryPanel, checkpointPhaseLabels as phaseLabels, type PipelineTelemetry } from "./pipeline-telemetry";
import { useI18n } from "./i18n";
import { formatBytes, formatCount, formatDuration, formatPercent, formatRate, hitRate } from "./format";
import {
  ColumnToggle,
  DataTable,
  Disclosure,
  InlineMeter,
  Meter,
  PanelSection,
  PhaseBars,
  Segmented,
  StatGrid,
  type TelemetryColumn,
} from "./components/telemetry";
import type { TelemetrySnapshot } from "./types";

export interface OperationLatency { operations: number; errors: number; p50Micros: number; p95Micros: number; p99Micros: number }
export interface DetailTelemetry {
  latency?: { read: OperationLatency; write: OperationLatency } | null;
  runtime?: {
    runtimeId: string;
    pipeline?: PipelineTelemetry | null;
    allocatorMemory?: {arenaBytes: number; allocatedBytes: number; freeBytes: number; anonymousResidentBytes: number; trimAttempts: number; lastTrimMicros: number} | null;
    metadataReads?: {intervalSeconds: number; rows: {reason: string; object: string; mode: string; operations: number; requestedBytes: number; returnedBytes: number; errors: number; elapsedMicros: number; maxMicros: number; inFlight: number; operationsPerSecond: number; requestedMbps: number}[]} | null;
    codecBuffers?: {retainedBytes: number; activeBytes: number; peakActiveBytes: number; hits: number; misses: number; evictions: number} | null;
    readCacheCompression?: {
      decodedResidentBytes: number; compressedResidentBytes: number; compressedLogicalBytes: number;
      attempts: number; admissions: number; compressionNanos: number; hits: number; decompressions: number;
      decompressionNanos: number; promotions: number; demotions: number; failures: number; bypasses: number;
      workingBytes: number; peakWorkingBytes: number; maxWorkingBytes: number;
    } | null;
    cacheWindow?: {seconds: number; pools: {id: string; hits: number; misses: number; evictions: number}[]} | null;
    scrub?: {state: string; totalContainers: number; verifiedContainers: number; resumedContainers?: number; newlyVerifiedContainers?: number; remainingContainers?: number; verifiedBytes: number; readBytes: number; currentContainer?: string | null; error?: string | null} | null;
    cacheBudget?: {
      maximumMemoryUsedBasisPoints: number; effectiveLimitBytes: number; availableBytes: number; budgetBytes: number;
      pools: { id: string; fallbackTier: string; residentBytes: number; targetBytes: number; leasedBytes: number; hits: number; misses: number; evictions: number }[];
    } | null;
    ioUring: { ringEntries: number; inflightBytes: number; maxInflightBytes: number; peakInflightBytes: number; submitted: number; completed: number };
    caches: { id: string; hits: number; misses: number; evictions: number; residentBytes?: number | null; residentPages?: number | null }[];
    reduction: { skippedColdCandidates?: number; explorationReads?: number; backendBaseReads?: number; warmBaseReuses?: number; successfulBaseTrials?: number; enabled: boolean; queries: number; candidates: number; acceptedPrefixes: number; acceptedSparseXor: number; savedPayloadBytes: number; fallbacks: number; errors: number };
    checkpoint?: { completedAt: number; generation: number; totalMs: number; unattributedMs?: number | null; phases: { id: string; wallMs: number; cpuMs: number }[] } | null;
    gc?: {
      state: string; observedAt: number; totalMs?: number | null; readBytes?: number | null; writeBytes?: number | null;
      unlinkedBytes?: number | null; candidates?: number | null; victims?: number | null; abortedCandidates?: number | null;
      phasesMs?: Record<string, number> | null;
      metadataGc?: {
        markMode: string; exactReason?: string | null; wallMs: number; barrierWaitMs: number;
        objectGraphReadBytes: number; candidateReadBytes: number; catalogReadBytes: number;
        catalogWriteBytes: number; unlinkedBytes: number; rootSyncs: number; catalogChainRuns: number;
      } | null;
      catalogExaminedBytes?: number | null; catalogWriteBytes?: number | null; candidateProofReadBytes?: number | null;
      reverseDependencyEdges?: number | null; reverseDependencyRequiredChunks?: number | null;
      candidateQueueRetained?: number | null; candidateQueueScannedRows?: number | null;
      catalogPendingUpdates?: number | null; exactRetirementMs?: number | null;
      exactRunsRetired?: number | null; exactRunSetsRetired?: number | null;
    } | null;
    exactCache?: { protectedLimitBytes: number; protectedResidentBytes: number } | null;
    exactMembership?: {
      leasedRuns: number; filters: number; constructedFilters: number; missingFilters: number;
      pageBoundsRuns: number; missingPageBounds: number; pageBoundsBytes: number;
      probes: number; definitelyAbsent: number; requiresExactLookup: number;
    } | null;
    exactWarm?: { state: string } | null;
  } | null;
}

/**
 * Seven views over one runtime sample. Every view follows the same shape:
 * a titled section with its question, the headline values as a meter or a stat
 * grid, repeating records as a table, and forensic detail behind a collapsed
 * disclosure. See `components/telemetry` for the presentation rules.
 */
const tabs = ["Latenzen", "io_uring", "Metadata-Reads", "Caches", "Lesevermeidung", "GC & Scrub", "Checkpoint & Pipeline"];
const LATENCY = 0, IO_URING = 1, METADATA_READS = 2, CACHES = 3, REDUCTION = 4, MAINTENANCE = 5, CHECKPOINT = 6;
/** Tab indices by name, so callers never hard-code a position in `tabs`. */
export const detailTabs = {
  latency: LATENCY, ioUring: IO_URING, metadataReads: METADATA_READS, caches: CACHES,
  reduction: REDUCTION, maintenance: MAINTENANCE, checkpoint: CHECKPOINT,
} as const;
const tabGroups = [
  { label: "Antwortzeiten & I/O", indices: [LATENCY, IO_URING, METADATA_READS] },
  { label: "RAM & Lesevermeidung", indices: [CACHES, REDUCTION] },
  { label: "Wartung & Commit", indices: [MAINTENANCE, CHECKPOINT] },
];
const tabOrder = tabGroups.flatMap(group => group.indices);
const tabHints = [
  "Wie lange dauern Dateizugriffe?",
  "Wie stark ist die asynchrone DATA-Verarbeitung belegt?",
  "Welche Lesewege reichen Anfragen an das Betriebssystem weiter?",
  "Welche Caches vermeiden Backend-Zugriffe und wie viel RAM nutzen sie?",
  "Welche Vergleichsversuche vermeiden DATA-Lesezugriffe?",
  "Was prüfen und bereinigen die Hintergrundprozesse?",
  "Wie verteilt sich die Checkpoint-Dauer und wo wartet die Schreibannahme?",
];

const scrubStates: Record<string, string> = { running: "Läuft", complete: "Abgeschlossen", failed: "Fehlgeschlagen", cancelled: "Unterbrochen" };
const gcStates: Record<string, string> = { running: "Läuft", failed: "Fehlgeschlagen", noCandidates: "Keine Kandidaten", noProfitableCandidates: "Keine profitablen Kandidaten", catalogRebuilt: "Katalog erneuert", collected: "Abgeschlossen", metadataOnly: "Nur Metadaten", dataOnly: "Nur DATA" };
const gcPhaseLabels: Record<string, string> = {
  recovery: "Recovery", metadataGc: "Metadata-GC", candidateCatalog: "Kandidatenkatalog",
  candidateProof: "Kandidaten prüfen", relocation: "Umlagerung", retiringActivation: "Rückzug aktivieren",
  pinDrain: "Pins abwarten", victimVerify: "Victims prüfen", unlink: "Container entfernen",
  dataSync: "DATA synchronisieren", removedActivation: "Entfernung aktivieren",
  postCollectionCatalog: "Katalog nachführen",
};
const markModes: Record<string, string> = {
  reused: "Katalog wiederverwendet", addition_delta: "Nur Zugänge nachgetragen",
  catalog_compaction: "Katalog verdichtet", exact_snapshot: "Exakter Katalog neu aufgebaut",
};
const exactReasons: Record<string, string> = {
  process_start: "Prozessstart", unclassified_publication: "Nicht zugeordnete Publikation",
  metadata_root_pin_drain: "Metadata-Root-Pins abgewartet", wal_rotation: "WAL-Rotation",
  uncertain_wal_durability: "WAL-Dauerhaftigkeit unklar", delta_chain_limit: "Delta-Kette zu lang",
  recovery_checkpoint_pin_change: "Recovery-Checkpoint-Pin geändert",
};
const warmStates: Record<string, string> = {
  warmed: "Index vorgewärmt", cancelled: "Abgebrochen", "no-new-demand": "Kein neuer Bedarf",
};
const metadataReasons: Record<string,string> = {other:"Nicht zugeordnet",indexLookup:"Index-Abfrage · Cache-Miss",indexCompaction:"Index-Zusammenführung",indexAudit:"Index-Prüfung",indexEnvelope:"Index-Header / Footer",manifest:"Manifest lesen",namespace:"Namespace / Verwaltungsgraph",recoveryScrub:"Recovery / Scrub",garbageCollection:"Garbage Collection"};
const metadataObjects: Record<string,string> = {exactIndex:"Exact Index",similarityIndex:"Similarity Index",metadataObject:"Metadatenobjekt",smallFile:"Small-File-Container",control:"Commit / Journal / Verwaltung",other:"Weitere Dateien"};
const metadataModes: Record<string,string> = {directRange:"Direkter Bereich",directFile:"Direkte Datei",directStructure:"Direkte Struktur",directLease:"Direkter Zugriff mit Dateilease",bufferedRange:"Gepufferter Bereich",bufferedFile:"Gepufferte Datei",bufferedStructure:"Gepufferte Struktur",mmap:"mmap"};
const cacheLabels: Record<string, string> = { unifiedRead: "Unified Read Cache", locationProofs: "Location-Nachweise", verifiedRead: "Verified Read", exactIndex: "Exact Index", similarityIndex: "Similarity Index", containerDescriptors: "Container Descriptors", historicalProofs: "Historical Proofs", manifestNodes: "Manifest Nodes", metadataObjects: "Metadata Objects" };
const cacheDescriptions: Record<string, string> = {
  unifiedRead: "Gemeinsamer Speicher für Nutzdaten, Metadaten, Indexe und geprüfte Nachweise",
  locationProofs: "Kompakte Nachweise bereits geprüfter Speicherorte für Dedup und Commit",
  verifiedRead: "Geprüfte Nutzdaten für Lesen und Vergleichsbasen",
  exactIndex: "Zuordnung von Chunk-IDs zu Speicherorten",
  similarityIndex: "Kandidaten für ähnliche Daten",
  containerDescriptors: "Aufbau und Speicherorte im Container",
  historicalProofs: "Bereits geprüfte Container-Nachweise",
  manifestNodes: "Geprüfte Dateibereiche für Lesen und Fast Clone",
  metadataObjects: "Unveränderliche Namespace- und Manifest-Objekte",
};

type CachePool = { id: string; hits: number; misses: number; evictions: number; residentBytes?: number | null; residentPages?: number | null };

export function DetailTelemetryPanel({ sample, historical, loading, initialTab = LATENCY }: { sample?: TelemetrySnapshot; historical: boolean; loading: boolean; initialTab?: number }) {
  const { t, locale } = useI18n();
  const [tab, setTab] = useState(initialTab);
  const [cacheRange, setCacheRange] = useState<"total" | "5m">("total");
  const [cacheCounters, setCacheCounters] = useState(false);
  const [metadataTotals, setMetadataTotals] = useState(false);
  const details = sample?.details;
  const runtime = details?.runtime;
  const count = (value?: number | null) => formatCount(value, locale);
  const bytes = (value?: number | null) => formatBytes(value, locale);
  const duration = (value?: number | null) => formatDuration(value, locale);
  const percent = (value?: number | null) => formatPercent(value, locale);
  const timestamp = (value: number) => new Date(value * 1000).toLocaleString(locale);
  const empty = <p className="detail-empty">{t("Runtime-Messdaten sind momentan nicht verfügbar. Die Anzeige wird automatisch aktualisiert.")}</p>;

  const checkpoint = runtime?.checkpoint;
  const nestedPhases = new Set(["cdc", "hashFill", "exactLookup", "encode", "containerPublish"]);
  const chartPhases = checkpoint?.unattributedMs != null
    ? [...checkpoint.phases.filter(phase => !nestedPhases.has(phase.id)), {id:"unattributed", wallMs:checkpoint.unattributedMs}]
    : checkpoint?.phases ?? [];
  const gc = runtime?.gc;
  const metadataGc = gc?.metadataGc;
  const gcPhases = Object.entries(gc?.phasesMs ?? {})
    .filter(([, ms]) => Number.isFinite(ms))
    .map(([id, ms]) => ({ id, label: gcPhaseLabels[id] ?? id, ms }));
  const reduction = runtime?.reduction;
  const budget = runtime?.cacheBudget;
  const scrub = runtime?.scrub;
  const compression = runtime?.readCacheCompression;
  const metadataReads = runtime?.metadataReads;
  const ioUring = runtime?.ioUring;
  const residentTotal = budget?.pools.reduce((sum, pool) => sum + pool.residentBytes, 0) ?? 0;
  // Budget-governed pools carry the tier and the reservation; the remaining caches
  // report their own counters and would otherwise never be shown at all.
  const budgetPools = (budget?.pools ?? []).filter(pool => pool.id !== "codecBuffers");
  const ungoverned = (runtime?.caches ?? []).filter(cache => !budgetPools.some(pool => pool.id === cache.id));
  const pools: CachePool[] = [...budgetPools, ...ungoverned].sort((a, b) => {
    const tier = (id: string) => budgetPools.find(pool => pool.id === id)?.fallbackTier;
    return Number(tier(b.id) === "data") - Number(tier(a.id) === "data");
  });
  const windowCounts = (id: string) => cacheRange === "total"
    ? pools.find(pool => pool.id === id)
    : runtime?.cacheWindow?.seconds ? runtime.cacheWindow.pools.find(pool => pool.id === id) : undefined;

  const latencyRows = details?.latency
    ? ([["Read", details.latency.read], ["Write", details.latency.write]] as const).map(([name, item]) => ({ name, item }))
    : [];
  const percentile = (item: OperationLatency, value: number) =>
    item.operations + item.errors === 0 ? "—" : value > 1e15 ? "> 100 ms" : duration(value / 1000);
  const latencyColumns: TelemetryColumn<{ name: string; item: OperationLatency }>[] = [
    { key: "operation", label: "Operation", render: row => row.name },
    { key: "p50", label: "p50", numeric: true, render: row => percentile(row.item, row.item.p50Micros) },
    { key: "p95", label: "p95", numeric: true, render: row => percentile(row.item, row.item.p95Micros) },
    { key: "p99", label: "p99", numeric: true, render: row => percentile(row.item, row.item.p99Micros) },
    { key: "operations", label: "Erfolgreich", numeric: true, render: row => count(row.item.operations) },
    { key: "errors", label: "Fehler", numeric: true, render: row => count(row.item.errors) },
  ];

  const poolColumns: (TelemetryColumn<CachePool> | false)[] = [
    { key: "cache", label: "Cache", render: pool => <>
      <span>{cacheLabels[pool.id] ?? pool.id}</span>
      {cacheDescriptions[pool.id] && <small className="cache-description">{t(cacheDescriptions[pool.id])}</small>}
    </> },
    Boolean(budget) && { key: "tier", label: "Rückfall auf", render: pool => {
      const tier = budgetPools.find(item => item.id === pool.id)?.fallbackTier;
      return <span className={`cache-tier ${tier === "data" ? "data" : "metadata"}`}>
        {pool.id === "unifiedRead" ? "Metadata + DATA" : tier === "data" ? "DATA" : tier === "metadata" ? "Metadata" : "—"}
      </span>;
    } },
    { key: "rate", label: "Hit Rate", numeric: true, render: pool => {
      const counts = windowCounts(pool.id);
      const rate = hitRate(counts?.hits, counts?.misses);
      return <InlineMeter percent={rate} text={percent(rate)} />;
    } },
    cacheCounters && { key: "hits", label: "Hits", numeric: true, render: pool => count(windowCounts(pool.id)?.hits) },
    cacheCounters && { key: "misses", label: "Misses", numeric: true, render: pool => count(windowCounts(pool.id)?.misses) },
    cacheCounters && { key: "evictions", label: "Evictions", numeric: true, render: pool => count(windowCounts(pool.id)?.evictions) },
    { key: "resident", label: "Belegung", numeric: true, render: pool => pool.residentBytes != null
      ? bytes(pool.residentBytes)
      : pool.residentPages != null ? `${count(pool.residentPages)} ${t("Seiten")}` : "—" },
    Boolean(budget) && { key: "target", label: "Zielbudget", numeric: true, render: pool => bytes(budgetPools.find(item => item.id === pool.id)?.targetBytes) },
    Boolean(budget) && cacheCounters && { key: "leased", label: "Reserviert", numeric: true, render: pool => bytes(budgetPools.find(item => item.id === pool.id)?.leasedBytes) },
  ];

  const metadataRows = [...(metadataReads?.rows ?? [])].sort((a, b) => b.requestedMbps - a.requestedMbps || b.requestedBytes - a.requestedBytes);
  const metadataInterval = (metadataReads?.intervalSeconds ?? 0) > 0;
  const metadataColumns: (TelemetryColumn<(typeof metadataRows)[number]> | false)[] = [
    { key: "reason", label: "Ursache", render: row => t(metadataReasons[row.reason] ?? row.reason) },
    { key: "object", label: "Daten", render: row => t(metadataObjects[row.object] ?? row.object) },
    { key: "mode", label: "Zugriffsweg", render: row => t(metadataModes[row.mode] ?? row.mode) },
    { key: "mbps", label: "Angefordert · MB/s", numeric: true, render: row => metadataInterval ? formatRate(row.requestedMbps, "", locale).trim() : "—" },
    { key: "ops", label: "Aufrufe/s", numeric: true, render: row => metadataInterval ? count(row.operationsPerSecond) : "—" },
    { key: "inflight", label: "Laufende Reads", numeric: true, render: row => row.mode === "mmap" ? "—" : count(row.inFlight) },
    metadataTotals && { key: "operations", label: "Aufrufe", numeric: true, render: row => count(row.operations) },
    metadataTotals && { key: "requested", label: "Angefordert", numeric: true, render: row => bytes(row.requestedBytes) },
    metadataTotals && { key: "returned", label: "Erfolgreich geliefert", numeric: true, render: row => bytes(row.returnedBytes) },
    metadataTotals && { key: "errors", label: "Fehler", numeric: true, render: row => count(row.errors) },
    metadataTotals && { key: "average", label: "Ø Lesezeit", numeric: true, render: row => row.mode === "mmap" || !row.operations ? "—" : duration(row.elapsedMicros / row.operations / 1000) },
    metadataTotals && { key: "maximum", label: "Max. Lesezeit", numeric: true, render: row => row.mode === "mmap" || !row.operations ? "—" : duration(row.maxMicros / 1000) },
  ];
  const metadataMbps = metadataRows.reduce((sum, row) => sum + row.requestedMbps, 0);
  const metadataOperations = metadataRows.reduce((sum, row) => sum + row.operationsPerSecond, 0);
  const metadataInFlight = metadataRows.filter(row => row.mode !== "mmap").reduce((sum, row) => sum + row.inFlight, 0);

  const checkpointColumns: TelemetryColumn<{ id: string; wallMs: number; cpuMs: number }>[] = [
    { key: "phase", label: "Phase", render: phase => t(phaseLabels[phase.id] ?? phase.id) },
    { key: "wall", label: "Wall time", numeric: true, render: phase => duration(phase.wallMs) },
    { key: "cpu", label: "Process CPU", numeric: true, render: phase => duration(phase.cpuMs) },
  ];

  const warmBaseRate = hitRate(reduction?.warmBaseReuses, reduction?.backendBaseReads);
  const membership = runtime?.exactMembership;
  const membershipRate = membership && membership.probes
    ? (membership.definitelyAbsent * 100) / membership.probes
    : null;
  const gcForensics = [
    { label: "Katalog untersucht", value: bytes(gc?.catalogExaminedBytes) },
    { label: "Katalog geschrieben", value: bytes(gc?.catalogWriteBytes) },
    { label: "Kandidatennachweise gelesen", value: bytes(gc?.candidateProofReadBytes) },
    { label: "Rückwärtskanten", value: count(gc?.reverseDependencyEdges) },
    { label: "Benötigte Chunks", value: count(gc?.reverseDependencyRequiredChunks) },
    { label: "Kandidatenwarteschlange", value: count(gc?.candidateQueueRetained) },
    { label: "Durchsuchte Warteschlangenzeilen", value: count(gc?.candidateQueueScannedRows) },
    { label: "Offene Katalogänderungen", value: count(gc?.catalogPendingUpdates) },
    { label: "Exact-Rückzug", value: duration(gc?.exactRetirementMs) },
    { label: "Zurückgezogene Runs", value: count(gc?.exactRunsRetired) },
    { label: "Zurückgezogene Run-Sets", value: count(gc?.exactRunSetsRetired) },
  ];

  return <section className="detail-telemetry" aria-label={t("Detailtelemetrie")}>
    <div className="detail-telemetry-heading">
      <h2>{t("Ursachen & Details")}</h2>
      <span>
        {sample ? `${t(historical ? "Letzter Messpunkt im Zeitraum" : "Messpunkt")}: ${new Date(sample.observedAt).toLocaleString(locale)}` : t("Keine Messwerte im gewählten Zeitraum.")}
        {runtime?.runtimeId && ` · ${t("Runtime")} ${runtime.runtimeId}`}
      </span>
    </div>
    {scrub && <div className="detail-scrub" role={scrub.state === "failed" ? "alert" : "status"}>
      <strong>{t("Hintergrundprüfung")}: {t(scrubStates[scrub.state] ?? scrub.state)}</strong>
      <span>{count(scrub.verifiedContainers)} / {count(scrub.totalContainers)} {t("Container geprüft")} · {bytes(scrub.readBytes)} {t("gelesen")}</span>
      {scrub.resumedContainers !== undefined && <span>{count(scrub.resumedContainers)} {t("aus vorheriger Prüfung übernommen")} · {count(scrub.newlyVerifiedContainers ?? 0)} {t("neu geprüft")} · {count(scrub.remainingContainers ?? 0)} {t("noch ausstehend")}</span>}
      {scrub.state === "running" && <><progress aria-label={t("Hintergrundprüfung")} value={scrub.verifiedContainers} max={Math.max(1, scrub.totalContainers)} /><small>{t("Lesezugriffe werden vollständig geprüft. Die Hintergrundprüfung begrenzt ihre Last; automatische Speicherbereinigung wartet auf ihren Abschluss.")}</small></>}
      {scrub.state === "failed" && <small>{t("Datenprüfung fehlgeschlagen. Neue Schreibzugriffe sind gesperrt. Details stehen im Dienstprotokoll.")}{scrub.error ? ` · ${scrub.error}` : ""}</small>}
    </div>}
    <div className="detail-tabs detail-tab-groups" role="tablist" aria-label={t("Detailtelemetrie")}>
      {tabGroups.map(group => <div className="detail-tab-group" role="presentation" key={group.label}><span className="detail-tab-group-label">{t(group.label)}</span><div role="presentation">{group.indices.map(index => <button key={tabs[index]} role="tab" id={`detail-tab-${index}`} aria-controls="detail-panel" aria-selected={tab === index} tabIndex={tab === index ? 0 : -1} onClick={() => setTab(index)} onKeyDown={event => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        const position = tabOrder.indexOf(index);
        const next = tabOrder[event.key === "Home" ? 0 : event.key === "End" ? tabOrder.length - 1 : (position + (event.key === "ArrowRight" ? 1 : -1) + tabOrder.length) % tabOrder.length];
        setTab(next); document.getElementById(`detail-tab-${next}`)?.focus();
      }}>{t(tabs[index])}</button>)}</div></div>)}
    </div>
    <div id="detail-panel" role="tabpanel" aria-labelledby={`detail-tab-${tab}`} tabIndex={0} aria-busy={loading}>
      <p className="detail-panel-hint">{t(tabHints[tab])}</p>
      {loading ? <p>{t("Lädt")}</p> : <>

        {tab === LATENCY && (details?.latency ? <PanelSection
          title="Antwortzeiten der Dateizugriffe"
          description="Histogramm-Perzentile seit dem Mount, inklusive fehlgeschlagener Requests. Werte sind Bucket-Obergrenzen, keine Intervallmittelwerte."
        >
          <DataTable label="Antwortzeiten" columns={latencyColumns} rows={latencyRows} rowKey={row => row.name} />
        </PanelSection> : empty)}

        {tab === IO_URING && (ioUring ? <PanelSection
          title="Data-Tier io_uring"
          description="Aktuelle Belegung des asynchronen DATA-Pfads; die Zähler laufen seit dem Mount."
        >
          <Meter
            label="In-Flight"
            ariaLabel="In-Flight Belegung"
            value={ioUring.inflightBytes}
            max={ioUring.maxInflightBytes}
            valueText={bytes(ioUring.inflightBytes)}
          />
          <StatGrid columns={3} items={[
            { label: "In-Flight Limit", value: bytes(ioUring.maxInflightBytes) },
            { label: "Peak In-Flight", value: bytes(ioUring.peakInflightBytes) },
            { label: "Ring Entries", value: count(ioUring.ringEntries) },
            { label: "Submitted", value: count(ioUring.submitted) },
            { label: "Completed", value: count(ioUring.completed) },
            { label: "Offene Vorgänge", value: count(Math.max(0, ioUring.submitted - ioUring.completed)) },
          ]} />
        </PanelSection> : empty)}

        {tab === METADATA_READS && (metadataReads ? <PanelSection
          title="Metadata-Lesewege"
          description="Direkte Backend-Reads nach Ursache. Der Unified Read Cache bedient wiederverwendbare Inhalte vor dem Backend. Angeforderte Bereiche enthalten keinen Ausrichtungs- oder Format-Overhead; physische MB/s und IOPS stehen bei den Laufwerken."
          aside={<ColumnToggle label="Summen und Lesezeiten seit Mount anzeigen" checked={metadataTotals} onChange={setMetadataTotals} />}
        >
          <StatGrid columns={4} items={[
            { label: "Messintervall", value: metadataInterval ? `${count(metadataReads.intervalSeconds)} s` : t("Erster Messpunkt · Raten noch nicht verfügbar") },
            { label: "Angefordert · Summe", value: metadataInterval ? formatRate(metadataMbps, "MB/s", locale) : "—" },
            { label: "Aufrufe/s · Summe", value: metadataInterval ? count(metadataOperations) : "—" },
            { label: "Laufende Reads · Summe", value: count(metadataInFlight) },
          ]} />
          <DataTable
            label="Metadata-Lesewege"
            columns={metadataColumns}
            rows={metadataRows}
            rowKey={row => `${row.reason}/${row.object}/${row.mode}`}
            empty="Noch keine Metadata-Reads erfasst."
            note="Lesezeiten erfassen den direkten Backend-Aufruf einschließlich Pufferaufbau. Das Öffnen der Datei ist nicht enthalten. Separate Metadatenzugriffe des Host-Dateisystems werden hier nicht gezählt."
          />
        </PanelSection> : empty)}

        {tab === CACHES && (runtime ? <>
          {budget && <PanelSection
            title="Gemeinsames RAM-Budget"
            description="Alle Caches teilen sich ein Budget. Wiederverwendete DATA-Inhalte erhalten mehr Schutz bei der Verdrängung."
          >
            <Meter
              label="Cache-Belegung"
              ariaLabel="Cache-Budget Belegung"
              value={residentTotal}
              max={budget.budgetBytes}
              valueText={bytes(residentTotal)}
            />
            <StatGrid columns={4} items={[
              { label: "RAM-Obergrenze", value: percent(budget.maximumMemoryUsedBasisPoints / 100) },
              { label: "Effektives RAM", value: bytes(budget.effectiveLimitBytes) },
              { label: "Verfügbares RAM", value: bytes(budget.availableBytes) },
              { label: "Gemeinsames Cache-Budget", value: bytes(budget.budgetBytes) },
            ]} />
          </PanelSection>}
          {runtime.exactCache && <PanelSection
            title="Exact Index · geschütztes RAM"
            description="Diese Index-Seiten bleiben von der Verdrängung ausgenommen, damit ein Lookup ein RAM-Zugriff bleibt."
          >
            <Meter
              label="Geschützt im RAM"
              ariaLabel="Geschütztes Index-RAM"
              value={runtime.exactCache.protectedResidentBytes}
              max={runtime.exactCache.protectedLimitBytes}
              valueText={bytes(runtime.exactCache.protectedResidentBytes)}
              limitText={bytes(runtime.exactCache.protectedLimitBytes)}
            />
          </PanelSection>}
          <PanelSection
            title="Cache-Wirkung"
            description="Treffer vermeiden Zugriffe auf das angegebene Tier. RAM-Belegung und Budgets gelten immer zum Messpunkt."
            aside={<div className="section-controls">
              <Segmented
                label="Cache-Zeitraum"
                value={cacheRange}
                onChange={setCacheRange}
                options={[{ value: "5m", label: "Letzte 5 Minuten" }, { value: "total", label: "Gesamt seit Mount" }]}
              />
              <ColumnToggle label="Zähler & Reservierung anzeigen" checked={cacheCounters} onChange={setCacheCounters} />
            </div>}
          >
            <p className="detail-note">{t(cacheRange === "total" ? "Cache Hit Rates seit dem Mount. Ohne Zugriffe wird keine Rate angezeigt." : "Trefferrate und Zählerdifferenzen der letzten 5 Minuten vor diesem Messpunkt.")}</p>
            {cacheRange === "5m" && <p className="detail-note">{runtime.cacheWindow?.seconds ? `${t("Erfasster Zeitraum")}: ${count(runtime.cacheWindow.seconds)} s` : t("Für dieses Zeitfenster sind noch keine Messdaten verfügbar.")}</p>}
            <DataTable
              label="Cache-Wirkung"
              columns={poolColumns}
              rows={pools}
              rowKey={pool => pool.id}
              empty="Keine Cache-Pools im Messpunkt."
              note={budget ? "Zielbudget wird laufend angepasst. Reservierter Speicher wird erst nach der Verdrängung für andere Caches freigegeben. Die Belegung enthält Cache-Verwaltungsdaten." : undefined}
            />
          </PanelSection>
          {compression && <PanelSection
            title="Verified Read · RAM-Kompression"
            description="Geprüfte Nutzdaten bleiben komprimiert im RAM, wenn das Speicher spart; andernfalls bleiben sie unkomprimiert. Beide Darstellungen teilen sich das gemeinsame Cache-Budget."
          >
            <StatGrid columns={5} items={[
              { label: "Direkt im RAM", value: bytes(compression.decodedResidentBytes) },
              { label: "Komprimiert im RAM", value: bytes(compression.compressedResidentBytes) },
              { label: "Darin enthaltene Nutzdaten", value: bytes(compression.compressedLogicalBytes) },
              { label: "RAM durch Kompression gespart", value: bytes(Math.max(0, compression.compressedLogicalBytes - compression.compressedResidentBytes)) },
              { label: "Cache-Kompressionsfaktor", value: compression.compressedResidentBytes ? `${count(compression.compressedLogicalBytes / compression.compressedResidentBytes)}×` : "—" },
            ]} />
            <Disclosure
              summary="Kompressionskosten · seit Mount"
              note="Komprimierte Treffer benötigen keinen DATA-Zugriff. Gleichzeitig aktive Leser können eine Dekompression teilen. Die Arbeitsreserve begrenzt temporäre Codec-Puffer und zählt separat zur Cache-Belegung."
            >
              <StatGrid columns={4} items={[
                { label: "Kompressionsversuche", value: count(compression.attempts) },
                { label: "Komprimiert aufgenommen", value: count(compression.admissions) },
                { label: "Annahmequote", value: compression.attempts ? percent(compression.admissions * 100 / compression.attempts) : "—" },
                { label: "Treffer auf komprimierte Einträge", value: count(compression.hits) },
                { label: "Dekompressionen", value: count(compression.decompressions) },
                { label: "Ø Dekompression inkl. Prüfung", value: compression.decompressions ? duration(compression.decompressionNanos / compression.decompressions / 1e6) : "—" },
                { label: "Ø Kompressionsversuch", value: compression.attempts ? duration(compression.compressionNanos / compression.attempts / 1e6) : "—" },
                { label: "In direkte Darstellung übernommen", value: count(compression.promotions) },
                { label: "Wieder komprimiert", value: count(compression.demotions) },
                { label: "Kompression ausgelassen", value: count(compression.bypasses) },
                { label: "Ungültige Cache-Einträge", value: count(compression.failures) },
                { label: "Spitze der Codec-Arbeitsreserve", value: bytes(compression.peakWorkingBytes) },
              ]} />
              <Meter
                label="Codec-Arbeitsreserve"
                value={compression.workingBytes}
                max={compression.maxWorkingBytes}
                valueText={bytes(compression.workingBytes)}
                limitText={bytes(compression.maxWorkingBytes)}
              />
            </Disclosure>
          </PanelSection>}
          <div className="cache-advanced">
            {runtime.codecBuffers && <Disclosure
              summary="Wiederverwendbare Codec-Puffer"
              note="Freie Puffer nutzen das gemeinsame RAM-Budget nachrangig. Aktive Puffer können auch von Cache-Einträgen oder Lesern gehalten werden; die Werte werden nicht addiert. Zähler gelten seit dem Mount."
            >
              <StatGrid columns={2} items={[
                { label: "Freie Puffer im Pool", value: bytes(runtime.codecBuffers.retainedBytes) },
                { label: "Puffer in Verwendung", value: bytes(runtime.codecBuffers.activeBytes) },
                { label: "Spitze in Verwendung", value: bytes(runtime.codecBuffers.peakActiveBytes) },
                { label: "Puffer wiederverwendet", value: count(runtime.codecBuffers.hits) },
                { label: "Neue Puffer angelegt", value: count(runtime.codecBuffers.misses) },
                { label: "Puffer freigegeben", value: count(runtime.codecBuffers.evictions) },
                { label: "Zielbudget", value: bytes(budget?.pools.find(pool => pool.id === "codecBuffers")?.targetBytes) },
              ]} />
            </Disclosure>}
            {runtime.allocatorMemory && <Disclosure
              summary="Prozessspeicher und Allocator"
              note="Allocator-Werte enthalten Caches und Arbeitsspeicher. Freie Blöcke können bereits aus dem RAM entfernt sein; diese Werte werden nicht addiert. Messung im Hintergrund, normalerweise alle 30 Sekunden."
            >
              <StatGrid columns={2} items={[
                { label: "Anonymes RAM", value: bytes(runtime.allocatorMemory.anonymousResidentBytes) },
                { label: "Vom Allocator belegt", value: bytes(runtime.allocatorMemory.allocatedBytes) },
                { label: "Freie Allocator-Blöcke", value: bytes(runtime.allocatorMemory.freeBytes) },
                { label: "Allocator-Arenen", value: bytes(runtime.allocatorMemory.arenaBytes) },
                { label: "RAM-Bereinigungen", value: count(runtime.allocatorMemory.trimAttempts) },
                { label: "Letzte RAM-Bereinigung", value: duration(runtime.allocatorMemory.lastTrimMicros / 1000) },
              ]} />
            </Disclosure>}
          </div>
        </> : empty)}

        {tab === REDUCTION && (reduction ? <>
          <PanelSection
            title="Ähnlichkeitsvergleich & Lesevermeidung"
            description="Die Similarity-Auswahl vermeidet unprofitable DATA-Leseversuche. Warme Basen kommen aus dem Verified-Read-Cache; Stichproben halten die Auswahl lernfähig."
            aside={<span className="cache-tier">{t(reduction.enabled ? "Aktiv · seit dem Mount" : "Deaktiviert · seit dem Mount")}</span>}
          >
            <Meter
              label="Basen aus RAM statt aus DATA"
              value={reduction.warmBaseReuses}
              max={warmBaseRate == null ? null : (reduction.warmBaseReuses ?? 0) + (reduction.backendBaseReads ?? 0)}
              valueText={percent(warmBaseRate)}
              hint="Anteil der Vergleichsbasen, die ohne Backend-Leseversuch bereitstanden"
            />
            <StatGrid columns={3} items={[
              { label: "Kalte Kandidaten übersprungen", value: count(reduction.skippedColdCandidates) },
              { label: "Basen aus RAM wiederverwendet", value: count(reduction.warmBaseReuses) },
              { label: "Backend-Leseversuche (Basen)", value: count(reduction.backendBaseReads) },
            ]} />
          </PanelSection>
          <div className="detail-columns">
            <PanelSection title="Auswahl & Lernverhalten">
              <StatGrid columns={2} items={[
                { label: "Queries", value: count(reduction.queries) },
                { label: "Kandidaten", value: count(reduction.candidates) },
                { label: "Stichproben-Leseversuche", value: count(reduction.explorationReads) },
                { label: "Basisversuche mit Mehrgewinn", value: count(reduction.successfulBaseTrials) },
              ]} />
            </PanelSection>
            <PanelSection title="Ergebnis der Kodierung">
              <StatGrid columns={2} items={[
                { label: "Accepted Prefix", value: count(reduction.acceptedPrefixes) },
                { label: "Accepted Sparse-XOR", value: count(reduction.acceptedSparseXor) },
                { label: "Eingesparte Payload", value: bytes(reduction.savedPayloadBytes) },
                { label: "Independent Fallbacks", value: count(reduction.fallbacks) },
                { label: "Fehler", value: count(reduction.errors) },
              ]} />
            </PanelSection>
          </div>
          <p className="detail-note">{t("Die Zähler beschreiben Versuche seit dem Mount, keine Bytes oder physische I/Os. Stichproben sind Teil der Backend-Leseversuche; eingesparte Payload beschreibt nur Advanced Reduction.")}</p>
          {membership && <PanelSection
            title="Exact-Index · vermiedene Lookups"
            description="Membership-Filter beantworten „liegt nicht vor“ ohne eine Index-Seite zu lesen. Nur der Rest wird im Index nachgeschlagen."
            aside={runtime?.exactWarm && <span className="cache-tier">{t(warmStates[runtime.exactWarm.state] ?? runtime.exactWarm.state)}</span>}
          >
            <Meter
              label="Ohne Index-Lookup beantwortet"
              value={membership.definitelyAbsent}
              max={membership.probes}
              valueText={percent(membershipRate)}
              hint="Anteil der Prüfungen, die der Filter allein entscheiden konnte"
            />
            <StatGrid columns={3} items={[
              { label: "Prüfungen", value: count(membership.probes) },
              { label: "Sicher nicht vorhanden", value: count(membership.definitelyAbsent) },
              { label: "Index-Lookup nötig", value: count(membership.requiresExactLookup) },
            ]} />
            <Disclosure
              summary="Filterabdeckung der Index-Runs"
              note="Fehlende Filter oder Seitengrenzen erzwingen den vollständigen Index-Lookup, bis sie aufgebaut sind."
            >
              <StatGrid columns={4} items={[
                { label: "Geleaste Runs", value: count(membership.leasedRuns) },
                { label: "Filter vorhanden", value: count(membership.filters) },
                { label: "Filter aufgebaut", value: count(membership.constructedFilters) },
                { label: "Filter fehlen", value: count(membership.missingFilters) },
                { label: "Runs mit Seitengrenzen", value: count(membership.pageBoundsRuns) },
                { label: "Seitengrenzen fehlen", value: count(membership.missingPageBounds) },
                { label: "Seitengrenzen im RAM", value: bytes(membership.pageBoundsBytes) },
              ]} />
            </Disclosure>
          </PanelSection>}
        </> : empty)}

        {tab === MAINTENANCE && (runtime ? <>
          <PanelSection title="Letzter GC-Lauf">
            {gc ? <>
              <p className="detail-note">{timestamp(gc.observedAt)} · {t(gcStates[gc.state] ?? gc.state)}</p>
              <StatGrid columns={4} items={[
                { label: "Dauer", value: duration(gc.totalMs) },
                { label: "Kandidaten", value: count(gc.candidates) },
                { label: "Geprüfte Victims", value: count(gc.victims) },
                { label: "Abgebrochene Kandidaten", value: count(gc.abortedCandidates) },
                { label: "Relocation Read", value: bytes(gc.readBytes) },
                { label: "Relocation Write", value: bytes(gc.writeBytes) },
                { label: "Unlinked", value: bytes(gc.unlinkedBytes) },
              ]} />
            </> : <p className="detail-note">{t("Seit dem Mount wurde noch kein GC-Lauf gestartet.")}</p>}
          </PanelSection>
        {gcPhases.length > 0 && <PanelSection
          title="Dauer des letzten GC-Laufs nach Phase"
          description="Die Phasen laufen nacheinander; zusammen ergeben sie die Gesamtdauer des Laufs."
        >
          <PhaseBars phases={gcPhases} />
        </PanelSection>}
        {metadataGc && <PanelSection
          title="Metadata-GC im selben Lauf"
          description="Namespace- und Manifestobjekte werden im selben Zyklus bereinigt. Nur ein exakt aufgebauter Katalog darf löschen."
          aside={<span className="cache-tier">{t(markModes[metadataGc.markMode] ?? metadataGc.markMode)}</span>}
        >
          {metadataGc.exactReason && <p className="detail-note">{t("Exakter Katalog nötig")}: {t(exactReasons[metadataGc.exactReason] ?? metadataGc.exactReason)}</p>}
          <StatGrid columns={4} items={[
            { label: "Dauer", value: duration(metadataGc.wallMs) },
            { label: "Auf Barriere gewartet", value: duration(metadataGc.barrierWaitMs) },
            { label: "Objektgraph gelesen", value: bytes(metadataGc.objectGraphReadBytes) },
            { label: "Unlinked", value: bytes(metadataGc.unlinkedBytes) },
          ]} />
          <Disclosure summary="Katalogarbeit der Metadata-GC">
            <StatGrid columns={3} items={[
              { label: "Kandidaten gelesen", value: bytes(metadataGc.candidateReadBytes) },
              { label: "Katalog gelesen", value: bytes(metadataGc.catalogReadBytes) },
              { label: "Katalog geschrieben", value: bytes(metadataGc.catalogWriteBytes) },
              { label: "Root-Syncs", value: count(metadataGc.rootSyncs) },
              { label: "Katalog-Kettenläufe", value: count(metadataGc.catalogChainRuns) },
            ]} />
          </Disclosure>
        </PanelSection>}
        {gc && gcForensics.some(item => item.value !== "—") && <Disclosure
          summary="Katalog, Abhängigkeiten und Exact-Rückzug"
          note="Zähler des letzten Laufs. Sie beschreiben geprüfte Arbeit, keine physischen I/Os."
        >
          <StatGrid columns={4} items={gcForensics} />
        </Disclosure>}
          <PanelSection title="Hintergrundprüfung">
            {scrub ? <>
              <Meter
                label="Geprüfte Container"
                value={scrub.verifiedContainers}
                max={scrub.totalContainers}
                valueText={count(scrub.verifiedContainers)}
                limitText={count(scrub.totalContainers)}
                hint={scrub.currentContainer ? "Aktuell geprüfter Container" : undefined}
              />
              {scrub.currentContainer && <p className="detail-note detail-code">{scrub.currentContainer}</p>}
              <StatGrid columns={5} items={[
                { label: "aus vorheriger Prüfung übernommen", value: count(scrub.resumedContainers) },
                { label: "neu geprüft", value: count(scrub.newlyVerifiedContainers) },
                { label: "noch ausstehend", value: count(scrub.remainingContainers) },
                { label: "Geprüfte Container-Bytes", value: bytes(scrub.verifiedBytes) },
                { label: "Gelesene Bytes", value: bytes(scrub.readBytes) },
              ]} />
              {scrub.error && <p className="detail-note" role="note">{scrub.error}</p>}
            </> : <p className="detail-note">{t("Keine Messdaten zur Hintergrundprüfung verfügbar.")}</p>}
          </PanelSection>
        </> : empty)}

        {tab === CHECKPOINT && <>
          {runtime?.pipeline && <PipelineTelemetryPanel pipeline={runtime.pipeline} />}
          {checkpoint ? <PanelSection
            title="Letzter abgeschlossener Checkpoint"
            description={checkpoint.unattributedMs != null
              ? "Das Diagramm zeigt getrennte Hauptphasen. CDC, Hash, Exact Lookup, Encoding und Container Publish sind Teil der Manifestplanung. Sonstige Verwaltung ist die verbleibende Gesamtdauer. Process CPU umfasst alle Prozess-Threads während einer Phase."
              : "Process CPU umfasst alle während der Phase aktiven Threads. Die Phasen bilden nicht die gesamte Checkpoint-Dauer ab."}
          >
            <StatGrid columns={3} items={[
              { label: "Abgeschlossen", value: timestamp(checkpoint.completedAt) },
              { label: "Generation", value: count(checkpoint.generation) },
              { label: "Gesamtdauer", value: duration(checkpoint.totalMs) },
            ]} />
            <PhaseBars phases={chartPhases.map(phase => ({id: phase.id, label: phaseLabels[phase.id] ?? phase.id, ms: phase.wallMs}))} />
            <DataTable label="Checkpoint-Phasen" columns={checkpointColumns} rows={checkpoint.phases} rowKey={phase => phase.id} />
          </PanelSection> : runtime ? <p className="detail-empty">{t("Seit dem Mount wurde noch kein Checkpoint abgeschlossen.")}</p> : empty}
        </>}

      </>}
    </div>
  </section>;
}
