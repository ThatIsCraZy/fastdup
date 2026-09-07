import { useState } from "react";
import ReactECharts from "echarts-for-react";
import { useI18n } from "./i18n";
import type { TelemetrySnapshot } from "./types";

export interface OperationLatency { operations: number; errors: number; p50Micros: number; p95Micros: number; p99Micros: number }
export interface DetailTelemetry {
  latency?: { read: OperationLatency; write: OperationLatency } | null;
  runtime?: {
    runtimeId: string;
    cacheWindow?: {seconds: number; pools: {id: string; hits: number; misses: number; evictions: number}[]} | null;
    scrub?: {state: string; totalContainers: number; verifiedContainers: number; resumedContainers?: number; newlyVerifiedContainers?: number; remainingContainers?: number; verifiedBytes: number; readBytes: number; currentContainer?: string | null; error?: string | null} | null;
    cacheBudget?: {
      maximumMemoryUsedBasisPoints: number; effectiveLimitBytes: number; availableBytes: number; budgetBytes: number;
      pools: { id: string; fallbackTier: string; residentBytes: number; targetBytes: number; leasedBytes: number; hits: number; misses: number; evictions: number }[];
    } | null;
    ioUring: { ringEntries: number; inflightBytes: number; maxInflightBytes: number; peakInflightBytes: number; submitted: number; completed: number };
    caches: { id: string; hits: number; misses: number; evictions: number; residentBytes?: number | null; residentPages?: number | null }[];
    reduction: { enabled: boolean; queries: number; candidates: number; acceptedPrefixes: number; acceptedSparseXor: number; savedPayloadBytes: number; fallbacks: number; errors: number };
    checkpoint?: { completedAt: number; generation: number; totalMs: number; phases: { id: string; wallMs: number; cpuMs: number }[] } | null;
    gc?: { state: string; observedAt: number; totalMs?: number | null; readBytes?: number | null; writeBytes?: number | null; unlinkedBytes?: number | null; candidates?: number | null; victims?: number | null; abortedCandidates?: number | null } | null;
  } | null;
}

const tabs = ["Latenzen", "io_uring", "Caches", "GC & Reduction", "Checkpoint-Phasen"];
const cacheLabels: Record<string, string> = { verifiedRead: "Verified Read", exactIndex: "Exact Index", similarityIndex: "Similarity Index", containerDescriptors: "Container Descriptors", historicalProofs: "Historical Proofs" };
const phaseLabels: Record<string, string> = { freeze: "Freeze", cdc: "CDC", hashFill: "Hash / FILL", exactLookup: "Exact Lookup", encode: "Encoding", containerPublish: "Container Publish", indexPublish: "Index Publish", metadataCommit: "Metadata Commit" };

export function DetailTelemetryPanel({ sample, historical, loading }: { sample?: TelemetrySnapshot; historical: boolean; loading: boolean }) {
  const { t, locale } = useI18n();
  const [tab, setTab] = useState(0);
  const [cacheRange, setCacheRange] = useState<"total" | "5m">("total");
  const details = sample?.details;
  const runtime = details?.runtime;
  const number = (value?: number | null) => value == null ? "—" : value.toLocaleString(locale, { maximumFractionDigits: 2 });
  const bytes = (value?: number | null) => value == null ? "—" : value >= 1e9 ? `${number(value / 1e9)} GB` : value >= 1e6 ? `${number(value / 1e6)} MB` : value >= 1e3 ? `${number(value / 1e3)} KB` : `${number(value)} B`;
  const timestamp = (value: number) => new Date(value * 1000).toLocaleString(locale);
  const empty = <p className="detail-empty">{t("Runtime-Messdaten sind momentan nicht verfügbar. Die Anzeige wird automatisch aktualisiert.")}</p>;
  const rows = (values: [string, string][]) => <dl className="telemetry-values">{values.map(([label, value]) => <div key={label}><dt>{t(label)}</dt><dd>{value}</dd></div>)}</dl>;
  const checkpoint = runtime?.checkpoint;
  const gc = runtime?.gc;
  const reduction = runtime?.reduction;
  const budget = runtime?.cacheBudget;
  const scrub = runtime?.scrub;
  return <section className="detail-telemetry" aria-label={t("Detailtelemetrie")}>
    <div className="detail-telemetry-heading"><h2>{t("Detailtelemetrie")}</h2><span>{sample ? `${t(historical ? "Letzter Messpunkt im Zeitraum" : "Messpunkt")}: ${new Date(sample.observedAt).toLocaleString(locale)}` : t("Keine Messwerte im gewählten Zeitraum.")}</span></div>
    {scrub && <div className="detail-scrub" role={scrub.state === "failed" ? "alert" : "status"}>
      <strong>{t("Hintergrundprüfung")}: {t(({running:"Läuft",complete:"Abgeschlossen",failed:"Fehlgeschlagen",cancelled:"Unterbrochen"} as Record<string,string>)[scrub.state] ?? scrub.state)}</strong>
      <span>{number(scrub.verifiedContainers)} / {number(scrub.totalContainers)} {t("Container geprüft")} · {bytes(scrub.readBytes)} {t("gelesen")}</span>
      {scrub.resumedContainers !== undefined && <span>{number(scrub.resumedContainers)} {t("aus vorheriger Prüfung übernommen")} · {number(scrub.newlyVerifiedContainers ?? 0)} {t("neu geprüft")} · {number(scrub.remainingContainers ?? 0)} {t("noch ausstehend")}</span>}
      {scrub.state === "running" && <><progress aria-label={t("Hintergrundprüfung")} value={scrub.verifiedContainers} max={Math.max(1, scrub.totalContainers)} /><small>{t("Lesezugriffe werden vollständig geprüft. Die Hintergrundprüfung begrenzt ihre Last; automatische Speicherbereinigung wartet auf ihren Abschluss.")}</small></>}
      {scrub.state === "failed" && <small>{t("Datenprüfung fehlgeschlagen. Neue Schreibzugriffe sind gesperrt. Details stehen im Dienstprotokoll.")}</small>}
    </div>}
    <div className="detail-tabs" role="tablist" aria-label={t("Detailtelemetrie")}>
      {tabs.map((label, index) => <button key={label} role="tab" id={`detail-tab-${index}`} aria-controls="detail-panel" aria-selected={tab === index} tabIndex={tab === index ? 0 : -1} onClick={() => setTab(index)} onKeyDown={event => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
        setTab(next); document.getElementById(`detail-tab-${next}`)?.focus();
      }}>{t(label)}</button>)}
    </div>
    <div id="detail-panel" role="tabpanel" aria-labelledby={`detail-tab-${tab}`} tabIndex={0} aria-busy={loading}>
      {loading ? <p>{t("Lädt")}</p> : <>
        {tab === 0 && (details?.latency ? <>
          <p className="detail-note">{t("Histogramm-Perzentile seit dem Mount, inklusive fehlgeschlagener Requests. Werte sind Bucket-Obergrenzen, keine Intervallmittelwerte.")}</p>
          <div className="telemetry-table-scroll"><table><thead><tr>{["Operation", "p50", "p95", "p99", "Erfolgreich", "Fehler"].map(label => <th key={label}>{t(label)}</th>)}</tr></thead><tbody>
            {([['Read', details.latency.read], ['Write', details.latency.write]] as const).map(([name, item]) => <tr key={name}><th>{name}</th>{[item.p50Micros, item.p95Micros, item.p99Micros].map((value, index) => <td key={index}>{item.operations + item.errors === 0 ? "—" : value > 1e15 ? "> 100 ms" : `${number(value / 1000)} ms`}</td>)}<td>{number(item.operations)}</td><td>{number(item.errors)}</td></tr>)}
          </tbody></table></div>
        </> : empty)}
        {tab === 1 && (runtime ? <>
          <p className="detail-note">{t("Data-Tier io_uring: aktuelle Belegung und kumulative Zähler seit dem Mount.")}</p>
          {rows([["In-Flight", bytes(runtime.ioUring.inflightBytes)], ["In-Flight Limit", bytes(runtime.ioUring.maxInflightBytes)], ["Peak In-Flight", bytes(runtime.ioUring.peakInflightBytes)], ["Ring Entries", number(runtime.ioUring.ringEntries)], ["Submitted", number(runtime.ioUring.submitted)], ["Completed", number(runtime.ioUring.completed)]])}
          <progress aria-label={t("In-Flight Belegung")} value={runtime.ioUring.inflightBytes} max={Math.max(1, runtime.ioUring.maxInflightBytes)} />
        </> : empty)}
        {tab === 2 && (runtime ? <>
          {budget && <>
            <p className="detail-note">{t("Gemeinsames RAM-Budget: Die Verteilung folgt dem gemessenen Nutzen. Caches, die DATA-Zugriffe vermeiden, erhalten Vorrang.")}</p>
            {rows([["RAM-Obergrenze", `${number(budget.maximumMemoryUsedBasisPoints / 100)} %`], ["Effektives RAM", bytes(budget.effectiveLimitBytes)], ["Verfügbares RAM", bytes(budget.availableBytes)], ["Gemeinsames Cache-Budget", bytes(budget.budgetBytes)], ["Cache-Belegung", bytes(budget.pools.reduce((sum, pool) => sum + pool.residentBytes, 0))]])}
            <progress aria-label={t("Cache-Budget Belegung")} value={budget.pools.reduce((sum, pool) => sum + pool.residentBytes, 0)} max={Math.max(1, budget.budgetBytes)} />
          </>}
          <div className="cache-range" role="group" aria-label={t("Cache-Zeitraum")}><button aria-pressed={cacheRange === "5m"} onClick={() => setCacheRange("5m")}>{t("Letzte 5 Minuten")}</button><button aria-pressed={cacheRange === "total"} onClick={() => setCacheRange("total")}>{t("Gesamt seit Mount")}</button></div>
          <p className="detail-note">{t(cacheRange === "total" ? "Cache Hit Rates seit dem Mount. Ohne Zugriffe wird keine Rate angezeigt." : "Trefferrate und Zählerdifferenzen der letzten 5 Minuten vor diesem Messpunkt. RAM-Belegung und Budgets gelten zum Messpunkt.")}</p>
          {cacheRange === "5m" && <p className="detail-note">{runtime.cacheWindow?.seconds ? `${t("Erfasster Zeitraum")}: ${number(runtime.cacheWindow.seconds)} s` : t("Für dieses Zeitfenster sind noch keine Messdaten verfügbar.")}</p>}
          <div className="telemetry-table-scroll"><table><thead><tr>{["Cache", ...(budget ? ["Rückfall auf"] : []), "Hit Rate", "Hits", "Misses", "Evictions", "Belegung", ...(budget ? ["Zielbudget", "Reserviert"] : [])].map(label => <th key={label}>{t(label)}</th>)}</tr></thead><tbody>{(budget?.pools ?? runtime.caches).map(cache => {
            const pool = budget?.pools.find(item => item.id === cache.id);
            const counts = cacheRange === "total" ? cache : runtime.cacheWindow?.seconds ? runtime.cacheWindow.pools.find(pool => pool.id === cache.id) : undefined;
            return <tr key={cache.id}><th>{cacheLabels[cache.id] ?? cache.id}</th>{budget && <td>{pool?.fallbackTier === "data" ? "DATA" : pool?.fallbackTier === "metadata" ? "Metadata" : "—"}</td>}<td>{counts && counts.hits + counts.misses ? `${number(counts.hits * 100 / (counts.hits + counts.misses))} %` : "—"}</td><td>{number(counts?.hits)}</td><td>{number(counts?.misses)}</td><td>{number(counts?.evictions)}</td><td>{cache.residentBytes != null ? bytes(cache.residentBytes) : "residentPages" in cache && cache.residentPages != null ? `${number(cache.residentPages)} ${t("Seiten")}` : "—"}</td>{budget && <><td>{bytes(pool?.targetBytes)}</td><td>{bytes(pool?.leasedBytes)}</td></>}</tr>;
          })}</tbody></table></div>
          {budget && <p className="detail-note">{t("Zielbudget wird laufend angepasst. Reservierter Speicher wird erst nach der Verdrängung für andere Caches freigegeben. Die Belegung enthält Cache-Verwaltungsdaten.")}</p>}

        </> : empty)}
        {tab === 3 && (runtime ? <div className="detail-columns"><div><h3>{t("Letzter GC-Lauf")}</h3>{gc ? <><p className="detail-note">{timestamp(gc.observedAt)} · {t(({running:"Läuft",failed:"Fehlgeschlagen",noCandidates:"Keine Kandidaten",noProfitableCandidates:"Keine profitablen Kandidaten",catalogRebuilt:"Katalog erneuert",collected:"Abgeschlossen"} as Record<string,string>)[gc.state] ?? gc.state)}</p>{rows([["Dauer", gc.totalMs == null ? "—" : `${number(gc.totalMs)} ms`], ["Kandidaten", number(gc.candidates)], ["Geprüfte Victims", number(gc.victims)], ["Abgebrochene Kandidaten", number(gc.abortedCandidates)], ["Relocation Read", bytes(gc.readBytes)], ["Relocation Write", bytes(gc.writeBytes)], ["Unlinked", bytes(gc.unlinkedBytes)]])}</> : <p>{t("Seit dem Mount wurde noch kein GC-Lauf gestartet.")}</p>}</div>
          <div><h3>Advanced Reduction</h3><p className="detail-note">{t(reduction?.enabled ? "Aktiv · seit dem Mount" : "Deaktiviert · seit dem Mount")}</p>{reduction && rows([["Queries", number(reduction.queries)], ["Kandidaten", number(reduction.candidates)], ["Accepted Prefix", number(reduction.acceptedPrefixes)], ["Accepted Sparse-XOR", number(reduction.acceptedSparseXor)], ["Eingesparte Payload", bytes(reduction.savedPayloadBytes)], ["Independent Fallbacks", number(reduction.fallbacks)], ["Fehler", number(reduction.errors)]])}</div></div> : empty)}
        {tab === 4 && (checkpoint ? <>
          <p className="detail-note">{t("Letzter abgeschlossener Checkpoint")}: {timestamp(checkpoint.completedAt)} · Generation {number(checkpoint.generation)} · {number(checkpoint.totalMs)} ms</p>
          <ReactECharts style={{height:280}} option={{animation:false,textStyle:{fontFamily:'Inter, "Segoe UI", sans-serif'},grid:{left:145,right:30,top:15,bottom:35},tooltip:{trigger:'axis',valueFormatter:(value:number)=>`${number(value)} ms`},xAxis:{type:'value',name:'ms',axisLabel:{color:'#afbecb'},splitLine:{lineStyle:{color:'#253945'}}},yAxis:{type:'category',inverse:true,data:checkpoint.phases.map(phase=>phaseLabels[phase.id]??phase.id),axisLabel:{color:'#afbecb'}},series:[{type:'bar',data:checkpoint.phases.map(phase=>phase.wallMs),itemStyle:{color:'#63c4d5'},barMaxWidth:16}]}} />
          <div className="telemetry-table-scroll"><table><thead><tr><th>{t("Phase")}</th><th>Wall time</th><th>Process CPU</th></tr></thead><tbody>{checkpoint.phases.map(phase=><tr key={phase.id}><th>{phaseLabels[phase.id]??phase.id}</th><td>{number(phase.wallMs)} ms</td><td>{number(phase.cpuMs)} ms</td></tr>)}</tbody></table></div>
          <p className="detail-note">{t("Process CPU umfasst alle während der Phase aktiven Threads. Die Phasen bilden nicht die gesamte Checkpoint-Dauer ab.")}</p>
        </> : runtime ? <p className="detail-empty">{t("Seit dem Mount wurde noch kein Checkpoint abgeschlossen.")}</p> : empty)}
      </>}
    </div>
  </section>;
}
