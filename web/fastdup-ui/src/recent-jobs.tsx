import { useEffect, useRef, useState, type CSSProperties } from "react";
import { ChevronDown, ListChecks } from "lucide-react";
import { useI18n } from "./i18n";
import type { JobStatus } from "./types";

const labels: Record<string, string> = { provision:"Provisionierung",adopt:"Repository-Übernahme",mount:"Mount",unmount:"Unmount",offline_scrub:"Offline-Scrub",update_settings:"Einstellungen",upsert_share:"SMB-Freigabe",delete_share:"Share-Löschung" };
const states = { queued:"Wartet", running:"Läuft", succeeded:"Abgeschlossen", failed:"Fehlgeschlagen" };

export function RecentJobs({ jobs }: { jobs: JobStatus[] }) {
  const { t, locale } = useI18n();
  const [open, setOpen] = useState(false);
  const [height, setHeight] = useState(150);
  const [maxHeight, setMaxHeight] = useState(() => Math.max(96, Math.floor(window.innerHeight * 0.65)));
  const [resizing, setResizing] = useState(false);
  const drag = useRef<{ y: number; height: number } | null>(null);
  const clamp = (value: number) => Math.max(96, Math.min(maxHeight, Math.round(value)));
  useEffect(() => {
    const resize = () => {
      const maximum = Math.max(96, Math.floor(window.innerHeight * 0.65));
      setMaxHeight(maximum); setHeight(current => Math.min(current, maximum));
    };
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, []);
  useEffect(() => {
    if (!resizing) return;
    const cursor = document.body.style.cursor;
    const select = document.body.style.userSelect;
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    return () => { document.body.style.cursor = cursor; document.body.style.userSelect = select; };
  }, [resizing]);
  const recent = [...jobs].sort((a,b)=>b.createdAt-a.createdAt || b.updatedAt-a.updatedAt).slice(0,20);
  const running = recent.filter(job=>job.state==='running'||job.state==='queued').length;
  const failed = recent.filter(job=>job.state==='failed').length;
  return <section className={`recent-jobs ${open ? 'expanded' : ''}`} aria-label={t("Letzte Jobs")} style={{"--job-content-height": `${height}px`} as CSSProperties}>
    {open && <div className="recent-jobs-resizer" role="separator" aria-label={t("Höhe der Jobliste")}
      aria-orientation="horizontal" aria-valuemin={96} aria-valuemax={maxHeight} aria-valuenow={height}
      aria-valuetext={`${height} px`} aria-controls="recent-jobs-content" tabIndex={0}
      title={t("Ziehen, um die Höhe anzupassen. Pfeiltasten ändern die Höhe ebenfalls.")}
      onPointerDown={event => {
        if (event.button !== 0) return;
        event.preventDefault(); event.currentTarget.focus();
        drag.current = {y: event.clientY, height}; setResizing(true);
        event.currentTarget.setPointerCapture(event.pointerId);
      }}
      onPointerMove={event => { if (drag.current) setHeight(clamp(drag.current.height + drag.current.y - event.clientY)); }}
      onPointerUp={event => { drag.current = null; setResizing(false); event.currentTarget.releasePointerCapture(event.pointerId); }}
      onLostPointerCapture={() => { drag.current = null; setResizing(false); }}
      onKeyDown={event => {
        if (!["ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)) return;
        event.preventDefault(); setHeight(current => event.key === "Home" ? 96 : event.key === "End" ? maxHeight : clamp(current + (event.key === "ArrowUp" ? 24 : -24)));
      }}
    />}

    <button className="recent-jobs-toggle" aria-expanded={open} aria-controls="recent-jobs-content" onClick={()=>setOpen(value=>!value)}>
      <ListChecks size={18}/><strong>{t("Letzte Jobs")}</strong>
      <span>{running ? t("{count} aktiv", {count:running}) : t("Keine aktiven Jobs")}</span>
      {failed > 0 && <span className="recent-jobs-failed">{t("{count} fehlgeschlagen", {count:failed})}</span>}
      <ChevronDown size={17} className="recent-jobs-chevron" />
    </button>
    <div id="recent-jobs-content" hidden={!open} className="recent-jobs-content">
      {recent.length ? <table><thead><tr>{["Aufgabe","Status","Fortschritt","Gestartet","Aktualisiert","Details"].map(label=><th key={label}>{t(label)}</th>)}</tr></thead><tbody>{recent.map(job=><tr key={job.id}>
        <th>{t(labels[job.kind]??job.kind)}</th><td><span className={`job-state ${job.state}`}>{t(states[job.state])}</span></td>
        <td><div className="recent-job-progress"><progress aria-label={`${t(labels[job.kind]??job.kind)} ${t("Fortschritt")}`} max={10000} value={Math.max(0,Math.min(10000,job.progressBasisPoints))}/><span>{Math.round(job.progressBasisPoints/100)} %</span></div></td>
        <td>{new Date(job.createdAt*1000).toLocaleString(locale)}</td><td>{new Date(job.updatedAt*1000).toLocaleString(locale)}</td><td className="recent-job-message">{job.message}</td>
      </tr>)}</tbody></table> : <p className="detail-empty">{t("Noch keine Jobs vorhanden.")}</p>}
    </div>
  </section>;
}
