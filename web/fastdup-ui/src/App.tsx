import { WebUsersSettings, CertificateSettings } from "./settings-access";
import { RecentJobs } from "./recent-jobs";
import { DetailTelemetryPanel } from "./detail-telemetry";
import { appendResourceSample, resourceChartOption, type ResourceSample } from "./resource-history";
import { I18nProvider, useI18n, type UiLanguage } from "./i18n";
import {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import applianceMark from "./assets/fastdup-mark-impuls.svg";
import ReactECharts from "echarts-for-react";
import {
  Activity,
  AlertTriangle,
  Bell,
  ArrowDownToLine,
  ArrowUpFromLine,
  Layers3,
  FolderSymlink,
  ScrollText,
  SlidersHorizontal,
  Check,
  CheckCircle2,
  ChevronDown,
  CircleGauge,
  Clock3,
  Cpu,
  Database,
  FileStack,
  Gauge,
  HardDrive,
  KeyRound,
  LayoutDashboard,
  LockKeyhole,
  LoaderCircle,
  LogOut,
  MemoryStick,
  Network,
  Pencil,
  Play,
  Plus,
  RefreshCcw,
  Save,
  ServerCog,
  ShieldCheck,
  TerminalSquare,
  Trash2,
  Unplug,
  UserRound,
  X,
} from "lucide-react";
import { Badge } from "./components/ui/badge";
import { Button } from "./components/ui/button";
import { Card, CardContent, CardHeader } from "./components/ui/card";
import {
  type AuditEvent,
  type ApplianceSnapshot,
  type BlockTarget,
  type DiskTelemetry,
  type JobStatus,
  type RepositorySettings,
  type SessionInfo,
  type ShareSettings,
  type TelemetrySnapshot,
  emptyApplianceSnapshot,
} from "./types";

const navigation = [
  ["Übersicht", LayoutDashboard],
  ["Repository", Layers3],
  ["Laufwerke", HardDrive],
  ["SMB-Freigaben", FolderSymlink],
  ["Telemetrie", Activity],
  ["Ereignisse", ScrollText],
  ["Einstellungen", SlidersHorizontal],
] as const;

const stateLabels: Record<string, string> = {
  uninitialized: "Nicht initialisiert",
  provisioning: "Provisionierung",
  unmounted: "Unmounted",
  mounting: "Wird gemountet",
  recovering: "Recovery",
  online: "Online",
  unmounting: "Wird ausgehängt",
  scrubbing: "Offline-Scrub",
  error: "Fehler",
};

function repositoryTone(state: string) {
  return state === "error" ? "error" : state === "online" ? "healthy" : state === "unmounted" || state === "uninitialized" ? "neutral" : "warning";
}

const jobLabels: Record<string, string> = {
  provision: "Provisionierung",
  adopt: "Repository-Übernahme",
  mount: "Mount",
  unmount: "Unmount",
  offline_scrub: "Offline-Scrub",
  update_settings: "Einstellungen",
  upsert_share: "SMB-Freigabe",
  delete_share: "Share-Löschung",
};

type NoticeTone = "working" | "success" | "error";

interface Notice {
  id: string;
  tone: NoticeTone;
  title: string;
  message: string;
}

function LanguageSelector({ value, onChange, disabled = false }: { value: UiLanguage; onChange: (value: UiLanguage) => void; disabled?: boolean }) {
  const { t } = useI18n();
  return <label className="language-control" title={t("Gilt nur für deinen Benutzer und wird auf der Appliance gespeichert.")}>
    <span>{disabled ? t("Wird gespeichert…") : t("UI-Sprache")}</span>
    <select aria-label={t("UI-Sprache")} value={value} onChange={event => onChange(event.target.value as UiLanguage)} disabled={disabled}>
      <option value="de">Deutsch</option><option value="en">English</option>
    </select>
  </label>;
}

function UiPreferencesDialog({ language, saving, error, onChange, onClose, trigger }: {
  language: UiLanguage;
  saving: boolean;
  error: string | null;
  onChange: (language: UiLanguage) => void;
  onClose: () => void;
  trigger: React.RefObject<HTMLButtonElement | null>;
}) {
  const { t } = useI18n();
  const modal = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = modal.current;
    const previousOverflow = document.body.style.overflow;
    dialog?.showModal();
    document.body.style.overflow = "hidden";
    return () => {
      dialog?.close();
      document.body.style.overflow = previousOverflow;
      trigger.current?.focus();
    };
  }, [trigger]);
  return <dialog ref={modal} className="ui-preferences-dialog" aria-labelledby="ui-preferences-title"
    onCancel={event => { event.preventDefault(); onClose(); }}
    onClick={event => { if (event.target === event.currentTarget) onClose(); }}
    onKeyDown={event => {
      if (event.key !== "Tab") return;
      const controls = Array.from(event.currentTarget.querySelectorAll<HTMLElement>('button:not(:disabled), select:not(:disabled)'));
      const first = controls[0], last = controls.at(-1);
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    }}>
    <div className="ui-preferences-content">
      <header><span className="preferences-icon"><SlidersHorizontal size={22} /></span>
        <div><span className="section-kicker">{t("Persönliche Einstellungen")}</span><h2 id="ui-preferences-title">{t("UI-Einstellungen")}</h2></div>
        <button className="preferences-close" onClick={onClose} aria-label={t("Dialog schließen")}><X size={18} /></button>
      </header>
      <LanguageSelector value={language} onChange={onChange} disabled={saving} />
      <p>{t("Gilt nur für deinen Benutzer und wird auf der Appliance gespeichert.")}</p>
      {error && <p role="alert" className="form-error">{t("Sprache konnte nicht gespeichert werden")}: {error}</p>}
      <footer><Button onClick={onClose}>{t("Schließen")}</Button></footer>
    </div>
  </dialog>;
}

function NotificationCenter({
  notices,
  dismiss,
}: {
  notices: Notice[];
  dismiss: (id: string) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="notice-stack" aria-live="polite">
      {notices.map((notice) => (
        <section
          className={`notice notice-${notice.tone}`}
          role={notice.tone === "error" ? "alert" : "status"}
          key={notice.id}
        >
          {notice.tone === "success" ? (
            <CheckCircle2 />
          ) : notice.tone === "error" ? (
            <AlertTriangle />
          ) : (
            <LoaderCircle className="notice-spinner" />
          )}
          <span>
            <strong>{notice.title}</strong>
            <small>{notice.message}</small>
          </span>
          <button onClick={() => dismiss(notice.id)} aria-label={t("Meldung schließen")}>
            <X />
          </button>
        </section>
      ))}
    </div>
  );
}

function formatBytes(value: number, locale: string) {
  if (!Number.isFinite(value)) return "—";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let current = value;
  let unit = 0;
  while (current >= 1000 && unit < units.length - 1) {
    current /= 1000;
    unit += 1;
  }
  return `${new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(current)} ${units[unit]}`;
}

function repositoryDisks(snapshot: ApplianceSnapshot): DiskTelemetry[] {
  if (!snapshot.repository) return [];
  const roles = new Map<string, string>();
  for (const [targetId, role] of [
    [snapshot.repository.metadataTarget, "Metadata"],
    [snapshot.repository.dataTarget, "DATA"],
  ] as const) {
    const target = snapshot.targets.find((item) => item.stableId === targetId);
    if (!target) continue;
    const kernelNames = target.backingDisks.length
      ? target.backingDisks.map((disk) => disk.kernelName)
      : [target.kernelName];
    for (const kernelName of kernelNames) roles.set(kernelName, role);
  }
  return snapshot.telemetry.disks
    .filter((disk) => roles.has(disk.id))
    .map((disk) => ({ ...disk, role: roles.get(disk.id) ?? disk.role }));
}

function api<T>(
  path: string,
  init: RequestInit = {},
  csrf?: string,
): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body) headers.set("content-type", "application/json");
  if (csrf) headers.set("x-csrf-token", csrf);
  return fetch(path, { ...init, headers, credentials: "include" }).then(
    async (response) => {
      if (!response.ok) {
        const problem = await response
          .json()
          .catch(() => ({ message: response.statusText }));
        throw new Error(
          problem.message || "Die Appliance-Anfrage ist fehlgeschlagen",
        );
      }
      if (response.status === 204) return undefined as T;
      return response.json() as Promise<T>;
    },
  );
}

function MetricCard({
  icon: Icon,
  label,
  value,
  detail,
  tone = "cyan",
}: {
  icon: typeof Gauge;
  label: string;
  value: string;
  detail: string;
  tone?: "cyan" | "violet" | "green" | "amber";
}) {
  return (
    <Card className={`metric-card tone-${tone}`}>
      <CardHeader>
        <span>{label}</span>
        <Icon size={17} />
      </CardHeader>
      <CardContent>
        <strong>{value}</strong>
        <small>{detail}</small>
      </CardContent>
    </Card>
  );
}

function AppSidebar({
  active,
  onChange,
  alarms,
}: {
  active: string;
  onChange: (item: string) => void;
  alarms: number;
}) {
  const { t } = useI18n();
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">
          <img src={applianceMark} alt="" />
        </div>
        <div>
          <strong>FastDup</strong>
          <span>Control Plane</span>
        </div>
      </div>
      <div className="appliance-label">
        <span>Appliance</span>
        <strong>fd-appliance-01</strong>
      </div>
      <nav aria-label={t("Hauptnavigation")}>
        {navigation.map(([label, Icon]) => (
          <button
            key={label}
            className={active === label ? "active" : ""}
            aria-current={active === label ? "page" : undefined}
            onClick={() => onChange(label)}
          >
            <Icon size={18} />
            <span>{t(label)}</span>
            {label === "Ereignisse" && alarms > 0 && <i>{alarms}</i>}
          </button>
        ))}
      </nav>
      <div className="sidebar-health">
        <ShieldCheck size={18} />
        <div>
          <strong>{t("System geschützt")}</strong>
          <span>{t("Kein Appliance-Reboot erforderlich")}</span>
        </div>
      </div>
    </aside>
  );
}

function ConfirmDialog({
  title,
  children,
  danger,
  confirmLabel,
  onConfirm,
  onClose,
}: {
  title: string;
  children: React.ReactNode;
  danger?: boolean;
  confirmLabel: string;
  onConfirm: () => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  return (
    <div
      className="dialog-backdrop"
      role="presentation"
      onMouseDown={(event) => event.target === event.currentTarget && onClose()}
    >
      <section
        className={`dialog ${danger ? "danger-dialog" : ""}`}
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="dialog-title"
      >
        <button
          className="dialog-close"
          onClick={onClose}
          aria-label={t("Schließen")}
        >
          <X size={17} />
        </button>
        <span className="dialog-icon">
          {danger ? <AlertTriangle /> : <ServerCog />}
        </span>
        <h2 id="dialog-title">{title}</h2>
        <div className="dialog-copy">{children}</div>
        <footer>
          <Button variant="secondary" onClick={onClose}>{t("Abbrechen")}</Button>
          <Button variant={danger ? "danger" : "secondary"} onClick={onConfirm}>
            {confirmLabel}
          </Button>
        </footer>
      </section>
    </div>
  );
}

function RepositoryHero({
  telemetry,
  runCommand,
  busy,
}: {
  telemetry: TelemetrySnapshot;
  runCommand: (kind: "mount" | "unmount" | "offline_scrub") => void;
  busy: boolean;
}) {
  const { t, locale } = useI18n();
  const online = telemetry.repositoryState === "online";
  const mountable = ["unmounted", "error"].includes(
    telemetry.repositoryState,
  );
  const generation = telemetry.commitGeneration;
  const checkpointAge = telemetry.lastCheckpointSeconds;
  return (
    <section className="repo-hero">
      <div className="repo-identity">
        <span className="repo-icon">
          <Database size={25} />
        </span>
        <div>
          <div className="eyebrow">FastDup Repository</div>
          <h1>Production Repository</h1>
          <p>
            <Badge className={repositoryTone(telemetry.repositoryState)}>
              <span className="status-dot" />
              {t(stateLabels[telemetry.repositoryState])}
            </Badge>
            <span>
              Generation {generation?.toLocaleString(locale) ?? "—"}
            </span>
            <span>
              {checkpointAge === undefined
                ? "Checkpoint —"
                : t("Checkpoint vor {seconds} s", { seconds: checkpointAge })}
            </span>
          </p>
        </div>
      </div>
      <div className="hero-actions">
        <Button
          variant="secondary"
          onClick={() => runCommand("offline_scrub")}
          disabled={!online || busy}
        >
          <TerminalSquare size={16} /> Offline-Scrub
        </Button>
        {online ? (
          <Button
            variant="danger"
            onClick={() => runCommand("unmount")}
            disabled={busy}
          >
            <Unplug size={16} /> Unmount
          </Button>
        ) : (
          <Button
            variant="secondary"
            onClick={() => runCommand("mount")}
            disabled={!mountable || busy}
          >
            <Play size={16} /> Mount
          </Button>
        )}
      </div>
    </section>
  );
}

function throughputOption(snapshot: TelemetrySnapshot, extended = false, locale = "de-DE") {
  return {
    animation: false,
    textStyle: { fontFamily: 'Inter, "Segoe UI", sans-serif' },
    grid: {
      left: 18,
      right: 20,
      top: extended ? 38 : 24,
      bottom: 24,
      containLabel: true,
    },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#101c24",
      borderColor: "#263947",
      textStyle: { color: "#d9e6ec" },
    },
    legend: extended ? { top: 4, textStyle: { color: "#afbecb" } } : undefined,
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: snapshot.series.map((point) => point.time),
      axisLabel: {
        color: "#afbecb",
        formatter: (value: string) =>
          new Date(value).toLocaleTimeString(locale, {
            hour: "2-digit",
            minute: "2-digit",
          }),
      },
      axisLine: { lineStyle: { color: "#263842" } },
    },
    yAxis: {
      type: "value",
      name: "MB/s",
      nameTextStyle: { color: "#afbecb" },
      axisLabel: { color: "#afbecb" },
      splitLine: { lineStyle: { color: "rgba(68,94,108,.24)" } },
    },
    series: [
      {
        name: "POSIX Read",
        type: "line",
        smooth: 0.28,
        showSymbol: false,
        data: snapshot.series.map((point) => point.read),
        lineStyle: { color: "#22d3ee", width: 2 },
        itemStyle: { color: "#22d3ee" },
        areaStyle: { color: "rgba(34,211,238,.12)" },
      },
      {
        name: "POSIX Write",
        type: "line",
        smooth: 0.28,
        showSymbol: false,
        data: snapshot.series.map((point) => point.write),
        lineStyle: { color: "#b69aff", width: 2 },
        itemStyle: { color: "#b69aff" },
        areaStyle: { color: "rgba(139,92,246,.09)" },
      },
    ],
  };
}

function Overview({
  snapshot,
  disks,
  runCommand,
  busy,
}: {
  snapshot: TelemetrySnapshot;
  disks: DiskTelemetry[];
  runCommand: (kind: "mount" | "unmount" | "offline_scrub") => void;
  busy: boolean;
}) {
  const { t, locale } = useI18n();
  const chartOption = useMemo(() => throughputOption(snapshot, false, locale), [snapshot, locale]);
  const usedPercent = snapshot.dataCapacityBytes
    ? (snapshot.dataUsedBytes / snapshot.dataCapacityBytes) * 100
    : 0;
  return (
    <>
      <RepositoryHero telemetry={snapshot} runCommand={runCommand} busy={busy} />
      <section className="metric-grid">
        <MetricCard
          icon={ArrowDownToLine}
          label="POSIX Frontend Read"
          value={`${snapshot.frontendReadMbps.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} MB/s`}
          detail={t("Live · 1 Sekunde")}
        />
        <MetricCard
          icon={ArrowUpFromLine}
          label="POSIX Frontend Write"
          value={`${snapshot.frontendWriteMbps.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} MB/s`}
          detail={t("Live · 1 Sekunde")}
          tone="violet"
        />
        <MetricCard
          icon={Layers3}
          label="Exact Dedup Rate"
          value={`${snapshot.dedupRate.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} %`}
          detail={t("{ratio}× physische Gesamtreduktion", { ratio: snapshot.reductionRatio.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) })}
          tone="green"
        />
        <MetricCard
          icon={Cpu}
          label={t("Systemressourcen")}
          value={`${snapshot.cpuPercent.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })} % CPU`}
          detail={t("{percent} % RAM belegt", { percent: snapshot.ramPercent.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 }) })}
          tone="amber"
        />
      </section>
      <section className="dashboard-grid">
        <Card className="throughput-card">
          <CardHeader>
            <div>
              <span className="section-kicker">Live performance</span>
              <h2>POSIX Frontend Throughput</h2>
            </div>
            <div className="legend">
              <span className="read">Read</span>
              <span className="write">Write</span>
              <span className="range-label">{t("Letzte 15 min")}</span>
            </div>
          </CardHeader>
          <CardContent>
            <ReactECharts
              option={chartOption}
              style={{ height: 290 }}
              lazyUpdate
            />
          </CardContent>
        </Card>
        <Card className="capacity-card">
          <CardHeader>
            <div>
              <span className="section-kicker">Data tier</span>
              <h2>{t("Physische Kapazität")}</h2>
            </div>
            <Gauge size={19} />
          </CardHeader>
          <CardContent>
            <div
              className="capacity-ring"
              style={
                { "--used": `${usedPercent * 3.6}deg` } as React.CSSProperties
              }
            >
              <div>
                <strong>{usedPercent.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}%</strong>
                <span>{t("belegt")}</span>
              </div>
            </div>
            <div className="capacity-numbers">
              <p>
                <span>{t("Verwendet")}</span>
                <strong>{formatBytes(snapshot.dataUsedBytes, locale)}</strong>
              </p>
              <p>
                <span>{t("Verfügbar")}</span>
                <strong>
                  {formatBytes(
                    snapshot.dataCapacityBytes - snapshot.dataUsedBytes, locale
                  )}
                </strong>
              </p>
              <p>
                <span>{t("Gesamt")}</span>
                <strong>{formatBytes(snapshot.dataCapacityBytes, locale)}</strong>
              </p>
            </div>
            <div className="reserve-note">
              <ShieldCheck size={15} />{t("10 % Operating Reserve aktiv")}</div>
          </CardContent>
        </Card>
      </section>
      <DiskTelemetryTable disks={disks} />
    </>
  );
}

function DiskTelemetryTable({ disks }: { disks: DiskTelemetry[] }) {
  const { t, locale } = useI18n();
  return (
    <Card className="disk-card">
      <CardHeader>
        <div>
          <span className="section-kicker">Storage paths</span>
          <h2>{t("Outstanding I/O pro Target")}</h2>
        </div>
        <Badge className="live">
          <span className="pulse" />1 s Sampler
        </Badge>
      </CardHeader>
      <CardContent>
        <div className="disk-table header-row">
          <span>{t("Rolle & Gerät")}</span>
          <span>{t("Typ / Kapazität")}</span>
          <span>HBA Port</span>
          <span>Read / Write</span>
          <span>Outstanding I/O</span>
          <span>Status</span>
        </div>
        {disks.map((disk) => (
          <div className="disk-table" key={disk.id}>
            <span className="disk-name">
              <i>
                <HardDrive size={19} />
              </i>
              <span>
                <strong>{disk.role}</strong>
                <small>{disk.model}</small>
              </span>
            </span>
            <span>
              <strong>{disk.kind}</strong>
              <small>{formatBytes(disk.capacityBytes, locale)}</small>
            </span>
            <span>
              <strong>{disk.hbaPort || "nicht verfügbar"}</strong>
              <small>{t("Hardwarepfad")}</small>
            </span>
            <span>
              <strong>
                {disk.readMbps.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })} / {disk.writeMbps.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })} MB/s
              </strong>
              <small>{disk.utilization.toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })} % Utilization</small>
            </span>
            <span className="io-cell">
              <strong>{disk.outstandingIo}</strong>
              <i>
                <b
                  style={{ width: `${Math.min(100, disk.outstandingIo * 2)}%` }}
                />
              </i>
            </span>
            <span>
              <Badge className="healthy">
                <span className="status-dot" />{t("Gesund")}</Badge>
            </span>
          </div>
        ))}
        {disks.length === 0 && (
          <div className="disk-empty">{t("Kein Repository gebunden – keine relevanten Targets.")}</div>
        )}
      </CardContent>
    </Card>
  );
}

function RepositoryPage({
  snapshot,
  runCommand,
  busy,
}: {
  snapshot: ApplianceSnapshot;
  runCommand: (kind: "mount" | "unmount" | "offline_scrub") => void;
  busy: boolean;
}) {
  const { t, locale } = useI18n();
  const online = snapshot.telemetry.repositoryState === "online";
  return (
    <>
      <RepositoryHero
        telemetry={snapshot.telemetry}
        runCommand={runCommand}
        busy={busy}
      />
      <div className="two-column">
        <Card>
          <CardHeader>
            <div>
              <span className="section-kicker">Runtime</span>
              <h2>{t("Zustandsmaschine")}</h2>
            </div>
            <Badge className={repositoryTone(snapshot.telemetry.repositoryState)}>
              {t(stateLabels[snapshot.telemetry.repositoryState])}
            </Badge>
          </CardHeader>
          <CardContent className="state-flow">
            {["Unmounted", "Mounting", "Recovering", "Online"].map(
              (state, index) => (
                <div
                  className={
                    state.toLowerCase() === snapshot.telemetry.repositoryState
                      ? "current"
                      : ""
                  }
                  key={state}
                >
                  <span>{index + 1}</span>
                  <strong>{state}</strong>
                </div>
              ),
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div>
              <span className="section-kicker">Checkpoint</span>
              <h2>Durability Status</h2>
            </div>
            <ShieldCheck size={19} />
          </CardHeader>
          <CardContent className="detail-list">
            <p>
              <span>{t("Aktive Generation")}</span>
              <strong>
                {snapshot.telemetry.commitGeneration?.toLocaleString(locale) ??
                  "—"}
              </strong>
            </p>
            <p>
              <span>{t("Checkpoint-Alter")}</span>
              <strong>
                {snapshot.telemetry.lastCheckpointSeconds === undefined
                  ? "—"
                  : t("{seconds} Sekunden", { seconds: snapshot.telemetry.lastCheckpointSeconds ?? "—" })}
              </strong>
            </p>
            <p>
              <span>Recovery-Latch</span>
              <strong className="ok">{t("Bereit")}</strong>
            </p>
            <p>
              <span>{t("Swap-Grenze")}</span>
              <strong>0 B</strong>
            </p>
          </CardContent>
        </Card>
      </div>
      <Card className="action-card">
        <CardHeader>
          <div>
            <span className="section-kicker">{t("Wartung")}</span>
            <h2>{t("Repository-Aktionen")}</h2>
          </div>
        </CardHeader>
        <CardContent>
          <div>
            <TerminalSquare />
            <span>
              <strong>Offline-Scrub</strong>
              <small>{t("Prüft Pool-Identität, Metadaten und DATA-Container. SMB wird kontrolliert unterbrochen.")}</small>
            </span>
            <Button
              variant="secondary"
              onClick={() => runCommand("offline_scrub")}
              disabled={!online || busy}
            >{t("Starten")}</Button>
          </div>
          <div>
            <RefreshCcw />
            <span>
              <strong>{t("Sauberer Remount")}</strong>
              <small>{t("SIGINT, Checkpoint, FUSE-Unmount, Start und Health-Check ohne Appliance-Reboot.")}</small>
            </span>
            <Button
              variant="secondary"
              onClick={() => runCommand("unmount")}
              disabled={!online || busy}
            >
              Unmount
            </Button>
          </div>
        </CardContent>
      </Card>
    </>
  );
}

function TargetCard({
  target,
  selected,
  onSelect,
  role,
}: {
  target: BlockTarget;
  selected: boolean;
  onSelect: () => void;
  role: string;
}) {
  const { t, locale } = useI18n();
  return (
    <button
      className={`target-card ${selected ? "selected" : ""}`}
      disabled={!target.eligible}
      onClick={onSelect}
    >
      <span className="target-icon">
        <HardDrive size={22} />
      </span>
      <span className="target-copy">
        <em>{role}</em>
        <strong>{target.model || target.kernelName}</strong>
        <small>
          {target.targetType} · {formatBytes(target.capacityBytes, locale)}
        </small>
        <small>{target.hbaPort || "HBA-Port nicht verfügbar"}</small>
        <small>
          SN {target.serial || "nicht verfügbar"} ·{" "}
          {target.filesystem || "unformatiert"}
        </small>
        {target.backingDisks.length > 0 && (
          <small>{target.backingDisks.length}{t("physische Backing-Disks")}</small>
        )}
        {!target.eligible && <b>{target.eligibilityReason}</b>}
      </span>
      {selected && <Check size={18} />}
    </button>
  );
}

function DrivesPage({
  snapshot,
  submit,
  busy,
}: {
  snapshot: ApplianceSnapshot;
  submit: (body: unknown) => Promise<void>;
  busy: boolean;
}) {
  const { t, locale } = useI18n();
  const [metadata, setMetadata] = useState("");
  const [data, setData] = useState("");
  const [confirm, setConfirm] = useState(false);
  const sortedTargets = useMemo(
    () => [
      ...snapshot.targets.filter((target) => target.eligible),
      ...snapshot.targets.filter((target) => !target.eligible),
    ],
    [snapshot.targets],
  );
  const selectedMeta = snapshot.targets.find(
    (target) => target.stableId === metadata,
  );
  const selectedData = snapshot.targets.find(
    (target) => target.stableId === data,
  );
  const revision =
    selectedMeta?.inventoryRevision || selectedData?.inventoryRevision || "";
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">Block inventory</span>
          <h1>{t("Laufwerke & Provisionierung")}</h1>
          <p>{t("Nur erkannte, sichere Targets können ausgewählt werden. Gerätepfade sind niemals Freitext.")}</p>
        </div>
        <Badge>
          <RefreshCcw size={12} />{t("Inventar aktuell")}</Badge>
      </div>
      <div className="selection-columns">
        <Card>
          <CardHeader>
            <div>
              <span className="step-number">1</span>
              <h2>{t("Metadata-Target auswählen")}</h2>
            </div>
          </CardHeader>
          <CardContent className="target-list">
            {sortedTargets.map((target) => (
              <TargetCard
                key={`meta-${target.stableId}`}
                target={target}
                role="METADATA"
                selected={metadata === target.stableId}
                onSelect={() => setMetadata(target.stableId)}
              />
            ))}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div>
              <span className="step-number">2</span>
              <h2>{t("DATA-Target auswählen")}</h2>
            </div>
          </CardHeader>
          <CardContent className="target-list">
            {sortedTargets.map((target) => (
              <TargetCard
                key={`data-${target.stableId}`}
                target={target}
                role="DATA"
                selected={data === target.stableId}
                onSelect={() => setData(target.stableId)}
              />
            ))}
          </CardContent>
        </Card>
      </div>
      <div className="provision-bar">
        <span>
          <ShieldCheck size={18} />
          <span>
            <strong>{t("Schutzprüfung aktiv")}</strong>
            <small>{t("Root, Boot, Swap, Mounts, Holder und gemeinsame physische Abstammung werden ausgeschlossen.")}</small>
          </span>
        </span>
        <Button
          variant="danger"
          disabled={
            Boolean(snapshot.repository) ||
            busy ||
            !metadata ||
            !data ||
            metadata === data
          }
          onClick={() => setConfirm(true)}
        >
          <Trash2 size={15} />{t("Neues Repository initialisieren")}</Button>
      </div>
      {confirm && selectedMeta && selectedData && (
        <ConfirmDialog
          danger
          title={t("Targets vollständig löschen?")}
          confirmLabel={t("LÖSCHEN & INITIALISIEREN")}
          onClose={() => setConfirm(false)}
          onConfirm={() => {
            setConfirm(false);
            void submit({
              kind: "provision",
              metadata_target: metadata,
              data_target: data,
              inventory_revision: revision,
              confirmed: true,
            });
          }}
        >
          <p>{t("Diese Aktion entfernt unwiderruflich alle Daten auf beiden Targets.")}</p>
          <div className="wipe-summary">
            <strong>METADATA</strong>
            <span>
              {selectedMeta.model} · {formatBytes(selectedMeta.capacityBytes, locale)}
            </span>
            <strong>DATA</strong>
            <span>
              {selectedData.model} · {formatBytes(selectedData.capacityBytes, locale)}
            </span>
          </div>
          <p>{t("GPT und XFS werden neu angelegt; anschließend wird die Pool-Identität geschrieben und das Repository online gebracht.")}</p>
        </ConfirmDialog>
      )}
    </>
  );
}

const emptyShare: ShareSettings = {
  advancedReduction: "off",
  id: "",
  revision: 0,
  name: "",
  description: "",
  enabled: true,
  hidden: false,
  readOnly: false,
  guestAccess: false,
  encryption: "desired",
  accessBasedEnumeration: true,
  allowedUsers: [],
  allowedGroups: [],
  logicalQuota: undefined,
};

function SharesPage({
  repositoryError,
  repositoryReady,
  setupRepository,
  shares,
  save,
  remove,
  principals,
}: {
  repositoryError: boolean;
  repositoryReady: boolean;
  setupRepository: () => void;
  shares: ShareSettings[];
  save: (share: ShareSettings) => void;
  remove: (share: ShareSettings) => void;
  principals: { users: string[]; groups: string[] };
}) {
  const { t } = useI18n();
  const [editing, setEditing] = useState<ShareSettings | null>(null);
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">Samba 4.23.5</span>
          <h1>{t("SMB-Freigaben")}</h1>
          <p>{t("FastDup-optimierte Shares werden atomar validiert und ohne Reboot neu geladen.")}</p>
        </div>
        <Button
          variant="secondary"
          disabled={!repositoryReady}
          onClick={() => setEditing({ ...emptyShare, id: crypto.randomUUID() })}
        >
          <Plus size={15} />{t("Freigabe anlegen")}</Button>
      </div>
      {!repositoryReady && <div className="repository-required">
        <Database size={22} /><div><strong>{t(repositoryError ? "Repository prüfen" : "Zuerst ein Repository einrichten")}</strong>
        <p>{t(repositoryError ? "Das Repository meldet einen Fehler. Behebe diesen vor dem Anlegen neuer Shares." : "Shares benötigen ein eingerichtetes Repository als Speicherziel.")}</p></div>
        <Button variant="secondary" onClick={setupRepository}>{t(repositoryError ? "Repository öffnen" : "Repository einrichten")}</Button>
      </div>}
      <div className="share-grid">
        {shares.map((share) => (
          <Card className="share-card" key={share.id}>
            <CardHeader>
              <div>
                <span className={`share-status ${share.enabled ? "on" : ""}`} />
                <div>
                  <h2>\\fd-appliance-01\{share.name}</h2>
                  <small>{share.description}</small>
                </div>
              </div>
              <Badge>{share.hidden ? t("Versteckt") : t("Sichtbar")}</Badge>
            </CardHeader>
            <CardContent>
              <div className="share-flags">
                <span>{share.readOnly ? "Read only" : "Read / Write"}</span>
                <span>
                  {share.encryption === "required"
                    ? "Encryption required"
                    : "Encryption desired"}
                </span>
                <span>{share.accessBasedEnumeration ? "ABE" : t("Kein ABE")}</span>
                <span className={share.guestAccess ? "danger-text" : ""}>
                  {share.guestAccess ? t("Gastzugriff") : t("Authentifiziert")}
                </span>
                <span className={share.logicalQuota ? "quota-flag" : ""}>
                  {share.logicalQuota
                    ? `Quota ${share.logicalQuota.value} ${share.logicalQuota.unit.toUpperCase()}`
                    : t("Repository-Kapazität")}
                </span>
              </div>
              <div className="fixed-profile">
                <LockKeyhole size={15} />
                <span>
                  <strong>FastDup Optimized VFS</strong>
                  <small>64 KiB Clone Alignment · 1 GiB Maximum</small>
                </span>
                <Badge className="warning">{t("EXPERIMENTELL")}</Badge>
              </div>
              <footer>
                <Button
                  variant="ghost"
                  onClick={() => setEditing({ ...share })}
                >
                  <Pencil size={14} />{t("Bearbeiten")}</Button>
                <Button variant="ghost" onClick={() => remove(share)}>
                  <Trash2 size={14} />{t("Löschen")}</Button>
              </footer>
            </CardContent>
          </Card>
        ))}
      </div>
      {editing && (
        <ShareEditor
          share={editing}
          principals={principals}
          onClose={() => setEditing(null)}
          onSave={(share) => {
            setEditing(null);
            save(share);
          }}
        />
      )}
    </>
  );
}

function Toggle({
  checked,
  onChange,
  label,
  detail,
  dangerous,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  detail?: string;
  dangerous?: boolean;
}) {
  return (
    <label className={`toggle-row ${dangerous ? "danger-toggle" : ""}`}>
      <span>
        <strong>{label}</strong>
        {detail && <small>{detail}</small>}
      </span>
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      <i />
    </label>
  );
}

function ShareEditor({
  share,
  principals,
  onSave,
  onClose,
}: {
  share: ShareSettings;
  principals: { users: string[]; groups: string[] };
  onSave: (share: ShareSettings) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const modal = useRef<HTMLDialogElement>(null);
  const [returnFocus] = useState(() => document.activeElement);
  useEffect(() => {
    const dialog = modal.current;
    const previousOverflow = document.body.style.overflow;
    dialog?.showModal();
    document.body.style.overflow = "hidden";
    return () => {
      dialog?.close();
      document.body.style.overflow = previousOverflow;
      if (returnFocus instanceof HTMLElement) returnFocus.focus();
    };
  }, [returnFocus]);
  const [draft, setDraft] = useState(share);
  const [guestWarning, setGuestWarning] = useState(false);
  const set = <K extends keyof ShareSettings>(
    key: K,
    value: ShareSettings[K],
  ) => setDraft((current) => ({ ...current, [key]: value }));
  const toggleItem = (key: "allowedUsers" | "allowedGroups", item: string) =>
    set(
      key,
      draft[key].includes(item)
        ? draft[key].filter((value) => value !== item)
        : [...draft[key], item],
    );
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (draft.guestAccess && !share.guestAccess) {
      setGuestWarning(true);
      return;
    }
    onSave(draft);
  };
  return (
    <dialog
      ref={modal}
      className="share-modal"
      aria-labelledby="share-editor-title"
      onKeyDown={(event) => {
        if (event.key !== "Tab") return;
        const scope = guestWarning
          ? event.currentTarget.querySelector(".dialog")
          : event.currentTarget;
        const controls = Array.from(scope?.querySelectorAll<HTMLElement>(
          'button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex="0"]',
        ) ?? []).filter((element) => element.getClientRects().length > 0);
        const first = controls[0];
        const last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }}
      onCancel={(event) => {
        event.preventDefault();
        if (guestWarning) setGuestWarning(false);
        else onClose();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget && !guestWarning) onClose();
      }}
    >
      <form className="share-editor" onSubmit={submit}>
        <header>
          <div>
            <span className="section-kicker">SMB Share</span>
            <h2 id="share-editor-title">{share.revision ? t("Freigabe bearbeiten") : t("Neue Freigabe")}</h2>
          </div>
          <button type="button" onClick={onClose} aria-label={t("Dialog schließen")}>
            <X />
          </button>
        </header>
        <div className="share-editor-body">
          <label className="field">
            <span>{t("Freigabename")}</span>
            <input
              autoFocus
              required
              pattern="[A-Za-z0-9][A-Za-z0-9._-]{0,63}"
              value={draft.name}
              onChange={(event) => set("name", event.target.value)}
              placeholder={t("z. B. production")}
            />
            <small>{t("Nur Buchstaben, Ziffern, Punkt, Unterstrich und Bindestrich.")}</small>
          </label>
          <label className="field">
            <span>{t("Beschreibung")}</span>
            <input
              maxLength={120}
              value={draft.description}
              onChange={(event) => set("description", event.target.value)}
            />
          </label>
          <label className="field">
            <span>{t("Logisches Schreiblimit")}</span>
            <div className="capacity-input">
              <input
                aria-label={t("Kapazitätswert")}
                type="number"
                inputMode="numeric"
                min={1}
                max={999}
                step={1}
                disabled={!draft.logicalQuota}
                required={Boolean(draft.logicalQuota)}
                value={draft.logicalQuota?.value ?? ""}
                placeholder="25"
                onChange={(event) => {
                  if (!draft.logicalQuota) return;
                  set("logicalQuota", {
                    ...draft.logicalQuota,
                    value: Number(event.target.value),
                  });
                }}
              />
              <select
                aria-label={t("Kapazitätseinheit")}
                value={draft.logicalQuota?.unit ?? "none"}
                onChange={(event) => {
                  const unit = event.target.value;
                  set(
                    "logicalQuota",
                    unit === "none"
                      ? undefined
                      : {
                          value: draft.logicalQuota?.value || 1,
                          unit: unit as "gb" | "tb" | "pb",
                        },
                  );
                }}
              >
                <option value="none">{t("Keine")}</option>
                <option value="gb">GB</option>
                <option value="tb">TB</option>
                <option value="pb">PB</option>
              </select>
            </div>
            <small>{t("1–999 · Wird am Share-Root angezeigt und als harte logische Quota erzwungen. Dedup- und Clone-Daten zählen vollständig; Sparse-Holes nicht.")}</small>
          </label>
          <label className="field">
            <span>{t("Advanced Reduction für diese Freigabe")}</span>
            <select value={draft.advancedReduction ?? "inherit"}
              onChange={(event) => set("advancedReduction", event.target.value === "inherit" ? undefined : event.target.value as ShareSettings["advancedReduction"])}>
              <option value="off">{t("Aus")}</option>
              <option value="dependent_v1">{t("Similarity aktiv")}</option>
              <option value="inherit">{t("Repository-Standard")}</option>
            </select>
            <small>{t("Wirkt online auf neue Schreibvorgänge. Exact Dedup und normale Kompression bleiben aktiv. Aktivierte Freigaben verwenden einen gemeinsamen Kandidatenindex.")}</small>
          </label>
          <div className="toggle-stack">
            <Toggle
              checked={draft.enabled}
              onChange={(value) => set("enabled", value)}
              label={t("Freigabe aktiv")}
            />
            <Toggle
              checked={draft.hidden}
              onChange={(value) => set("hidden", value)}
              label={t("Versteckt")}
              detail={t("Setzt browseable = no; der Name bleibt unverändert.")}
            />
            <Toggle
              checked={draft.readOnly}
              onChange={(value) => set("readOnly", value)}
              label={t("Schreibgeschützt")}
            />
            <Toggle
              dangerous
              checked={draft.guestAccess}
              onChange={(value) => set("guestAccess", value)}
              label={t("Gastzugriff erlauben")}
              detail={t("Erlaubt Zugriff ohne authentifizierten Benutzer.")}
            />
            <Toggle
              checked={draft.accessBasedEnumeration}
              onChange={(value) => set("accessBasedEnumeration", value)}
              label="Access-Based Enumeration"
            />
          </div>
          <label className="field">
            <span>{t("SMB-Verschlüsselung")}</span>
            <select
              value={draft.encryption}
              onChange={(event) =>
                set(
                  "encryption",
                  event.target.value as ShareSettings["encryption"],
                )
              }
            >
              <option value="desired">{t("Gewünscht")}</option>
              <option value="required">{t("Erforderlich")}</option>
            </select>
          </label>
      <PrincipalPicker
        title={t("Erlaubte Samba-Benutzer")}
        items={[...new Set([...principals.users, ...draft.allowedUsers])]}
            selected={draft.allowedUsers}
            onToggle={(item) => toggleItem("allowedUsers", item)}
          />
      <PrincipalPicker
        title={t("Erlaubte lokale Gruppen")}
        items={[...new Set([...principals.groups, ...draft.allowedGroups])]}
            selected={draft.allowedGroups}
            onToggle={(item) => toggleItem("allowedGroups", item)}
          />
          <div className="profile-lock">
            <LockKeyhole />
            <span>
              <strong>{t("Optimiertes Profil fest vorgegeben")}</strong>
              <small>{t("vfs objects = fastdup · FastDup aktiv · 64 KiB Alignment · 1 GiB Maximum")}</small>
            </span>
          </div>
        </div>
        <footer>
          <Button type="button" variant="secondary" onClick={onClose}>{t("Abbrechen")}</Button>
          <Button type="submit" variant="secondary">
            <Save size={15} />{t("Aktivieren")}</Button>
        </footer>
      </form>
      {guestWarning && (
        <ConfirmDialog
          danger
          title={t("Gastzugriff aktivieren?")}
          confirmLabel={t("Gastzugriff aktivieren")}
          onClose={() => setGuestWarning(false)}
          onConfirm={() => {
            setGuestWarning(false);
            onSave(draft);
          }}
        >
          <p>{t("Nicht authentifizierte Clients erhalten entsprechend der Freigaberechte Zugriff. Diese Änderung wird sofort per Samba-Reload aktiviert.")}</p>
        </ConfirmDialog>
      )}
    </dialog>
  );
}

function PrincipalPicker({
  title,
  items,
  selected,
  onToggle,
}: {
  title: string;
  items: string[];
  selected: string[];
  onToggle: (item: string) => void;
}) {
  return (
    <fieldset className="principal-picker">
      <legend>{title}</legend>
      {items.map((item) => (
        <label key={item}>
          <input
            type="checkbox"
            checked={selected.includes(item)}
            onChange={() => onToggle(item)}
          />
          <span>{item}</span>
        </label>
      ))}
    </fieldset>
  );
}

function TelemetryPage({
  snapshot,
  disks,
  loadHistory,
  liveResources,
}: {
  snapshot: TelemetrySnapshot;
  disks: DiskTelemetry[];
  loadHistory: (seconds: number) => Promise<TelemetrySnapshot[]>;
  liveResources: ResourceSample[];
}) {
  const { t, locale } = useI18n();
  const [range, setRange] = useState("Live");
  const [history, setHistory] = useState<TelemetrySnapshot[] | null>(null);
  const [loading, setLoading] = useState(false);
  const ranges = [
    "Live",
    "15 min",
    "1 h",
    "6 h",
    "24 h",
    "7 d",
    "30 d",
    "90 d",
  ];
  const rangeSeconds: Record<string, number> = {
    "15 min": 15 * 60,
    "1 h": 60 * 60,
    "6 h": 6 * 60 * 60,
    "24 h": 24 * 60 * 60,
    "7 d": 7 * 24 * 60 * 60,
    "30 d": 30 * 24 * 60 * 60,
    "90 d": 90 * 24 * 60 * 60,
  };
  const displayedSnapshot = useMemo(
    () =>
      history
        ? {
            ...snapshot,
            series: history.map((sample) => ({
              time: sample.observedAt,
              read: sample.frontendReadMbps,
              write: sample.frontendWriteMbps,
            })),
          }
        : snapshot,
    [history, snapshot],
  );
  const resourceSamples = history ?? liveResources;
  const historyRequest = useRef(0);
  const selectRange = (item: string) => {
    const request = ++historyRequest.current;
    setRange(item);
    if (item === "Live") {
      setHistory(null);
      setLoading(false);
      return;
    }
    setLoading(true);
    void loadHistory(rangeSeconds[item] ?? 900)
      .then(samples => { if (request === historyRequest.current) setHistory(samples); })
      .finally(() => { if (request === historyRequest.current) setLoading(false); });
  };
  const resourceOption = useMemo(() => resourceChartOption(resourceSamples, locale), [resourceSamples, locale]);
  return (
    <>
      <div className="page-title telemetry-title">
        <div>
          <span className="section-kicker">Observability</span>
          <h1>{t("Tiefentelemetrie")}</h1>
          <p>{t("Synchronisierte Live-Daten aus POSIX-Rand, Host, Prozess und physischem Block-Layer.")}</p>
        </div>
        <div className="range-picker">
          {ranges.map((item) => (
            <button
              className={range === item ? "active" : ""}
              onClick={() => selectRange(item)}
              key={item}
            >
              {item}
            </button>
          ))}
        </div>
      </div>
      <section className="metric-grid telemetry-metrics">
        <MetricCard
          icon={Activity}
          label="Frontend Read"
          value={`${snapshot.frontendReadMbps.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} MB/s`}
          detail={t("POSIX erfolgreich")}
        />
        <MetricCard
          icon={Activity}
          label="Frontend Write"
          value={`${snapshot.frontendWriteMbps.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} MB/s`}
          detail={t("POSIX erfolgreich")}
          tone="violet"
        />
        <MetricCard
          icon={CircleGauge}
          label="Dedup Rate"
          value={`${snapshot.dedupRate.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} %`}
          detail="exact / (exact + new)"
          tone="green"
        />
        <MetricCard
          icon={Clock3}
          label="Checkpoint"
          value={
            snapshot.lastCheckpointSeconds === undefined
              ? "—"
              : `${snapshot.lastCheckpointSeconds} s`
          }
          detail={t("Alter der aktiven Generation")}
          tone="amber"
        />
      </section>
      <Card className="telemetry-chart">
        <CardHeader>
          <div>
            <span className="section-kicker">
              {t("Zeitraum")} · {range}
            </span>
            <h2>{t("POSIX Throughput & gemeinsamer Zeitcursor")}</h2>
          </div>
          <Badge className={loading ? "warning" : "live"}>
            <span className="pulse" />
            {loading ? t("Lädt") : range === "Live" ? "Live" : range}
          </Badge>
        </CardHeader>
        <CardContent>
          <ReactECharts
            option={throughputOption(displayedSnapshot, true, locale)}
            style={{ height: 330 }}
          />
        </CardContent>
      </Card>
      <div className="two-column telemetry-row">
        <Card className="telemetry-chart">
          <CardHeader>
            <div>
              <span className="section-kicker">Host & process</span>
              <h2>{t("CPU und RAM")}</h2>
            </div>
          </CardHeader>
          <CardContent>
            {history === null && resourceSamples.length < 2 && <p className="chart-note">{t("Live-Messwerte werden gesammelt…")}</p>}
            {history?.length === 0 && <p className="chart-note">{t("Keine Messwerte im gewählten Zeitraum.")}</p>}
            <ReactECharts option={resourceOption} style={{ height: 260 }} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div>
              <span className="section-kicker">Reduction</span>
              <h2>{t("Dedup & physische Reduktion")}</h2>
            </div>
          </CardHeader>
          <CardContent className="reduction-panel">
            <div>
              <span
                style={
                  {
                    "--percent": `${snapshot.dedupRate * 3.6}deg`,
                  } as React.CSSProperties
                }
              >
                <strong>{snapshot.dedupRate.toLocaleString(locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%</strong>
              </span>
              <small>Exact Dedup</small>
            </div>
            <div>
              <span
                style={
                  {
                    "--percent": `${Math.min(100, snapshot.reductionRatio * 20) * 3.6}deg`,
                  } as React.CSSProperties
                }
              >
                <strong>{snapshot.reductionRatio.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}×</strong>
              </span>
              <small>{t("Gesamtreduktion")}</small>
            </div>
            <p>{t("FILL und Recipe-Reuse sind bewusst nicht in der Exact-Dedup-Rate enthalten.")}</p>
          </CardContent>
        </Card>
      </div>
      <DiskTelemetryTable disks={disks} />
      <DetailTelemetryPanel sample={history === null ? snapshot : history.at(-1)} historical={history !== null} loading={loading} />
    </>
  );
}

function EventsPage({
  jobs,
  alerts,
  exportAudit,
}: {
  jobs: JobStatus[];
  alerts: string[];
  exportAudit: () => void;
}) {
  const { t, locale } = useI18n();
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">Audit & jobs</span>
          <h1>{t("Ereignisse")}</h1>
          <p>{t("Nachvollziehbare Managementaktionen, Jobs, Warnungen und Alarme.")}</p>
        </div>
        <Button variant="secondary" onClick={exportAudit}>
          <FileStack size={15} />{t("Audit exportieren")}</Button>
      </div>
      {alerts.map((alert, index) => (
        <div className="alarm-banner" key={`${alert}-${index}`}>
          <AlertTriangle />
          <span>
            <strong>{t("Kritischer Alarm")}</strong>
            <small>{alert}</small>
          </span>
        </div>
      ))}
      <Card className="events-table">
        <CardHeader>
          <div>
            <span className="section-kicker">{t("Letzte Vorgänge")}</span>
            <h2>{t("Job-Verlauf")}</h2>
          </div>
        </CardHeader>
        <CardContent>
          <div className="event-row event-head">
            <span>{t("Zeit")}</span>
            <span>{t("Aktion")}</span>
            <span>Status</span>
            <span>{t("Fortschritt")}</span>
            <span>{t("Ergebnis")}</span>
          </div>
          {jobs.map((job) => (
            <div className="event-row" key={job.id}>
              <span>
                {new Date(job.updatedAt * 1000).toLocaleString(locale)}
              </span>
              <strong>{job.kind.replaceAll("_", " ")}</strong>
              <Badge
                className={
                  job.state === "failed"
                    ? "error"
                    : job.state === "succeeded"
                      ? "healthy"
                      : "warning"
                }
              >
                {job.state}
              </Badge>
              <span className="job-progress">
                <i>
                  <b style={{ width: `${job.progressBasisPoints / 100}%` }} />
                </i>
                {(job.progressBasisPoints / 100).toLocaleString(locale, { minimumFractionDigits: 0, maximumFractionDigits: 0 })} %
              </span>
              <span>{job.message}</span>
            </div>
          ))}
          {jobs.length === 0 && (
            <div className="event-empty">{t("Noch keine Managementvorgänge.")}</div>
          )}
        </CardContent>
      </Card>
    </>
  );
}

function SettingsPage({ settings, fingerprint, save, password, regenerateTls, csrfToken, username, onCertificateChanged }: {
  settings: RepositorySettings; fingerprint: string; save: (value: RepositorySettings) => void;
  password: () => void; regenerateTls: () => void; csrfToken: string; username: string; onCertificateChanged: (fingerprint: string) => void;
}) {
  const { t } = useI18n();
  const [section, setSection] = useState("Repository");
  const [draft, setDraft] = useState(settings);
  const [extensionDraft, setExtensionDraft] = useState(settings.smallFileExtensions.join("\n"));
  const request = useCallback(<T,>(path: string, init?: RequestInit) => api<T>(path, init, csrfToken), [csrfToken]);
  useEffect(() => { setDraft(settings); setExtensionDraft(settings.smallFileExtensions.join("\n")); }, [settings]);
  const set = <K extends keyof RepositorySettings>(key: K, value: RepositorySettings[K]) => setDraft(current => ({...current, [key]: value}));
  const validPressure = draft.pressureLowBasisPoints >= 5000 && draft.pressureHighBasisPoints <= 9950 && draft.pressureLowBasisPoints < draft.pressureHighBasisPoints;
  return <>
    <div className="page-title"><div><h1>{t("Einstellungen")}</h1><p>{t("Repository-Betrieb, Web-Benutzer und HTTPS-Zertifikate verwalten.")}</p></div>
      {section === "Repository" && <Button variant="secondary" disabled={!validPressure} onClick={() => save(draft)}><Save size={15}/>{t("Übernehmen")}</Button>}
    </div>
    <nav className="settings-nav" aria-label={t("Einstellungsbereiche")}>
      {["Repository", "Web-Benutzer", "Zertifikate"].map(name => <button key={name} aria-pressed={section === name} onClick={() => setSection(name)}>{t(name)}</button>)}
    </nav>
    {section === "Repository" && <div className="settings-cards">
      <Card><CardHeader><div><h2>{t("Start & Datenablage")}</h2><p>{t("Startverhalten, Datenreduktion und Dateien auf dem schnellen Metadata-Tier.")}</p></div></CardHeader><CardContent className="settings-form">
            <Toggle
              checked={draft.autoMount}
              onChange={(value) => set("autoMount", value)}
              label="Auto-Mount"
              detail={t("Repository nach Dienststart automatisch online bringen.")}
            />

            <label className="field">
              <span>{t("Advanced Reduction: Repository-Standard")}</span>
              <select
                value={draft.advancedReduction}
                onChange={(event) =>
                  set(
                    "advancedReduction",
                    event.target
                      .value as RepositorySettings["advancedReduction"],
                  )
                }
              >
                <option value="off">{t("Aus")}</option>
                <option value="dependent_v1">{t("Similarity aktiv")}</option>
              </select>
              <small>{t("Wird online aktiv. Pro Freigabe überschreibbar; neue Basen werden inkrementell aufgenommen.")}</small>
            </label>

            <label className="field">
              <span>{t("Small-File-Tier · Dateiendungen")}</span>
              <textarea
                rows={5}
                value={extensionDraft}
                onChange={(event) => {
                  setExtensionDraft(event.target.value);
                  set(
                    "smallFileExtensions",
                    event.target.value
                      .split(/[\s,;]+/)
                      .map((extension) => extension.trim())
                      .filter(Boolean),
                  );
                }}
                placeholder={".json\n.xml\n.vmdk"}
                spellCheck={false}
              />
              <small>{t("Eine Endung pro Zeile, inklusive Punkt. Groß-/Kleinschreibung wird ignoriert; maximal 64 Endungen. Die Änderung wird ohne Remount aktiv.")}</small>
            </label>

      </CardContent></Card>
      <Card><CardHeader><div><h2>{t("Speicherbereinigung (Online-GC)")}</h2><p>{t("Der GC gibt Speicher nicht mehr benötigter Daten frei. Die Schwellen beziehen sich auf die Belegung des Data-Tiers, nicht auf RAM oder CPU.")}</p></div></CardHeader><CardContent className="settings-form">
            <Toggle
              checked={draft.onlineGcEnabled}
              onChange={(value) => set("onlineGcEnabled", value)}
              label="Online-GC"
              detail={t("Nicht mehr benötigte Daten werden im laufenden Betrieb bereinigt.")}
            />

        <div className="thresholds">
          <label className="field"><span>{t("Druckmodus beenden bei (%)")}</span><input type="number" min="50" max="99" step="0.1" value={draft.pressureLowBasisPoints / 100} onChange={event => set("pressureLowBasisPoints", Math.round(Number(event.target.value) * 100))}/><small>{t("Pressure Low: Fällt die Belegung auf diesen Wert, endet der GC-Druckmodus.")}</small></label>
          <label className="field"><span>{t("Druckmodus starten ab (%)")}</span><input type="number" min="50" max="99.5" step="0.1" value={draft.pressureHighBasisPoints / 100} onChange={event => set("pressureHighBasisPoints", Math.round(Number(event.target.value) * 100))}/><small>{t("Pressure High: Ab dieser Belegung wird die Bereinigung priorisiert.")}</small></label>
        </div>
        {!validPressure && <p className="form-error" role="alert">{t("Die untere Schwelle muss kleiner als die obere sein (50–99,5 %).")}</p>}
        <p className="settings-explanation">{t("Beispiel: Ab 90 % startet der Druckmodus und bleibt aktiv, bis höchstens 85 % belegt sind. Das verhindert ständiges Ein- und Ausschalten. Regulärer GC kann auch unterhalb dieser Schwellen laufen.")}</p>
            <label className="field">
              <span>{t("Wartungsfenster (UTC)")}</span>
              <input
                value={draft.maintenanceWindowUtc || ""}
                onChange={(event) =>
                  set("maintenanceWindowUtc", event.target.value || undefined)
                }
                placeholder="02:00-05:00"
              />
              <small>{t("Täglich in UTC, Format HH:MM-HH:MM. Leer lassen, um kein festes Zeitfenster vorzugeben.")}</small>
            </label>

      </CardContent></Card>
    </div>}
    {section === "Web-Benutzer" && <WebUsersSettings request={request} username={username} changePassword={password}/>}
    {section === "Zertifikate" && <CertificateSettings request={request} fingerprint={fingerprint} regenerate={regenerateTls} onImported={onCertificateChanged}/>}
  </>;
}

function PasswordPage({
  session,
  onChanged,
  onLogout,
}: {
  session: SessionInfo;
  onChanged: (session: SessionInfo) => void;
  onLogout: () => void;
}) {
  const { t } = useI18n();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [repeat, setRepeat] = useState("");
  const [error, setError] = useState("");
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (next.length < 12 || next !== repeat) {
      setError(
        t("Das neue Passwort muss mindestens 12 Zeichen lang sein und übereinstimmen."),
      );
      return;
    }
    api<SessionInfo>(
      "/api/v1/session/password",
      {
        method: "PUT",
        body: JSON.stringify({ currentPassword: current, newPassword: next }),
      },
      session.csrfToken,
    )
      .then(onChanged)
      .catch((reason: Error) => setError(reason.message));
  };
  return (
    <div className="password-gate">
      <Card>
        <CardHeader>
          <div>
            <span className="section-kicker">{t("Erstzugang")}</span>
            <h2>{t("Initialpasswort ändern")}</h2>
          </div>
          <KeyRound />
        </CardHeader>
        <CardContent>
          <div className="gate-note">
            <AlertTriangle />
            <span>{t("Bis zum Passwortwechsel sind alle Managementaktionen gesperrt.")}</span>
          </div>
          <form onSubmit={submit}>
            <label className="field">
              <span>{t("Aktuelles Passwort")}</span>
              <input
                type="password"
                autoComplete="current-password"
                value={current}
                onChange={(event) => setCurrent(event.target.value)}
              />
            </label>
            <label className="field">
              <span>{t("Neues Passwort")}</span>
              <input
                type="password"
                minLength={12}
                autoComplete="new-password"
                value={next}
                onChange={(event) => setNext(event.target.value)}
              />
            </label>
            <label className="field">
              <span>{t("Neues Passwort wiederholen")}</span>
              <input
                type="password"
                minLength={12}
                autoComplete="new-password"
                value={repeat}
                onChange={(event) => setRepeat(event.target.value)}
              />
            </label>
            {error && <p className="form-error">{error}</p>}
            <Button type="submit" variant="secondary">{t("Passwort setzen & aktivieren")}</Button>
          </form>
          <button className="text-button" onClick={onLogout}>{t("Abmelden")}</button>
        </CardContent>
      </Card>
    </div>
  );
}

function Login({ onLogin }: { onLogin: (session: SessionInfo) => void }) {
  const { t, language, setLanguage } = useI18n();
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    api<SessionInfo>("/api/v1/session/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    })
      .then(onLogin)
      .catch((reason: Error) => setError(reason.message))
      .finally(() => setBusy(false));
  };
  return (
    <main className="login-page">
      <div className="login-language"><LanguageSelector value={language} onChange={setLanguage} /></div>
      <section className="login-brand">
        <div className="brand-mark">
          <img src={applianceMark} alt="" />
        </div>
        <div>
          <strong>FastDup</strong>
          <span>Appliance Control Plane</span>
        </div>
      </section>
      <Card className="login-card">
        <CardHeader>
          <div>
            <span className="section-kicker">{t("Sichere Verbindung")}</span>
            <h2>{t("Administrator-Anmeldung")}</h2>
          </div>
          <ShieldCheck size={20} />
        </CardHeader>
        <CardContent>
          <p>{t("Lokale Verwaltung der FastDup Storage Appliance.")}</p>
          <form onSubmit={submit}>
            <label className="field">
              <span>{t("Benutzername")}</span>
              <input
                autoFocus
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
              />
            </label>
            <label className="field">
              <span>{t("Passwort")}</span>
              <input
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
            </label>
            {error && <p className="form-error">{error}</p>}
            <Button type="submit" variant="secondary" disabled={busy}>
              {busy ? t("Anmeldung…") : t("Anmelden")}
            </Button>
          </form>
          <div className="local-note">
            <LockKeyhole size={15} />{t("Appliance-lokal · HttpOnly Session · Argon2id")}</div>
        </CardContent>
      </Card>
    </main>
  );
}

function Application() {
  const { t, language, setLanguage } = useI18n();
  const [active, setActive] = useState("Übersicht");
  const workspaceScroll = useRef<HTMLElement>(null);
  useEffect(() => { if (workspaceScroll.current) workspaceScroll.current.scrollTop = 0; }, [active]);
  const [snapshot, setSnapshot] = useState<ApplianceSnapshot>(() =>
    emptyApplianceSnapshot(),
  );
  const [session, setSession] = useState<SessionInfo | null | undefined>(
    undefined,
  );
  const [savingLanguage, setSavingLanguage] = useState(false);
  const [languageError, setLanguageError] = useState<string | null>(null);
  const [uiPreferencesOpen, setUiPreferencesOpen] = useState(false);
  const accountTrigger = useRef<HTMLButtonElement>(null);
  const accountMenu = useRef<HTMLDivElement>(null);
  const [liveResources, setLiveResources] = useState<ResourceSample[]>([]);
  useEffect(() => {
    if (session) setLanguage(session.uiLanguage === "en" ? "en" : "de");
  }, [session?.username, session?.uiLanguage, setLanguage]);
  const changeLanguage = async (next: UiLanguage) => {
    if (!session || savingLanguage) return;
    setSavingLanguage(true);
    setLanguageError(null);
    const username = session.username;
    try {
      const result = await api<{ uiLanguage: UiLanguage }>(
        "/api/v1/session/language", { method: "PUT", body: JSON.stringify({ language: next }) }, session.csrfToken,
      );
      setSession(current => current?.username === username ? { ...current, uiLanguage: result.uiLanguage } : current);
    } catch (error) {
      setLanguageError(error instanceof Error ? error.message : t("Unbekannter Fehler"));
      notify({ id: "ui-language", tone: "error", title: t("Sprache konnte nicht gespeichert werden"), message: error instanceof Error ? error.message : t("Unbekannter Fehler") });
    } finally { setSavingLanguage(false); }
  };
  const [fresh, setFresh] = useState(false);
  const [accountOpen, setAccountOpen] = useState(false);
  useEffect(() => {
    if (!accountOpen) return;
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Node && !accountMenu.current?.contains(event.target)) setAccountOpen(false);
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setAccountOpen(false); accountTrigger.current?.focus(); }
    };
    document.addEventListener("pointerdown", outside);
    document.addEventListener("keydown", escape);
    return () => { document.removeEventListener("pointerdown", outside); document.removeEventListener("keydown", escape); };
  }, [accountOpen]);
  const [confirmAction, setConfirmAction] = useState<
    "mount" | "unmount" | "offline_scrub" | null
  >(null);
  const [alerts, setAlerts] = useState<string[]>([]);
  const [notices, setNotices] = useState<Notice[]>([]);
  const [principals, setPrincipals] = useState({
    users: [] as string[],
    groups: [] as string[],
  });
  const lastSample = useRef(0);
  const notify = useCallback((notice: Notice) => {
    setNotices((current) => [
      notice,
      ...current.filter((item) => item.id !== notice.id),
    ].slice(0, 5));
  }, []);

  const refresh = useCallback(
    () =>
      api<ApplianceSnapshot>("/api/v1/snapshot")
        .then((value) => {
          setSnapshot(value);
          setLiveResources(current => appendResourceSample(current, value.telemetry));
          setFresh(true);
        })
        .catch(() => setFresh(false)),
    [],
  );
  useEffect(() => {
    api<SessionInfo>("/api/v1/session")
      .then((value) => {
        setSession(value);
        if (!value.mustChangePassword) void refresh();
        if (!value.mustChangePassword)
          void api<{ users: string[]; groups: string[] }>(
            "/api/v1/samba/principals",
          )
            .then(setPrincipals)
            .catch(() => setPrincipals({ users: [], groups: [] }));
      })
      .catch(() => setSession(null));
  }, [refresh]);
  useEffect(() => {
    if (!session || session.mustChangePassword) return;
    const source = new EventSource("/api/v1/events", { withCredentials: true });
    source.addEventListener("snapshot", (event) => {
      const telemetry = JSON.parse(
        (event as MessageEvent).data,
      ) as TelemetrySnapshot;
      lastSample.current = Date.now();
      setFresh(true);
      setSnapshot((current) => ({ ...current, telemetry }));
      setLiveResources(current => appendResourceSample(current, telemetry));
    });
    source.addEventListener("job", (event) => {
      const job = JSON.parse((event as MessageEvent).data) as JobStatus;
      setSnapshot((current) => ({
        ...current,
        jobs: [job, ...current.jobs.filter((item) => item.id !== job.id)].slice(0, 20),
      }));
      notify({
        id: `job-${job.id}`,
        tone:
          job.state === "failed"
            ? "error"
            : job.state === "succeeded"
              ? "success"
              : "working",
        title: `${t(jobLabels[job.kind] ?? job.kind)} ${
          job.state === "failed"
            ? t("fehlgeschlagen")
            : job.state === "succeeded"
              ? t("abgeschlossen")
              : t("läuft")
        }`,
        message: job.message,
      });
      if (job.state === "succeeded") void refresh();
    });
    source.addEventListener("alert", (event) => {
      const alert = JSON.parse((event as MessageEvent).data) as {
        message: string;
      };
      setAlerts((current) => [alert.message, ...current]);
      notify({
        id: `alert-${Date.now()}`,
        tone: "error",
        title: t("Appliance-Alarm"),
        message: alert.message,
      });
    });
    source.onerror = () => setFresh(false);
    const stale = window.setInterval(
      () => setFresh(Date.now() - lastSample.current < 3000),
      1000,
    );
    return () => {
      source.close();
      window.clearInterval(stale);
    };
  }, [notify, refresh, session]);

  const submit = async (command: unknown) => {
    if (!session) return;
    try {
      const job = await api<JobStatus>(
        "/api/v1/repository/commands",
        {
          method: "POST",
          headers: { "idempotency-key": crypto.randomUUID() },
          body: JSON.stringify(command),
        },
        session.csrfToken,
      );
      setSnapshot((current) => ({ ...current, jobs: [job, ...current.jobs] }));
      notify({
        id: `job-${job.id}`,
        tone: "working",
        title: t("{job} gestartet", { job: t(jobLabels[job.kind] ?? job.kind) }),
        message: job.message,
      });
    } catch (reason) {
      notify({
        id: `request-${Date.now()}`,
        tone: "error",
        title: t("Aktion nicht gestartet"),
        message:
          reason instanceof Error
            ? reason.message
            : t("Die Appliance-Anfrage ist fehlgeschlagen"),
      });
    }
  };
  const runCommand = (kind: "mount" | "unmount" | "offline_scrub") =>
    kind === "mount" ? void submit({ kind }) : setConfirmAction(kind);
  const saveShare = async (share: ShareSettings) => {
    if (!session) return;
    try {
      const job = await api<JobStatus>(
        "/api/v1/shares",
        { method: "POST", body: JSON.stringify(share) },
        session.csrfToken,
      );
      setSnapshot((current) => ({
        ...current,
        jobs: [job, ...current.jobs],
      }));
      notify({
        id: `job-${job.id}`,
        tone: "working",
        title: t("SMB-Freigabe wird aktiviert"),
        message: job.message,
      });
    } catch (reason) {
      notify({
        id: `share-${Date.now()}`,
        tone: "error",
        title: t("Freigabe nicht geändert"),
        message: reason instanceof Error ? reason.message : t("Unbekannter Fehler"),
      });
    }
  };
  const removeShare = (share: ShareSettings) => {
    if (
      !window.confirm(
        t("Freigabe „{name}“ löschen? Aktive Sessions werden getrennt.", { name: share.name }),
      ) ||
      !session
    )
      return;
    void api<JobStatus>(
      `/api/v1/shares/${encodeURIComponent(share.id)}`,
      { method: "DELETE", headers: { "if-match": String(share.revision) } },
      session.csrfToken,
    )
      .then((job) => {
        setSnapshot((current) => ({
          ...current,
          jobs: [job, ...current.jobs],
        }));
        notify({
          id: `job-${job.id}`,
          tone: "working",
          title: t("Share-Löschung gestartet"),
          message: job.message,
        });
      })
      .catch((reason: Error) =>
        notify({
          id: `share-delete-${Date.now()}`,
          tone: "error",
          title: t("Freigabe nicht gelöscht"),
          message: reason.message,
        }),
      );
  };
  const saveSettings = (settings: RepositorySettings) =>
    void submit({
      kind: "update_settings",
      expected_revision: settings.revision,
      settings,
    });
  const exportAudit = async () => {
    try {
      const records = await api<AuditEvent[]>("/api/v1/audit");
      const cell = (value: string | number) =>
        `"${String(value).replaceAll('"', '""')}"`;
      const csv = [
        [t("Zeit"), t("Akteur"), t("Aktion"), t("Ergebnis"), "Details"],
        ...records.map((record) => [
          new Date(record.timestamp * 1000).toISOString(),
          record.actor,
          record.action,
          record.outcome,
          record.detail,
        ]),
      ]
        .map((row) => row.map(cell).join(";"))
        .join("\n");
      const url = URL.createObjectURL(
        new Blob([`\uFEFF${csv}\n`], { type: "text/csv;charset=utf-8" }),
      );
      const link = document.createElement("a");
      link.href = url;
      link.download = `fastdup-audit-${new Date().toISOString().slice(0, 10)}.csv`;
      link.click();
      URL.revokeObjectURL(url);
      notify({
        id: "audit-export",
        tone: "success",
        title: t("Audit exportiert"),
        message: t("{count} Audit-Einträge wurden als CSV bereitgestellt.", { count: records.length }),
      });
    } catch (reason) {
      notify({
        id: "audit-export",
        tone: "error",
        title: t("Audit-Export fehlgeschlagen"),
        message: reason instanceof Error ? reason.message : t("Unbekannter Fehler"),
      });
    }
  };
  const loadTelemetryHistory = async (seconds: number) => {
    const now = Math.floor(Date.now() / 1000);
    try {
      return await api<TelemetrySnapshot[]>(
        `/api/v1/telemetry/history?from=${now - seconds}&to=${now}&limit=1500`,
      );
    } catch (reason) {
      notify({
        id: "telemetry-history",
        tone: "error",
        title: t("Telemetrie-Zeitraum nicht geladen"),
        message: reason instanceof Error ? reason.message : t("Unbekannter Fehler"),
      });
      return [];
    }
  };
  const logout = () => {
    if (!session) return;
    api<void>(
      "/api/v1/session/logout",
      { method: "POST" },
      session.csrfToken,
    )
      .then(() => { setSession(null); setLiveResources([]); })
      .catch((reason: Error) =>
        notify({
          id: "logout",
          tone: "error",
          title: t("Abmeldung fehlgeschlagen"),
          message: reason.message,
        }),
      );
  };

  if (session === undefined)
    return (
      <div className="boot-screen">
        <div className="brand-mark">
          <img src={applianceMark} alt="" />
        </div>
        <span>{t("Control Plane wird geladen…")}</span>
      </div>
    );
  if (!session) return <Login onLogin={setSession} />;

  const operationBusy = snapshot.jobs.some(
    (job) => job.state === "queued" || job.state === "running",
  );

  let content: React.ReactNode;
  if (session.mustChangePassword)
    content = (
      <PasswordPage
        session={session}
        onChanged={setSession}
        onLogout={logout}
      />
    );
  else if (active === "Übersicht")
    content = (
      <Overview
        snapshot={snapshot.telemetry}
        disks={repositoryDisks(snapshot)}
        runCommand={runCommand}
        busy={operationBusy}
      />
    );
  else if (active === "Repository")
    content = (
      <RepositoryPage
        snapshot={snapshot}
        runCommand={runCommand}
        busy={operationBusy}
      />
    );
  else if (active === "Laufwerke")
    content = (
      <DrivesPage snapshot={snapshot} submit={submit} busy={operationBusy} />
    );
  else if (active === "SMB-Freigaben")
    content = (
      <SharesPage
        repositoryError={snapshot.repository?.state === "error"}
        repositoryReady={!!snapshot.repository && ["online", "unmounted"].includes(snapshot.repository.state)}
        setupRepository={() => setActive(snapshot.repository?.state === "error" ? "Repository" : "Laufwerke")}
        shares={snapshot.shares}
        principals={principals}
        save={(share) => void saveShare(share)}
        remove={removeShare}
      />
    );
  else if (active === "Telemetrie")
    content = (
      <TelemetryPage
        snapshot={snapshot.telemetry}
        disks={repositoryDisks(snapshot)}
        loadHistory={loadTelemetryHistory}
        liveResources={liveResources}
      />
    );
  else if (active === "Ereignisse")
    content = (
      <EventsPage jobs={snapshot.jobs} alerts={alerts} exportAudit={exportAudit} />
    );
  else
    content = (
      <SettingsPage
        csrfToken={session.csrfToken}
        username={session.username}
        onCertificateChanged={fingerprint => {
          setSnapshot(current => ({...current, certificateFingerprint: fingerprint}));
          setSession(current => current ? {...current, certificateFingerprint: fingerprint} : current);
        }}
        settings={snapshot.settings}
        fingerprint={
          snapshot.certificateFingerprint || session.certificateFingerprint
        }
        save={saveSettings}
        password={() => setSession({ ...session, mustChangePassword: true })}
        regenerateTls={() => {
          void api<{ certificateFingerprint: string }>(
            "/api/v1/tls/regenerate",
            { method: "POST" },
            session.csrfToken,
          )
            .then(({ certificateFingerprint }) => {
              setSnapshot((current) => ({
                ...current,
                certificateFingerprint,
              }));
              notify({
                id: "tls-regenerate",
                tone: "success",
                title: t("TLS-Zertifikat erneuert"),
                message: t("Das neue Zertifikat ist ohne Reboot aktiv."),
              });
            })
            .catch((reason: Error) =>
              notify({
                id: "tls-regenerate",
                tone: "error",
                title: t("TLS-Zertifikat nicht erneuert"),
                message: reason.message,
              }),
            );
        }}
      />
    );

  return (
    <div className="app-shell">
      <AppSidebar active={active} onChange={setActive} alarms={alerts.length} />
      <main ref={workspaceScroll}>
        <header className="topbar">
          <div>
            <h2>{session.mustChangePassword ? t("Sicherheit") : t(active)}</h2>
            <span>Appliance Control Plane</span>
          </div>
          <div className="topbar-actions">
            <Badge className={fresh ? "live" : "stale"}>
              <span className="pulse" />
              <span className="telemetry-status-full">{fresh ? "Live" : t("Telemetrie veraltet")}</span>
              <span className="telemetry-status-compact">{fresh ? "Live" : t("Veraltet")}</span>
            </Badge>
            <button
              aria-label={t("Benachrichtigungen")}
              onClick={() => setActive("Ereignisse")}
            >
              <Bell size={18} />
              {alerts.length > 0 && <i>{alerts.length}</i>}
            </button>
            <div className="account-menu" ref={accountMenu}>
              <button
                className="account-trigger"
                ref={accountTrigger}
                aria-haspopup="menu"
                aria-expanded={accountOpen}
                onClick={() => setAccountOpen((value) => !value)}
              >
                <span className="avatar">{session.username.slice(0, 2).toUpperCase()}</span>
                <span>
                  <strong>{session.username}</strong>
                  <small>Administrator</small>
                </span>
                <ChevronDown size={14} />
              </button>
              {accountOpen && (
                <div className="account-popover" role="menu" onKeyDown={event => {
                  if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
                  const items = Array.from(event.currentTarget.querySelectorAll<HTMLButtonElement>('[role="menuitem"]'));
                  const index = items.findIndex(item => item === document.activeElement);
                  const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 : (index + (event.key === "ArrowUp" ? -1 : 1) + items.length) % items.length;
                  event.preventDefault(); items[next]?.focus();
                }}>
                  <div>
                    <UserRound size={16} />
                    <span>
                      <strong>{session.username}</strong>
                      <small>Administrator</small>
                    </span>
                  </div>
                  <button role="menuitem" autoFocus onClick={() => {
                    setAccountOpen(false); setLanguageError(null); setUiPreferencesOpen(true);
                  }}><SlidersHorizontal size={16} />{t("UI-Einstellungen")}</button>
                  <button
                    role="menuitem"
                    onClick={() => {
                      setAccountOpen(false);
                      setActive("Einstellungen");
                    }}
                  ><KeyRound size={16} />{t("Passwort ändern")}</button>
                  <button role="menuitem" className="logout" onClick={logout}>
                    <LogOut size={15} />{t("Abmelden")}</button>
                </div>
              )}
            </div>
          </div>
        </header>
        <div className="page-content">{content}</div>
      </main>
      {!session.mustChangePassword && <RecentJobs jobs={snapshot.jobs} />}
      {uiPreferencesOpen && <UiPreferencesDialog language={language} saving={savingLanguage} error={languageError}
        onChange={changeLanguage} onClose={() => setUiPreferencesOpen(false)} trigger={accountTrigger} />}
      {confirmAction && (
        <ConfirmDialog
          danger={confirmAction !== "mount"}
          title={
            confirmAction === "offline_scrub"
              ? t("Offline-Scrub starten?")
              : t("Repository unmounten?")
          }
          confirmLabel={
            confirmAction === "offline_scrub"
              ? t("SMB unterbrechen & prüfen")
              : t("Kontrolliert unmounten")
          }
          onClose={() => setConfirmAction(null)}
          onConfirm={() => {
            const command = confirmAction;
            setConfirmAction(null);
            void submit({ kind: command });
          }}
        >
          <p>
            {confirmAction === "offline_scrub"
              ? t("Alle SMB-Sessions werden kontrolliert getrennt. Nach erfolgreicher Prüfung wird das Repository automatisch wieder online gebracht. Bei einem Fehler bleibt es sicher unmounted.")
              : t("Der Daemon erhält SIGINT, schreibt einen Checkpoint und hängt FUSE sauber aus. Ein automatisches SIGKILL wird niemals verwendet.")}
          </p>
        </ConfirmDialog>
      )}
      <NotificationCenter
        notices={notices}
        dismiss={(id) =>
          setNotices((current) => current.filter((notice) => notice.id !== id))
        }
      />
    </div>
  );
}

export function App() {
  return <I18nProvider><Application /></I18nProvider>;
}
