import {
  FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import ReactECharts from "echarts-for-react";
import {
  Activity,
  AlertTriangle,
  Bell,
  Box,
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
  Settings,
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
  ["Repository", Database],
  ["Laufwerke", HardDrive],
  ["SMB-Freigaben", Network],
  ["Telemetrie", Activity],
  ["Ereignisse", FileStack],
  ["Einstellungen", Settings],
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

function NotificationCenter({
  notices,
  dismiss,
}: {
  notices: Notice[];
  dismiss: (id: string) => void;
}) {
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
          <button onClick={() => dismiss(notice.id)} aria-label="Meldung schließen">
            <X />
          </button>
        </section>
      ))}
    </div>
  );
}

function formatBytes(value: number) {
  if (!Number.isFinite(value)) return "—";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let current = value;
  let unit = 0;
  while (current >= 1000 && unit < units.length - 1) {
    current /= 1000;
    unit += 1;
  }
  return `${new Intl.NumberFormat("de-DE", { maximumFractionDigits: 1 }).format(current)} ${units[unit]}`;
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
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">
          <Box size={21} />
        </div>
        <div>
          <strong>FastDup</strong>
          <span>CONTROL PLANE</span>
        </div>
      </div>
      <div className="appliance-label">
        <span>APPLIANCE</span>
        <strong>fd-appliance-01</strong>
      </div>
      <nav aria-label="Hauptnavigation">
        {navigation.map(([label, Icon]) => (
          <button
            key={label}
            className={active === label ? "active" : ""}
            onClick={() => onChange(label)}
          >
            <Icon size={18} />
            <span>{label}</span>
            {label === "Ereignisse" && alarms > 0 && <i>{alarms}</i>}
          </button>
        ))}
      </nav>
      <div className="sidebar-health">
        <ShieldCheck size={18} />
        <div>
          <strong>System geschützt</strong>
          <span>Kein Appliance-Reboot erforderlich</span>
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
          aria-label="Schließen"
        >
          <X size={17} />
        </button>
        <span className="dialog-icon">
          {danger ? <AlertTriangle /> : <ServerCog />}
        </span>
        <h2 id="dialog-title">{title}</h2>
        <div className="dialog-copy">{children}</div>
        <footer>
          <Button variant="secondary" onClick={onClose}>
            Abbrechen
          </Button>
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
          <div className="eyebrow">FASTDUP REPOSITORY</div>
          <h1>Production Repository</h1>
          <p>
            <Badge className={online ? "healthy" : "warning"}>
              <span className="status-dot" />
              {stateLabels[telemetry.repositoryState]}
            </Badge>
            <span>
              Generation {generation?.toLocaleString("de-DE") ?? "—"}
            </span>
            <span>
              {checkpointAge === undefined
                ? "Checkpoint —"
                : `Checkpoint vor ${checkpointAge} s`}
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

function throughputOption(snapshot: TelemetrySnapshot, extended = false) {
  return {
    animationDuration: 350,
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
    legend: extended ? { top: 4, textStyle: { color: "#8297a1" } } : undefined,
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: snapshot.series.map((point) => point.time),
      axisLabel: {
        color: "#70838d",
        formatter: (value: string) =>
          new Date(value).toLocaleTimeString("de-DE", {
            hour: "2-digit",
            minute: "2-digit",
          }),
      },
      axisLine: { lineStyle: { color: "#263842" } },
    },
    yAxis: {
      type: "value",
      name: "MB/s",
      nameTextStyle: { color: "#70838d" },
      axisLabel: { color: "#70838d" },
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
        areaStyle: { color: "rgba(34,211,238,.12)" },
      },
      {
        name: "POSIX Write",
        type: "line",
        smooth: 0.28,
        showSymbol: false,
        data: snapshot.series.map((point) => point.write),
        lineStyle: { color: "#8b5cf6", width: 2 },
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
  const chartOption = useMemo(() => throughputOption(snapshot), [snapshot]);
  const usedPercent = snapshot.dataCapacityBytes
    ? (snapshot.dataUsedBytes / snapshot.dataCapacityBytes) * 100
    : 0;
  return (
    <>
      <RepositoryHero telemetry={snapshot} runCommand={runCommand} busy={busy} />
      <section className="metric-grid">
        <MetricCard
          icon={Activity}
          label="POSIX Frontend Read"
          value={`${snapshot.frontendReadMbps.toFixed(1)} MB/s`}
          detail="Live · 1 Sekunde"
        />
        <MetricCard
          icon={Activity}
          label="POSIX Frontend Write"
          value={`${snapshot.frontendWriteMbps.toFixed(1)} MB/s`}
          detail="Live · 1 Sekunde"
          tone="violet"
        />
        <MetricCard
          icon={CircleGauge}
          label="Exact Dedup Rate"
          value={`${snapshot.dedupRate.toFixed(1)} %`}
          detail={`${snapshot.reductionRatio.toFixed(2)}× physische Gesamtreduktion`}
          tone="green"
        />
        <MetricCard
          icon={Cpu}
          label="Systemressourcen"
          value={`${snapshot.cpuPercent.toFixed(0)} % CPU`}
          detail={`${snapshot.ramPercent.toFixed(0)} % RAM belegt`}
          tone="amber"
        />
      </section>
      <section className="dashboard-grid">
        <Card className="throughput-card">
          <CardHeader>
            <div>
              <span className="section-kicker">LIVE PERFORMANCE</span>
              <h2>POSIX Frontend Throughput</h2>
            </div>
            <div className="legend">
              <span className="read">Read</span>
              <span className="write">Write</span>
              <span className="range-label">Letzte 15 min</span>
            </div>
          </CardHeader>
          <CardContent>
            <ReactECharts
              option={chartOption}
              style={{ height: 290 }}
              notMerge
              lazyUpdate
            />
          </CardContent>
        </Card>
        <Card className="capacity-card">
          <CardHeader>
            <div>
              <span className="section-kicker">DATA TIER</span>
              <h2>Physische Kapazität</h2>
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
                <strong>{usedPercent.toFixed(0)}%</strong>
                <span>belegt</span>
              </div>
            </div>
            <div className="capacity-numbers">
              <p>
                <span>Verwendet</span>
                <strong>{formatBytes(snapshot.dataUsedBytes)}</strong>
              </p>
              <p>
                <span>Verfügbar</span>
                <strong>
                  {formatBytes(
                    snapshot.dataCapacityBytes - snapshot.dataUsedBytes,
                  )}
                </strong>
              </p>
              <p>
                <span>Gesamt</span>
                <strong>{formatBytes(snapshot.dataCapacityBytes)}</strong>
              </p>
            </div>
            <div className="reserve-note">
              <ShieldCheck size={15} /> 10 % Operating Reserve aktiv
            </div>
          </CardContent>
        </Card>
      </section>
      <DiskTelemetryTable disks={disks} />
    </>
  );
}

function DiskTelemetryTable({ disks }: { disks: DiskTelemetry[] }) {
  return (
    <Card className="disk-card">
      <CardHeader>
        <div>
          <span className="section-kicker">STORAGE PATHS</span>
          <h2>Outstanding I/O pro Target</h2>
        </div>
        <Badge className="live">
          <span className="pulse" />1 s Sampler
        </Badge>
      </CardHeader>
      <CardContent>
        <div className="disk-table header-row">
          <span>Rolle & Gerät</span>
          <span>Typ / Kapazität</span>
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
              <small>{formatBytes(disk.capacityBytes)}</small>
            </span>
            <span>
              <strong>{disk.hbaPort || "nicht verfügbar"}</strong>
              <small>Hardwarepfad</small>
            </span>
            <span>
              <strong>
                {disk.readMbps.toFixed(0)} / {disk.writeMbps.toFixed(0)} MB/s
              </strong>
              <small>{disk.utilization.toFixed(0)} % Utilization</small>
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
                <span className="status-dot" />
                Gesund
              </Badge>
            </span>
          </div>
        ))}
        {disks.length === 0 && (
          <div className="disk-empty">
            Kein Repository gebunden – keine relevanten Targets.
          </div>
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
              <span className="section-kicker">RUNTIME</span>
              <h2>Zustandsmaschine</h2>
            </div>
            <Badge className="healthy">
              {stateLabels[snapshot.telemetry.repositoryState]}
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
              <span className="section-kicker">CHECKPOINT</span>
              <h2>Durability Status</h2>
            </div>
            <ShieldCheck size={19} />
          </CardHeader>
          <CardContent className="detail-list">
            <p>
              <span>Aktive Generation</span>
              <strong>
                {snapshot.telemetry.commitGeneration?.toLocaleString("de-DE") ??
                  "—"}
              </strong>
            </p>
            <p>
              <span>Checkpoint-Alter</span>
              <strong>
                {snapshot.telemetry.lastCheckpointSeconds === undefined
                  ? "—"
                  : `${snapshot.telemetry.lastCheckpointSeconds} Sekunden`}
              </strong>
            </p>
            <p>
              <span>Recovery-Latch</span>
              <strong className="ok">Bereit</strong>
            </p>
            <p>
              <span>Swap-Grenze</span>
              <strong>0 B</strong>
            </p>
          </CardContent>
        </Card>
      </div>
      <Card className="action-card">
        <CardHeader>
          <div>
            <span className="section-kicker">WARTUNG</span>
            <h2>Repository-Aktionen</h2>
          </div>
        </CardHeader>
        <CardContent>
          <div>
            <TerminalSquare />
            <span>
              <strong>Offline-Scrub</strong>
              <small>
                Prüft Pool-Identität, Metadaten und DATA-Container. SMB wird
                kontrolliert unterbrochen.
              </small>
            </span>
            <Button
              variant="secondary"
              onClick={() => runCommand("offline_scrub")}
              disabled={!online || busy}
            >
              Starten
            </Button>
          </div>
          <div>
            <RefreshCcw />
            <span>
              <strong>Sauberer Remount</strong>
              <small>
                SIGINT, Checkpoint, FUSE-Unmount, Start und Health-Check ohne
                Appliance-Reboot.
              </small>
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
          {target.targetType} · {formatBytes(target.capacityBytes)}
        </small>
        <small>{target.hbaPort || "HBA-Port nicht verfügbar"}</small>
        <small>
          SN {target.serial || "nicht verfügbar"} ·{" "}
          {target.filesystem || "unformatiert"}
        </small>
        {target.backingDisks.length > 0 && (
          <small>{target.backingDisks.length} physische Backing-Disks</small>
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
          <span className="section-kicker">BLOCK INVENTORY</span>
          <h1>Laufwerke & Provisionierung</h1>
          <p>
            Nur erkannte, sichere Targets können ausgewählt werden. Gerätepfade
            sind niemals Freitext.
          </p>
        </div>
        <Badge>
          <RefreshCcw size={12} />
          Inventar aktuell
        </Badge>
      </div>
      <div className="selection-columns">
        <Card>
          <CardHeader>
            <div>
              <span className="step-number">1</span>
              <h2>Metadata-Target auswählen</h2>
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
              <h2>DATA-Target auswählen</h2>
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
            <strong>Schutzprüfung aktiv</strong>
            <small>
              Root, Boot, Swap, Mounts, Holder und gemeinsame physische
              Abstammung werden ausgeschlossen.
            </small>
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
          <Trash2 size={15} /> Neues Repository initialisieren
        </Button>
      </div>
      {confirm && selectedMeta && selectedData && (
        <ConfirmDialog
          danger
          title="Targets vollständig löschen?"
          confirmLabel="LÖSCHEN & INITIALISIEREN"
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
          <p>
            Diese Aktion entfernt unwiderruflich alle Daten auf beiden Targets.
          </p>
          <div className="wipe-summary">
            <strong>METADATA</strong>
            <span>
              {selectedMeta.model} · {formatBytes(selectedMeta.capacityBytes)}
            </span>
            <strong>DATA</strong>
            <span>
              {selectedData.model} · {formatBytes(selectedData.capacityBytes)}
            </span>
          </div>
          <p>
            GPT und XFS werden neu angelegt; anschließend wird die
            Pool-Identität geschrieben und das Repository online gebracht.
          </p>
        </ConfirmDialog>
      )}
    </>
  );
}

const emptyShare: ShareSettings = {
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
  shares,
  save,
  remove,
  principals,
}: {
  shares: ShareSettings[];
  save: (share: ShareSettings) => void;
  remove: (share: ShareSettings) => void;
  principals: { users: string[]; groups: string[] };
}) {
  const [editing, setEditing] = useState<ShareSettings | null>(null);
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">SAMBA 4.23.5</span>
          <h1>SMB-Freigaben</h1>
          <p>
            FastDup-optimierte Shares werden atomar validiert und ohne Reboot
            neu geladen.
          </p>
        </div>
        <Button
          variant="secondary"
          onClick={() => setEditing({ ...emptyShare, id: crypto.randomUUID() })}
        >
          <Plus size={15} /> Freigabe anlegen
        </Button>
      </div>
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
              <Badge>{share.hidden ? "Versteckt" : "Sichtbar"}</Badge>
            </CardHeader>
            <CardContent>
              <div className="share-flags">
                <span>{share.readOnly ? "Read only" : "Read / Write"}</span>
                <span>
                  {share.encryption === "required"
                    ? "Encryption required"
                    : "Encryption desired"}
                </span>
                <span>{share.accessBasedEnumeration ? "ABE" : "Kein ABE"}</span>
                <span className={share.guestAccess ? "danger-text" : ""}>
                  {share.guestAccess ? "Gastzugriff" : "Authentifiziert"}
                </span>
                <span className={share.logicalQuota ? "quota-flag" : ""}>
                  {share.logicalQuota
                    ? `Quota ${share.logicalQuota.value} ${share.logicalQuota.unit.toUpperCase()}`
                    : "Repository-Kapazität"}
                </span>
              </div>
              <div className="fixed-profile">
                <LockKeyhole size={15} />
                <span>
                  <strong>FastDup Optimized VFS</strong>
                  <small>64 KiB Clone Alignment · 1 GiB Maximum</small>
                </span>
                <Badge className="warning">EXPERIMENTELL</Badge>
              </div>
              <footer>
                <Button
                  variant="ghost"
                  onClick={() => setEditing({ ...share })}
                >
                  <Pencil size={14} /> Bearbeiten
                </Button>
                <Button variant="ghost" onClick={() => remove(share)}>
                  <Trash2 size={14} /> Löschen
                </Button>
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
    <div
      className="drawer-backdrop"
      onMouseDown={(event) => event.target === event.currentTarget && onClose()}
    >
      <form className="drawer" onSubmit={submit}>
        <header>
          <div>
            <span className="section-kicker">SMB SHARE</span>
            <h2>{share.revision ? "Freigabe bearbeiten" : "Neue Freigabe"}</h2>
          </div>
          <button type="button" onClick={onClose}>
            <X />
          </button>
        </header>
        <div className="drawer-body">
          <label className="field">
            <span>Freigabename</span>
            <input
              required
              pattern="[A-Za-z0-9][A-Za-z0-9._-]{0,63}"
              value={draft.name}
              onChange={(event) => set("name", event.target.value)}
              placeholder="z. B. production"
            />
            <small>
              Nur Buchstaben, Ziffern, Punkt, Unterstrich und Bindestrich.
            </small>
          </label>
          <label className="field">
            <span>Beschreibung</span>
            <input
              maxLength={120}
              value={draft.description}
              onChange={(event) => set("description", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Logisches Schreiblimit</span>
            <div className="capacity-input">
              <input
                aria-label="Kapazitätswert"
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
                aria-label="Kapazitätseinheit"
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
                <option value="none">Keine</option>
                <option value="gb">GB</option>
                <option value="tb">TB</option>
                <option value="pb">PB</option>
              </select>
            </div>
            <small>
              1–999 · Wird am Share-Root angezeigt und als harte logische
              Quota erzwungen. Dedup- und Clone-Daten zählen vollständig;
              Sparse-Holes nicht.
            </small>
          </label>
          <div className="toggle-stack">
            <Toggle
              checked={draft.enabled}
              onChange={(value) => set("enabled", value)}
              label="Freigabe aktiv"
            />
            <Toggle
              checked={draft.hidden}
              onChange={(value) => set("hidden", value)}
              label="Versteckt"
              detail="Setzt browseable = no; der Name bleibt unverändert."
            />
            <Toggle
              checked={draft.readOnly}
              onChange={(value) => set("readOnly", value)}
              label="Schreibgeschützt"
            />
            <Toggle
              dangerous
              checked={draft.guestAccess}
              onChange={(value) => set("guestAccess", value)}
              label="Gastzugriff erlauben"
              detail="Erlaubt Zugriff ohne authentifizierten Benutzer."
            />
            <Toggle
              checked={draft.accessBasedEnumeration}
              onChange={(value) => set("accessBasedEnumeration", value)}
              label="Access-Based Enumeration"
            />
          </div>
          <label className="field">
            <span>SMB-Verschlüsselung</span>
            <select
              value={draft.encryption}
              onChange={(event) =>
                set(
                  "encryption",
                  event.target.value as ShareSettings["encryption"],
                )
              }
            >
              <option value="desired">Gewünscht</option>
              <option value="required">Erforderlich</option>
            </select>
          </label>
      <PrincipalPicker
        title="Erlaubte Samba-Benutzer"
        items={[...new Set([...principals.users, ...draft.allowedUsers])]}
            selected={draft.allowedUsers}
            onToggle={(item) => toggleItem("allowedUsers", item)}
          />
      <PrincipalPicker
        title="Erlaubte lokale Gruppen"
        items={[...new Set([...principals.groups, ...draft.allowedGroups])]}
            selected={draft.allowedGroups}
            onToggle={(item) => toggleItem("allowedGroups", item)}
          />
          <div className="profile-lock">
            <LockKeyhole />
            <span>
              <strong>Optimiertes Profil fest vorgegeben</strong>
              <small>
                vfs objects = fastdup · FastDup aktiv · 64 KiB Alignment · 1 GiB
                Maximum
              </small>
            </span>
          </div>
        </div>
        <footer>
          <Button type="button" variant="secondary" onClick={onClose}>
            Abbrechen
          </Button>
          <Button type="submit" variant="secondary">
            <Save size={15} /> Aktivieren
          </Button>
        </footer>
      </form>
      {guestWarning && (
        <ConfirmDialog
          danger
          title="Gastzugriff aktivieren?"
          confirmLabel="Gastzugriff aktivieren"
          onClose={() => setGuestWarning(false)}
          onConfirm={() => {
            setGuestWarning(false);
            onSave(draft);
          }}
        >
          <p>
            Nicht authentifizierte Clients erhalten entsprechend der
            Freigaberechte Zugriff. Diese Änderung wird sofort per Samba-Reload
            aktiviert.
          </p>
        </ConfirmDialog>
      )}
    </div>
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
}: {
  snapshot: TelemetrySnapshot;
  disks: DiskTelemetry[];
  loadHistory: (seconds: number) => Promise<TelemetrySnapshot[]>;
}) {
  const [range, setRange] = useState("15 min");
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
  const resourceSamples = history ?? [snapshot];
  const selectRange = (item: string) => {
    setRange(item);
    if (item === "Live") {
      setHistory(null);
      return;
    }
    setLoading(true);
    void loadHistory(rangeSeconds[item] ?? 900)
      .then(setHistory)
      .finally(() => setLoading(false));
  };
  const resourceOption = useMemo(
    () => ({
      ...throughputOption(displayedSnapshot, true),
      xAxis: {
        type: "category",
        data: resourceSamples.map((sample) => sample.observedAt),
        axisLabel: { color: "#70838d" },
        axisLine: { lineStyle: { color: "#324650" } },
      },
      yAxis: {
        type: "value",
        max: 100,
        name: "%",
        axisLabel: { color: "#70838d" },
        splitLine: { lineStyle: { color: "rgba(68,94,108,.24)" } },
      },
      series: [
        {
          name: "CPU",
          type: "line",
          showSymbol: false,
          data: resourceSamples.map((sample) => sample.cpuPercent),
          lineStyle: { color: "#f5b84b" },
        },
        {
          name: "RAM",
          type: "line",
          showSymbol: false,
          data: resourceSamples.map((sample) => sample.ramPercent),
          lineStyle: { color: "#3ddc97" },
        },
      ],
    }),
    [displayedSnapshot, resourceSamples],
  );
  return (
    <>
      <div className="page-title telemetry-title">
        <div>
          <span className="section-kicker">OBSERVABILITY</span>
          <h1>Tiefentelemetrie</h1>
          <p>
            Synchronisierte Live-Daten aus POSIX-Rand, Host, Prozess und
            physischem Block-Layer.
          </p>
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
          value={`${snapshot.frontendReadMbps.toFixed(1)} MB/s`}
          detail="POSIX erfolgreich"
        />
        <MetricCard
          icon={Activity}
          label="Frontend Write"
          value={`${snapshot.frontendWriteMbps.toFixed(1)} MB/s`}
          detail="POSIX erfolgreich"
          tone="violet"
        />
        <MetricCard
          icon={CircleGauge}
          label="Dedup Rate"
          value={`${snapshot.dedupRate.toFixed(1)} %`}
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
          detail="Alter der aktiven Generation"
          tone="amber"
        />
      </section>
      <Card className="telemetry-chart">
        <CardHeader>
          <div>
            <span className="section-kicker">
              RANGE · {range.toUpperCase()}
            </span>
            <h2>POSIX Throughput & gemeinsamer Zeitcursor</h2>
          </div>
          <Badge className={loading ? "warning" : "live"}>
            <span className="pulse" />
            {loading ? "LÄDT" : range === "Live" ? "LIVE" : range.toUpperCase()}
          </Badge>
        </CardHeader>
        <CardContent>
          <ReactECharts
            option={throughputOption(displayedSnapshot, true)}
            style={{ height: 330 }}
          />
        </CardContent>
      </Card>
      <div className="two-column telemetry-row">
        <Card className="telemetry-chart">
          <CardHeader>
            <div>
              <span className="section-kicker">HOST & PROCESS</span>
              <h2>CPU und RAM</h2>
            </div>
          </CardHeader>
          <CardContent>
            <ReactECharts option={resourceOption} style={{ height: 260 }} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div>
              <span className="section-kicker">REDUCTION</span>
              <h2>Dedup & physische Reduktion</h2>
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
                <strong>{snapshot.dedupRate.toFixed(1)}%</strong>
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
                <strong>{snapshot.reductionRatio.toFixed(2)}×</strong>
              </span>
              <small>Gesamtreduktion</small>
            </div>
            <p>
              FILL und Recipe-Reuse sind bewusst nicht in der Exact-Dedup-Rate
              enthalten.
            </p>
          </CardContent>
        </Card>
      </div>
      <DiskTelemetryTable disks={disks} />
      <div className="telemetry-tabs">
        <span>Latenzen p50/p95/p99</span>
        <span>io_uring In-Flight</span>
        <span>Cache Hit Rates</span>
        <span>GC & Reduction</span>
        <span>Checkpoint Phasen</span>
      </div>
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
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">AUDIT & JOBS</span>
          <h1>Ereignisse</h1>
          <p>
            Nachvollziehbare Managementaktionen, Jobs, Warnungen und Alarme.
          </p>
        </div>
        <Button variant="secondary" onClick={exportAudit}>
          <FileStack size={15} /> Audit exportieren
        </Button>
      </div>
      {alerts.map((alert, index) => (
        <div className="alarm-banner" key={`${alert}-${index}`}>
          <AlertTriangle />
          <span>
            <strong>Kritischer Alarm</strong>
            <small>{alert}</small>
          </span>
        </div>
      ))}
      <Card className="events-table">
        <CardHeader>
          <div>
            <span className="section-kicker">LETZTE VORGÄNGE</span>
            <h2>Job-Verlauf</h2>
          </div>
        </CardHeader>
        <CardContent>
          <div className="event-row event-head">
            <span>Zeit</span>
            <span>Aktion</span>
            <span>Status</span>
            <span>Fortschritt</span>
            <span>Ergebnis</span>
          </div>
          {jobs.map((job) => (
            <div className="event-row" key={job.id}>
              <span>
                {new Date(job.updatedAt * 1000).toLocaleString("de-DE")}
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
                {(job.progressBasisPoints / 100).toFixed(0)} %
              </span>
              <span>{job.message}</span>
            </div>
          ))}
          {jobs.length === 0 && (
            <div className="event-empty">Noch keine Managementvorgänge.</div>
          )}
        </CardContent>
      </Card>
    </>
  );
}

function SettingsPage({
  settings,
  fingerprint,
  save,
  password,
  regenerateTls,
}: {
  settings: RepositorySettings;
  fingerprint: string;
  save: (value: RepositorySettings) => void;
  password: () => void;
  regenerateTls: () => void;
}) {
  const [draft, setDraft] = useState(settings);
  const [confirm, setConfirm] = useState(false);
  useEffect(() => setDraft(settings), [settings]);
  const set = <K extends keyof RepositorySettings>(
    key: K,
    value: RepositorySettings[K],
  ) => setDraft((current) => ({ ...current, [key]: value }));
  const needsRemount = draft.advancedReduction !== settings.advancedReduction;
  return (
    <>
      <div className="page-title">
        <div>
          <span className="section-kicker">RUNTIME CONFIGURATION</span>
          <h1>Einstellungen</h1>
          <p>
            Bestätigte Änderungen werden sofort aktiv oder vollständig
            zurückgerollt.
          </p>
        </div>
        <Button
          variant="secondary"
          onClick={() => (needsRemount ? setConfirm(true) : save(draft))}
        >
          <Save size={15} /> Übernehmen
        </Button>
      </div>
      <div className="settings-layout">
        <Card>
          <CardHeader>
            <div>
              <span className="section-kicker">REPOSITORY</span>
              <h2>Betriebsrichtlinien</h2>
            </div>
          </CardHeader>
          <CardContent className="settings-form">
            <Toggle
              checked={draft.autoMount}
              onChange={(value) => set("autoMount", value)}
              label="Auto-Mount"
              detail="Repository nach Dienststart automatisch online bringen."
            />
            <Toggle
              checked={draft.onlineGcEnabled}
              onChange={(value) => set("onlineGcEnabled", value)}
              label="Online-GC"
              detail="Hot-fähig; wird sofort über den Management-Socket aktiviert."
            />
            <label className="field">
              <span>Advanced Reduction</span>
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
                <option value="off">Aus</option>
                <option value="prefix_v1">Prefix v1</option>
              </select>
              <small>
                Eine Änderung erfordert automatischen Remount und gegebenenfalls
                Index-Rebuild.
              </small>
            </label>
            <label className="field">
              <span>Wartungsfenster (UTC)</span>
              <input
                value={draft.maintenanceWindowUtc || ""}
                onChange={(event) =>
                  set("maintenanceWindowUtc", event.target.value)
                }
                placeholder="Sonntag 02:00–05:00"
              />
            </label>
            <div className="thresholds">
              <label className="field">
                <span>Pressure Low</span>
                <input
                  type="number"
                  min="5000"
                  max="9900"
                  value={draft.pressureLowBasisPoints}
                  onChange={(event) =>
                    set("pressureLowBasisPoints", Number(event.target.value))
                  }
                />
                <small>Basispunkte</small>
              </label>
              <label className="field">
                <span>Pressure High</span>
                <input
                  type="number"
                  min="5000"
                  max="9950"
                  value={draft.pressureHighBasisPoints}
                  onChange={(event) =>
                    set("pressureHighBasisPoints", Number(event.target.value))
                  }
                />
                <small>Basispunkte</small>
              </label>
            </div>
          </CardContent>
        </Card>
        <aside>
          <Card>
            <CardHeader>
              <div>
                <span className="section-kicker">SICHERHEIT</span>
                <h2>Administrator</h2>
              </div>
              <UserRound size={18} />
            </CardHeader>
            <CardContent className="security-card">
              <p>
                <span>Benutzer</span>
                <strong>admin</strong>
              </p>
              <p>
                <span>Rolle</span>
                <strong>Administrator</strong>
              </p>
              <Button variant="secondary" onClick={password}>
                <KeyRound size={15} /> Passwort ändern
              </Button>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <div>
                <span className="section-kicker">TLS</span>
                <h2>Zertifikat-Fingerprint</h2>
              </div>
              <ShieldCheck size={18} />
            </CardHeader>
            <CardContent className="fingerprint">
              <code>{fingerprint}</code>
              <small>SHA-256 · lokal selbstsigniert · Reload ohne Reboot</small>
              <Button variant="secondary" onClick={regenerateTls}>
                <RefreshCcw size={14} /> Zertifikat regenerieren
              </Button>
            </CardContent>
          </Card>
        </aside>
      </div>
      {confirm && (
        <ConfirmDialog
          title="Automatischen Remount durchführen?"
          confirmLabel="Änderung aktivieren"
          onClose={() => setConfirm(false)}
          onConfirm={() => {
            setConfirm(false);
            save(draft);
          }}
        >
          <p>
            Advanced Reduction überschreitet eine Prozessgrenze. FastDup führt
            kontrolliert Unmount, Indexprüfung, Start und Health-Check aus. Bei
            einem Fehler wird die vorherige Konfiguration wiederhergestellt.
          </p>
        </ConfirmDialog>
      )}
    </>
  );
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
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [repeat, setRepeat] = useState("");
  const [error, setError] = useState("");
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (next.length < 12 || next !== repeat) {
      setError(
        "Das neue Passwort muss mindestens 12 Zeichen lang sein und übereinstimmen.",
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
            <span className="section-kicker">ERSTZUGANG</span>
            <h2>Initialpasswort ändern</h2>
          </div>
          <KeyRound />
        </CardHeader>
        <CardContent>
          <div className="gate-note">
            <AlertTriangle />
            <span>
              Bis zum Passwortwechsel sind alle Managementaktionen gesperrt.
            </span>
          </div>
          <form onSubmit={submit}>
            <label className="field">
              <span>Aktuelles Passwort</span>
              <input
                type="password"
                autoComplete="current-password"
                value={current}
                onChange={(event) => setCurrent(event.target.value)}
              />
            </label>
            <label className="field">
              <span>Neues Passwort</span>
              <input
                type="password"
                minLength={12}
                autoComplete="new-password"
                value={next}
                onChange={(event) => setNext(event.target.value)}
              />
            </label>
            <label className="field">
              <span>Neues Passwort wiederholen</span>
              <input
                type="password"
                minLength={12}
                autoComplete="new-password"
                value={repeat}
                onChange={(event) => setRepeat(event.target.value)}
              />
            </label>
            {error && <p className="form-error">{error}</p>}
            <Button type="submit" variant="secondary">
              Passwort setzen & aktivieren
            </Button>
          </form>
          <button className="text-button" onClick={onLogout}>
            Abmelden
          </button>
        </CardContent>
      </Card>
    </div>
  );
}

function Login({ onLogin }: { onLogin: (session: SessionInfo) => void }) {
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
      <section className="login-brand">
        <div className="brand-mark">
          <Box size={25} />
        </div>
        <div>
          <strong>FastDup</strong>
          <span>APPLIANCE CONTROL PLANE</span>
        </div>
      </section>
      <Card className="login-card">
        <CardHeader>
          <div>
            <span className="section-kicker">SICHERE VERBINDUNG</span>
            <h2>Administrator-Anmeldung</h2>
          </div>
          <ShieldCheck size={20} />
        </CardHeader>
        <CardContent>
          <p>Lokale Verwaltung der FastDup Storage Appliance.</p>
          <form onSubmit={submit}>
            <label className="field">
              <span>Benutzername</span>
              <input
                autoFocus
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
              />
            </label>
            <label className="field">
              <span>Passwort</span>
              <input
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
            </label>
            {error && <p className="form-error">{error}</p>}
            <Button type="submit" variant="secondary" disabled={busy}>
              {busy ? "Anmeldung…" : "Anmelden"}
            </Button>
          </form>
          <div className="local-note">
            <LockKeyhole size={15} /> Appliance-lokal · HttpOnly Session ·
            Argon2id
          </div>
        </CardContent>
      </Card>
    </main>
  );
}

export function App() {
  const [active, setActive] = useState("Übersicht");
  const [snapshot, setSnapshot] = useState<ApplianceSnapshot>(() =>
    emptyApplianceSnapshot(),
  );
  const [session, setSession] = useState<SessionInfo | null | undefined>(
    undefined,
  );
  const [fresh, setFresh] = useState(false);
  const [accountOpen, setAccountOpen] = useState(false);
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
    });
    source.addEventListener("job", (event) => {
      const job = JSON.parse((event as MessageEvent).data) as JobStatus;
      setSnapshot((current) => ({
        ...current,
        jobs: [job, ...current.jobs.filter((item) => item.id !== job.id)],
      }));
      notify({
        id: `job-${job.id}`,
        tone:
          job.state === "failed"
            ? "error"
            : job.state === "succeeded"
              ? "success"
              : "working",
        title: `${jobLabels[job.kind] ?? job.kind} ${
          job.state === "failed"
            ? "fehlgeschlagen"
            : job.state === "succeeded"
              ? "abgeschlossen"
              : "läuft"
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
        title: "Appliance-Alarm",
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
        title: `${jobLabels[job.kind] ?? job.kind} gestartet`,
        message: job.message,
      });
    } catch (reason) {
      notify({
        id: `request-${Date.now()}`,
        tone: "error",
        title: "Aktion nicht gestartet",
        message:
          reason instanceof Error
            ? reason.message
            : "Die Appliance-Anfrage ist fehlgeschlagen",
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
        title: "SMB-Freigabe wird aktiviert",
        message: job.message,
      });
    } catch (reason) {
      notify({
        id: `share-${Date.now()}`,
        tone: "error",
        title: "Freigabe nicht geändert",
        message: reason instanceof Error ? reason.message : "Unbekannter Fehler",
      });
    }
  };
  const removeShare = (share: ShareSettings) => {
    if (
      !window.confirm(
        `Freigabe „${share.name}“ löschen? Aktive Sessions werden getrennt.`,
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
          title: "Share-Löschung gestartet",
          message: job.message,
        });
      })
      .catch((reason: Error) =>
        notify({
          id: `share-delete-${Date.now()}`,
          tone: "error",
          title: "Freigabe nicht gelöscht",
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
        ["Zeit", "Akteur", "Aktion", "Ergebnis", "Details"],
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
        title: "Audit exportiert",
        message: `${records.length} Audit-Einträge wurden als CSV bereitgestellt.`,
      });
    } catch (reason) {
      notify({
        id: "audit-export",
        tone: "error",
        title: "Audit-Export fehlgeschlagen",
        message: reason instanceof Error ? reason.message : "Unbekannter Fehler",
      });
    }
  };
  const loadTelemetryHistory = async (seconds: number) => {
    const now = Math.floor(Date.now() / 1000);
    try {
      return await api<TelemetrySnapshot[]>(
        `/api/v1/telemetry/history?from=${now - seconds}&to=${now}&limit=50000`,
      );
    } catch (reason) {
      notify({
        id: "telemetry-history",
        tone: "error",
        title: "Telemetrie-Zeitraum nicht geladen",
        message: reason instanceof Error ? reason.message : "Unbekannter Fehler",
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
      .then(() => setSession(null))
      .catch((reason: Error) =>
        notify({
          id: "logout",
          tone: "error",
          title: "Abmeldung fehlgeschlagen",
          message: reason.message,
        }),
      );
  };

  if (session === undefined)
    return (
      <div className="boot-screen">
        <div className="brand-mark">
          <Box />
        </div>
        <span>Control Plane wird geladen…</span>
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
      />
    );
  else if (active === "Ereignisse")
    content = (
      <EventsPage jobs={snapshot.jobs} alerts={alerts} exportAudit={exportAudit} />
    );
  else
    content = (
      <SettingsPage
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
                title: "TLS-Zertifikat erneuert",
                message: "Das neue Zertifikat ist ohne Reboot aktiv.",
              });
            })
            .catch((reason: Error) =>
              notify({
                id: "tls-regenerate",
                tone: "error",
                title: "TLS-Zertifikat nicht erneuert",
                message: reason.message,
              }),
            );
        }}
      />
    );

  return (
    <div className="app-shell">
      <AppSidebar active={active} onChange={setActive} alarms={alerts.length} />
      <main>
        <header className="topbar">
          <div>
            <h2>{session.mustChangePassword ? "Sicherheit" : active}</h2>
            <span>Appliance Control Plane</span>
          </div>
          <div className="topbar-actions">
            <Badge className={fresh ? "live" : "stale"}>
              <span className="pulse" />
              {fresh ? "LIVE" : "TELEMETRIE VERALTET"}
            </Badge>
            <button
              aria-label="Benachrichtigungen"
              onClick={() => setActive("Ereignisse")}
            >
              <Bell size={18} />
              {alerts.length > 0 && <i>{alerts.length}</i>}
            </button>
            <div className="account-menu">
              <button
                className="account-trigger"
                aria-haspopup="menu"
                aria-expanded={accountOpen}
                onClick={() => setAccountOpen((value) => !value)}
              >
                <span className="avatar">AD</span>
                <span>
                  <strong>{session.username}</strong>
                  <small>Administrator</small>
                </span>
                <ChevronDown size={14} />
              </button>
              {accountOpen && (
                <div className="account-popover" role="menu">
                  <div>
                    <UserRound size={16} />
                    <span>
                      <strong>{session.username}</strong>
                      <small>Administrator</small>
                    </span>
                  </div>
                  <button
                    role="menuitem"
                    onClick={() => {
                      setAccountOpen(false);
                      setActive("Einstellungen");
                    }}
                  >
                    Passwort ändern
                  </button>
                  <button role="menuitem" className="logout" onClick={logout}>
                    <LogOut size={15} /> Abmelden
                  </button>
                </div>
              )}
            </div>
          </div>
        </header>
        <div className="page-content">{content}</div>
      </main>
      {confirmAction && (
        <ConfirmDialog
          danger={confirmAction !== "mount"}
          title={
            confirmAction === "offline_scrub"
              ? "Offline-Scrub starten?"
              : "Repository unmounten?"
          }
          confirmLabel={
            confirmAction === "offline_scrub"
              ? "SMB unterbrechen & prüfen"
              : "Kontrolliert unmounten"
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
              ? "Alle SMB-Sessions werden kontrolliert getrennt. Nach erfolgreicher Prüfung wird das Repository automatisch wieder online gebracht. Bei einem Fehler bleibt es sicher unmounted."
              : "Der Daemon erhält SIGINT, schreibt einen Checkpoint und hängt FUSE sauber aus. Ein automatisches SIGKILL wird niemals verwendet."}
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
