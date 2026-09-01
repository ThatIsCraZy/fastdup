export type RepositoryState =
  | "uninitialized"
  | "provisioning"
  | "unmounted"
  | "mounting"
  | "recovering"
  | "online"
  | "unmounting"
  | "scrubbing"
  | "error";

export interface DiskTelemetry {
  id: string;
  role: string;
  model: string;
  kind: string;
  capacityBytes: number;
  hbaPort: string;
  outstandingIo: number;
  readMbps: number;
  writeMbps: number;
  readIops?: number;
  writeIops?: number;
  utilization: number;
}

export interface SeriesPoint {
  time: string;
  read: number;
  write: number;
}

export interface TelemetrySnapshot {
  sequence: number;
  observedAt: string;
  repositoryState: RepositoryState;
  frontendReadMbps: number;
  frontendWriteMbps: number;
  dedupRate: number;
  reductionRatio: number;
  cpuPercent: number;
  ramPercent: number;
  dataUsedBytes: number;
  dataCapacityBytes: number;
  lastCheckpointSeconds: number;
  disks: DiskTelemetry[];
  series: SeriesPoint[];
}

export interface BackingDisk {
  stableId: string;
  model: string;
  serial: string;
  hbaPort: string;
}
export interface BlockTarget {
  stableId: string;
  path: string;
  kernelName: string;
  model: string;
  serial: string;
  wwn: string;
  targetType: string;
  capacityBytes: number;
  hbaPort: string;
  filesystem?: string;
  eligible: boolean;
  eligibilityReason?: string;
  backingDisks: BackingDisk[];
  inventoryRevision: string;
}
export interface RepositorySettings {
  revision: number;
  autoMount: boolean;
  advancedReduction: "off" | "prefix_v1";
  onlineGcEnabled: boolean;
  maintenanceWindowUtc?: string;
  pressureLowBasisPoints: number;
  pressureHighBasisPoints: number;
}
export interface ShareSettings {
  id: string;
  revision: number;
  name: string;
  description: string;
  enabled: boolean;
  hidden: boolean;
  readOnly: boolean;
  guestAccess: boolean;
  encryption: "desired" | "required";
  accessBasedEnumeration: boolean;
  allowedUsers: string[];
  allowedGroups: string[];
  logicalQuota?: {
    value: number;
    unit: "gb" | "tb" | "pb";
  };
}
export interface JobStatus {
  id: string;
  kind: string;
  state: "queued" | "running" | "succeeded" | "failed";
  progressBasisPoints: number;
  message: string;
  createdAt: number;
  updatedAt: number;
}
export interface ApplianceSnapshot {
  telemetry: TelemetrySnapshot;
  targets: BlockTarget[];
  settings: RepositorySettings;
  shares: ShareSettings[];
  jobs: JobStatus[];
  certificateFingerprint: string;
}
export interface SessionInfo {
  username: string;
  csrfToken: string;
  mustChangePassword: boolean;
  certificateFingerprint: string;
}

const series = Array.from({ length: 90 }, (_, index) => ({
  time: new Date(Date.now() - (89 - index) * 10_000).toISOString(),
  read: 590 + Math.sin(index / 4) * 130 + (index % 7) * 12,
  write: 470 + Math.cos(index / 5) * 105 + (index % 5) * 14,
}));

export const previewTelemetry: TelemetrySnapshot = {
  sequence: 18427,
  observedAt: new Date().toISOString(),
  repositoryState: "online",
  frontendReadMbps: 842.6,
  frontendWriteMbps: 611.4,
  dedupRate: 72.8,
  reductionRatio: 3.68,
  cpuPercent: 46.2,
  ramPercent: 61.8,
  dataUsedBytes: 47.3e12,
  dataCapacityBytes: 80e12,
  lastCheckpointSeconds: 3,
  disks: [
    {
      id: "meta",
      role: "Metadata",
      model: "Micron 7450 MAX",
      kind: "NVMe SSD",
      capacityBytes: 3.2e12,
      hbaPort: "PCIe 0000:41:00.0",
      outstandingIo: 7,
      readMbps: 126,
      writeMbps: 184,
      readIops: 14800,
      writeIops: 20100,
      utilization: 37,
    },
    {
      id: "data",
      role: "DATA",
      model: "HBA 9500-16i / RAID-6",
      kind: "HDD Array",
      capacityBytes: 80e12,
      hbaPort: "SAS HBA 03:00.0 · phy 0–11",
      outstandingIo: 34,
      readMbps: 889,
      writeMbps: 653,
      readIops: 3380,
      writeIops: 2910,
      utilization: 71,
    },
  ],
  series,
};

export const previewSnapshot: ApplianceSnapshot = {
  telemetry: previewTelemetry,
  targets: [
    {
      stableId: "wwn-meta",
      path: "/dev/nvme0n1",
      kernelName: "nvme0n1",
      model: "Micron 7450 MAX",
      serial: "MTFDKCC3T2TFR",
      wwn: "eui.0001",
      targetType: "NVMe SSD",
      capacityBytes: 3.2e12,
      hbaPort: "PCIe 0000:41:00.0",
      eligible: true,
      backingDisks: [],
      inventoryRevision: "preview-r1",
    },
    {
      stableId: "wwn-data",
      path: "/dev/mapper/data-array",
      kernelName: "dm-4",
      model: "Broadcom 9560 RAID-6",
      serial: "RAID6-A01",
      wwn: "naa.6000",
      targetType: "RAID",
      capacityBytes: 80e12,
      hbaPort: "SAS HBA 03:00.0 · phy 0–11",
      eligible: true,
      backingDisks: Array.from({ length: 12 }, (_, i) => ({
        stableId: `sas-${i}`,
        model: "Exos X20",
        serial: `ZVT${1000 + i}`,
        hbaPort: `phy ${i}`,
      })),
      inventoryRevision: "preview-r1",
    },
    {
      stableId: "root",
      path: "/dev/sda",
      kernelName: "sda",
      model: "Intel S4510",
      serial: "ROOT01",
      wwn: "naa.root",
      targetType: "SATA SSD",
      capacityBytes: 960e9,
      hbaPort: "SATA 00:17.0 · port 0",
      filesystem: "xfs",
      eligible: false,
      eligibilityReason: "Root-/Boot-Gerät",
      backingDisks: [],
      inventoryRevision: "preview-r1",
    },
  ],
  settings: {
    revision: 4,
    autoMount: true,
    advancedReduction: "prefix_v1",
    onlineGcEnabled: true,
    maintenanceWindowUtc: "Sonntag 02:00–05:00",
    pressureLowBasisPoints: 8500,
    pressureHighBasisPoints: 9000,
  },
  shares: [
    {
      id: "prod",
      revision: 2,
      name: "production",
      description: "Primäre FastDup-Datenablage",
      enabled: true,
      hidden: false,
      readOnly: false,
      guestAccess: false,
      encryption: "desired",
      accessBasedEnumeration: true,
      allowedUsers: ["backup"],
      allowedGroups: ["storage-admins"],
      logicalQuota: { value: 80, unit: "tb" },
    },
    {
      id: "archive",
      revision: 1,
      name: "archive",
      description: "Schreibgeschütztes Archiv",
      enabled: true,
      hidden: true,
      readOnly: true,
      guestAccess: false,
      encryption: "required",
      accessBasedEnumeration: true,
      allowedUsers: [],
      allowedGroups: ["auditors"],
      logicalQuota: { value: 2, unit: "pb" },
    },
  ],
  jobs: [
    {
      id: "j1",
      kind: "offline_scrub",
      state: "succeeded",
      progressBasisPoints: 10000,
      message: "Scrub ohne Befund abgeschlossen",
      createdAt: Date.now() / 1000 - 518400,
      updatedAt: Date.now() / 1000 - 518100,
    },
  ],
  certificateFingerprint:
    "72:9F:2E:16:9B:78:01:AA:5E:34:8C:1D:CC:42:0A:19:48:41:77:8E:90:31:6A:D4:06:D9:CC:35:72:B0:9F:11",
};
