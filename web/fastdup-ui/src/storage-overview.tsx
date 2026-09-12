import { HardDrive, Database, FileStack } from "lucide-react";
import { useI18n } from "./i18n";
import { Card, CardContent, CardHeader } from "./components/ui/card";
import type { ApplianceSnapshot } from "./types";

export function StorageOverview({snapshot}: {snapshot: ApplianceSnapshot}) {
 const {t, locale} = useI18n();
 const repository = snapshot.repository!;
 const usage = snapshot.telemetry.storageUsage;
 const bytes = (value?: number | null) => {
  if (value == null) return "—";
  const units = ["B","KB","MB","GB","TB","PB"];
  let index = 0; while (value >= 1000 && index < units.length-1) { value /= 1000; index++; }
  return `${value.toLocaleString(locale,{maximumFractionDigits:2})} ${units[index]}`;
 };
 const logical = usage?.logicalAllocatedBytes;
 const physical = usage?.metadataUsedBytes != null && usage?.dataUsedBytes != null ? usage.metadataUsedBytes + usage.dataUsedBytes : undefined;
 const scale = Math.max(logical ?? 0, physical ?? 0, 1);
 const volumes = [
  {role:"Metadata", target:repository.metadataTarget, kernel:repository.metadataKernelName, uuid:repository.metadataUuid, used:usage?.metadataUsedBytes, capacity:usage?.metadataCapacityBytes, description:"Metadaten, Indizes und Small Files"},
  {role:"DATA", target:repository.dataTarget, kernel:repository.dataKernelName, uuid:repository.dataUuid, used:usage?.dataUsedBytes, capacity:usage?.dataCapacityBytes, description:"Container mit deduplizierten und komprimierten Nutzdaten"},
 ];
 return <>
  <div className="page-title"><div><span className="section-kicker">Storage</span><h1>{t("Laufwerke & Belegung")}</h1><p>{t("Logische Dateibelegung und tatsächlicher Platzbedarf auf den Repository-Laufwerken.")}</p></div></div>
  <Card><CardHeader><h2>{t("Logisch vs. physisch")}</h2></CardHeader><CardContent>
   <div className="storage-comparison">{([{label:"Logische Belegung",amount:logical,Icon:FileStack},{label:"Physische Belegung",amount:physical,Icon:Database}]).map(({label, amount, Icon}) => {
    return <div className="storage-measure" key={String(label)}><span><Icon size={20}/>{t(String(label))}</span><strong>{bytes(amount)}</strong><div className="storage-bar"><i style={{width:`${(amount ?? 0) / scale * 100}%`}}/></div></div>;
   })}</div>
   <p className="detail-note">{t("Logisch: belegte Dateibereiche vor Datenreduktion, ohne Sparse-Löcher. Physisch: Metadata und DATA zusammen, einschließlich Indizes, Dateisystem-Overhead und noch nicht bereinigter Daten.")}</p>
   {usage?.logicalObservedAt != null ? <p className="detail-note">{t("Logische Belegung erfasst")}: {new Date(usage.logicalObservedAt*1000).toLocaleString(locale)}</p> : <p className="detail-note">{t("Die logische Belegung wird von der laufenden Runtime ermittelt.")}</p>}
  </CardContent></Card>
  <div className="storage-volumes">{volumes.map(volume => {
   const target = snapshot.targets.find(target => target.stableId === volume.target);
   const devices = target?.backingDisks.length ? target.backingDisks : [{kernelName:target?.kernelName ?? volume.kernel,model:target?.model ?? "",serial:target?.serial ?? "",hbaPort:target?.hbaPort ?? ""}];
   return <Card key={volume.role}><CardHeader><div><h2>{volume.role}</h2><p>{t(volume.description)}</p></div></CardHeader><CardContent>
    <div className="storage-volume-usage"><strong>{bytes(volume.used)}</strong><span>{t("belegt von")} {bytes(volume.capacity)}</span></div>
    {volume.capacity != null && volume.used != null && <progress aria-label={`${volume.role} ${t("Belegung")}`} value={volume.used} max={Math.max(1,volume.capacity)}/>}
    <p className="detail-note">{target?.path ?? volume.kernel} · UUID {volume.uuid}</p>
    <div className="storage-device-list">{devices.map(device => {
     const disk = snapshot.telemetry.disks.find(disk => disk.id === device.kernelName);
     return <div className="storage-device" key={device.kernelName}><HardDrive size={22}/><div><strong>{device.kernelName} · {device.model || disk?.model || t("Modell nicht verfügbar")}</strong><small>{device.hbaPort || disk?.hbaPort || t("Hardwarepfad nicht verfügbar")}</small><small>{t("Lesen / Schreiben")}: {disk ? `${disk.readMbps.toLocaleString(locale,{maximumFractionDigits:1})} / ${disk.writeMbps.toLocaleString(locale,{maximumFractionDigits:1})} MB/s` : "—"}</small><small>{t("Lesen / Schreiben")}: {disk?.readIops == null ? "—" : disk.readIops.toLocaleString(locale,{maximumFractionDigits:1})} / {disk?.writeIops == null ? "—" : disk.writeIops.toLocaleString(locale,{maximumFractionDigits:1})} IOPS</small><small>{t("Auslastung")}: {disk ? `${disk.utilization.toLocaleString(locale,{maximumFractionDigits:1})} % · ${disk.outstandingIo} ${t("ausstehende I/Os")}` : "—"}</small></div></div>;
    })}</div>
   </CardContent></Card>;
  })}</div>
 </>;
}
