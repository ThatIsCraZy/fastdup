import { useEffect, useState } from "react";
import { Button } from "./components/ui/button";
import { Card, CardContent, CardHeader } from "./components/ui/card";
import { useI18n } from "./i18n";
import type { ApplianceSnapshot, VeeamSettings } from "./types";

const defaults: VeeamSettings = {
  revision: 0, interface: "eth0", address: "192.0.2.50", prefix: 24,
  gateway: "192.0.2.1", dns: "192.0.2.53", sshPublicKey: "", sshEnabled: true,
  hardenedImmutability: false, advancedReduction: "off", logicalQuota: null,
};

export function VeeamPage({ snapshot, busy, submit }: {
  snapshot: ApplianceSnapshot; busy: boolean; submit: (command: unknown) => Promise<void>;
}) {
  const { t } = useI18n();
  const [settings, setSettings] = useState<VeeamSettings>(snapshot.veeam ?? defaults);
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [pending, setPending] = useState(false);
  useEffect(() => {
    if (snapshot.veeam) {
      setSettings(snapshot.veeam);
      if (!snapshot.veeam.sshEnabled) setPassword("");
    }
  }, [snapshot.veeam?.revision]);
  const online = snapshot.telemetry.repositoryState === "online";
  const blocked = busy || pending;
  const update = (patch: Partial<VeeamSettings>) => setSettings(current => ({ ...current, ...patch }));
  const send = async (command: unknown) => {
    setPending(true);
    try { await submit(command); } finally { setPending(false); }
  };
  return <>
    <div className="page-title"><div><span className="section-kicker">Veeam</span>
      <h1>{t("Veeam-Servicecontainer")}</h1>
      <p>{t("Eigenes Linux-Repository mit persistentem Dienstzustand und eigener IP-Adresse.")}</p>
    </div></div>
    <Card><CardHeader><h2>{t("Repository-Anbindung")}</h2>
      <span>{t(snapshot.veeamActive ? "Aktiv" : "Gestoppt")}</span></CardHeader><CardContent>
      <p>{t("Container-Pfad")}: <code>/repository</code> · {t("Reservierter Ordner")}: <code>veeam</code></p>
      <p>{t("Der Name veeam ist für SMB-Freigaben gesperrt. Quota, Advanced Reduction und Immutability gelten ausschließlich für diesen Ordner.")}</p>
      <p role="note">{t("Zertifikats-Erstkontakt ist für diesen Linux-Container in Veeam 13 nicht verfügbar; Veeam verlangt dafür seine Infrastructure Appliance.")}</p>
      <p role="note">{t(settings.hardenedImmutability ? "Hardened Repository: Der Veeam-Immutability-Dienst darf Schutzflags nur in diesem Repository setzen oder löschen." : "Linux-Repository mit Fast Clone; Hardened Immutability ist deaktiviert.")}</p>
      {!online && <p role="status">{t("Zum Provisionieren oder Starten muss das Repository online sein.")}</p>}
      <div className="form-actions">
        <Button disabled={blocked || !online || !snapshot.veeam || snapshot.veeamActive} onClick={() => void send({ kind: "start_veeam" })}>{t("Starten")}</Button>
        <Button variant="secondary" disabled={blocked || !snapshot.veeamActive} onClick={() => void send({ kind: "stop_veeam" })}>{t("Stoppen")}</Button>
      </div>
    </CardContent></Card>
    <Card><CardHeader><h2>{t("Container provisionieren")}</h2></CardHeader><CardContent>
      <form className="veeam-form" onSubmit={event => {
        event.preventDefault();
        void send({ kind: "configure_veeam", settings: { ...settings, revision: snapshot.veeam?.revision ?? 0 }, ...(settings.sshEnabled && password ? { bootstrap_password: password } : {}) });
      }}>
        <fieldset disabled={blocked || !online}>
          <div className="form-grid">
            <label>{t("Host-Netzwerkschnittstelle")}<input required maxLength={12} value={settings.interface} onChange={e => update({ interface: e.target.value })} /></label>
            <label>{t("IPv4-Adresse")}<input required value={settings.address} onChange={e => update({ address: e.target.value })} /></label>
            <label>{t("Netzpräfix")}<input required type="number" min={1} max={30} value={settings.prefix} onChange={e => update({ prefix: Number(e.target.value) })} /></label>
            <label>Gateway<input required value={settings.gateway} onChange={e => update({ gateway: e.target.value })} /></label>
            <label>DNS<input required value={settings.dns} onChange={e => update({ dns: e.target.value })} /></label>
          </div>
          <label>{t("SSH-Public-Key für die Veeam-Installation")}<textarea rows={3} maxLength={8192} value={settings.sshPublicKey} onChange={e => update({ sshPublicKey: e.target.value })} placeholder="ssh-ed25519 …" /></label>
          <label className="checkbox-row"><input type="checkbox" checked={settings.sshEnabled} onChange={e => update({ sshEnabled: e.target.checked })} />{t("SSH-Installationszugang aktiv")}</label>
          <p>{t("Der SSH-Schlüssel ist optional für die Administration. Nach der Veeam-Installation SSH hier deaktivieren.")}</p>
          {settings.sshEnabled && <>
            <label>{t("Temporäres Installationspasswort")}<input type={showPassword ? "text" : "password"} autoComplete="new-password" minLength={16} maxLength={128} value={password} onChange={e => setPassword(e.target.value)} /></label>
            <div className="form-actions"><Button type="button" variant="secondary" onClick={() => {
              const bytes = crypto.getRandomValues(new Uint8Array(24));
              setPassword(Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("")); setShowPassword(true);
            }}>{t("Passwort erzeugen")}</Button>
            <Button type="button" variant="secondary" onClick={() => setShowPassword(value => !value)}>{t(showPassword ? "Verbergen" : "Anzeigen")}</Button></div>
            <p>{t("Single-use-Anmeldung in Veeam: Benutzer veeam, temporäres Passwort und sudo. Das Passwort wird nicht in der FastDup-Konfiguration gespeichert.")}</p>
          </>}
          <label className="checkbox-row"><input type="checkbox" checked={settings.hardenedImmutability} onChange={e => update({ hardenedImmutability: e.target.checked })} />{t("Hardened Immutability")}</label>
          <p>{t("Erlaubt ausschließlich dem isolierten Veeam-Immutability-Dienst, Backupdateien in diesem Repository unveränderlich zu schützen.")}</p>
          <label className="checkbox-row"><input type="checkbox" checked={settings.advancedReduction === "dependent_v1"} onChange={e => update({ advancedReduction: e.target.checked ? "dependent_v1" : "off" })} />Advanced Reduction</label>
          <label className="checkbox-row"><input type="checkbox" checked={!!settings.logicalQuota} onChange={e => update({ logicalQuota: e.target.checked ? { value: 1, unit: "tb" } : null })} />{t("Logische Quota setzen")}</label>
          {settings.logicalQuota && <div className="form-grid">
            <label>{t("Quota")}<input required type="number" min={1} max={999} value={settings.logicalQuota.value} onChange={e => update({ logicalQuota: { value: Number(e.target.value), unit: settings.logicalQuota!.unit } })} /></label>
            <label>{t("Einheit")}<select value={settings.logicalQuota.unit} onChange={e => update({ logicalQuota: { value: settings.logicalQuota!.value, unit: e.target.value as "gb" | "tb" | "pb" } })}><option value="gb">GB</option><option value="tb">TB</option><option value="pb">PB</option></select></label>
          </div>}
          <p>{t("Änderungen starten den Container neu. Backupdaten und installierte Veeam-Dienste bleiben erhalten.")}</p>
          <Button type="submit">{t(snapshot.veeam ? "Speichern und starten" : "Provisionieren")}</Button>
        </fieldset>
      </form>
    </CardContent></Card>
  </>;
}
