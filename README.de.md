# fastdup

<p align="center">
  <strong>Deduplizierender POSIX-Speicher: als RPM installieren und bequem im Browser verwalten.</strong>
</p>

<p align="center">
  <a href="README.md">English</a> · Deutsch
</p>

<p align="center">
  <a href="https://github.com/ThatIsCraZy/fastdup/releases/latest"><img alt="Aktuelle Version" src="https://img.shields.io/github/v/release/ThatIsCraZy/fastdup"></a>
  <a href="LICENSE"><img alt="Apache-2.0-Lizenz" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <img alt="Rocky Linux 10 x86-64" src="https://img.shields.io/badge/platform-Rocky%20Linux%2010%20x86--64-lightgrey">
</p>

<p align="center">
  <strong><a href="https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm">RPM herunterladen</a></strong>
  · <a href="https://thatiscrazy.github.io/fastdup/">Produktseite</a>
  · <a href="https://github.com/ThatIsCraZy/fastdup/releases/tag/v0.5">Release-Informationen</a>
</p>

fastdup ist eine experimentelle Single-Node-Storage-Appliance für Linux. Sie
stellt normale Dateien und Verzeichnisse über FUSE und SMB/Samba bereit und
reduziert den physischen Speicherbedarf durch Exact Dedup und Zstd-Kompression.
Administratoren bedienen die Appliance über eine eingebettete HTTPS-WebUI—ein
separater Management-Stack ist nicht nötig.

> [!WARNING]
> fastdup ist ein Forschungsprototyp und kein produktionsreifes Backup-Produkt.
> Verwende es nicht als einzige Kopie wichtiger Daten. Die aktuellen Grenzen
> sind weiter unten aufgeführt.

## Auf Rocky Linux 10 installieren

Benötigt werden:

- Rocky Linux 10 auf x86-64
- Root-Rechte für die RPM-Installation
- zwei leere, physisch getrennte Blockgeräte: eines für Metadata und eines für DATA
- TCP 8080 aus dem Management-Netz erreichbar

Aktuelles Binärpaket herunterladen und installieren:

```bash
curl -LO https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm
curl -LO https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS

sudo dnf install ./fastdup-0.5.0-1.el10.x86_64.rpm
sudo systemctl enable --now fastdup-agent.service fastdup-control.service
```

Danach im Browser öffnen:

```text
https://<appliance-host>:8080/
```

Das erste Zertifikat ist selbstsigniert. Mit `admin` / `fastdup01.` anmelden und
das Startpasswort sofort ersetzen. Die WebUI verlangt mindestens zwölf Zeichen,
bevor sie eine administrative Änderung annimmt.

Das Paket startet absichtlich nur die Management-Dienste. Es formatiert **keine**
Datenträger und mountet nicht automatisch ein Repository.

> [!CAUTION]
> Die Repository-Provisionierung löscht Partitionstabellen und sämtliche Daten
> auf beiden ausgewählten Geräten. Vor dem Fortfahren Modell, Seriennummer, WWN,
> Kapazität und HBA-Pfad in der WebUI prüfen.

## Einfache Verwaltung über die WebUI

Die WebUI ist im RPM enthalten und wird von der lokalen Control Plane
ausgeliefert. Über eine einzige Browseroberfläche kann ein Administrator:

- Metadata- und DATA-Geräte mit Schutz vor gefährlichen Auswahlen provisionieren
- das Repository mounten, sauber aushängen, recovern und offline scrubben
- SMB-Freigaben, Zugriffsregeln, Verschlüsselungsvorgaben und Quotas verwalten
- Durchsatz, Kapazität, Deduplizierung, CPU/RAM, Disk-I/O und Checkpoints überwachen
- Online-GC, Druckschwellen, Auto-Mount, Wartungsfenster, Small-File-Platzierung
  und optionale Advanced Reduction konfigurieren
- Jobs und Alarme prüfen, den Audit-Verlauf exportieren, TLS erneuern und
  Passwörter ändern

![fastdup-WebUI-Übersicht mit Beispieldaten](docs/assets/webui-overview.png)

<p align="center"><em>WebUI-Vorschau mit Beispieldaten: Repository-Zustand, Durchsatz, Datenreduktion, Kapazität und Disk-I/O.</em></p>

| Sichere Geräteauswahl und Provisionierung | SMB-Freigaben und logische Quotas |
| --- | --- |
| ![Laufwerksprovisionierung in der fastdup-WebUI](docs/assets/webui-drives.png) | ![Verwaltung von SMB-Freigaben in der fastdup-WebUI](docs/assets/webui-shares.png) |

<p align="center"><em>Die Screenshots stammen automatisiert aus der echten React-WebUI und verwenden deren mitgelieferte Preview-Daten.</em></p>

## Ersteinrichtung

1. **RPM installieren** und `fastdup-agent` sowie `fastdup-control` wie oben starten.
2. **WebUI öffnen**, das erste Zertifikat akzeptieren oder lokal als
   vertrauenswürdig hinterlegen, anmelden und ein neues Admin-Passwort setzen.
3. Unter **„Laufwerke“** ein zulässiges Metadata-Gerät und ein zulässiges
   DATA-Gerät wählen. Die WebUI schließt Root-, Boot-, Swap-, gemountete,
   gehaltene und physisch überlappende Targets aus.
4. **Destruktive Bestätigung genau prüfen** und das Repository initialisieren.
   fastdup erzeugt und mountet die erforderlichen XFS-Pools mit festen Rollen.
5. Das **Repository mounten**.
6. Unter **„SMB-Freigaben“** einen Share anlegen; Benutzer/Gruppen, Read-only,
   Verschlüsselung, Access-Based Enumeration und optionale logische Quota wählen.
7. Den Betrieb über **„Übersicht“**, **„Telemetrie“** und **„Ereignisse“** überwachen.

TCP 8080 nur für das vorgesehene Management-Netz freigeben. Der SMB-Zugriff
folgt der Samba- und Firewall-Policy des Hosts.

## Tägliche Administration

| Aufgabe | Bereich der WebUI | Hinweise |
| --- | --- | --- |
| Zustand und Kapazität prüfen | **Übersicht** | Live-Durchsatz, Reduktion, Reserve und Disk-I/O |
| Mounten oder sauber aushängen | **Repository** | Unmount stoppt neue Mutationen und checkpointet zuerst |
| Integrität prüfen | **Repository → Offline-Scrub** | Erfordert ein offline geschaltetes Repository |
| Physische Targets verwalten | **Laufwerke** | Stabile Geräteidentitäten statt frei eingegebener Pfade |
| SMB-Shares anlegen/begrenzen | **SMB-Freigaben** | Benutzer/Gruppen, Read-only, Encryption, ABE und logische Quota |
| Historische Metriken ansehen | **Telemetrie** | Durchsatz, Ressourcen, Disk-I/O und Datenreduktion |
| Jobs und Alarme prüfen | **Ereignisse** | Fortschritt, Fehler, Alerts und CSV-Audit-Export |
| Runtime-Policy ändern | **Einstellungen** | GC, Druck, Platzierung, Wartung, TLS und Passwort |

systemd-Slices trennen den Repository-Prozess von der Management Plane. Ein
Neustart der WebUI oder ihres Agents stoppt das gemountete Repository nicht.
Der Repository-Runtime läuft ohne Swap und führt beim Shutdown über `SIGINT`
zuerst einen Checkpoint aus.

## Was fastdup speichert

Anwendungen sehen ein veränderbares POSIX-Dateisystem. Darunter speichert
fastdup unveränderliche, inhaltsidentifizierte Container und Manifeste:

```text
POSIX-/FUSE-/SMB-Write
          │
          ▼
 Live-Namensraum ──► SeqCDC-v1 ──► BLAKE3-256 ──► Exact Lookup
                                                        │
                              vorhandener geprüfter Chunk ◄─┤
                                                        │ neu
                                                        ▼
                                                  RAW oder Zstd
                                                        │
                                                        ▼
                                  Container ──► Manifest ──► Commit-WAL
```

Die dauerhaften Formate bleiben maßgeblich. Exact-/Similarity-Indizes,
Bloom-Filter und Read Caches sind nur Beschleunigung und können neu aufgebaut
werden. Recovery, Offline-Scrub und Writer prüfen dieselben versionierten
Invarianten.

Der aktuelle Dateisystemumfang enthält Random Writes, Sparse Files, Hardlinks,
Symlinks, xattrs/ACLs, Record Locks, reine Metadatenklone über
`copy_file_range`, Crash-Recovery, DATA-Tier-Recovery-Checkpoints, adaptive GC,
logische Quotas pro Share und policygesteuerte Small-File-Platzierung.

## Wartung über die Kommandozeile

Routinearbeiten gehören in die WebUI. Für Recovery oder skriptgesteuerte
Offline-Wartung Repository vorher beenden und aushängen:

```bash
fastdup-maintenance --offline scrub METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline metadata-gc METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline rebuild-exact METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline rebuild-pool-indexes METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline scrub-gc METADATA_ROOT DATA_ROOT
```

Der [Wartungsleitfaden](docs/operations/scrub-and-exact-index-rebuild.md)
beschreibt Recovery, Scrub, Index-Rebuild und GC im Detail.

Software entfernen und Repository-Daten absichtlich behalten:

```bash
sudo dnf remove fastdup
```

## Aktuelle Grenzen

- nur Linux x86-64; das veröffentlichte RPM zielt auf Rocky Linux 10
- benötigt ehrlichen Stable Storage und zwei physisch getrennte XFS-Tiers
- keine eingebaute Device Redundancy, Replikation, WORM, Encryption-at-Rest-
  Policy, Cloud Tier oder Schutz vor Geräteverlust
- POSIX-Umfang und breite Samba-/Client-Konformität noch unvollständig
- Samba Fast Clone bleibt experimentell und ist noch nicht für Veeam qualifiziert
- Advanced Similarity Reduction bleibt bis zu breiterer Workload-Evidenz opt-in
- kein Produktions-Support, Performance-SLA oder Kapazitätsversprechen

Kommerzielle Backup-Appliances wie Dell PowerProtect Data Domain und HPE
StoreOnce bieten ausgereifte Integrationen, Retention, Cyber Resilience und
Support, die fastdup nicht besitzt. fastdup ist eine offene Implementierung für
Evaluation und Storage-Forschung, kein Drop-in-Ersatz.

## Für Entwickler

```bash
cd /source/fastdup

export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$TMPDIR"
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p fastdup-appliance
```

Cargo legt `CARGO_TARGET_DIR` selbst an; dieses Verzeichnis nicht vorher
erstellen.

- [Verbindliche Domain-Sprache](CONTEXT.md)
- [Architecture Decision Records](docs/adr/)
- [Spezifikationen dauerhafter Formate](docs/specs/)
- [Testpläne](docs/testing/)
- [Betriebsanleitungen](docs/operations/)
- [Reproduzierbare Benchmarks](docs/benchmarks/)
- [Architektur der Control Plane](docs/operations/control-plane.md)
- [Status und Grenzen des Samba-VFS-Moduls](samba/vfs_fastdup/README.md)

## Lizenz

Der Rust-Workspace und die Projektdokumentation stehen unter der
[Apache License 2.0](LICENSE). Das In-Process-Samba-Modul unter
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) steht separat unter
GPL-3.0-or-later, wie für ein Samba-VFS-Modul erforderlich.
