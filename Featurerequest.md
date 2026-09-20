# Feature Request: Fastdup als integrierte Veeam-Backup-Appliance

**Status:** Konzept / Request for Comments — keine nachgewiesene Veeam-Kompatibilität, keine Herstellerfreigabe.

**Produktidee:** Fastdup verbindet Deduplizierung, synthetische Vollbackups per Fast Clone
und unveränderbare Backups in einer softwaredefinierten Appliance. Die originalen
Veeam-Repository-Dienste (Installer/Deployment Service, Transport/Data Mover,
Immutability Service — gemeinsam installiert und aktualisiert) laufen in einem lokalen
Linux-Systemcontainer. Eine XFS-kompatible Schnittstelle übersetzt deren Dateisystemzugriffe
auf Fastdup: Fast Clone nutzt Metadatenreferenzen, Schreib- und Löschsperren werden im
Storage-Kern erzwungen. Ein separater Repository-Server und der Umweg über SMB entfallen.

Vorbild für den Containeransatz: Open-E dokumentiert ein Veeam Hardened Repository im
LXC-Container auf einer Storage-Appliance mit echtem XFS auf ZFS-ZVOLs. Der Containeransatz
ist damit nicht bloß theoretisch; die Fastdup-Dateisystememulation bleibt eine zusätzliche,
unbewiesene Integrationsschicht.

## Ausgangslage im aktuellen Code (v0.8.0, Format-Epoch 3)

Heute ist Fastdup eine host-installierte RPM-Appliance, deren einziger
Repository-Frontend-Pfad SMB ist: Samba auf dem Host exportiert Freigaben auf einem
FUSE-Mount. Veeam-Kompatibilität existiert ausschließlich über SMB
(`FSCTL_DUPLICATE_EXTENTS_TO_FILE` → `vfs_fastdup` → `copy_file_range`), bestätigt in
`docs/testing/veeam-backup-2026-09-19.md` (5 erfolgreiche B&R-Sessions). Das README
(`README.de.md`, Abschnitt „in Arbeit“) kündigt die Integration des Veeam Repository
Agent an, damit Fastdup als Linux-Repository statt nur als SMB-Freigabe dient.

Vorhandene Bausteine, auf denen das Konzept aufsetzt:

| Baustein | Fundstelle | Nutzen für das Konzept |
|---|---|---|
| POSIX-Seam, ein `Namespace::dispatch` für Modell und FUSE | `crates/fastdup-posix/src/lib.rs`, ADR 0033 | einzige Eintrittsstelle für eine XFS-kompatible Übersetzung |
| Range-Clone (`copy_file_range` → `Operation::CloneRange`), Metadaten-only, kein Buffered-Fallback | `crates/fastdup-posix/src/fuse_adapter.rs:1660`, `lib.rs:5443`, ADR 0043 | Fast-Clone-Engine, bereits ohne Datenkopie |
| `FS_IOC_GET/SETFLAGS` + `FS_IOC_FSGET/FSSETXATTR` (nur `FS_IMMUTABLE_FL`) | `crates/fastdup-posix/src/fuse_adapter.rs:1116-1223`, `crates/fastdup-format/src/namespace.rs:26` | `chattr +i`-Kompatibilität für Veeams Immutable-Flags |
| Immutability-Erzwinger für Inhalt, Namen, Metadaten, Clone-Ziele, Rename | `crates/fastdup-posix/src/lib.rs` (Write ~4736, Open ~4925, Truncate ~4563, Unlink/Rmdir ~5222/5303, Clone ~5516, Rename ~6027) | „Löschsperren im Storage-Kern“ existieren bereits |
| POSIX-Records-Locks (`getlk`/`setlk`) | `fuse_adapter.rs`, ADR 0027 | Sperrbasis; `flock` fehlt noch |
| Appliance-Lease, Recovery-Latch, Pool-Bindung/Isolation | `crates/fastdup-appliance/src/appliance_lease.rs`, `appliance_recovery_latch.rs`, `pool_binding.rs` (ADRs 0069/0070/0080/0081) | Eindeutiger Mount-Besitzer, fail-closed Start |
| Control Plane (axum :8080) + Root-Agent über `/run/fastdup/agent.sock` | `crates/fastdup-control/src/bin/`, ADR 0086 | Betreiber-Schicht, getrennt von Storage-Autorität |
| Fault-Injection, SIGKILL-Remount-Harness, Veeam-Qualifikationsdoku | `crates/fastdup-testkit/`, `docs/testing/` | Qualifizierungsrahmen für beide neuen Schichten |

## Notwendige Umsetzung

### 1. Vollständige Linux-Laufzeit im Systemcontainer

Heute: `packaging/rpm/fastdup.spec` installiert auf den Host; `docs/operations/control-plane.md`
setzt den gemeinsamen Mount-Namespace von Agent und Repository-Daemon ausdrücklich voraus.
Es existiert kein LXC-/OCI-Artefakt („Container“ meint im Repo ausschließlich das
Storage-Container-Format).

Notwendig:

- LXC-Systemcontainer als Produktbaustein: ausgewählte Distribution (el10 wie das RPM),
  Bash, Paketverwaltung, systemd im Container mit sauberer cgroup-Delegation.
- Neue Packaging-Zweige: LXC-Config/Profile im Repo, Container-Image-Build parallel zu
  `packaging/build-rpm.sh`; systemd-Units (`packaging/systemd/`) bleiben auf dem Host für
  Fastdup selbst, bekommen aber eine Container-Verwaltungseinheit (Start/Stop,
  Mount-Abhängigkeit).
- Entkopplung des Repository-Mounts vom Host-SMB-Startpfad:
  `crates/fastdup-appliance/src/share_backend.rs` startet/stoppt heute host-`smb.service`
  um den Mount herum (`packaging/systemd/smb.service.d/20-fastdup.conf`). Für das neue
  Konzept wird ein alternativer Backend-Pfad benötigt, der den Mount in den Container
  hineinreicht statt Samba zu konfigurieren.

### 2. Installation und Updates

Heute: keine SSH-Komponente; Provisionierung läuft über die Control Plane
(`crates/fastdup-control/src/control.rs` mit `lsblk`/`mkfs.xfs`/Unit-Start).

Notwendig:

- Der Container muss als „Managed Linux Server“ erreichbar sein: Erstinstallation durch
  Veeam über Single-use-SSH-Zugang mit temporärem sudo, danach SSH schließen.
  Das erfordert einen provisionierten SSH-Zustand im Container-Image plus einen
  Host-seitigen Verwaltungsvermerk (Control Plane, `crates/fastdup-control/src/store.rs`),
  wann der Zugang zu schließen ist.
- Komponentenupdates laufen gemäß Veeam-Dokumentation über den Installer Service ohne SSH:
  Paketinstallation und Dienstneustarts müssen im laufenden Container zuverlässig möglich
  bleiben. Das schließt ein, dass Fastdup-Mount und Repository-Betrieb einen
  Veeam-Dienstneustart überleben (Supervisor-Verhalten des Daemons prüfen:
  `crates/fastdup-appliance/src/bin/fastdup-durable-fuse.rs`).

### 3. Rechte und Sicherheitsgrenzen

Heute: Transport läuft implizit privilegiert, wo er den Host-Mount bedient; Immutability
setzen/löschen verlangt uid 0 (`crates/fastdup-posix/src/lib.rs:4377`), und jeder
Host-Root-Prozess erreicht den `allow_other`-Mount
(`fuse_adapter.rs:546 volatile_mount_options`) und kann das Flag löschen. Samba zwingt
uid `fastdup-smb` und kann das Flag gar nicht setzen. Es gibt **kein**
Autorisierungsmodell und keine Aufbewahrungsfrist.

Notwendig:

- UID/GID-Mapping zwischen Container und Fastdup-Autorität definieren: Container-Root ist
  nicht der Root-Kontext, den Fastdup heute für `SETFLAGS` akzeptiert. Der
  Request-Context in `fastdup-posix` (`RequestContext`) muss um eine mandatsbasierte
  Autorisierung erweitert werden, mit der nur der autorisierte Immutability-/Retention-
  Dienst (und die Appliance-Verwaltung) das `FS_IMMUTABLE_FL` ändern darf — nicht pauschal
  „wer auch immer uid 0 im Container ist“.
- Capabilities des Containers minimal halten (Transport unprivilegiert, Immutability-Dienst
  mit Bedarf-Rechten); ein pauschal privilegierter Container ist keine Produktlösung.
- Veeam darf seine Repository-Umgebung verwalten, ohne Kontrolle über den Appliance-Host:
  ADR 0086 (Control Plane nie Storage-Autorität) bleibt Leitlinie; die neue Autorisierung
  gehört in den Storage-Kern, nicht in die Control Plane.

### 4. Dauerhafter Systemzustand

Heute: persistenter Zustand ist auf Fastdup-Pools und `/var/lib/fastdup/` (Control-Plane-SQLite)
gemappt; für einen Veeam-Container existiert nichts.

Notwendig:

- Persistentes State-Verzeichnis für den Container (Zertifikate, Konfiguration,
  Schutzinformationen), das Container-Neustarts und Updates überlebt — analog zu
  `packaging/tmpfiles.d/`, aber als deklarierte, updatefeste Ablage.
- Besonders kritisch: Veeam verwaltet `/etc/veeam/immureposvc` eigene Dateien für
  Zeitsprung-Erkennung und Schutzstatus, teilweise mit Immutable-Attributen. Dieser Zustand
  muss (a) persistent liegen, (b) über denselben Autorisierungsmechanismus wie das Backup-
  Verzeichnis geschützt sein, und (c) darf ein Container-Rebuild nicht löschen.
- Zeitsprung-Erkennung von Veeam und Fastdups Zeitbasis (Commit-Ketten-Fencing, ADR 0071)
  müssen konsistent bleiben; das ist zu spezifizieren, nicht anzunehmen.

### 5. Netzwerk und Storage-Anbindung

Heute: Firewall-Öffnung nur für SMB (`crates/fastdup-control/src/firewall.rs`); Mount am
Host unter `/srv/fastdup/repository` mit `ConditionPathIsMountPoint`
(`packaging/systemd/fastdup-repository.service`).

Notwendig:

- Stabil erreichbarer Container-Endpunkt und die Veeam-Ports
  (Installer/Deployment-Service, Transport, Immutability) als deklarierte, additive
  Firewall-Profile statt SMB-only.
- Fastdup wird als Repository-Mount in den Container eingebunden (bind mount /
  mount-propagation aus dem Host-Mount-Namespace des Daemons), nicht als flüchtiger
  Containerinhalt; Repository-Eigentümer und Rechte müssen über das UID/GID-Mapping (§3)
  stimmen.
- Fail-closed statt Container-Fallback: fehlt der Fastdup-Mount, muss der Betrieb blockiert
  werden. Heute leisten das systemd-Bedingungen auf dem Host; im Container braucht es
  dieselbe Bedingung auf den bind-gemounteten Pfad (z. B. Unit-Bedingung plus Startblocker
  im Repository-Dienst), damit Veeam niemals ins Container-Root-Dateisystem schreibt.

### 6. XFS-Kompatibilität: echte Funktionen statt Kennung

Der Kern des Konzepts und der größte unbewiesene Teil:

- **Reflink-Übersetzung:** `copy_file_range` funktioniert bereits metadaten-only
  (ADR 0043), aber **`FICLONE`/`FICLONERANGE` liefert der Mount als `ENOTTY`**
  (`fuse_adapter.rs:1105` gibt für alles außer den vier Flag-Ioctls `ENOTTY` zurück).
  `docs/research/veeam-smb-fast-clone.md` belegt: Der Kernel konsumiert `FICLONERANGE`
  in der generischen Ioctl-Schicht; FUSE hat kein `remap_file_range`, der Aufruf erreicht
  den Userspace nicht. Ein Linux-Repository-Pfad ohne SMB braucht daher eine neue
  Übersetzungsschicht — Optionen (Kernel-seitiger Übersetzer, erweitertes virtiofs o. ä.)
  sind als Machbarkeitsstudie vorab zu entscheiden. Der SMB-Umweg, den dieses Konzept
  abschaffen will, ist heute der einzige bewiesene Fast-Clone-Pfad.
- **Kein stiller Datenkopier-Fallback:** die ADR-0043-Zusage (keine Rundung, kein
  Buffered-Fallback, exakt eine native Operation) muss auf jeden neuen Pfad übergehen;
  jede Unstimmigkeit (Overlaps, Policy-Mismatch, unsupported Recipes) muss weiterhin
  fail-closed fehlschlagen.
- **Immutable-Semantik:** der Bestand aus §3/§6 deckt `chattr +i` bereits ab; zusätzlich
  gehört das Verhalten bei `FS_IMMUTABLE_FL` auf *Verzeichnissen* (Schutz neuer/entfernter
  Namen im Repository) gegen XFS-Referenzverhalten qualifiziert.
- **Eigenständige Aufbewahrungsfrist (retention clock):** existiert nicht.
  `user.immutable.until` wird nur byte-exakt gespeichert (ADR 0027), kein Takt, keine
  Autorisierung. Nötig: ein Storage-Kern-Policy-Objekt, das die Frist selbst gegen
  Container-Root durchsetzt (Verzicht erst nach Fristablauf, autorisiert, protokolliert).
  Das berührt ADR 0027 und erfordert eine neue ADR.

### 7. Verbindliche Persistenz (Durability-Garantie)

Heute garantiert ADR 0003 bewusst *keine* Verstärkung durch `fsync`/SMB-`FLUSH`
(`Operation::Sync` ignoriert `data_only`, `lib.rs:4168`; wartet nur auf das Verlassen der
RAM-Publikations-Queues, `crates/fastdup-appliance/src/checkpoint/write_through.rs:3876`);
Commit-Ziel 2 s, Admission-Guard 5 s, ADR-Fenster 10 s
(`crates/fastdup-appliance/src/checkpoint_trigger.rs`).

Für dieses Einsatzmodell darf ein bestätigter Schreib- oder Schutzvorgang nicht nur in RAM
angekommen sein. Nötig:

- Ein optionaler synchroner Commit-Modus pro Share/Repository: `fsync`/`FLUSH` und das
  Setzen/Löschen von Immutable-Flags liefern erst nach durablem Commit (WAL-sync,
  `crates/fastdup-store/src/generation/commit.rs` + `generation_log.rs`) zurück.
- Das widerspricht ADR 0003 und ADR 0027 (`O_SYNC`/`O_DSYNC` teilen heute das
  Zehn-Sekunden-Fenster) → neue ADR, plus Fault-Injection-Fälle in
  `crates/fastdup-testkit` (Riss zwischen Bestätigung und Commit, SIGKILL nach
  Schutzsetzung, `sigkill_harness.rs`-Verlängerung).

## Qualifizierungsplan (zwei unbewiesene Schichten trennen)

1. **Phase A — Container mit echtem XFS:** LXC-Container, Veeam-Installer-Service
   installiert die originalen Repository-Dienste gegen ein echtes XFS-Dateisystem (Reflink
   aktiv, `chattr +i` nativ). Damit werden Containerprobleme (Installation, Updates,
   Rechte, Ports, Persistenz, Mount-Fail-closed) ohne Fastdup-Risiko geprüft.
2. **Phase B — XFS-Emulation durch Fastdup:** identischer Aufbau, Repository-Pfad auf
   Fastdup-Mount. Vergleichspunkt ist Phase A; jede Abweichung (Ioctl-Verhalten, Flags,
   Locks, `statfs`-Geometrie `crates/fastdup-appliance/src/statfs.rs`, Reflink-Semantik)
   ist explizit zu entscheiden. Die bestehenden Veeam-Qualifikationsdokumente unter
   `docs/testing/` dienen als Vorlage für eine
   `docs/testing/veeam-hardened-repository-*.md`-Reihe inkl. Synthese-Vollbackup,
   Immutability-Verletzungstests (Root aus dem Container darf nicht löschen) und
   Update-Läufe.

## Risiken und offene Punkte

- **FICLONE-Übersetzung ist der kritische Pfad:** ohne SMB ist der heute einzige bewiesene
  Fast-Clone-Weg abgeschafft; eine Kernel-nahe Lösung ist Neuentwicklung mit
  Qualifikationsrisiko.
- **Autorisierungsmodell für Immutability** berührt akzeptierte ADRs (0027) und den
  POSIX-Seam; Design vor Implementierung als ADR.
- **Synchroner Commit-Modus** widerspricht ADR 0003; Zielkonflikt Commit-Latenz vs.
  Bestätigung braucht eine klare Leistungsbilanz (Benchmark nach AGENTS-Regel).
- **Herstellerfreigabe:** Veeam dokumentiert für reguläre Linux-Repositories weiterhin
  XFS mit Reflink-Unterstützung; ein emuliertes Dateisystem ist offiziell unsupported.
  Produkt-Claim muss „technisch plausibles Konzept“ bleiben, bis Phase A/B bestanden sind.
