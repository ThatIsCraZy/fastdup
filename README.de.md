# fastdup

<p align="center">
  <strong>Absturzsicherer, deduplizierender POSIX-Speicher für Linux — entwickelt in Rust.</strong>
</p>

<p align="center">
  <a href="README.md">English</a> · Deutsch
</p>

<p align="center">
  <a href="https://github.com/ThatIsCraZy/fastdup/releases/latest"><img alt="Aktuelle Version" src="https://img.shields.io/github/v/release/ThatIsCraZy/fastdup"></a>
  <a href="LICENSE"><img alt="Apache-2.0-Lizenz" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <img alt="Linux x86-64" src="https://img.shields.io/badge/platform-Linux%20x86--64-lightgrey">
  <img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-orange">
</p>

fastdup ist ein experimentelles Single-Node-**Dateisystem und Storage-Appliance
mit Deduplizierung**. Es stellt über FUSE und optional SMB/Samba einen
veränderbaren Linux-POSIX-Namensraum bereit und speichert identische,
inhaltsabhängig gebildete Chunks nur einmal. Die Rust Storage Engine verbindet
SeqCDC, BLAKE3, Zstd, unveränderliche Container, neu aufbaubare Indizes,
Crash-Recovery, Integrity Scrubbing und adaptive Garbage Collection.

> [!WARNING]
> fastdup ist ein Forschungsprototyp und kein produktionsreifes Backup-Produkt.
> Es bietet keinen Schutz vor Geräteverlust, keine WORM-Garantie, keine
> Replikation und keinen Hersteller-Support. Verwende es zur Evaluation und
> Entwicklung, nicht als einzige Kopie wichtiger Daten.

## Warum fastdup?

Die Storage-Semantik vieler Deduplizierungssysteme bleibt hinter einer
proprietären Backup-Appliance verborgen. fastdup macht die interessanten Teile
nachvollziehbar:

- **Transparente Exact Dedup:** Identische Logical Chunks teilen physischen
  Speicher, ohne das bytegenaue Dateiverhalten zu verändern.
- **Echte POSIX-Semantik:** Random Writes, Sparse Files, Hardlinks, Symlinks,
  xattrs/ACLs, Besitz, Zeitstempel, Record Locks und offene gelöschte Inodes.
- **Absturzsichere Generationen:** Unveränderliche Manifeste und ein
  gechecksummtes Commit-WAL wählen nach einem Crash den neuesten vollständig
  dauerhaften Namensraum.
- **Wahrheit bleibt von Beschleunigung getrennt:** Manifeste und geprüfte
  Container sind maßgeblich; Exact-/Similarity-Indizes, Bloom-Filter und Read
  Caches dürfen verworfen und neu aufgebaut werden.
- **Prüfbare Haltbarkeit:** Writer, Recovery und Offline-Scrub erzwingen
  dieselben versionierten Invarianten, belegt durch ADRs, Fault Injection und
  Benchmarks.
- **Begrenzter Ressourcenverbrauch:** `io_uring`, begrenzte Ingest-Lanes,
  Cache-Steuerung und adaptive DATA-/Metadata-GC sind auf planbaren
  Single-Node-Betrieb ausgelegt.

## Funktionsweise

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

SeqCDC-v1 bildet inhaltsabhängige Chunks zwischen 16 KiB und 256 KiB.
BLAKE3-256 identifiziert ihren Inhalt. Neue benachbarte Chunks werden zu
höchstens 512 KiB großen Compression Regions zusammengefasst und unabhängig
als RAW oder Zstd Level 3 gespeichert. Gleichförmige allokierte Bereiche werden
FILL-Extents; nicht allokierte Nullbereiche bleiben HOLE-Extents und benötigen
keinen DATA-Record.

Der optionale Advanced-Reduction-Pfad ergänzt einen neu aufbaubaren Similarity
Index und `ZSTD_PREFIX`-Records mit Tiefe eins. Er muss explizit aktiviert
werden und fällt bei fehlendem oder veraltetem Beschleunigungszustand immer auf
unabhängig dekodierbares RAW/Zstd zurück.

## Aktueller Funktionsumfang

- Dateien anlegen, öffnen, lesen, schreiben, verkürzen, umbenennen und löschen
- Random Writes, Append, `O_TRUNC`, `O_EXCL`, `RENAME_NOREPLACE`, `flush` und
  `fsync`
- Sparse DATA-/FILL-/HOLE-Extents, Hole Punch, Zero Range und DATA-/HOLE-Seeks
- reine Metadatenklone über `copy_file_range`
- Unterverzeichnisse, Hardlinks, Symlinks, xattrs/ACLs, Rechte und Besitz
- Crash-Recovery auf die jüngste vollständige Commit-Generation
- unabhängige DATA-Tier-Recovery-Checkpoints bei vollständigem Metadata-Verlust
- neu aufbaubare Exact- und Similarity-Indizes
- Offline-Scrub sowie adaptive Online-/Offline-GC für DATA und Metadaten
- Kernel-Page-Cache und Readahead für Read-only-Handles mit gezielter
  Bereichsinvalidierung
- logische Quotas pro Share und policygesteuerte Small-File-Platzierung
- HTTPS-Control-Plane mit eingebetteter WebUI
- experimentelles Samba-VFS-Modul für SMB Fast Clone

Der dauerhafte Pfad unterstützt Exact Dedup, RAW/Zstd, Sparse Files,
Checkpoints, Recovery, Scrub und GC. Advanced Reduction bleibt opt-in, bis
breitere Backup-Corpora ausgewertet sind. fastdup unterstützt ausschließlich
**Linux auf x86-64**; AVX2-/BMI2-Pfade werden zur Laufzeit ausgewählt und
besitzen skalare Gegenstücke.

## Version 0.5 installieren

Das native RPM ist für **Rocky Linux 10 auf x86-64** gebaut und enthält den
FUSE-Runtime, die Maintenance-CLI, den privilegierten Appliance-Agent, die
HTTPS-Control-Plane, die eingebettete WebUI, systemd-Policies und das
Samba-VFS-Modul.

```bash
curl -LO \
  https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm
sudo dnf install ./fastdup-0.5.0-1.el10.x86_64.rpm
sudo systemctl enable --now fastdup-agent.service fastdup-control.service
```

Öffne `https://<appliance-host>:8080/`. Das erste Zertifikat ist
selbstsigniert. Die initialen Zugangsdaten lauten `admin` / `fastdup01.`; die
WebUI verlangt sofort ein neues Passwort mit mindestens zwölf Zeichen.

Der Repository-Dienst startet erst, nachdem in der WebUI zwei leere, physisch
getrennte Geräte für Metadata und DATA ausgewählt wurden.

> [!CAUTION]
> Die Provisionierung löscht die Partitionstabelle und den gesamten Inhalt
> beider ausgewählter Geräte. Prüfe die Geräteidentitäten vor der Bestätigung.

Die Release-Seite enthält zusätzlich ein Source-RPM und SHA-256-Prüfsummen:
[fastdup 0.5 Release](https://github.com/ThatIsCraZy/fastdup/releases/tag/v0.5).

## Bauen und testen

Vorausgesetzt werden Linux, eine aktuelle Rust-Toolchain und für reale
Mount-Tests `/dev/fuse`. Alle erzeugten Artefakte müssen gemäß
Repository-Policy unter `.artifacts/` bleiben:

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

Für einen reproduzierbaren Rocky-Linux-RPM-Build werden Node.js/npm,
`rpm-build`, `patchelf` und die Samba-Development-Pakete benötigt. Danach:

```bash
./packaging/build-rpm.sh
```

## Lokalen Lab-Mount starten

Der Produktivbetrieb verlangt getrennte physische XFS-Dateisysteme. Für ein
lokales Single-Disk-Lab genügen getrennte Verzeichnisse mit dem ausdrücklich
gesetzten Lab-Override:

```bash
mkdir -p \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers

FASTDUP_POOL_ISOLATION=lab-allow-shared \
  /source/fastdup/.artifacts/target/release/fastdup-durable-fuse \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers
```

Der Daemon läuft im Vordergrund. `Ctrl-C` stoppt die Annahme neuer Mutationen,
erstellt einen abschließenden Checkpoint und hängt sauber aus.

### Advanced Reduction aktivieren

Bei beendetem Daemon muss zuerst ein kohärentes Exact-/Similarity-Indexpaar
aufgebaut werden:

```bash
BIN=/source/fastdup/.artifacts/target/release
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN/fastdup-maintenance" --offline rebuild-pool-indexes "$META" "$DATA"
FASTDUP_POOL_ISOLATION=lab-allow-shared \
FASTDUP_ADVANCED_REDUCTION=prefix-v1 \
  "$BIN/fastdup-durable-fuse" \
  /source/fastdup/.artifacts/mount "$META" "$DATA"
```

Fehlt das Paar oder ist es veraltet, bleiben Writes verfügbar und fallen auf
unabhängiges RAW/Zstd zurück.

## Wartung

Vor dem verpflichtenden `--offline`-Modus Daemon beenden und Mount aushängen:

```bash
BIN=/source/fastdup/.artifacts/target/release/fastdup-maintenance
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN" --offline scrub "$META" "$DATA"
"$BIN" --offline metadata-gc "$META" "$DATA"
"$BIN" --offline rebuild-exact "$META" "$DATA"
"$BIN" --offline rebuild-pool-indexes "$META" "$DATA"
"$BIN" --offline scrub-gc "$META" "$DATA"
```

Der [Wartungsleitfaden](docs/operations/scrub-and-exact-index-rebuild.md)
beschreibt Recovery, Scrub, Index-Rebuild und GC im Detail.

## Repository-Übersicht

| Pfad | Aufgabe |
| --- | --- |
| `crates/fastdup-format` | Versionierte Container-, Manifest-, Commit- und Indexformate |
| `crates/fastdup-store` | SeqCDC, Reduktion, Container, Indizes, Scrub und GC |
| `crates/fastdup-io-uring` | Begrenzte asynchrone Linux-Container-I/O |
| `crates/fastdup-posix` | POSIX-Modell, Live-Dirty-Overlay und FUSE-Adapter |
| `crates/fastdup-appliance` | Ingest, Checkpoints, Recovery und Programme |
| `crates/fastdup-control` | HTTPS-API, WebUI, Provisionierung und Telemetrie |
| `crates/fastdup-testkit` | Deterministische Fehler, Crash-Modell und Corpus-Werkzeuge |
| `samba/vfs_fastdup` | Experimentelles Samba-VFS-Modul für Fast Clone |

## Design und Nachweise

- [Verbindliche Domain-Sprache](CONTEXT.md)
- [Architecture Decision Records](docs/adr/)
- [Spezifikationen dauerhafter Formate](docs/specs/)
- [Testpläne](docs/testing/)
- [Betriebsanleitungen](docs/operations/)
- [Reproduzierbare Benchmarks](docs/benchmarks/)
- [Methodik des Vergleichs mit kommerziellen Appliances](docs/research/commercial-backup-appliance-comparison.md)
- [Status und Grenzen des Samba-VFS-Moduls](samba/vfs_fastdup/README.md)

Ein Beispiel: Beim dokumentierten Rocky-ISO-Workload erreichte der isolierte
AVX2-/BMI2-SeqCDC-Scanner 8.009 MiB/s (2,90× gegenüber dem skalaren Scanner),
während der gepaarte Single-Stream-SMB-Benchmark den Median des
End-to-End-Durchsatzes um 13,8 % verbesserte. Das sind host- und
workloadspezifische Messungen, keine Leistungszusage; siehe den
[vollständigen Benchmark](docs/benchmarks/seqcdc-prototype-2026-08-22.md).

## Grenzen

Vor dem produktiven Einsatz fehlen noch:

- vollständige POSIX-Abdeckung und eine breitere Client-/Samba-Matrix
- Schutz vor Geräteverlust, Replikation, Immutability und Encryption-Policy
- Langzeit-, Random-Kill- und physische Power-Cut-Tests
- breitere versionierte Backup-Corpora und ein Produktions-Gate für Advanced Reduction
- reale Veeam-Protokollevidenz für das Samba-Modul
- Kapazitäts- und Supportzusagen

Kommerzielle Systeme wie Dell PowerProtect Data Domain und HPE StoreOnce lösen
überlappende Backup-Storage-Probleme, bieten aber ausgereifte Integrationen,
Replikation, Retention, Cyber Resilience und Support, die fastdup nicht besitzt.
fastdup dient der Untersuchung und Erweiterung einer offenen POSIX-Dedup-Engine;
es ist weder Überlegenheitsversprechen noch Drop-in-Ersatz.

## Lizenz

Der Rust-Workspace und die Projektdokumentation stehen unter der
[Apache License 2.0](LICENSE). Das In-Process-Samba-Modul unter
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) steht separat unter
GPL-3.0-or-later, wie für ein Samba-VFS-Modul erforderlich.
