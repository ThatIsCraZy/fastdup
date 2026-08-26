# fastdup

fastdup ist ein experimenteller Single-Node-POSIX-Speicher für Linux. Ein
FUSE-Dateisystem nimmt normale Dateioperationen an, zerlegt neue Inhalte mit
SeqCDC-v1 und speichert identische Chunks nur einmal. Dateien werden bytegenau
aus unveränderlichen, geprüften Containern und versionierten Metadaten
wiederhergestellt.

Das Repository ist ein Prototyp und noch kein Backup-Produkt. Der dauerhafte
Pfad umfasst Exact Dedup, RAW/Zstd-Encoding, sparse Dateien, Checkpoints,
Recovery, Scrub, einen neu aufbaubaren Exact Index sowie adaptives DATA- und
Metadata-GC. Similarity, Delta, Dictionary und Reorder sind Forschungs- oder
Referenzpfade und noch nicht Teil des dauerhaften FUSE-Formats.

Die verbindlichen Begriffe stehen in [CONTEXT.md](CONTEXT.md). Entscheidungen
über Haltbarkeit und Formate stehen in den [ADRs](docs/adr/).

## Aktueller Funktionsumfang

Der FUSE-Pfad unterstützt unter anderem:

- Dateien anlegen, öffnen, lesen, schreiben, verkürzen, umbenennen und löschen
- zufällige Writes, Append, `O_TRUNC`, `O_EXCL`, `RENAME_NOREPLACE`, `flush`
  und `fsync`
- sparse Dateien mit DATA-, FILL- und HOLE-Extents
- Metadatenklone über `copy_file_range`, ohne bereits unveränderliche DATA
  erneut zu lesen oder zu chunken
- absturzsichere Checkpoints und Recovery auf den jüngsten vollständigen
  Commit
- Unterverzeichnisse, Hardlinks, Symlinks, xattrs/ACLs, Besitz, Rechte,
  Zeitstempel und flüchtige POSIX-Record-Locks
- `statfs`, dünne Allokation, Hole Punch, Zero Range und DATA/HOLE-Seeks
- Offline-Scrub, adaptives Online-/Offline-GC und Neuaufbau des Exact Index

Der Adapter bildet noch nicht den gesamten POSIX-Umfang ab. Insbesondere BSD
`flock`, mmap-/Kernel-Cache-Verhalten und die breite Client- und
Crash-Konformitätsmatrix sind noch offen. Der verbleibende Umfang ist in der
[POSIX-Testplanung](docs/testing/posix-conformance.md) erfasst.

## Aktueller Projektstand

Die zuletzt abgeschlossenen Abschnitte haben Online-/Metadata-GC und den
Haltbarkeitspfad bei ausbleibendem I/O-Fortschritt geschlossen:

- Online GC beweist Kandidaten gegen die aktuelle und vorherige Commit-
  Generation, alle aktiven Metadata Root Pins und die aktive Exact-Index-
  Generation. Vor `RETIRING` werden diese Bindungen unter der gemeinsamen
  Publikationsbarriere erneut geprüft.
- Metadata GC markiert alle dauerhaften und prozesslokal gepinnten Graphen.
  Persistente Snapshot-/Addition-Kataloge beschleunigen unveränderte oder rein
  additive Läufe, erhalten aber nur nach einem exakten Mark Löschbefugnis.
- Ein kernelgestützter Appliance Lease schließt einen zweiten Writer und
  gleichzeitige Offline-Wartung aus. Ungültige Start-Policies scheitern, bevor
  das Repository geöffnet oder verändert wird.
- Ein expliziter Durability Supervisor schließt Mutation Admission nach fünf
  Sekunden ohne Checkpoint-Fortschritt. Deterministische Tests halten sowohl
  Metadata- als auch DATA-Sync an, bewegen die monotone Testzeit ohne reale
  Wartezeit und belegen: angenommene Writes bleiben lesbar, spätere Writes
  gelangen nicht mehr in den Namespace.
- Ein leerer, dauerhafter Appliance Recovery Latch wird vor dem Öffnen des
  Repositorys schreibbar gesetzt. Crash oder fehlgeschlagener Abschluss lassen
  ihn stehen; nur vollständige Recovery plus sauberer Shutdown oder ein
  erfolgreicher Offline-Scrub entfernen ihn. Fehler beim Setzen/Löschen sowie
  fehlerhafte Dateien und Symlinks werden fail-closed geprüft.
- Der Supervisor, die Latch-I/O und deren Synchronisation liegen ausschließlich
  im Daemon-/Maintenance-Kontrollpfad. Die POSIX-Mutations- und Ingest-Hot-Loops
  erhielten weder Clock-Dispatch noch Dateisystem-I/O oder neue Locks.
- Der vollständige serielle Workspace-Test, Clippy, der Release-Build und die
  reale siebenstufige SIGKILL/FUSE-Remount-Matrix sind für diesen Stand grün.
  Dauerhaft blockierte oder fehlerhaft bestätigende Hardware bleibt außerhalb
  des unterstützten Ausfallmodells.

## Empfohlener nächster Entwicklungsabschnitt

Als Nächstes sollte ein stabiler Format-/Downgrade-Epoch eingeführt werden.
Aktuell schützen Versionsfelder einzelne Objekte, aber noch kein
repositoryweiter Zaun verhindert, dass ein älteres Binary nach einem Upgrade
wieder schreibend öffnet und neuere gültige Zustände falsch behandelt. Das ist
vor breiteren Deployment- und Power-Cut-Kampagnen die wichtigste verbleibende
Produktionsgrenze.

Der Abschnitt ist abgeschlossen, wenn:

1. ein ADR die repositoryweite Epoch, unterstützte Lese-/Schreibbereiche und
   das Verhalten bei Upgrade und Downgrade eindeutig festlegt;
2. der Writer die Epoch vor der ersten Veröffentlichung eines davon abhängigen
   Formats dauerhaft anhebt;
3. Start, Recovery und Offline-Scrub unbekannte oder nicht schreibbare Epochen
   vor jeder Mutation ablehnen;
4. Fail-before/fail-after und Crash-Tests an jeder Epoch-Publikationsoperation
   nur einen vollständig alten oder vollständig neuen Zustand akzeptieren; und
5. ein älteres Binary einen neueren Writer-Zustand niemals stillschweigend
   zurückstuft oder überschreibt.

Danach sollte der noch separate Container-Verzeichnis-Scan durch einen
dauerhaften, aus den Container-Envelopes rekonstruierbaren Generation-
High-Water ersetzt werden. Es folgen breitere randomisierte Process-Kill- und
Blockgeräte-Power-Cut-Kampagnen und die offenen POSIX-/Samba-Matrizen.

## Ingest-Pipeline

Ein Write durchläuft den Live-Namensraum und anschließend die dauerhafte
Reduktionspipeline:

1. SeqCDC-v1 bestimmt inhaltsabhängige Chunkgrenzen.
2. BLAKE3-256 bildet die Chunk-ID und prüft rekonstruierte Bytes.
3. Der Exact Index sucht eine bereits geprüfte Location. Er ist nur eine
   Beschleunigung und keine Wahrheitsquelle.
4. Neue benachbarte Chunks werden zu höchstens 512 KiB großen Compression
   Regions zusammengefasst.
5. Jede Region wird RAW oder als unabhängiger Zstd-Level-3-Record gespeichert.
   Zstd wird nur gewählt, wenn die vollständige Speicherung mindestens 4 KiB
   und 3 Prozent spart.
6. Ein Checkpoint veröffentlicht Container, Manifeste, Exact-Index-Runs und
   den neuen Namespace-Commit in der vorgeschriebenen Reihenfolge.

Gleichförmige allokierte Bereiche werden als FILL gespeichert. Nicht
allokierte Nullbereiche bleiben HOLE und benötigen keinen DATA-Record.

```text
POSIX/FUSE write
      |
      v
Live-Namensraum -> SeqCDC -> BLAKE3 -> Exact Lookup
                                           | Treffer
                                           +----------> vorhandene Location
                                           |
                                           | neu
                                           v
                                      RAW oder Zstd
                                           |
                                           v
                                Container -> Manifest -> Commit-WAL
```

Die Stufen teilen sich ein begrenztes CPU- und Speicherbudget. Dadurch kann
ein einzelner Stream freie Kerne nutzen, ohne bei mehreren Streams unbegrenzt
zusätzliche Arbeit oder Speicher zu erzeugen.

## SeqCDC-v1 und SIMD

SeqCDC-v1 ist das Standardprofil für Write-through-Ingest und
Checkpoint-Rechunking. Die Parameter sind:

| Parameter | Wert |
| --- | ---: |
| Modus | Increasing |
| Sequenzlänge | 6 Bytes |
| Skip-Trigger | 50 Gegenflanken |
| Skip-Länge | 1.024 Bytes |
| Minimale Chunkgröße | 16 KiB |
| Maximale Chunkgröße | 256 KiB |

Auf CPUs mit AVX2 und BMI2 verwendet der Scanner automatisch einen
vektorisierten Kernel. Auf anderen CPUs läuft die skalare Implementierung.
Beide Pfade liefern exakt dieselben Grenzen; Differentialtests prüfen diese
Eigenschaft. Punktuelles `unsafe` ist auf den SIMD-Kernel begrenzt, der sichere
Aufrufer prüft die CPU-Features und Slice-Grenzen.

`FASTDUP_SEQCDC_FORCE_SCALAR=1` schaltet ausschließlich für Diagnose und
Vergleichsmessungen auf den skalaren Pfad. Für den normalen Betrieb ist keine
Umgebungsvariable nötig.

Auf der gemessenen Rocky-ISO erreichte der isolierte AVX2/BMI2-Scanner 8.009
MiB/s und damit das 2,90-Fache des skalaren SeqCDC-Scanners. Im gepaarten
SingleStream-SMB-Test stieg der Median des Gesamtdurchsatzes gegenüber
skalarem SeqCDC um 13,8 Prozent; bei zwei gleichzeitigen Streams waren es
4,4 Prozent. Das sind Ergebnisse eines bestimmten Hosts und keine
Kapazitätszusage. Aufbau, Rohdaten, Streuung und Einschränkungen stehen im
[SeqCDC-Benchmark](docs/benchmarks/seqcdc-prototype-2026-08-22.md).

Der Wechsel auf SeqCDC-v1 änderte die Policy- und Exact-Index-Profilidentitäten.
Ältere FastCDC-Prototyp-Repositories sind absichtlich inkompatibel. Die
Entscheidung ist in [ADR 0054](docs/adr/0054-use-seqcdc-v1-as-the-default-chunking-profile.md)
festgehalten.

## Haltbarkeit und Integrität

Akzeptierte Änderungen sind sofort im Live-Namensraum sichtbar. Ein Checkpoint
friert einen konsistenten Präfix ein und veröffentlicht ihn erst, nachdem DATA,
Metadaten und Commit Record vollständig geschrieben und geprüft wurden. Nach
einem Absturz lädt fastdup die jüngste vollständige Generation. Eine
unterbrochene Datei kann deshalb mit ihrem bereits committeten Präfix
zurückkehren.

Der Standard-Daemon plant etwa alle fünf Sekunden einen Checkpoint. Bei 512
MiB einzigartiger, noch nicht committeter DATA stoppt er kurz die Aufnahme
neuer Mutationen, um die Dirty-DATA-Menge zu begrenzen. `fsync` erzeugt keine
private Transaktion und verschärft die Haltbarkeitsgarantie nicht.

Die dauerhaften Grenzen werden mehrfach geprüft:

- BLAKE3-256 bindet Logical Chunks an ihre Inhalte.
- CRC32C, Header, Footer, physische Länge und Container-Hash prüfen Container.
- Unveränderliche Manifeste beschreiben jede sichtbare Dateiversion.
- Ein verkettetes, gechecksummtes Commit-WAL wählt die Namespace-Generation.
- Der Exact Index darf fehlen oder neu aufgebaut werden, ohne zur
  Inhaltsautorität zu werden.
- Scrub prüft erreichbare Generationen, Container und aktive Locations.

Container werden zunächst aufgebaut, vollständig erneut gelesen und geprüft,
mit `fsync` stabilisiert, atomisch ohne Überschreiben veröffentlicht und durch
einen Directory-Sync abgeschlossen. Das Modell setzt einen ehrlichen
Stable-Storage-Stack voraus. RAID, Gerätespiegelung, Snapshots und Schutz gegen
Geräteverlust gehören nicht zu fastdup.

Details stehen im
[Checkpoint-Testplan](docs/testing/durable-posix-checkpoint.md), in der
[Container-Spezifikation](docs/specs/container-v1.md), der
[Metadatenspezifikation](docs/specs/metadata-generation-v1.md) und der
[Exact-Index-Spezifikation](docs/specs/exact-index-run-v1.md).

## Bauen und testen

Vorausgesetzt werden Linux, eine aktuelle Rust-Toolchain und für einen echten
Mount ein nutzbares `/dev/fuse`. Alle erzeugten Dateien bleiben unter
`.artifacts`:

```bash
cd /source/fastdup

export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR" "$TMPDIR"

cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p fastdup-appliance
```

## Lokalen Mount starten

Mountpunkt, Metadatenwurzel und Containerwurzel müssen bestehende
Verzeichnisse sein. Metadaten und Container dürfen nicht dasselbe Verzeichnis
sein. Für einen lokalen Funktionstest reichen getrennte Unterverzeichnisse;
repräsentative Messungen sollten getrennte Metadata- und DATA-Geräte nutzen.

```bash
mkdir -p \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers

/source/fastdup/.artifacts/target/release/fastdup-durable-fuse \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers
```

Der Daemon läuft im Vordergrund. `Ctrl-C` stoppt die Mutationsannahme, führt
den abschließenden Checkpoint aus und hängt den Mount aus.

## Offline-Wartung

Vor Wartungsarbeiten muss der fastdup-Daemon beendet und der Mount ausgehängt
sein. `--offline` ist deshalb verpflichtend.

```bash
BIN=/source/fastdup/.artifacts/target/release/fastdup-maintenance
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN" --offline scrub "$META" "$DATA"
"$BIN" --offline metadata-gc "$META" "$DATA"
"$BIN" --offline rebuild-exact "$META" "$DATA"
"$BIN" --offline scrub-gc "$META" "$DATA"
```

`scrub-gc` koppelt Garbage Collection an einen erfolgreichen Scrub und den
beobachteten Füllstand des DATA-Geräts. Ablauf und Ausgaben beschreibt die
[Wartungsanleitung](docs/operations/scrub-and-exact-index-rebuild.md).

Bei laufendem Daemon kann ein sofortiger, vollständig vom Daemon koordinierter
Online-GC-Durchlauf angefordert werden:

```bash
"$BIN" --online gc-now "$META"
```

Der Appliance Lease verhindert, dass derselbe Repository-Stand gleichzeitig
über den Offline-Pfad geöffnet wird.

## SMB und Samba

Eine Samba-Freigabe kann auf dem FUSE-Mount liegen. Das experimentelle Modul
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) ergänzt Duplicate Extents
und Integrity FSCTLs für Fast-Clone-Clients. Es übersetzt einen gültigen
`FSCTL_DUPLICATE_EXTENTS_TO_FILE`-Aufruf in genau einen Linux-
`copy_file_range`-Aufruf und fällt bei Fehlern nicht unbemerkt auf eine
gepufferte Kopie zurück.

Das Modul ist noch nicht für Veeam Fast Clone freigegeben. Es fehlen reale
Veeam-Traces, breitere Protokolltests und weitere Lock-, Alignment- und
Fehlerfälle. Der portable Contract-Test läuft mit:

```bash
sh samba/vfs_fastdup/tests/run.sh
```

## Workspace

| Komponente | Aufgabe |
| --- | --- |
| `fastdup-format` | Versionierte Container-, Manifest-, Commit- und Exact-Index-Bytes |
| `fastdup-store` | SeqCDC, Reduktion, Container, Exact Index, Scrub und GC |
| `fastdup-io-uring` | Linux-`io_uring`-Pfad für Container-I/O mit geprüftem Fallback |
| `fastdup-posix` | POSIX-Modell, Live-Dirty-Overlay und Low-Level-FUSE-Adapter |
| `fastdup-appliance` | Ingest, Checkpoints, Recovery und ausführbare Programme |
| `fastdup-copy-metrics` | Günstige Hot-Path- und Kopiertelemetrie |
| `fastdup-testkit` | Deterministische Fehler, Crash-Modell und Corpus-Werkzeuge |
| `samba/vfs_fastdup` | Experimentelles Samba-VFS-Modul für Fast Clone |

## Grenzen

Vor einem produktiven Einsatz fehlen insbesondere:

- vollständige POSIX-Abdeckung und breitere Client-Kompatibilität
- ein stabiler Downgrade-/Format-Epoch-Zaun
- Schutz vor Geräteverlust
- Langzeit-, Zufalls-Kill- und echte Stromausfalltests auf Blockgeräten
- dauerhafte Writer-, Recovery- und Scrub-Invarianten für Similarity, Delta,
  Dictionary und Reorder
- Veeam-Protokollevidenz für das Samba-Modul

Messwerte sind workload- und hostabhängig. Reproduzierbare Methoden und
Einschränkungen liegen unter [docs/benchmarks](docs/benchmarks/), Testpläne
unter [docs/testing](docs/testing/) und Betriebsnotizen unter
[docs/operations](docs/operations/).

Der aktuelle reale Online-GC-Interferenzlauf ist unter
[docs/benchmarks/online-gc-interference-2026-08-26.md](docs/benchmarks/online-gc-interference-2026-08-26.md)
dokumentiert.
