# fastdup

fastdup ist ein einzelner POSIX-Speicher auf FUSE-Basis. Er speichert Dateien
bytegenau, reduziert wiederholte Inhalte und schreibt dauerhaft nur vollständig
geprüfte Generationen. Der aktuelle Stand ist ein experimenteller Prototyp,
kein Backup-Produkt. Besonders wichtig: Exact Dedup, RAW/Zstd-Container,
Manifeste, Commit-WAL und Wiederherstellung sind im dauerhaften FUSE-Pfad
implementiert. Similarity, Delta, Dictionary und Reorder existieren bisher nur
in der Referenz-Pipeline.

Die wichtigsten Begriffe und Abgrenzungen stehen in [CONTEXT.md](CONTEXT.md).
Das dauerhafte Format ist absichtlich versioniert. Die genauen Bytes stehen in
den Spezifikationen für [Container](docs/specs/container-v1.md),
[Metadatengenerationen](docs/specs/metadata-generation-v1.md) und den
[Exact Index](docs/specs/exact-index-run-v1.md).

## Was heute funktioniert

Der dauerhafte FUSE-Pfad kann Dateien anlegen, öffnen, lesen, schreiben,
verkürzen, umbenennen, löschen und Verzeichnisse auflisten. Er unterstützt
zufällige Writes, append, `O_TRUNC`, `O_EXCL`, `RENAME_NOREPLACE`, `fsync`,
`flush` und sparse Dateien mit sehr großen Offsets. Ein Write in eine Lücke
legt nur die geschriebenen Bytes ab. Unbelegte Bereiche bleiben HOLE-Extents.

`copy_file_range` erzeugt Metadatenklone. Dabei liest fastdup weder die
Quell-DATA noch zerlegt es sie neu, wenn die Quelle vollständig allokiert und
bereits unveränderlich ist. Der Test klonte 2 MiB ab einem nicht an
FastCDC-Grenzen ausgerichteten Offset. Der Checkpoint schrieb dabei keine neuen
Container und die Wiederherstellung war bytegenau. Details und Fehlerfälle
stehen im [Fast-Clone-Testplan](docs/testing/veeam-fast-clone.md).

Der derzeitige FUSE-Adapter implementiert keinen vollständigen POSIX-Umfang.
`mkdir`, Unterverzeichnisse, Hardlinks, Symlinks, xattrs, Besitz- und
Rechteänderungen, Dateisperren, `statfs` sowie `fallocate` fehlen oder liefern
`EOPNOTSUPP`. Die [POSIX-Konformanzplanung](docs/testing/posix-conformance.md)
führt den verbleibenden Umfang mit Tests auf.

## Dauerhaftes Speicher- und Integritätsmodell

Jede akzeptierte Änderung wird sofort im Live-Namensraum sichtbar. Ein
Checkpoint friert einen konsistenten Präfix ein und veröffentlicht ihn nur,
nachdem DATA, Metadaten und Commit Record vollständig geschrieben und geprüft
sind. Nach einem Absturz lädt fastdup die jüngste vollständige Generation.
Eine unterbrochene Ingest-Datei kann daher mit einem gültigen, bereits
committeten Präfix zurückkehren.

Der Standard-Daemon plant einen Checkpoint alle fünf Sekunden. Bei 512 MiB
einzigartiger, noch nicht eingecheckter DATA schließt er kurz die Aufnahme
neuer Mutationen, damit die Dirty-DATA-Menge begrenzt bleibt. Bereits
angenommene Writes und Reads bleiben verfügbar. `fsync` macht daraus keine
eigene private Transaktion. Siehe [Checkpoint-Design und Fehlerfälle](docs/testing/durable-posix-checkpoint.md).

Der Speicher prüft Daten an mehreren Grenzen:

- BLAKE3-256 identifiziert jeden Logical Chunk und verifiziert wiederhergestellte Bytes.
- CRC32C, Header, Footer und physische Länge prüfen Container-Struktur und Veröffentlichung.
- Ein Commit-WAL mit verketteten, gecheckten Records wählt die sichtbare Namespace-Generation.
- Immutable Manifeste beschreiben Dateien inklusive DATA-, FILL- und HOLE-Extents.
- Der Exact Index beschleunigt Treffer, ist aber keine Wahrheitsquelle. Fehlt er
  oder ist er beschädigt, sucht der Leser über geprüfte Container weiter.
- Offline Scrub prüft erreichbare Generationen, Container und aktive Locations.
  Der [Ablauf für Scrub und Index-Rebuild](docs/operations/scrub-and-exact-index-rebuild.md)
  beschreibt diese Wartung.

Container durchlaufen `BUILDING`, vollständigen Reread, `fsync`, atomisches
Veröffentlichen ohne Überschreiben und Directory-Sync. Ein Fehlereinwurf-Test
akzeptiert bei der Wiederherstellung nur "nicht vorhanden" oder "vollständig
geprüft". Das Dateiformat speichert Felder einzeln und hängt nicht vom
Speicherlayout von Rust ab.

## Datenreduktion und Algorithmen

Der dauerhafte Pfad verwendet FastCDC v2020 mit 16 KiB Minimum, 64 KiB Ziel
und 256 KiB Maximum. Gleiche Chunks trifft der Exact Index über BLAKE3-256 und
verwendet eine bereits geprüfte Location erneut. Neue, benachbarte Chunks
bilden maximal 512 KiB große Compression Regions. Der Writer wählt RAW oder
unabhängiges Zstd Level 3 nur dann, wenn die komplette Speicherung mindestens
4 KiB und 3 Prozent spart. Gleichförmige allokierte Bereiche werden als FILL
gespeichert, unallokierte Nullbereiche als HOLE.

| Technik | Dauerhafter FUSE-Pfad | Referenz-Pipeline |
| --- | --- | --- |
| FastCDC, BLAKE3-Exact Dedup, FILL und HOLE | Ja | Ja |
| RAW und unabhängiges Zstd Level 3 | Ja | Ja |
| Immutable Container, Manifest, Commit-WAL, Exact-Index-Runs | Ja | Nein |
| LZ4 plus Zstd-Level-1 Inkompressibilitäts-Schranke | Benchmark-Policy, Store bleibt auf `off` | Nein |
| Zstd-Dictionaries | Noch nicht dauerhaft | Ja |
| Similarity-Suche und Depth-1 Sparse-XOR Delta | Noch nicht dauerhaft | Ja |
| Bounded Reorder | Noch nicht dauerhaft | Ja |
| Automatische Garbage Collection | Noch nicht online | Nein |

Die Inkompressibilitäts-Schranke probiert bei Regionen ab 128 KiB zuerst LZ4
und bei Bedarf Zstd Level 1, um einen voraussichtlich nutzlosen Zstd-Level-3
Lauf zu vermeiden. Sie schreibt keine zusätzlichen Formate. RAW und Zstd
bleiben die einzigen unabhängigen Records. Die aktuelle Rocky-ISO-Messung
verfehlte jedoch die CPU- und Platz-Grenzen für die Freigabe. Deshalb nutzt der
Store weiter den Baseline-Modus. Die Messwerte und Kriterien stehen in
[zstd-incompressibility-gate](docs/research/zstd-incompressibility-gate.md).

Die Referenz-Pipeline ist nützlich, um die späteren Algorithmen gegen echte
Daten zu prüfen. Sie ist kein Ersatz für den dauerhaften FUSE-Pfad. Ihre
Similarity-Suche begrenzt Kandidaten, Delta-Tiefe und Speicher pro Bucket.
Ein Delta darf nur einen Base Chunk benötigen. Dictionaries bleiben immutable
und inhaltsidentifiziert. [Die vollständige Referenzbeschreibung](docs/benchmarks/data-reduction-reference-v1.md)
enthält Grenzen, Korpora und negative Fälle.

## Gemessene Datenreduktion

Die Quoten hängen stark vom Datenbestand ab. Sie sind keine Kapazitätszusage.
Alle folgenden Läufe haben die Ergebnisse bytegenau wiederhergestellt.

| Pfad und Workload | Ergebnis | Einordnung |
| --- | ---: | --- |
| Dauerhafter FUSE-Lauf, 50 eng verwandte Rocky-ISO-Varianten | 61,18x Logical/DATA, 42,95x mit Metadaten | 98,311 Prozent der Checkpoint-Bytes waren Exact Hits. Gelöschte Container blieben mangels GC liegen. Das ist daher Ingest-Reduktion, keine aktuelle freie Kapazität. |
| Samba plus FUSE, drei serielle Kopien derselben 2,07-GB Rocky-ISO | 2,840x bis 2,904x | Alle drei Dateien blieben live. Der Exact-Index-Filter senkte Exact-Page-Zugriffe um rund 75 Prozent bei etwa 41 KiB Filterdaten. |
| Referenz-Pipeline, zehn leicht veränderte Rocky-ISOs | 10,676x Payload-Reduktion | Enthält Exact, Zstd, Similarity, Delta, FILL und Reorder. Container-Overhead, Metadaten und Dateisystembelegung sind nicht enthalten. |
| Referenz-Pipeline, XML und JSON | 6,174x Payload-Reduktion | Jede CDC-Region änderte sich, deshalb gab es dort keine Exact Hits. |

Der aktuelle Ende-zu-Ende-FUSE-Lauf schrieb mit einem p95 von 545,3 MB/s und
las bytegeprüft mit einem p95 von 922,7 MB/s. Seine DATA-Checkpoints lagen bei
2,496 s p95 und 2,846 s maximal. Er erfüllte die geprüfte 10-Sekunden-Grenze,
lief 601 Sekunden, schrieb 124,2 GB logisch und startete nach dem Abschluss in
55 ms mit einem leeren Namespace. Die vollständige Messmethode, I/O-Zähler und
Einschränkungen stehen in [io-intensive-fuse-600s](docs/benchmarks/io-intensive-fuse-600s.md).

## SMB und Samba

`samba/vfs_fastdup` ist ein experimentelles GPL-3.0-or-later VFS-Modul für
Samba 4.23.5. Eine normale SMB-Freigabe kann den fastdup-FUSE-Mount verwenden.
Das Modul ergänzt gezielt die Funktionen, die ein Fast-Clone-Client benötigt:

- Es meldet `FILE_SUPPORTS_BLOCK_REFCOUNTING` nur bei explizit aktivierter Freigabe.
- Es übersetzt `FSCTL_DUPLICATE_EXTENTS_TO_FILE` in genau einen Linux-
  `copy_file_range`-Aufruf auf dem FUSE-Dateideskriptor.
- Es implementiert einen festen, neustartstabilen Integrity-Information-Zustand.
- Es lehnt unzulässige, nicht ausgerichtete, übergroße, überlappende oder kurze
  Klone ab. Es kopiert dann nicht heimlich gepufferte Daten.
- Es hält `CLOSE` hinter allen vorher akzeptierten Operationen auf demselben
  Samba-Handle zurück.

Ein Loopback-Test mit SMB 3.1.1 konnte das Modul laden sowie PUT, Listing, GET,
CLOSE, Bytevergleich und Delete ausführen. Der Modul-Contract und der Build
gegen Samba 4.23.5 sind dokumentiert in [samba/vfs_fastdup/README.md](samba/vfs_fastdup/README.md).
Veeam Fast Clone ist noch nicht freigegeben. Es fehlen ein echter Veeam-Trace,
Protokolltests und die verbleibenden Ausrichtungs-, Lock- und Fehlerfälle.

## Architektur

| Komponente | Aufgabe |
| --- | --- |
| `fastdup-format` | Versionierte, geprüfte Bytes für Container, Manifeste, Commit Records und Exact-Index-Runs |
| `fastdup-store` | Aufbau, Verifikation und atomische Veröffentlichung immutable Container |
| `fastdup-posix` | Gemeinsames POSIX-Modell, Live-Dirty-Overlay und Low-Level-FUSE-Adapter |
| `fastdup-appliance` | Checkpoints, Recovery, Metadatengenerationen und der dauerhafte FUSE-Daemon |
| `fastdup-testkit` | Deterministische I/O-Fehler, Crash-Modell und Corpus-Werkzeuge |
| `samba/vfs_fastdup` | Experimentelles Samba-VFS-Modul für Duplicate Extents und Integrity FSCTLs |

Die Datenebene trennt Metadaten und DATA. Metadaten, Index, WAL und Recovery
liegen auf dem Metadata Tier. Große immutable Container liegen auf dem Data
Tier. Der Speicher setzt Schutz und Redundanz der Geräte voraus. RAID,
Snapshots und der Schutz vor Geräteverlust gehören nicht zum Projekt.

## Lokal bauen und prüfen

Alle erzeugten Dateien gehören unter `.artifacts`. Die folgenden Variablen
setzen dafür Rustup, Cargo, Build-Ausgabe und temporäre Dateien auf lokale
Verzeichnisse:

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

mkdir -p "$CARGO_TARGET_DIR" "$TMPDIR"
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Eine Matrix für die Inkompressibilitäts-Schranke liest einen Pfad, prüft jedes
geschriebene Containerbild und gibt CSV aus:

```bash
cargo run --release -p fastdup-format --example incompressibility_gate_matrix -- \
  ISO_PATH 8 v1
```

Für das Samba-Modul gibt es einen portablen Contract-Test:

```bash
sh samba/vfs_fastdup/tests/run.sh
```

## Grenzen vor einem produktiven Einsatz

fastdup ist noch kein Produkt für produktive Backups. Die wichtigsten offenen
Punkte sind vollständige POSIX-Abdeckung, Online-GC mit RETIRING-Transitions,
Metadata-GC, Schutz gegen Geräteverlust, Langzeit-Lasttests, zufällige
Kill-Tests und Stromausfalltests auf Blockgeräten. Dictionary, Similarity,
Delta und Reorder brauchen vor einem dauerhaften Format jeweils Writer-,
Recovery- und Scrub-Invarianten. Für Samba fehlen der reale Veeam-Trace und
Protokollevidenz für Fast Clone.

Die [ADRs](docs/adr/) enthalten die akzeptierten Entscheidungen und ihre
Folgen. Die [Test- und Benchmark-Dokumentation](docs/testing/) sowie
[Benchmarks](docs/benchmarks/) enthalten die belastbaren Messwerte.
