# Online Similarity und Share-Policy: Implementierungsnachweis

Online-Kandidaten werden ohne Remount, Offline-Rebuild oder querybaren
RAM-Overlay aufgenommen. Share-Einstellungen erlauben `off`, `dependent_v1`
oder Vererbung des Repository-Standards. Neu angelegte UI-Shares starten mit
explizitem `off`. Bestehende abhängige Daten bleiben nach Abschalten lesbar.

Implementiert gemäß den präzisierten Entscheidungen in
[ADR 0089](../adr/0089-refresh-reduction-snapshots-without-remounting.md).
Diese Notiz dokumentiert lokale Tests, keine Freigabe für Default-on-Betrieb
und keinen neu ausgeführten Kernel- oder SMB-Plattenbenchmark.

## A/B: gezielt ähnliche Daten

Fixture `online_similarity_performance_ab` in
`crates/fastdup-appliance/tests/write_through_ingest.rs`, Rust Release-Build:
4 MiB deterministische Pseudozufallsdaten plus acht Varianten mit je einer
Byteänderung pro referenzierter SeqCDC-Chunk-Range; insgesamt 36 MiB logische
Dateidaten. Pro Arm ein frisches In-Memory-Repository, normale Exact-Dedup-,
Compression-, Write-through- und Checkpoint-Pfade. Nur die Advanced-Policy
unterscheidet sich. Der On-Arm wartet nach der Basis einmal auf die erste
Online-Publikation, nicht auf einen Rebuild. Diese Wartezeit ist mitgemessen.

Drei aufeinanderfolgende Läufe des finalen Release-Testbinaries, nach Ende
der parallel ausgeführten Builds und Regressionstests:

| Lauf | Off DATA-Bytes | On DATA-Bytes | On Similarity-Bytes | Off ms | On ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 37.429.248 | 4.562.944 | 65.920 | 163 | 276 |
| 2 | 37.429.248 | 4.562.944 | 65.920 | 136 | 228 |
| 3 | 38.191.104 | 4.562.944 | 65.920 | 122 | 254 |

Die DATA-Zahlen summieren vollständige `.fdc`-Dateilängen inklusive
Container-Overhead. Similarity-Bytes summieren sämtliche Dateien des
separaten Similarity-Speichers nach Owner-Teardown. Exact- und Namespace-
Metadaten sowie echte Dateisystem-Allokation sind hier nicht enthalten.

- On: **8,27:1** logische Bytes / DATA-Dateibytes.
- Einschließlich Similarity-Index: **8,16:1**. Das ist ein kombinierter
  Datenreduktionsfaktor, kein isolierter Exact-Dedup-Faktor.
- Gegenüber dem Off-Median: **87,8 % weniger DATA-Dateibytes**.
- Median der gemessenen Wall Time: **136 ms → 254 ms**, also **+86,8 %**.
  Das ist keine Messung der Prozess-CPU-Zeit und kein Festplatten-Durchsatz.
- Je On-Lauf: 453 Queries, 32.866.552 eingesparte Payload-Bytes, zwei
  abgeschlossene Online-Batches, keine ausgelassenen Einträge oder Fehler.
- Jeder Off-Lauf: **null Similarity-Queries** und kein Similarity-Indexinhalt.

Die kleinen absoluten Laufzeiten und der asynchrone Ingest begrenzen die
Aussagekraft. DATA-Größe kann durch zeitabhängige Batch-/Container-Grenzen
variieren. Frühere Entwicklungsproben mit gleichzeitig laufenden Builds
zeigten ebenfalls andere Batch-Grenzen und Einsparungen; sie werden nicht mit
den finalen Läufen vermischt. Der gezielt geeignete Korpus belegt Wirkung,
nicht die zu erwartende Einsparung für beliebige Linux-Kernel-Versionen.
Ähnlichkeitssuche kostet messbar Rechenarbeit und gegebenenfalls Base-I/O.

Rohlog: `.artifacts/online-similarity-ab-three-runs.log`. Reproduktion:

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target TMPDIR=/source/fastdup/.artifacts/tmp cargo test -p fastdup-appliance --release --test write_through_ingest online_similarity_performance_ab -- --ignored --nocapture
```

## Funktions-, Fehler- und Überlastnachweise

- **Online-Sichtbarkeit:** neue unabhängige Basen werden im selben geöffneten
  Repository nützlich; kein Stop oder Rebuild zwischen Dateien.
- **Share-Policy:** vorhandene und neue Kinder erben ihre Policy; explizites
  Off gewinnt gegen globales On; Änderungen wirken live auf neue Planung;
  uneindeutige Hardlinks und verschachtelte Policy-Roots werden abgewiesen.
- **Recovery:** abhängige Daten bleiben korrekt lesbar; online gelernte Basen
  werden nach Crash/Reopen ohne Rebuild wieder genutzt; ein expliziter neuer
  Offline-Rebuild ersetzt den älteren Online-Stand.
- **BucketState64:** 80 Publikationen, wiederholte Fan-in-four-Kompaktierung,
  deterministische kleinste IDs und gleicher Query-Inhalt nach Neustart.
- **Fault Injection:** jeder Before-/After-I/O-Fault über vier Publikationen
  einschließlich Kompaktierung liefert nach Crash einen vollständigen Prefix.
  Getestet sind außerdem torn Head, korrupte selektierte Partition,
  reservierte Formatbytes, Identitäts- und Chronologieverletzungen.
- **Dateileases:** gehaltene alte mmap-Dateien bleiben trotz Kompaktierung
  vorhanden und lesbar; erst nach Freigabe werden sie ausgemustert.
- **Überlast:** blockierter Similarity-Head-Sync und volle Zwei-Batch-Queue
  blockieren weder DATA/Exact-Publikation noch Checkpoints. Nur Hinweise
  werden ausgelassen und gezählt.
- **GC:** ein laufender abhängiger Publish verhindert RETIRING; während
  RETIRING gibt es unmittelbaren Independent-Fallback. Ein fehlgeschlagener
  Exact-Publish behält einen begrenzten Schutz bis zum Owner-Teardown.
- **Bedienung:** Management-Protokoll, persistierte Share-Overrides,
  Legacy-Konfiguration und UI-Selects einschließlich Live-Standardwechsel.

Erfolgreich: Store-Suite (145 Tests), POSIX/Control (102), Appliance-
Library/Binaries/Ingest (67), Testkit Maintenance/Online-Similarity (60),
UI (15), Workspace-Clippy mit `-D warnings`, TypeScript-Typprüfung und
`git diff --check`. Weitere ignorierte Benchmarks wurden nicht automatisch
ausgeführt. Alle generierten Artefakte liegen unter `.artifacts/`.

## Verbleibende Qualifikation

Vor Default-on-Einsatz auf den echten Platten: gleicher Korpus und identische
Share-Policy im Off-/On-Vergleich, Schreib-Latenz p99/max, negative Index-Probes
bei vielen Familien, höhere Kompaktierungslevel, Indexwachstum und GC unter
anhaltender Last messen. Unausgewählte Artefakte fehlgeschlagener Index-
Publikationen können bis zur Offline-Wartung auf dem Datenträger bleiben.
Die Implementierung ändert weder die installierten laufenden Dienste noch
den vorhandenen Mount oder frühere Benchmark-Korpora.
