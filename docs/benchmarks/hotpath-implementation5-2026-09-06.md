# Hotpath-Runde 5: Umsetzung und Messung

Stand: 2026-09-06. Alle sechs priorisierten Punkte und beide zusätzlichen
Ansätze aus [Audit 5](../research/hotpath-audit5-2026-09-06.md) sind umgesetzt.
Die Vergleichsbasis ist der Arbeitsstand zu Beginn dieser Runde, einschließlich
der noch nicht committeten Runde 4, auf `a2c59d1`.

## Änderungen

1. **Rechunking am Commit-Schnitt:** Detached Container tragen zusätzlich die
   früheste Sequenz ihrer vollständigen Chunks. Der Publication-Fence wartet
   auf den relevanten Publication-Prefix, auch wenn ein Container zugleich
   jüngere Chunks enthält. Der Retirement-Zielordinal wird einmal bestimmt;
   später eintreffende Arbeit verlängert das Warten nicht.
2. **Partielle Commit-Drains:** Vollständige Chunks eines partiellen Containers
   gehen durch dieselbe begrenzte Publication-Queue wie volle Container. Die
   Inode-Lane wird vor Encoding, DATA-Publication und Retirement-Warten
   freigegeben. Ein begrenzter Antwortkanal meldet Fehler an den Checkpointer;
   Frozen-Token und residente Daten bleiben für den Retry verfügbar.
3. **FILL-Erkennung:** Nach einem kurzen skalaren Abweisungspfad folgen sichere
   32-Byte-Blockvergleiche, die der Compiler vektorisiert. Fragmentenden bleiben
   explizit begrenzt; eigenes `unsafe` oder ausgerichtete Payloads sind unnötig.
4. **Materialisierung:** Die Parallelität richtet sich zusätzlich nach den
   tatsächlich zu kopierenden Bytes: ein Worker pro 512 KiB, mindestens einer,
   begrenzt durch konfigurierte Worker und Jobs. Kleine Kopien behalten die
   CPU-Zulassung, belegen aber weniger Worker.
5. **Proof-Lookups:** Der Ingest-Pfad verbindet Lookup und Active-Promotion
   unter einer Generation-Sperre. Ein Active-Treffer benötigt keinen zweiten
   Baumzugriff zur Admission. Frozen-Promotion, History-Fallback, ExactReuse-
   Vorrang und das gemeinsame Proof-Budget bleiben erhalten. Auch die allgemeine
   Admission verwendet einen einzelnen `BTreeMap::entry`-Zugriff.
6. **Fragmentierte Reads:** Ab mehr als 16 unbedeckten Intervallen bestimmt
   `ReadPlan` die betroffene Spanne per Binärsuche und ersetzt sie einmal als
   zusammenhängenden Bereich. Kleine Listen behalten ihren kurzen Pfad.
7. **Flache Manifest-Rezepte:** Ein geteilter Index kumulativer Endpositionen
   begrenzt Bereichsabfragen auf überlappende Extents. Leere und einzelne
   Extents brauchen keine Index-Allokation; 2.048 Extents benötigen 16 KiB
   zusätzliche Indexdaten pro gemeinsamem Rezept.
8. **Read-Cache-Metadaten:** Cache-Einträge vergleichen die bereits im
   verifizierten Payload vorhandene vollständige Identität. Die zweite Kopie
   von Chunk-ID und Länge entfällt. Auf diesem x86_64-Build sinkt `CacheEntry`
   von 224 auf 184 Bytes und das vierfache `CacheSet` von 904 auf 744 Bytes:
   17,7 % weniger Set-Metadaten, keine pauschale 17,7-%-RSS-Ersparnis.

Implementierung: [Checkpoint/Ingest](../../crates/fastdup-appliance/src/checkpoint.rs),
[ReadPlan](../../crates/fastdup-posix/src/versioned_file.rs),
[Manifest-Reader](../../crates/fastdup-store/src/manifest_reader.rs),
[Read-Cache](../../crates/fastdup-store/src/read_cache.rs).
Die Ergänzungen zu ADR 0041, 0046, 0050 und 0059 dokumentieren die Koordination
und Speicherbudgets. Dauerhafte Formate und deren Versionsnummern ändern sich
nicht; die Umsetzung enthält kein neues `unsafe`.

## Verifikation

- 705 Tests in Appliance, POSIX, Store, Format und Testkit bestanden, keine
  Fehler; 15 bestehende manuelle/umgebungsabhängige Tests ignoriert. Darin sind
  Recovery, Offline-Scrub, Write-through und die vorhandenen Fehlermatrizen
  enthalten. Dieser abschließende Lauf verwendet das Debug-Testprofil.
- Clippy für alle Targets von Appliance, POSIX und Store mit `-D warnings`
  bestanden; das finale Release-FUSE-Binary erfolgreich gebaut.
- 30.720 FILL-Differentialfälle prüfen Fragmentgrenzen, kurze Reste und
  Abweichungen an Anfang, Mitte und Ende gegen die skalare Referenz.
- Read-Regressionen prüfen fragmentierte Active/Frozen-Lagen, externe Quellen,
  Löcher, Truncate/Grow, EOF und die Stabilität bereits gepinnter Read-Pläne.
  Die Manifest-Probe vergleicht gemischte DATA/DATA-Slice/FILL/HOLE-Rezepte
  einschließlich leerer Bereiche und überlaufender Anfragen mit einem linearen
  Oracle.
- Queue-Tests prüfen den Container über dem Commit-Schnitt und den endlichen
  Fence trotz späterer Einreihungen. Ein Integrationstest hält eine partielle
  DATA-Publication bei `SyncFile` an: Derselbe Inode verarbeitet währenddessen
  weitere 40 MiB bis zu einer zweiten Publication. Nach Crash-Recovery ist
  ausschließlich der eingefrorene 16-MiB-Prefix bytegenau sichtbar.
- Ein injizierter Fehler vor dem partiellen `SyncFile` lässt den Frozen-Token
  bestehen. Der anschließende Retry und die Crash-Recovery erhalten alle Bytes.

## Isoliertes A/B des finalen Codes

Getrennte Quellkopie mit dem tatsächlichen neuen Code und der Referenz aus dem
gesicherten Ausgangsstand; nur dort ergänzte Messmodule. Elf wechselweise
angeordnete Samples pro Vergleich, Release-Profil, keine überlappenden Builds
oder SMB-Läufe. Beide vollständigen Messdurchgänge zeigen denselben Verlauf;
die Tabelle enthält den zweiten Durchgang.

| Operation | Ausgangsstand | Neu | Faktor |
| --- | ---: | ---: | ---: |
| Active-Proof-Hit, 1.024 Schlüssel | 235,5 ns | 88,7 ns | 2,65× |
| ReadPlan, 1 MiB, 1 Patch | 0,059 µs | 0,058 µs | 1,01× |
| ReadPlan, 1 MiB, 16 Patches | 0,965 µs | 0,976 µs | 0,99× |
| ReadPlan, 1 MiB, 128 Patches | 14,071 µs | 9,451 µs | 1,49× |
| ReadPlan, 1 MiB, 512 Patches | 146,383 µs | 42,530 µs | 3,44× |
| ReadPlan, 1 MiB, 4.096 Patches | 8.585,019 µs | 435,255 µs | 19,72× |
| Flat-Recipe-Range, 1 Extent | 18,4 ns | 21,9 ns | 0,84× |
| Flat-Recipe-Range, 32 Extents | 36,4 ns | 24,8 ns | 1,46× |
| Flat-Recipe-Range, 512 Extents | 438,1 ns | 28,9 ns | 15,16× |
| Flat-Recipe-Range, 2.048 Extents | 1.745,2 ns | 34,0 ns | 51,35× |

Der einzelne Flat-Extent wird absolut rund 3,5 ns teurer. Die großen
ReadPlan-Faktoren betreffen Fragmentierungsstress und messen Planbildung plus
Freigabe, ohne DATA-I/O; die Manifest-Messung betrifft nur Rezeptmetadaten.
Der Proof-Test umfasst 100.000 Zugriffe pro Sample, der Manifest-Test 20.000
4-KiB-Abfragen über wechselnde Positionen von 16-KiB-Extents. Diese Faktoren
beschreiben keinen vollständigen FUSE-/SMB-Read.

Die unverändert übernommenen FILL- und Materialisierungsansätze haben die
bereits im Audit dokumentierte A/B-Evidenz: 64-KiB-FILL-Scan etwa 11,6× schneller;
2 × 64 KiB materialisieren 39,20 → 11,29 µs mit einem budgetierten Worker.
Frühe FILL-Abweisung kostet dort rund 0,5 ns zusätzlich. Die funktionalen
Regressionen laufen hier erneut gegen die Produktionsimplementierung.

## SMB SingleStream: Normal und Advanced

Maßgeblich ist ausschließlich die Serie in
`.artifacts/benchmarks/smb-implementation5-verified-20260906/`. Ihr Umfang
wurde vor dem Start auf fünf Läufe je Build und Modus festgelegt. Ein
unveränderter Aufruf des Skills `smb-single-stream-benchmark` lädt pro Lauf
dieselbe Rocky-10.2-Minimal-ISO mit 2.072.444.928 Bytes dreimal seriell hoch.
Nach 12 Sekunden Settle wird die physische DATA- und Metadata-Belegung bei
drei lebenden Dateien gemessen. Das ergibt 20 Läufe / 60 Uploads mit frischen
Repositories und wechselnder A/B-Reihenfolge.

Normal verwendet `FASTDUP_ADVANCED_REDUCTION=off`, Advanced `dependent-v1`.
Beide Varianten verwenden dieselben separaten XFS-Dateisysteme `/dev/sdb1`
und `/dev/sdc1`, dieselbe Samba-Konfiguration auf Port 1445 und dasselbe
SMB3-Protokoll. Builds und Mikrobenchmarks überlappen keine SMB-Messung.
Die Prüfung kontrolliert Binär- und ISO-Hashes, tatsächlich aktive
Advanced-Queries, akzeptierte abhängige Records, Prozess-Swap und Cleanup.

Alle 20 Läufe bestanden, ohne Prozess-Swap, Advanced-Fehler oder Cleanup-
Fehler. Normal meldet keine Advanced-Queries; alle Advanced-Läufe enthalten
tatsächlich akzeptierte abhängige Records.

Die Tabelle enthält Mediane über fünf Läufe. Durchsatz ist die gesamte
logische Upload-Menge geteilt durch die Summe der drei Upload-Zeiten.
Reduction berücksichtigt die physische DATA- und Metadata-Belegung. Die
p99-Spalte ist der Median des größten abgeschlossenen **Datei**-Writes pro
Lauf: Bei drei Samples entspricht dieser Wert dem Runner-p99, nicht einer
Per-SMB-Request-p99.

| Build | Modus | Schreiben MiB/s | Reduction | Belegt MiB | Daemon-CPU s | RAM-HWM MiB | Datei-Max/p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Ausgangsstand | Normal | 1375,9 | 67,5602 % | 1923,46 | 11,35 | 556,26 | 1954,0 |
| Neu | Normal | 1324,2 | 67,8235 % | 1907,84 | 11,72 | 494,34 | 2014,2 |
| Ausgangsstand | Advanced | 1227,6 | 67,9030 % | 1903,13 | 17,17 | 677,79 | 2402,1 |
| Neu | Advanced | 1235,4 | 67,8994 % | 1903,35 | 17,02 | 607,66 | 2359,9 |

**Vorher/Nachher ist der Write-Durchsatz kein Gewinn:** Im paarweisen Median
liegt Normal bei **−3,51 %**, Advanced bei **−1,17 %**. Die fünf Einzelpaare
betragen in Normal +0,25 / −3,51 / −5,06 / −1,56 / −44,10 %, in Advanced
−4,63 / +4,40 / +2,16 / −1,17 / −7,35 %. Allein die unpaarigen
Durchsatzmediane würden Advanced hier fälschlich als Beschleunigung erscheinen
lassen. Die isolierten Read- und Proof-Gewinne sind kein Nachweis schnellerer
SMB-Uploads.

Der mediane RAM-Höchststand sinkt um **61,93 MiB / 11,1 %** in Normal und
**70,13 MiB / 10,3 %** in Advanced. Davon sind **6,90 MiB** unmittelbar als
kleinere Read-Cache-Metadaten nachweisbar: 41.367.040 → 34.128.768 Bytes.
Die übrige RSS-Differenz hängt auch von gleichzeitig lebenden Pipeline-Daten
ab; sie ist keine feste prozentuale Speicherersparnis für jede Arbeitslast.
Normal benötigt median **15,61 MiB weniger** Repository-Platz, entsprechend
**0,263 Prozentpunkten** zusätzlicher Reduction. Advanced liegt praktisch
gleichauf: neu 0,22 MiB mehr Belegung / 0,0037 Prozentpunkte weniger Reduction.

**Normal gegen Advanced im neuen Build:** Advanced spart median weitere
**4,50 MiB** beziehungsweise **0,076 Prozentpunkte** Reduction. Sein
Durchsatzmedian ist **6,7 % niedriger**, die Daemon-CPU-Zeit steigt von
11,72 auf 17,02 Sekunden. Die erste ISO-Kopie erreicht median 981,3 MiB/s
in Normal und 837,5 MiB/s in Advanced; Kopien zwei und drei zusammen erreichen
1598,0 beziehungsweise 1620,1 MiB/s. Drei identische ISO-Kopien werden vor
allem durch Exact-Deduplizierung reduziert. Die geringe zusätzliche
Similarity-Ersparnis lässt sich nicht auf veränderte Backup-Stände übertragen.

## Rechunking und Grenzen der Durchsatzmessung

Die Normal-Baseline verarbeitet pro Lauf 11,36–59,07 MiB erneut durch
Chunking; neu sind es **2,44–3,29 MiB**. Der Median sinkt von 19,82 auf
2,79 MiB, also um rund **86 %**. Advanced enthält in der Baseline zwei
Ausschläge von 49,55 und 33,56 MiB, während die neuen Läufe bei
2,73–3,13 MiB bleiben. Der größte einzelne Commit über beide Modi sinkt
von 30,85 auf **1,093 MiB**. Unvermeidbare Rand-Chunks bleiben erhalten;
die großen vorhandenen Bereiche werden in diesen Läufen als Rezepte genutzt.
Die deterministischen Fence-, Fehler- und Recovery-Tests prüfen den
Mechanismus unabhängig vom SMB-Timing.

Am Ende der gültigen Serie fallen drei aufeinanderfolgende Läufe stark ab:
Normal-neu Runde 5 erreicht 775,8 MiB/s, danach Advanced-alt 773,3 und
Advanced-neu 716,5 MiB/s. Gleichzeitig steigt die Daemon-CPU-Zeit von zuvor
etwa 11–12 auf 20,31 Sekunden beziehungsweise von etwa 17 auf 29–30 Sekunden.
Mehrere Checkpoint-CPU-Phasen werden teurer. Die Ursache dieser zeitlichen
Änderung ist nicht isoliert; es gibt keinen Beleg, sie allein der Implementierung
zuzuschreiben. **Alle Läufe bleiben in der festgelegten Auswertung.**

Als ausdrücklich nachträgliche Sensitivitätsprüfung, ohne die fünften Paare,
liegt der paarweise Median bei Normal −2,54 % und Advanced +0,50 %.
Auch das belegt keinen allgemeinen Write-Speedup. Die Throughput-Spannen
der vollständigen Serie betragen neu 775,8–1379,4 MiB/s in Normal und
716,5–1271,4 MiB/s in Advanced. Die Testumgebung läuft lokal über SMB;
die Ergebnisse sind weder ein Signifikanznachweis noch eine Cold-Cache-
oder physische HDD-Seek-Messung. Für die Write-Geschwindigkeit bleibt damit
weiterer Untersuchungsbedarf; die nachgewiesenen Verbesserungen dieser Runde
liegen bei Read-Planung, Proof-Lookups, Rechunk-Arbeit und Speicherbedarf.

## Provenienz und verworfene Vorläufe

Die erste, anschließend um Diagnoseläufe erweiterte SMB-Serie wurde als
Vorher/Nachher-Vergleich vollständig verworfen. Ihre vermeintliche Baseline
mit SHA256 `74fabc7931cbc597de4d643c513f50bd64abf035bc4bcef682c6ec8f1c8cf702`
enthält als Kompilierungsverzeichnis die frühere
`.artifacts/hotpath-audit5-20260906/probe`-Quellkopie. Sie bildet daher nicht
verlässlich den Ausgangsstand dieser Runde ab.

Ursache ist die gemeinsame Cargo-Target-Ablage: Ein Build aus einer isolierten
Quellkopie überschreibt den gleichnamigen Top-Level-Binärpfad. Ein anschließender
Cache-Hit des Hauptarbeitsstands meldet Erfolg, stellt dessen Executable aber
nicht zwingend wieder her. Das fiel beim abschließenden Hashvergleich des
zurückgebauten Haupt-Binaries auf. Die Rohdaten bleiben erhalten; ihre
Verzeichnisse enthalten jeweils `comparison-validity.json` mit dem
Ausschlussgrund. Es werden keine ausgewählten langsamen Läufe herausgefiltert.

Für die gültige Serie wurde die Baseline vollständig aus `source-before.tar.gz`
wiederhergestellt. Store, POSIX und Appliance einschließlich FUSE wurden
für beide Stände gezielt erneut kompiliert und die fertigen Binaries unmittelbar
danach unveränderlich gesichert. Build-Protokolle, Quellverzeichnis und
Binär-Hashes sind festgehalten. Der neue Endstand ist byteidentisch mit dem
bereits zuvor gemessenen Endstand. Die Quell-A/B-Messungen oben vergleichen
beide Funktionen innerhalb derselben Test-Binaries und sind von dem falschen
SMB-Baseline-Pfad nicht betroffen.

ISO-SHA256:
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
Verifiziertes Ausgangs-Binary:
`26ff512f98679e0e1bf7803f526af24cc0f28b941029183a9284ab094665661d`.
Verifiziertes Endstand-Binary:
`c246baa3b664cc8915af0a7070e60cc9f30059377b3c57bbb6c09abffb388ab9`.

## Reproduktion und Rohdaten

- `.artifacts/hotpath-implementation5-20260906/`: Ausgangssnapshot,
  Implementierungsdiff, verifizierte Binaries, `provenance.json`,
  `test-final.txt`, `clippy-final.txt`, `build-baseline-verified.txt`,
  `build-current-verified.txt` und `micro-ab1.txt` / `micro-ab2.txt`.
  `verify_provenance.py` prüft Ausgangsquellen, erlaubte Deltas und Binär-Hashes;
  `rebuild_verified_binaries.py` erzwingt die passenden Builds und sichert
  deren Executables unmittelbar.
  `baseline-workspace/` enthält den wiederhergestellten Ausgangsstand;
  `probe/` enthält die isolierten A/B-Messmodule, `diagnosis/` die ausdrücklich
  temporären Quellvarianten aus der verworfenen Vorserie.
- **`.artifacts/benchmarks/smb-implementation5-verified-20260906/`**:
  gültige Serie mit `run_comparison.py`, `analyze.py`, Dry-Runs,
  Originalaufrufen, Konfiguration, Einzel-JSONs und `summary.json`.
- `.artifacts/benchmarks/smb-implementation5-20260906/`,
  `smb-implementation5-diagnosis-20260906/` und
  `smb-implementation5-followup-20260906/` im selben Elternverzeichnis:
  verworfene Vorserie, Diagnose-Binaries und ursprüngliche Auswertung mit
  expliziter Kennzeichnung der ungültigen Vergleichsbasis.

Alle Cargo-Aufrufe verwenden das workspace-lokale Target- und Temp-Verzeichnis.
Bei einer Reproduktion nach Builds aus Quellkopien sind die betroffenen
Quellen gezielt neu zu kompilieren und die Binaries unmittelbar zu sichern;
ein bloßer erfolgreicher Cargo-Cache-Hit genügt für die Zuordnung nicht.
Nach Abschluss sind die laufbezogenen Repositories, FUSE- und Bind-Mounts
sowie der dedizierte Samba-Prozess entfernt; bestehende andere Dienste bleiben
aktiv. Das Top-Level-Release-Binary entspricht wieder dem gemessenen Endstand.
