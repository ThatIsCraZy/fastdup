# Sechste Hotpath-Implementierung: Proof-Arena und Exact-Reader-Wiederverwendung

Die sechs empfohlenen Ansätze aus
[Audit 6](../research/hotpath-audit6-2026-09-06.md) sind umgesetzt. Unveränderte
Exact-Runs werden beim normalen Generation-Append über ihre bereits geprüften
Mapping-Owner weitergereicht. Kleine Hash-Batches fordern weniger Worker an;
große können weiterhin den ganzen Pool nutzen. Proofs, Read-Antworten und
Pending-Staging benötigen weniger Metadaten oder wiederholte Durchläufe.

## Implementierte Änderungen

| Bereich | Ergebnis |
| --- | --- |
| Generation Proofs | `HashTable<u32>` verweist in eine zusammenhängende Proof-Arena. Full-Key-Vergleich, ExactReuse-Vorrang, Frozen/Active-Übergänge und kombinierte 65.536-Einträge-Grenze bleiben erhalten. |
| RAM-Accounting | Active und Frozen melden tatsächliche Arena- und Hash-Kapazitäten einschließlich Reserven. Erfolgreicher Abschluss konsumiert und sortiert die Arena vor Historical-Admission. |
| Hash-Worker | Ein FILL-Vorpass unter einem Permit gibt die Nicht-FILL-Ordinals weiter. Bytezahl und Zahl unabhängiger Vierergruppen begrenzen die Parallelität. Vor erneuter Admission wird das erste Permit freigegeben. |
| Exact-Append | Activation-Record und gespeichertes Run Set werden weiterhin gelesen und geprüft. Passende immutable Mapping-Owner und Page Bounds werden geteilt; neue Runs vollständig auditiert. |
| Read-Antwort | `into_read_view` verschiebt das Backing in einen kleinen geprüften Bereichs-Owner; Single-Extent-Antworten verpacken keine vollständigen Exact-Metadaten mehr. |
| Pending-Staging | Append prüft neue Grenzen und erhält das Byte-Accounting. Detach prüft unabhängig sämtliche Chunks und berechnet im selben Durchlauf die früheste enthaltene Sequenz. |
| Prefix-Read | Zstd schreibt über die sichere Vec-WriteBuf-API in reservierte Kapazität. Vorheriges Nullfüllen entfällt; Outputlänge, CRC/Struktur und Ziel-Hash bleiben geprüft. |

Der allgemeine Worker-Queue-Umbau und der uneinheitlich gemessene Prefix-
Encoding-Umbau wurden entsprechend der Audit-Empfehlung nicht übernommen.
Es gibt weder ein neues Containerformat noch zusätzliche Unsafe-Operationen.

## Korrektheit und Fehlergrenzen

Der vollständige Workspace-Lauf besteht mit **765 erfolgreichen Tests**;
**16 Tests sind ignoriert**. Anschließend besteht ein zusätzlich ergänzter
gezielter Test für den optimierten Mapping-Pfad, insgesamt **766 erfolgreiche
Produkttests** über diese beiden Läufe. Clippy für den ganzen Workspace und
alle Targets besteht mit `-D warnings`.

Neue und bestehende Tests prüfen unter anderem:

- Volle Identität trotz identischen Tabellenhashes, ExactReuse-Vorrang bei
  abgebrochenem Freeze und die bisherige sortierte Historical-Admission.
- Bytegleiche Klassifizierung fragmentierter gemischter Batches bei einem
  Teilgrant von zwei statt zehn Permits; FILL bleibt bei einem Worker.
- Überlappende Pending-Appends sowie falsche Byte-Summen und Reihenfolgen
  am unabhängigen Detach-Boundary.
- Gleichen Antwortbereich samt Backing-Adresse bei nichtnulligem Payload-
  Offset; umgekehrte und zu große Bereiche bleiben ablehnbar.
- Zu kurzen und zu langen Prefix-Output sowie einen beschädigten Record.
- Tatsächlich geteilte Mapping-/Bloom-Owner beim Append, neu auditierte
  Mapping-Owner bei Recovery und Entfernen der optionalen Hints bei Swap.
- Ablehnung einer anderen Run-Identität und Auswahl des aktuellen dauerhaften
  Activation-Records bei veraltetem installiertem Prozesszustand.
- Fehler vor dem Activation-WAL-Schreiben und nach erfolgreichem WAL-Sync auf
  echtem FsStorageIo mit Mapping-Leases: Recovery und Offline-Activation-Audit
  wählen den alten bzw. vollständigen neuen Zustand; anschließender Append
  verwendet den korrekten Vorgänger. Das ergänzt die bestehenden vollständigen
  Fault-Matrizen, es simuliert keinen zusätzlichen Stromausfall.

Öffentliches Recovery, öffentliche Activation, positional Publication-Audit
und Offline-Scrub behalten ihre unabhängigen Prüfungen. Wiederverwendete
Mapping-Leases schützen weiterhin vor Mutation und Löschung. Die Regeln sind
in ADR 0050, 0059 und 0079 ergänzt.

## A/B mit der fertigen Implementierung

Die isolierte Quellkopie enthält die ursprünglichen Funktionen aus dem
Ausgangssnapshot und die aktuelle Implementierung im selben Test-Binary.
Die Varianten wechseln ihre Reihenfolge über elf Zeitproben; Exact-Append
verwendet neun. Zwei vollständige Wiederholungen laufen seriell, ohne anderen
Build oder Benchmark. Tabellenwerte sind Mediane.

| Echte Operation | Vorher, Lauf 1 / 2 | Nachher, Lauf 1 / 2 |
| --- | ---: | ---: |
| Hash-Klassifizierung, 8 × 16 KiB Nicht-FILL | 36,0 / 33,8 µs | 31,4 / 27,1 µs |
| 64 × 16 KiB Nicht-FILL | 396,0 / 384,0 µs | 155,4 / 143,9 µs |
| 64 × 64 KiB Nicht-FILL | 536,7 / 532,6 µs | 453,7 / 422,6 µs |
| 128 × 256 KiB Nicht-FILL | 1.852,9 / 1.992,9 µs | 1.867,2 / 1.976,3 µs |
| 64 × 16 KiB FILL | 455,2 / 402,8 µs | 24,6 / 20,7 µs |
| 64 × 64 KiB gemischt | 604,4 / 467,5 µs | 435,8 / 319,2 µs |
| 64 × 64 KiB Nicht-FILL, fragmentiert | 798,4 / 600,5 µs | 757,1 / 564,0 µs |
| Exact-Append, 65.536 vorhandene + 32 neue Einträge | 21,945 / 20,273 ms | 5,393 / 5,965 ms |

Der aktuelle Hash-Aufruf enthält FILL-Vorpass, Admission, Telemetrie und
Ergebniszusammenführung. Der Ausgangsaufruf enthält die frühere Admission und
Telemetrie um die alte Klassifizierung. Damit ist der Zusatzaufwand des neuen
Vorpasses mitgemessen. Der 32-MiB-Batch bleibt innerhalb ungefähr ±1 %; die
kleineren Batches gewinnen. Der fragmentierte Fall teilt jede 64-KiB-Allocation
in 17.003-Byte-Fragmente. Die Tests verwenden warme Daten im festen
Zehn-Thread-Pool dieses Hosts, keine Cold-Read-Messung.

Beim Exact-Append umfasst die Messung das echte zweite Append mit
Dateipublikation und Activation. Jede Variante bekommt pro Zeitprobe einen
eigenen frischen Workspace-Testordner. Vorher und nachher bleiben die gleichen
alten und neuen Schlüssel auffindbar. Die gemessene Zeit sinkt um etwa
**71–75 %**, einschließlich der verbleibenden Prüfungen und Syncs.

Die Arena-, Read-Owner-, Pending- und Prefix-Decode-A/B-Proben stehen im
vorangehenden Audit. Dessen RAM-Zahlen sind strukturbezogene VmRSS-Messungen,
keine hier erneut gemessene prozentuale Gesamtersparnis des SMB-Daemons.

## SMB-Vergleich

Die Serie verwendet den Skill `smb-single-stream-benchmark`: drei identische
Rocky-ISO-Kopien nacheinander pro Lauf, zwölf Sekunden Settle-Zeit und Messung
bei drei noch lebenden Dateien. Es gibt drei Vorher-/Nachher-Paare je Modus,
mit wechselnder Reihenfolge, insgesamt zwölf Läufe. Normal verwendet `off`,
Advanced `dependent-v1`. Die Auswertung prüft aktiv konfigurierte Similarity,
erfolgreiche abhängige Encodings, unveränderte ISO und Samba-Konfiguration,
separate Datenträger, Swap-Freiheit des Daemons und erfolgreiche Bereinigung.

Alle zwölf Läufe bestehen, der Daemon verwendet in keinem Lauf Swap, sämtliche
laufbezogenen Repositories werden bereinigt. Metadata liegt auf `/dev/sdb1`,
DATA auf `/dev/sdc1`; SMB läuft lokal über Port 1445. Die bestehenden
Lab-Einstellungen, Samba-Signing/-Encryption und die ISO bleiben identisch.

Mediane über je drei Läufe, Durchsatz aggregiert über die drei Uploads:

| Messgröße | Normal vorher | Normal neu | Advanced vorher | Advanced neu |
| --- | ---: | ---: | ---: | ---: |
| SMB Write | 1.369,1 MiB/s | **1.390,6 MiB/s** | 1.271,8 MiB/s | **1.280,3 MiB/s** |
| Erste ISO-Kopie | 993,9 MiB/s | 1.084,7 MiB/s | 855,6 MiB/s | 886,7 MiB/s |
| Kopien 2 und 3 zusammen | 1.662,7 MiB/s | 1.678,7 MiB/s | 1.651,9 MiB/s | 1.661,6 MiB/s |
| Reduction | 67,8235 % | **67,8221 %** | 67,9330 % | **67,8956 %** |
| Repository-Belegung | 1.907,84 MiB | 1.907,93 MiB | 1.901,35 MiB | 1.903,57 MiB |
| Daemon-CPU-Zeit | 11,32 s | 11,29 s | 16,86 s | 16,57 s |
| Spitzen-RAM / HWM | 503,13 MiB | **520,47 MiB** | 645,91 MiB | **582,17 MiB** |
| Abgeschlossener Upload, p99/max | 1.988,5 ms | 1.822,2 ms | 2.310,0 ms | 2.229,1 ms |
| Rechunk-Arbeit pro Lauf | 2,747 MiB | 3,136 MiB | 2,784 MiB | 2,776 MiB |

Der **Median der paarweisen Durchsatzänderungen** beträgt **+3,06 % Normal**
und **+1,76 % Advanced**. Die einzelnen Normal-Paare ergeben +3,06 %, +4,51 %
und −1,22 %; Advanced +1,76 %, +2,57 % und +0,29 %. Der Vergleich der
Gruppenmediane in der Tabelle ergibt dagegen +1,57 % und +0,68 % — diese
unterschiedlichen statistischen Größen werden nicht gleichgesetzt.
Alle Paare bleiben enthalten. Drei Paare auf diesem Host belegen keinen
allgemeinen oder statistisch abgesicherten Speedup.

Die Throughput-Spanne beträgt neu 1.369,5–1.430,8 MiB/s in Normal und
1.270,4–1.294,2 MiB/s in Advanced. `p99/max` bezeichnet nach dem Skill die
vollständige SMB-Dateiübertragung bis zur Bestätigung, nicht einzelne
SMB-Requests. Bei drei Kopien ist p99 der größte Wert; die Tabelle zeigt
den Median dieses Werts über drei Läufe.

Beim gesamten Prozess sinkt der RAM-Höchststand in Advanced median um
**63,74 MiB / 9,87 %**. Normal steigt dagegen um **17,34 MiB / 3,45 %**.
Damit ist die strukturbezogene Proof-Ersparnis aus dem Audit keine belegte
Gesamtersparnis für den Normal-Daemon. Gleichzeitig lebende Pipeline-Daten
und Mapping-/Cache-Lebensdauern beeinflussen den Prozesshöchststand ebenfalls.
Eine feste kausale Aufteilung dieser HWM-Unterschiede wurde nicht gemessen.

Normal belegt median **0,086 MiB mehr**, die Reduction ändert sich nur um
−0,00145 Prozentpunkte. Advanced belegt **2,219 MiB mehr**, also
**−0,03742 Prozentpunkte Reduction**. Das entspricht etwa 0,12 % zusätzlichem
physischem Repository-Platz. Die früheren Advanced-Läufe akzeptieren
82–88 Prefixes, die neuen 58–79; alle melden null Reduction-Fehler. In diesem
ISO-Korpus wird in keinem Lauf Sparse-XOR ausgewählt. Die verminderte Zahl
der Prefix-Encodings ist beobachtet; ihr kausaler Anteil an den einzelnen
Änderungen und am asynchronen Snapshot-Timing ist nicht isoliert. Es wird
deshalb keine exakt gleichbleibende Reduction behauptet.

**Normal gegen Advanced im neuen Stand:** Advanced erreicht **7,93 % weniger
Durchsatz**, spart dafür **4,36 MiB** Repository-Platz bzw. **0,07352
Prozentpunkte** Reduction. Die CPU-Zeit steigt von 11,29 auf 16,57 s.
Der Test schreibt dieselbe ISO dreimal und wird stark von Exact-Dedup
bestimmt; er misst nicht den Similarity-Nutzen zwischen veränderten
Backup-Ständen. Die Advanced-Telemetrie bestätigt in jedem Lauf aktivierte
Similarity-Abfragen und tatsächlich ausgewählte Prefix-Encodings.

## Provenienz und Reproduktion

Ausgangs-Binary (Runde 5):
`c246baa3b664cc8915af0a7070e60cc9f30059377b3c57bbb6c09abffb388ab9`.
Neues Binary:
`0508551d6ce15826f42cb64d74bec1c4897a2c61a2cf296bd9cac53ece618b4a`.
ISO:
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.

- `.artifacts/hotpath-implementation6-20260906/`: Ausgangssnapshot,
  Quellprovenienz, Implementierungsdiff, gesicherte Binaries,
  `build_current.py`, Build-/Test-/Clippy-Protokolle, isolierte Probe sowie
  `micro-ab.txt` und `micro-ab-repeat.txt`.
- `.artifacts/benchmarks/smb-implementation6-20260906/`: Runner und Auswertung,
  originale Aufrufe, Dry-Runs, Konfiguration, zwölf Einzel-JSONs und Summary.

Alle Cargo-Targets, Temp-Dateien und Messartefakte liegen unter dem Workspace.
Nach der Probe wurden Format, Store und Appliance einschließlich FUSE gezielt
im Hauptarbeitsstand neu kompiliert und das fertige Executable unmittelbar
gesichert. Ein Cargo-Cache-Hit auf einen gemeinsamen Top-Level-Binärpfad wird
nicht als Nachweis der Quellzuordnung verwendet.

Nach Abschluss sind FUSE, die beiden laufbezogenen Bind-Mounts und der
dedizierte Samba-Prozess entfernt. Das Top-Level-Release-Binary entspricht
dem gemessenen neuen Stand. Vorhandene Änderungen aus früheren Runden sind
erhalten; `implementation.patch` beschreibt ausschließlich diese Runde.
