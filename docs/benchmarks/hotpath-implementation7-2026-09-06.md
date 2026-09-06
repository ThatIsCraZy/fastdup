# Hotpath-Implementierung 7: weniger Metadatenarbeit und Buffer-Referenzen

Stand: 2026-09-06. Basis ist Commit
`86135ce76c93041935487c7a179f5194aa7c2e04`; umgesetzt sind die sechs Vorschläge
aus [Audit 7](../research/hotpath-audit7-2026-09-06.md).

Die isolierten Gegenproben am tatsächlichen Produktionscode bestätigen weniger
Arbeit beim Ordnen, Extrahieren und Cache-Aufnehmen. Der SMB-Vergleich umfasst
24 erfolgreiche Läufe mit insgesamt 72 vollständigen ISO-Uploads. Der Median
der gepaarten Durchsatzänderungen beträgt +0,88 % mit Normal und +1,06 % mit
Advanced. Die Streuung erlaubt daraus keinen belastbaren allgemeinen
Durchsatzgewinn von einem Prozent abzuleiten.

## Umgesetzte Änderungen

1. **Record-Reihenfolge:** Ein `HashTable<usize>` referenziert die vorhandene
   Chunk-ID-Liste. `sort_by_cached_key` berechnet den Ordinal einmal pro Record.
   Vollständige ID-Vergleiche, Duplikatablehnung und abschließende Prüfung der
   gesamten Chunk-Reihenfolge einschließlich der Record-Partitionen bleiben
   erhalten. Die bereits im Workspace verwendete `hashbrown`-Abhängigkeit ist
   jetzt auch eine Produktionsabhängigkeit von `fastdup-format`.
2. **Ingest-Fragmente:** Payload und Mutation Sequence liegen in einer Deque.
   Vollständig konsumierte Segmente übertragen ihren Owner; Teilschnitte nutzen
   `MutationPayload::checked_split_to` mit der sicheren `Bytes::split_to`-API.
   Einzelfragmente stehen inline. Die ursprüngliche Backing-Größe bleibt für
   das Accounting erhalten. Jeder erzeugte Chunk prüft seine vollständige
   Fragmentlänge; die unabhängige Vollprüfung des verbleibenden Tails erfolgt
   an den Batch-Grenzen statt nach jedem Schnitt.
3. **Read-Provenienz:** Der private Nachweis für einen unabhängig verifizierten
   Record hält nur die tatsächlich benötigten physischen Koordinaten. Die
   positive Recordlänge erlaubt eine kompakte `Option`; bereits bewiesene
   Null-Dependency und doppelte Chunk-Koordinaten entfallen. Vollständige
   Record-/Chunk-Verifikation und Candidate-Abgleich bleiben erhalten.
4. **Cache-Aufnahme:** Verifizierte Payloads werden direkt konsumiert. Die
   temporäre Liste aus kopierter ID, Länge und Payload sowie der Vergleich
   dieser Kopien mit ihrem Ursprung entfallen. Gemeinsames Backing,
   Speicherabrechnung, Pressure-Verhalten, Lock-Reihenfolge und Eviction bleiben
   unverändert.
5. **Normal Reduction:** Die vorhandenen Compression Regions werden geborgt.
   Flattening, eine ausschließlich wahre Auswahlmaske und der erneute Aufbau
   derselben Regions entfallen. Advanced erstellt seine Auswahl weiterhin.
6. **Advanced-Parallelisierung:** Candidate Preparation fordert höchstens einen
   Worker je angefangene 64 KiB Targetbytes an, begrenzt durch Targetzahl und
   gemeinsames CPU-Budget. Dieser Abschnitt enthält Fingerprint, Candidate-Lookup
   und gegebenenfalls unabhängiges Encoding. Die separaten Base-/Codec-Trial-
   Wellen behalten ihr Budget. Große Batches können weiterhin den gesamten
   Pool nutzen; Teilzuteilungen und Permit-Rückgabe bleiben unterstützt.

Die tatsächliche Tail-Implementierung geht über den Audit-Prototyp hinaus:
Mehrfragment-Schnitte reservieren sämtliche erforderliche Speicherung vor der
ersten Mutation. Die Vorprüfung besucht ausschließlich das konsumierte Präfix.
Bei mehr als 1.024 Fragmenten wird direkt in den vorreservierten Coalescing-
Buffer kopiert, ohne zusätzlichen Fragmentvektor. Ein Fehler dieser expliziten
Reservierungen lässt den Tail unverändert.

Es gibt keine neue Unsafe-Stelle, kein neues Containerformat und keine
Migration. Die Umsetzung nutzt sichere Ownership-Übergaben und kleinere
Datenstrukturen. ADR 0046 und ADR 0050 dokumentieren die RAM- und Pipeline-
Invarianten. Rechunk-Semantik und Grenzen bleiben bestehen.

## Komponentengegenproben am Produktionscode

Die ursprünglichen und fertig implementierten Quellen wurden isoliert kopiert
und um identische Test-Workloads ergänzt. Gemessen wurden Release-Testbinaries
in zwei Durchgängen mit umgekehrter Reihenfolge, je Fall elf Zeitproben.
Die folgende Tabelle zeigt jeweils die Mediane beider Durchgänge separat.
Builds und SMB-Läufe liefen nicht gleichzeitig mit diesen Messungen.

| Ausschnitt | Einheit | Vorher → aktuell, Durchgang 1 | Vorher → aktuell, Durchgang 2 |
| --- | --- | ---: | ---: |
| Record-Ordnung, 512 Records aus drei Teillisten | µs | 166,585 → 16,163 | 161,608 → 15,811 |
| Record-Ordnung, 2.048 Records aus drei Teillisten | µs | 917,604 → 89,574 | 910,131 → 88,171 |
| Tail, 4 MiB in 4-KiB-Segmenten | µs | 41,409 → 22,425 | 38,719 → 22,377 |
| Tail, 4 MiB in 1-MiB-Segmenten | µs | 3,543 → 1,938 | 3,054 → 2,036 |
| Tail, 4 MiB in 64-Byte-Segmenten, Belastungsprobe | µs | 2.244,409 → 1.496,942 | 2.284,164 → 1.413,869 |
| Cache-Aufnahme, acht neue Views | ns | 795,162 → 371,702 | 795,532 → 390,786 |
| Cache-Aufnahme, acht bereits vorhandene Views | ns | 527,908 → 229,806 | 520,894 → 229,640 |
| Vollständiger Advanced-Planer, 8 × 16 KiB ohne Treffer | µs | 366,259 → 127,661 | 319,255 → 131,432 |
| Vollständiger Advanced-Planer, 8 × 16 KiB mit Treffern | µs | 763,109 → 578,696 | 773,284 → 597,974 |
| Vollständiger Advanced-Planer, 64 × 64 KiB ohne Treffer | µs | 1.356,778 → 1.333,830 | 1.101,154 → 1.377,676 |
| Vollständiger Advanced-Planer, 64 × 64 KiB mit Treffern | µs | 5.988,905 → 5.950,795 | 5.906,019 → 6.071,576 |

Die Tail-Probe verwendet feste 64-KiB-Schnitte und 256 KiB verbleibenden Suffix,
einschließlich der Vorreservierung der fertigen Implementierung. Sie misst
Ownership, Fragmentverwaltung und Prüfungen; SeqCDC, Hashing und Encoding
sind nicht enthalten. Die Record-Probe misst Metadatenarbeit an RAW-Plänen.

Die Planer-Probe nutzt einen Zehn-Worker-Pool, echte Exact-/Similarity-Indizes,
Base-Lesezugriffe und Codec-Trials mit warmem Dateisystemcache. Entscheidungen
und Similarity-Einträge werden gegen die serielle Planung geprüft, einschließlich
Permit-Rückgabe und erfolgreicher abhängiger Records im Trefferfall. Kleine
Batches profitieren auch mit Treffern. Große Batches fordern unverändert zehn
Worker an; die Streuung des großen No-Hit-Falls bleibt sichtbar und wird nicht
als gesicherter Gewinn gewertet.

Die Cache-Probe verwendet dieselbe feste 16-MiB-Grenze mit 16 Shards und ohne
automatische Pressure-Aktualisierung. Warme Cache-Hits liefern keinen stabilen
Geschwindigkeitsgewinn: bei 32 Views 32,439 → 28,977 ns im ersten und
31,123 → 41,469 ns im zweiten Durchgang. Der Gewinn der Aufnahme darf nicht
als entsprechender Gewinn des gesamten Read-Pfads interpretiert werden.
Für Normal-Routing gibt es in dieser Runde keine gesonderte neue Zeitprobe;
der Audit-Prototyp und die vollständigen SMB-Läufe sind die Zeitbelege.

## Speicher

Gemessene Größen auf diesem x86-64-Build:

| Struktur | Vorher | Aktuell |
| --- | ---: | ---: |
| `VerifiedChunkPayload` | 176 Byte | 128 Byte |
| Vierfach assoziatives Cache-Set | 744 Byte | 552 Byte |
| `ChunkFragments`-Deskriptor | 40 Byte | 56 Byte |
| Read-Cache-Metadaten im SMB-Lauf | 34.128.768 Byte | 25.392.000 Byte |

Der größere Fragmentdeskriptor ersetzt beim häufigen Einzelfragment einen
separaten Heap-Vektor. Der Read-Cache spart in allen gemessenen Läufen exakt
8.736.768 Byte beziehungsweise **8,332 MiB** feste Metadaten. Das ist keine
entsprechende Zusage für das gesamte Prozess-RSS: Dessen HWM-Median steigt mit
Normal um 12,61 MiB und sinkt mit Advanced um 8,22 MiB. Die Ursache dieser
schwankenden Gesamtbelegung ist mit diesen Messungen nicht isoliert.

## SMB: Normal gegen Advanced und Vorher gegen aktuell

Verwendet wurde der unveränderte Runner des Skills
`/root/.codex/skills/smb-single-stream-benchmark/SKILL.md`.
Je Binary und Modus gab es sechs Läufe. Jeder lädt dieselbe Rocky-10.2-Minimal-
ISO dreimal sequenziell hoch; alle drei Dateien bleiben während der Messung
vorhanden. Ein Lauf überträgt 6.217.334.784 logische Byte. Nach zwölf Sekunden
Settle-Zeit wird die allozierte Repositorygröße einschließlich Metadaten erfasst.

Dedizierte Samba-Instanz auf Loopback-Port 1445, Single Stream, getrennte XFS-
Disks: `/dev/sdb1` für Metadaten und `/dev/sdc1` für Container. Alle Laufartefakte
liegen über Workspace-Bind-Mounts unter `.artifacts`. Gleiche Konfiguration,
1-GiB-Small-File-Quota, `lab-allow-shared`, Normal mit `off`, Advanced mit
`dependent-v1`. Die Telemetrie bestätigt im Advanced-Modus Similarity-Abfragen
und akzeptierte abhängige Records, im Normal-Modus keine Similarity-Abfragen.

Alle Werte der folgenden Tabelle sind Mediane von sechs Läufen. Durchsatz
ist je Lauf logische Gesamtmenge geteilt durch die Summe der drei Put-Zeiten.

| Messgröße | Normal vorher | Normal aktuell | Advanced vorher | Advanced aktuell |
| --- | ---: | ---: | ---: | ---: |
| Gesamtdurchsatz, MiB/s | 1.402,19 | **1.404,21** | 1.294,26 | **1.298,78** |
| Erste Kopie, MiB/s | 1.076,98 | 1.093,29 | 896,54 | 891,10 |
| Kopien zwei und drei zusammen, MiB/s | 1.648,50 | 1.637,14 | 1.647,99 | 1.684,08 |
| Reduction | 67,82267 % | **67,82079 %** | 67,90831 % | **67,92251 %** |
| Alloziertes Repository, MiB | 1.907,895 | 1.908,006 | 1.902,816 | 1.901,975 |
| Daemon-CPU, Sekunden | 11,08 | 10,95 | 16,34 | 16,40 |
| Daemon-HWM, MiB | 483,62 | 496,23 | 602,97 | 594,75 |
| Längster vollständiger Put pro Lauf, ms | 1.835,17 | 1.807,87 | 2.204,55 | 2.218,23 |
| Rechunk-Arbeit pro Lauf, MiB | 2,693 | 2,807 | 2,852 | 2,599 |

**Aktuelles Advanced gegen aktuelles Normal:** 7,51 % weniger Gesamtdurchsatz
für 0,10172 Prozentpunkte zusätzliche Reduction, entsprechend 6,031 MiB
weniger alloziertem Speicher in diesem dreifachen ISO-Korpus. Dieser Korpus
belohnt vor allem Exact-Deduplizierung identischer Folgeschreibvorgänge; er
beschreibt keine allgemeine Similarity-Reduktionsquote für andere Daten.

Vorher und aktuell wurden innerhalb jedes Modus unmittelbar gepaart und die
Reihenfolge alterniert. Die gepaarten Durchsatzänderungen sind:

| Paar | Normal | Advanced |
| --- | ---: | ---: |
| 1 | +4,14 % | −2,92 % |
| 2 | +0,10 % | +2,97 % |
| 3 | +1,97 % | −3,81 % |
| 4 | −11,18 % | +1,02 % |
| 5 | +0,03 % | +2,28 % |
| 6 | +1,66 % | +1,09 % |
| Median der Paaränderungen | **+0,88 %** | **+1,06 %** |

Das Verhältnis der Gruppenmediane ist eine andere Kennzahl: +0,14 % mit Normal
und +0,35 % mit Advanced. Beide Auswertungen stehen vollständig zur Verfügung.
Kein Lauf wurde ausgeschlossen; insbesondere bleibt Normal-Paar 4 enthalten.
Die aktuellen Einzeldurchsätze reichen von 1.247,19 bis 1.463,93 MiB/s mit
Normal und von 1.210,32 bis 1.341,30 MiB/s mit Advanced.

Ursprünglich waren drei Paare je Modus geplant. Deren gepaarte Mediane waren
+1,97 % mit Normal und −2,92 % mit Advanced. Wegen des gemischten Ergebnisses
und der RAM-Streuung wurden die vollständigen Planer-Gegenproben geprüft und
drei weitere Paare je Modus ergänzt. Der Produktionscode und die gemessenen
Binaries blieben dabei unverändert. Die erste Kohorte bleibt separat erhalten;
die Erweiterung ist keine bestätigende statistische Signifikanzprüfung und
belegt keine behobene Regression. Eine Ursache des Kohortenunterschieds wurde
nicht nachgewiesen.

Die vom Skill ausgegebene p99 bei drei Samples entspricht dem längsten
vollständigen Datei-Put. Sie ist keine p99 einzelner SMB-Requests. Ein neuer
SMB-Read-Durchsatzbenchmark wurde in dieser Runde nicht ausgeführt.

## Korrektheit und Reproduzierbarkeit

- `cargo test --workspace -- --test-threads=1`: **772 bestanden, 0 fehlgeschlagen,
  16 ignoriert**, einschließlich Integrationstests und Doc-Tests.
- `cargo clippy --workspace --all-targets -- -D warnings`: bestanden.
- Neue bzw. erweiterte Tests prüfen konsumierende Splits, Backing-Abrechnung,
  fortlebende Views, Fehlerzustand, Mutation Sequences, Fragment-Coalescing,
  korrupte Tail-Längen, Teilzuteilungen und Permit-Rückgabe, Full-Key-
  Hashkollisionen, ungültige Reihenfolgen sowie alle Candidate-Koordinaten.
- Gemischte RAW-, Zstd-, vorbereitete unabhängige, transplantierte, Prefix- und
  Sparse-XOR-Pläne erzeugen nach Umordnung die erwarteten Containerbytes;
  unabhängiges Dekodieren und Korruptionsprüfungen bleiben Teil der Tests.
- Alle 24 SMB-Läufe: Status bestanden, keine Advanced-Fehler, kein Prozess-Swap,
  drei lebende Dateien bei der Messung und erfolgreiche Bereinigung.

Cargo verwendet durchgängig `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target`
und `TMPDIR=/source/fastdup/.artifacts/tmp`. Produktionsquellen wurden nach
Tests, Clippy und verifiziertem Release-Build nicht verändert.

Quellensnapshot, Build- und Testlogs, identische A/B-Workloads, Messbinaries,
Rohwerte und Wiederholungsanleitung:
`/source/fastdup/.artifacts/hotpath-implementation7-20260906/`.
SMB-Kommandos, Laufberichte, Daemon-Telemetrie, beide Kohorten und Gesamtauswertung:
`/source/fastdup/.artifacts/benchmarks/smb-implementation7-20260906/`.
Die vollständige Zusammenfassung steht jeweils in `components-summary.json`
beziehungsweise `summary.json`. Generierte Korpora und Messartefakte werden
nicht in Source Control aufgenommen.

SHA-256 der unveränderlichen SMB-Binaries:

- Vorher: `0508551d6ce15826f42cb64d74bec1c4897a2c61a2cf296bd9cac53ece618b4a`
- Aktuell: `d57df6958d9f30f1b63e478ddaadb0c715c264db4b9e4ecbcfe78ee6c3a59780`

ISO: 2.072.444.928 Byte, SHA-256
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
