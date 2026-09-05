# Linux 6.12: permanente Online-Similarity über 50 Versionen

Datum: 2026-09-05  
Binary SHA-256: `4b3a07b20156251ffba996cee02f9e367326ff43fde009e4f338fc76d6642d90`

## Ergebnis

Die 50 unkomprimierten TAR-Streams von Linux 6.12.1 bis 6.12.50 enthalten
77.363.015.680 logische Bytes (72,05 GiB). Mit der aktuellen permanenten
Online-Similarity und einer explizit aktivierten Share-Policy belegt das
vollständige Repository 3.245.277.184 allokierte Bytes (3,02 GiB). Das ergibt
einen konservativen gesamten Datenreduktionsfaktor von **23,84:1**.

Der kontrollierte Exact-/Compression-Arm mit explizit ausgeschalteter
Similarity belegt 11.736.629.248 Bytes (10,93 GiB), entsprechend **6,59:1**.
Online-Similarity spart damit in diesem A/B **8.491.352.064 Bytes (7,91 GiB)
oder 72,35 %** des ansonsten benötigten Repositories.

| Messwert | Similarity aus | Online-Similarity | Differenz |
| --- | ---: | ---: | ---: |
| Logische Bytes | 77.363.015.680 | 77.363.015.680 | 0 |
| DATA allokiert | 10.813.915.136 | 2.044.182.528 | −8.769.732.608 |
| Metadata allokiert | 922.714.112 | 1.201.094.656 | +278.380.544 |
| davon Similarity-Index | 0 | 225.996.800 | +225.996.800 |
| Repository allokiert | 11.736.629.248 | 3.245.277.184 | −8.491.352.064 |
| Gesamter Reduction-Faktor | **6,59:1** | **23,84:1** | **3,62× höher** |

Der Faktor enthält Exact Dedup, FILL/Sparse-Behandlung, normale Kompression,
abhängige Similarity-Codecs, alle Container, Exact-/Similarity-Indizes und
Namespace-Metadaten. Er ist deshalb ein Repository-Datenreduktionsfaktor und
kein isolierter Exact-Dedup-Faktor. Alle Größen sind XFS-Allokation aus
`du -s -B1`, nicht nur apparent file length.

## Aufbau

- Offizieller, bereits einmal gegen das signierte Kernel.org-SHA256-Manifest
  geprüfter Quellkorpus: `linux-6.12.1.tar.xz` bis
  `linux-6.12.50.tar.xz`, zusammen 7.404.728.320 allokierte Bytes.
- Jedes Archiv wurde in genau eine rotierende unkomprimierte TAR-Datei
  entpackt und danach als opaker TAR-Stream kopiert. Die Staging-Belegung blieb
  unter dem autorisierten 10-GB-Limit. Es gab keine Ziel-Readbacks und keine
  erneute Quellhash-Prüfung.
- Zwei frische isolierte Repositories auf `/dev/sdb1` (Metadata) und
  `/dev/sdc1` (DATA), identisches Release-Binary und identische Reihenfolge
  6.12.1 bis 6.12.50.
- Beide Prozesse starteten mit Repository-Standard `off`. Nach dem einmaligen
  Mount wurde der `/tar`-Share über den Management-Socket explizit auf `off`
  beziehungsweise `dependent_v1` gesetzt. Die Antworten waren erfolgreich.
- Jeder Arm schrieb alle 50 TARs in **einem einzigen Mount**, jede Datei
  überschritt per `sync FILE` die `fsync`-Durability-Grenze. Es gab innerhalb
  eines Arms keinen Remount, Stop oder Index-Rebuild.
- Online GC war aus. Nach Datei 50 durfte die best-effort Similarity-Queue
  20 Sekunden auslaufen; ihre Telemetrie blieb anschließend stabil. Erst nach
  der vollständigen Messung wurde der jeweilige isolierte Benchmark-Mount für
  den nächsten A/B-Arm beziehungsweise das Aufräumen beendet.
- Die isolierten physischen Benchmark-Repositories wurden nach den Snapshots
  gelöscht. Bestehende Repository-Daten und der Quellkorpus blieben erhalten.

Der Vergleich misst die aktuelle Advanced-Reduction-Funktion als Ganzes. Er
isoliert nicht den Anteil der permanenten Queue von der inzwischen ebenfalls
unterstützten einmaligen Materialisierung fragmentierter Write-through-Chunks.
Beide Änderungen erklären, warum der aktuelle Pfad deutlich mehr Kandidaten
sieht als der ältere Offline-Odd/Even-Versuch.

## Similarity-Wirkung

| Zähler | Wert |
| --- | ---: |
| Queries | 1.031.922 |
| Kandidaten | 2.577.285 |
| Base Reads / gelesene Base-Bytes | 845.934 / 47.115.854.055 |
| Sparse-XOR Trials / akzeptiert | 845.934 / 62.995 |
| Prefix Trials / akzeptiert | 845.934 / 728.889 |
| Akzeptierte abhängige Encodings gesamt | 791.884 (76,74 % der Queries) |
| No-Candidate-Fallbacks | 107.709 (10,44 % der Queries) |
| Independent-Fallbacks nach Kandidatenprüfung | 132.329 |
| Berechnete eingesparte Payload-Bytes | 10.051.439.069 (9,36 GiB) |
| Advanced-Reduction-Fehler | 0 |

Der alte Odd/Even-Snapshot-Test akzeptierte nur 0,154 % seiner Queries und
erzielte netto 0,0668 % zusätzliche physische Einsparung. Im aktuellen
sequentiellen Lauf wird jede veröffentlichte unabhängige Version ohne Remount
zu einer möglichen Basis für die nächste. Zusammen mit der Abdeckung
fragmentierter Chunks steigt die Akzeptanz auf 76,74 %. Der A/B-Vergleich mit
demselben Binary bestätigt, dass die große physische Differenz tatsächlich am
aktivierten Advanced-Pfad hängt; der Off-Share führte null Similarity-Queries
und erzeugte null Similarity-Indexbytes.

## Queue und Kompaktierung

| Online-Zähler | Wert |
| --- | ---: |
| Publizierte Batches | 1.565 |
| Kompaktierungen | 519 |
| Aktive Familien am Ende | 8 |
| Ausgelassene Einträge | 61.590 |
| Publikationsfehler | 0 |

Der Index ist permanent und wurde während desselben Mounts fortlaufend
abgefragt und erweitert. Abfälle der gemessenen Repository-Allokation bei
Datei 23, 31, 41 und 50 zeigen die Freigabe ersetzter immutable Familien nach
Kompaktierung und Lease-Ablauf. Der aktive Similarity-Index belegt am Ende
215,53 MiB, 6,96 % des gesamten Online-Repositories.

Die begrenzte Queue hat unter dieser kontinuierlichen Last 61.590 Hint-Einträge
bewusst ausgelassen. Das ist kein Daten- oder Exact-Verlust und erzeugte keinen
Fehler, zeigt aber eine reale Backpressure-Grenze. Trotz dieser Drops erreicht
der aktuelle Lauf 23,84:1; ein vollständig gedrosselter Maximal-Einsparungslauf
wurde nicht als Ersatz für die reale kontinuierliche Ingest-Last verwendet.

## Performance

Gemessen ist die Summe beziehungsweise Verteilung von Kopie plus Datei-`fsync`.
Das vorherige XZ-Entpacken ist nicht Teil des Store-Durchsatzes.

| Messwert | Similarity aus | Online-Similarity | Änderung |
| --- | ---: | ---: | ---: |
| Summe Copy+fsync | 364,37 s | 631,00 s | +73,17 % |
| Effektiver Durchsatz | 202,48 MiB/s | 116,92 MiB/s | −42,25 % |
| Median pro TAR | 7,06 s | 12,51 s | +77,31 % |
| p95 pro TAR | 11,73 s | 18,57 s | +58,34 % |
| Maximum pro TAR | 12,62 s | 18,83 s | +49,24 % |
| Separates XZ-Entpacken gesamt | 78,46 s | 81,82 s | +4,28 % |

Die Mehrkosten passen zu 845.934 Trial-Paaren und 43,88 GiB kumulativ
gelesenen Base-Daten. Das ist der zentrale Trade-off: Für diesen stark
versionsähnlichen Korpus sinkt die physische Belegung dramatisch, während der
Write-through-Durchsatz um rund 42 % fällt. Der Off-Arm meldete acht
Checkpoint-Warnungen über fünf Sekunden, der Online-Arm keine; die höhere
Dateizeit des Online-Arms entsteht daher nicht aus zusätzlichen solchen
Warnpausen, sondern aus Candidate-/Base-/Codec-Arbeit. In beiden Logs gibt es
keinen Panic und keinen Similarity-Publikationsfehler.

## Evidenz

Rohdaten liegen unter
`/source/fastdup/.artifacts/kernel-online-similarity-50-20260905/`:

- `comparison.json`: finale A/B-Größen und Faktoren;
- `files.tsv`: alle 100 Datei-, Zeit- und Allokationsmessungen;
- `off/final.json` und `online/final.json`: finale Arm-Snapshots;
- `*/share-policy-response.json`: erfolgreiche live Share-Umschaltung;
- `*/inspect.json`: identische logische Write-Zähler und null Frontend-Fehler;
- `*/final-reduction-telemetry.log` und `*/final-online-telemetry.log`;
- `*/daemon.log`: vollständige Runtime-Telemetrie;
- `/source/fastdup/.artifacts/run_kernel_online_similarity_ab.sh`: Runner.

Der Quellkorpus bleibt unter
`/source/fastdup/.artifacts/kernel-benchmark-source` eingebunden. Die rotierende
TAR-Datei und beide isolierten Benchmark-Repositories wurden entfernt; die
Ergebnisdateien belegen 2,8 MiB.
