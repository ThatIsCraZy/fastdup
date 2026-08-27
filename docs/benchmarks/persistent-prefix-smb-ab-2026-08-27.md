# Persistentes Zstd-PREFIX: SMB-ABBA-Lauf

Datum: 2026-08-27

## Fragestellung

Der Lauf prüft den produktiven, dauerhaften Pfad vom gepaarten
Exact-/Similarity-Rebuild über opt-in `ZSTD_PREFIX` bis Restore, Scrub und GC.
Er ist kein Beleg für Dictionary, Sparse-XOR-Delta oder Reorder und kein
allgemeiner Kapazitäts- oder Durchsatzanspruch.

## Aufbau

- Rocky Linux 10.2 Minimal ISO, 2.072.444.928 Bytes, SHA-256
  `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
- Zehn deterministische Versionen aus `corpus.md`, jeweils acht verteilte
  Ein-Byte-XOR-Änderungen.
- Ein frisches Repository pro Probe auf getrennten XFS-Geräten für Metadata
  (`/dev/sda`) und DATA (`/dev/sdb`).
- Version 00 wird mit deaktivierter Prefix-Auswahl ingestiert. Nach dem Commit erzeugt
  `rebuild-pool-indexes` ein kohärentes Exact-/Similarity-Paar. Version 01 bis
  09 werden danach einzeln über SMB3 geschrieben.
- Reihenfolge `off A1`, `prefix-v1 B1`, `prefix-v1 B2`, `off A2`.
- Nach 12 Sekunden Settle bleibt jede Version live; Version 09 wird über SMB
  zurückgelesen und per SHA-256 bytegenau geprüft. Danach werden Version 01 bis
  04 gelöscht, erneut committed und `gc-now` inklusive Scrub ausgeführt.
- Jeder Daemon läuft in einer eigenen transienten cgroup mit
  `MemorySwapMax=0`; `memory.swap.current` und der beobachtete Prozess-`VmSwap`
  bleiben in allen vier Proben null. Historischer Host-Swap ist nur Telemetrie.
- Completed-write-p99 ist bei neun seriellen Dateien der größte beobachtete
  Dateiabschluss, keine per-request SMB-Latenz.

Die vollständigen Schema-v1-Reports liegen lokal unter
`.artifacts/benchmarks/advanced-reduction-ab-20260827/`.

## Ergebnis

| Policy | Probe | SMB MiB/s | completed-write p99 | Repository Bytes | Restore MiB/s | Peak RSS | GC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| off | A1 | 585,0 | 8,83 s | 2.049.150.976 | 232,7 | 2.196.672.512 | 7,61 s |
| prefix-v1 | B1 | 675,1 | 6,82 s | 2.046.472.192 | 205,4 | 2.210.893.824 | 22,82 s |
| prefix-v1 | B2 | 644,6 | 7,49 s | 2.047.438.848 | 272,6 | 2.214.023.168 | 39,83 s |
| off | A2 | 712,9 | 6,30 s | 2.046.918.656 | 211,0 | 2.198.888.448 | 10,69 s |
| off, Mittel | 2 | 649,0 | 7,57 s | 2.048.034.816 | 221,9 | 2.197.780.480 | 9,15 s |
| prefix-v1, Mittel | 2 | 659,8 | 7,15 s | 2.046.955.520 | 239,0 | 2.212.458.496 | 31,33 s |

Gegen den Mittelwert der beiden Off-Proben bedeutet das für `prefix-v1`:

- 1,67 % mehr SMB-Durchsatz und 5,47 % niedrigere completed-write-p99;
- 1.079.296 Bytes beziehungsweise 0,0527 % weniger allokierte
  Repository-Bytes;
- 7,70 % mehr Restore-Durchsatz bei großer Streuung der Einzelwerte;
- 14.678.016 Bytes beziehungsweise 0,668 % mehr Peak RSS; und
- 3,42-mal so lange GC-Laufzeit im Mittel.

Alle vier Daemons blieben bei 0 Bytes Prozess-`VmSwap`. Host-Swap war bereits
vor den Proben belegt, gehörte aber nicht zum beaufsichtigten fastdup-Prozess
oder zu dessen eigener cgroup und ist deshalb kein Fehlschlag.

## Entscheidungs- und Cache-Telemetrie

Beide `prefix-v1`-Proben trafen dieselbe dauerhafte Entscheidung:

| Zähler | B1 | B2 |
| --- | ---: | ---: |
| Similarity Queries | 73 | 73 |
| Candidates / Base Reads / Prefix Trials | 6 / 6 / 6 | 6 / 6 / 6 |
| Angenommene Prefixes | 6 | 6 |
| Eingesparte Payload-Bytes | 776.040 | 776.040 |
| Kein Candidate / Fehler | 67 / 0 | 67 / 0 |
| Cache Hits / Misses | 145 / 126 | 151 / 124 |
| Cache Hit Rate | 53,50 % | 54,90 % |
| Resident / Target / Capacity Pages | 105 / 512 / 512 | 110 / 512 / 512 |
| Evictions / Pressure Rejections | 21 / 0 | 14 / 0 |

Damit sind Aktivierung, begrenzter Candidate-Pfad, Cache-Governance und
Fallback im realen SMB-Pfad beobachtbar. Die sechs Base Reads umfassten
776.409 Bytes; die Auswahl sparte davon 776.040 Payload-Bytes. Kein Fehler
erzwang einen unabhängigen Fallback.

## Bewertung

Die Richtung bei Durchsatz und completed-write-p99 ist positiv, aber zwei
Proben pro Policy reichen nicht für eine belastbare Leistungszusage. Der
Kapazitätsgewinn von 0,0527 % ist für dieses nur acht Byte pro Version
verändernde Corpus klein und liegt in derselben Größenordnung wie die
Variation der Container- und Metadata-Allokation. Auch die Restore-Werte
streuen zu stark, um den Mittelwert als Effekt der Policy zu werten.

Die GC-Kosten sind das klare Stoppsignal für eine Default-Aktivierung. B2
entfernte tatsächlich 21 Container, schrieb einen Ersatzcontainer, verlagerte
sechs Chunks und gewann netto 3.244.032 Bytes zurück; B1 benötigte trotz
fehlender Verlagerung 22,82 Sekunden. Die Ursache und Varianz müssen auf
breiteren, realistisch entwickelten Backup-Familien getrennt vermessen werden.
`prefix-v1` bleibt deshalb opt-in.

## Während des Laufs gefundener Fehler

Der erste reale Rebuild fand eine zuvor nicht abgedeckte Grenze:

- Identische Chunk IDs aus mehreren Containern gelangten doppelt in den
  Similarity-Partitionsstrom und verletzten dessen kanonische Ordnung. Der
  externe Sort verdichtet nun identische Einträge vor der Partitionierung und
  lehnt widersprüchliche Duplikate als Korruption ab.

Der Fall besitzt einen deterministischen Regressionstest. Rebuild,
bytegenauer Restore, Scrub und GC waren danach in allen vier Proben
erfolgreich. Neue Repositories verwenden in beiden Laufmodi vom ersten Commit
an ausschließlich die aktuelle Policy.

## Nächstes Gate

Vor einer Default-Aktivierung sind mindestens ein breiteres versioniertes
Backup-Corpus, mehr ABBA-Wiederholungen und eine Phasenanalyse der GC-Laufzeit
erforderlich. Die Abnahme muss Kapazität, Write-p99, Restore und GC gemeinsam
begrenzen; ein kleiner Write-Durchsatzvorteil allein genügt nicht.
