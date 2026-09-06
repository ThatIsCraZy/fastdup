# Hotpath-Audit 7 – nach Implementierungsrunde 6

Stand: 2026-09-06, Commit `86135ce76c93041935487c7a179f5194aa7c2e04`.
Der Produktionsbaum war zu Beginn sauber. Dieser Audit verändert ausschließlich
diesen Bericht; die ausführbaren Prototypen liegen unter
`/source/fastdup/.artifacts/hotpath-audit7-20260906/`.

Sechs weitere Ansatzpunkte sind durch isolierte A/B-Proben gestützt. Die größten
relativen Gewinne betreffen verbleibende Metadatenarbeit und Buffer-Ownership.
Die Messungen enthalten keine neue SMB-Messung und ergeben keinen addierbaren
Faktor für den gesamten Write- oder Read-Pfad.

## Ergebnis und Reihenfolge

Mediane des abschließenden Laufs `verification-final.txt`:

| Priorität | Änderung | Gemessener Ausschnitt | Basis → Challenger |
| --- | --- | --- | ---: |
| 1 | Chunk-Reihenfolge über einen kompakten Hash-Index ordnen | 512 Records, drei jeweils geordnete Teillisten | 150,75 → 13,85 µs |
| 1 | Ingest-Fragmente konsumierend teilen; Einzelfragment inline halten | 4 MiB in 4-KiB-Fragmenten, feste 64-KiB-Cuts | 41,21 → 22,42 µs |
| 1 | Read-Provenienz auf tatsächlich benötigte Felder verkleinern | Größe eines `VerifiedChunkPayload` | 176 → 128 Byte |
| 2 | Workerzahl der Advanced-Fingerprint-Phase separat wählen | 8 × 16 KiB, effektiv 8 → 2 Worker | 309,11 → 118,87 µs |
| 2 | Read-Cache direkt aus verifizierten Payloads befüllen | erstmalige Aufnahme von acht Chunk-Views | 609,76 → 468,47 ns |
| 3 | Normal Reduction direkt mit den vorbereiteten Regions weiterführen | Routing von 2.048 materialisierten Chunks | 12,23 → 4,37 µs |

Die neue Read-Struktur spart 27,3 % ihrer eigenen Metadaten. Das ist keine
entsprechende Einsparung am Prozess-RSS. Ein vierfach assoziatives Cache-Set
wird im Layoutmodell von 744 auf 552 Byte kleiner. Für alle sechs Kandidaten
reichen sichere Rust- und Bibliotheks-APIs; ein neuer Container-Codec oder eine
Formatmigration ist dafür nicht erforderlich.

## 1. Die Record-Reihenfolge wird über wiederholte Baumabfragen rekonstruiert

In `crates/fastdup-format/src/container.rs:3171` baut
`order_adaptive_records` aus der bereits bekannten Chunk-Reihenfolge eine
`BTreeMap<ChunkId, usize>`. `sort_by_key` fragt diese Tabelle für die
Sortiervergleiche wiederholt ab. Selbst bereits geordnete Records bezahlen
Tabellenaufbau und Lookups. Bei Advanced Reduction entstehen getrennte Listen
gewöhnlicher, vorbereiteter unabhängiger und abhängiger Records; die gemeinsame
Reihenfolge muss anschließend wiederhergestellt werden.

Der Challenger hält die vorhandene Chunk-ID-Liste als Arena und speichert nur
Ordinals in `hashbrown::HashTable<usize>`. Die Gleichheitsprüfung verwendet die
volle Chunk-ID. `sort_by_cached_key` bestimmt die Record-Ordinals einmal.
Duplikate in der vorgegebenen Reihenfolge, unbekannte oder fehlende Chunks sowie
eine unmögliche Reihenfolge innerhalb eines Multi-Chunk-Records bleiben Fehler.
Die abschließende vollständige Reihenfolgeprüfung bleibt erhalten.

| Records | Eingabereihenfolge | BTree, µs | Hash-Ordinals, µs |
| ---: | --- | ---: | ---: |
| 64 | bereits geordnet | 5,27 | 1,08 |
| 512 | bereits geordnet | 64,39 | 7,52 |
| 512 | drei geordnete Teillisten | 150,75 | 13,85 |
| 2.048 | bereits geordnet | 442,36 | 45,40 |
| 2.048 | drei geordnete Teillisten | 879,53 | 76,60 |
| 2.048 | zufällig permutiert | 3.480,42 | 79,29 |

Die drei Teillisten enthalten 60/20/20 % der Records. Das bildet die Form des
Zusammenführens nach, nicht eine gemessene Codec-Verteilung eines SMB-Laufs.
Die zufällige Permutation ist eine Belastungsprobe. Die Zeitprobe verwendet
RAW-Pläne mit kurzen Nutzdaten: Sie misst Tabellenaufbau, Sortierung, Prüfung
und Freigabe der Metadaten, keine Kompression oder Publikation. 512 beziehungsweise
2.048 Chunk-Ordinals entsprechen der Größenordnung eines 32-MiB-Containers bei
64 beziehungsweise 16 KiB pro Chunk; komprimierte Multi-Chunk-Records können
die tatsächliche Recordzahl deutlich verringern.

Ein zusätzlicher Test ordnet echte gemischte RAW-/Multi-Chunk-Zstd-Pläne und
serialisiert beide Ergebnisse. Die Containerbytes sind vollständig identisch;
der unabhängige Reader dekodiert alle ursprünglichen Nutzdaten. Full-Key-
Hashkollisionen, doppelte IDs, fehlende IDs und innerhalb eines Records
vertauschte IDs sind separat geprüft. Eine Umsetzung sollte auch die bestehenden
Prefix-, Sparse-XOR- und Transplant-Integrationstests durchlaufen.

## 2. Beim Schneiden werden Buffer-Referenzen und kleine Vektoren vervielfacht

`crates/fastdup-appliance/src/checkpoint.rs:1752` entfernt den vorderen
`MutationPayload` aus dem Tail, erzeugt einen Slice für den verbrauchten Teil
und bei einem Teilschnitt einen zweiten Slice für den Rest. Anschließend fällt
der ursprüngliche Owner weg. Beim vollständigen Verbrauch ersetzt damit ein
Clone plus Drop eine mögliche reine Ownership-Übergabe.

Die Probe trennt drei Änderungen:

1. Nur die Vollprüfung des verbleibenden Tails durch lokale Längen- und
   Zustandsprüfungen ersetzen.
2. Payload und Mutation Sequence in einer gemeinsamen Deque halten; vollständig
   verbrauchte Payloads verschieben, Teilschnitte sicher mit `Bytes::split_to`
   erzeugen. Die ursprüngliche Backing-Größe bleibt für Accounting erhalten.
3. Einen einzelnen Payload direkt im Chunk speichern und erst für mehrere
   Fragmente einen `Vec` verwenden.

Die unabhängige Prüfung jedes zurückgegebenen Chunks bleibt in allen Varianten
aktiv. Die vollständige Tail-Prüfung läuft am Ende der Probe. Auch die bestehende
Coalescing-Grenze von mehr als 1.024 Fragmenten bleibt unverändert.

Zeit je 4 MiB, jeweils mit interleaved Push/Drain und 256 KiB verbleibendem Suffix:

| Eingangsfragment / Cut | Basis, µs | nur inkrementell | konsumierend + gemeinsame Deque | zusätzlich Inline-Einzelfragment |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB / 64 KiB | 3,22 | 3,06 | 2,06 | 1,59 |
| 64 KiB / 64 KiB | 3,25 | 3,20 | 2,02 | 1,29 |
| 4 KiB / 64 KiB | 41,21 | 40,21 | 22,01 | 22,42 |
| 1 KiB / 64 KiB | 156,37 | 151,05 | 81,01 | 79,73 |
| 64 Byte / 64 KiB | 2.440,31 | 2.424,84 | 1.265,78 | 1.286,05 |
| 64 Byte / 128 KiB, mit Coalescing | 2.570,58 | 2.546,57 | 1.431,98 | 1.546,91 |

Damit ist die wiederholte Tail-Vollprüfung unter den tatsächlichen Bytegrenzen
ein kleinerer Kostenpunkt als zunächst aus dem Code ersichtlich. Der tragende
Gewinn kommt von Ownership und Allokationen. Inline bringt vor allem beim
Einzelfragment etwas; bei stark fragmentierten Chunks ist es nicht durchgehend
schneller als die konsumierende Vec-Variante.

Ein einzelner Chunk hält aktuell einen 40-Byte-Deskriptor und in dieser
Release-Probe einen Vec mit vier Slots à 40 Byte. Die Inline-Variante benötigt
56 Byte ohne diese 160-Byte-Heap-Allokation: rechnerisch 144 Byte weniger pro
Einzelfragment-Chunk, ohne Allocator-Overhead. Bei 2.048 solchen Chunks wären das
288 KiB. Mehrfragment-Chunks bekommen dagegen einen 16 Byte größeren Deskriptor.
Der Tail-Deskriptor selbst schrumpft im Modell von 80 auf 40 Byte.

Die Proben prüfen alle Ausgabe-Bytes und die jeweils höchste eingeschlossene
Mutation Sequence, einschließlich der kleinen Coalescing-Stressfälle. Die
Zeitmessung enthält feste Cuts, keine SeqCDC-Suche, FILL-Klassifizierung,
BLAKE3-Hashes oder Publikation. Eine Umsetzung muss lokale Invarianten an allen
Mutationen erhalten, Reserve-Fehler vor Ownership-Verlust behandeln und an den
Batch-/Detach-Grenzen unabhängig validieren. Einfach nur Assertions zu entfernen
ist nicht die vorgeschlagene Änderung.

## 3. Verifizierte Read-Payloads tragen entbehrliche Location-Felder

`crates/fastdup-format/src/container.rs:5255` enthält pro Payload eine
`Option<ExactIndexLocation>`. Dieser Nachweis wird ausschließlich beim
unabhängigen Candidate-Decode gesetzt. Seine Dependency-ID ist dort bereits als
null geprüft. Außerdem gehören Chunk-Ordinal und dekodierter Offset zum
jeweiligen Payload; die entsprechenden Felder der gespeicherten ersten Location
werden bei `matches_independent_candidate` nicht verwendet.

Der Challenger ersetzt diese Option durch eine private, inline gespeicherte
Independent-Record-Provenienz: Container-ID, Generation, Record-Offset,
Record-Länge, CRC, dekodierte Länge, Payload-Länge und Codec. Die bereits
bewiesene positive Record-Länge verwendet `NonZeroU32`, wodurch die Option ohne
zusätzlichen Tag Platz findet. Es entsteht weder ein weiterer Owner noch ein
zusätzlicher Zeigerzugriff. Die vollständige Chunk-ID, alle verwendeten
Candidate-Vergleiche und die Bytes samt Backing bleiben erhalten.

| Typ auf diesem x86-64-Build | bisher | kompakt |
| --- | ---: | ---: |
| optionale Record-Provenienz | 104 Byte | 56 Byte |
| VerifiedChunkPayload | 176 Byte | 128 Byte |
| vierfaches Cache-Set einschließlich Charge-Referenzen | 744 Byte | 552 Byte |

Gleiche vierfach assoziative Suchschleifen mit vollständigem ID-/Längenvergleich
und Owner-Clone, ohne Locks oder Pressure Refresh:

| Payload-Einträge | bisher, ns/Lookup | kompakt, ns/Lookup |
| ---: | ---: | ---: |
| 1.024 | 12,56 | 11,59 |
| 65.536 | 58,21 | 41,57 |
| 262.144 | 95,50 | 80,13 |

Das ist ein Datenstrukturmodell mit echten bisherigen Payloads und dem kompakten
Gegentyp, kein vollständiger Cache-/FUSE-Benchmark. Bei gleicher Zahl von 65.536
Einträgen sinkt die Set-Metadatenmenge um genau 3 MiB. Unter der bestehenden
Geometrieformel mit 16 Shards ergeben sich rechnerisch etwa 2,94 MiB weniger
Set-Metadaten bei einem 1-GiB-Cachelimit und 23,54 MiB bei 8 GiB. Dabei kann die
kleinere Geometrie bereits etwas mehr Sets zulassen. Diese Zahlen sind keine
RSS-Messungen; Payload-Backing, Owner und Pressure-Policy bleiben relevant.

RAW mit einem Slice in das besessene Containerbild sowie Multi-Chunk-Zstd sind
geprüft. Änderungen an ID, Länge, Ordinal, dekodiertem Offset, Container-ID,
Generation, Record-Offset, Record-Länge, CRC, Record-Längenfeldern und Codec
werden abgewiesen. Ein fehlender Nachweis bleibt ein fehlender Nachweis.
Unabhängige Reads, Recovery und Scrub dürfen keine Verifikationsschritte verlieren.
Die neue Struktur ist reine RAM-Repräsentation, keine Serialisierung von Rust-Layout.

## 4. Advanced-Fingerprints brauchen eine eigene Arbeitsgrößenregel

Runde 6 hat die Workerzahl der Chunk-ID-Klassifizierung angepasst.
`crates/fastdup-store/src/persistent_reduction.rs:267` verwendet für die
Fingerprint-Vorbereitung weiterhin den übergebenen Gesamtworkerwunsch;
`WorkerPermits::map` begrenzt ihn lediglich durch die Anzahl der Targets.
Fingerprinting kostet pro Byte deutlich mehr als die Chunk-ID-Klassifizierung.
Deren Schwellenwerte sollten daher nicht ungeprüft übertragen werden.

Die Probe führt das echte `timed_fingerprint`, den Aufbau des
`SimilarityIndexEntry`, die Counter-Updates und `map_admitted` in einem festen
Pool mit zehn Threads aus. Ergebnisse sind bei allen Limits identisch und alle
Permits werden zurückgegeben. Kandidaten-Lookup, Base-I/O und Codec-Trials sind
hier nicht enthalten.

| Target-Batch | 1 Worker, µs | 2 | 4 | Wunsch 10 |
| --- | ---: | ---: | ---: | ---: |
| 8 × 16 KiB | 150,92 | 118,87 | 157,10 | 309,11* |
| 64 × 16 KiB | 1.291,92 | 690,53 | 444,96 | 453,97 |
| 64 × 64 KiB | 5.141,67 | 2.638,74 | 1.468,86 | 1.085,49 |
| 128 × 256 KiB | 41.196,23 | 21.187,01 | 11.525,82 | 6.450,46 |

\* Bei acht Jobs werden tatsächlich acht Worker gewährt.

Der kleine Batch profitiert klar von zwei Workern. Bei 1 MiB sind vier und zehn
nahe beieinander; dieser Unterschied allein rechtfertigt noch keine harte Grenze.
Bei 4 und 32 MiB lohnt sich die breite Parallelisierung. Eine Umsetzung sollte
nur den Wunsch der Fingerprint-/Vorbereitungsphase anhand von Bytes und
Jobanzahl begrenzen, die Trial-Phase separat behandeln und Teilzuteilungen
weiterhin erlauben. Es gibt keinen Beleg für einen neuen prozessweiten Threaddeckel.
Die gemeinsame Queue unverändert zu lassen vermeidet die bereits in Runde 6
beobachtete Regression pauschalen Job-Batchings.

Für eine endgültige Regel fehlen noch die gesamte Kandidatenvorbereitung bei
verschiedenen Hit-Raten und die Interaktion mit parallelem Encode/Read.
Die Sweep-Messung belegt einen neuen Tuningpunkt, keinen gemessenen
MultiStream- oder SMB-Skalierungsfaktor.

## 5. Cache-Aufnahme erweitert bereits vorhandene Identitäten in neue Tupel

`crates/fastdup-store/src/read_cache.rs:586` wandelt den
`Vec<VerifiedChunkPayload>` in einen neuen Vec aus
`(ChunkId, u64, VerifiedChunkPayload)` um. Danach vergleicht die Aufnahme die
soeben kopierten IDs und Längen wieder mit denselben Payloads. Auf diesem Build
wächst der Eintrag dabei von 176 auf 216 Byte. Bei 32 Chunk-Views werden für den
neuen Vektor 6.912 Byte angelegt und Metadaten übertragen.

Die Alternative konsumiert direkt die verifizierten Payloads und leitet den
CacheKey erst beim jeweiligen Lookup ab. Die gemeinsame Backing-Identität und
Allokationsgröße werden weiterhin geprüft. Globale Admission-Sperre,
Shard-Sperren, Victim-Reihenfolge, Charge-Owner und Pressure-Accounting sind in
der Probe unverändert. Auch der gesperrte oder zu große Aufnahmefall vermeidet
so die vorgeschaltete Tupel-Allokation.

| Views / Zustand | bisher, ns/Gruppe | direkt, ns/Gruppe |
| --- | ---: | ---: |
| 1 / erstmalig | 137,65 | 117,21 |
| 8 / erstmalig | 609,76 | 468,47 |
| 8 / bereits vorhanden | 420,77 | 294,32 |
| 32 / erstmalig | 2.132,76 | 1.665,49 |
| 32 / bereits vorhanden | 1.242,45 | 914,87 |
| 32 / Cache durch Pressure deaktiviert | 721,67 | 348,34 |

Eingaben sind tatsächlich dekodierte, verifizierte Zstd-Records mit gemeinsamem
Backing. Die Probe verwendet den echten Cache und prüft Entry Count sowie
Resident Bytes. Fixture-Klones und das Leeren für erstmalige Aufnahme liegen
außerhalb der Zeitmessung. Die End-to-End-Dekompression liegt ebenfalls außerhalb.
Bei 32 erstmalig aufgenommenen Views war der Gewinn in einer früheren Wiederholung
nahe null; die vermiedene Allokation ist stabil, ihre relative Zeitersparnis hängt
von Cache-Zustand und Timing ab. Diese Prozentwerte lassen sich nicht mit der
kompakten Payload-Probe multiplizieren.

## 6. Normal Reduction durchläuft unnötig das Advanced-Auswahlrouting

`crates/fastdup-appliance/src/checkpoint.rs:4103` baut auch bei deaktivierter
Advanced Reduction einen flachen Target-Vektor. Anschließend entsteht eine
vollständig wahre Auswahlmaske, und `ordinary_region_slices` zerlegt die bereits
vorbereiteten Regions erneut und prüft materialisierte Partitionen noch einmal.

Für Normal Reduction kann die vorhandene Region-Liste unmittelbar weitergereicht
werden. Die weiterhin benötigte Chunk-Reihenfolge lässt sich direkt daraus
ableiten. Damit entfallen Target-Kopie, Maske und zweiter Region-Vektor. Der
Advanced-Pfad mit tatsächlich gemischten Entscheidungen behält seine Auswahl.

| Chunks | Regions | bisher, µs | direkt geliehen, µs |
| ---: | --- | ---: | ---: |
| 512 | Borrowed | 2,38 | 0,64 |
| 512 | materialisiert | 2,86 | 0,59 |
| 2.048 | Borrowed | 10,00 | 4,32 |
| 2.048 | materialisiert | 12,23 | 4,37 |

Die Fixture enthält eindeutige, vorab gehashte 16-KiB-Chunks und bis zu 512-KiB-
Regions. ID-Reihenfolge, Gruppierung, Bytes und geliehene Adressen sind gleich.
Gemessen wird ausschließlich dieses Routing. Es ist ein kleiner, gut abgrenzbarer
Zusatz zu den größeren Änderungen; die Kompressionsarbeit wird dadurch nicht
entsprechend schneller.

## Geprüft und nicht neu empfohlen

Ein One-shot-BLAKE3-Pfad für Einzelfragmente brachte gegenüber dem vorhandenen
Streaming-Hasher bei 16/64/256 KiB keinen stabilen Vorteil. Im abschließenden
Lauf lagen die Zeiten bei 3,44/13,61/53,39 gegenüber 3,42/13,47/53,79 µs.
Frühere Wiederholungen wechselten ebenfalls die Richtung. Dafür rechtfertigt
sich kein zusätzlicher Hotpath-Zweig und erst recht kein eigenes Unsafe.

Die vorhandenen SIMD-Pfade für SeqCDC, FILL, BLAKE3 und CRC bleiben Teil der
Basis. Der Host meldet AVX2 und BMI2, keine AVX-512-Unterstützung. Dieser Audit
hat keinen weiteren Intrinsics-Kern mit belegtem Vorteil gefunden.

Die Formatgrenze bleibt bei vollständiger Record-Verifikation und bis zu
512 KiB dekodierter Compression Region relevant. Kleinere Regions und
unabhängig verifizierbare Subframes wurden bereits in Audit 2 und 4 behandelt;
dieser Audit liefert dafür keinen neuen Cold-Read-/Reduction-Vergleich. Das
Streichen der Record-CRC oder von Chunk-Hashes wäre keine gleichwertige
Optimierung. Die oben gemessenen Änderungen benötigen keine neue Formatversion.

## Reproduktion und Aussagegrenzen

Alle zehn abschließenden Audit-Tests bestehen. Ein zweiter Lauf derselben finalen
Suite ist separat protokolliert. Es wurden weder die vollständige Workspace-Suite
noch neue SMB-Läufe ausgeführt, da der Produktionscode unverändert bleibt.
Eine Umsetzung benötigt die betroffenen Integrations-/Fault-Tests und danach
wieder getrennte Normal-/Advanced-SMB-Läufe mit Throughput, CPU, RAM und Reduction.
Insbesondere eine andere Scheduling-Reihenfolge kann den Zeitpunkt verfügbarer
Similarity-Snapshots verändern, obwohl Fingerprints und Codec-Regeln gleich bleiben.

Artefakte unter `/source/fastdup/.artifacts/hotpath-audit7-20260906/`:

- `source-before.tar.gz`, `initial-head.txt`, `initial-status.txt` und
  `initial-diff.patch`: exakte Ausgangslage.
- `probe/`, `probe.patch`, `REPRODUCE.md`: ausführbare Prototypen und Wiederholung.
- `verification-final.txt`, `verification-repeat.txt`, `metrics-final.json`:
  abschließende Ergebnisse; die Logs enthalten für die empfohlenen A/B-Kandidaten
  auch die elf einzelnen Zeitstichproben je Variante, jeweils auf die innere
  Wiederholungszahl normiert.
- `probes-first.txt`, `probes-second.txt`, `probes-third.txt`: frühere Wiederholungen
  und im Reproduktionsdokument bezeichnete Fixture-Erweiterungen.
- `provenance.json`, `environment.json`: Quellen-/Patch-Hashes, unveränderte
  Produktionsdateien, Compiler und gemeldete CPU-/Speichertopologie.

Alle Cargo-Aufrufe verwenden `--lib --release`,
`CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` und
`TMPDIR=/source/fastdup/.artifacts/tmp`. Varianten rotieren über elf Stichproben;
Builds und Benchmarkprozesse laufen zeitlich getrennt. Die Testsuiten laufen
seriell, nur die ausdrückliche Worker-Probe parallelisiert intern. Die
Tabellen zeigen Mediane, keine Konfidenzintervalle oder garantierten E2E-Gewinne.

Produktionsbinary und zurückgelegte Basis haben unverändert SHA-256
`0508551d6ce15826f42cb64d74bec1c4897a2c61a2cf296bd9cac53ece618b4a`.
