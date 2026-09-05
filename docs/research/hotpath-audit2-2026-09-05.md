# Zweiter Hotpath-Audit: CPU, Parallelität, Reads und Containerformat

Stand: `1801e4a65296b54102a4929dcbc19458ff448dec`, 2026-09-05.
Die zuvor implementierten sieben Optimierungen sind Bestandteil dieses Stands.
Dieser Audit ergänzt ihn um isolierte A/B-Prototypen; Produktionscode wurde
nicht verändert. Alle Prototypen, Fixtures, Builds und Logs liegen unter
`/source/fastdup/.artifacts/hotpath-audit2-20260905/`.

Die größten unmittelbar belegten Chancen benötigen weder ein neues Format
noch neues `unsafe`: weniger Buchhaltung je Fingerprint-Byte, direkte
Bucket-Seitenauswahl, fortlaufende Kompaktionscursor und parallelisierte
Advanced-Planung. Das Containerformat hat eine erkennbare Schwäche bei kleinen
Reads aus großen Zstd-Records. Ein Teil davon ist bereits durch eine andere
Record-Gruppierung im bestehenden Format lösbar.

## Priorisierung

| Priorität | Änderung | Evidenz | Formatänderung |
| --- | --- | --- | --- |
| 1 | Fingerprint-Minimizer in 64-Byte-Blöcken berechnen | 1,43–1,54× schneller, gleicher Fingerprint | nein |
| 1 | Advanced-Planung einschließlich Fallback-Kompression in begrenzte CPU-Jobs verlagern | serieller Aufrufpfad; isolierte Fingerprint-Batches skalieren auf etwa 6,1× mit zehn Threads | nein |
| 1 | Similarity-Kompaktion direkt aus ihrem laufenden Cursor speisen | 1,67–1,78× schnellerer vollständiger Bucket-Scan | nein |
| 1 | Verzeichnis geprüfter Bucket-Seitengrenzen | 3,32–3,41× schnellere gemischte positive/negative Punktabfragen | nein, mit budgetierter RAM-Beschleunigung |
| 2 | Encode-Jobs dynamisch innerhalb erteilter Worker-Permits verteilen | 1,41–1,47× bei ungleich teuren Jobs | nein |
| 2 | Immutable DATA-FDs und bekannte Längen begrenzt wiederverwenden | ca. 1,71× bei warmen 64-KiB-Dateilesen; Nutzen bei 1 MiB schwankt stark | nein |
| 2 | Kleine Reads: kleinere Records beziehungsweise begrenzte parallele Decodes | 8,08–8,26× Einzel-Chunk-Decode; 3,7–4,8× Batch-Decode mit zehn Threads | kleinere Records: nein |
| 2 | Gemeinsame Region-Gruppierung und logische Reihenfolge für alle Encoding-Pläne | konkreter Fragment-/Codec-Split im Writer; Gesamteffekt noch ungemessen | nein |
| 3 | Einmal geprüfte neue Similarity-Partition beim Aktivieren weiterreichen | vollständiges Audit vor Publication und erneut beim Öffnen belegt | nein; Publication-Protokoll sorgfältig anpassen |
| 3 | Antwort-Views bis FUSE und initialisierungsarme io_uring-Read-Puffer | noch vorhandene Kopie beziehungsweise Nullinitialisierung im Code | nein; eigener A/B erforderlich |

Faktoren gelten für die jeweilige isolierte Operation. Sie sind keine
SMB-Prognose und dürfen nicht multipliziert werden. Ein neues SMB-A/B mit den
Challengern wurde in diesem Audit nicht ausgeführt.

## 1. Fingerprinting: weniger Arbeit pro Byte

`reduction_similarity.rs:63` berechnet den rollenden Shingle-Hash und ruft für
jedes Byte `observe_shingle` auf. Dort werden das Minimum und der Span-Zähler
aktualisiert und das Erreichen von 64 Shingles geprüft (`:163`). Die Hash-Tabelle
und AVX2-Votes existieren bereits; deren Einführung wäre keine neue Optimierung.

Der Challenger verarbeitet jeweils die verbleibenden Shingles eines vollständigen
64er-Spans in einer inneren Schleife. Diese aktualisiert nur Rolling Hash und
Minimum. Span-Buchhaltung und `commit_minimizer` passieren einmal pro Block.
Der erste Span beginnt wie bisher mit dem Shingle aus den ersten 32 Bytes;
Teilspans am Ende werden identisch abgeschlossen. Minimizer, Superfeatures,
Sketch und Profil bleiben gleich. Es werden keine Byte-Prüfungen übersprungen.

Auf 32 MiB aus dem Rocky-ISO, Offset 256 MiB, in 64-KiB-Chunks:

| Lauf | Bisher, ms | Blocklauf, ms | Faktor |
| --- | ---: | ---: | ---: |
| 3 | 80,143 | 52,023 | 1,541× |
| 4 | 70,403 | 49,093 | 1,434× |

Je Lauf neun alternierende A/B-Samples, angegeben sind Mediane. Frühere
Vorbereitungsläufe lagen zwischen 1,28× und 1,71×; die Streuung spricht gegen
eine präzisere allgemeine Leistungszusage. Der finale Harness prüft 1.041
Rand-/Inhaltsfälle sowie die vollständige Ausgabe des 512-Chunk-Batches gegen
den unveränderte Implementierung des Fingerprint-Profils. Die Änderung selbst ist
sicheres Rust und verwendet den vorhandenen SIMD-Kern unverändert.

## 2. Die Advanced-Planung liegt vor dem parallelen Encode-Bereich

`checkpoint.rs:3828` ruft zuerst `plan_new_chunk_encodings` auf. Erst danach
werden Worker-Permits genommen und `encode_cpu.begin()` gestartet. Die Planung
(`:3925`) läuft in einer gewöhnlichen `for chunk in new_chunks`-Schleife und
umfasst Materialisierung, Fingerprinting, Indexabfragen, Base-Auflösung und
Codec-Trials. Es gibt höchstens zwei permanente Publication-Worker (`:3176`).
Zehn verfügbare Rayon-Worker beschleunigen diese vorgezogene Arbeit daher nicht.

Das hat zwei Folgen:

- Die aktuelle Encode-Telemetrie enthält einen wesentlichen Teil der
  Advanced-CPU-Arbeit nicht. Im vorherigen SMB-Paar steigt gesamte Daemon-CPU
  von 11,32 auf 20,19 Sekunden, während `write_through_encode_cpu` sogar weniger
  aufsummierte Runnable-Wall-Zeit meldet. Diese Zähler messen unterschiedliche
  Bereiche; daraus lässt sich keine genaue Kostenaufteilung ableiten.
- Bei einem NoCandidate-Fallback für einen materialisierten Chunk wird sogar
  dessen unabhängiger Zstd-Trial seriell vor dem parallelen Bereich ausgeführt.

Die bestehende Fingerprint-Funktion skaliert auf demselben 32-MiB-Batch:

| Threads | Lauf 3, ms | Lauf 4, ms |
| --- | ---: | ---: |
| 1 | 78,635 | 70,127 |
| 2 | 42,513 | 37,881 |
| 4 | 25,890 | 19,300 |
| 8 | 14,690 | 12,847 |
| 10 | 12,965 | 11,495 |

Das ist reine CPU-Arbeit mit permanentem Pool außerhalb der Zeitmessung,
keine parallelisierte vollständige Publication. Ein sinnvoller Umbau pinnt
eine Similarity-Sicht für einen begrenzten Batch, behält Current-Exact-Pins und
Publication-Guard, berechnet Chunk-Pläne in erlaubter Parallelität und sammelt
Ergebnisse wieder nach Eingabeordinal. Generationen, Byte-Budgets und
Retirement-Reihenfolge dürfen nicht pro Worker umgangen werden.

Base-I/O sollte dabei eine eigene begrenzte Phase bleiben: viele blockierende
Reads im gemeinsamen CPU-Pool würden die freigewordenen Worker wieder belegen.
Planungszeit für Fingerprint, Lookup, Trial, Base-I/O und Warteschlangen sollte
getrennt erfasst werden. Mehr OS-Threads allein beheben diese Struktur nicht.

## 3. Kompaktion findet ihre Buckets zweimal

`SimilarityBuckets::next` in `similarity_index_repository.rs:1829` liest
Bucket-Seiten sequenziell und kennt die aktuelle Referenz. Statt deren
Entry-Ordinals direkt zu verwenden, ruft es bei jedem neuen Key erneut
`bucket_entries(key)` auf (`:1879`). Diese Methode startet über `read_bucket`
wieder eine binäre Seitensuche. Die sequenzielle Enumeration liest zudem mit
`storage.read_exact_at`, obwohl der Run im Normalfall bereits gemappt ist.

Der Challenger behält Referenzen bis zum Key-Wechsel, verwendet direkt ihre
Ordinals und führt einen Bucket über Seitengrenzen hinweg fort. Bestehende
Page-Decoder und Entry-Prüfungen bleiben aktiv. Im Benchmark werden die
vollständigen Bucket-/Entry-Ergebnisse beider Wege verglichen.

16.384 Einträge mit vier Referenzen je Eintrag und vier Vertretern je Bucket:

| Lauf | Wiederholte Suche, ms | Fortlaufender Cursor, ms | Faktor |
| --- | ---: | ---: | ---: |
| 3 | 30,200 | 18,117 | 1,667× |
| 4 | 29,260 | 16,486 | 1,775× |

Die produktive Umsetzung sollte weiterhin nur einen Bucket halten. Dass der
Vergleichsharness zum Gleichheitsvergleich beide vollständigen Ergebnislisten
sammelt, ist keine vorgeschlagene produktive Speicherstrategie.

## 4. Punktabfragen brauchen keine vollständigen Zwischen-Seiten

`read_bucket` (`similarity_index_repository.rs:1274`) dekodiert oder lädt für
jeden Schritt der binären Suche eine komplette Bucket-Seite, nur um deren
letzten Key zu prüfen. Selbst ein Cache-Hit bedeutet Mutex, Arc-Clone,
Druckprüfung und atomare Zähler. Bei einem Miss werden CRC, Einträge und
Geometrie erneut geprüft und ein Seitenobjekt alloziert.

Während des ohnehin vollständigen Immutable-Run-Audits kann ein kompaktes
Verzeichnis der letzten Keys je Bucket-Seite entstehen. Dann wählt eine
binäre Suche darin die erste passende Seite; nur tatsächlich benötigte
Seiten durchlaufen den bestehenden Read-/Decode-Pfad. Das entspricht dem
bereits verwendeten Grundgedanken der Exact-Seitengrenzen.

8.192 gemischte positive/negative Punktabfragen auf derselben Fixture:

| Lauf | Bisher, ms | Seitengrenzen, ms | Faktor |
| --- | ---: | ---: | ---: |
| 3 | 18,378 | 5,532 | 3,322× |
| 4 | 16,406 | 4,806 | 3,413× |

Die Grenzen benötigen hier **6.288 Bytes** zusätzlich. Aufbau und Audit sind
nicht Teil der Query-Zeit. Diese Metadaten müssen dem Index-Memory-Governor
unterliegen und bei fehlendem Budget einen Fallback behalten; mit großen Pools
ist auch eine kleine Pro-Seite-Allokation nicht automatisch unbeschränkt
zulässig. Der Pointer auf einen bereits geprüften Run ist keine Erlaubnis,
Recovery-/Scrub-Prüfungen auf beliebigen neuen Bytes auszulassen.

Die SMB-Telemetrie zeigt 5.613.888 Similarity-Page-Hits und 596.654 Misses.
Diese gemeinsamen Zähler enthalten **Queries und Publication/Kompaktion**;
sie dürfen nicht als Seitenzahl je Vordergrund-Query interpretiert werden.

## 5. Dynamische Arbeit innerhalb fester Worker-Grenzen

`container.rs:1880` erzeugt genau N parallele Worker-Jobs. Jeder bearbeitet die
festen Ordinals `worker, worker+N, ...`. Rayon kann diese großen Jobs stehlen,
aber ihre unterschiedlich teuren einzelnen Regionen nicht neu verteilen.

Ein Prototyp verwendet dieselben acht Worker und dieselbe Encode-Funktion,
vergibt die nächste Region jedoch über einen gemeinsamen atomaren Cursor.
Die Ausgabe wird nach Ordinal eingesammelt. Auf 128 Prehashed-Encode-Jobs à
256 KiB mit periodisch unterschiedlich teuren Eingaben ergibt das 1,410× und
1,466×. Auch homogene Eingaben profitierten in den beiden lokalen Läufen;
diese Werte schwanken stärker und sind kein Beleg für unvermeidbaren Gewinn
bei jedem Container. Der finale Harness prüft jede Job-Ordinal und ihre
Payloadgröße gegen die serielle Berechnung.

Die Produktionsänderung muss bei N erteilten Permits bleiben. Ein unbeschränktes
`regions.par_iter()` im globalen Pool würde die bestehende Admission umgehen.
Kleine Regionen können in kurzen dynamischen Paketen vergeben werden, um den
Atomic- und Scheduling-Aufwand zu begrenzen. Auch die statischen Hash-Shards
in `checkpoint.rs:1721` sollten erst gegen bytegewichtete oder dynamische
Pakete gemessen werden; Chunkanzahl ist kein zuverlässiger Kostenmaßstab.

## 6. Read-I/O: open/stat pro Range vermeiden

`fastdup-io-uring/src/lib.rs:295` öffnet für jeden Range-Read die Datei und liest
erneut ihre Länge. Der normale FS-Adapter tut dasselbe. Ein bereits geprüfter
Container-Descriptor erspart diese Syscalls momentan nicht.

Ein sicherer Vergleich auf warmen ISO-Daten, identischer wiederverwendeter
Ausgabepuffer, jeweils `open + metadata + pread` gegen gehaltenen FD und bekannte
immutable Länge, ergibt:

| Readgröße | Lauf 3 | Lauf 4 |
| --- | ---: | ---: |
| 4 KiB | 5,588× | 5,731× |
| 64 KiB | 1,711× | 1,706× |
| 1 MiB | 1,095× | 1,003× |

Das ist der Dateizugriff ohne io_uring-Kanal, Record-Prüfung oder FUSE. Besonders
für große Stream-Reads ist daraus wenig sicherer Gesamtgewinn abzuleiten.
Ein FD-Cache muss begrenzt, an immutable Container-Identitäten gebunden und in
RETIRING/GC invalidiert werden. Dauerhaft offene gelöschte Dateien würden
sonst physische Freigabe verzögern. Bestehende Generation-Leases und
Dateideskriptor-Grenzen sind Teil des Designs, keine nachträglichen Details.

## 7. Read-Granularität und Parallelität

Ein Zstd-Record darf aktuell 512 KiB dekodierte Daten halten
(`container.rs:18`). `decode_owned_candidate_payloads` dekodiert den gesamten
Record und prüft alle enthaltenen Chunk-Identitäten. Ein kleiner Ausschnitt
kann daher erhebliche Decode- und Hash-Arbeit auslösen. Das ist eine Folge der
gewählten Integritäts-/Kompressionsgrenze, keine unnötige zweite CRC-Prüfung.

Die Fixture enthält 512 unterschiedliche 64-KiB-Chunks, deren zweite Hälfte
jeweils die erste wiederholt. Dieselben Chunks wurden einmal zu acht pro
512-KiB-Zstd-Record und einmal einzeln kodiert. Beide Bilder wurden vollständig
verifiziert; die angeforderten Chunk-Bytes sind gleich.

| Geometrie | Containerbytes |
| --- | ---: |
| 512-KiB-Records | 16.924.672 |
| 64-KiB-Records | 16.986.112 |

64 einzelne Chunk-Reads, je einer aus jedem großen Record, ohne Payload-Cache:
**8,081× / 8,256×** schneller mit den kleinen Records. Das Containerbild wächst
in dieser Fixture um **0,363 %**. Andere Inhalte können bei kleinerem
Zstd-Fenster deutlich mehr Kompressionsleistung verlieren. Dieser Versuch ist
kein Argument, alle Workloads pauschal auf 64 KiB umzustellen.

Das bestehende Format kann beide Formen bereits lesen und schreiben. Zuerst
lohnt daher eine workloadabhängige Gruppierung beziehungsweise ein 64-/128-/
256-/512-KiB-A/B auf realen Restore-Profilen.

Zusätzlich ist der Decode der Records im Store-Read-Plan derzeit seriell
(`lib.rs:2980–3048`). Das isolierte Dekodieren von 64 großen unabhängigen Records
skaliert von einem auf zehn Threads um **3,70× / 4,82×**. Die Fixture umfasst
32 MiB dekodierte Daten; das ist ausdrücklich größer als ein heutiger
1-MiB-Range-Batch. Eine Umsetzung braucht wenige begrenzte Read-/Decode-Slots,
Größenschwellen gegen Scheduling-Overhead und gemeinsame CPU-/Speicherbudgets.
Physische HDD-Reihenfolge und Singleflight-Fehlerfreigabe bleiben verbindlich.

## 8. Fragment- und Codec-Gruppierung korrigieren

In `checkpoint.rs:3951–3970` wird ein fragmentierter Target einmal materialisiert.
Falls es keinen Candidate gibt, erzwingt `Cow::Owned` einen einzelnen
vorbereiteten unabhängigen Record. Ein bereits zusammenhängender Target ohne
Candidate landet dagegen in der normalen Region-Gruppierung. Die Form des
Receive-Buffers entscheidet damit über Kompressionsgrenze und CPU-Parallelität.

Zusätzlich sammelt der Writer gewöhnliche Regionen, vorbereitete unabhängige
Records und abhängige Records in getrennten Listen. `container.rs:1915–1931`
hängt diese Klassen nacheinander an. Dadurch kann die physische Reihenfolge
von der logischen Eingabereihenfolge abweichen.

Ein geordneter Plan mit Payload-Ownern könnte materialisierte Bytes behalten,
gewöhnliche Fallbacks gemeinsam gruppieren und Records nach ursprünglichem
Ordinal ausgeben. So würde die vermiedene zweite Materialisierung erhalten
bleiben. Das braucht kein neues Format, wohl aber Messungen von Compression,
Read-Coalescing und abhängigem Base-I/O. Es ist ein belegter Mechanismus, keine
bereits isolierte Erklärung der gesamten 13-MB-Differenz im vorherigen SMB-Paar.

## 9. Neue Similarity-Runs werden wieder vollständig gelesen

`stage_built_family` (`similarity_index_repository.rs:291`) auditiert die frisch
geschriebene Partition. `publish_buckets` aktiviert danach die Familie und ruft
`recover_generation` auf. Das anschließende Öffnen als Immutable Mapping führt
wieder das vollständige Hash-/Semantik-Audit aus (`similarity_mmap.rs:93`).

Der richtige Ansatz ist ein weiterreichbarer Owner der einmal vollständig
geprüften, veröffentlichten Partition. Ein mögliches Protokoll veröffentlicht
die immutable physische Datei, prüft sie vor dem Familien-/Head-Commit und
behält genau diese geprüfte Mapping-Instanz für die Aktivierung. Orphan-Dateien
nach Fehlern bleiben unselektiert. Das muss gegen die Publication-Regeln in
ADR 0076 und die Head-Fault-Matrix geprüft werden. Wiederanlauf und Offline-Scrub
prüfen weiter eigenständig. Es gibt hier noch keinen A/B-Nachweis für den
Gesamteffekt; vollständige Audits schlicht zu entfernen wäre kein akzeptabler
Challenger.

## 10. Weitere Zero-Copy-/Unsafe-Kandidaten

Die verbleibende FUSE-Kopie liegt in `assemble_manifest_read`
(`manifest_reader.rs:685`): Payloads werden in den Antwort-`Vec` kopiert.
`Bytes::from(data)` im FUSE-Adapter übernimmt diesen Vec bereits ohne weitere
Payloadkopie. Ein vollständig aus einem verifizierten Owner bestehender Read
könnte als Owner plus Range bis zur Antwort reichen. Gemischte DATA/HOLE/FILL-
Antworten benötigen eine begrenzte Segmentliste oder den bestehenden
Montage-Fallback. Eine native vectored Antwort müsste auch das derzeit einzelne
`ReplyData.data`-Bufferinterface erweitern.

`io_uring::Backend::read` (`lib.rs:595`) nullt den ganzen Read-Puffer vor dem
Kernel-Read. Ein Pufferpool oder ein eng gekapselter Spare-Capacity-Owner könnte
diesen Durchlauf vermeiden. Bei einer Unsafe-Variante darf initialisierte Länge
erst aus erfolgreichen CQEs entstehen; Short Reads, EOF, Fehler, Cancellation
und Drop müssen die Buffer-Lebenszeit korrekt begrenzen. Dafür liegt in diesem
Audit kein A/B vor, somit rechtfertigt er noch keine neue Unsafe-Integration.

Weitere handgeschriebene AVX2-/AVX-512-XOR-, POPCNT- oder `memcpy`-Kerne sind
gegenüber den gemessenen strukturellen Einsparungen nachrangig. Der Shingle-Hash
hat eine echte Abhängigkeit vom vorherigen Zustand; parallele Chunks und
Blockschleifen sind zunächst besser belegte Hebel als breitere Register.

## Containerformat: welche Änderung wäre begründet?

Zwei Richtungen sind technisch plausibel, aber noch keine fertigen
Formatvorschläge:

1. **Anspringbare Zstd-Teilbereiche.** Ein versionierter Record-Typ könnte eine
   geschützte Teilbereichstabelle und unabhängig prüfbare Frames enthalten.
   Kleine Reads müssten dann nur die benötigten Frames dekodieren. Die
   logische Chunk-ID bleibt BLAKE3 über den ganzen Chunk; ein Teil-Read darf
   diese Prüfung nicht unbegründet ersetzen. Frame-/Chunk-Grenzen,
   gegebenenfalls zusätzliche kryptografische Teilbereichsnachweise und deren
   Metadatenkosten gehören zum Design. Gegen kleinere bestehende Records muss
   ein solcher Typ erst gewinnen.
2. **Kompaktere Record-/Chunk-Verzeichnisse.** V3 verwendet 128 Bytes
   Record-Header, 64 Bytes je Chunk-Tabelleneintrag und 128 Bytes je Recovery-
   Index-Eintrag (`container.rs:14–47`). Bei einem RAW-Einzelchunk sind das
   320 Bytes Struktur vor Alignment und Container-Envelope: etwa 0,49 % bei
   64 KiB, aber viel bei winzigen abhängigen Payloads. Ein Record-Verzeichnis
   mit kompakten Chunk-Verweisen könnte wiederholte Koordinaten reduzieren.
   Eine ausgelagerte Directory kann kalten Reads zusätzliche Metadaten-I/O
   aufzwingen; genau dieser Tradeoff muss gemessen werden.

Beide Varianten brauchen eine explizite neue Formatkennung, feldweise
Serialisierung sowie gepaarte Writer-/Reader-/Recovery-/Scrub-Prüfungen und
Fault Injection. Alte Container dürfen nicht still neu interpretiert werden.
Ein Formatbruch ist für die priorisierten CPU-/Index-/Threading-Gewinne nicht
nötig; für eine allgemeine Formatmigration reicht dieser Audit noch nicht als
Performance-Nachweis.

## Einordnung, Nachweise und nächster Umsetzungsschnitt

Das letzte SMB-Paar mit identischen ISOs ist kein Grund, Similarity generell
abzuschalten. Der inzwischen im Repository dokumentierte
[50-Versionen-Linux-Lauf](../benchmarks/linux-6.12-online-similarity-2026-09-05.md)
zeigt auf verwandten Versionen 23,84:1 statt 6,59:1 Repository-Reduction, bei
niedrigerem Durchsatz. Die gefundenen Einsparungen zielen darauf, diese Funktion
billiger zu machen und die Read-Kosten sichtbar zu halten.

Empfohlene Reihenfolge: Block-Fingerprint, fortlaufender Kompaktionscursor und
budgetierte Seitengrenzen zuerst; anschließend begrenzte parallele Planung und
dynamische Jobverteilung. Danach FD-Wiederverwendung, Read-Pipeline und
Gruppierung nach Workload. Ein Format-Prototyp sollte mit einem klaren
Random-Read-/Metadatenziel separat antreten.

Messumgebung: dieselbe VM mit zehn vCPUs wie beim SMB-Vergleich, Releaseprofil
mit Overflow-Checks und gepinnten Abhängigkeiten. Hardware-PMU-Ereignisse
`cycles` und `instructions` sind hier nicht unterstützt; es werden keine
Hardware-Cache-Miss- oder Lock-Contention-Profilwerte behauptet. CPU-Zeit aus
der bestehenden Daemon-Telemetrie und neue Wall-Time-A/Bs sind getrennt.

`run-3.txt` und `run-4.txt` sind die in diesem Bericht verwendeten Messungen;
`run-1.txt`/`run-2.txt` dokumentieren Vorversuche. Pro A/B neun Samples für
Fingerprint/Index und elf für FD/Record/Scheduling, Reihenfolge alternierend.
Thread-Skalierung nutzt neun beziehungsweise elf Wiederholungen je Threadzahl.
Bei Run 4 prüft der Scheduling-Harness zusätzlich alle geordneten Job-Ergebnisse;
diese Prüfung liegt außerhalb der Zeitmessung. Die breite Produktions-Testsuite
wurde ohne Produktionsänderung nicht erneut gefahren.

Reproduktion aus dem vorbereiteten workspace-lokalen Harness:

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
cargo build --release --offline \
  --manifest-path /source/fastdup/.artifacts/hotpath-audit2-20260905/Cargo.toml
/source/fastdup/.artifacts/target/release/hotpath-audit2 all
```

Einzelmodi: `fingerprint`, `index`, `scheduler`, `fd`, `record`.
`source-hashes.json` bindet Produktionsstand und Prototypquellen;
`fingerprint-block64.patch` und `index-prototypes.patch` zeigen die isolierten
Ergänzungen. `src/main.rs` enthält Fixtures, A/B-Reihenfolge, Vergleiche und
Thread-Benchmarks. Es wurde kein neues Unsafe-Kernelstück benötigt.
