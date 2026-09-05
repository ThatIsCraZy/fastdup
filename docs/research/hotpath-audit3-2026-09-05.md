# Dritter Hotpath-Audit nach der zweiten Umsetzung

Stand: 2026-09-05, aktueller Arbeitsbaum einschließlich der
[zweiten Umsetzung und SMB-Messung](../benchmarks/hotpath-implementation2-2026-09-05.md).
Dieser Audit untersucht die danach verbleibende Arbeit. Seine Prototypen
liegen isoliert unter `.artifacts/hotpath-audit3-20260905/`; die folgenden
Challenger sind noch nicht in den Produktionspfad übernommen.

Die nächste kleine Parallelitätskorrektur ist besonders konkret: Die neue
Batch-Admission reserviert bei wenigen Jobs zu viele Worker. Daneben erlaubt
das unveränderte Fingerprint-Profil kleinere SIMD-Vote-Zähler. Zwei weitere
Chancen betreffen die weiterhin serielle Materialisierung und den
Containeraufbau. Ein Formatwechsel ist für diese Schritte nicht erforderlich.

## Priorisierung und Evidenz

| Priorität | Nächster Schritt | Nachweis | Status |
| --- | --- | --- | --- |
| 1 | CPU-Permits auf die tatsächliche Jobzahl begrenzen | Ein Job hält im aktuellen Helfer zehn Permits; isolierter Probe-Aufruf zeigt 0 statt 9 freie Permits | Konkreter Strukturverlust, noch kein End-to-End-A/B |
| 1 | Fingerprint-Votes von i32 auf i16 verkleinern | 1.795 Gleichheitsfälle; drei alternierende A/Bs ergeben 1,050–1,077× | Gemessener SIMD-Prototyp |
| 2 | Container im adaptiven Writer ohne vorheriges Vollnullen aufbauen | Vollnullung vor bereits initialisiertem Payload plus zusätzliche Header-/Padding-Nullung im RAW-Writer | Code-Nachweis, Gewinn noch ungemessen |
| 2 | Region-Materialisierung innerhalb des CPU-Budgets parallelisieren | Materialisierung bleibt eine serielle Schleife vor den parallelen Planungs-/Encode-Phasen | Code-Nachweis, Gewinn noch ungemessen |
| 2 | Gemeinsame Read-Owner über benachbarte DATA-Extents nutzen | Der neue Owner-Reply greift nur bei genau einer Extent; mehrere DATA-Extents landen weiterhin im Ausgabe-Vec | Code-Nachweis, Nutzen abhängig von Read-Geometrie |
| 2 | Rootweite FD-/Mutationssperre feiner aufteilen | Cache-Hits und Mutationen teilen einen Mutex; manche Backend-Aktionen laufen unter dieser Sperre | Konkurrenzpfad belegt, Wartezeit noch nicht gemessen |
| 3 | Cache-Admission-Gruppen ohne quadratische Ownersuche zusammenführen | Lineares `.find` je neuer Gruppe; bei n verschiedenen Ownern n(n−1)/2 Vergleiche | Komplexität belegt, reale Kosten noch ungemessen |

Die Faktoren beschreiben ausschließlich den isolierten Fingerprint-Aufruf.
Sie sind keine SMB-Prognose. Für die übrigen Punkte wird bewusst kein
erfundener Beschleunigungsfaktor angegeben.

## 1. Kleine Trial-Wellen reservieren ungenutzte CPU-Worker

`crates/fastdup-store/src/persistent_reduction.rs:801`, `map_admitted`,
nimmt zuerst `admission.acquire(desired)`. Erst danach begrenzt die
Parallel-Schleife ihre Jobanzahl auf `min(lease.workers(), inputs.len())`.
Bei einem Input, zehn gewünschten Workern und zehn verfügbaren Permits
führt ein Worker Arbeit aus, während neun weitere Permits bis zum Ende
des Aufrufs ungenutzt gebunden bleiben. Das kann Hashing und Demand-Decodes
ausbremsen. Kleine Base-Trial-Wellen sind dafür ein plausibler konkreter
Auslöser; im letzten Advanced-SMB gab es insgesamt nur 73 Base-Reads.

Der isolierte Harness ruft den **kopierten aktuellen Helfer** mit einem Job
auf und liest innerhalb der Closure `admission.available()`:

```text
single_job_permits current_available=0 bounded_available=9 total=10 active_jobs=1
```

Der Challenger begrenzt die Anfrage bereits vor `acquire` auf
`min(desired, inputs.len())`. Die Probe belegt die unnötige Reservierung,
keinen gemessenen SMB-Verlust. Vor Integration sollte ein konkurrierender
Hash-/Trial-Benchmark den Durchsatz- und Latenzeffekt prüfen. Null Inputs,
partielle Grants und unveränderte geordnete Ergebnisse gehören zur Prüfung.

Das ist ein neu sichtbarer Restpunkt des allgemeinen Batch-Helfers aus der
zweiten Umsetzung; die dynamischen Encoder-/Hash-Worker sind davon getrennt.

## 2. Derselbe Fingerprint mit 16-Bit-Votes

`crates/fastdup-store/src/reduction_similarity.rs:166` hält 512 i32-Zähler,
also 2.048 Bytes. Das v1-Profil begrenzt logische Chunks auf 256 KiB und
committet einen Minimizer je höchstens 64 Shingles. Damit fallen auch beim
größten Chunk höchstens **4.096 Votes pro Zähler** an. i16 reicht für den
Wertebereich −4.096 bis +4.096 vollständig aus.

Der Prototyp übernimmt den gesamten aktuellen Fingerprint-Ablauf und
verkleinert ausschließlich Vote-Zähler und Delta-Tabelle. Die Tabelle
schrumpft von 8 auf 4 KiB, der Akkumulator von 2 auf 1 KiB. AVX2 addiert
16 i16-Lanes statt acht i32-Lanes. Je zwei aus einem Byte indizierte
16-Byte-Tabellenzeilen werden zu einer 256-Bit-Delta-Lane kombiniert.
Rolling Hash, Minimizer-Grenzen, Seeds, Superfeatures und Vorzeichenregel
beim Sketch-Abschluss bleiben unverändert.

32 MiB aus dem Rocky-ISO ab Offset 256 MiB, in 512 Chunks à 64 KiB;
je Lauf elf A/B-Samples mit alternierender Reihenfolge:

| Lauf | Aktuell i32, ms | Challenger i16, ms | Faktor |
| --- | ---: | ---: | ---: |
| 1 | 52,622 | 49,366 | 1,066× |
| 2 | 53,332 | 50,805 | 1,050× |
| 3 | 53,730 | 49,886 | 1,077× |

Alle Läufe vergleichen 255 kurze Längen, 1.024 verschieden lange und
positionierte ISO-Ausschnitte, vier konstante Maximalchunks und alle
512 Batch-Chunks. Superfeatures und sämtliche Sketch-Wörter stimmen
in allen 1.795 Fällen mit der aktuellen Implementierung überein.
Die kurzen/konstanten Fälle decken auch Randbereiche ohne volle Shingles
und maximal gleichgerichtete Votes ab.

Die schmalere SIMD-Routine bleibt ein isolierter Unsafe-Prototyp. Vor
produktiver Übernahme braucht sie zusätzlich die vollständige skalare
Fallback-Integration und deren Orakeltests. Die Safety-Grenze muss AVX2-
Dispatch, exakt 512 Zähler, feste Tabellenindizes und den v1-Maximalwert
explizit festhalten. Eine spätere Erhöhung des maximalen Chunks darf diese
Zählerbreite nicht stillschweigend übernehmen.

Rohdaten: `fingerprint-run1.txt`, `fingerprint-run2.txt`,
`fingerprint-run3-permits.txt` im Audit-Artefaktverzeichnis.
Harness: `src/main.rs`; gekapselte Prototyp-Kerne in
`store/src/reduction_similarity.rs` und `store/src/similarity_simd.rs`.
Die Implementierungskopie wird als `fastdup-store-audit3` gebaut; sie ist
keine Abhängigkeit des Produktions-Workspaces.

## 3. Der adaptive Container-Writer nullt sein vollständiges Ergebnis vorab

`crates/fastdup-format/src/container.rs:6503` allokiert über
`AlignedContainerBytes::zeroed(file_length)` einen vollständig genullten
Container. Danach schreibt jeder `AdaptiveRecordPlan` Header, Tabelle,
Payload und Padding hinein. RAW nullt seine Header-/Padding-Bereiche sogar
erneut in `encode_prehashed_raw_record_into`; vorbereitete Records kopieren
bereits vollständig initialisierte Bytes über den gesamten Zielbereich.

Für andere Writer existiert mit `AlignedContainerBuilder` bereits eine
sichere Strategie: Kapazität reservieren, Alignment-Präfix und wirkliche
Lücken initialisieren, vorhandene Bytes anhängen. Den adaptiven Writer
entsprechend zu erweitern kann den zusätzlichen Speicherdurchlauf vermeiden.
Für RAW/Zstd müssten Header und Chunk-Tabelle feldweise erzeugt, Payloads
angehängt und CRC-Felder erst nach Fertigstellung gepatcht werden. Dabei
soll kein temporärer kompletter Record eine neue gleich große Kopie erzeugen.

Vor einer Änderung: exakte Container-Bytegleichheit, Alignment, Padding,
Commitment, Recovery und Scrub prüfen; anschließend RAW- und gemischte
Container in warmem sowie frischem Allokationszustand messen. `vec![0; n]`
kann von Betriebssystem-/Allocator-Effekten profitieren, weshalb aus der
gezählten Nullung allein kein fixer Laufzeitgewinn folgt.

## 4. Materialisierung ist weiterhin eine serielle CPU-Phase

`crates/fastdup-appliance/src/checkpoint.rs:1385`,
`prepare_compression_regions`, legt zunächst Regionpläne an und materialisiert
sie anschließend nacheinander. Das beseitigt die vorherige doppelte
Materialisierung fragmentierter Advanced-Ziele, liegt aber weiterhin vor
der begrenzten parallelen Arbeit. Unabhängige Regionen könnten unter
demselben Worker-/Speicherbudget materialisiert und nach Ordinal gesammelt
werden. Reine Views sollten dabei keine unnötigen Jobs auslösen.

Die aktuellen Normal-SMB-Zähler melden ungefähr 879 MB
`compression_region_materialization_bytes`, zuvor ungefähr 246 MB.
Korrektur nach Codeprüfung bei der Umsetzung: `collect_prehashed_decoded`
im Format bucht seine Konkatenation bereits in denselben Zähler. Die im
ursprünglichen Audit vermutete fehlende Buchung besteht nicht. Die höhere
Zahl dieser Kopierklasse ist real; sie erfasst jedoch nicht sämtliche
Bytekopien des Prozesses. Ein zusätzlicher Teilzähler und eine getrennte
Vorbereitungszeit erlauben nun, Format-Konkatenation und die vorgezogene
Region-Materialisierung auseinanderzuhalten und deren Parallelisierung zu
messen.

## 5. Zero-Copy-Antwort über mehrere Extents erweitern

`crates/fastdup-store/src/manifest_reader.rs:713` nutzt den Owner-Reply nur
bei `if let [located] = extents`. Bei mehreren DATA-Extents werden die
verifizierten Payloads weiterhin in ein neues Vec kopiert. Das ist auch
dann der Fall, wenn benachbarte Chunks aus einem gemeinsam dekodierten
Zstd-Record stammen und ihre Bytebereiche direkt aneinandergrenzen.

Ein geeigneter nächster Baustein ist ein geprüfter Read-View mit gemeinsamem
Owner und Range. Er darf ausschließlich aneinandergrenzende, bereits
verifizierte Bereiche desselben Owners zusammenfassen. Ein solcher View
repräsentiert einen Dateiausschnitt; ihm darf keine erfundene Chunk-ID
zugewiesen werden. RAW-Batches können Header-/Padding-Lücken zwischen den
Payloads enthalten und erfüllen diese Bedingung häufig nicht.

Gemischte DATA/HOLE/FILL-Antworten würden für vollständiges Scatter/Gather
zusätzlich eine passende FUSE-Reply-Schnittstelle benötigen. Erst ein
Read-Benchmark mit realen Extent-Grenzen zeigt, wie häufig der einfachere
gemeinsame-Owner-Pfad tatsächlich greift.

## 6. FD-Cache und Mutationen teilen die rootweite Sperre

`crates/fastdup-store/src/lib.rs:3866`, `open_read_range`, hält die
`immutable_leases`-Sperre auch beim Cache-Hit und beim langsamen Open/Stat
eines Misses. `with_file_mutation` und `with_file_rename` halten dieselbe
Sperre während ihrer Backend-Closure. Beim entsprechenden io_uring-Pfad
wartet diese Closure auf die Backend-Antwort. Eine langsame Operation kann
damit auch Reads anderer Container dieses Roots aufhalten.

Der Hauptpfad für Owned-Container-Publication verwendet eine eigene
Operation; daraus darf keine Behauptung abgeleitet werden, alle Writes seien
nun durch diesen Mutex serialisiert. Zunächst müssen Hold-/Wait-Zeiten pro
Aufrufer gemessen werden, insbesondere bei gleichzeitigen Reads, GC und
Mutationen. Ein anschließender Umbau könnte dateibezogene Mutations-Tokens
und getrennte Cache-Shards einsetzen. Die bisher atomare Beziehung zwischen
Mapping-Lease, FD-Invalidierung und Mutation muss dabei erhalten bleiben.

## 7. Cache-Gruppierung hat noch eine quadratische Suche

`crates/fastdup-store/src/read_cache.rs:217`, `VerifiedChunkRead::new`, sucht
für jede Admission-Gruppe linear nach einem bereits vorhandenen Owner.
Für n verschiedene Owner entstehen n(n−1)/2 Pointervergleiche. Ein
temporärer Index nach tatsächlicher Backing-Identität könnte das für große
Batches vermeiden. Der Payload-Anfangspointer reicht dafür nicht: Zwei
verschiedene Chunk-Views desselben Owners beginnen an unterschiedlichen
Adressen.

Die Read-Batches sind begrenzt, und für wenige Gruppen kann der heutige
Vec-Scan günstiger sein als eine zusätzliche HashMap-Allokation. Deshalb
vorerst Priorität 3: 1/4/16/64/128 Gruppen mit vielen beziehungsweise keinen
gemeinsamen Ownern messen, dann gegebenenfalls erst oberhalb einer
ermittelten Schwelle indizieren.

## Containerformat

Die zweite Umsetzung hat mit realen SeqCDC-Chunks den Tradeoff bereits
quantifiziert: Beim Tar-Ausschnitt sind 64-KiB-Records für kleine Reads
6,38× schneller, benötigen aber 9,27 % mehr Platz; beim RAW-dominierten
ISO ändern sie beides praktisch nicht. Dafür genügt das aktuelle Format.
Ein späteres Format mit unabhängig dekodierbaren Subframes und separaten
Integritätsgrenzen könnte den Zielkonflikt anders lösen, ist aber weiterhin
eine Designhypothese. Aus den jetzigen Daten folgt keine ausreichende
Begründung für eine globale Migration.
