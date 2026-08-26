# Bausteine für erweiterte Datenreduktion

Stand: 2026-08-25. Diese Notiz bewertet fertige Rust- und C-Bausteine für
Similarity Search, Delta, Zstd-Dictionaries, SIMD und Byte-Puffer. Der erste
Prototyp weist Zstd Prefix inzwischen die Container-Codec-ID 3 zu. Der
poolweite Similarity-Zustand hat außerdem ein feldweise serialisiertes,
rebuildbares Snapshot-Format mit 4-KiB-Seiten und Streaming-Recovery.

## Entscheidung

| Bereich | Empfehlung | Begründung |
| --- | --- | --- |
| Similarity Index | **Implemented prototype:** `reduction_similarity.rs` plus `similarity_index_repository.rs` | Deterministische Min-Hash-Repräsentanten, höchstens 256 untersuchte IDs, 16 Ergebnisse, vollständiger NVMe-Snapshot und seitenweiser Neustart-Rebuild. |
| Delta | **Implemented prototype:** Zstd Reference Prefix gegen vorhandenes `SparseXorDelta` | Prefix ist in der bereits eingebundenen Zstd-Version vorhanden, referenziert genau eine Base und passt damit zu Tiefe 1 aus ADR 0010. [Zstd API][zstd-api] |
| Dictionary | **Keep:** `zstd::dict`, später vorbereitete `CDict` und `DDict` | Training, Bulk-Codec und sichere Wrapper sind bereits transitiv vorhanden. Es braucht keine zweite Dictionary-Bibliothek. [zstd-rs Dictionary-Quelle][zstd-dict] |
| Hashing | **Keep:** BLAKE3 1.8.6 | BLAKE3 bringt SSE2, SSE4.1, AVX2, AVX-512, NEON, WASM und x86-Runtime-Dispatch bereits mit. [BLAKE3 1.8.6][blake3] |
| Byte-Eigentum | **Keep und in `fastdup-store` nutzen:** `bytes::Bytes` | Der Workspace nutzt `Bytes` schon für `MutationPayload`; Slices teilen das Backing in O(1). [Bytes 1.12.1][bytes-source] |
| Xdelta und qbsdiff | **Prototype only** | Beide taugen als Vergleich für Delta-Qualität. Ein dauerhafter Decoder würde neue Format-, FFI- oder Codec-Abhängigkeiten schaffen. |
| HNSW und USearch | **Reject** | Ein approximativer, einfügungsabhängiger Graph bildet die feste Bucket-Semantik aus ADR 0018 nicht ab. [hnsw_rs][hnsw-rs] [USearch][usearch-rust] |
| SIMD-Helfer | **Measure first** | 64 Byte Sketch-Daten sind zu klein, um eine neue Dispatch-Abhängigkeit ohne Profil zu rechtfertigen. Fingerprint-Votes und Delta-Scan sind die besseren Kandidaten. |

Der erste belastbare Vergleich ist damit klein: `SparseXorDelta` gegen
`ZSTD_PREFIX`, unter derselben Candidate-, Trial- und Kostenpolitik. Xdelta
und qbsdiff liefern nur eine obere Vergleichslinie für mögliche Einsparung.

## Vorhandene Module und Versionen

Das Workspace-Manifest und `Cargo.lock` enthalten bereits:

- `zstd` 0.13.3, `zstd-safe` 7.2.4 und Zstd C 1.5.7;
- `blake3` 1.8.6 mit Rayon;
- `bytes` 1.12.1 im Lockfile;
- den gemeinsamen Rayon-Pool aus ADR 0050.

Der bestehende Reduction-Code hat schon die richtigen Grundformen:

- `SimilarityFingerprint::v1` ist die skalare Referenz für vier
  Superfeatures und einen 512-Bit-Sketch.
- `SimilarityIndex` hält nur Einträge, die noch von mindestens einem
  64er-Bucket referenziert werden, und führt vier sortierte Buckets ohne
  temporäre ID-Menge zusammen.
- `SimilarityIndexRun` speichert den vollständigen poolweiten
  Fingerprint-Strom feldweise. Recovery und Offline Scrub lesen jeweils nur
  eine 4-KiB-Seite und prüfen zusätzlich den vollständigen BLAKE3-Run-Hash.
- `SparseXorDelta` erzeugt kanonische, nicht überlappende XOR-Runs gegen genau
  einen unabhängig dekodierbaren Base Chunk.
- `WorkerCodec` hält je Reduction-Worker wiederverwendete Zstd-Kontexte und
  setzt `NbWorkers(0)`. Zstd erzeugt also keinen zweiten Worker-Pool.
- `ReductionDictionary` begrenzt Dictionary und Trainingsmenge und
  identifiziert die exakten Bytes mit BLAKE3.

## Similarity und Cache-Lines

### Kein allgemeiner ANN-Index

`hnsw_rs` 0.3.4 ist ein Pure-Rust-HNSW mit Hamming-Distanz, parallelem
Einfügen und optionalen SIMD-Backends. Ebenen werden zufällig gewählt und der
Graph hängt von der Einfügungsfolge ab. Das portable SIMD-Feature benötigt
Nightly. Lizenz: MIT oder Apache-2.0. [Repository][hnsw-rs]

USearch 2.26.0 ist ein schneller nativer HNSW-Kern mit Rust-Wrapper,
SIMD-Distanzen und persistierbaren Indizes. Für fastdup bedeutet das C++-FFI,
einen approximativen Graphen und ein zweites Indexformat. [Rust API][usearch-rust]

Beide lösen ein größeres, aber anderes Problem. Fastdup rankt höchstens 256
vorselektierte 512-Bit-Sketches. Der eigene Index bleibt daher die bessere und
prüfbare Lösung.

### RAM-Layout

Der heutige Bucket liest zusammenhängende Chunk IDs und löst danach jeden
residenten Sketch über einen separaten `BTreeMap`-Lookup auf. Evizierte
Nicht-Repräsentanten bleiben nur im persistenten Snapshot. Der nächste
RAM-Prototyp sollte die residenten Einträge in eine dichte Arena verschieben:

```text
BucketKey -> sorted RepresentativeOrdinal[u32]

arena:
  ChunkId[ordinal]
  LogicalLength[ordinal]
  Superfeatures[ordinal][4]
  SketchWords[ordinal][8]
```

So schrumpft ein Bucket-Verweis von 32 auf 4 Byte. Sketches und IDs bleiben
linear lesbar. Die Ordinalnummer ist nur Beschleunigung, niemals Identität
oder Location.

Der größte lokale Fingerprint-Puffer ist derzeit `[i32; 512]`, also 2 KiB.
Bei höchstens 256 KiB Chunkgröße und einem Minimizer je 64 Shingles entstehen
höchstens 4.096 Minimizer. `[i16; 512]` reicht deshalb und halbiert den Puffer
auf 1 KiB. Die Umstellung braucht eine Compile-Time-Assertion:

```text
MAX_MINIMIZERS_V1 <= i16::MAX
```

Dieser Beweis gehört zum Profil und darf bei größeren Chunks nicht still
weitergelten.

64-Byte-Alignment ist sinnvoll für gemeinsam aktualisierte Worker-Counter,
um False Sharing zu vermeiden. Es ist nicht sinnvoll, jeden Chunk ID oder
Fingerprint aufzublähen. RAM-Größe und Alignment dürfen per `const`-Assertion
gesichert werden. Dauerhafte Strukturen bleiben feldweise serialisiert.

## Delta-Bausteine

### Zstd Reference Prefix

Zstd 1.5.7 bietet `ZSTD_CCtx_refPrefix` und `ZSTD_DCtx_refPrefix`. Ein Prefix
gilt für genau den nächsten Frame, wird nicht kopiert und muss während der
Operation unverändert leben. Die Dekompression benötigt dieselben Prefix-
Bytes. Die offizielle API beschreibt den Ansatz als Diff plus Kompression.
[Zstd API][zstd-api]

`zstd-safe` 7.2.4 kapselt beide Funktionen sicher. Der vorhandene `zstd`-Crate
exportiert `zstd_safe`, daher braucht fastdup weder eine direkte `zstd-sys`-
Abhängigkeit noch eigenen Unsafe-Code. [Wrapper-Quelle][zstd-safe]

Der codec-3 `ZSTD_PREFIX`-Record serialisiert:

- Base Chunk ID und Base-Länge;
- Ziel-Chunk-ID und dekodierte Ziel-Länge;
- das versionierte Zstd-Writerprofil;
- den vollständigen Zstd-Frame.

Die Base muss unabhängig dekodierbar sein. Der Reader verifiziert die Base,
dekodiert in einen vorab begrenzten Zielpuffer und prüft danach Ziel-Länge und
BLAKE3. Zstd-Ausgabe ist keine dauerhafte Identität. Ein anderer Writer darf
einen anderen gültigen Frame erzeugen. Innerhalb eines gepinnten Builds muss
ADR 0050 weiterhin bytegleiche Containerbilder bei einem und mehreren Workern
fordern.

Das experimentelle `DeterministicRefPrefix`-Flag sollte nicht verwendet
werden. Die stabile single-threaded Prefix-API mit `NbWorkers(0)` reicht.

### Sparse XOR

`SparseXorDelta` bleibt wertvoll, weil es ein kleines kanonisches Format,
einfache Kostenrechnung und klare Bounds hat. Es gewinnt bei wenigen
Byteänderungen. Nach Einfügungen oder Verschiebungen wird sein XOR-Payload
schnell groß; hier hat Zstd Prefix die bessere Chance.

SIMD lohnt sich beim Erkennen langer gleicher Abschnitte. Der Scanner kann 32
oder 64 Byte vergleichen und gleiche Cache-Lines überspringen. Run-Grenzen und
Payload müssen danach exakt dieselbe kanonische Folge wie der skalare Oracle
ergeben. Sichere 8-Byte-Ladungen und `u64`-XOR sind der erste Benchmark, eigene
Intrinsics erst der zweite.

### Externe Delta-Module

| Modul | Fakten | Urteil |
| --- | --- | --- |
| Xdelta 3.2 | C-Library, Apache-2.0, VCDIFF nach RFC 3284. VCDIFF trennt das portable Decode-Format vom Encoder und verwendet ADD, COPY und RUN. [Repository][xdelta] [RFC 3284][vcdiff] | Guter Offline-Benchmark. Production würde eine neue C-FFI-, Build-, Fuzzing- und Lifetime-Grenze schaffen. |
| `qbsdiff` 1.4.4 | Rust-API für vollständige Source- und Target-Slices, Patcher schreibt über `Write`; eigener Crate verbietet Unsafe, Lizenz MIT. Nutzt Suffix Array, Rayon und bzip2. Byteidentische Ausgabe zu bsdiff wird ausdrücklich nicht zugesagt. [Quelle][qbsdiff-source] [Manifest][qbsdiff-manifest] | Benchmark only. bzip2 führt eine zweite Kompressionspolitik ein, der Encoder verspricht keine stabile Ausgabe. |
| `bidiff` main, 2.0-dev | paralleler Hash-Index, mmap oder Tempfile, eigener Rayon-Pfad und Zstd-Patch-Chunks; MIT oder Apache-2.0. [Repository][bidiff] | Für große Dateien interessant, aber kein fertiger Chunk-Hot-Path. Eigene Pools und Speicherpolitik passen nicht zu ADR 0050. |

Keines dieser Formate sollte dauerhaft lesbar gemacht werden, bevor ein
Corpus-Vergleich einen klaren Gewinn gegen Zstd Prefix und Sparse XOR zeigt.

## Dictionary-Training

`zstd::dict` 0.13.3 ist der Production-Baustein:

- `from_samples` kopiert alle Samples in einen zusammenhängenden Puffer;
- `from_sample_iterator` sammelt ebenfalls vollständig;
- `from_continuous` nimmt einen schon zusammenhängenden Puffer und eine
  Längenliste direkt;
- `EncoderDictionary::copy` und `DecoderDictionary::copy` erzeugen
  vorbereitete `CDict`- und `DDict`-Objekte.

Der exakte Source dokumentiert diese Semantik. [zstd-rs Dictionary-Quelle][zstd-dict]
Eine echte Streaming-Training-API gibt es dort nicht. Der Reservoir sollte
deshalb von Anfang an als begrenzter append-only Byte-Puffer plus Sample-
Längen gebaut werden. `from_continuous` vermeidet dann die zweite Sample-Kopie.
Sample-Auswahl und Reihenfolge müssen von stabilen IDs abhängen, nicht von
Worker-Fertigstellungszeiten.

Dictionary-Training darf über Zstd-Versionen andere Bytes liefern. Das ist
unproblematisch: Die vollständigen Bytes erhalten eine BLAKE3-ID, und jedes
Retraining erzeugt bei abweichendem Inhalt ein neues Dictionary Object.

Vorbereitete `CDict`- und `DDict`-Objekte vermeiden Tabellenaufbau bei jedem
Familienwechsel. `zstd-safe` markiert beide als `Send + Sync` und bietet
`sizeof()` für Memory Accounting. [zstd-safe Quelle][zstd-safe] Ein aktiver
Catalog-Eintrag sollte Rohbytes, vorbereitete Tabellen und deren gemessenen
Speicher vollständig berechnen. Die Tabellen sind verwerfbare
Beschleunigung. Nur die BLAKE3-geprüften Dictionary-Bytes sind dauerhaft.

Die experimentellen by-reference-Konstruktoren sparen eine kleine Kopie,
binden aber Rust- und C-Lifetimes. Die stabilen Copy-Konstruktoren sind der
bessere erste Schritt.

## SIMD

BLAKE3 muss nicht optimiert werden. Seine offizielle Rust-Implementierung
enthält bereits SIMD und x86-Runtime-Dispatch. [BLAKE3 1.8.6][blake3]

Für die 512-Bit-Distanz sind acht skalare `u64::count_ones()` der Oracle.
[`simd-popcnt` 1.0.0][simd-popcnt] kann POPCNT, AVX2, AVX-512, NEON und SVE zur
Laufzeit wählen. Es hat keine Abhängigkeiten, ist MIT oder Apache-2.0 und setzt
Rust 1.89 voraus. Version 1.0.0 erschien erst am 29. Juli 2026. Seine
Durchsatzbeispiele betreffen größere Arrays. Für 64 Byte kann Dispatch mehr
kosten als er spart.

[`pulp` 0.22.3][pulp] ist eine sichere allgemeine SIMD-Abstraktion mit
Runtime-Dispatch und MIT-Lizenz. Sein breiteres Dependency-Set lohnt sich nur,
wenn Fingerprint-Votes oder gebatchtes Ranking im Profil dominieren.
`std::simd` bleibt Nightly. [Portable SIMD][portable-simd]

Reihenfolge der Messung:

1. Fingerprint-Votes und Shingle-Hashing;
2. Sparse-XOR-Gleichlauf und Run-Erkennung;
3. Ranking im 256-Repräsentanten-Worst-Case;
4. erst dann `pulp`, `simd-popcnt` oder ein separater SIMD-Crate.

Dispatch erfolgt einmal pro Worker oder Batch, nie pro Cache-Line. CPU-
Features dürfen nur Laufzeit ändern, nicht Fingerprint, Ranking, Codec-Auswahl
oder Containerbytes.

## Zero-Copy

`Bytes` ist ein kleiner, klonbarer Eigentumsträger. Er belegt vier `usize`,
teilt Backing Storage und erzeugt Slices in O(1). `Bytes::from(Vec<u8>)` und
`Bytes::from(Box<[u8]>)` übernehmen vorhandene Allokationen. [Quelle][bytes-source]

Empfohlener Datenfluss:

```text
MutationPayload(Bytes)
  -> ChunkView(Bytes::slice)
  -> RegionView
  -> RAW hält View bis zum Layout
  -> Codec leiht &[u8]
```

Zstd Bulk und Prefix erwarten zusammenhängenden Input. Umfasst eine
Compression Region mehrere Mutation-Puffer, braucht sie genau eine
Koaleszierung in den workerlokalen 512-KiB-Scratch. Das ist eine bewusste,
begrenzte Kopie.

`compress_to_buffer` und `decompress_to_buffer` schreiben in Caller-owned
`Vec<u8>` oder Slices. [Bulk-Kompressor][zstd-bulk] Damit kann ein Worker
Scratch wiederverwenden und Output genau einmal erzeugen. Kompression ist
nicht Zero-Copy. Das Ziel ist: keine Input-Ownership-Kopie, ein Output-Puffer,
keine weitere Codec-Output-Kopie.

Sparse-XOR-Decode kopiert die Base direkt in den finalen Zielpuffer und wendet
Runs dort an. Der Puffer darf erst nach vollständiger Längen- und BLAKE3-
Prüfung in den Read Cache oder an den Aufrufer gelangen.

## Assertions und dauerhafte Grenzen

Fremde oder dauerhafte Bytes erzeugen normale Fehler, keine Assertions:

| Fall | Behandlung |
| --- | --- |
| unbekannter Codec oder Profil | Reader-Fehler |
| fehlendes oder falsch gehashtes Dictionary | Corruption, Location unbrauchbar |
| falsche Base-ID oder Base-Länge | Corruption |
| Delta-Run außerhalb des Ziels, Überlappung oder Payload-Lücke | Corruption |
| Decode-Länge über Profilmaximum | vor Allokation ablehnen |
| gleichzeitig ausgeliehener Worker-Kontext | production-fatal `ASSERT` |
| interner Candidate-, Trial-, Bucket- oder Inflight-Bound verletzt | production-fatal `ASSERT` plus Telemetrie |
| RAM-Layout oder `i16`-Vote-Beweis verletzt | Compile-Time-Assertion |

`debug_assert!` reicht für keine Durable-Invariante. Ein beschädigter
Container darf umgekehrt keine Prozess-Assertion auslösen.

Jeder SIMD-Pfad braucht den skalaren Oracle, Golden-Vektoren, unaligned Inputs,
Längen um jede Vektorbreite und dieselbe Ausgabe bei allen verfügbaren CPU-
Pfaden und Worker-Zahlen.

Jeder neue dauerhafte Record braucht dieselbe Invariante an Writer, normalem
Reader, Recovery und Offline Scrub. Fault Injection muss mindestens fehlende
oder falsche Base, fehlendes oder falsches Dictionary, abgeschnittenen Frame,
manipulierte Decode-Länge, Run-Überlauf und Crash zwischen Dictionary-
Publikation und erstem abhängigen Record abdecken. GC darf nie das letzte
erreichbare Dependency-Objekt entfernen.

## Empfohlene Modulgrenzen

```text
SimilarityProfile
  fingerprint(bytes) -> Fingerprint
  candidates(fingerprint, length, limit) -> bounded candidates

DeltaTrialCodec
  trial(verified_base, target, scratch) -> Trial
  decode(record, verified_base, output) -> VerifiedTarget

EncodingCostPolicy
  physical bytes + CPU/read/base/fanout cost -> winner

DurableRecordCodec
  field-by-field encode/decode for assigned codec ID
```

Similarity liefert nur Kandidaten. Sparse XOR und Zstd Prefix erzeugen nur
Trials. Keiner der Codecs entscheidet selbst über die 5-Prozent-plus-4-KiB-
Schwelle. Erst die gemeinsame, versionierte Kostenpolitik wählt einen Record.

## Reihenfolge

1. Den Similarity-Snapshot beim Exact-Index-Rebuild direkt aus verifizierten
   Container-Chunks erzeugen und generationengleich veröffentlichen.
2. Reduction-Input auf `Bytes`-Views und Caller-owned Codec-Scratch umstellen,
   ohne Formatänderung.
3. Den `i16`-Vote-Beweis und die dichte Similarity-Arena messen.
4. Sparse XOR und Zstd Prefix durch dieselbe Trial-Schnittstelle führen. Die
   Grenze von vier Trial Encodes bleibt bestehen.
5. Base-Liveness und abhängige Codec-3-Locations in GC und Scrub gemeinsam
   prüfen.
6. Dictionary-Reservoir und Catalog getrennt bauen. Training darf ordinary
   ingest nie blockieren.

## Quellen

[zstd-api]: https://facebook.github.io/zstd/doc/api_manual_v1.5.7.html
[zstd-dict]: https://github.com/gyscos/zstd-rs/blob/v0.13.3/src/dict.rs
[zstd-bulk]: https://github.com/gyscos/zstd-rs/blob/v0.13.3/src/bulk/compressor.rs
[zstd-safe]: https://github.com/gyscos/zstd-rs/blob/v0.13.3/zstd-safe/src/lib.rs
[blake3]: https://github.com/BLAKE3-team/BLAKE3/tree/1.8.6
[bytes-source]: https://github.com/tokio-rs/bytes/blob/v1.12.1/src/bytes.rs
[simd-popcnt]: https://docs.rs/crate/simd-popcnt/1.0.0
[pulp]: https://docs.rs/pulp/0.22.3/pulp/
[portable-simd]: https://github.com/rust-lang/portable-simd
[hnsw-rs]: https://github.com/jean-pierreBoth/hnswlib-rs
[usearch-rust]: https://docs.rs/usearch/2.26.0/usearch/
[xdelta]: https://github.com/jmacd/xdelta
[vcdiff]: https://www.rfc-editor.org/rfc/rfc3284.html
[qbsdiff-source]: https://github.com/hucsmn/qbsdiff/blob/master/src/lib.rs
[qbsdiff-manifest]: https://github.com/hucsmn/qbsdiff/blob/master/Cargo.toml
[bidiff]: https://github.com/divvun/bidiff
