# Datenstrukturen für den poolweiten Similarity Index

Stand: 2026-08-25.

## Ergebnis

`BTreeMap` ist für den vollständigen poolweiten Similarity Index nicht die
richtige residente Datenstruktur. Ein bloßer Tausch gegen `HashMap` behebt
jedoch nur Lookup-Kosten, nicht das Wachstum mit der Zahl unterschiedlicher
Bucket-Schlüssel.

Die empfohlene Aufteilung ist:

1. Ein kleiner mutierbarer Hot-State verwendet SwissTable-Hashmaps und eine
   dichte Arena. Buckets speichern `u32`-Ordinals statt 32-Byte-Chunk-IDs.
2. Der vollständige Snapshot liegt nach `BucketKey` sortiert in festen
   4-KiB-Seiten auf NVMe. Ein kleiner, druckabhängiger Page-Cache hält nur
   tatsächlich gelesene Directory- und Representative-Seiten.
3. Der Exact Index bestätigt weiterhin Chunk-Identität, aktive Location und
   unabhängige Dekodierbarkeit der Base.

Damit bleibt die Similarity-Schnittstelle unverändert. Die Implementation
entscheidet intern zwischen Hot-State und persistentem Adapter.

## Warum `BTreeMap` nicht mit dem Pool wachsen sollte

Rust beschreibt `BTreeMap` als cache-freundlicher als einen binären Suchbaum,
weil ein Knoten mehrere Elemente zusammenhängend speichert. Die aktuelle
Implementation durchsucht diese kleinen Knoten linear. Das ist für geordnete
Iteration und kleine mutable Mengen vernünftig, bringt aber weiterhin
Knotenallokationen und logarithmische Vergleiche mit sich.

Im fastdup-v1-Profil erzeugt jeder Chunk vier `BucketKey`s. Bei weitgehend
einzigartigen Superfeatures nähert sich die Zahl residenter Buckets daher
`4 * ChunkCount`. Jeder heutige Bucket besitzt zusätzlich einen eigenen
`Vec<ChunkId>`. Auch wenn jeder Bucket höchstens 64 IDs hält, ist die Zahl der
Buckets nicht begrenzt. Das ist das eigentliche Problem.

Quelle: [Rust `BTreeMap` background](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html#background)

## Mutable Hot-Datenstruktur

Rust `HashMap` ist eine Portierung von SwissTable mit quadratischem Probing und
SIMD-Lookup. Die Iterationsreihenfolge ist nicht stabil. Das stört fastdup
nicht, solange Buckets ausschließlich per Schlüssel adressiert werden und die
64 Repräsentanten weiterhin nach vollständiger Chunk-ID sortiert bleiben.

Für den Hot-State ist daher sinnvoll:

```text
HashMap<BucketKey, Vec<RepresentativeOrdinal>>
HashMap<ChunkId, RepresentativeOrdinal>

ResidentArena:
  metadata[ordinal]
  aligned_sketch[ordinal]
  reference_count[ordinal]
```

Ein Bucket-Verweis schrumpft von 32 auf 4 Byte. Ein 64er-Bucket benötigt für
seine Verweise 256 statt 2.048 Byte. Die vollständige Chunk-ID bleibt in der
Arena und bestimmt weiterhin Auswahl und Reihenfolge.

Quelle: [Rust `HashMap`](https://doc.rust-lang.org/std/collections/struct.HashMap.html)

## Persistenter Pool-Index

Der persistente Snapshot ist immutable. Seine Zugriffsmuster sind vier exakte
`BucketKey`-Lookups je Query und anschließend ein kurzer sequenzieller Read der
Repräsentanten. Ein seitenorientiertes sortiertes Directory passt besser als
eine prozessresidente Map:

```text
Directory page:
  sorted BucketKey -> representative page range

Representative page:
  sorted ChunkId or snapshot-local ordinal

Arena page:
  ChunkId, logical length, 512-bit sketch
```

RocksDB verwendet dieselben allgemeinen Bausteine für große sortierte
On-Disk-Indizes: Block-basierte Tabellen, begrenzte und geshardete Block-Caches
sowie Prefix-Indizes beziehungsweise Bloom-Filter, um unnötige I/Os zu
vermeiden. fastdup braucht dafür nicht RocksDB selbst. Das vorhandene immutable
Run-Format kann die wenigen benötigten Regeln gezielter und feldweise
serialisiert abbilden.

Quellen:

- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)
- [RocksDB Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek)

## Bewertete fertige Module

### `fst`

`fst` ist für sehr große immutable Byte-Key-Maps gebaut, kann konstant
speichernd erzeugt werden und unterstützt Lookup auf memory-mapped Daten. Die
Werte sind auf `u64` begrenzt, was für einen Offset reichen würde. Es verlangt
lexikografisch sortierte Eingabe.

Gegen den Einsatz sprechen hier zwei Punkte. Erstens benötigt die übliche
große On-Disk-Nutzung ein Memory Mapping. Zweitens wäre das FST ein zweites
inneres Dateiformat, dessen Integrität und Versionierung zusätzlich zum
Similarity Run geprüft werden müsste. Ein festes Directory mit 16-Byte-Key und
seitenlokaler binärer Suche ist einfacher zu scrubben.

Quelle: [`fst` crate documentation](https://docs.rs/fst/latest/fst/)

### `redb`, LMDB und RocksDB

`redb` speichert copy-on-write B+trees und bietet transaktionale, zero-copy
Reads. LMDB ist ebenfalls B-tree-basiert und liefert Daten direkt aus einem
Memory Mapping. RocksDB bietet SSTs, Prefix-Indizes und Block-Caches.

Diese Datenbanken lösen zusätzlich veränderliche Transaktionen, MVCC,
Allokation und Recovery. Der Similarity Index ist dagegen rebuildbare
Acceleration und wird als immutable Generation veröffentlicht. Eine komplette
eingebettete Datenbank würde eine zweite Durability- und Recovery-Logik
einführen. Die B+tree- und Block-Cache-Muster sind nützlich, die Abhängigkeiten
selbst derzeit nicht.

Quellen:

- [`redb` design](https://github.com/cberner/redb/blob/master/docs/design.md)
- [LMDB public header](https://github.com/openldap/openldap/blob/master/libraries/liblmdb/lmdb.h)
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)

### Memory Mapping

Memory Mapping vermeidet eine Kopie zwischen `pread`-Puffer und Parser und
überlässt Page Residency dem Kernel. `memmap2` markiert jedoch alle
file-backed Konstruktoren als `unsafe`, weil Änderungen oder Truncation des
Backing Files undefiniertes Verhalten verursachen können. Linux dokumentiert
außerdem `SIGBUS` für Zugriffe hinter das aktuelle Dateiende.

`unsafe` ist für fastdup zulässig, wenn ein gemessener Nutzen die zusätzliche
Vertrauensgrenze rechtfertigt. Der mmap-Pfad gehört deshalb in einen kleinen,
separat auditierten Adapter. Eine gemappte Run-Generation muss bis zum letzten
Reader unveränderlich, ungekürzt und gegen Löschung gepinnt bleiben. Der
feldweise Decoder, Checksummen und Query-Bounds bleiben für `pread` und `mmap`
identisch. Der reproduzierbare Benchmark hat den Nutzen belegt; ADR 0061
aktiviert den mmap-Pfad für `FsStorageIo` hinter einer Generation-Lease.
Publication, Offline-Scrub und generische Adapter behalten die korrekte
`read_exact_at`-Baseline.

Quellen:

- [`memmap2::Mmap` safety](https://docs.rs/memmap2/latest/memmap2/struct.Mmap.html#safety)
- [Linux `mmap(2)`](https://www.man7.org/linux/man-pages/man2/mmap.2.html)

## Umsetzungsschritte

1. Den mutierbaren RAM-Index auf SwissTable plus `u32`-Arena umstellen. Die
   Query-Reihenfolge bleibt Sketch-Distanz und vollständige Chunk-ID.
2. Das Similarity-Run-Format um sortierte Bucket-Directory- und
   Representative-Seiten ergänzen. *(Als Format v2 umgesetzt.)*
3. Queries lesen Buckets direkt aus dem Run. Der RAM-Index wird ein begrenzter
   Hot-Delta und Page-Cache, nicht mehr eine Kopie aller Bucket-Schlüssel.
   *(Der Snapshot-Querypfad und ein direkter 4-KiB-Page-Cache sind umgesetzt.)*
4. Snapshot-Building durch externes Sortieren und Streaming-Encode an den
   Exact-Index-Rebuild anbinden. *(Seiten-Encoder, Repository-Publication und
   ein begrenzter mehrstufiger K-Way-Merge für Einträge und Bucket-Referenzen
   sind umgesetzt. BucketKey-partitionierte Run-Familien mit atomarem Manifest
   sind ebenfalls umgesetzt. Der gemeinsame Rebuild nach ADR 0062 speist Exact
   und Similarity aus einem verifizierten Container-Scan, bindet das Family-
   Manifest an die Exact-Run-Set-ID und aktiviert es zuletzt.)*
5. mmap gegen `read_exact_at` messen und den schnelleren Pfad unter den
   oben genannten Lebensdauer- und Fault-Injection-Invarianten aktivieren.
   *(Der reproduzierbare Seiten-Benchmark misst mmap 1,413-mal schneller. Die
   produktive Aktivierung ist mit ADR 0061 umgesetzt. Die Generation-Lease
   schließt Truncate, Ersetzung und Reclamation bis zum letzten Reader aus;
   siehe `docs/benchmarks/similarity-page-access-2026-08-25.md`.)*
