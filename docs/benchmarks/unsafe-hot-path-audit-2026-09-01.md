# Unsafe-Hot-Path-Audit, 2026-09-01

## Ergebnis

Der aktuelle Schreib- und Lesepfad rechtfertigt `unsafe` an den bereits eng
gekapselten SIMD-, mmap- und Plattformgrenzen. Der Audit fand keinen weiteren
Produktionskernel, bei dem Bounds-Check-Elision, unaligned Feldzugriffe,
manuelle Payload-Kopien oder uninitialisierte Containerbilder einen
reproduzierbaren Vorteil gegenüber sicherem Rust liefern. Deshalb wurde kein
neuer `unsafe`-Produktionscode eingeführt.

Zwei wiederholbare Benchmarkprogramme wurden ergänzt:

- `fastdup-store/examples/unsafe_hotpath_ab.rs` vergleicht sichere und
  `unsafe` Feldzugriffe, Payload-Kopien, ausgerichtete Bildkonstruktion und
  langfristige Lookup-Arenen.
- `fastdup-store/examples/gc_candidate_mmap_bench.rs` vergleicht den
  Produktionspfad des GC-Kandidatenkatalogs mit und ohne immutable-file lease.

Die Rohresultate dieses Laufs liegen unter
`.artifacts/unsafe-audit/`. Große Fixtures und Binärartefakte bleiben wie von
ADR 0025 gefordert außerhalb der Versionsverwaltung.

## Host und Methode

- Intel Core i7-1370P VM, 10 online CPUs, AVX2/BMI2/SSE4.2, kein AVX-512;
- Linux `6.12.0-211.49.1.el10_2.x86_64`, XFS;
- Rust `1.97.1`, LLVM `22.1.6`, Releaseprofil mit Overflow-Checks;
- alternierende Reihenfolge und Median aus sieben oder elf Samples;
- Rocky-ISO: 2.072.444.928 Bytes, SHA-256
  `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`;
- alle Build-, Fixture- und Messartefakte unter `/source/fastdup/.artifacts/`.

Die VM stellt keine PMU-Zähler für Cycles, Instructions oder Branch Misses
bereit. Der Audit verwendet deshalb Wall-Time, identische Ergebnis-Checks,
Prozess-Fault/Swap-Zähler der bestehenden mmap-Benchmarks und Disassembly für
die Bounds-Check-Frage.

## Produktionsnahe A/B-Ergebnisse

| Pfad | Sicher/Referenz | `unsafe`-Challenger | Ergebnis |
| --- | ---: | ---: | ---: |
| SeqCDC über Rocky ISO | skalar 3.325,8 MiB/s | AVX2/BMI2 10.712,7 MiB/s | **3,221x** |
| Similarity-Fingerprint, 256 KiB | skalar 25,844 ns/Byte | AVX2 2,544 ns/Byte | **10,160x** |
| Exact Lookup, 262.144 Einträge | positional 2.869,0 ns/Query | mmap 1.563,2 ns/Query | **1,835x** |
| Similarity-Seite, 100.000 Einträge | positional 1.609,6 ns/Query | mmap 1.162,8 ns/Query | **1,384x** |
| GC-Katalog `find_row`, 100.000 Zeilen | positional 24.067,1 ns/Query | mmap 481,9 ns/Query | **49,945x** |

SeqCDC und Similarity liefern jeweils exakt dasselbe Ergebnis wie ihre
skalaren Orakel. Die mmap-Vergleiche decodieren dieselben langlebigen
Feldformate; nur die Seitquelle wechselt. Der Exact-Lauf meldete null Major
Faults, 119.984 KiB Peak RSS und null Swap in beiden Varianten.

Die GC-Differenz ist groß, weil eine binäre Suche im positional Fallback pro
Stufe einen 96-Byte-Read ausführt. Sie gilt für zufällige `find_row`-Suchen und
darf nicht auf den gebatchten sequenziellen Shortlist-Scan oder DATA-I/O
übertragen werden.

## Neue `unsafe`-Kandidaten

Der Mikrobenchmark bildet die festen 128-Byte-Index-/Recovery-Felder und die
256-KiB-Chunk-Kopie der Produktionspfade nach. Jede Variante prüft vor der
Messung Ergebnisgleichheit.

| Kandidat | Sicher | `unsafe` | `unsafe` relativ |
| --- | ---: | ---: | ---: |
| drei Feldreads pro 128-Byte-Eintrag, 4-KiB-Seite | 13,645 ns | 13,769 ns | 0,991x |
| drei Feldwrites pro 128-Byte-Eintrag, 4-KiB-Seite | 14,990 ns | 14,780 ns | 1,014x |
| 256-KiB `copy_from_slice` | 3.899,8 ns | 3.894,4 ns | 1,001x |

Das Release-Disassembly enthält für die sicheren Feldzugriffe keine
Bounds-Check-Branches. LLVM faltete den sicheren Feldwriter und den
`write_unaligned`-Writer auf dieselbe Funktion zusammen. Die sichere
Payload-Kopie und `copy_nonoverlapping` enden beide im optimierten `memcpy`.
Die Restdifferenzen von höchstens 1,4 Prozent sind kein belastbarer Vorteil und
rechtfertigen kein zusätzliches Safety-Invariant.

### Ausgerichtetes Containerbild

Der stärkste scheinbare Kandidat war eine 4-KiB-ausgerichtete, uninitialisierte
Allokation. Die Kontrolle enthält zusätzlich eine sichere Append-Variante, die
Header/Body/Footer genau einmal initialisiert, statt erst das ganze Bild zu
nullen und den Payload danach zu überschreiben.

| Bildgröße | aktuelles zero-then-copy | safe append | unsafe uninit | unsafe gegen safe |
| ---: | ---: | ---: | ---: | ---: |
| 128 KiB | 3.717,3 ns | 2.186,1 ns | 2.131,9 ns | 1,025x |
| 4 MiB | 243.075,7 ns | 149.137,6 ns | 149.011,4 ns | 1,001x |
| 32 MiB | 5.162,7 µs | 5.192,0 µs | 5.111,7 µs | 1,016x |

Das Vermeiden der doppelten Beschreibung ist bei kleinen und mittleren Bildern
real, erfordert aber kein `unsafe`: safe append erreicht praktisch dieselbe
Leistung.

#### Produktionsumbau und Nachmessung

Der Writer für bereits materialisierte RAW-, Zstd- und Prefix-Records verwendet
nun einen privaten sicheren `AlignedContainerBuilder`. Er reserviert die
vollständige seitenalignierte Kapazität einmal und hängt Header, Records,
Recovery Index, explizites Nullpadding und Footer in dauerhafter Reihenfolge an.
Das fertige `AlignedContainerBytes` wird erst nach vollständiger Initialisierung
freigegeben. Der adaptive Direkt-Encoder bleibt unverändert: Er kodiert Records
bereits unmittelbar in deren finale Bereiche und hätte durch Zwischen-`Vec`s
zusätzliche Allokationen und Kopien.

Der produktive RAW-Writer wurde vor und nach dem Umbau mit demselben
Release-Binary-Quellbenchmark gemessen. Jeder Einzelwert ist bereits der Median
aus neun Samples; anschließend wurden drei unmittelbar wechselnde
Baseline/Challenger-Paare ausgewertet. Die Gesamtkodierung enthält weiterhin
Record-Encoding, CRC32C, BLAKE3 und Recovery-Index-Aufbau und streut daher
stärker als der isolierte Montagebenchmark.

| logische Nutzlast | Containerbild | gepaarter Median Durchsatzänderung |
| ---: | ---: | ---: |
| 128 KiB | 140 KiB | **+4,0 %** |
| 4 MiB | 4.112 KiB | **+2,1 %** |
| 32 MiB | 32.820 KiB | **+1,2 %** |

Der Writer-/Reader-Test beweist Nullpadding und injiziert dort einen Fehler
gegen vollständigen Reader und Publication-Verifier. Ein Store-Test injiziert
denselben Fehler in ein publiziertes Containerobjekt und verlangt Ablehnung an
Recovery- und Offline-Audit-Grenze. Die bestehenden Byte-Identitäts-,
Einzelbyte-Korruptions- und Direct-I/O-Alignment-Tests bleiben grün.

Benchmark:

```text
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo run --release -p fastdup-format --example container_assembly_bench
```

Rohdaten und die eingefrorenen Baseline-/Challenger-Binaries liegen unter
`.artifacts/unsafe-audit/container-assembly-*`.

### Long-lived Lookup-Arena

Ein 128-MiB cache-line-blocked Lookup-Array wurde mit vier Millionen
deterministischen Random-Probes verglichen. Die sichere Heap-Allokation hatte
129.024 KiB `AnonHugePages`, die dedizierte THP-advised mmap-Allokation 131.072
KiB. Der Median lag bei 37,676 ms gegen 36,405 ms, also **1,035x** zugunsten
der dedizierten Arena. Das ist ein kleiner, aber messbarer Vorteil der
isolierten mmap/THP-Platzierung; es ist kein Argument für unchecked Zugriffe
im Probe-Loop. Bounds-Checks bleiben im öffentlichen Slice-Interface.

## Vollständige Unsafe-Klassifikation

### Beibehalten

- `seqcdc`: AVX2/BMI2 hinter Runtime-Feature-Detection und skalarem
  Differentialorakel; 3,221x in diesem Lauf.
- `similarity_simd`: AVX2 hinter Runtime-Feature-Detection und goldenem
  Differentialorakel; 10,160x.
- `exact_index_mmap`, `similarity_mmap`, `gc_candidate_mmap`: read-only
  Mapping hinter root-weiten Immutable-File-Leases, vollständigem Audit und
  positional Offline-Scrub; 1,384x bis 49,945x an den gemessenen Nähten.
- `long_lived_arena`: dedizierte anonyme Mapping- und THP-Platzierung für große
  rebuildbare Tabellen; der Zugriff bleibt ein sicher geliehener Slice.
- `fastdup-io-uring`: Plattform-FFI und Submission-Queue-Pushes. Die unsafe
  Blöcke existieren wegen FD-/Pointer-Lebenszeiten, nicht zur Elision von
  Rust-Checks. ADR 0058 und der Publisher-Benchmark bleiben die
  End-to-End-Evidenz; zusätzliche unchecked SQE- oder Bufferzugriffe sind nicht
  gerechtfertigt.

### Kein Performance-Unsafe

- `maintenance_ioprio` ist nur der schmale Syscall-Adapter für
  Background-I/O-Policy.
- die `fuse_mount_smoke`-mmap-Blöcke gehören ausschließlich zum Kernel-
  Semantiktest.
- FUSE Receive-Ownership, `bytes::Bytes`, Sparse-Overlay, Manifestplanung,
  Hash-/CRC-/Codec-Aufrufe und Read-Cache bleiben auf sicheren Interfaces.
  BLAKE3, CRC32C, Zstd, LZ4 und libc `memcpy` nutzen bereits ihre eigenen
  getesteten SIMD-/FFI-Implementierungen; lokale Pointerloops würden diese
  nicht verbessern.

## Reproduktion

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp

cargo run --release -p fastdup-store --example unsafe_hotpath_ab
cargo run --release -p fastdup-store --example gc_candidate_mmap_bench

cargo run --release -p fastdup-appliance --example seqcdc_challenger -- \
  /source/fastdup/.artifacts/benchmark-source/Rocky-10.2-x86_64-minimal.iso \
  6 50 1024 7

cargo test --release -p fastdup-store \
  similarity_fingerprint_scalar_and_avx2_microbenchmark -- \
  --ignored --nocapture

cargo run --release -p fastdup-exact-bench \
  --bin fastdup-exact-lookup-bench -- \
  --root /source/fastdup/.artifacts/unsafe-audit/exact-mmap-fixture \
  --entries 262144 --queries 200000 --rounds 7

cargo run --release -p fastdup-similarity-bench \
  --bin fastdup-similarity-page-bench -- \
  --file /source/fastdup/.artifacts/unsafe-audit/similarity-pages.fds \
  --generate --entries 100000 --queries 1000000 --rounds 7
```

## Entscheidung

Keine neue `unsafe`-Optimierung wird in Writer, Reader, durable Decoder oder
Cache-Probe übernommen. Die vorhandenen SIMD-/mmap-Plattformmodule bleiben
klein und auditiert. Der einzige neue Optimierungshinweis ist safe append für
Containerbilder; er braucht vor einer Integration einen eigenen
Produktions-Assembler-A/B mit Format-, Recovery-, Scrub- und
Fault-Injection-Gates.
