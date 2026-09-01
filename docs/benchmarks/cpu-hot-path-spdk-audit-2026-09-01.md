# CPU-Hot-Path- und SPDK-Audit, 2026-09-01

## Ergebnis

Der Audit fand zwei lokal messbare und semantisch kleine Verbesserungen:

1. Sparse-XOR-Trials überspringen auf AVX2-Hosts identische 32-Byte-Lanes.
   Der isolierte Scan wurde bei sparsamen Änderungen **13,503x** schneller;
   der vollständige Trial einschließlich bestehender Hash-, Decode- und
   Verifikationsarbeit wurde **1,437x** schneller.
2. Der `io_uring`-Ring-Owner verwendet einen einmal auf Ringtiefe reservierten
   CQE-Scratch-Vektor. Gegen eine neue `Vec` je Reap war diese Naht je nach
   CQE-Batch **1,094x bis 5,333x** schneller und allokiert im steady state
   nicht mehr.

Beide Änderungen bewahren die bestehende x86-64-Baseline. AVX2 wird zur
Laufzeit erkannt; der skalare Sparse-XOR-Pfad bleibt das semantische Orakel.
Durable Formate, CRC-Wireformat, Chunk IDs, Codec-Auswahl, I/O-Reihenfolge und
Recovery-/Scrub-Regeln ändern sich nicht.

Die primärquellenbasierte Übertragung der SPDK-Muster ist separat unter
[`docs/research/spdk-hot-path-insights.md`](../research/spdk-hot-path-insights.md)
dokumentiert. Alle SPDK-Links sind dort auf Revision
`0578808fc00fda1f97953b814674ab26528f8148` gepinnt.

## Host und Methode

- Intel Core i7-1370P VM, zehn online CPUs, AVX2/BMI2/SSE4.2/PCLMULQDQ,
  kein AVX-512;
- Linux `6.12.0-211.49.1.el10_2.x86_64`, Rust `1.97.1`, LLVM `22.1.6`;
- Releaseprofil mit Overflow-Checks und Debug-Symbolen;
- alternierende A/B-Reihenfolge und Median aus sieben beziehungsweise neun
  Samples;
- alle neuen Build-, Test- und Messartefakte unter
  `/source/fastdup/.artifacts/`.

Für das breite Inventar wurde das letzte vollständige SingleStream-Profil
`.artifacts/profiles/smb-bottleneck-current-20260825.perf.data` erneut
ausgewertet. Es ist ein historisches CPU-Sample desselben Hosts, kein Profil
des heutigen Diffs. Ein neuer vollständiger SMB-Lauf benötigt den externen
Samba-/cgroup-/Pool-Aufbau und wird deshalb nicht durch einen lokalen
Mikrobenchmark vorgetäuscht.

## Inventar der CPU-Hotpaths

| Anteil im breiten Profil | Symbol/Pfad | ISA-/SPDK-Befund | Entscheidung |
| ---: | --- | --- | --- |
| 26,63 % | BLAKE3 `hash_many` | bereits AVX2-dispatched; SPDK hat keinen kryptographischen Content-Hash-Ersatz | beibehalten |
| 18,17 % | Zstd `doubleFast` | optimierter Upstream-Codec, worker-lokale Kontexte entsprechen SPDK per-channel state | beibehalten |
| 13,17 % | glibc `memmove` | bereits `avx_unaligned_erms`; SPDK verwendet im Softwarebackend ebenfalls libc | keine lokale SIMD-Kopie |
| 9,03 % | SeqCDC | bestehender AVX2/BMI2-Kernel, scalar-identisch und bereits end-to-end vermessen | beibehalten |
| 7,59 % | glibc `memset` | bereits AVX2/ERMS; überwiegend Initialisierung/Allokation, keine langsamere skalare Schleife | Ownership/Initialisierung statt neuer Intrinsics optimieren |
| 6,03 % | Stable-Chunk-Batch | Segment-/Batch-Orchestrierung um den vektorisierten SeqCDC-Kern | keine eigenständige SIMD-Naht |
| 1,15 % | BLAKE3 `compress_in_place` | bereits SSE4.1 | beibehalten |
| 0,95 % | CRC32C `parallel3` | bestehender SSE4.2-Runtimepfad; SPDK bevorzugt ISA-L oder echte DSA-Hardware | Softwarepfad beibehalten, DSA nur auf qualifiziertem Host A/B testen |
| 0,82 % | glibc `memcmp` | bereits AVX2/MOVBE | keine lokale Vergleichsschleife |
| < 0,5 % je Pfad | Proof Cache, Exact Decode, Allokator, Rayon-Steal | kleine, verzweigte oder synchronisierende Arbeit | keine SIMD-Priorität |

Die Prozentwerte sind Sample-Anteile, keine additiven Kapazitätszusagen. Sie
zeigen vor allem, dass die dominanten Bytepfade bereits in spezialisierten
AVX2/SSE-/libc-/Codec-Kernen laufen. Weitere Intrinsics um diese Bibliotheken
würden dieselbe Arbeit duplizieren.

## Neue Sparse-XOR-AVX2-Naht

`SparseXorDelta::encode_trial` scannt bis zu 256 KiB große, gleich lange Base-
und Target-Chunks. Bei für Delta interessanten Daten sind große Bereiche
identisch und nur wenige Bytes geändert. Der neue Kernel lädt zwei unaligned
32-Byte-Lanes, vergleicht sie mit `VPCMPEQB` und gewinnt mit `VPMOVMSKB` eine
exakte Gleichheitsmaske. Eine vollständig gleiche Lane wird in einem Schritt
übersprungen; gemischte Lanes werden bytegenau in dieselben kanonischen Runs
wie zuvor zerlegt.

Die Unsafe-Grenze liegt ausschließlich in
`crates/fastdup-store/src/similarity_simd.rs`. Der sichere Aufrufer prüft AVX2
und gleiche Slice-Längen. Jede 32-Byte-Last ist durch die Schleifenbedingung
begrenzt; Run- und XOR-Ausgabe verwenden sichere `Vec`-Operationen. Der
skalare Writerpfad bleibt erhalten.

Der Differentialtest umfasst Längen um jede Lane-Grenze (`1`, `31`, `32`,
`33`, `63`, `64`, `65`, `4.096`, `262.144`), identische Chunks, Edits an
Lane-Rändern, alternierende Bytes und vollständig verschiedene Chunks. Die
bestehenden Writer-/Reader-Sweeps prüfen zusätzlich das dekodierte Ziel und
seinen BLAKE3 Chunk ID.

Gemessen wurde ein 256-KiB-Chunk mit einem geänderten Byte je 4 KiB:

| Naht | skalar | AVX2 | Speedup |
| --- | ---: | ---: | ---: |
| nur Run-/XOR-Scan | 118.834,6 ns/Chunk | 8.800,3 ns/Chunk | **13,503x** |
| vollständiger `encode_trial` | 365.066,2 ns/Chunk | 254.030,2 ns/Chunk | **1,437x** |

Der vollständige Gewinn ist kleiner, weil Base- und Target-Identitäten sowie
die unmittelbar erzeugte Delta-Rekonstruktion weiterhin unabhängig geprüft
werden. Diese Arbeit wird nicht zugunsten einer schöneren SIMD-Zahl entfernt.

## Wiederverwendbarer CQE-Scratch

`RingWorker::reap_completions` materialisiert CQEs weiterhin außerhalb des
geliehenen Completion-Queue-Handles, damit die anschließende Zustandsmaschine
den Ring wieder mutabel verwenden kann. Neu ist nur die Lebenszeit des
Puffers: `completion_scratch` wird einmal mit `ring_entries` reserviert und je
Reap geleert. Die CQE-Reihenfolge bleibt gleich. Eine Assertion hält fest,
dass die ringgroße Kapazität nie verloren geht.

Der A/B-Benchmark bildet exakt die alte Neuallokation und die neue
`clear`/`extend`-Variante für Ringtiefe 256 nach:

| CQEs je Reap | neue `Vec` | Scratch-Reuse | Speedup |
| ---: | ---: | ---: | ---: |
| 1 | 8,543 ns | 1,602 ns | **5,333x** |
| 8 | 10,939 ns | 4,839 ns | **2,260x** |
| 64 | 36,882 ns | 31,967 ns | **1,154x** |
| 256 | 132,293 ns | 120,975 ns | **1,094x** |

Ein anschließender realer Smoke-Lauf veröffentlichte 1.000 Container mit acht
Publishern: 1.354 Container/s, p50 5,65 ms, p99 9,67 ms, 9.986 eingereichte
und vollständig abgeschlossene Operationen. Dieser einzelne Lauf beweist die
CQE-/Publication-Funktion, aber ohne gepaarten stabilen Storage-Baseline-Lauf
keinen end-to-end Durchsatzgewinn.

## Geprüft, aber nicht übernommen

- **FILL-/Repeated-byte-Scan:** Ein isolierter AVX2-Prototyp ist bei einem
  vollständigen 256-KiB-FILL deutlich schneller. Im breiten Profil liegt
  `classify_stable_chunk_shard` jedoch nur bei rund 0,1 %, während zufällige
  Daten im skalaren Pfad typischerweise nach den ersten Bytes abbrechen. SPDKs
  eigenes `spdk_mem_all_zero` ist ebenfalls skalar. Ohne vollständiges
  `hash_and_fill`-Corpus-A/B und SingleStream-Evidenz wird kein weiterer
  Unsafe-Kernel aufgenommen.
- **CRC32C/Copy+CRC:** Das Rust-`crc32c`-Backend verwendet bereits einen
  dreifach verschachtelten SSE4.2-Pfad. SPDKs Software-Copy+CRC kopiert und
  hasht ebenfalls in zwei Durchläufen; nur DSA fusioniert dies wirklich. Auf
  diesem Host ist kein DSA verfügbar. Eine neue Pflichtabhängigkeit oder ein
  handgeschriebener Pointerloop wäre unbelegt.
- **Similarity-Sketch-Distanz:** Acht skalare XOR+POPCNT-Wörter pro Kandidat
  sind hart auf 256 Vertreter begrenzt. AVX2 besitzt kein Vektor-POPCNT;
  AVX-512/VPOPCNTDQ ist auf dem Messhost nicht verfügbar und darf nicht zur
  Baseline werden.
- **Exact-/Similarity-Prefetch:** mmap, dichte Page Bounds und cache-line-
  getrennte Daten sind bereits vorhanden. Der nächste Binärsuchschritt bietet
  ohne nachgewiesene LLC-Stalls keinen stabilen Vorlaufabstand. Keine
  spekulative Prefetch-Intrinsic.
- **Completion-Slot-Tabelle:** SPDK adressiert CQ-Tracker direkt. Für fastdup
  muss `user_data` dabei Slot und Generation binden, damit Wiederverwendung
  keine alte Completion einer neuen Operation zuordnet. Das bleibt ein
  separates P2-A/B gegen die heute kleine, vorreservierte `HashMap`; es wurde
  nicht zusammen mit der risikofreien Scratch-Änderung eingeführt.
- **IAA/Deflate, Busy Polling, globale Native-/AVX-Builds:** passen nicht zum
  bestehenden Zstd-Durable-Format, HDD-DATA-Pfad beziehungsweise ADR 0078.

## Reproduktion

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp

cargo test --release -p fastdup-store \
  sparse_xor_scalar_and_avx2_microbenchmark --lib -- \
  --ignored --nocapture

cargo run --release -p fastdup-io-uring \
  --example completion_scratch_bench

cargo test -p fastdup-store sparse_xor --lib
cargo test -p fastdup-io-uring
cargo clippy -p fastdup-store -p fastdup-io-uring \
  --all-targets -- -D warnings
```

Rohresultate dieses Laufs liegen in
`.artifacts/tmp/sparse-xor-simd-benchmark.txt`,
`.artifacts/tmp/completion-scratch-benchmark-final.txt` und
`.artifacts/tmp/cqe-publisher-final.txt`.
