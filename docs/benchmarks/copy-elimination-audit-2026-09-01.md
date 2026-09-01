# Copy-Elimination-Audit, 2026-09-01

## Ergebnis

Der aktuelle Write-through-Pfad hatte noch genau eine nahezu vollständige
vermeidbare Input-Kopie: Borrowed Compression Regions wurden vor Zstd zu einem
zusammenhängenden `Vec<u8>` verbunden. Der letzte intakte SingleStream-Lauf
zählte dafür 2.021.019.019 Byte bei 2.072.444.928 Byte logischem Erst-Upload,
also 97,519 Prozent der Nutzlast. FUSE-Request-Adaption, Publication-Verify,
Container-Assembly und Chunk-Fragment-Coalescing standen in demselben Lauf bei
null Byte.

Der Format-Encoder konsumiert bei der produktiv verwendeten
`IncompressibilityGatePolicy::Off` nun die vorhandenen `PrehashedChunk`-Slices
nacheinander in einem einzigen Zstd-Frame. Er behält Regiongröße,
Chunk-Tabelle, Zstd-Level, Savings-Cap, Record-CRC und Readerformat bei. Wenn
der begrenzte Output bereits voll ist, bevor der Input vollständig konsumiert
ist, steht außerdem schon fest, dass Zstd RAW nicht mehr schlagen kann; der
Encoder beendet diesen Trial dann früh.

Die bestehende V1-/LZ4-Gate-Implementierung bleibt unverändert. Sie benötigt
für den LZ4-Blocktest weiterhin zusammenhängenden Input und darf nicht durch
eine semantisch andere Vorprüfung ersetzt werden.

## A/B-Messung

`fragmented_zstd_bench` vergleicht den vollständigen Formatpfad einschließlich
Zstd, Record-Encoding, CRC, Recovery Index und Container-Sealing:

- Baseline: acht 64-KiB-Chunks erst in eine 512-KiB-Region kopieren, dann über
  `PrehashedContiguousRegion` kodieren;
- Challenger: dieselben acht Chunks ohne Join-Kopie über die bestehende
  Borrowed-Region kodieren;
- Releaseprofil, alternierende Reihenfolge, Median aus neun Samples zu je 31
  Encodes.

| 512-KiB-Fixture | Materialize + Bulk | Borrowed Stream | Speedup |
| --- | ---: | ---: | ---: |
| synthetisch komprimierbar | 165.563 ns | 149.401 ns | **1,108x** |
| synthetisch inkompressibel | 357.986 ns | 161.140 ns | **2,222x** |
| Rocky ISO, 25 % | 398.417 ns | 210.815 ns | **1,890x** |
| Rocky ISO, 50 % | 356.031 ns | 167.698 ns | **2,123x** |
| Rocky ISO, 75 % | 355.298 ns | 155.098 ns | **2,291x** |

Der größere Gewinn bei inkompressiblen Daten kombiniert die entfernte
512-KiB-Kopie mit dem sicheren frühen Cap-Abbruch. Vor der Messung decodiert
der Benchmark beide Container vollständig und verlangt identische logische
Records.

Nach dem Wiederherstellen der getrennten XFS-Datentraeger bestand der
vollstaendige SMB-Benchmark einschliesslich vorgeschriebenem Dry-Run. Der
abschliessende Drei-Upload-Lauf erreichte 601,0 / 1.570,1 / 1.576,2 MiB/s,
aggregiert 1.022,1 MiB/s, bei 3,104x Datenreduktion und null Byte Prozess-Swap.
Der kombinierte Endstand steht in
`.artifacts/benchmarks/smb-single-stream-buffer-reuse-final-repeat.json`; die
isolierte Einordnung der zusaetzlichen Buffer-Reuse-Aenderungen steht in
`docs/benchmarks/hot-buffer-reuse-2026-09-01.md`.

## Verbleibende große Kopien

| Naht | Zustand | Entscheidung |
| --- | --- | --- |
| FUSE Receive → `MutationPayload` | Production übernimmt den owned Receive-Buffer; Zähler zuletzt 0 | beibehalten |
| `MutationPayload`-Slices | `bytes::Bytes` teilt das Backing; nur kleine langlebige Survivors werden gegen höchstens 4x Retention einmal kompaktiert | beibehalten, Memory-Bound statt blindem Zero-Copy |
| fragmented Chunk → Compression Region | Nur Chunks, die selbst Request-Grenzen kreuzen, müssen noch einmal zusammengeführt werden | verbleibender P2; ein fragmentfähiger `PrehashedChunk` würde das öffentliche Format-Interface deutlich verbreitern |
| Zstd-Output → finales Record-Feld | Die parallele Codec-Phase muss die Payloadlänge vor dem Containerlayout kennen | beibehalten; direkte finale Ausgabe würde Zweipass-Kompression oder ein segmentiertes Containerbild verlangen |
| RAW-Input → finales Record-Feld | Eine notwendige Source-to-DATA-Kopie in das zusammenhängende, ausgerichtete Containerbild | beibehalten; echte Elision verlangt vectored Direct-I/O plus segmentierte CRC-/Publication-Lifetimes |
| Container-Allokation | Der adaptive Writer nullt das finale ausgerichtete Bild vor direkten Record-Writes | kein unsicheres uninitialisiertes Bild; bisherige A/B-Evidenz zeigt gegenüber sicherer Initialisierung keinen belastbaren Vorteil |
| Publication → `io_uring` | Owned Publication wird ohne Vollbildkopie übergeben; borrowed Fallback bleibt messbar | beibehalten |
| Verified Read → FUSE Reply | Decoder-`Vec` wird in `Arc` übernommen; nur die angeforderte Reply-Reihenfolge wird final assembliert | notwendige Ausgabekopie beibehalten |
| mmap Exact/Similarity/GC | Parser liest immutable Seiten direkt aus dem Mapping | bereits kopiefrei |
| Offline Rechunk | Sliding Reader-Puffer und ausgegebener Chunk besitzen heute getrennte `Vec`s | nicht im Write-through-Hotpath; erst mit eigenem Profil und Ownership-Redesign verfolgen |

Die historische `memmove`-Zuordnung zu
`ResidentDirtyData::retained_fragment` ist durch die heutige
`MutationPayload`-/`Bytes`-Implementierung überholt. Die verbliebenen kleinen
Serializer-Kopien schreiben feste 2- bis 128-Byte-Felder und sind weder
Payloadkopien noch eigenständige Optimierungsziele.

## Reproduktion

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp

cargo run --release -p fastdup-format --example fragmented_zstd_bench -- \
  /source/fastdup/.artifacts/benchmark-source/Rocky-10.2-x86_64-minimal.iso

cargo test -p fastdup-format -p fastdup-store -p fastdup-appliance
cargo clippy -p fastdup-format -p fastdup-store -p fastdup-appliance \
  --all-targets -- -D warnings
```

Die Rohmessung liegt unter
`.artifacts/tmp/fragmented-zstd-copy-benchmark-iso.txt`. Die letzte intakte
Copy-Telemetrie-Baseline ist
`.artifacts/benchmarks/smb-quota-ab-b2-20260901.json`.
