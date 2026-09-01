# Hot-Buffer-Reuse-Audit, 2026-09-01

## Ergebnis

Fuenf plausible Allokations- beziehungsweise Ownership-Naehte wurden in einem
alternierenden Release-A/B isoliert gemessen. Drei davon bleiben als
Produktionsaenderung:

- der FUSE-Receive-Pfad recycelt bis zu zwei bereits initialisierte
  Request-Puffer nach dem letzten `Bytes`-Owner;
- verifizierte RAW- und Prefix-Records geben ihren decodierten Payload-`Vec`
  direkt an den Aufrufer weiter;
- ein sauberer Read ohne ueberlagernde Epoch-Daten gibt die Allokation des
  committed Readers direkt als Reply-Ergebnis zurueck.

Die 4-KiB-Publication-Samples und die grossen Containerbilder wurden nicht als
Pool uebernommen. Sample-Reuse war im isolierten Kernel schnell, verlor aber im
echten `io_uring`-Publisher. Ein globaler Containerbild-Pool war bei 4 MiB
langsamer und wuerde bei den nur dort positiven 32/64-MiB-Faellen dauerhaft
32--64 MiB ausserhalb des bestehenden Memory-Governors binden.

## Isolierte A/B-Messung

`allocation_reuse_ab` verwendet das Releaseprofil, neun alternierende Samples
und jeweils den Median. Die Baseline und der Challenger fuehren dieselbe
Nutzdatenarbeit aus; nur Allokation, Kopie oder Ownership unterscheiden sich.

| Kernel | Groesse | Baseline | Reuse/Move | Speedup | Entscheidung |
| --- | ---: | ---: | ---: | ---: | --- |
| FUSE Receive | 128 KiB | 3.526 ns | 1.857 ns | **1,899x** | implementiert |
| FUSE Receive | 1 MiB | 29.858 ns | 16.771 ns | **1,780x** | implementiert |
| FUSE Receive | 4 MiB | 185.919 ns | 96.058 ns | **1,935x** | implementiert |
| drei Publication-Samples | 12 KiB | 222 ns | 88 ns | 2,513x | nach Publisher-A/B verworfen |
| verifizierter Payload | 128 KiB | 40.548 ns | 38.401 ns | **1,056x** | implementiert |
| verifizierter Payload | 256 KiB | 83.000 ns | 77.046 ns | **1,077x** | implementiert |
| committed Reply | 128 KiB | 6.574 ns | 2.447 ns | **2,686x** | implementiert |
| committed Reply | 1 MiB | 129.454 ns | 39.037 ns | **3,316x** | implementiert |
| aligned Containerbild | 4 MiB | 157.305 ns | 166.688 ns | 0,944x | verworfen |
| aligned Containerbild | 32 MiB | 5.270.291 ns | 1.724.648 ns | 3,056x | nicht memory-safe integrierbar |
| aligned Containerbild | 64 MiB | 10.056.140 ns | 3.691.751 ns | 2,724x | nicht memory-safe integrierbar |

Die virtuelle Maschine stellt die Hardware-PMU fuer `cycles`, `instructions`,
`cache-references` und `cache-misses` nicht bereit. Eine direkte L1/L2-Aussage
waere deshalb erfunden. `perf stat -r 3` bestaetigt aber die erwartete
Seiteneffekt-Richtung: beim 1-MiB-Receive sinken Minor Faults von 590 auf 334,
beim 256-KiB-Payload-Kernel von 786.672 auf 270 und beim 1-MiB-Reply-Kernel von
1.966.478 auf 846. Das misst vermiedene Page-Fault-/Allokationsarbeit, nicht
die Cache-Hierarchie selbst.

## Echter Publisher-A/B

Der unveraenderte Publisher und ein Challenger mit einem wiederverwendeten
4-KiB-Samplepuffer liefen pro Groesse fuenfmal alternierend, jeweils mit acht
Publishern. Die Tabelle zeigt den Median der kompletten Laufzeit und den Median
des vom Lauf gemeldeten p99.

| Payload/Modus | Baseline wall | Sample-Reuse wall | Baseline p99 | Sample-Reuse p99 |
| --- | ---: | ---: | ---: | ---: |
| 128 KiB, buffered, 1.500 Container | 1,074 s | 1,115 s | 8,562 ms | 9,152 ms |
| 4 MiB, direct, 100 Container | 208,303 ms | 209,041 ms | 23,083 ms | 24,365 ms |

Damit ist der isolierte 4-KiB-Gewinn kein produktiver Gewinn. Die Variante
wurde vor dem finalen Build zurueckgenommen.

## SingleStream-SMB-A/B

Der angeforderte Skill-Lauf verwendet die Rocky-10.2-Minimal-ISO dreimal
seriell, einen frischen Repository-Root, `/dev/sdc` als XFS-Containerdisk und
`/dev/sdb` als getrennte XFS-Metadatendisk. Die temporaere Baseline enthaelt
denselben aktuellen Format-/Zstd-Code, aber nicht die drei oben beschriebenen
Reuse-/Ownership-Aenderungen. Beide Challenger-Laeufe verwenden denselben
Release-Build; jeder Lauf bestand Cleanup und die Prozess-Zero-Swap-Pruefung.

| Lauf | Uploads MiB/s | Aggregat | p99/max | Daemon CPU | Peak RSS | Swap |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 582,5 / 1.538,6 / 1.541,6 | 994,9 MiB/s | 3.393,0 ms | 11,43 s | 541,6 MB | 0 |
| Reuse 1 | 615,9 / 1.517,7 / 1.492,3 | 1.016,0 MiB/s | 3.209,1 ms | 10,04 s | 617,9 MB | 0 |
| Reuse 2 | 601,0 / 1.570,1 / 1.576,2 | 1.022,1 MiB/s | 3.288,4 ms | 9,63 s | 569,5 MB | 0 |

Beide Challenger-Laeufe sind im Aggregat positiv: **+2,1 %** und **+2,7 %**.
Der erste Upload mit neuer Nutzdatenpublikation gewinnt **+5,7 %** und
**+3,2 %**; p99/max sinkt um **5,4 %** und **3,1 %**. Die Daemon-CPU sinkt um
12,2 % beziehungsweise 15,7 %. Peak RSS schwankt in diesen kurzen Laeufen und
ist beim Challenger hoeher; der FUSE-Recycler selbst bleibt unveraendert auf
zwei Receive-Allokationen begrenzt. Alle Laeufe erreichen rund 3,103x
Datenreduktion.

Gegen die aeltere Referenz `smb-quota-ab-b2-20260901.json` mit 1.293,9 MiB/s
liegt der heutige Endstand dennoch rund 21 % niedriger. Das direkte A/B ordnet
diese Luecke nicht dem Recycler zu: die gleiche aktuelle Format-Baseline ist
noch langsamer. Gleichzeitig stieg die aufsummierte Container-Write-Zeit der
identischen `/dev/sdc` von 11,0 s auf 17,2 s und der volle Ingest-Ring wartete
1,317 s statt 0,058 s, waehrend die Daemon-CPU von 12,01 s auf 9,63 s sank. Der
Abstand korreliert in diesem Lauf daher mit I/O-/Pipeline-Wartezeit, nicht mit
mehr CPU-Arbeit durch den neuen Pool.

Die finalen Reports sind
`.artifacts/benchmarks/smb-single-stream-hot-buffer-baseline.json`,
`.artifacts/benchmarks/smb-single-stream-buffer-reuse-final.json` und
`.artifacts/benchmarks/smb-single-stream-buffer-reuse-final-repeat.json`.

## Sicherheitsgrenzen

Der FUSE-Recycler behaelt genau zwei Spare-Puffer, also nicht mehr als die
bisherige Prefetch-Implementierung. Bei mehr gleichzeitig gehaltenen Writes
allokiert der Dispatch korrekt synchron; spaete Rueckgaben werden bei vollem
Pool freigegeben. Weil recycelte Puffer einen alten Suffix enthalten koennen,
akzeptiert der Decoder einen Request nur noch, wenn die deklarierte
FUSE-Headerlaenge exakt der abgeschlossenen `readv`-Laenge entspricht.

Der Clean-Read-Fastpath gilt nur, wenn keine geplante Epoch den angeforderten
Bereich durch Daten, Holes oder Truncation beeinflusst. Kurze committed Reads
bleiben ein I/O-Fehler.

## Reproduktion

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp

cargo run --release -p fastdup-store --example allocation_reuse_ab
cargo test -p fuse3 --features file-lock,tokio-runtime
cargo test -p fastdup-store -p fastdup-posix -p fastdup-io-uring
```

Rohdaten liegen unter `.artifacts/benchmarks/allocation-reuse/` in
`micro-ab.txt`, `perf-*.csv` und `publisher-ab.txt`.
