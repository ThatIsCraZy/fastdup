# Write-/Read-Hotpath-Implementierung, 2026-09-05

Alle sieben priorisierten Punkte aus dem
[Hotpath-Audit](../research/hotpath-opportunities-2026-09-05.md) sind implementiert.
Basis ist der dort untersuchte Arbeitsbaum auf `916248d`, einschließlich seiner
vorhandenen Online-Similarity-/Share-Policy-Änderungen. Es wurde kein neuer
Storage-Codec und kein neues dauerhaftes Format eingeführt.

## Änderungen

1. **Record-Verifikation einmal pro Decode.** Private validierte Record-Typen
   tragen die CRC-/Strukturprüfung vom Dispatcher zum RAW- beziehungsweise
   abhängigen Decoder. Öffentliche Einzeldecoder prüfen weiterhin selbst.
   Sparse-XOR validiert seine Run-Tabelle direkt aus den serialisierten Feldern,
   ohne dafür einen temporären `Vec<SparseXorRun>` anzulegen. Target-BLAKE3 und
   Exact-Koordinaten bleiben geprüft.
2. **Gewinnfähige Trial-Grenzen.** Die unabhängige Payloadgröße bestimmt vor
   Base-I/O den exakten Cap `independent - max(4096, ceil(independent*5/100))`.
   Ein bereits besserer Trial verengt den Cap weiter; Gleichstände ersetzen
   den bisherigen Gewinner nicht. Sparse-XOR zählt `36 + 8*runs + xor_bytes`
   während des bestehenden Scans. Der AVX2-Pfad verwendet die vorhandene
   Gleichheitsmaske, inklusive Run-Übergängen zwischen Lanes; der skalare Pfad
   bleibt das Oracle. Abgewiesene Trials werden nicht in vorbereitete durable
   Records konvertiert.
3. **Verifizierte Bases wiederverwenden.** Frontend-Reads und der persistente
   Ingest-Planner reichen ihren gemeinsamen begrenzten Verified Read Cache in
   die Base-Auflösung. Ein Hit gilt nur für eine weiterhin aktive unabhängige
   Exact-Location mit passenden Container-/Generation-/Record-/Chunk-Koordinaten.
   Der Writer behält den Payload-Owner und borgt die Bytes für Trials.
   Generation-Pins und Publication-Guards bleiben erhalten. Recovery und
   vollständige Container-/Scrub-Reads erhalten diesen Cache nicht.
4. **Antworten ohne vorheriges vollständiges Nullen.** DATA wird sicher
   angehängt; HOLE und FILL werden direkt mit ihrem endgültigen Wert angehängt.
   Coverage- und Längenprüfungen bleiben bestehen. RAW-/Zstd-Writer initialisieren
   im Record nur Header, Tabelle und Padding vor dem Payload-Copy.
5. **Zstd-Kontexte wiederverwenden.** Unabhängige Record-Reads nutzen einen
   threadlokalen Bulk-Decompressor. Prefix-Trials nutzen den unten beschriebenen
   threadlokalen CCtx-Adapter mit vollständigem Reset nach jedem Trial.
6. **Singleflight für alle drei Read-Wege.** Skalare, gebatchte und abhängige
   Reads teilen Record-I/O und Decode. Ein Batch hält eigene Leader nur bis zu
   deren Erledigung und wartet erst danach auf fremde Flights. RAII gibt
   wartende Aufrufer auch bei I/O-/Decode-Fehlern frei. Die physische Sortierung
   und die bestehende Coalescing-Grenze von 1 MiB bleiben erhalten.
7. **RAW-Payloads als Views des eingelesenen Owners.** Die neue owned
   Format-Decodierung behält den `Arc<Vec<u8>>` des Records oder Batches.
   Logischer `decoded_offset` und physischer Backing-Offset sind getrennt.
   Cache-Admission fasst gemeinsam gehaltene Backings zusammen und verrechnet
   die gesamte `Vec`-Kapazität einmal. Provenienz liegt inline im ohnehin
   budgetierten Cache-Slot. Die abschließende Kopie in den FUSE-Antwort-`Vec`
   bleibt Bestandteil des bestehenden Frontend-Interfaces.

## Neue Unsafe-Grenze und ihr A/B-Nachweis

`crates/fastdup-store/src/prefix_context.rs` enthält genau eine neue
Unsafe-Operation: Nach erfolgreichem
`CCtx::reset(ResetDirective::SessionAndParameters)` wird die nun ungebundene
Dictionary-Lebenszeit von `CCtx<'a>` zu `CCtx<'static>` umgesetzt.

Der Kontext wird vor `ref_prefix` exklusiv aus dem Thread-Local genommen.
`NbWorkers(0)` und das synchrone `compress2` schließen noch laufende native
Arbeit nach Rückkehr aus. Nach Erfolg, Output-Cap-Rejection und sonstigen
Fehlern wird immer vollständig zurückgesetzt. Bei Reset-Fehler oder Unwind
wird der Kontext verworfen, solange die geliehene Base noch lebt. Nur ein
erfolgreich zurückgesetzter Kontext geht zurück in den Pool. Es gibt keinen
zurückgegebenen geliehenen Pointer und keinen gleichzeitig zugreifenden Owner.

Die lokal gepinnte Bibliothek ist `zstd-safe 7.2.4` / `zstd 1.5.7`.
Deren `zstd.h` dokumentiert für Parameter-Reset das Entfernen aller
Dictionary-Referenzen und für den kombinierten Reset zuerst Session-, dann
Parameter-Reset. Der Kommentar unmittelbar an der Unsafe-Stelle hält diese
Voraussetzungen fest. Der Test
`reused_prefix_context_detaches_rejected_and_short_lived_bases` verwendet
wechselnde, nach jedem Durchgang freigegebene Bases und mehrere Output-Caps;
Frames und Rejections stimmen mit einem frischen sicheren CCtx überein,
akzeptierte Frames werden zurückdecodiert.

Release-A/B gegen diesen frischen sicheren Kontext, gleiche Parameter und
Output-Allokation: 11 alternierend angeordnete Samples mit je 512 Trials;
angegeben ist jeweils der Median. Host: Intel Core i7-1370P, VM mit zehn CPUs,
AVX2, Rust 1.97.1; Repository-Releaseprofil mit Overflow-Checks.

| Target | Frischer CCtx, ns/Trial | Neuer Adapter, ns/Trial | Faktor |
| --- | ---: | ---: | ---: |
| 16 KiB | 17.836,1 | 11.570,5 | 1,542× |
| 64 KiB | 50.771,4 | 44.613,8 | 1,138× |
| 256 KiB | 202.190,7 | 193.797,2 | 1,043× |

Die kleine Fixture zeigt einen deutlichen Vorteil und rechtfertigt die enge
Unsafe-Grenze nach `AGENTS.md`. Die großen Trials profitieren weniger.
Threadlokale Kontexte halten native Codec-Allokationen bis zum Threadende;
diese Kosten sind gegenüber dem bisherigen Neuaufbau pro Trial zu beachten.
Die bestehenden SIMD-Loads bleiben unverändert begrenzt und feature-gated.

## Weitere Messungen und Verifikation

Der vorhandene `fastdup-verified-restore-bench` wurde als Baseline und mit der
Implementierung gebaut. Beide verwenden dieselben Abhängigkeiten und dasselbe
Releaseprofil. Drei A/B-Paare wechseln die Reihenfolge; jede Binary liefert
zuvor den Median aus elf Restore-Runden. Die Tabelle zeigt den Median der drei
Prozessmediane. Je Lauf werden 128 deterministische RAW-Chunks über io_uring,
Exact-Lookup, verifizierten Read-Plan und Antwortmontage gelesen; abschließendes
BLAKE3 prüft den gesamten zurückgelesenen Datenstrom. Der Dateisystemcache ist
warm; ein Verified Payload Cache ist in dieser Fixture nicht eingerichtet.

| Chunkgröße | Baseline, MiB/s | Implementierung, MiB/s | Faktor | Physische DATA-Reads, beide |
| --- | ---: | ---: | ---: | ---: |
| 64 KiB | 878,114 | 1.064,128 | 1,212× | 16 |
| 256 KiB | 1.040,683 | 1.193,480 | 1,147× | 64 |

Logs: `restore-ab.txt`, `restore-ab.json` und `restore-*.txt` im
Artefaktverzeichnis. SHA-256 der gesicherten Binaries:

```text
baseline  f1c4634603b51c19dded1001844e2e20e8554c35ca92ea725503c81b13e8ae9c
candidate fb5472c6b57925f72ae842b86d228b93eb9eeaf49609ca35c3cf0b1e5f8a4760
```

Ein zusätzlicher isolierter Sparse-XOR-Vergleich nutzt ein bewusst aussichtsloses
256-KiB-Target, dessen jedes zweite Byte geändert ist, und einen Cap von 4 KiB.
Vollständiges Materialisieren benötigt im Median 582.150,8 ns, der begrenzte
Scan 1.473,6 ns (395,055×). Verglichen werden der unbegrenzte und begrenzte Aufruf
des implementierten Encoders, jeweils 11 alternierende Samples mit 128 Trials.
Das quantifiziert ausschließlich vermiedene Arbeit in dieser Extremfixture,
keinen typischen Write-Durchsatz und keinen separaten SIMD-vs.-Scalar-Gewinn.
Log: `sparse-ab.txt`; Prefix-Log: `prefix-ab.txt`.

Die vollständige Suite von `fastdup-format`, `fastdup-store`, `fastdup-posix`,
`fastdup-appliance` und `fastdup-testkit` besteht: **674 Tests erfolgreich,
0 fehlgeschlagen**, 9 ignorierte Tests. Nach den abschließenden Änderungen an
Sparse-XOR-Validierung und Grenztests wurde Format/Store erneut vollständig
geprüft: **275 Tests erfolgreich**, 4 ignorierte Tests einschließlich der beiden
explizit separat ausgeführten A/B-Tests. Die breitere Suite enthält insbesondere
Recovery-, Scrub-, Publication-, Maintenance- und Ingest-Prüfungen.

Neue gezielte Tests decken RAW-Owner-Lebenszeiten, Payload-Korruption,
identische Chunk-IDs an unterschiedlichen physischen Locations, gemeinsame
Backing-Abrechnung, Base-Wiederverwendung über Requests, Cache-Pressure-Purge,
weiterhin physisch lesenden Scrub, SIMD-Lane-Grenzen, exakte Cost-Caps,
CCtx-Reuse nach Cap-Rejection sowie erfolgreiche und fehlgeschlagene parallele
Record-Reads ab. Die Paralleltests erwarten einen coalesced RAW-Read
beziehungsweise zwei Reads für Base plus abhängigen Target; Timeouts begrenzen
das Warten auf fehlgeschlagene Leader.

`cargo clippy --all-targets -- -D warnings` besteht für Format, Store und
Appliance. `cargo fmt --all --check` und `git diff --check` bestehen ebenfalls.
Testlogs: `tests-all.txt`, `tests-final.txt`, `clippy.txt`.

## Reproduktion und Artefakte

Alle Build-, Test-, Corpus- und Benchmark-Artefakte liegen unter
`/source/fastdup/.artifacts/hotpath-implementation-20260905/`.
`baseline/` hält die vor der Implementierung gesicherten Quelldateien;
`baseline-workspace/` ist der daraus rekonstruierte isolierte Vergleichsstand.
`baseline-restore` und `candidate-restore` sind getrennt gesicherte Binaries;
`source-hashes.json` enthält die Hashes der implementierten Quellen.
Die Messprogramme prüfen zurückgelesene Bytes und verwenden die gepinnte
`Cargo.lock`.

```bash
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
cargo test --release --offline -p fastdup-store --lib prefix_context_reuse_ab -- --ignored --nocapture
cargo test --release --offline -p fastdup-store --lib bounded_sparse_rejection_ab -- --ignored --nocapture
cargo test --offline -p fastdup-format -p fastdup-store -p fastdup-posix -p fastdup-appliance -p fastdup-testkit
cargo clippy --offline -p fastdup-format -p fastdup-store -p fastdup-appliance --all-targets -- -D warnings
python3 /source/fastdup/.artifacts/hotpath-implementation-20260905/restore-ab.py
```

Die zusätzlichen A/B-Tests sind absichtlich ignoriert und werden einzeln im
Releaseprofil gestartet. Lokale CPU-/Warm-Restore-Faktoren sind keine Aussage
über SMB-Durchsatz oder kalte HDD-Latenzen und dürfen nicht miteinander
multipliziert werden. Ein SMB-/HDD-End-to-End-A/B war nicht Teil dieser lokalen A/B-Messungen.
Der anschließend ausgeführte
[SMB-Vergleich zwischen normaler und Advanced Reduction](smb-normal-vs-advanced-2026-09-05.md)
dokumentiert den aktuellen SingleStream-Stand mit diesen Änderungen.
