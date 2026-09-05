# Dritte Hotpath-Optimierung: Umsetzung und Messungen

2026-09-05. Alle sieben konkreten Punkte aus dem
[dritten Audit](../research/hotpath-audit3-2026-09-05.md) sind integriert.
Referenz ist der Arbeitsbaum nach der zweiten Optimierungsrunde, nicht der
ältere Git-HEAD. Das dauerhafte Containerformat, Fingerprint-Profil und die
Codec-Auswahlregeln bleiben unverändert.

## Änderungen

1. **CPU-Admission:** Der gemeinsame Batch-Helfer fragt höchstens so viele
   Permits an, wie tatsächlich Jobs vorliegen. Leere Batches reservieren keine;
   partielle Grants und Ergebnisreihenfolge bleiben erhalten. Kleine
   Base-Trial-Wellen blockieren damit keine ungenutzten Worker mehr.
2. **SIMD-Votes:** 512 i16-Zähler statt i32, 4 statt 8 KiB Delta-Tabelle und
   16 statt acht AVX2-Vote-Lanes pro Addition. Der v1-Maximalwert von 4.096
   Minimizers wird statisch an die Zählerbreite gebunden und beim Akkumulieren
   geprüft. Skalare Ausführung, Golden-Fingerprint und i32-Orakel bleiben
   erhalten. Der AVX2-Kern ist die einzige hier veränderte Unsafe-Schnittstelle.
3. **Containeraufbau:** Der adaptive Writer reserviert eine ausgerichtete
   Kapazität und hängt initialisierte Bytes an. Nur Metadata/Alignment-Lücken
   werden genullt. Gemeinsame feldweise Metadata-Encoder bedienen auch die
   bisherigen Record-APIs; es entsteht kein zusätzlicher vollständiger
   temporärer RAW-/Zstd-Record. CRC und Container-Commitment folgen der
   vollständigen Initialisierung. Der gesamte Format-Code bleibt sicheres Rust.
4. **Region-Materialisierung:** Nur tatsächlich zu materialisierende Regionen
   werden unter dem gemeinsamen CPU-Budget parallel vorbereitet. Borrowed
   Views bleiben beim Koordinator. Die Owner werden nach Regionordinal
   gesammelt, bevor die Advanced-Planung Views darauf erhält. Vorbereitungszeit
   und der Format-Anteil der Copy-Bytes werden separat ausgewiesen.
5. **Antwort-Views:** Mehrere benachbarte DATA-/DATA_SLICE-Extents können einen
   `VerifiedReadView` bis zum FUSE-Reply behalten. Unterschiedliche Owner,
   Quelllücken, wiederholte/rückwärts angeordnete Bereiche und HOLE/FILL führen
   zur bisherigen zusammenhängenden Antwort. Jeder Chunk wird nur einmal
   angefordert; der neue View erhält keine fiktive Chunk-ID.
6. **Dateisperren:** Kurze, auf acht Shards verteilte FD-Cache-Zugriffe;
   dateibezogene Synchronisierung für Open/Stat, Mapping-Leases und Mutationen.
   Rename sperrt beide Namen lexikalisch. Eine gemeinsame FIFO hält weiterhin
   höchstens 128 FDs über alle Shards. Keine Registry-/Cache-Sperre umfasst
   Backend-I/O. Mapping-Leases halten die Root-Registry auch nach dem Drop
   sämtlicher Storage-Adapter am Leben.
7. **Admission-Gruppen:** Ein einzelner Owner benötigt keinen neuen
   Gruppierungs-Vec. Kleine beziehungsweise stark geteilte Batches behalten
   die lineare Suche. Erst nach 32 verschiedenen Ownern und mit mindestens
   32 weiteren Gruppen wird ein temporärer Hash-Index angelegt. Sein Schlüssel
   bezeichnet den tatsächlichen lebenden Backing-Owner, nicht den möglicherweise
   unterschiedlichen Anfangspointer eines Chunk-Ausschnitts.

Die zugehörigen Regeln sind in [ADR 0046](../adr/0046-bound-verified-read-cache-by-live-memory-headroom.md),
[ADR 0050](../adr/0050-overlap-reduction-stages-under-one-memory-and-cpu-budget.md)
und [ADR 0061](../adr/0061-map-immutable-similarity-runs-under-generation-leases.md)
ergänzt.

## Isolierte A/B-Ergebnisse

Mediane aus alternierenden Vergleichssamples; die Faktoren gelten für den
jeweiligen Teilpfad, nicht für SMB. Die Tests liefen ohne parallele Builds oder
SMB-Last. Zehn effektive CPUs; Materialisierung mit acht erlaubten Workern.

| Teilpfad | Lauf 1 alt / neu | Lauf 2 alt / neu | Beobachtung |
| --- | ---: | ---: | --- |
| Tatsächlicher Fingerprint, 32 MiB ISO | 50,479 / 41,264 ms | 50,500 / 44,018 ms | 1,223× / 1,147× |
| Sicherer skalarer / neuer AVX2-Fingerprint | 24,551 / 1,217 ns pro Byte | 24,753 / 1,354 ns pro Byte | 20,181× / 18,288× |
| RAW-Containeraufbau, 32 MiB | 10,289 / 10,187 ms | 9,349 / 9,124 ms | nur 1,010× / 1,025× |
| Ein kleiner CPU-Job plus neun konkurrierende Jobs | 57,241 / 38,931 ms | 55,686 / 39,837 ms | 1,470× / 1,398× |
| Region-Materialisierung, ein / acht Worker | 15,851 / 3,303 ms | 16,320 / 2,748 ms | 4,799× / 5,940× |
| Warmer Read über zwei Extents, Vec / View, 64 KiB | 1.512,7 / 257,6 ns | 1.434,5 / 246,9 ns | 5,872× / 5,809× |
| Warmer Read über zwei Extents, Vec / View, 256 KiB | 5.496,1 / 249,2 ns | 5.173,9 / 250,8 ns | 22,057× / 20,627× |

Beim 4-KiB-Read ist der Effekt deutlich kleiner. Das Read-A/B benutzt die
tatsächlichen `VerifiedManifestFile::read_at`-/`read_shared_at`-APIs mit warmem
verifiziertem Cache und einem Ausschnitt über die gemeinsame Extent-Grenze.
Es misst keine kalten Disks, keine FUSE-Kernelübertragung und keinen SMB-Read.
Der Vec-Kompatibilitätspfad kopiert die verifizierten Antwortbytes einmal.

Die Materialisierungsfixture umfasst 255 Chunks à 128 KiB; die ersten vier
bleiben Borrowed Views, die übrigen besitzen jeweils zwei Fragmente. Gemessen
wird die vollständige Vorbereitung einschließlich Planbildung, Allokationen
und geordneter Sammlung. Das Admission-A/B führt identische BLAKE3-Arbeit
aus und verändert ausschließlich die Permit-Reservierung des einzelnen Jobs.

Der Container-Vergleich verwendet den vorherigen vollständig genullten
Assembler als Test-Orakel. Bei RAW ist das Vollnullen unter diesem Allocator
offenbar kein großer zusätzlicher Zeitanteil. Deshalb wird trotz weniger
vorgeschriebener Nullschreibarbeit kein großer Write-Gewinn behauptet.
Ein vollständiger gemischter Container mit RAW, Zstd, vorbereitetem Independent,
Transplant, Prefix und Sparse-XOR wird bytegleich verglichen und anschließend
mit Base-Resolver unabhängig dekodiert. Injizierte Payload-Korruption schlägt
weiterhin fehl.

### FD-Cache und Sperren

| Warme Dateien | Alt / neu, ns, Lauf 1 | Alt / neu, ns, Lauf 2 |
| --- | ---: | ---: |
| 1 | 98,71 / 32,58 | 111,32 / 37,56 |
| 64 | 137,30 / 44,94 | 136,64 / 48,07 |
| 128 | 136,14 / 49,89 | 140,85 / 49,87 |

Das ist nur `open_read_range` mit warmem FD-Cache, ohne `pread`: etwa
2,7–3,1× schneller. Der neue Hit vermeidet außerdem die vorherige PathBuf-
Allokation. Die tatsächlichen Adapter aus getrennten Vorher-/Nachher-Kopien
werden jeweils auf demselben Host gemessen; der zweite Lauf kehrt die
Adapterreihenfolge um.

Ein kontrollierter zweiter Versuch hält die Mutations-Closure eines anderen
Dateinamens für **künstliche 20 ms** an. Bisher wartet der Read rund
20,2 ms mit. Danach benötigt der warme Read 0,42–0,57 µs, der kalte Open/Stat
24,27–30,55 µs. Das belegt die entfernte rootweite Kopplung unter dieser
künstlichen Backend-Verzögerung; es ist kein allgemeiner I/O-Durchsatzfaktor.

### Cache-Gruppierung

Der erste Challenger indizierte bereits ab 32 Eingabegruppen. Er war bei
wenigen verschiedenen Ownern teilweise langsamer. Die endgültige Auswahl
orientiert sich deshalb zusätzlich an den bereits beobachteten verschiedenen
Ownern. Im zweiten A/B:

| Verschiedene Owner / Gruppen | Linear, ns | Adaptive Gruppierung, ns | Faktor |
| --- | ---: | ---: | ---: |
| 64 / 64 | 2.485,0 | 2.340,7 | 1,062× |
| 128 / 128 | 6.407,8 | 4.177,2 | 1,534× |
| 256 / 256 | 21.703,3 | 11.109,8 | 1,954× |

Kleine und stark geteilte Batches zeigen keinen verlässlichen Zeitgewinn und
legen keinen Hash-Index an. Der Benchmark enthält Aufräumen der Gruppen;
Fixture-Erzeugung und Eingabe-Clones liegen außerhalb der Messung.

### Reproduktion und Unsafe-Nachweis

Alle Dateien liegen unter `.artifacts/hotpath-implementation3-20260905/`:

- `source-before.tar.gz` hält die Quellbasis der zweiten Runde fest;
  `lowlevel-ab/` enthält getrennte Adapter-/Fingerprint-Vergleichskopien.
- `lowlevel-ab{1,2}.txt`: tatsächliches i32/i16-Fingerprint-A/B und FD-Vergleich.
  255 kurze Längen, 1.024 verschieden positionierte/lange ISO-Ausschnitte,
  vier konstante Maximalchunks und 512 Batch-Chunks sind vollständig gleich:
  **1.795 Fälle**. Das ISO ist identisch mit dem SMB-Corpus, Offset 256 MiB.
- `simd-safe-ab{1,2}.txt`: sicherer skalarer Pfad gegen die neue AVX2-Routine;
  sieben alternierende Samples mit je 16 Maximalchunks. Das rechtfertigt die
  schmale Unsafe-Schnittstelle zusätzlich zum Vergleich mit dem alten SIMD-Kern.
- `assembly-ab*`, `admission-ab*`, `materialization-ab*`, `read-reply-ab*`,
  `grouping-ab*`: elf Samples je Seite. `micro-commands.json` und `run_micro.py`
  halten die ausgeführten Befehle fest.

Die reproduzierbaren manuellen Benchmarks liegen als ignorierte Release-Tests
direkt in den entsprechenden Crates. Beispiel:

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test --release -p fastdup-store --lib \
  similarity_fingerprint_scalar_and_avx2_microbenchmark -- --ignored --nocapture
```

Der AVX2-Kern prüft Feature-Dispatch, lädt ausschließlich vollständig
initialisierte, feste Vote-/Tabellenbereiche und bleibt innerhalb des
i16-Profillimits. Maximal gleichgerichtete Votes werden zusätzlich bis ±4.096
gegen ein i32-Orakel geprüft. Neues Unsafe im Container-Writer, im FD-Cache
oder im Antwort-View gibt es nicht.

## SMB: Normal und Advanced

Unveränderter `smb-single-stream-benchmark`-Runner, frische Repositories,
dreimal dasselbe Rocky-10.2-Minimal-ISO seriell, drei live Dateien und zwölf
Sekunden Settle-Zeit vor der Allokationsmessung. Logisch zusammen
6.217.334.784 Bytes. Normal verwendet `off`, Advanced `dependent-v1`.

SMB über Loopback, Port 1445; Samba/smbclient 4.23.5. Metadaten auf
`/dev/sdb1`, Container auf `/dev/sdc1`, jeweils XFS. `lab-allow-shared` mit
deklarierter Small-File-Quota von 1 GiB, keine harte XFS-Projektquota.
`--require-zero-swap` gilt für den Daemon, nicht für historischen Swap anderer
Host-Prozesse. Die Samba-Konfiguration und die ISO-Datei sind unverändert.

### Drei Vergleichspaare je Modus

Die Vorher-/Nachher-Reihenfolge wechselt im zweiten Paar. Alle Tabellenwerte
sind Mediane der drei vollständigen Laufwerte; Gesamt-MiB/s sind gesamte
Bytes durch gesamte Upload-Zeit. Reduktion umfasst allozierte DATA- und
Metadatenblöcke.

| Stand / Modus | Gesamt MiB/s | Erste Kopie MiB/s | Repo Bytes | Reduktion | Daemon-CPU s | Completed-Write p99/max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vorher Normal | 1.049,2 | 612,6 | 2.001.027.072 | 67,815 % | 10,25 | 3.226,2 |
| Jetzt Normal | 1.061,0 | 618,6 | 2.000.527.360 | 67,823 % | 10,27 | 3.195,0 |
| Vorher Advanced | 911,2 | 485,2 | 1.999.343.616 | 67,842 % | 15,44 | 4.073,2 |
| Jetzt Advanced | 941,5 | 515,5 | 1.995.546.624 | 67,904 % | 14,96 | 3.834,3 |

Beobachtet wurden damit **+1,1 % Normal** und **+3,3 % Advanced** beim
Gesamtdurchsatz, bei Advanced **6,2 % mehr** Durchsatz für die erste Kopie
und **3,1 % weniger** Daemon-CPU. Die Werte streuen; dies ist kein Nachweis
einer festen allgemeinen Beschleunigung. Die isolierten Verbesserungen
übersetzen sich insbesondere nicht proportional in SMB-Geschwindigkeit.

Advanced liegt gegenüber Normal noch rund 11,3 % beim Gesamtdurchsatz zurück.
Sein medianer Platzvorteil beträgt lediglich 4.980.736 Bytes beziehungsweise
0,080 Prozentpunkte Reduktion. Auf diesem dreifach identischen ISO dominiert
weiterhin Exact Dedup; der Unterschied rechtfertigt keine Behauptung eines
großen zusätzlichen Similarity-Nutzens.

### Abschlussläufe und alle Rohwerte

Die Vergleichsserie wurde vor der letzten Absicherung der Root-Registry-
Lebensdauer durchgeführt. Dieser zusätzliche Owner und sein Regressionstest
ändern keine Encoding-Regel. Nach dem erneuten Release-Build wurden beide
Modi nochmals mit der endgültigen Binärdatei qualifiziert. Sie sind separat
aufgeführt und gehen nicht als vierter Wert in die Drei-Paar-Mediane ein.

| Lauf (chronologisch) | Gesamt MiB/s | Erste Kopie MiB/s | Repo Bytes | Reduktion | CPU s | p99/max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline-normal-r1 | 1.019,5 | 585,4 | 2.001.354.752 | 67,8101 % | 10,23 | 3.376,3 |
| current-normal-r1 | 1.053,3 | 614,0 | 2.000.039.936 | 67,8312 % | 10,27 | 3.219,1 |
| baseline-advanced-r1 | 955,2 | 522,1 | 2.001.944.576 | 67,8006 % | 15,44 | 3.785,3 |
| current-advanced-r1 | 944,8 | 509,2 | 1.995.476.992 | 67,9046 % | 14,85 | 3.881,6 |
| current-normal-r2 | 1.074,4 | 631,6 | 2.000.601.088 | 67,8222 % | 10,23 | 3.129,3 |
| baseline-normal-r2 | 1.083,3 | 641,1 | 2.000.605.184 | 67,8221 % | 10,52 | 3.083,0 |
| current-advanced-r2 | 940,6 | 515,5 | 1.995.546.624 | 67,9035 % | 14,97 | 3.834,3 |
| baseline-advanced-r2 | 911,2 | 480,3 | 1.995.182.080 | 67,9094 % | 15,30 | 4.115,4 |
| baseline-normal-r3 | 1.049,2 | 612,6 | 2.001.027.072 | 67,8154 % | 10,25 | 3.226,2 |
| current-normal-r3 | 1.061,0 | 618,6 | 2.000.527.360 | 67,8234 % | 10,38 | 3.195,0 |
| baseline-advanced-r3 | 907,3 | 485,2 | 1.999.343.616 | 67,8424 % | 15,44 | 4.073,2 |
| current-advanced-r3 | 941,5 | 524,8 | 2.000.650.240 | 67,8214 % | 14,96 | 3.766,2 |
| current-normal-final | 1.040,9 | 606,3 | 2.000.732.160 | 67,8201 % | 10,44 | 3.259,7 |
| current-advanced-final | 930,5 | 502,9 | 1.997.246.464 | 67,8762 % | 15,27 | 3.930,4 |

Alle **14 Läufe bestanden**, jeweils ohne Daemon-Swap und ohne Cleanup-Fehler.
Nach den Abschlussläufen sind die beiden zusätzlichen Benchmark-Bind-Mounts
und der eigene Samba auf Port 1445 beendet. Der bestehende System-Samba auf
Port 445 läuft weiter.

SHA-256 zur eindeutigen Zuordnung:

- Vorher-Binary: `e18b07f7913a8386d2a9038610fd362ec5d92f28c5cbf84b723a42324feeb5ca`
- Binary der Drei-Paar-Vergleichsserie: `768284b1634f5d0fc65f8dbb6ca8c4e139e4a05651c1993be24ad0ae90f13bb1`
- Endgültiges Binary der beiden `final`-Läufe: `3b6b5473cecf4995e6b6f8d32bb09823147e755fc1fdea6434eba22cc72dc0da`
- Rocky-ISO: `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`
- Samba-Konfiguration: `3a6c4b7507fe3306170a4938b5d8f9d1c201af4f7c2f3c797fda4a30beebfeed`

Der Runner bestätigt abgeschlossene SMB-Writes und Dateilängen, führt aber
keinen vollständigen Hash-Readback aller ISO-Dateien aus. p99 ist bei genau
drei abgeschlossenen Datei-Writes nearest-rank gleich dem Maximum. Es handelt
sich um Dateiabschlusslatenz, nicht um die Latenz einzelner SMB-WRITE-Requests.
Ein SMB-Read-Durchsatztest ist nicht Bestandteil dieses Write-Skills.

Rohreports, Befehle, Konfiguration und Runner-Skripte:
`.artifacts/benchmarks/smb-implementation3-20260905/`.

## Prüfungen und Telemetrie

Sieben Crates: **702 bestandene Tests, 0 Fehler, 15 ignorierte manuelle
Benchmarks/Qualifikationen in 107 Suiten** (`test-all1.txt`). Nach der
Gruppierungsanpassung wurden Store-Unit-, Manifest-Read- und Storage-Range-
Tests wiederholt. Zusätzlich kamen bestandene Tests für die globale
128-FD-Grenze und für eine Lease hinzu, die alle Storage-Adapter überlebt.
Die sechs abschließenden Storage-Range-Tests sind grün (`test-lease-final.txt`).
Release-Build und Clippy über alle Targets der sechs Produktionscrates sind
erfolgreich (`build-release-final.txt`, `clippy-final.txt`). Die abschließende
Formatprüfung und `git diff --check` sind ebenfalls fehlerfrei.

Korrektur gegenüber der früheren Dokumentation: Die Format-Konkatenation wurde
bereits vor dieser Runde als `CompressionRegionMaterialization` gezählt.
`compression_region_concatenation_bytes` ist jetzt ein **Teilzähler** dieser
bisherigen Gesamtsumme. Er darf nicht nochmals zur Gesamtsumme addiert werden.
Die äußere `write_through_materialization wall_ns` misst die gesamte
Vorbereitung einschließlich Admission-Warten, nicht reine CPU-Zeit.
Die frühere falsche Erklärung einer fehlenden Buchung wurde in Audit und
Benchmarkbericht korrigiert.
