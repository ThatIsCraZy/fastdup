# Hotpath-Runde 4: Umsetzung und Messung

Implementiert am 2026-09-05; Abschlussprüfung am 2026-09-06 (Europe/Berlin).
Basis: `a2c59d1`. Alle sieben priorisierten Punkte aus
[Audit 4](../research/hotpath-audit4-2026-09-05.md) sind umgesetzt, einschließlich
des Prefix-API-Mismatch. Kein dauerhaftes Container-Format wurde verändert;
es kommt kein neues `unsafe` hinzu.

## Änderungen

1. **Sichtbare Read-Intervalle:**
   [ReadPlan](../../crates/fastdup-posix/src/versioned_file.rs) löst Active,
   Frozen und Committed von neu nach alt auf. Verdeckte DATA/HOLE-Bereiche
   veranlassen keinen älteren Read. Dirty-Daten bleiben unveränderliche Bytes-Views.
2. **Live-Reader und API-Mismatch:**
   [VerifiedLocationFile](../../crates/fastdup-appliance/src/checkpoint.rs)
   hält einen Chunk-Ausschnitt eines gemeinsamen veröffentlichten Read-Rezepts.
   [VerifiedManifestFile](../../crates/fastdup-store/src/manifest_reader.rs)
   verwendet die writer-getragenen Locations als Kandidaten im gemeinsamen
   verifizierten Record-Reader. Er löst Prefix-/Sparse-XOR-Bases auf, nutzt den
   gemeinsamen Cache und funktioniert auch vor Target-Indexaktivierung. Die
   Konstruktion liest keine DATA erneut. Inode-Sperren werden vor DATA-I/O gelöst.
3. **Externalisierung als Batch:** Disjunkte akzeptierte Kandidaten werden
   gemeinsam aus SparseData entfernt. Überlappende Kandidaten behalten ihre
   Eingabereihenfolge; Active-/Frozen-Sequenzprüfungen bleiben bestehen.
4. **CPU-Permits:** Hash-Shards und Encode-Regionen begrenzen ihre Anforderung
   vor Erwerb. Abgeschlossene Worker geben Permits früh zurück; für serielle
   Assembly bleibt eines erhalten. CPU-Zuteilung zählt die tatsächlich
   reservierte CPU-Arbeit; wartende Ingest-I/O verkleinert keine statische Quote.
5. **Advanced-Wellen:** Fertige Target-Slots werden vor der nächsten CPU-Phase
   nachgefüllt; identische verifizierte Bases werden innerhalb einer Welle
   geteilt. Höchstens acht Base-Owner, ursprüngliche Trial-Reihenfolge und
   -Budgets bleiben erhalten. Es gibt kein neues paralleles HDD-I/O.
6. **Zstd-Zielpuffer:** `zstd_owned_payload` nutzt den sicheren Vec-WriteBuf-Pfad
   ohne vorheriges Nullfüllen. Die tatsächlich geschriebene Länge wird weiterhin
   gegen das Policy-Cap geprüft, auch wenn der Allocator zusätzliche Kapazität gab.
7. **Segmentierte FUSE-Replies:** Verifizierte Owner bleiben bis zum einmaligen
   `writev` erhalten. Ab mehr als 128 Body-Segmenten wird gebündelt. Bestehende
   Bytes-/Vec-APIs bleiben nutzbar; kurze Device-Writes werden als Fehler behandelt.
   Das spart Userspace-Kopien, ist kein Kernel-Zero-Copy.

## Verifikation

- 548 Release-Tests in Format, Store, POSIX und Appliance erfolgreich;
  zusätzlich 149 Testkit-Tests, einschließlich Recovery, Scrub und Fehlerfällen.
  15 bereits manuell/umgebungsabhängig markierte Tests blieben ignoriert.
- Clippy für alle Targets dieser vier Crates erfolgreich.
- FUSE-Features `async-io-runtime,unprivileged` und
  `tokio-runtime,unprivileged` separat kompiliert.
- Echter FUSE-Mount-Smoke-Test erfolgreich. `strace` zeigt Antworten mit bis
  zu sieben IOVs in einem `writev`, einschließlich erfolgreicher Byte-Prüfung
  der gemischten Reads im Smoke-Test.
- Neue Regressionen prüfen stabile Dirty-Owner trotz späterer Mutation,
  sichtbare Teilintervalle, verzögerte externe Reads und verdeckte Frozen-Daten.
- Live-Prefix-Regression: Target fehlt noch im aktivierten Exact Index, Base
  ist aktiviert; kompletter und partieller Read sind bytegleich. Die
  Reader-Konstruktion verursacht keine Storage-Operation.
- Live-Record-Regression: acht 16-KiB-Chunks eines 512-KiB-Zstd-Records benötigen
  einen DATA-Range-Read und behalten einen gemeinsamen Owner. Eine anschließende
  Record-Beschädigung wird beim unabhängigen Read abgewiesen.
- Ein blockierter Worker hält nach Abschluss seiner drei Mit-Worker nur noch
  ein Permit. Auch ein injizierter Worker-Panic gibt alle Permits frei.
- 25 gemischte Targets liefern mit einem und vier Workern dieselben vorbereiteten
  Records und Codec-Entscheidungen wie serielle Advanced-Planung.

## Lokales Read-A/B

Kopien des tatsächlichen VersionedFile-Codes vor/nach der Änderung, gleicher
in-memory CommittedFile und gleicher Compiler; je neun wechselnd angeordnete
Samples. 5.000 Reads/Sample bei 4/64 KiB, 500 bei 1 MiB. Gemessen wird
`plan_read` plus die kompatible zusammenhängende `execute_shared`-Antwort.
Die Fixtures sind vollständig resident, mit 0, 1 oder 16 Dirty-Extents.
Dies misst keine FUSE-/SMB-Latenz und keine HDD-Performance.

| Anfrage / Dirty-Extents | Baseline | Neu | Faktor |
| --- | ---: | ---: | ---: |
| 4 KiB / 1 | 298,7 ns | 75,2 ns | 3,97x |
| 64 KiB / 16 | 27,90 µs | 1,736 µs | 16,07x |
| 1 MiB / 16 | 991,34 µs | 44,26 µs | 22,40x |
| 64 KiB / 0 | 94,4 ns | 82,6 ns | 1,14x |

Die Einzel-Extent-Fälle behalten den vorhandenen Byte-Owner und berühren keine
Payload erneut. Deren sehr große Faktoren im vollständigen Rohprotokoll
beschreiben die eingesparte Kopierarbeit, keinen möglichen Netzwerkdurchsatz.

## SMB SingleStream: Normal und Advanced

Unveränderter Runner aus `smb-single-stream-benchmark`: Rocky-10.2-Minimal-ISO,
2.072.444.928 Bytes, drei serielle Kopien pro Lauf; 12 Sekunden Settle, drei
lebende Dateien bei der Messung. Pro Variante drei gültige Läufe (neun Uploads),
abwechselnde A/B-Reihenfolge. Separate XFS-Dateisysteme `/dev/sdb1` und `/dev/sdc1`,
identische Samba-Konfiguration auf dem isolierten Port 1445, keine Daemon-Swap-Nutzung.
`off` bedeutet Normal, `dependent-v1` aktiviert Advanced/Similarity.

ISO-SHA256: `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
Baseline-Binary: `673ecb0ee614c6d87d56ba176e795862141af57c5c109da6371a07a8e2d9b8b3`.
Neues Binary: `4149608281b79417048b482b959461a5bf28896bc49f944e01791d38d23fd05a`.

Die Tabelle enthält Mediane über drei Läufe. Die letzte Spalte ist der Median
des größten abgeschlossenen Datei-Writes pro Lauf; bei drei Samples entspricht
dies dem Runner-p99, nicht einer Per-SMB-Request-p99.

| Build | Modus | Schreiben MiB/s | Reduction inkl. Metadata | Belegt MiB | Daemon-CPU s | Datei-Write-Max/p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| baseline | normal | 991.0 | 67.5933 % | 1921.49 | 15.01 | 2569.2 |
| current | normal | 1022.4 | 67.4095 % | 1932.39 | 14.68 | 2348.1 |
| baseline | advanced | 993.2 | 67.7401 % | 1912.79 | 21.79 | 3015.4 |
| current | advanced | 1011.8 | 67.7194 % | 1914.02 | 22.06 | 3023.4 |

Normal gewinnt im paarweisen Median **3,17 %** Geschwindigkeit
(Einzelpaare +8,95 %, +3,03 %, +3,17 %). Sein medianer Platzverbrauch steigt dabei
um **10,90 MiB**, die Reduction sinkt um **0,184 Prozentpunkte**.
Advanced zeigt **keinen stabilen Geschwindigkeitsgewinn**: Einzelpaare
−8,07 %, −0,89 %, +2,27 %, Median −0,89 %. Der Vergleich allein der unpaarigen
Geschwindigkeitsmediane würde hier ein zu günstiges Bild erzeugen.

Mit dem neuen Build spart Advanced gegenüber Normal median **18,37 MiB**
Repository-Platz bzw. **0,310 Prozentpunkte** zusätzliche Reduction. Die
Write-Geschwindigkeitsmediane unterscheiden sich um rund 1 %, während die
Daemon-CPU-Zeit von 14,68 auf 22,06 Sekunden steigt. Dieses ISO mit drei
identischen Kopien wird stark von Exact-Deduplizierung geprägt; andere Backup-
Korpora können deutlich andere Similarity-Gewinne liefern.

Das erste Normal-A/B-Paar wurde verworfen, weil die lokale Read-Probe den
Baseline-Upload überlappte. Runde 4 ersetzt es vollständig. Auch die lokale
Read-Probe wurde anschließend allein wiederholt. Verwendet werden Normal-Runden
2/3/4 und Advanced-Runden 1/2/3. Die ausgeschlossenen Rohdaten bleiben erhalten.
Keine dieser Messungen ist eine physische HDD-Seek- oder Cold-Cache-Messung.

## Weiterer Befund: zusätzliche Arbeit in Commit-Tails

Der Normal-Checkpoint verarbeitet in den gültigen neuen Läufen 47,54 / 61,00 /
66,36 MB nochmals durch Chunking, gegenüber 15,18 / 31,15 / 16,48 MB in den
zugehörigen Baselines. Ein großer Teil wird anschließend per Exact-Reuse wieder
verwendet. Das ist ein konkreter weiterer Ansatzpunkt: warum zum Freeze-/Commit-
Zeitpunkt nicht mehr unveränderte Bereiche als vorbereitete Rezepte vorliegen.
Der Zusammenhang mit Pipeline-/Commit-Timing ist plausibel; die Ursache des
zusätzlichen belegten Platzes ist mit diesen Messungen noch nicht isoliert.
Ein neuer Container-Codec oder weiteres Unsafe ist durch diesen Befund nicht
begründet.

## Reproduktion und Rohdaten

- `.artifacts/hotpath-implementation4-20260905/`: gesicherte Baseline, aktuelles
  Binary, Quellsnapshot, Read-Probe mit Provenienz, Test-/Clippy-Protokolle und
  FUSE-Smoke-/writev-Protokolle. Maßgeblich: `test-final.txt`, `testkit.txt`,
  `clippy-verified.txt`, `read-probe-final.txt`.
- `.artifacts/benchmarks/smb-implementation4-20260905/`: unveränderte
  Runner-Aufrufe, beide Wrapper, Dry-Runs, Einzel-JSONs, Konfigurationskopie,
  `excluded-runs.json` und `summary.json`.
- Sämtliche Builds verwenden workspace-lokale `CARGO_TARGET_DIR` und `TMPDIR`.
  Benchmark-Mounts und der isolierte Samba-Prozess wurden nach Abschluss entfernt.
