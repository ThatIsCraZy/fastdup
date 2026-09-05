# Weitere Write-/Read-Hotpath-Optimierungen, 2026-09-05

Status: Die sieben priorisierten Punkte wurden anschließend implementiert;
Änderungen, Tests und Integrationsmessungen stehen im
[Implementierungsbericht](../benchmarks/hotpath-implementation-2026-09-05.md).
Die folgende Untersuchung dokumentiert den Stand **vor** der Implementierung.

Der untersuchte Arbeitsbaum enthielt konkrete Optimierungsmöglichkeiten.
Die am besten belegte kleine Änderung ist die Entfernung einer doppelten
RAW-Record-CRC-Prüfung. Im Advanced-Reduction-Pfad sind frühe Kostenabbruchgrenzen
und die Wiederverwendung verifizierter Bases besonders interessant. Zusätzliche
handgeschriebene SIMD-Kerne sind dafür überwiegend nicht erforderlich.

Geprüft wurde der Arbeitsbaum auf Basis von `916248d`, einschließlich der
vorhandenen, noch nicht committeten Online-Similarity-/Share-Policy-Arbeit.
Während des ursprünglichen Audits wurden Produktionscode und bestehende
Änderungen nicht verändert. Die
Challenger, Messprogramme und Logs liegen ausschließlich unter
`/source/fastdup/.artifacts/hotpath-audit-20260905/`.

## Prioritäten

| Reihenfolge | Pfad | Befund und vorgeschlagene Änderung | Evidenz |
| --- | --- | --- | --- |
| 1 | Read, RAW | Record-CRC genau einmal prüfen und die geprüfte Struktur intern weiterreichen. | Produktionsdecoder-A/B: 1,18–1,30x über 16–256 KiB. |
| 2 | Write, Advanced Reduction | Unerreichbare Mindestersparnis vor Base-I/O erkennen; Sparse-XOR und Prefix auf den noch gewinnfähigen Preis begrenzen. | Konkreter Kontrollflussbefund; End-to-End-Effekt noch ungemessen. |
| 3 | Read/Write, abhängige Records | Verifizierte unabhängige Bases über mehrere Targets/Requests wiederverwenden. | Cache wird bei Base-Auflösung nicht abgefragt; Wiederverwendung derzeit nur teilweise innerhalb eines Read-Plans. |
| 4 | Read, Antwortmontage | DATA mit `extend_from_slice`, HOLE/FILL mit passender Initialisierung anhängen, statt den ganzen Output zuerst zu nullen. | Isolierte Montage: 1,86x bei 128 KiB, 1,30–1,41x bei 1 MiB. |
| 5 | Write/Read, Zstd | Prefix-CCtx und unabhängigen Read-DCtx wiederverwenden. | Kleiner Prefix-Trial: 1,56–1,58x; große Trials schwanken. Read-Decompression: 1,04–1,10x auf einer einfachen Fixture. |
| 6 | Parallele Reads | Vorhandenes Record-Singleflight auch in den gebatchten und abhängigen Read-Pfad integrieren. | Die beiden Pfade umgehen heute den Coordinator. |
| 7 | RAW-Read, Zero-Copy | Den eingelesenen Record-/Batch-Owner als verifizierten Payload-View behalten. | Eine vollständige RAW-Payloadkopie ist noch vorhanden; Ownership-Umbau nötig. |

Die Faktoren beschreiben jeweils die gemessene lokale Operation, keinen
SMB-/HDD-Gesamtdurchsatz. Sie dürfen weder addiert noch multipliziert werden.

## Wiederholte Verifikation im Reader

In `crates/fastdup-format/src/container.rs:3695` prüft
`decode_encoding_record_mode` die vollständige Record-CRC. Bei RAW ruft es
anschließend `decode_raw_record_view` auf; dieser prüft dieselbe CRC bei
Zeile 5131 erneut. Der produktive Exact-Read erreicht diesen Weg über
`decode_candidate_payloads`. Der Chunk-BLAKE3 ist hingegen eine eigenständige,
erforderliche Integritätsprüfung und bleibt erhalten.

Ein isolierter Challenger lässt bei RAW die äußere CRC-Prüfung aus und behält
die vollständige Prüfung im RAW-Decoder. Das liefert bei identischer Ausgabe
einen reproduzierbaren Vorteil. Für eine Integration wäre eine intern einmal
validierte Record-Sicht sauberer: Öffentliche Decoder müssen weiterhin
unabhängig validieren können; interne Weitergabe darf keine frei fälschbare
"schon geprüft"-Flag sein. Fehlerpriorität bei gleichzeitig beschädigtem
Header und Checksumme sollte bewusst festgelegt werden.

Eine verwandte Doppelung betrifft abhängige Records:
`decode_dependent_candidate_using` (`container.rs:1038`) ruft zunächst
`DependentRecord::dependency` auf. Der anschließende eigentliche Decode ruft
für Prefix beziehungsweise Sparse-XOR erneut `Self::dependency` auf
(`container.rs:4253`, `container.rs:4563`). Beide Wege validieren jeweils die
vollständige Record-Struktur und CRC. Bei Sparse-XOR wird außerdem die Run-Tabelle
erneut in einen `Vec` decodiert (`container.rs:4887`). Eine geprüfte Record-Sicht
könnte Dependency, Run-Geometrie und CRC-Evidenz gemeinsam tragen. Der zusätzliche
Gewinn für diese Codecs wurde hier nicht gemessen.

## Advanced Reduction: aussichtslose Arbeit früher beenden

In `crates/fastdup-store/src/persistent_reduction.rs:337` wird das unabhängige
Encoding vorbereitet. Danach folgen Base-Lesen, Sparse-XOR und Prefix; erst
bei Zeile 421 wird die Mindestverbesserung von 4 KiB und 5 Prozent angewendet.

Wenn das unabhängige Encoding beispielsweise nur 2 KiB belegt, kann kein
abhängiges Encoding weitere 4 KiB einsparen. Trotzdem kann der aktuelle Pfad
noch verifizierte Base-Reads und Codec-Trials ausführen. Bereits unmittelbar
nach dem unabhängigen Encoding lässt sich dieser Fall ohne Heuristik beenden.

Allgemein ergibt sich die maximal akzeptable abhängige Größe aus:

```text
max_dependent = independent - max(4096, ceil(independent * 5 / 100))
```

Nicht darstellbare/negative Grenzen und codecbedingt unmögliche Größen bedeuten
sofortigen Fallback. Ein bereits gefundener besserer Kandidat verengt den Cap
zusätzlich; die bisherige Gleichstandsregel bleibt erhalten.

Sparse-XOR hat derzeit überhaupt keinen Output-Cap:
`reduction_similarity.rs:1075` baut alle Runs und XOR-Bytes auf. Danach werden
die Run-Ranges in `DeltaRun` und bei `into_prepared_record` nochmals in
`SparseXorRun` umgewandelt. `persistent_reduction.rs:381` bereitet sogar einen
Trial vollständig auf, bevor geprüft wird, ob er den bisher besten schlägt.
Verwerfen vor dieser Konvertierung spart zusätzliche Allokationen.

Ein extremes, aber zulässiges Beispiel ist ein 256-KiB-Target mit jedem zweiten
Byte geändert: 131.072 Runs und 131.072 XOR-Bytes ergeben im aktuellen Kostenmodell
1.179.684 Bytes. Dieser Kandidat kann das unabhängige Encoding nicht schlagen,
wird aber vollständig materialisiert.

Die vorhandene AVX2-Gleichheitsmaske in
`crates/fastdup-store/src/similarity_simd.rs:101` bietet die passende Lowlevel-Naht:
Aus der invertierten Maske lassen sich geänderte Bytes und beginnende Runs
zählen. Die monoton wachsenden Kosten `36 + 8 * runs + xor_bytes` erlauben einen
exakten Abbruch schon während des Scans. Run-Übergänge zwischen 32-Byte-Lanes
müssen berücksichtigt werden. Das benötigt keine zweite Vorprüfung über die
ganzen Bytes und kann das bestehende SIMD-Ergebnis verwenden.

Als nachrangigen SIMD-Versuch würde ich gemischte Masken mit `trailing_zeros`/
`trailing_ones` in zusammenhängende Bereiche zerlegen. Der aktuelle Kernel
behandelt eine vollständig gleiche Lane schnell, fällt bei jeder gemischten
Lane aber in 32 Einzelbyte-Entscheidungen zurück. Zuerst sollte der Kosten-Cap
integriert werden, damit kein schnellerer Kernel lediglich aussichtslose
Trials beschleunigt. Für diese beiden Änderungen liegt hier noch kein A/B vor.

## Base-Wiederverwendung und fehlendes Singleflight

`ManifestReader::read_cached_many` (`manifest_reader.rs:505`) prüft den Cache
für angeforderte Targets. Die spätere Base-Auflösung in
`ContainerRepository::find_verified_independent_base_read_with_index`
(`fastdup-store/src/lib.rs:2533`) führt dagegen Exact-Lookup und Record-Read aus,
ohne den gemeinsamen Verified Read Cache abzufragen.

Der gebatchte Read besitzt bereits eine lokale `verified_bases`-Map
(`lib.rs:2937`). Sie verhindert Wiederholungen innerhalb dieses einen Plans.
Bei einem späteren Request für einen anderen Target-Chunk derselben Base wird
diese wieder eingelesen und verifiziert, obwohl die vorherige Base als
Admission-Gruppe an den Cache weitergereicht wurde. Der Writer ruft dieselbe
Base-Auflösung je Target erneut auf. Eine begrenzte Base-Wiederverwendung auf
Batch-Ebene ist deshalb ein konkreter Kandidat für weniger I/O, Decode und Hash.

Die Base-Eignung darf dabei nicht aus einem nackten Cache-Hit nach Chunk ID
abgeleitet werden. Ein Cache-Payload allein trägt keine verifizierte unabhängige
Location-Provenienz. Unabhängige Location, aktuelle Auswahl-/Generation-Pins,
Depth-one und der GC-Publication-Guard müssen erhalten bleiben. Ein passender
Base-Proof-Owner kann verifizierte Bytes und diese Evidenz gemeinsam tragen.

Zusätzlich wandelt `find_verified_independent_base_with_index` (`lib.rs:2511`)
den vorhandenen `VerifiedChunkPayload` in einen `Vec<u8>` um. Bei einem Chunk-View
aus einem mehrteiligen Zstd-Record erzwingt `into_payload` eine weitere Kopie.
Der Writer könnte den Owner behalten und für den Trial nur `as_slice()` borgen.

Record-Singleflight ist vorhanden, aber nur im skalaren unabhängigen
`read_verified_location_payload` (`lib.rs:2204`). Der gebatchte Plan liest bei
Zeile 2864 direkt mit `read_exact_at`, und
`read_verified_dependent_location_payload` (`lib.rs:2275`) geht ebenfalls am
Coordinator vorbei. Gleichzeitige Cache-Misses können daher denselben Record
mehrmals lesen und decodieren. Bestehende Singleflight-Tests decken vor allem
skalare Geschwister-Reads ab. Ein Umbau muss physische Coalescing-Reihenfolge,
Fehlerweitergabe und begrenzte Flight-Lebenszeiten bewahren.

## Initialisierung, Ownership und Codec-Kontexte

`assemble_manifest_read` (`manifest_reader.rs:675`) reserviert den gesamten
Output, initialisiert ihn mit Null und überschreibt danach DATA und FILL.
Die validierte Manifestpartition wird bereits in logischer Reihenfolge
durchlaufen. Daher reicht sicheres Anhängen: DATA per `extend_from_slice`,
HOLE/FILL per `resize` mit dem jeweiligen Wert. Der bisherige Coverage-Check
bleibt erhalten. Der gemessene Montagekernel enthält ausschließlich DATA;
Sparse-Overlays und vollständige Manifestplanung sind nicht Teil dieser Zahl.

Der RAW-Decoder kopiert nach seiner Verifikation weiterhin `payload.to_vec()`
(`container.rs:3735`). Eine owned Record-/Batch-Sicht könnte diese Kopie entfernen.
Der aktuelle Typ verwendet dasselbe `offset` jedoch als Backing-Offset und als
verifizierte `decoded_offset`-Koordinate. Für einen RAW-View hinter dem 192-Byte-
Record-Header müssten diese Begriffe getrennt werden. Bei gemeinsam gehaltenen
1-MiB-Batches muss die vollständige Backing-Kapazität weiter unter den
Memory-Governor fallen; kleine Views dürfen keinen unkontrollierten Retention-
Multiplikator erzeugen. Ein anschließender Scatter/Gather-FUSE-Reply wäre ein
größerer, getrennt zu messender Umbau des heutigen `Vec<u8>`-Interfaces.

`ZstdPrefixCodec::encode_prehashed_trial` (`reduction_prefix.rs:306`) erzeugt
für jeden Trial einen neuen CCtx. Unabhängige Encoder haben bereits einen
worker-lokalen Kontext; der Prefix-Pfad noch nicht. Das A/B verwendet einen
batch-lokalen CCtx, vollständigen Session-/Parameter-Reset und pro Trial erneut
`ref_prefix`, dieselben Parameter und denselben Output-Cap. Es vergleicht
byte-identische Frames sowie Cap-Rejections. Alle Base-Slices leben im
Benchmark länger als der Kontext. Die geliehene Prefix-Lebenszeit im
`zstd-safe`-Interface verbietet, daraus ohne weiteres einen statischen
Thread-Local mit kurzlebigen Bases zu machen. Ein begrenzter Batch-Owner ist
zuerst zu prüfen; ein FFI-Lebenszeitadapter bräuchte eine eigene Unsafe-Evidenz.

Unabhängige Zstd-Reads verwenden `zstd::bulk::decompress`
(`container.rs:3802`), das in der installierten Bibliotheksversion einen neuen
Decompressor anlegt. Ein worker-lokaler Decompressor ist einfacher wiederzuverwenden
als der geliehene Prefix-Kontext. Die hier gemessene einförmige 256-KiB-Fixture
liefert nur begrenzte Evidenz; ein gemischter Restore-Corpus bleibt erforderlich.

Auch der adaptive Writer initialisiert mehrfach: erst das gesamte Containerbild
(`container.rs:6286`), danach den vollständigen jeweiligen RAW-/Zstd-Record
(`container.rs:5090`, `container.rs:3377`), dann dessen Payload. Der Challenger
begrenzt die zweite Initialisierung auf Header/Chunk-Tabelle und Padding. Die
Containerbytes bleiben identisch. Der Gewinn ist klein und schwankend, daher
nachrangig gegenüber Reader-CRC und vermiedenen Trials. Uninitialisierter
Unsafe-Speicher ist dafür nicht nötig.

## Gemessene Ergebnisse und Prüfungen

Host: Intel Core i7-1370P unter einer VM mit zehn CPUs, AVX2/BMI2/POPCNT,
Rust 1.97.1. Releaseprofil mit Overflow-Checks, zwei komplette Durchläufe,
jeweils elf Samples mit alternierender A/B-Reihenfolge. Die folgenden Bereiche
sind die beiden Median-Speedups, keine statistischen Konfidenzintervalle.

| Operation | Größe | Lauf 2 | Lauf 3 |
| --- | ---: | ---: | ---: |
| RAW `decode_candidate_payloads`, eine statt zwei CRC-Prüfungen | 16 KiB | 1,298x | 1,238x |
| gleicher Decoder | 64 KiB | 1,179x | 1,204x |
| gleicher Decoder | 256 KiB | 1,208x | 1,213x |
| reine DATA-Antwortmontage, safe append | 128 KiB | 1,856x | 1,861x |
| gleiche Montage | 1 MiB | 1,411x | 1,303x |
| Prefix-Kontext-Reuse, sparse akzeptiert | 16 KiB | 1,575x | 1,563x |
| Prefix-Kontext-Reuse, Cap-Rejection | 16 KiB | 1,394x | 1,411x |
| Prefix-Kontext-Reuse, sparse akzeptiert | 64 KiB | 1,070x | 1,152x |
| Prefix-Kontext-Reuse, sparse akzeptiert | 256 KiB | 0,983x | 1,107x |
| unabhängiger Zstd-Decompressor-Reuse | 256 KiB | 1,097x | 1,040x |
| adaptiver Writer, weniger Record-Nullung | 128 KiB | 1,009x | 1,052x |
| gleicher Writer | 4 MiB | 1,003x | 1,030x |

Die Reader-Messung benutzt den produktiven öffentlichen Formatdecoder mit
vorbereitetem Descriptor/Exact-Kandidaten und behält dessen verifizierte
Payload-Owner. I/O und Manifestplanung sind ausgeschlossen. Das Writer-A/B
enthält den vollständigen prehashed adaptiven Encoder einschließlich Zstd-Trial,
Record-CRC und Sealing bei einem Worker und Gate Off; es enthält keine
Publication. Die synthetischen Writerdaten sind inkompressibel.

Beide finalen Läufe verwenden dieselben Abhängigkeitsversionen wie die
Repository-`Cargo.lock`. `run-1.txt` war ein vorbereitender Versuch mit
abweichenden Dependency-Versionen und zusätzlicher Owned-Ausgabekonvertierung;
er ist ausdrücklich nicht Grundlage dieser Tabelle.

Alle 121 übernommenen Format-Tests bestehen gegen den isolierten Challenger.
Zusätzlich verwerfen beide Varianten 922 gezielte Einzelbyte-Korruptionen in
RAW-Header/Chunk-Tabelle, Payload-Stichproben und Padding. Die Writer-Ausgaben
sind byte-identisch, akzeptierte Prefix-Frames werden zurückdecodiert, und
akzeptierte sowie abgewiesene Prefix-Trials stimmen zwischen Fresh/Reuse überein.
Das ersetzt nicht die Store-Recovery-/Offline-Scrub-/FUSE-Fault-Matrix vor einer
Produktionsintegration. Ein aktueller SMB-/HDD-End-to-End-A/B wurde nicht gefahren.

Artefakte: `src/main.rs`, `format-challenger.patch`, `Cargo.lock`,
`source-hashes.json`, `run-2.txt`, `run-3.txt`, `format-tests.txt` im oben
genannten Auditverzeichnis. Reproduktion:

```bash
mkdir -p /source/fastdup/.artifacts/tmp /source/fastdup/.artifacts/target
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
cargo run --release --offline \
  --manifest-path /source/fastdup/.artifacts/hotpath-audit-20260905/Cargo.toml
cargo test --release --offline -p format-challenger \
  --manifest-path /source/fastdup/.artifacts/hotpath-audit-20260905/Cargo.toml
```

## Bereits gut abgedeckte Lowlevel-Pfade

SeqCDC, Similarity-Votes und Sparse-XOR-Gleichheitserkennung besitzen bereits
SIMD-Implementierungen mit skalaren Orakeln. Exact-/Similarity-/GC-mmap,
FUSE-Receive-Ownership, fragmentfähiger Zstd-Input, prehashed Writer-Evidenz,
worker-lokale unabhängige Encoder und CQE-Scratch sind ebenfalls vorhanden.
Im aktuellen Produktionspfad werden Target-/Base-Hashes der vorbereiteten
abhängigen Trials bereits wiederverwendet; die selbstprüfenden Test-/Convenience-
Interfaces dürfen nicht mit diesem Pfad verwechselt werden.

Neue unchecked Feldzugriffe, eigenes `memcpy`, ein BLAKE3-Ersatz oder eine
pauschale AVX-512-Baseline sind durch diesen Audit nicht begründet. Die
vorhandene Evidenz in den Audits vom 2026-09-01 bleibt relevant. Die nächsten
lohnenden Schritte reduzieren die Zahl der Byte-Durchläufe, Base-Reads und
Codec-Trials, bevor sie einzelne Instruktionen ersetzen.
