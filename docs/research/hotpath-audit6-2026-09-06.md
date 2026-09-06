# Sechster Hotpath-Audit: Proof-Speicher, Arbeitsgranularität und Exact-Audits

Geprüft wurde der Arbeitsstand nach Implementierungsrunde 5. Dieser Audit
ändert keinen Produktionscode. Sichere Prototypen und instrumentierte Proben
liegen ausschließlich unter `.artifacts/hotpath-audit6-20260906/`.

## Ergebnis

| Priorität | Neuer Ansatz | Evidenz und Reichweite |
| --- | --- | --- |
| 1 | Active/Frozen Proofs als kompakte Arena mit Hash-Index | Bei 65.536 Einträgen 310 → 37 ns je Lookup inklusive Mutex; zusätzlicher Prozess-RAM 16,8 → 9,8 MiB. |
| 2 | Hash-Worker nach Arbeitsumfang anfordern | Echte Klassifizierung: 1 MiB aus 64 Chunks 379 → 149 µs mit zwei statt zehn Workern. 32 MiB profitieren weiterhin von zehn Workern. |
| 3 | Unveränderte, bereits auditierte Exact-Reader weiterreichen | Ein Append von 32 Einträgen auditiert denselben alten Run mit 65.536 Einträgen zweimal: zusammen rund 14 ms eines 21-ms-Aufrufs. Ablauf gemessen, noch kein Challenger. |
| 4 | Kleinen konsumierenden Owner für einzelne Read-Antworten verwenden | Owner-Körper 176 → 24 Byte; 4-KiB-Antwortverpackung 46,6 → 30,2 ns. Keine Payload-Kopie auf beiden Seiten. |
| 5 | Pending-Invarianten inkrementell erhalten | Nachbildung der bestehenden Aufruffolge mit echten Pending-Typen: bei 64-KiB-Chunks 32,6 → 17,2 µs pro 32-MiB-Pending-Aufbau. |
| 6 | Prefix-Decoding direkt in die Vec-Kapazität schreiben | Bei 64–256 KiB etwa 3–6 % weniger Zeit in der vollständigen lokalen Decode-Probe. Kleiner nachgelagerter Kandidat. |

Das sind lokale Messungen und ein Ablaufbefund, keine gemessenen SMB-Zuwächse.
Insbesondere die nanosekundenschnelle Read-Verpackung macht nur einen kleinen
Teil eines Reads aus. Der größere strukturelle Ansatz im Exact-Publisher ist
noch nicht als vollständige Alternative implementiert oder per A/B bewertet.

## 1. Kompakte Generation Proofs

`crates/fastdup-appliance/src/checkpoint.rs:661` speichert Active/Frozen als
`BTreeMap<(ChunkId, u32), GenerationProof>`. Der 36-Byte-Schlüssel wiederholt
Informationen im 144-Byte-Proof. Die Baumstruktur benötigt zusätzlich freie
Slots und Knotenzeiger; jeder Lookup hält die gemeinsame Generation-Sperre.
Runde 5 hat Lookup und Promotion bereits zusammengelegt. Hier geht es um
die verbleibende Datenstruktur, nicht erneut um diese Zusammenlegung.

Der Challenger enthält `Vec<GenerationProof>` und `hashbrown::HashTable<u32>`.
Die Tabelle speichert nur den Arena-Index. Die Auswahl prüft stets die volle
Chunk-ID und logische Länge; der kurze Tabellenhash ist keine Identität.
Das Hash-Mixing folgt dem vorhandenen Historical-Proof-Cache. Frozen lässt sich
als ganzer Owner übertragen. Beim Abschluss wird die Tabelle freigegeben und
die konsumierte Arena nach dem bisherigen Schlüssel sortiert. Damit bleibt
die Reihenfolge der Historical-Admission erhalten.

Mediane aus `proof-repeat.txt`, mit einem Mutex auf beiden Lookup-Seiten:

| Einträge | BTree-Lookup | Arena-Lookup | Aufbau und sortierte Entnahme BTree | Arena |
| ---: | ---: | ---: | ---: | ---: |
| 1.024 | 77,48 ns | 19,02 ns | 0,186 ms | 0,167 ms |
| 32.768 | 187,56 ns | 24,48 ns | 10,796 ms | 9,678 ms |
| 65.536 | 309,96 ns | 37,25 ns | 26,990 ms | 20,345 ms |

Die erste Wiederholung mit konsumierender Entnahme (`proof-final.txt`) ergab
bei 65.536 Einträgen 304,70 → 39,56 ns und 25,187 → 18,907 ms.
Der vollständige Probe-Lebenszyklus wird damit ebenfalls schneller. Eine
frühere Probe klonte die Arena zur Entnahme und war bei 32.768 Einträgen
teilweise langsamer; dieses zusätzliche Klonen braucht die Umsetzung nicht.
Die Lebenszyklusprobe enthält Aufbau, Sortierung, Ausgabe und Freigabe, aber
keine Historical-Cache-Admission oder vollständige Commit-Pipeline.

Frische Prozesse, jeweils identische bereits angelegte Eingabefixture:

| 65.536 Proofs | Zusätzlicher VmRSS |
| --- | ---: |
| BTree | 17.588.224 Byte |
| Arena und Hash-Index | 10.313.728 Byte |
| Differenz | −7.274.496 Byte, rund −41 % |

Die direkt abgefragte Arena-/Tabellen-Allokation beträgt 10.092.560 Byte.
VmRSS enthält zusätzlich Allocator-/Prozess-Effekte. Das ist keine Aussage über
den gesamten fastdup-Prozess. Active und Frozen dürfen zusammen höchstens
65.536 Proofs enthalten. Die ebenfalls erhaltenen Proben mit 131.072 bzw.
262.144 Einträgen sind reine Skalierungsproben oberhalb dieser Grenze und
begründen keine größere Produktiv-Ersparnis.

Bei Umsetzung müssen Full-Key-Vergleich, ExactReuse-Vorrang, Frozen/Active-
Promotion, Publikations-Claims, Budget und Trace-Ereignisse erhalten bleiben.
Die Kapazitäten beider Arenen und Tabellen müssen ins RAM-Accounting eingehen;
`len * entry_size` reicht wegen Reserven und gleichzeitigem Active/Frozen
nicht. Die Lookup-Probe ist unkontendiert. Sie belegt kürzere Sperrzeit,
keinen gemessenen MultiStream-Skalierungsfaktor. Weiteres Sharding wäre ein
eigener Eingriff in Freeze- und Claim-Koordination.

## 2. Arbeitsgranularität vor Queue-Umbau

`checkpoint.rs:3725` begrenzt die Hash-Worker bisher durch
`min(worker_budget, ceil(chunk_count / 4))`. Das berücksichtigt weder die
Bytezahl noch den großen Kostenunterschied zwischen FILL und BLAKE3.
`classify_stable_chunk_batch` arbeitet bereits mit atomar vergebenen
Vierergruppen; ein weiterer Mutex-Umbau löst diesen Fall nicht.

Die Probe ruft die echte Klassifizierung mit echten ChunkFragments in einem
festen Pool mit zehn Threads auf. Sie variiert nur die angeforderten Permits,
prüft gegen die serielle Ausgabe und kontrolliert die vollständige
Permit-Rückgabe. Dieser Host hat fünf gemeldete Kerne mit zehn logischen CPUs.

Aus `probes-release.txt`, alle Werte µs je Batch:

| Eingabe | 1 Worker | 2 | 4 | 5 | 10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 × 16 KiB, Nicht-FILL | 29,2 | 40,1 | – | – | – |
| 16 × 64 KiB, Nicht-FILL | 230,7 | 177,1 | 172,3 | – | – |
| 64 × 16 KiB, Nicht-FILL | 240,1 | 149,2 | 198,9 | 240,4 | 379,2 |
| 64 × 64 KiB, Nicht-FILL | 989,5 | 567,0 | 384,6 | 406,0 | 513,3 |
| 128 × 256 KiB, Nicht-FILL | 8.733,4 | 4.550,7 | 2.650,6 | 2.124,0 | 1.970,6 |
| 64 × 16 KiB, FILL | 22,7 | 34,4 | 170,5 | 222,5 | 394,0 |

Die Wiederholungen `classify-first.txt` und `probes-final.txt` zeigen dieselbe
Richtung: Kleine Batches brauchen weniger Worker, der 32-MiB-Batch nutzt zehn
sinnvoll. Das rechtfertigt eine gemessene Mindestarbeitsmenge pro Worker und
einen günstigen FILL-Pfad. Es rechtfertigt keinen festen Fünf-Thread-Deckel;
ADR 0050 behält den gemeinsamen Pool und die Nutzung verfügbarer CPUs bei.
Ein FILL-Vorpass müsste sein Ergebnis weiterreichen, damit er keinen zweiten
vollständigen Scan erzeugt. Schwellenwerte müssen auf Nicht-FILL, FILL,
gemischten und fragmentierten Batches validiert werden. Die Probe verwendet
zusammenhängende, warme, gleich große Chunks und misst keine SMB-Wartezeiten.

Separat enthält `crates/fastdup-store/src/cpu_admission.rs:19` weiterhin eine
gemeinsame `Mutex<VecDeque>` mit einem Lock pro Job. Ein sicherer Challenger
entnimmt bis zu vier Jobs pro Lock; ein zweiter verteilt Ordinals atomar und
verwendet einzelne gesperrte Owner-Slots. Ergebnisreihenfolge und Permit-
Lebensdauer bleiben erhalten.

Vier Worker, 256 winzige 64-Byte-Hashjobs: Queue 42,7 → 15,9 µs durch
Batching. Bei 64 Jobs à 64 KiB verschlechtert sich derselbe Umbau dagegen
von 312,7 auf 331,5 µs. Andere Wiederholungen bestätigen die Regression bei
größeren Jobs. Die Owner-Slots ergeben ebenfalls keinen durchgängigen Gewinn
und benötigen zusätzliche Metadaten. Deshalb kein pauschaler Ersatz von
`WorkerPermits::map`; zuerst Granularität und dann gegebenenfalls eine
ausdrücklich für günstige Jobs gewählte Entnahmegröße.

Die erste Queue-Probe (`probes-first.txt`) erzeugte pro Job zusätzliche
Referenzzähler-Last auf demselben Arc. Sie wird nicht für diese Entscheidung
verwendet. Ab `probes-second.txt` sind die Eingaben nur Ordinals; der
unveränderliche Hash-Payload wird ohne Arc-Clone geliehen. Auch dann hat der
Zehn-Thread-Pool hohe Kosten für kleine Batches. Die genaue Aufteilung in
Scheduling, Wakeups und SMT-Effekte ist nicht isoliert.

## 3. Unveränderte Exact-Runs werden bei Append erneut auditiert

In `crates/fastdup-store/src/exact_index_repository.rs` führt der normale
Append über `recover_active` (`:991`) und `open_activated_record` (`:1290`)
zu `verify_run_set_dependencies` (`:1749`). Nach dem neuen L0-Run ruft
`activate` (`:793`) diese Prüfung erneut für die vollständige Run-Auswahl auf.
`audit_named_with_membership` (`:1342`) baut dabei Mapping, Page Bounds und
Bloom-Hint erneut auf, auch wenn derselbe alte Run bereits geschützt und
auditiert im installierten Prozesszustand vorhanden ist.

Die instrumentierte echte FsStorageIo-Probe legt 65.536 Index-Einträge an und
hängt 32 neue an, ohne Compaction. Die zweite Operation enthält:

| Membership-/Mapping-Audit | Einträge | Zeit |
| --- | ---: | ---: |
| Alter Run bei vorheriger Generation | 65.536 | 7.101 µs |
| Derselbe alte Run bei neuer Generation | 65.536 | 6.936 µs |
| Neuer Run bei neuer Generation | 32 | 101 µs |

Gesamter Append: 20.563 µs. Diese Zeiten sind eine lokale Ablaufmessung auf
dem Workspace-Dateisystem; die beiden alten Audits sind rund 68 % davon.
Das ist keine bereits erzielte Ersparnis und keine Laufzeitprognose für die
Metadaten-SSD. Die Fixture modelliert Index-Einträge, keine DATA-Verifikation.

Ansatz: Beim internen Generation-Append den aktuell ausgewählten Activation-
Record feststellen und passende bereits geprüfte Reader übernehmen. Alte
Mapping-Owner und Page Bounds können geteilt werden. Neue, unbekannte oder
abweichende Runs müssen vollständig geprüft werden. Ein identischer Name
allein ist kein ausreichender Wiederverwendungsnachweis: Profil, Generation,
Run-Hash, Länge und dauerhaft gehaltene Immutable-Lease gehören zusammen.

Die aktuelle Vorgabe aus ADR 0079 prüft vor Selektierbarkeit vollständig.
Eine Änderung muss den Übergang vom geprüften Owner zur Nachfolgegeneration
explizit festlegen. Öffentliches Recovery und Offline-Scrub bleiben unabhängig;
Adapters ohne Immutable-Lease behalten den bestehenden Audit. Neue Run-Set-
Abhängigkeiten, Generation-Fences, Race-Erkennung und Activation-WAL-Sync
bleiben maßgeblich. Bei kleinerem RAM-Budget kann ein Bloom-Hint entfallen,
auch wenn sein Mapping wiederverwendet wird. Geteilte Allokationen und ihre
Lebensdauer müssen korrekt budgetiert werden.

Die unmittelbare Writer-Prüfung neuer Exact-Dateien (`publish`, `:565`) ist
ein anderer Boundary. `descriptor_from_complete_bytes` dekodiert nur die
Envelope, nicht noch einmal den vollständigen Run. Diese beiden Stellen
werden hier nicht als zusätzliche unnötige Komplettscans gezählt. Insbesondere
wird nicht vorgeschlagen, den unabhängigen positional Publication-Audit
einfach zu entfernen.

## 4. Weniger Owner-Metadaten bei Read-Antworten

Der Single-Extent-Pfad in `crates/fastdup-store/src/manifest_reader.rs:865`
verwendet `Bytes::from_owner(payload).slice(start..end)`. Dadurch landen alle
176 Byte von `VerifiedChunkPayload` im Antwort-Owner. Der Mehrsegmentpfad
verwendet bereits `VerifiedReadView` mit 24 Byte (`:959`).

Eine konsumierende Variante `into_read_view(self, range)` kann die Arc samt
geprüften Bereichskoordinaten verschieben. Die Antwort braucht danach keine
Chunk-ID und keinen Exact-Locator. Die Bereichsprüfung bleibt vollständig;
auch eine kleine Antwort hält das ganze zugehörige Backing gültig.

Die 4-KiB-Probe misst in `probes-release.txt` 46,62 → 30,22 ns, in
`probes-final.txt` 45,49 → 29,42 ns. Das spart 152 Byte Owner-Körper pro
gleichzeitig gehaltener Antwort. Gemeinsame Box-/Allocator-Metadaten sind
darin nicht enthalten. Beide Varianten sind bereits Zero-Copy bezüglich der
Payload; die Optimierung betrifft Verpackung und Besitzübergang.

Ein bereits vorher erzeugtes und wiederverwendetes `Bytes` benötigt nur rund
11 ns zum Slicen. Das ist eine weitergehende Möglichkeit, aber kein fairer
kostenloser Ersatz: Erzeugung, zusätzliche Cache-Repräsentation und verbleibende
Backing-Lebensdauer müssten separat gemessen und budgetiert werden. Zuerst
den kleinen konsumierenden Owner umsetzen.

## 5. Bereits geprüfte Pending-Präfixe nicht erneut durchlaufen

`assert_pending_write_through_state` (`checkpoint.rs:3109`) läuft über sämtliche
Pending-Chunks und summiert deren Bytezahl. `extract_stable_chunks` prüft am
Eingang und am Ausgang; `assert_bounded_write_through_lane` ruft den Vollscan
ebenfalls auf. Diese `assert!`-Prüfungen sind auch im Release-Build aktiv.
Beim Wachstum eines Containers werden alte unveränderte Einträge deshalb
mehrfach durchlaufen.

Die Probe verwendet die echten Pending-Typen und Produktionsvalidatoren. Sie
bildet den Aufbau von 32 MiB in 1-MiB-Fenstern mit den wiederholten Prüfungen
nach. Der Challenger prüft neue Einträge und ihre Grenze zum Vorgänger lokal,
führt Byte-Accounting fort und macht beim vollständigen Abschluss einen
Vollscan. Beide Seiten legen dieselben Chunk-Owner an.

| Chunkgröße | Einträge | Bisherige Folge | Inkrementell |
| --- | ---: | ---: | ---: |
| 16 KiB | 2.048 | 140,7 µs | 76,4 µs |
| 64 KiB | 512 | 32,6 µs | 17,2 µs |
| 256 KiB | 128 | 8,7 µs | 4,4 µs |

Die absoluten Einsparungen sind klein. Die Probe ist eine Nachbildung der
Operationsfolge, kein vollständiger Ingest-Aufruf. Sinnvoll ist ein privater
Pending-State mit geprüften Append-/Take-/Clear-Operationen, checked
Arithmetik und vollständiger Prüfung beim Detach. ADR 0050 fordert diese
Detach-Prüfung ausdrücklich. Bloßes Umwandeln aller Assertions in
`debug_assert!` wäre kein gleichwertiger Challenger. Fehler-, Teilflush-,
Freeze- und Backpressure-Pfade müssen die gleichen Invarianten erhalten.

## 6. Prefix-Ausgabe ohne vorherige Nullinitialisierung

Normales Zstd nutzt die direkte Vec-Ausgabe bereits. Beim Prefix-Read
initialisiert `crates/fastdup-format/src/container.rs:4540` den gesamten
Decode-Puffer mit Nullen und lässt ihn anschließend von Zstd überschreiben.
Die sichere vorhandene `zstd_safe::WriteBuf`-Implementierung für Vec kann
stattdessen in die reservierte Kapazität schreiben und die initialisierte
Länge setzen. Der Challenger behält Prefix-/Record-Prüfung, tatsächliche
Outputlänge und Ziel-Chunk-Hash bei. Er verwendet weiterhin einen frischen
DCtx; Context-Pooling ist hier nicht Teil der Messung.

In `probes-release.txt` ergeben sich 17,181 → 16,644 µs für 64 KiB und
68,042 → 64,070 µs für 256 KiB. Andere Wiederholungen zeigen ebenfalls
ungefähr 3–6 % Gewinn; bei 16 KiB ist kein relevanter Gewinn belegt. Die
Fixture verwendet eine zufällige Base mit wenigen geänderten Zielbytes.
Sie prüft zusätzlich die Ablehnung eines beschädigten Records. Vor Umsetzung
müssen insbesondere zu großer/kleiner Frame-Output, Abbruch und exakte
Längenbegrenzung erhalten bleiben; Vec-Kapazität kann größer als angefordert
ausfallen.

Auch Prefix-Encoding (`crates/fastdup-store/src/reduction_prefix.rs:305`)
initialisiert seinen Trial-Puffer noch mit Nullen. Der gemessene sichere
Vec-Challenger wahrt die Trial-Byte-Grenze ausdrücklich. Seine Ergebnisse
sind jedoch gemischt: meist wenige Prozent, teils Gleichstand, teils
Regression. Für diesen separaten Write-Umbau gibt es noch keinen ausreichend
stabilen Vorteil über alle getesteten Fälle.

## Containerformat, SIMD und weiteres Unsafe

Das derzeitige Containerformat hat 128-Byte-Record-Header, 64-Byte-Chunk-Table-
Einträge und 128-Byte-Recovery-Index-Einträge. Der Recovery-Eintrag wiederholt
Record-Felder pro Chunk, reserviert 24 Byte und hält 32 Dependency-Bytes auch
für unabhängige Chunks bereit (`container.rs:2921`). Eine Trennung in kompakte
Chunk-Zeilen und Record-Deskriptoren ist ein möglicher Formatansatz.

Allein eine hypothetische Halbierung von 128 auf 64 Byte spart bei 32 MiB
logischer Payload und 64-KiB-Chunks jedoch nur 32 KiB, also 0,098 % dieser
Payload; bei 16-KiB-Chunks 128 KiB bzw. 0,391 %. Das ist keine Aussage über
den Anteil an stark komprimierten physischen Daten. Record-Deskriptoren würden
von dieser Bruttoersparnis wieder etwas verbrauchen und könnten zusätzliche
Indirektion oder I/O bei Base-Auflösung erzeugen. Ein 64-Byte-Eintrag ist
hier kein fertig entworfener vollständiger Formatnachfolger.

Ohne A/B für Cold Read, Base-Lookup, Recovery und Scrub ist eine Migration
derzeit schwächer begründet als die RAM- und Wiederverwendungsänderungen.
Writer, Reader/Recovery und Scrub müssten bei einer Formatänderung dieselben
neuen Grenzen und Fault-Fälle unterstützen. Bestehende vollständige Record-
und Chunk-Prüfung beim unabhängigen Read wird nicht als frei streichbare
Doppelarbeit behandelt (ADR 0030, 0059).

Die in Runde 5 eingeführte sichere SIMD-FILL-Erkennung ist bereits Teil der
Basis. Die neuen stärkeren Befunde betreffen Datenlayout, Owner, zusätzliche
Durchläufe und Thread-Granularität. Kein eigener neuer Unsafe-Block oder
Intrinsics-Pfad ist dafür nötig. Der Prefix-Prototyp nutzt die sichere
Vec-API der vorhandenen Zstd-Abhängigkeit; der vorhandene kleine CCtx-Wrapper
wird generisch über deren WriteBuf, ohne neue Unsafe-Operation.

## Reproduktion und Grenzen

Alle Artefakte liegen unter `.artifacts/hotpath-audit6-20260906/`:

- `source-before.tar.gz`, `initial-status.txt`, `initial-diff.patch`: genaue
  Ausgangsdateien einschließlich vorhandener uncommitteter Änderungen.
- `fastdup-durable-fuse-baseline`: unveränderte Produktionsbasis; SHA-256
  `c246baa3b664cc8915af0a7070e60cc9f30059377b3c57bbb6c09abffb388ab9`.
- `probe/`, `probe.patch`, `REPRODUCE.md`, `environment.json`,
  `source-manifest.json`: isolierte Quellen, Wiederholungsanleitung und
  Provenienz. Der Patch ist gegen den Snapshot, nicht gegen Git HEAD.
- `proof-final.txt`, `proof-repeat.txt`, `map-*-65536-rss.txt`: finale
  Proof-Struktur mit konsumierender Entnahme und RAM am Produktionslimit.
- `probes-second.txt`, `probes-final.txt`, `probes-release.txt`,
  `classify-first.txt`: einzelne Wiederholungen und die bezeichneten
  Ablauf-/A/B-Werte. Historische Varianten sind oben ausdrücklich bezeichnet.
- `verification-final.txt`: gemeinsamer abschließender Lauf aller zehn
  Audit-Tests. Der RSS-Test macht ohne Modusvariable keinen Messlauf; die
  beiden ausgewählten RSS-Prozesse sind separat protokolliert.

Release-Tests laufen seriell mit `--test-threads=1`, CARGO_TARGET_DIR und
TMPDIR liegen unter der Workspace-`.artifacts`. Innerhalb der Zeitproben
rotieren die Varianten über elf Wiederholungen; die Lifecycle-Probe verwendet
neun. Tabellen berichten Mediane, keine statistischen Konfidenzintervalle.
Die Prozesse der A/B-Suite laufen nicht parallel zueinander. Die Tests prüfen
unter anderem Full-Key-Hashkollisionen, Admission-Vorrang, gleiche
Klassifikation, Antwortbytes und Bereichsgrenzen, Rückgabe aller Permits,
Prefix-Trial-Grenzen und beschädigte Prefix-Records.

Es wurde kein neuer SMB-Lauf und keine erneute vollständige Repository-Suite
gestartet: Der Produktionsstand bleibt unverändert. Eine Umsetzung braucht
die betroffenen Integrations-/Fault-Tests und anschließend getrennte Normal-
und Advanced-SMB-Messungen. Lookup-/Owner-Zeiten dürfen nicht als
End-to-End-Faktor ausgegeben werden. Die Vorschläge ändern absichtlich weder
Chunking noch Codec-Auswahl; unveränderte Reduction muss insbesondere nach
einer Änderung der Ausführungsreihenfolge erneut nachgewiesen werden.
