# Hotpath-Audit 8 mit Umsetzung

Stand: 2026-09-06. Ausgangspunkt ist die vollständig implementierte Runde 7,
einschließlich ihrer noch nicht committeten Änderungen. Die Basisbinary hat
SHA-256 `d57df6958d9f30f1b63e478ddaadb0c715c264db4b9e4ecbcfe78ee6c3a59780`.
Der neue Audit ergänzt drei Optimierungen. Frühere Änderungen bleiben erhalten.

## Umgesetzt: Writer-Index direkt aufbauen

`writer_record_evidence` erzeugte für jeden Encoding Record einen eigenen
`Vec<IndexEntry>`. Die Containerassembly kopierte dessen Einträge anschließend
in ihren bereits vollständig reservierten Indexvektor. Gerade viele kleine
RAW-Records verursachten dadurch viele kurze Heap-Allokationen.

`IndexEntry::append_from_encoded_record` schreibt jetzt direkt in diesen
Containervektor. Publication Locations werden aus dem gerade angehängten Slice
abgeleitet. Alle drei Writerpfade verwenden die Änderung, einschließlich des
Testorakels mit vorab genulltem Containerbuffer. Feldweise Serialisierung,
Checksummen, Sortierreihenfolge und unabhängige Prüfung bleiben erhalten.
Reader und Scrub behalten ihren unabhängig aufgebauten Erwartungsindex.

Zwei Release-Gegenproben am tatsächlichen Code, jeweils elf Zeitstichproben:

| Ausschnitt | Vorher → aktuell, Durchgang 1 | Vorher → aktuell, Durchgang 2 |
| --- | ---: | ---: |
| Writer-Evidence, 512 RAW-Records | 18,845 → 10,783 µs | 19,547 → 10,624 µs |
| Vollständiges 32-MiB-RAW-Containerencoding | 8.560,674 → 9.125,646 µs | 8.499,487 → 8.502,142 µs |

Die Indexarbeit sinkt damit um 43–46 %. Der vollständige Writer zeigt in dieser
Probe keinen stabilen Zeitgewinn; Payloadkopie und CRC dominieren den größeren
Ausschnitt. Die Zahl der temporären Indexallokationen sinkt trotzdem eindeutig
von einer je Record auf null. Der Outputvektor war bereits vorher reserviert.
Die Fixture erzwingt RAW-Pläne mit bekannten IDs; sie misst weder SeqCDC noch
die Codec-Auswahl und ist kein Modell für die FILL-Erkennung des Ingests.

Die BLAKE3-Hashes der erzeugten Containerbytes stimmen zwischen den echten
Vorher-/Nachher-Binaries für alle drei gemessenen Geometrien überein.
Unabhängiges Dekodieren prüft ihre Nutzdaten. Der bestehende gemischte Test
deckt RAW, Zstd, vorbereitete unabhängige und transplantierte Records sowie
Prefix und Sparse-XOR einschließlich Korruption ab.

## Umgesetzt: Entry-Seiten innerhalb einer Similarity-Abfrage behalten

Ein Query-Cursor lud bisher jeden Kandidaten mit `read_entry` aus dem
gemeinsamen Page Cache. Mehrere Einträge derselben bereits verifizierten Seite
verursachten erneut Slot-Lock, vollständigen Cache-Key-Abgleich, Arc-Clone,
Hit-Zähler und Arc-Drop. Gleichzeitige Abfragen populärer Buckets konkurrierten
damit immer wieder um dieselben Locks und Referenzzähler.

Jeder der vier Cursor hält jetzt seine letzte Entry-Seite. Die Referenz gehört
ausschließlich zur laufenden Abfrage auf einem unveränderlichen Index;
Partition und Seitenordinal müssen passen. Bei einem Wechsel erfolgt der
bisherige geprüfte Page-Cache-/Decode-Zugriff. Vollständige Entry-Gleichheit,
Bucket-Beziehungen, Reihenfolge, Profil, Länge, Sketch-Distanz und die maximal
16 ausgewählten Kandidaten werden weiterhin geprüft. Eine Abfrage ohne
besetzte Buckets erzeugt außerdem keinen leeren Kandidatenpuffer mit Kapazität 16.

Der abgegrenzte Query-Working-Set hält höchstens vier zusätzliche Seitenreferenzen,
mit bis zu 25 dekodierten Einträgen je Seite. Sie verschwinden am Cursorende
auch bei deaktivierter gemeinsamer Cache-Aufnahme. Die feste Cachekapazität
wächst nicht. Dies spart Synchronisationsarbeit, nicht pauschal Prozess-RAM.

Die Probe verwendet echte publizierte und wieder geöffnete Similarity-Indizes
mit deterministischen Testeinträgen. Der Fingerprint ist vorab berechnet;
Base-Lesen und Codec-Trials sind nicht Teil dieser Zeitmessung. Angegeben sind
Mikrosekunden pro Abfrage, bei mehreren Threads Gesamtdauer durch Abfragezahl.

| Kandidaten / Threads | Vorher → aktuell, Durchgang 1 | Vorher → aktuell, Durchgang 2 |
| --- | ---: | ---: |
| 16 auf derselben Seite / 10 | 16,289 → 1,568 µs | 16,704 → 1,681 µs |
| 64 auf drei aufeinanderfolgenden Seiten / 1 | 22,204 → 17,762 µs | 26,148 → 17,930 µs |
| 64 auf drei aufeinanderfolgenden Seiten / 10 | 28,237 → 3,422 µs | 34,913 → 3,638 µs |
| 64 auf 64 verschiedenen Seiten / 10 | 10,562 → 11,007 µs | 12,271 → 12,746 µs |
| Ein Kandidat / 1 | 0,727 → 0,789 µs | 0,734 → 0,783 µs |

Die gemeinsamen Page-Cache-Zugriffe pro warmer Abfrage sinken im 16er-Fall
von 68 auf 8 und im dichten 64er-Fall von 261 auf 17. Im verstreuten 64er-Fall
bleiben es 260. Der große Mehrthread-Gewinn gilt für die bewusst stark
konkurrierende Fixture mit gemeinsam genutzten Seiten. Ohne Seitenwiederverwendung
zeigt sie etwa 4 % Zusatzkosten; beim Einzelkandidaten sind es 49–62 ns.
Das ist kein allgemeiner Faktor für Advanced Reduction oder den SMB-Durchsatz.

Die Probe prüft jede Kandidatenliste gegen unabhängig sortierte erwartete IDs
und Sketch-Distanzen. Nach Pressure-Purge muss dieselbe Liste entstehen.
Ein neuer Produktionstest prüft Seitenwechsel über 51 Einträge, ungültige
Ordinals, Partitionswechsel sowie die vollständige Freigabe einer nur noch
vom Query gehaltenen Seite nach Pressure-Eviction.

## Umgesetzt: keine Condvar-Benachrichtigung ohne wartende Threads

`WorkerPermitLease::release` rief nach jeder Rückgabe `notify_all` auf,
auch wenn kein Coordinator auf CPU-Permits wartete. Bei zehn einzeln
retirierten Workern wiederholte sich dieser Weg zehnmal pro Lease.

`WorkerPermits` zählt jetzt registrierte Condvar-Waiter unter derselben Mutex
wie die verfügbaren Permits. Ein Waiter registriert sich vor `wait`, bleibt
während des atomaren Unlock-/Wait-Übergangs registriert und meldet sich nach
erneutem Lock ab. `release` benachrichtigt nur bei einem registrierten Waiter.
Damit kann die Prüfung keinen Wakeup verlieren. Teilzuteilungen, Spurious
Wakeups, frühe Worker-Rückgabe und Panic-Unwind behalten ihre Semantik.
Die Änderung ergänzt einen `usize` im gemeinsamen Admission-Zustand.

| Unkontendierter Zyklus | Vorher → aktuell, Durchgang 1 | Vorher → aktuell, Durchgang 2 |
| --- | ---: | ---: |
| Acquire und Rückgabe eines Permits | 173,228 → 61,458 ns | 233,391 → 84,883 ns |
| Zehn Permits, neun frühe Rückgaben und abschließender Drop | 1.298,257 → 217,576 ns | 1.385,821 → 265,938 ns |

Eine getrennte `strace -f -c -e futex`-Gegenprobe desselben Permit-Tests zählt
**1.320.003 → 3 Futex-Aufrufe**. Die 1.320.000 entfallenen Aufrufe entsprechen
genau den Rückgaben ohne wartende Coordinator. Trace-Zeiten sind keine
Performancewerte und fließen nicht in die obige Zeitmessung ein.

Die separate Probe mit 64 tatsächlichen 64-KiB-BLAKE3-Jobs und zwei, vier bzw.
zehn Workern zeigt überwiegend die Kosten von Hashing und Scheduling. Sie
begründet keinen entsprechenden Faktor für ganze CPU-Batches. Die Queue und
Jobverteilung bleiben unverändert; früher verworfene pauschale Queue-Batching-
Varianten aus Audit 6 werden nicht erneut übernommen.

Ein neuer Test registriert vier wartende Coordinator, erzeugt eine zusätzliche
Condvar-Benachrichtigung und gibt danach Permits frei. Alle müssen fortschreiten;
am Ende sind Waiterzahl null und das volle Budget zurückgegeben. Acht Runden
ergänzen die bestehenden Tests für frühe Rückgabe, Teilzuteilung und Unwind.

## Geprüft und verworfen: Miss-Listen und reiner Cache-Hit-Fastpath

`VerifiedManifestFile::read_cached_many` reserviert zwei Miss-Listen auch bei
vollständigen Cache-Treffern. Drei sichere Varianten wurden gegen den echten
Code geprüft: verzögerte Reservierung, direkte Ausgabe des Hit-Präfixes und
verzögerte Reservierung ohne vorheriges Initialisieren aller Ergebnisplätze.

Der direkte Hit-Pfad beschleunigte warme 32er-Batches von 1,152 auf 0,945 µs
und von 1,178 auf 0,893 µs. Die Übergabe an den Miss-Pfad verursachte aber
zusätzliche Kosten: reine Acht-Chunk-Misses etwa 0,314 → 0,408 µs im ersten
Durchgang, 0,349 → 0,362 µs im zweiten. Auch die kleineren Varianten lieferten
keinen hinreichend gleichmäßigen Vorteil über Treffer, späten ersten Miss,
gemischte und vollständig fehlende Chunks.

Die zwei gesparten Puffer entsprechen bei 32 Requests nur 1.536 Byte
temporärem Speicher. Deshalb bleibt dieser Produktionspfad auf dem Stand von
Runde 7. Die verworfenen Quellen, Rohdaten und Entscheidung sind erhalten;
ihre Verbesserungen zählen nicht als Ergebnis der Umsetzung.

## SIMD, Unsafe, Zero-Copy und Format

Die neuen Gewinne entstehen durch weniger temporäre Datenstrukturen und
Synchronisation. Es gibt keine neue Unsafe-Stelle. Die vorhandenen SIMD-Pfade
für SeqCDC, FILL, CRC, BLAKE3, Fingerprint-Votes und Sparse-XOR bleiben erhalten.
Die Gegenproben liefern keinen Beleg für einen weiteren selbst geschriebenen
Intrinsics-Kern oder das Entfernen einer Integritätsprüfung.

Der Container besitzt weiterhin vollständige Record-CRC und Chunk-Hash-
Verifikation. Ein kleiner Read aus einer Zstd-Region kann deren vollständige
Dekompression und Prüfung erfordern. Die bereits bekannten Alternativen
kleinerer Regions oder unabhängig verifizierbarer Subframes brauchen weiterhin
einen eigenen Cold-Read-/Reduction-Vergleich. Dieser Audit belegt dafür keinen
neuen Formatvorteil und ändert keine dauerhaften Bytes oder Decode-Parameter.

## SMB-Abnahme mit Normal und Advanced Reduction

Der unveränderte SingleStream-Skill-Runner führte zwölf erfolgreiche Läufe
aus: drei unmittelbar gepaarte Vorher-/Nachher-Vergleiche je Modus mit
alternierender Reihenfolge. Jeder Lauf überträgt dieselbe Rocky-10.2-Minimal-
ISO dreimal sequenziell und misst die allozierte Repositorygröße nach zwölf
Sekunden, während alle drei Dateien leben. Insgesamt sind es 36 Uploads.
Die Baseline wurde für diese Runde neu gemessen.

Gleiche Samba-Konfiguration auf Loopback-Port 1445, separate XFS-Dateisysteme
auf `/dev/sdb1` für Metadaten und `/dev/sdc1` für Container. Beide Policies
wurden vorab per Dry-Run geprüft. `--require-zero-swap`, 1-GiB-Small-File-Quota
und `lab-allow-shared` gelten für alle Läufe. Telemetrie bestätigt Normal mit
`off` ohne Similarity-Abfragen und Advanced mit `dependent-v1`, aktiven
Abfragen und akzeptierten abhängigen Records. Es gab keine Advanced-Fehler,
keinen Prozess-Swap und keine fehlgeschlagene Bereinigung.

Mediane von jeweils drei Läufen:

| Messgröße | Normal vorher | Normal aktuell | Advanced vorher | Advanced aktuell |
| --- | ---: | ---: | ---: | ---: |
| Gesamtdurchsatz, MiB/s | 1.319,42 | **1.328,64** | 1.176,74 | **1.210,83** |
| Reduction | 67,82438 % | **67,82096 %** | 67,92636 % | **67,89869 %** |
| Alloziertes Repository, MiB | 1.907,793 | 1.907,996 | 1.901,746 | 1.903,387 |
| Daemon-CPU, s | 11,42 | 11,41 | 17,71 | 17,25 |
| Daemon-HWM, MiB | 464,09 | 471,34 | 630,43 | 582,05 |
| Längster vollständiger Datei-Put, ms | 1.889,38 | 1.913,87 | 2.377,26 | 2.322,25 |

Die drei gepaarten Durchsatzänderungen sind mit Normal −0,71/−1,04/+3,79 %,
mit Advanced +2,90/+1,77/+6,69 %. Ihr Median beträgt **−0,71 % bzw. +2,90 %**.
Das Verhältnis der Gruppenmediane ist eine andere Kennzahl: +0,70 % bzw.
+2,90 %. Alle Läufe bleiben enthalten; es gibt keine nachträgliche
Ausreißerbereinigung oder Behauptung statistischer Signifikanz.

Aktuelles Advanced ist in diesem ISO-Korpus **8,87 % langsamer** als aktuelles
Normal und spart zusätzlich **4,609 MiB**, entsprechend **0,07774 Prozentpunkten**
Reduction. Gegen die Advanced-Baseline liegt die aktuelle allozierte Größe
im Median 1,641 MiB höher. Die Auswahl- und Codec-Regeln sind unverändert;
die Messung beweist keine verbesserte Reduktionsquote. Insbesondere die
Verfügbarkeit von Online-Similarity-Snapshots und Commit-Schnitten bleibt
zeitabhängig. Eine konkrete Ursache der kleinen Quotendifferenz ist nicht isoliert.

Der gesamte Prozess-RAM sinkt nicht einheitlich. Die Read-Cache-Metadaten
bleiben in beiden Binaries bei 25.392.000 Byte. Die HWM-Werte belegen keinen
festen RAM-Sparbetrag dieser Änderungen. Bei drei Put-Samples entspricht die
vom Skill ausgegebene p99 dem längsten vollständigen Datei-Put; sie ist keine
p99 einzelner SMB-Requests. Ein neuer SMB-Read- oder SMB-MultiStream-Durchsatztest
ist nicht Bestandteil dieser Runde; Mehrthread-Messungen betreffen die oben
ausgewiesenen Komponenten.

## Weiterer Befund: seltener großer Rechunk-Rest am Commit-Schnitt

Nachtrag: Die [gezielte Diagnose und Reset-Korrektur](rechunk-lane-reset-2026-09-06.md)
behebt einen reproduzierbaren großen Fallback bei vertauschten benachbarten
Writes. Die Ursache des nachfolgenden ursprünglichen SMB-Samples bleibt
ohne damalige Offset-/Sequence-Spur offen.

Im dritten Advanced-Basislauf rechunkt Generation 7 **30.285.275 Byte
(28,882 MiB)** und encodiert davon 30.165.986 Byte in 361 neuen Chunks.
Über den ganzen Lauf fallen 31,539 MiB Rechunk-Arbeit an. In den anderen elf
Läufen sind es insgesamt etwa 2,68–3,18 MiB. Der auffällige Basislauf erreicht
auch den höchsten HWM seiner Gruppe. Er bleibt vollständig in der Auswertung;
sein Unterschied zum aktuellen Lauf ist kein Beleg für einen behobenen Fehler.

Die Codeprüfung bestätigt, dass der Commit-Pfad vor der Manifestplanung sowohl
Ingest/Publications abwartet als auch stabile Lane-Reste drainiert. Die spätere
Frozen-Rezeptaufnahme prüft nochmals Range-Abdeckung und Mutation Sequence.
Lane-Resets bei Diskontinuität, Truncate oder verworfene Rezeptaufnahme können
weiterhin residente Fallback-Daten hinterlassen. Die erhaltene Telemetrie
enthält für diesen Einzelfall keine Zuordnung der abgelehnten Range und
Sequence; welcher Übergang den großen Rest erzeugte, ist deshalb **offen**.

Das ist ein konkreter nächster Ansatzpunkt für weniger doppelte Arbeit:
Reset-/Rezept-Rejection-Gründe und betroffene Bytebereiche am Commit-Schnitt
korrelieren, dann einen reproduzierbaren Fall fixieren. Die Schutzprüfungen
werden auf Grundlage dieses einen Samples nicht abgeschwächt. Diese Runde
verändert den Rechunk-Pfad nicht.

## Validierung und Messartefakte

- Workspace-Suite: **774 bestanden, 0 fehlgeschlagen, 16 ignoriert**.
- `cargo clippy --workspace --all-targets -- -D warnings`: bestanden.
- Zwei Release-Durchgänge mit elf Stichproben je Komponentenkonfiguration,
  Vorher/Aktuell und Aktuell/Vorher. Nur die expliziten Threadproben parallelisieren.
- Identische Writerbilder und unabhängig erwartete Similarity-Rangfolgen;
  vollständige Permit-Rückgabe nach den Batchproben.

Artefakte: `/source/fastdup/.artifacts/hotpath8-20260906/`.
`source-before.tar.gz` und `initial-diff.patch` halten die vollständige Basis
einschließlich Runde 7 fest. Die ausführbaren Workloads stehen in den vier
`*_probe.rs`-Dateien; `prepare_probes.py`, `run_probes.py` und
`analyze_probes.py` beschreiben Aufbau und Auswertung. Die tatsächlichen
Testbinaries sowie die generierten Vorher-/Nachher-Quellen sind enthalten.

Ein erster Messsatz ist ausdrücklich ungültig: Cargo verwendete im gemeinsamen
Target-Verzeichnis dieselbe Baselinebinary für beide Quellbäume. Hashgleichheit
und unveränderte Page-Zähler deckten das auf. Er liegt unter
`invalid-shared-target-reuse` und wird nicht ausgewertet. Der korrigierte Runner
erzwingt frische Builds beider Crate-Roots, verlangt entsprechende Buildlogs
und unterschiedliche Binaryhashes. Die drei verworfenen Read-Varianten stehen
separat unter `v1-lazy-miss-lists`, `v2-hit-prefix` und `v3-lazy-push`.

Alle Cargo-Aufrufe verwenden `/source/fastdup/.artifacts/target` und
`TMPDIR=/source/fastdup/.artifacts/tmp`. Quellensnapshots, Korpora, temporäre
Repositories, Builds, Traces und Messberichte bleiben unter `.artifacts`.

SMB-Kommandos, unveränderte Samba-Konfiguration, zwölf Einzelberichte,
Daemon-Telemetrie und `summary.json` liegen unter
`/source/fastdup/.artifacts/benchmarks/smb-hotpath8-20260906/`.
Die neue gemessene Binary hat SHA-256
`8e4361f63fbeb02edc6c3b1ba7ca0b12bc0afcd34dbb1e9c082ff460b0b764b5`.
Die ISO enthält 2.072.444.928 Byte mit SHA-256
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
`final-verification.json` bestätigt den unveränderten gemessenen Quellstand,
die übereinstimmende Releasebinary und die entfernten Benchmark-Mounts samt
dediziertem Samba-Listener.
