# Fünfter Hotpath-Audit: Commit-Schnitte, Bereichssuche und RAM

Stand: 2026-09-06. Basis ist `a2c59d1` **einschließlich der noch nicht
committeten Implementierung aus Runde 4**. Dieser Audit ergänzt keine
Produktionsimplementierung. Quellkopien, Prototypen und Messungen liegen unter
`.artifacts/hotpath-audit5-20260906/`. Die vorher vorhandenen Änderungen bleiben
unverändert.

## Ergebnis und Reihenfolge

| Priorität | Ansatz | Neue Evidenz |
| --- | --- | --- |
| 1 | Publication-Fence um die früheste enthaltene Chunk-Sequenz erweitern | Deterministische Probe: 13 MiB Rechunking werden zu 0; derselbe Prefix wird vollständig als Rezept wiederverwendet. |
| 1 | FILL-Erkennung blockweise vergleichen | Sicheres Rust erzeugt SIMD; bei 64–256 KiB gleichförmigen Daten etwa 11× schnellerer Klassifikationsscan. |
| 1 | Fragmentierte Read-Pläne mit indizierten, gebündelten Intervalländerungen bilden | 128 abwechselnde 4-KiB-Patches: 16,17 → 11,12 µs; Fragmentierungsstress: 9,28 → 0,48 ms. |
| 2 | Flache Manifest-Rezepte über kumulative Endpositionen erschließen | 512 Extents: 479 → 26 ns pro Bereichsabfrage; 2.048 Extents: 1.828 → 30 ns. |
| 2 | Doppelte Identität im Verified-Read-Cache entfernen | CacheEntry 224 → 184 Bytes, CacheSet 904 → 744 Bytes; vollständige warme Treffer bleiben in den Proben bei etwa 36–40 ns. |
| 2 | Kleine Materialisierungs-Batches mit weniger CPU-Workern ausführen | 2 × 64 KiB kopieren: 39,20 → 11,29 µs mit einem budgetierten Worker. Große Batches profitieren weiterhin von Parallelität. |

Das sind Messungen einzelner Funktionen und Pipeline-Reproduktionen, keine
neuen SMB-Durchsatzwerte. Die Faktoren lassen sich nicht addieren oder auf
einen vollständigen Upload übertragen.

## 1. Rechunking: Ein Container kann einen Commit-Schnitt überspannen

Stellen im Produktionsstand:

- `crates/fastdup-appliance/src/checkpoint.rs:2082`, `DetachedContainerWork`.
- `crates/fastdup-appliance/src/checkpoint.rs:2296`, Publication-`wait_through`.
- `crates/fastdup-appliance/src/checkpoint.rs:3386`, `wait_for_commit_cut`.
- `crates/fastdup-appliance/src/checkpoint.rs:6255`, Einsammeln der Rezepte.

Der zunächst serielle Reproduktionslauf schreibt dreimal 96 MiB aus demselben
Rocky-ISO-Ausschnitt und setzt alle 13 MiB einen Commit. Er bleibt bei
271.610–523.359 Rechunk-Bytes pro Schnitt. Einfaches Weiterschreiben nach einem
expliziten Freeze reproduziert den großen Ausschlag ebenfalls noch nicht.
Mit gleichzeitigem Writer und Checkpointer entsteht dagegen ein Commit mit
13.875.781 Rechunk-Bytes. Die Probe schlägt an der Schranke von 1 MiB fehl.

Geprüfte Hypothesen waren fehlende Veröffentlichungen beim Einsammeln der
Rezepte, verlorene CDC-Kontinuität und die Ablehnung von Rezepten an Versions-
oder Bereichsgrenzen. Der erste Mechanismus ließ sich anschließend gezielt
isolieren:

1. 13 MiB schreiben und den Namespace-Schnitt einfrieren.
2. Auf 36 MiB weiterschreiben, sodass ein voller detached Container Daten
   vor **und** nach dem Schnitt enthält.
3. Seine DATA-Veröffentlichung zunächst mit `PausedStorageIo` an `SyncFile`
   anhalten und den Checkpointer starten.
4. DATA-Publication freigeben, aber die Übergabe der Publication-Ergebnisse an
   den Namespace mit einer zusätzlichen, ausschließlich in der Quellkopie
   vorhandenen Testbarriere anhalten. Die Proof-Verfügbarkeit und das Anbringen
   der Frozen-Rezepte werden damit getrennt kontrolliert.
5. Prüfen, ob der Planner diesen noch nicht angebrachten Prefix rechunkt;
   anschließend die Übergabe freigeben und alle Beteiligten abschließen lassen.

Die zuerst verwendete reine `SyncFile`-Pause war bei einer Wiederholung noch
vom Scheduling nach ihrer Freigabe abhängig und blieb einmal grün. Erst die
zusätzliche Barriere vor `retire_detached_container` macht das kritische
Zeitfenster direkt steuerbar. Dieser erfolglose Kontrollversuch bleibt als
`rechunk-storage-pause-nondeterministic.txt` erhalten.

Der aktuelle Fence betrachtet `work.through_sequence`, also die letzte
Sequenz des gesamten Batches. Liegt diese hinter dem Schnitt, wartet er auf
den Batch nicht. Dessen ältere Chunks können aber bereits zum Frozen Prefix
gehören. Der Checkpointer kann seine Liste vorbereiteter Rezepte bilden,
bevor die geordnete Publication-Retirement diese Rezepte angebracht hat.
Danach helfen später eintreffende Rezepte dieser bereits geplanten Lücke nicht
mehr: Sie wird gelesen, gechunkt und gehasht.

| Kontrollierte Probe | Rechunk-Bytes | Recipe-Reuse-Bytes |
| --- | ---: | ---: |
| Aktueller Fence | 13.631.488 | 0 |
| Isolierter Fence mit frühester Chunk-Sequenz | 0 | 13.631.488 |

Der Challenger speichert zusätzlich das Minimum der in den Chunks getragenen
Sequenzen und verwendet es für die Fence-Auswahl in Pending/In-flight.
Enqueue-Reihenfolge, letzte Batch-Sequenz und geordnete Retirement bleiben
erhalten. Er besteht auch die erneut ausgeführten seriellen, überlappenden
und konkurrierenden Szenarien. Die kontrollierte Probe prüft zusätzlich alle
13 MiB des eingefrorenen Readers und ausgewählte Live-Bereiche vor und nach
dem Schnitt bytegleich.

**Grenze:** Das erklärt einen konkreten Mechanismus für die zusätzlichen
Rechunk-Bytes im letzten SMB-Lauf. Es beweist noch nicht, welcher Anteil der
damaligen rund 11 MiB zusätzlichen Containerbelegung davon stammt. Rechunking
kann vorhandene Chunks per Exact wiederverwenden. Ebenso kann der korrekte
Fence mehr Zeit auf die ohnehin benötigte DATA-Veröffentlichung warten; der
Gewinn bei CPU-Arbeit ist kein automatisches Versprechen kürzerer Commit-Latenz.

Umsetzung unter ADR 0041/0050: Den relevanten, begrenzten Publication-Prefix
nach dem Ingest-Fence bestimmen und dessen Retirement vor Rezeptplanung
abwarten. Neuere, nicht benötigte Arbeiten dürfen kein unbegrenztes Warten
erzeugen. Zusätzlich zu den Proben sind Fehler/Retry, mehrere Inodes,
Truncate/Overwrite und Crash-Recovery des Frozen Prefix zu prüfen. Das ist
eine Änderung der Prozesskoordination, keine Änderung des Commit-WAL-Formats.

Rohdaten: `rechunk-baseline.txt`, `rechunk-overlap.txt`,
`rechunk-concurrent.txt`, `rechunk-blocked-baseline.txt`,
`rechunk-blocked-candidate.txt`, `rechunk-final.txt`, `rechunk-final2.txt`.
Die endgültige Kontrolle mit beiden Barrieren steht in `reproduce-baseline.txt`
und `reproduce-rechunk.txt`.

## 2. FILL: Der verbleibende Scan ist byteweise

`ChunkFragments::is_fill`, `crates/fastdup-appliance/src/checkpoint.rs:1318`,
prüft jedes Fragment mit `iter().all(...)`. Er wird vor der Chunk-ID-Berechnung
aufgerufen; bei gewöhnlichem Inhalt endet er schnell, bei FILL oder einer
späten Abweichung muss er dagegen den vollständigen Chunk durchlaufen.

Die isolierte Alternative prüft zunächst bis zu acht Bytes und vergleicht
danach sichere 32-Byte-Slices gegen `[first; 32]`. Nur der kurze Rest bleibt
byteweise. Es gibt weder neue Intrinsics noch eigenes Unsafe. Im erzeugten
Code sind `pcmpeqb`, `pand` und `pmovmskb` sichtbar
(`fill-assembly.txt`): Der Compiler nutzt hier zwei 16-Byte-Vergleiche pro Block.

Neun wechselweise angeordnete Samples, jeweils 2.000 Aufrufe, zweiter sauberer
Lauf:

| Eingabe | Bisher | Blockweise | Faktor |
| --- | ---: | ---: | ---: |
| 16 KiB FILL | 3.897 ns | 226 ns | 17,27× |
| 64 KiB FILL | 15.172 ns | 1.305 ns | 11,63× |
| 256 KiB FILL | 60.966 ns | 5.539 ns | 11,01× |
| 64 KiB, Abweichung im letzten Byte | 15.488 ns | 1.330 ns | 11,65× |
| 64 KiB, Abweichung im zweiten Byte | 1,33 ns | 1,82 ns | kleiner absoluter Mehraufwand |

12.288 Differentialfälle prüfen Längen 1–1.024, verschiedene Fragmentgrenzen
und Abweichungen am Anfang, in der Mitte und am Ende. Der erste saubere Lauf
bestätigt den großen Vorteil bei langem Scan. Die frühe Ablehnung wird etwa
0,4–0,6 ns teurer; eine pauschale Beschleunigung aller Eingaben wäre falsch.
Für normale Nicht-FILL-Chunks folgt ohnehin noch die vollständige BLAKE3-Arbeit.

Umsetzung: Diese eine Klassifikation ersetzen und die schnellen kurzen
Abweisungen behalten. FILL-Werte, Chunk-Grenzen und Reduction-Ergebnis müssen
identisch bleiben. Die Änderung benötigt keine Formatversion.

Rohdaten: `fill-final1.txt`, `fill-final2.txt`.

## 3. ReadPlan: Quadratische Verwaltung bei Fragmentierung

`ReadPlan::cover`, `crates/fastdup-posix/src/versioned_file.rs:1990`, beginnt
für jeden belegten Bereich erneut am Anfang von `uncovered`. Bei abwechselnden
Dirty-Patches und committed Lücken wächst diese Liste mit jedem Patch.
Das anschließende Abdecken vieler Lücken verschiebt sie außerdem durch
wiederholtes `Vec::remove` immer wieder. Die Sichtbarkeit ist korrekt; teuer
ist die Verwaltung ihrer Intervalle. Diese Planung erfolgt weiterhin unter
der Inode-Lesesperre, während DATA-I/O bereits außerhalb liegt.

Der erste Challenger überspringt nicht überlappende Intervalle per
`partition_point`, behält aber die einzelnen Entfernungen. Bei 4.096 Patches
bleiben damit noch 1,72 ms übrig. Der zweite Challenger bestimmt Anfang und
Ende des betroffenen Intervallbereichs, erzeugt die sichtbaren Spans und
ersetzt den gesamten Bereich mit einem `splice`. Höchstens zwei Randstücke
bleiben übrig.

1-MiB-Read mit abwechselnden gleich großen Dirty-Patches und committed Lücken;
gemessen wird nur die Planbildung, inklusive Freigabe des Plans:

| Patches | Bisher | Bereichsweise Änderung | Faktor |
| --- | ---: | ---: | ---: |
| 1 | 0,071 µs | 0,087 µs | 0,81× |
| 16 | 1,017 µs | 1,182 µs | 0,86× |
| 128, je 4 KiB Dirty | 16,169 µs | 11,124 µs | 1,45× |
| 512, je 1 KiB Dirty | 168,599 µs | 49,251 µs | 3,42× |
| 4.096, je 128 Bytes Dirty | 9.275,355 µs | 477,538 µs | 19,42× |

Die fertigen Antworten beider Pläne sind bytegleich. Zwei Läufe bestätigen
den Verlauf. Der große Faktor gehört zum Fragmentierungsstress, nicht zum
üblichen zusammenhängenden Read. Auch der zweite Challenger ist kein
allgemeiner Beweis linearer Laufzeit: Änderungen mitten in einem Vec können
weiterhin Elemente verschieben.

Umsetzung: Einen kurzen Pfad für kleine Intervalllisten behalten und größere
Überlappungen gebündelt verarbeiten; alternativ einen geordneten Sweep über
die höchstens zwei Dirty Epochs ausarbeiten. Die gezeigte Alternative sollte
wegen der kleinen Regressionen nicht unverändert für jeden Ein-Extent-Read
eingesetzt werden. Differentialtests müssen Active/Frozen, Hole, Truncate/Grow,
externe Quellen und später mutierte Owner abdecken.

Rohdaten: `read-plan.txt`, `read-plan-batched.txt`, `read-plan-final2.txt`.

## 4. Ein flaches Manifest wird für jeden kleinen Read vollständig durchsucht

`FlatManifestRecipe::read_range`,
`crates/fastdup-store/src/manifest_reader.rs:91`, durchläuft alle Extents,
auch nach dem Ende des angefragten Bereichs. Dasselbe gilt für
`allocated_bytes_in_range`. Die seit Runde 4 gemeinsam verwendeten
Publication-Reader werden in `from_published_locations` mit diesem Rezepttyp
gebaut. Dieser Befund betrifft somit auch Live-Reads externalisierter Daten.
Die normalen Tree-Manifeste haben bereits einen anderen, begrenzten Suchpfad.

Eine einmal gebildete Liste kumulativer Extent-Endpositionen erlaubt
Binärsuche und das anschließende Lesen nur der überlappenden Extents.
Die Probe verwendet variable FILL-Werte mit 16-KiB-Extents, 4-KiB-Anfragen an
wechselnden Positionen und denselben Ergebnis-Extent-Typ. Sie misst ausschließlich
Rezeptmetadaten, ohne DATA-I/O oder Antwortkopien.

| Extents | Linear | Endpositionsindex | Zusatz-RAM |
| --- | ---: | ---: | ---: |
| 32 | 40,0 ns | 23,0 ns | 256 Bytes |
| 512 | 479,4 ns | 26,2 ns | 4 KiB |
| 2.048 | 1.828,4 ns | 30,4 ns | 16 KiB |

Umsetzung: Den Index einmal beim validierten Rezeptaufbau erstellen und für
Read-/Allocation-Abfragen gemeinsam nutzen. Er kann bei sehr kleinen
Rezepten entfallen oder gröber gesampelt werden. Das ist ein gezielter Tausch
von wenig RAM gegen weniger Speicherzugriffe; der Index ist keine neue
durable Datenstruktur. Hole/FILL/DataSlice, EOF und leere Bereiche benötigen
Gleichheitsprüfungen. Die Kosten des Indexaufbaus sind nicht in der Tabelle
enthalten.

Rohdaten: `store-final1.txt`, `store-final2.txt`.

## 5. CacheKey enthält bereits im Payload gespeicherte Informationen

`CacheEntry`, `crates/fastdup-store/src/read_cache.rs:194`, trägt sowohl
`CacheKey { chunk_id, logical_length }` als auch `VerifiedChunkPayload`.
Der Payload besitzt dieselbe verifizierte ID und Länge bereits selbst.
Für Treffer und Admission kann direkt gegen diese Daten verglichen werden.

Auf dieser x86-64-Basis:

| Struktur | Bisher | Ohne redundanten Key |
| --- | ---: | ---: |
| VerifiedChunkPayload | 176 Bytes | 176 Bytes |
| CacheEntry | 224 Bytes | 184 Bytes |
| Vierfach-CacheSet mit Victim-Zähler | 904 Bytes | 744 Bytes |
| Feste Set-Metadaten bei 64 MiB Hard Limit | 911.232 Bytes | 749.952 Bytes |

Das spart **17,7 % der festen Set-Metadaten**, keine 17,7 % des gesamten RSS.
In dieser Konfiguration bleibt die Zahl der Sets gleich. Payload-Owner,
Backing-Charge, Shard-Locks und Ersetzungsregeln bleiben erhalten. Bei
1.000.000 Einträgen entspräche allein der entfernte Key etwa 38,1 MiB;
das ist eine Strukturrechnung, keine gemessene RSS-Senkung einer solchen
Cache-Konfiguration.

Eine vollständige Kopie des Cache-Moduls mit dieser Änderung prüft reale
warme `get`-Aufrufe inklusive Lock, Statistik und Payload-Clone. 1.019
gemeinsam residente Schlüssel, elf alternierende Samples à 200.000 Treffer:

| Lauf | Bisher | Kompakt |
| --- | ---: | ---: |
| Final 1 | 39,15 ns | 38,72 ns |
| Final 2 | 36,88 ns | 36,02 ns |
| Zusätzlicher Lauf | 39,80 ns | 38,18 ns |

Ein früher vollständiger Lauf lag bei 36,57 → 37,28 ns. Damit ist ein großer
Zugriffsgewinn nicht belegt, aber ein klarer Größengewinn ohne bisher sichtbare
substanzielle Zugriffseinbuße. Die fünf vorhandenen semantischen Cache-Tests
bestehen auch gegen das kompakte Modul; der manuelle Benchmark bleibt ignoriert.
Kollisionen, gemeinsame Backing-Abrechnung und Speicherdruck sind darunter.

Ein anfänglicher isolierter Feldvergleich bevorzugte den alten Aufbau, weil
er den Suchschlüssel aus eben dieser alten Arena las. Dieser asymmetrische
Mikrobenchmark wird **nicht** als Zugriffsnachweis verwendet; aus ihm stammen
nur die kontrollierten `size_of`-Werte.

Umsetzung unter ADR 0046: Redundanten Key entfernen, exakte ID-/Längenprüfung
und Owner-Abrechnung behalten, Geometrie weiter aus `size_of::<CacheSet>()`
berechnen. Größere Working Sets, Misses und parallele Leser sollten die
abschließende Abnahme ergänzen.

Rohdaten: `cache-full.txt`, `store-final1.txt`, `store-final2.txt`,
`cache-final3.txt`, `cache-compact-tests.txt`.

## 6. Workerzahl an die tatsächliche Kopierarbeit anpassen

`prepare_compression_regions`,
`crates/fastdup-appliance/src/checkpoint.rs:1380`, reicht materialisierende
Regionen an `WorkerPermits::map` weiter. Dieser begrenzt die Workerzahl nach
Jobzahl, berücksichtigt aber keine Arbeitsmenge. Bei mehr als einem Worker
entstehen Rayon-Jobs, Mutex-Zugriffe auf die Eingabequeue, Ergebnis-Vecs pro
Worker und eine zusätzliche geordnete Ergebnissammlung.

Die isolierte Probe ruft den tatsächlichen `map` mit dem Kopierkern
`slice.to_vec()` auf. Beide Seiten erwerben CPU-Permits: bis zu acht Worker
gegen einen Worker. Der Pool wird einmal angelegt und für alle Samples
wiederverwendet. Es wird kein Permit-Budget umgangen.

| Kopier-Batch | Bis zu acht Worker | Ein Worker |
| --- | ---: | ---: |
| 2 × 64 KiB | 39,20 µs | 11,29 µs |
| 4 × 64 KiB | 144,73 µs | 38,47 µs |
| 8 × 64 KiB | 252,41 µs | 81,78 µs |
| 8 × 512 KiB | 806,48 µs | 1.543,63 µs |
| 64 × 64 KiB | 634,51 µs | 1.525,09 µs |

Die Richtung bestätigt sich im zweiten Lauf. Bei 2 × 512 KiB wechselt der
kleine Vorteil allerdings die Seite; daraus sollte kein harter universeller
Schwellwert abgeleitet werden. Der sehr große 64 × 512-KiB-Fall profitiert auch
von anderem Allocator-/Working-Set-Verhalten und eignet sich nicht zum
Ableiten eines reinen Scheduler-Faktors.

Umsetzung: Vorzugsweise im Materialisierungsaufrufer nach Gesamtbytes und
Jobgrößen begrenzen, mit einem kurzen budgetierten seriellen Pfad. Für
Fingerprint-/Codec-Jobs darf nicht blind derselbe Byte-Schwellwert gelten:
Sie haben wesentlich mehr CPU-Arbeit pro Byte. Die Probe misst weder einen
vollständigen Materialisierungsaufruf noch die gesamte Container-Pipeline.

Rohdaten: `store-final1.txt`, `store-final2.txt`.

## Weitere Codebefunde ohne neues A/B

- `flush_stable_for_commit_cut` hält die Lane-Sperre während `publish_pending`,
  also auch über Encode und DATA-Durability. Der normale volle Container wird
  bereits detached veröffentlicht. Auch beim partiellen Commit-Drain lässt
  sich ein begrenzter Snapshot herauslösen und außerhalb der Lane-Sperre
  veröffentlichen. Dabei müssen Reservierung, Reihenfolge und die Beziehung
  zum oben beschriebenen Fence gemeinsam entworfen werden. Andernfalls wird
  lediglich Arbeit zwischen wartenden Stufen verschoben. Eine Messung des
  zusätzlichen Pipeline-Nutzens steht aus.
- Ein Online-Proof-Treffer läuft über `verified_entry`/`generation_entry` und
  anschließend `externalized_location`/`remember_active`. Das bedeutet erneut
  Lock und Baumzugriff. `remember_generation` macht bei einem vorhandenen
  Exact-Reuse-Proof nach `insert` zudem ein weiteres `get_mut`. Ein Entry-Zugriff
  kann zumindest diese letzte Suche sicher vermeiden. Eine kombinierte
  Lookup-/Touch-API muss Freeze und Active/Frozen-Ownership korrekt erhalten;
  den Touch einfach wegzulassen wäre keine gleichwertige Optimierung.

Diese Punkte sind konkrete nächste Kandidaten, aber noch keine gemessenen
Durchsatz- oder RAM-Gewinne.

## Containerformat, Zero-Copy und eigenes Unsafe

Die neuen belegten Gewinne erfordern keine Container-Migration. Der
owner-erhaltende Read-/FUSE-Pfad aus Runde 4 ist in der geprüften Basis bereits
vorhanden; die neue Arbeit liegt hauptsächlich in der Auswahl und Verwaltung
seiner Slices sowie in der Commit-Koordination.

Die Formatgrenze aus dem vorigen Audit bleibt bestehen: Ein kleiner Read aus
einem Zstd-Record kann das Dekodieren des ganzen Records erfordern, und die
vollständige Chunk-ID muss unabhängig geprüft werden. Kleinere, unabhängig
dekodierbare Frames mit authentifizierter Tabelle wären eine mögliche
Formatrichtung. Echte Teil-Chunk-Verifikation wäre darüber hinaus eine
Änderung des Integritätsmodells. Diese Runde hat hierzu **keinen neuen**
Format-/Reduction-Benchmark durchgeführt; daher keine Empfehlung für eine
Migration allein auf Basis der obigen Funktionsmessungen.

FILL profitiert bereits von compilererzeugtem SIMD aus sicherem Rust.
Zusätzliches Unsafe ist für die sechs Kandidaten nicht nötig. Die bestehende
SeqCDC-/Fingerprint-/Sparse-XOR-Vektorisierung wurde als vorhandene Optimierung
berücksichtigt, nicht erneut als fehlende Implementierung ausgegeben.

## Reproduktion, Messgrenzen und offene Abnahme

- Die Basis wurde vor den Proben als `initial-diff.patch` und
  `initial-status.txt` festgehalten. `source-provenance.json` hält Quell- und
  Binärhashes, Compiler sowie die Prüfung des unveränderten Produktionsdiffs.
- Prototypen stehen ausschließlich in `probe/`; `binaries/` bewahrt die
  verwendeten Release-Testprogramme. `reproduce.py` baut die isolierte Kopie
  mit workspace-lokalem Cargo-Target/TMPDIR und wiederholt die finalen Proben.
  Der optionale Baseline-Modus entfernt vorübergehend nur den experimentellen
  Fence in dieser Kopie, erwartet die rote Rechunk-Probe und stellt die Datei
  anschließend wieder her.
- Host: x86-64, zehn sichtbare CPUs, Intel i7-1370P; Rust 1.97.1,
  LLVM 22.1.6. Die CPU-Kopierprobe nutzt acht Rayon-Worker. Alle finalen
  Zeitmessungen laufen nacheinander, ohne parallele Builds oder SMB-Läufe.
- In der Exploration überschnitten sich das Ende der ersten CPU-Kopierprobe
  und der erste FILL-Lauf mit einem weiteren Build/Lauf. `cpu-admission.txt`
  und `fill-scan.txt` werden deshalb nicht als finale Timing-Evidenz verwendet.
  Die `*-final*`-Läufe wurden nach Ende dieser Prozesse seriell wiederholt.
- Gemessen werden Mediane alternierend ausgeführter Samples. Kleine
  Nanosekundenunterschiede sind keine Garantie einer konstanten Verbesserung.
  Die Probebuilds enthalten Warnungen zu absichtlich ungenutzten Teilen der
  kopierten Challenger-Module; es wurde kein neuer Produktions-Clippy-/Full-
  Suite-Lauf behauptet.
- Die Rechunk-Probe hatte zunächst einen roten Kontrolllauf. Der isolierte
  Challenger besteht die vier zugehörigen Szenarien zweimal. FILL-Differential-
  fälle, Read-/Manifest-Ergebnisgleichheit und fünf kompakte Cache-Tests bestehen.
  Das ist gezielte Audit-Evidenz, noch keine vollständige Produktionsabnahme
  der vorgeschlagenen Änderungen.

Nach einer Umsetzung bleibt der gewünschte getrennte SMB-Vergleich mit Normal
und Advanced/Similarity die End-to-End-Abnahme: Geschwindigkeit, p99/max,
CPU, Rechunk-/Recipe-Reuse-Bytes und tatsächlich allokierte DATA-/Metadatabytes.
FILL, Intervall- und Cache-Optimierungen sollten identische Reduction liefern;
der reparierte Publication-Fence muss hinsichtlich Containerbelegung separat
gemessen werden.
