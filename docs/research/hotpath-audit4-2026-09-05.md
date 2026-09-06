# Vierter Hotpath-Audit: Live-Reads, Kopien und CPU-Admission

Stand: 2026-09-05, Quellbasis `a2c59d1` (v0.6-Arbeitsbaum).
Die drei bisherigen Optimierungsrunden sind in dieser Basis enthalten.
Dieser Audit verändert keinen Produktionscode. Proben und Challenger liegen
unter `.artifacts/hotpath-audit4-20260905/`; die schon vorhandenen Änderungen
an ADRs bleiben unangetastet.

Die stärksten weiteren Ansatzpunkte liegen bei Live-Reads vor dem nächsten
Commit, beim Externalisieren veröffentlichter Chunks und bei der tatsächlichen
Lebensdauer von CPU-Reservierungen. Die geprüfte weitere Fingerprint-Variante
liefert dagegen noch keinen stabilen Vorteil.

## Prioritäten

| Priorität | Ansatz | Evidenz / Grenze |
| --- | --- | --- |
| 1 | Read-Plan aus den tatsächlich sichtbaren Bereichen bilden | Vollständig überschriebene Bereiche lesen trotzdem die alte Quelle; Dirty-Payload wird schon während der Planung kopiert. |
| 1 | Live-Quellen an gemeinsamen verifizierten Batch-/Cache-Reader anbinden; I/O aus Inode-Sperre lösen | 128-KiB-Probe: acht statt eines Reads desselben 512-KiB-Records. Zusätzlich reproduzierter API-Mismatch bei Prefix. |
| 1 | Externalisierung zusammenhängender Chunks wirklich als Batch anwenden | Pro 1-MiB-Eingabepuffer 128–288 KiB vermeidbare Reststückkopien; gruppierte Probe null. |
| 1 | Hash-/Encode-Permits vor Erwerb begrenzen und fertig gewordene Worker freigeben | Beide Aufrufer begrenzen teilweise erst nach Erwerb; Batch-Tail-Probe hält zehn Permits für einen verbleibenden Job. |
| 2 | Advanced-Base-Wellen nachfüllen und Base-Reads zusammenfassen | Codepfad lädt seriell, schließt eine komplette Welle vor der nächsten ab; auf dem bisherigen ISO nur etwa 73 ms summierte Base-Zeit. |
| 2 | Zstd direkt in reservierte Vec-Kapazität schreiben | Sicherer Bibliothekspfad, identische Frames und Cap-Entscheidungen; gemischte Fixture rund 1–2 % schneller. |
| 2 | FUSE-Antworten aus mehreren verifizierten Slices senden | Aktuelle Reply-Schnittstelle verlangt einen zusammenhängenden `Bytes`-Wert; vor allem RAW-/gemischte Reads kopieren weiter. Noch kein FUSE-A/B. |

## 1. Die sichtbare Version vor dem Lesen auflösen

Relevante Stellen:
[`ReadPlan::new` und `execute_shared`](../../crates/fastdup-posix/src/versioned_file.rs#L1944),
[`PlannedEpoch::new`](../../crates/fastdup-posix/src/versioned_file.rs#L1839),
[`plan_data`](../../crates/fastdup-posix/src/versioned_file.rs#L2021).

Der aktuelle Plan sammelt die überlappenden Daten jeder Dirty Epoch. Bei einem
gemischten Read allokiert `execute_shared` eine genullte Antwort, liest den
gesamten überlappenden committed Bereich über die Vec-API und kopiert ihn in die
Antwort. Anschließend werden ältere und jüngere Dirty-Daten darübergelegt.
Auch ein vollständig von neuem DATA oder einem Hole verdeckter committed
Bereich wird dabei gelesen. `plan_data` kopiert die Dirty-Bytes zusätzlich
bereits in `PlannedData.bytes: Vec<u8>`.

Die Probe verwendet den kopierten aktuellen `VersionedFile` und einen
zählenden committed Reader. Ein kompletter Overwrite erzeugt bei 4 KiB,
64 KiB und 1 MiB jeweils:

- genau einen unnötigen Read der vollständig verdeckten committed Quelle;
- eine Dirty-Plan-Kopie in voller Anfragelänge;
- danach die Antwortallokation und die Überlagerung.

Ein eng begrenzter Challenger prüft einen einzigen vollständig abdeckenden
aktiven DATA-Extent und gibt dessen vorhandenen `Bytes`-Owner als Slice zurück.
Die Ergebnisse sind bytegleich. Bei 64 KiB benötigt der aktuelle Pfad in den
beiden Läufen 5,362 beziehungsweise 5,464 µs; die enge View-Probe etwa 17 ns.
Das ist **kein vollständiger ReadPlan-Ersatz und kein FUSE-/SMB-Speedup**. Die
Probe zeigt, wie viel Arbeit dieser konkrete Vollabdeckungsfall vermeidet.

Umsetzung: Zuerst die sichtbaren Intervalle von der neuesten Epoch rückwärts
auflösen. Nur noch ungedeckte committed/externe Bereiche lesen. Für resident
DATA immutable `MutationPayload`-Views halten. Eine einzelne abdeckende View
kann direkt zurückgegeben werden; sonst einmal geordnet zusammensetzen.

Prüfung: teilweise/vollständige Overwrites, aktive plus eingefrorene Epoch,
Hole/FILL, Truncate mit anschließendem Grow, CloneRange, spätere Mutation nach
Planbildung und Fehler einer tatsächlich benötigten Quelle. Verdeckte Quellen
dürfen keine I/O auslösen; die eingeplante sichtbare Version muss stabil bleiben.

## 2. Live-Read-I/O unter der Inode-Sperre und am Batch-Reader vorbei

[`Namespace::read`](../../crates/fastdup-posix/src/lib.rs#L4708) hält
`object.state.read()` während `plan_read`.
[`plan_external`](../../crates/fastdup-posix/src/versioned_file.rs#L2051)
ruft schon dort `external.source.read_at(...)` auf. In der Probe ist der
externe Read-Zähler bereits **vor `execute_shared` gleich eins**. Dieser
Read kann Backend-I/O und Dekompression unter der Inode-Lesesperre ausführen
und damit einen Writer desselben Inodes aufhalten.

Der Write-through-Adapter
[`VerifiedLocationFile::read_at`](../../crates/fastdup-appliance/src/checkpoint.rs#L1140)
nutzt `ContainerRepository::read_verified_location`. Er hat weder einen
gemeinsamen Verified-Read-Cache noch einen Batch-Aufruf oder einen eigenen
`read_shared_at`-Pfad. Für einen Chunk-Ausschnitt kann zunächst ein kompletter
Chunk-Vec entstehen, danach ein weiterer Vec für den Ausschnitt und später
die ReadPlan-Antwortkopie. Der vorhandene Singleflight hilft gleichzeitig
laufenden gleichen Record-Reads, nicht aufeinanderfolgenden Aufrufen.

Mit dem tatsächlichen Store-Code, einem warmen Descriptor, einem Zstd-Record
aus 32 verschiedenen 16-KiB-Chunks und acht angefragten Chunks:

| Pfad | Antwortbytes | DATA-Range-Reads | gelesene Encoding-Bytes | notwendige Decodes |
| --- | ---: | ---: | ---: | ---: |
| Acht Location-Einzelaufrufe wie bei Live-Quellen | 131.072 | 8 | 32.256 | 8 × 512 KiB |
| Bereits vorhandener Manifest-Batch-Reader | 131.072 | 1 | 4.032 | 1 × 512 KiB |

Beide Ergebnisse sind bytegleich. Acht wiederholte Einzelaufrufe desselben
Chunks verursachen ebenfalls acht DATA-Reads. Gezählt werden Storage-API-Reads,
keine physischen HDD-Zugriffe; die Dateien liegen im warmen Dateisystemcache.

**Zusätzlicher Korrektheitsbefund:** Ein gültiger Prefix-Record wird vom
gleichen Location-Einzelaufruf mit `Format(DependentBaseRequired)` abgewiesen.
Der gepinnte Manifest-Reader liest denselben Target bytegleich mit Base-Auflösung.
`externalized_proven_location` konstruiert den Live-Adapter auch aus den
zurückgegebenen dependent Locations. Damit passt der gewählte Live-Read-Aufruf
nicht zu allen publizierbaren Codecs. Die Probe reproduziert diesen API-Mismatch;
ein vollständiger FUSE-Test im Zeitfenster vor dem Commit wurde nicht ausgeführt.

Umsetzung: Externe Quellen/Koordinaten unter der Inode-Sperre nur pinnen,
danach außerhalb der Sperre lesen. Ein gemeinsamer, Location-verifizierender
Batch-Pfad soll Record-Gruppierung, Verified-Cache, Owner-Views und Depth-1-
Base-Auflösung auch für Live-Quellen bereitstellen. Bestehende GC-/Generations-
Pins und die Prüfung der konkreten Location dürfen dabei nicht entfallen.

Prüfung: blockierter externer Read bei gleichzeitigem Write, gemeinsamer Record
über mehrere Live-Extents, wiederholte Reads, Cache-Druck, Prefix/Sparse-XOR
vor dem Commit sowie durch GC verlegte oder fehlerhafte Locations.

## 3. Externalisierung zerstückelt einen vorhandenen Batch

[`VersionedFile::externalize_many`](../../crates/fastdup-posix/src/versioned_file.rs#L1048)
läuft durch die Kandidaten und ruft für jeden einzeln
`self.active.data.externalize_many(vec![...])` auf. Der darunterliegende
[`SparseData`-Helfer](../../crates/fastdup-posix/src/lib.rs#L2049) kann bereits
zusammenhängende Kandidaten zu Runs verbinden und die Resident-Bereiche einmal
entfernen. Diese Fähigkeit kommt durch die Einzelaufrufe nicht zum Tragen.

Beim Entfernen einzelner Chunks werden Reststücke eines größeren Request-
Buffers erzeugt. `MutationPayload::retained_fragment` kopiert kleine Reststücke,
damit sie nicht mehr als die zulässige Backing-Amplifikation behalten. Das ist
für dauerhafte kleine Reste sinnvoll. Hier kann aber bereits der nächste
Kandidat denselben Rest weiter entfernen.

Audit-Zähler direkt an dieser Kopierstelle, ein 1-MiB-Write, anschließend
vollständige Externalisierung:

| Chunkgröße | Aktueller Wrapper | Gruppierter unterer Helfer |
| --- | ---: | ---: |
| 16 KiB | 294.912 kopierte Bytes | 0 |
| 64 KiB | 196.608 kopierte Bytes | 0 |
| 128 KiB | 131.072 kopierte Bytes | 0 |

Die Live-Bytes bleiben in allen sechs Fällen gleich. Das sind gezählte Kopien,
kein gemessener SMB-Durchsatzgewinn. Der Challenger umgeht für diese gültige
Fixture die äußere Einzeliteration; er ersetzt noch nicht deren allgemeine
Fehler-/Active-/Frozen-Behandlung.

Umsetzung: Kandidaten weiterhin einzeln auf Sequence, Holes und aktuelle
Coverage prüfen, gültige Active-Kandidaten aber gemeinsam anwenden. Frozen-
Recipes getrennt erhalten. Teilweise Annahme, veraltete Kandidaten und die
Backing-Amplifikationsgrenze müssen unverändert funktionieren.

## 4. CPU-Permits noch näher an tatsächliche CPU-Arbeit binden

Die vorige Runde hat `WorkerPermits::map` auf die Jobzahl begrenzt. Zwei
eigenständige Aufrufer behalten den alten Ablauf:

- [`extract_stable_chunks`](../../crates/fastdup-appliance/src/checkpoint.rs#L3611)
  erwirbt `desired_workers` und begrenzt erst danach auf `batch.len()`.
- [`publish_new_chunks`](../../crates/fastdup-appliance/src/checkpoint.rs#L3915)
  erwirbt ebenfalls zuerst. Der Format-Encoder reduziert intern auf
  `regions.len().max(1)`. Schon vorbereitete Independent-/Dependent-Records
  benötigen keine normalen Region-Encoder, können aber die große Reservierung
  während der seriellen Assemblierung behalten.

Zudem besitzt ein `WorkerPermits::map`-Batch nur eine gemeinsame Lease bis nach
der vollständigen Sammlung. Die kontrollierte Probe meldet:

```text
completed_jobs=9 active_jobs=1 available=0 new_grant=0
```

Neun Jobs haben ihre Nutzarbeit beendet, ein Job wird absichtlich angehalten.
Ein konkurrierender Aufrufer bekommt keines der neun potenziell freien Permits.
Nach Freigabe kommen alle zehn zurück; es handelt sich nicht um ein Leak.

Schließlich enthält `active_writers` auch `publish_detached_container` während
der Storage-Publication. `workers_per_ingest_job` teilt das Budget durch diese
Anzahl, selbst wenn ein Teil der gezählten Koordinatoren gerade I/O abwartet.
Bei zehn CPUs und drei so gezählten Jobs fragt eine CPU-Phase nur drei Worker
an, auch wenn mehr Permits verfügbar wären. Die praktische Häufigkeit ist noch
nicht gemessen.

Umsetzung: Hash-/Encode-Anfrage schon vor `acquire` begrenzen; vorbereitete
Records nur mit der nötigen seriellen CPU-Reservierung assemblieren. Größere
Leases in einzelne Worker-Anteile teilen beziehungsweise nach der Parallelphase
auf einen Anteil verkleinern. Fertige Worker geben ihren Anteil sofort zurück.
CPU-Fairness an tatsächlich ausführbare Phasen binden und zusätzliche freie
Kapazität kontrolliert nutzbar machen.

Prüfung: ungleich lange Jobs mit gleichzeitigem Hash/Decode, kleine und rein
vorbereitete Batches, Fehler/Panic-Abwicklung, neue konkurrierende Phasen und
nachweislich eingehaltenes Gesamtbudget. Kein wartendes `acquire` innerhalb
eines Rayon-Jobs oder während gehaltenen Singleflight-Leadern einführen.

## 5. Base-Wellen bilden noch serielle Barrieren

[`plan_batch_for_publication_cached`](../../crates/fastdup-store/src/persistent_reduction.rs#L312)
nimmt acht Targets, lädt deren nächste Basis nacheinander und startet erst
danach die parallelen Trials. Die nächste Target-Welle beginnt erst, wenn alle
Targets dieser Welle beendet sind. Ein einzelnes Target mit weiteren Trials
kann damit eine unterfüllte Restwelle aufrechterhalten.

Umsetzung in zwei Schritten: gleiche benötigte Base-Identitäten pro Welle
zusammenfassen und bestehende Record-/Cache-Gruppierung wiederverwenden;
anschließend freie Slots mit neuen Targets nachfüllen. Höchstens acht
verifizierte Base-Owner bleiben erhalten. Ein nächster Kandidat darf erst nach
dem Ergebnis des vorherigen Trials angefordert werden, damit das bisherige
Trial-Budget keine zusätzliche spekulative I/O bekommt.

Eine weitere asynchrone Überlappung gehört an die bestehende bounded I/O-
Schnittstelle, nicht als blockierender Read in alle Rayon-Worker. Vor höherer
HDD-Parallelität verlangt auch ADR 0077 entsprechende physische Messungen.
Im letzten Advanced-ISO-Abschlusslauf entfielen nur rund 72,7 ms auf 69
Base-Reads, bei 3.389 ms summierter Fingerprint-Zeit. Dieser Ansatz dürfte bei
vielen Similarity-Treffern oder kalten Bases wichtiger sein als bei diesem ISO.
Diese Telemetriewerte stammen aus der vorherigen Runde, nicht aus einem neuen
SMB-Lauf; summierte Phasenzeit ist weder reine CPU-Zeit noch Gesamtlatenz.

## 6. Sicherer Zstd-Vec-Pfad spart noch eine Zielpuffernullung

[`AdaptiveEncoderV1::zstd_owned_payload`](../../crates/fastdup-format/src/container.rs#L3760)
reserviert `payload_cap`, setzt mit `resize` alle Bytes auf null und übergibt
eine mutable Slice. Die im Lockfile vorhandene `zstd-safe`-Version 7.2.4 kann
bereits direkt in einen reservierten `Vec<u8>` schreiben. Sie setzt dessen
initialisierte Länge erst nach einem erfolgreichen Ergebnis. Der fragmentierte
Encoder verwendet diese Eigenschaft bereits über `OutBuffer`.

Der Challenger verändert nur dieses Ausgabeziel. Kontext-Reset, Level 3,
Fehlerbehandlung und Nutzgrößen-Cap bleiben erhalten. Frames und Ablehnungen
sind identisch, einschließlich Caps unmittelbar unter, auf und über der
Framegröße. Da Vec-Kapazität mindestens der reservierten Größe entspricht,
muss der Challenger die geschriebenen Bytes nochmals gegen `payload_cap`
prüfen; die Allocator-Kapazität darf kein geändertes Auswahlkriterium werden.

Elf alternierende Samples je Seite, 128 Kompressionen pro Sample:

| Fixture | Faktor Lauf 1 | Faktor Lauf 2 |
| --- | ---: | ---: |
| 64 KiB, gemischt komprimierbar | 1,019× | 1,015× |
| 512 KiB, gemischt komprimierbar | 1,014× | 1,019× |
| 64 KiB, inkompressibel / abgelehnt | 1,106× | 1,061× |
| 512 KiB, inkompressibel / abgelehnt | 1,185× | 1,158× |
| 512 KiB, konstant | 1,242× | 1,146× |

Konstante Chunks werden im realen Ingest häufig bereits als FILL behandelt;
inkompressible Regionen können am Gate ausscheiden. Die größeren Faktoren
dieser Fälle sind deshalb kein realistisches pauschales Write-Versprechen.
Der gemischte Fall spricht für eine kleine, sichere Optimierung. Eigenes neues
Unsafe ist für diesen Bibliotheksaufruf nicht nötig.

## 7. Scatter/Gather bis zum FUSE-Reply

Der neue `VerifiedReadView` kann nur direkt benachbarte Quellbereiche desselben
Owners verbinden. Bei verschiedenen Ownern oder RAW-Headerlücken muss
[`ManifestReadOutput::owned`](../../crates/fastdup-store/src/manifest_reader.rs#L832)
weiterhin eine zusammenhängende Kopie bilden.
[`ReplyData`](../../vendor/fuse3/src/raw/reply.rs#L142) trägt genau einen
`Bytes`-Wert. `ResponseSender::send2` und `FuseConnection::write_vectored`
nehmen Header plus einen Body entgegen. Der Kernel-Vektorpfad wird also bereits
benutzt, transportiert aber noch keine Liste verifizierter DATA-Segmente.

Nächster Schritt: eine begrenzte Reply-Variante mit mehreren Owned-Slices,
durchgereicht bis zu einem einzigen vectored Reply. Alle betroffenen Chunks
müssen vor dem Reply verifiziert sein, alle Owner bis zum I/O-Abschluss leben.
Längen-/iovec-Limits und ein zusammenhängender Fallback sind nötig. HOLE/FILL
können begrenzte wiederverwendete Füllbereiche verwenden. Das entfernt eine
Userspace-Zusammenfügung; es behauptet keine kopierfreie Kernelübertragung.

Dieser Punkt war zuvor zurückgestellt. Die verbleibende Schnittstellengrenze
ist jetzt bis in den vendorten FUSE-Transport geprüft. Ein echtes FUSE-A/B mit
RAW, Zstd, gemischten Extents, kurzen Reads und vielen Segmenten fehlt noch.

## Geprüft, vorerst zurückgestellt: weiterer Fingerprint-Tuningversuch

Der Challenger legt die ausgehenden Byte-Hashes schon um 32 Bit rotiert in
einer zusätzlichen 2-KiB-Tabelle ab. Eine zweite Variante iteriert außerdem
über zwei begrenzte Slices mit `zip`, um Indexarithmetik zu vereinfachen.
Der vorhandene AVX2-Vote-Kern bleibt in allen Varianten identisch.

32 MiB desselben Rocky-ISO ab Offset 256 MiB, 512 Chunks à 64 KiB,
elf rotierende Samples pro Variante:

| Lauf | Aktuell ms | Rotierte Tabelle ms | Zusätzlich Slice-Zip ms |
| --- | ---: | ---: | ---: |
| 1 | 38,972 | 37,992 | 37,309 |
| 2 | 42,200 | 42,330 | 41,440 |
| 3 | 37,869 | 37,392 | 38,153 |

Jeweils 1.795 Fälle stimmen vollständig bei Superfeatures und Sketch überein:
255 kurze Längen, 1.024 ISO-Ausschnitte, vier konstante Maximalchunks und der
gesamte Batch. Die anfangs beobachteten 2–4 % Verbesserung sind jedoch nicht
stabil. Damit gibt es hier noch keinen ausreichenden Grund für zusätzliche
Tabellen oder Unsafe. Größere SIMD-Umbauten sollten erst ein Profil des
verbleibenden Rolling-/Minimizer-Kerns und ein klareres A/B gewinnen.

## Containerformat: welche Grenze tatsächlich bleibt

Ein RAW-Chunk benötigt aktuell 128 Bytes Record-Header, 64 Bytes Chunk-Tabelle
und 128 Bytes Recovery-Index-Eintrag, also **320 Bytes vor Padding und Envelope**.
Das sind bei 64-KiB-Chunks etwa 0,49 %. Ein Format mit gemeinsamem Metadata-
Bereich könnte Headerlücken reduzieren, aber der reine Platzgewinn ist klein.
Scatter/Gather kann die Antwortkopie schon ohne Formatmigration vermeiden.

Bei Zstd ist die wichtigere Grenze die vollständige Record-CRC, Dekompression
und Chunk-Verifikation für bis zu 512 KiB. Die bereits gemessene Tar-Geometrie
aus Runde 2 bleibt relevant: kleinere Records machten die kleinen Reads 6,38×
schneller, kosteten jedoch 9,27 % mehr Containerplatz. Das waren bestehende
Formatvarianten, keine neue Messung dieses Audits.

Eine mögliche neue Version könnte unabhängig dekodierbare Subframes mit einer
authentifizierten Offset-/Längentabelle und separaten Integritätsgrenzen tragen.
**Kleinere Zstd-Frames allein reichen aber nicht:** Der aktuelle Reader muss
weiterhin die vollständige BLAKE3-Chunk-ID prüfen. Ein 256-KiB-Chunk über vier
64-KiB-Frames verlangt daher weiterhin alle vier Frames. Subframes sollten
zunächst vollständige Chunks gruppieren; echte Teil-Chunk-Verifikation wäre
eine zusätzliche Änderung des Integritätsmodells.

Vor einer Migration: kleinen Random-Read und vollständigen Restore samt
Reduktion vergleichen; Writer, Reader, Recovery, Scrub, GC/Transplant und
Downgrade-/Versionsbehandlung paaren. Bestehende Container dürfen nicht still
anders interpretiert werden. Die aktuell belegten Live-Read-Mehrfachdecodes
sollten zuerst entfernt werden, bevor größere Formatkosten eingeführt werden.

## Reproduktion und Umfang

- `probe/`: isolierte Kopien von Store und POSIX sowie die Probe-Binärquelle;
  keine Abhängigkeit des Produktions-Workspaces.
- `source-provenance.json`: vollständiger HEAD, Hashes der Ausgangsquellen,
  zusätzliche inspizierte Dateien, Compiler und finale Probe-Binärdatei.
- `probe-run1.txt`, `probe-run2.txt`: Fingerprint-, Lease-, Live-Read-,
  Externalisierungs- und Zstd-Proben; jeweils erfolgreiche Assertions.
- `fingerprint-run3.txt`: zusätzlicher Lauf wegen des kleinen Zeitunterschieds.
- `live-read-final.txt`: reale Store-Range-Zähler, Bytegleichheit sowie
  dependent Location-Aufruf gegen funktionierenden Manifest-Read.
- `reproduce.sh`: erneuter Release-Build mit lokalem Target/TMPDIR und Aufrufe.

Die Proben liefen nacheinander, ohne parallele Builds oder SMB-Benchmarks,
mit einem Rayon-Pool aus zehn Workern. Die Quellkopien wurden nach Abschluss
gegen die Produktionsquellen geprüft und sind unverändert. Zwei frühe
Build-Versuche scheiterten an Fehlern des neu erstellten Probe-Codes; diese
wurden vor den aufgeführten Läufen korrigiert. Die finale Probe baut erfolgreich.
Es wurde keine neue Produktionsimplementierung, Unsafe-Schnittstelle oder
Container-Version aktiviert und kein neuer SMB-Gesamtdurchsatz gemessen.
