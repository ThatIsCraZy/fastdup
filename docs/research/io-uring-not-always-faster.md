# Wann `io_uring` langsamer sein kann und was das für fastdup bedeutet

Stand: 24. August 2026

## Kurzfassung

`io_uring` ist nicht in jeder Situation schneller als `pread`, `pwrite` oder
`fsync`. Der Vorteil entsteht vor allem dann, wenn eine Anwendung mehrere
Operationen gleichzeitig offen hält, Submission und Completion stapelt und
die CPU während eines laufenden I/O für andere Arbeit nutzt. Bei Queue-Tiefe
eins, synchronem Warten nach jeder Phase und gepuffertem I/O kann der Ring
keine Parallelität erzeugen. Dann bleiben seine Verwaltungsarbeit und mögliche
Weiterleitung an `io-wq` übrig.

Für fastdup ist das kein Freispruch der aktuellen Implementierung. Die lokale
Analyse zeigt einen sicheren Shutdown-Fehler und zwei starke Kandidaten für den
Durchsatzverlust:

1. Der Ring-Worker verarbeitet nur die Commands, die er zu Beginn eines
   Cohort-Fensters einsammelt. Danach fährt er diese Gruppe bis zum vollständigen
   `fsync`, Rename und Directory-`fsync` zu Ende. Später eintreffende Commands
   können nicht in die laufende Pipeline gelangen.
2. Nach jeder Phase wartet `submit_and_wait(entries.len())` auf alle CQEs der
   Gruppe. Eine langsame Operation hält damit auch bereits fertige Operationen
   und die Annahme neuer Arbeit auf.
3. Container-Verifikation läuft synchron aus dem Ring-Worker über den globalen
   Rayon-Pool. Dieser Aufruf nimmt keine CPU-Permits der Ingest-Pipeline.
   Während der Worker auf Rayon wartet, verarbeitet er keine CQEs und nimmt
   keine Commands an.

Die Hypothese "der Ring müsste immer schneller sein, also muss jeder Rückstand
ein Bug sein" ist zu stark. Die präzisere Diagnose lautet: Der beobachtete
Rückstand ist mit veröffentlichten Eigenschaften von `io_uring` vereinbar,
aber fastdup nutzt die dafür entscheidende asynchrone Event-Loop-Struktur noch
nicht. Zusätzlich existiert mindestens ein echter Lifecycle-Bug.

## Was die Primärquellen tatsächlich belegen

### Batching ist Teil des Leistungsmodells

Jens Axboes liburing-Dokumentation beschreibt Batching ausdrücklich als einen
Hauptvorteil. Mehrere SQEs lassen sich mit einem Aufruf einreichen, und
`io_uring_submit_and_wait()` kann Einreichung und Warten verbinden. Das dort
empfohlene Muster ist eine Event Loop: CQEs verarbeiten, daraus neue SQEs
erzeugen und anschließend den nächsten gestapelten Submit ausführen.
[liburing-Wiki, "Batching"](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023#batching)

Das belegt keinen festen Break-even-Punkt. Es belegt aber, dass niedrige
Queue-Tiefe und fehlendes Batching einen wesentlichen Teil des vorgesehenen
Vorteils entfernen. Bei genau einer Operation macht der normale Ring-Pfad
weiterhin einen `io_uring_enter`-Aufruf. Die aktuelle Rust-Bibliothek ruft bei
`submit_and_wait(want > 0)` diesen Kernel-Einstieg auf, sofern kein anderer
Sondermodus greift.
[Rust-`io-uring` 0.7.14, `submit_and_wait`](https://docs.rs/io-uring/0.7.14/src/io_uring/submit.rs.html#172-242)

Axboes ursprüngliches Designpapier formuliert den Anspruch für Page-Cache-I/O
vorsichtig: `io_uring` soll dafür so effizient wie die synchronen Schnittstellen
sein. Das Papier verspricht nicht, dass ein einzelner gepufferter Zugriff
grundsätzlich schneller wird.
[Jens Axboe, "Efficient IO with io_uring"](https://www.kernel.dk/io_uring.pdf)

### `fsync` und Rename sind keine frei laufenden Inline-Operationen

Der aktuelle Linux-Quellcode markiert `IORING_OP_FSYNC` mit
`REQ_F_FORCE_ASYNC` und kommentiert, dass `fsync` einen blockierenden Kontext
braucht. Der Kernel führt es über `vfs_fsync_range()` aus.
[Linux 6.12, `io_uring/sync.c`](https://github.com/torvalds/linux/blob/v6.12/io_uring/sync.c#L53-L82)

Dasselbe gilt für `IORING_OP_RENAMEAT`: Die Vorbereitung setzt
`REQ_F_FORCE_ASYNC`; erst im blockierenden Kontext ruft der Kernel
`filename_renameat2()` auf.
[Linux 6.12, `io_uring/fs.c`](https://github.com/torvalds/linux/blob/v6.12/io_uring/fs.c#L50-L93)

Der Kern bereitet solche asynchronen Arbeiten für `io-wq` vor und stellt sie
in dessen Queue. Für normale Dateien kann er Arbeit zusätzlich nach Inode
hashen, wenn die jeweilige Operation oder das Dateisystem Serialisierung
fordert.
[Linux 6.12, `io_uring/io_uring.c`](https://github.com/torvalds/linux/blob/v6.12/io_uring/io_uring.c#L1919-L1960)

Daraus folgt: Ein blockierender Aufrufer, der ein einzelnes `fsync` in einen
Ring schickt und sofort auf dessen CQE wartet, spart die Arbeit von `fsync`
nicht ein. Er fügt Ring-Submission, Completion und unter Umständen einen
`io-wq`-Wechsel hinzu. Einen Gewinn gibt es erst, wenn diese Wartezeit mit
anderer nützlicher Arbeit oder mehreren unabhängigen Operationen überlappt.
Der letzte Satz ist eine Schlussfolgerung aus dem Kernelpfad, keine Zusage des
Kernels über eine konkrete Laufzeit.

### Gepuffertes I/O ist nicht dasselbe wie echtes asynchrones Geräte-I/O

fastdup öffnet normale XFS-Dateien ohne `O_DIRECT` und verwendet
`IORING_OP_WRITE` sowie `IORING_OP_READ`. Damit bleibt der Page Cache im Pfad.
Der Kernel versucht I/O zunächst nichtblockierend und reicht Operationen, die
so nicht fortgesetzt werden können, an `io-wq` weiter. Genau dieses "async
punt" ist im Kernelquellcode dokumentiert.
[Linux 6.12, `io_uring/io_uring.c`, `io_queue_sqe`](https://github.com/torvalds/linux/blob/v6.12/io_uring/io_uring.c#L1919-L1960)

Registrierte Dateien und Buffer können wiederholte Referenz- und Mappingkosten
senken. Die offizielle `io_uring_register(2)`-Dokumentation beschreibt diese
langfristigen Referenzen ausdrücklich.
[`io_uring_register(2)`](https://man7.org/linux/man-pages/man2/io_uring_register.2.html)
fastdup verwendet derzeit weder Fixed Files noch Fixed Buffers. Das ist eine
unerschlossene Optimierung, aber bei wenigen großen Container-Schreibvorgängen
nicht automatisch die Hauptursache.

### SQPOLL ist kein universeller Beschleuniger

fastdup erstellt den Ring mit der Standardkonfiguration. `SQPOLL` ist nicht
aktiv. Deshalb benötigt jedes `submit_and_wait(... > 0)` weiterhin einen
Kernelaufruf.

Die offizielle SQPOLL-Manpage sagt ausdrücklich, dass SQPOLL bei niedriger
IOPS-Rate, CPU-Knappheit, burstiger Last und completion-dominierter Arbeit
nicht helfen oder die Leistung verschlechtern kann. Der Poll-Thread verbraucht
CPU; nach dem Idle-Schlaf verursacht sein Aufwecken zusätzliche Latenz. Auch
mit SQPOLL kann Batching den Durchsatz weiter verbessern.
[`io_uring_sqpoll(7)`](https://man7.org/linux/man-pages/man7/io_uring_sqpoll.7.html)

Das passt schlecht zu einem bereits CPU-lastigen SeqCDC-, BLAKE3- und
Zstd-Prozess. SQPOLL sollte erst nach der Event-Loop-Korrektur separat gemessen
werden. Es verdeckt sonst möglicherweise die fehlende Parallelität mit einem
dauerhaft laufenden Kernelthread.

### `io-wq` ist ein zusätzlicher CPU- und Scheduling-Verbraucher

liburing unterscheidet begrenzte und unbegrenzte asynchrone Worker. Die Zahl
der begrenzten Worker hängt standardmäßig von Ringgröße und CPU-Anzahl ab und
kann pro Ring geändert werden.
[`io_uring_register_iowq_max_workers(3)`](https://man7.org/linux/man-pages/man3/io_uring_register_iowq_max_workers.3.html)

Damit ist belegt, dass ein Ring für blockierende Dateisystemoperationen einen
eigenen Workerbestand haben kann. Wie stark dieser Bestand mit einer konkreten
Anwendung konkurriert, muss man messen. Für fastdup ist die Konkurrenz
plausibel, weil zusätzlich Rayon-Worker für Hashing und Encoding sowie ein
separater Rayon-Aufruf aus dem Ring-Verifier laufen.

### Es gibt Primärberichte über langsamere Ring-Pfade

Ein reproduzierter Bericht im liburing-Upstream verglich eine parallele
Delete-Anwendung. Der direkte Pfad brauchte im Mittel 23,9 ms, der Ring-Pfad
31,8 ms. Queue-Tiefe eins war laut Autor etwa dreimal langsamer; Begrenzung der
`io-wq`-Worker oder Verknüpfung der Operationen vermied einen weiteren
Leistungseinbruch.
[liburing Issue 830](https://github.com/axboe/liburing/issues/830)

Ein zweiter Bericht maß etwa 30 Prozent der Zeit des Hauptthreads in
`io_uring_submit` und beschrieb eine starke Worker-Vermehrung mit
`IOSQE_ASYNC`.
[liburing Issue 420](https://github.com/axboe/liburing/issues/420)

Andres Freund meldete außerdem bei Queue-Tiefe eins 20 bis 40 Prozent weniger
Leistung als mit synchronem I/O. Axboes Gegenmessung erreichte 170.000 IOPS mit
`io_uring` und 185.000 mit `pread2`. Die Ursache lag damals im Completion-Wait-
Scheduling und wurde im Kernel korrigiert. Linux 6.12 enthält den korrigierten
Pfad, deshalb erklärt dieser alte Kernel-Fehler nicht die aktuelle fastdup-
Messung. Der Fall belegt aber, dass selbst QD1 keine automatische Ring-Garantie
hat.
[Upstream-Patchdiskussion](https://patchew.org/linux/20230707162007.194068-1-andres%40anarazel.de/),
[korrigierter Linux-6.12-Pfad](https://github.com/torvalds/linux/blob/v6.12/io_uring/io_uring.c#L2463-L2481)

Diese Issues sind Primärberichte ihrer Autoren, aber keine kontrollierten
Kernelstudien und kein Beleg für einen allgemeinen Nachteil. Sie widerlegen
jedoch die Behauptung, `io_uring` sei unabhängig von Queue-Tiefe,
Dateisystemoperation und Workersteuerung immer schneller.

### Shutdown verlangt ein sauberes Lebenszeitprotokoll

`io_uring_queue_exit()` schließt den Ring und gibt seine Ressourcen frei;
ein Teil der Abrechnung läuft noch kurz asynchron.
[`io_uring_queue_exit(3)`](https://man7.org/linux/man-pages/man3/io_uring_queue_exit.3.html)
Der Kernel enthält außerdem explizite Cancel- und Exit-Pfade für ausstehende
Requests. Ring-Abbau ist daher kein bloßes Freigeben eines Userspace-Objekts.
[Linux 6.12, `io_uring/io_uring.c`](https://github.com/torvalds/linux/blob/v6.12/io_uring/io_uring.c#L2888-L2970)

Für fastdup liegt der beobachtete 120-Sekunden-Hänger allerdings vor dem
eigentlichen Ring-Abbau. Der Userspace-Worker verliert sein Shutdown-Command.
Das ist ein lokaler Logikfehler und keine `io_uring`-Eigenschaft.

## Abgleich mit dem aktuellen fastdup-Design

Die folgenden Aussagen stammen aus dem lokalen Code und den SMB-Messungen.
Sie sind bewusst von den extern belegten Aussagen getrennt.

### 1. Die Schnittstelle ist synchron geblieben

Der Adapter beschreibt selbst das Modell: Ein gemeinsamer Worker besitzt den
Ring, während `StorageIo` blockierend bleibt.
[`fastdup-io-uring/src/lib.rs`](../../crates/fastdup-io-uring/src/lib.rs#L1)

Jeder Aufrufer erzeugt einen Reply-Channel, sendet ein Command und wartet dann
auf die Antwort. Das gilt auch für `publish_owned_container`, `fsync`, Rename
und Root-Sync.
[`fastdup-io-uring/src/lib.rs`](../../crates/fastdup-io-uring/src/lib.rs#L599)

Dieses Modell kann gewinnen, wenn mehrere Aufrufer gleichzeitig blockieren und
der Worker ihre I/Os dauerhaft überlappt. Es verliert leicht, wenn die
effektive Queue-Tiefe eins bleibt. Dann kommen zum eigentlichen I/O ein
Userspace-Channel, zwei Thread-Wakeups, Ringverwaltung und der Kernelpfad hinzu.

### 2. Der Worker ist keine fortlaufende Event Loop

`worker_loop` sammelt nur am Anfang einer Runde Commands ein. Für eine
Container-Publikation wartet er höchstens 100 Mikrosekunden auf eine Cohort.
Dann ruft er `execute_operations()` auf.
[`fastdup-io-uring/src/lib.rs`](../../crates/fastdup-io-uring/src/lib.rs#L1502)

`execute_operations()` bleibt in seiner inneren Schleife, bis alle
Publikationen dieser Gruppe komplett beendet sind. Der Worker liest in dieser
Zeit keine neu angekommenen Commands. Eine zweite Publikation, die 101
Mikrosekunden später eintrifft, kann deshalb nicht mit späteren Phasen der
ersten Publikation überlappen.

Das erklärt die lokale Telemetrie gut: 75 Publikationen führten zu 74
Root-Sync-Submissions. Das geplante Directory-`fsync`-Batching fand praktisch
nicht statt.
[SMB-Bottleneck-Bericht](../../.artifacts/benchmarks/smb-single-stream-current-bottleneck-20260824.md#L49)

Bewertung: sehr wahrscheinlicher Konstruktionsfehler für diesen Workload.
Der Ring ist groß genug, aber der Userspace-Worker füttert ihn nicht
kontinuierlich.

### 3. Jede Phase hat eine globale Completion-Barriere

Der Worker reicht alle aktuellen SQEs ein und verlangt mit
`submit_and_wait(entries.len())` die gleiche Zahl an Completions. Erst nachdem
alle CQEs vorliegen, darf irgendeine Operation in ihre nächste Phase wechseln.
[`fastdup-io-uring/src/lib.rs`](../../crates/fastdup-io-uring/src/lib.rs#L1639)

Dadurch wartet eine schnelle Publikation auf die langsamste Operation ihrer
Gruppe. Noch problematischer ist, dass die nächste Phase der schnellen
Publikation nicht eingereicht wird, obwohl ihr CQE bereits im Completion-Ring
liegen kann. Das ist das Gegenteil der von Axboe beschriebenen Event Loop.

Bewertung: sehr wahrscheinlicher Durchsatzfehler. `submit_and_wait(1)` oder ein
CQE-getriebener Wait mit fortlaufender Command-Annahme würde fertige
Operationen sofort weiterführen. Der genaue Ersatz braucht einen
Korrektheitstest für die Publish-Reihenfolge.

### 4. Die frühere CPU-Verifikation hielt Publications auf

Der untersuchte Stand startete nach den Sample-Reads eine zweite vollständige
Writer-Image-Verifikation. ADR 0059 hat diesen Pool entfernt. Der aktuelle
Publisher übernimmt die vom Encoder erzeugte Location-Evidenz und wechselt
nach dem letzten erfolgreichen Sample direkt zu File Sync.

Die SMB-Telemetrie passt dazu. Im Ring-Pfad stieg die Runnable-Zeit von Encode
und Chunk-Hash um 7 bis 16 Prozent. Der isolierte Publisher war dagegen nicht
langsamer: Ring 598 und 613 ms, synchron 622 und 747 ms.
[SMB-Bottleneck-Bericht](../../.artifacts/benchmarks/smb-single-stream-current-bottleneck-20260824.md#L23)

Bewertung: starker Integrationsverdacht, aber noch kein kausaler Beweis. Der
nächste A/B-Test sollte die Verifikation in dasselbe Permit-Budget aufnehmen
oder sie außerhalb des Ring-Owners ausführen, ohne die I/O-Event-Loop zu
blockieren.

### 5. Der Shutdown-Hänger ist erklärt

`ActiveBackend::drop` sendet `Command::Shutdown` und wartet mit `join()` auf
den Worker.
[`fastdup-io-uring/src/lib.rs`](../../crates/fastdup-io-uring/src/lib.rs#L712)

Trifft das Command während der 100- oder 200-Mikrosekunden-Cohort-Schleife ein,
bricht nur diese innere Schleife ab. Das Command wird verworfen. Nach Ende der
laufenden Operationen wartet die äußere Schleife erneut auf `receiver.recv()`.
Der Sender existiert noch, weil `drop` auf `join()` wartet. Beide Seiten warten
dann unbegrenzt.

Bewertung: sicherer Implementierungsfehler. Er erklärt den im SMB-Test
beobachteten SIGINT-Timeout. Er erklärt nicht automatisch den Durchsatzverlust,
muss aber vor weiteren Ring-Experimenten behoben werden.

### 6. Weitere Kosten, deren Rang noch offen ist

- `write_at` kopiert geliehene Bytes zuerst in einen neuen `Vec`.
  `publish_owned_container` vermeidet diesen Pfad bereits, daher ist das nicht
  die Hauptkopie der aktuellen Container-Publikation.
- Dateien und Verzeichnisse werden vor den Ring-Commands synchron geöffnet.
  Fixed Files werden nicht verwendet.
- Nach den Schreibphasen ruft der Ring-Worker synchron `File::set_len()` auf.
  Bei einer neu erzeugten, lückenlos geschriebenen temporären Datei wirkt das
  redundant. Ob es entfernt werden darf, ist eine Format- und Fehlerfallfrage.
- Der Ring hat 256 Einträge. Die SMB-Telemetrie zeigt jedoch keine entsprechend
  hohe effektive Queue-Tiefe. Ringkapazität ersetzt keine gleichzeitig
  laufenden Operationen.

## Was als Nächstes gemessen werden sollte

Die nächsten Schritte sollten die Implementierung prüfen, nicht wahllos Ring-
Flags einschalten.

1. Zuerst den verlorenen Shutdown-Zustand beheben und mit einem Test abdecken,
   der Shutdown in beiden Cohort-Schleifen injiziert.
2. Telemetrie für echte Ring-Tiefe ergänzen: SQEs pro Submit, aktive
   Operationen, Zeit zwischen Command-Ankunft und erster SQE, CQE bis nächste
   SQE, io-wq-Weiterleitungen und Zeit des Ring-Workers in CPU-Verifikation.
   Der Kernel stellt dafür unter anderem Tracepoints für asynchron eingereihte
   Arbeit, CQ-Waits und verzögerte Requests bereit.
   [Linux 6.12, `include/trace/events/io_uring.h`](https://github.com/torvalds/linux/blob/v6.12/include/trace/events/io_uring.h)
3. Den Worker als echte Event Loop prototypisieren. Neue Commands müssen auch
   bei laufendem I/O aufgenommen werden. Ein CQE soll nur seine eigene
   Operation weiterschalten, ohne auf die langsamste Operation der Gruppe zu
   warten.
4. Container-Verifikation aus dem Ring-Owner entfernen oder an das gemeinsame
   CPU-Permit-Budget binden. Der Owner muss während BLAKE3-Arbeit weiter CQEs
   verarbeiten können.
5. Danach denselben SingleStream-A/B-Test gegen den synchronen Pfad ausführen.
   Der isolierte Publisher bleibt ein Zusatztest, weil er die gemessene
   CPU-Konkurrenz nicht enthält.
6. Erst wenn die Event Loop genügend Queue-Tiefe erreicht, Fixed Files,
   registrierte Buffer und SQPOLL jeweils einzeln testen. Für SQPOLL muss neben
   Durchsatz und p99 auch gesamte CPU-Zeit gelten.

## Urteil

Es gibt keinen Linux- oder liburing-Beleg für die Regel "`io_uring` ist immer
schneller". Die Primärquellen nennen Batching, gleichzeitige Arbeit und ein
passendes Completion-Modell als Bedingungen für den Nutzen. Sie dokumentieren
zugleich zusätzliche Worker und klare SQPOLL-Nachteile bei CPU-knappen oder
burstartigen Lasten.

Trotzdem ist der Verdacht auf eine fehlerhafte fastdup-Integration berechtigt.
Der Shutdown-Pfad ist nachweislich kaputt. Die globale Batch-Barriere, die
fehlende Aufnahme neuer Commands während laufender Publikationen und die
blockierende CPU-Verifikation im Ring-Owner sind keine unvermeidlichen
`io_uring`-Kosten. Sie verhindern genau die Überlappung, für die der Ring gebaut
wurde. Diese drei Punkte sollten vor einem Urteil über `io_uring` auf diesem
System korrigiert oder isoliert widerlegt werden.
