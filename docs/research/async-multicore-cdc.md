# Asynchrones und MultiCore-CDC im Write-through-Ingest

Stand: 2026-08-22. Diese Notiz bewertet Ausführungsmodelle für die bestehende
FastCDC-v1-Write-through-Pipeline. Sie schlägt keine Änderung des dauerhaften
Formats vor.

## Nicht verhandelbare Grenzen

Die akzeptierte FastCDC-v1-Policy hat feste Grenzen von 16 KiB, 64 KiB und
256 KiB. Gear-Tabelle und Masken sind ebenfalls fest. Jede Implementierung
muss dieselben Grenzen liefern. Das verlangt [ADR 0014](../adr/0014-allow-chunking-profiles-per-data-region.md).
Die Ingest-Lane bewahrt zudem eine begrenzte CDC-Suffixmenge, publiziert nur
vollständig verifizierte Container und hält CPU-, Speicher- und
Publication-Backpressure innerhalb eines gemeinsamen Budgets. Das legen
[ADR 0040](../adr/0040-prepublish-streaming-containers-and-coalesce-generation-commits.md),
[ADR 0041](../adr/0041-overlap-one-frozen-commit-with-bounded-ingest-lanes.md)
und [ADR 0050](../adr/0050-overlap-reduction-stages-under-one-memory-and-cpu-budget.md)
fest.

Das schließt die naheliegende Abkürzung aus, einen Byte-Stream an beliebigen
Stellen zu teilen und jede Hälfte unabhängig zu chunkieren. Ein Schnittpunkt
kann eine andere FastCDC-Geschichte erzeugen. Selbst wenn Restore korrekt
bliebe, änderten sich Chunk IDs, Exact-Hits und die physische Belegung.

## Was die Quellen tatsächlich zeigen

### FastCDC und Streaming

FastCDC beschleunigt CDC durch Gear-Hashing, das Überspringen möglicher
Grenzen vor der Mindestgröße und eine Normalisierung der Größenverteilung. Die
Originalauswertung meldet gegenüber dem besten untersuchten Rabin-CDC etwa
10x und gegenüber Gear- und AE-CDC etwa 3x höhere Chunking-Geschwindigkeit,
bei fast gleichem Dedupe-Ergebnis. Das ist ein Algorithmusvergleich, keine
Garantie für die End-to-End-Leistung von fastdup.

Die verwendete Rust-Implementierung bietet `StreamCDC` und `AsyncStreamCDC`,
aber beide sind ein Streaming-Adapter um einen einzelnen Chunker. `StreamCDC`
hat einen Puffer mit höchstens `max_size`. Asynchrones Lesen lässt daher
Wartezeit auf I/O verdecken, parallelisiert aber nicht die Grenzen eines
einzelnen Streams. Bei fastdup kommen die Daten bereits als besessene
FUSE-Schreibpuffer an. Ein Wechsel zu `AsyncStreamCDC` würde weder die
Annahme-Latenz noch die CPU-Arbeit des vorhandenen Scanners senken, aber eine
zweite Puffer- und Zustandsverwaltung einführen.

Der lokale Scanner ist bereits passend angeordnet: ein serieller,
zustandsbehafteter FastCDC-Schnitt pro Inode-Lane, dann unabhängiges Hashen
vollständiger logischer Chunks und Codieren vollständiger 512-KiB-Regionen.
Die 1-MiB-FUSE-Schreibjobs sind Transporteinheiten. Sie dürfen keine
CDC-Entscheidungsgrenzen werden.

### Paralleles CDC ist möglich, aber nicht billig

P-Dedupe teilt den Strom in Abschnitte, chunked sie parallel und repariert
anschließend die Abschnittsgrenzen. Das FAST'12-Poster nennt beispielhaft
128-KiB-Abschnitte und verlangt, dass ein Abschnitt größer als die maximale
Chunkgröße ist. Die spätere P-Dedupe-Arbeit berichtete 3 bis 4x auf vier
Kernen, verlor aber etwa 0,02 Prozentpunkte Dedupe. Für fastdup reicht das
nicht. Das Produkt verlangt identische FastCDC-v1-Grenzen, nicht nur einen
ähnlichen Reduktionswert.

MUCH zeigt einen anderen Weg. Es fordert ausdrücklich Chunking-Invarianz
unabhängig von Segmentgröße und Threadzahl und beweist diese über Dual-Mode
Chunking und ein Coalescing zwischen Segmenten. SS-CDC trennt das Rechnen
aller Rolling-Hash-Kandidaten von der leichten, seriellen Auswahl gültiger
Grenzen. Der Aufsatz behauptet so dieselben Grenzen wie sequentielles CDC.
Beide Ergebnisse sind wichtig, aber sie gelten für ihre eigenen
Rolling-Hash-Algorithmen und Reparaturregeln. Sie beweisen nicht, dass das
Gear-Verfahren von FastCDC ohne neue, profilierte Spezifikation auf dieselbe
Weise zerlegbar ist.

SS-CDC liefert trotzdem die richtige Architekturfrage: Kann eine Vorstufe
unabhängige *Kandidaten* berechnen, während ein einziger deterministischer
Selektor weiterhin die Minimum-, Normalisierungs- und Maximum-Regeln
anwendet? Nur wenn ein formaler Gleichheitsbeweis gegen den aktuellen
`segmented_fastcdc_cut` und ein Golden-Corpus vorliegt, wäre das ein
Kandidat. Die Kandidatenstufe müsste an jedem Segment genügend Überlappung
besitzen, um alle vom Gear-Zustand benötigten Bytes abzudecken. Ohne diesen
Beweis darf sie nicht in die Production-Policy.

Ein neuer akademischer VectorCDC-Manuskriptbefund verschärft die Bewertung für
FastCDC. Die Autoren fanden bei SS-CDC-Beschleunigung keinen Durchsatzgewinn
für FastCDC. Die Entkopplung rechnet Kandidaten auch im Bereich vor der
Mindestgröße und hebt damit FastCDCs Sub-Minimum-Skipping auf. Für Gear und
CRC meldeten sie etwa 3x beziehungsweise 2x, für FastCDC keinen Gewinn. Das
Paper ist als Manuskript ausgewiesen, nicht als bestätigter Produktionswert,
aber der negative Mechanismus entspricht genau der FastCDC-v1-Policy. Ein
SS-CDC-artiger Kandidaten-Scanner ist deshalb für fastdup abzulehnen, bis eine
FastCDC-spezifische Variante das Skipping bewahrt und einen besseren
End-to-End-Wert zeigt.

### Pipeline und Hash-Overlap

P-Dedupe trennt Chunking, Fingerprinting, Index und Schreiben als Pipelinestufen.
Das Prinzip passt zur aktuellen Trennung von Lane, Hash-Batch, Exact-Proof,
Container-Codierung und Publication. Es verschiebt aber die eigentliche
Frage: Für einen SingleStream muss jede asynchrone Stufe genug Arbeit haben,
um ihre Queue-, Fence- und Cache-Kosten zu bezahlen.

Die lokalen Gegenproben liefern hier ein klares Stoppsignal. Look-ahead von
CDC und Hashing innerhalb eines 32-MiB-Containers stieg CPU-Zeit und
Hash-Phasen ohne messbaren Gewinn. Vollständiger Batch-Look-ahead verbesserte
SingleStream nur 1,021x, verringerte aber den 2-Stream-Durchsatz und den
langsamsten Stream auf 0,983x sowie erhöhte Peak-RSS auf 1,048x. Eine echte
asynchrone CDC-Hash-Stufe erreichte zunächst 1,097x SingleStream, fiel aber
bei zwei Streams auf 0,887x und produzierte einen SMB-Timeout. Diese Versuche
sind in [der vorausgehenden SingleStream-Notiz](single-stream-ingest-parallelism.md)
dokumentiert. Sie widerlegen nicht paralleles CDC allgemein. Sie zeigen, dass
mehr in-flight Arbeit in dieser Pipeline die Fairness- und Backpressure-Kosten
nicht bezahlt.

### Was "asynchron" auf der CPU tatsächlich bedeuten sollte

Die Async-API eines Chunkers und die Ausführung der CDC-Berechnung sind zwei
verschiedene Dinge. Die offizielle Tokio-Dokumentation beschreibt einen
gebundenen `mpsc`-Kanal als Backpressure-Grenze: Sobald die Kapazität voll ist,
wartet der Sender. Ein ungebundener Kanal kann dagegen beliebig anwachsen und
damit den Prozessspeicher erschöpfen. Für diese Pipeline muss die Grenze in
Bytes und nicht nur in Nachrichten gezählt werden, weil FastCDC-Chunks variabel
sind.

Der asynchrone Teil sollte deshalb nur FUSE-/Datei-I/O und die Übergabe an eine
CPU-Pipeline entkoppeln. Die CDC- und Hash-Arbeit gehört in einen einzigen,
explizit dimensionierten CPU-Pool. Tokios `spawn_blocking` ist dafür allein
keine ausreichende Begrenzung: Die Runtime kann bis zu ihrem hohen Blocking-
Limit weitere Threads erzeugen. Die Tokio-Dokumentation empfiehlt bei vielen
CPU-Aufgaben ein Semaphore oder einen spezialisierten CPU-Executor wie Rayon.

Rayon garantiert bei `spawn` keine Ausführungsreihenfolge. Ein Completion-
Callback darf daher nie selbst die nächste per-Inode-Reihenfolge fortschreiben.
Jeder Auftrag braucht eine monotone Sequenznummer, einen begrenzten
Reorder-Ring pro Lane und einen einzigen Commit-Head. Der Head gibt Ergebnisse
nur in Eingangsreihenfolge an Index und Publication weiter. Ein globaler
Worker- und Byte-Permit muss vor dem Kopieren oder Einreihen des Auftrags
genommen werden und bis zur Ausgabe oder zum deterministischen Abbruch gehalten
werden.

Die robuste Form ist damit:

1. I/O-Task liest in einen byte-begrenzten Eingangspuffer.
2. Ein Dispatcher vergibt `(inode, sequence, segment)` an den globalen
   CPU-Pool.
3. CPU-Worker berechnen CDC-Kandidaten oder Chunk-Hashes ohne Lane-Locks.
4. Ein per-Lane-Reorder-Head publiziert ausschließlich den nächsten erwarteten
   Sequenzwert.
5. Queue-Bytes, Reorder-Bytes und Worker-Leases bilden ein gemeinsames Limit.

Diese Struktur verhält sich bei einem Stream wie eine kleine Pipeline und bei
mehreren Streams wie ein fairer Scheduler. Sie vermeidet genau die
unkontrollierten Completion-Fences, die im lokalen Async-Challenger den
MultiStream-Timeout ausgelöst haben.

Für BLAKE3 gilt zusätzlich: der normale `update`-Pfad ist einthreadig. Die
offizielle Rust-Dokumentation empfiehlt `update_rayon` nur für große
zusammenhängende Puffer und nennt auf x86-64 weniger als 128 KiB als Bereich,
in dem er meist langsamer ist. Mit 64-KiB-Zielchunks sollte fastdup daher
weiter über viele Chunk-IDs parallelisieren, nicht die Identität eines
einzelnen Chunks künstlich bündeln oder jedes Chunk intern parallel hashen.

SeqCDC ist keine FastCDC-Optimierung, sondern eine neue hashlose CDC-Policy.
Der Preprint meldet hohe SIMD-Durchsätze, aber andere Grenzregeln und nur
ähnliche Space Savings. Für fastdup bedeutete das neue Chunk IDs, ein neues
Profil und eine Migration. Es ist kein vertretbarer Performance-Patch für den
aktuellen Engpass.

## Bewertung für fastdup

| Ansatz | Wirkung auf Grenzen und Dedupe | SingleStream-Aussicht | MultiStream-Risiko | Urteil |
| --- | --- | --- | --- | --- |
| `AsyncStreamCDC` einsetzen | keine bessere Parallelität, zusätzlicher Puffer | keine belegte | klein, aber unnötig | ablehnen |
| den FastCDC-Scanner pro Lane seriell lassen | bitidentisch | hält die bekannte Referenz | gering | beibehalten |
| CDC-Hash-Look-ahead | unverändert möglich | lokal nicht promotionsfähig | gemessen negativ, bis Timeout | ablehnen |
| segmentparalleles FastCDC | nur mit neuem Gleichheitsbeweis | offen | hoher Merge-, Speicher- und Tail-Risiko | Forschung, nicht Implementierung |
| Kandidatenstufe plus serieller Selektor nach SS-CDC | nur mit FastCDC-spezifischem Beweis | laut VectorCDC ohne FastCDC-Gewinn | steuerbar nur mit strikten Tokens | ablehnen |
| ganze Batch nach CDC parallel hashen und codieren | unverändert | bereits der sinnvolle Parallelismus | begrenzbar über `WorkerPermits` | beibehalten und profilieren |
| BLAKE3 pro Chunk `update_rayon` | unverändert | bei 64 KiB voraussichtlich schlechter | Pool-Konkurrenz | ablehnen |
| SeqCDC oder anderes hashloses SIMD-CDC | andere Grenzen, neues Profil | CDC-Mikrobenchmark kann besser sein | Dedupe- und Migrationsrisiko | nicht als Performance-Patch |

Die beste kurzfristige Optimierung ist deshalb keine neue Async- oder
MultiCore-CDC-Stufe. Der serielle Gear-Scan muss mit genauer Phasenmessung
gegen die beiden wirklichen CPU-Kandidaten geprüft werden: verbleibende
Kopien und BLAKE3 pro Chunk. Erst wenn der Scanner im SingleStream mehr als
eine relevante CPU-Phase belegt und die nachfolgende Hash-/Encode-Arbeit
beweisbar Arbeit wartet, lohnt sich der deutlich größere FastCDC-spezifische
Forschungsumbau.

## Backpressure und Fairnessregeln für jeden späteren Challenger

1. Ein Byte-Token bleibt von der FUSE-Annahme bis zur Publication oder
   deterministischen Rückgabe an die Lane belegt. Eine CDC-Vorstufe darf nicht
   vor der Publication neue Byte-Kredite schaffen.
2. Pro Inode bleibt höchstens eine laufende CDC-Reihenfolge aktiv. Eine
   Fortsetzung, ein Close und ein Commit-Cut müssen diese Reihenfolge ohne
   einen ungebundenen Future-Graph abwarten können.
3. Der globale `WorkerPermits`-Zähler bleibt die Obergrenze für Scanner,
   BLAKE3, Codierung und jede Kandidatenstufe zusammen. Verschachtelte Rayon-
   oder Zstd-Pools würden diese Rechnung umgehen.
4. Bei zwei oder mehr aktiven Lanes erhält kein Stream einen exklusiven
   Vorhaltebatch. Die Vergabe erfolgt FIFO oder als gewichtete Round-Robin-
   Folge pro Byte, nicht pro Chunk. Sonst erschleichen kleine Chunks mehr
   Starts und ein einzelner langer Stream kann die Latenz anderer Clients
   verschlechtern.
5. Telemetrie pro Lane muss Queue-Bytes, Token-Wartezeit, CDC-Bytes,
   Kandidaten- und Endgrenzen, Hash- und Encode-runnable-Zeit, Publication-
   Rückstau sowie Close/Commit-Cut-Wartezeit enthalten. Ohne diese Werte ist
   ein Durchsatzmittel nicht aussagekräftig.

Tokio kann diese Regeln transportieren, ersetzt sie aber nicht. Sein bounded
`mpsc` erzeugt Backpressure. Seine `Semaphore` ist FIFO, doch ein vorn
wartendes `acquire_many` kann kleine Anforderungen dahinter blockieren. Ein
async-Umbau darf deshalb keinen 32-MiB-Container als eine Permit-Anforderung
modellieren. Rayon liefert Work-Stealing, aber keine Budget- oder
Fairnessgarantie. `WorkerPermits` muss vor Rayon-Arbeit vergeben werden und
die Lease bis zur letzten CPU-Stufe halten.

## NUMA und Cache

Linux alloziert anonymen Speicher zunächst lokal zur CPU, die die Allokation
ausführt. Unter Last kann der Scheduler einen Task dennoch auf einen anderen
NUMA-Knoten verschieben. Linux dokumentiert daher CPU-Affinität, cpusets und
NUMA-Memory-Policy als Werkzeuge für Anwendungen, die diesen Effekt messen.
Die Kernel-Workqueue-Dokumentation beschreibt denselben Zielkonflikt: mehr
Cache- und NUMA-Lokalität kann weniger Gesamtauslastung bedeuten.

Für fastdup folgt daraus keine Rechtfertigung für permanentes Pinning. Die
bisherige Benchmarkmaschine hat zehn effektive CPUs, aber die vorliegenden
Ergebnisse enthalten keinen Nachweis mehrerer NUMA-Nodes oder vieler
LLC-Shards. Zuerst erfassen: `lscpu -e`, NUMA-Node je Daemon-Thread,
`perf c2c` oder LLC-Misses, CPU-Migrationen, remote-NUMA-Faults und
Speicherbandbreite. Erst bei einem reproduzierten Remote-Memory- oder
Migrationseffekt lohnt ein optionaler, administrativer cpuset-/Affinity-Lauf.
Ein fest eingebautes Thread-Pinning würde verfügbare Kerne in Container- und
VM-Deployments eher verschenken und kann MultiStream-Fairness verschlechtern.

## Offene Risiken und Promotion-Gate

Ein FastCDC-paralleler Kandidat braucht einen Golden-Test, der für jede
Eingabe und für alle Segmentgrößen/Workerzahlen exakt dieselben Grenzoffsets
wie FastCDC-v1 liefert. Er braucht außerdem Fault-Injection bei voller Queue,
Commit-Cut während Kandidatenarbeit, Lane-Reuse, Close und Abbruch. Das ist
nicht optional, weil der Fehler erst nach einem späteren Upload als verlorener
Exact-Hit sichtbar werden kann.

Promovieren nur nach mindestens fünf alternierenden A/B-Paaren pro 1-, 2- und
4-Stream-SMB-Lauf mit festgehaltenen Samba-, Mount-, ISO- und CPU-Topologie-
Daten. Erforderlich sind ein positiver, über Laufstreuung liegender
SingleStream-Gewinn, kein Rückgang beim 2-/4-Stream-Gesamtdurchsatz oder
langsamsten Stream, kein schlechteres p99 der abgeschlossenen Dateien, kein
Timeout, kein Swap und kein Anstieg über das RSS-Budget. Zusätzlich müssen
Chunk-Grenzen, Chunk IDs, Containerbytes und Restore-Bytes für 1, 2 und N
Worker identisch sein.

## Primärquellen

- [FastCDC, USENIX ATC 2016, vollständiges Paper](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
- [P-Dedupe, FAST 2012 Poster](https://static.usenix.org/events/fast12/poster_descriptions/Xiadescription.pdf)
- [MUCH, IEEE Transactions on Computers 2013, vollständiges Paper](https://oslab.kaist.ac.kr/wp-content/uploads/esos_files/publication/conferences/international/MUCH_Multithreaded_Content_Based_File_Chunking.pdf)
- [SS-CDC, Network and Internet Computing 2019, vollständiges Paper](https://ranger.uta.edu/~sjiang/pubs/papers/ni19-ss-cdc.pdf)
- [VectorCDC-Manuskript: SS-CDC brachte FastCDC keinen Gewinn](https://cs.uwaterloo.ca/~alkiswan/papers/VectorCDC_TOS_2026.pdf)
- [SeqCDC Preprint: neue hashlose, SIMD-fähige CDC-Policy](https://sreeharshau.github.io/papers/VectorizedSeq_TPDS26.pdf)
- [Shredder, IBM Research und FAST 2012](https://research.ibm.com/publications/shredder-gpu-accelerated-incremental-storage-and-computation)
- [fastcdc-rs: Streaming- und Async-API](https://github.com/nlfiedler/fastcdc-rs)
- [BLAKE3 `Hasher`: Threading-Schwelle und API-Semantik](https://docs.rs/blake3/latest/blake3/struct.Hasher.html)
- [Tokio `mpsc`: bounded channel und Backpressure](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html)
- [Tokio `Semaphore`: FIFO und `acquire_many`-Head-of-line-Risiko](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html)
- [Rayon `ThreadPool`: `install` und Work-Stealing](https://docs.rs/rayon/latest/rayon/struct.ThreadPool.html)
- [Linux: NUMA-Scheduling und lokale Allokation](https://www.kernel.org/doc/html/latest/mm/numa.html)
- [Linux: Cache-/NUMA-Lokalität gegen Auslastung](https://www.kernel.org/doc/html/latest/core-api/workqueue.html)

## Lokale Belege

- [FastCDC-Profil und Streaming-Ingest](../../crates/fastdup-appliance/src/checkpoint.rs)
- [bestehende SingleStream-Challenger und Messungen](single-stream-ingest-parallelism.md)
- [CPU-Skalierung des permanenten Reduction-Worker-Pools](../benchmarks/reduction-worker-pool.md)
