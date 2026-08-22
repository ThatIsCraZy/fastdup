# Single-stream ingest: Quellenprüfung zur Parallelisierung

Stand: 2026-08-22. Diese Notiz prüft nur die Ausführungsstrategie. Sie ändert
weder Chunk-Grenzen noch das dauerhafte Format oder die Commit-Semantik.

Die vertiefte Recherche zu asynchronem und multicore-fähigem CDC steht in
[async-multicore-cdc.md](async-multicore-cdc.md).

## Befund

Der aktuelle Write-through-Pfad sammelt stabile FastCDC-Chunks in einer
Containerfüllung, verteilt die Chunk-ID-Berechnung über Rayon und nutzt pro
Chunk `ChunkId::of`, also `blake3::hash`. Die Zielgröße beträgt 64 KiB, die
Grenzen liegen bei 16 bis 256 KiB. Die nachfolgende Container-Codierung arbeitet
auf Regionen bis 512 KiB. Der Code begrenzt die gemeinsamen Encode-Worker über
`WorkerPermits`, teilt dieses Budget aber erst auf aktive Ingest-Jobs auf. Die
`into_par_iter`-Aufrufe laufen dabei im Rayon-Global-Pool, dessen Standardgröße
Rayon aus `RAYON_NUM_THREADS` oder den logischen CPUs ableitet.

Das erklärt die Messung besser als ein einzelner BLAKE3-Engpass. BLAKE3
parallelisiert einen einzelnen Aufruf von `update_rayon` erst über einen großen
zusammenhängenden Eingabepuffer. Seine eigene Dokumentation nennt 128 KiB auf
x86-64 als grobe Grenze, unter der dies langsamer sein kann. `update` und damit
`blake3::hash` bleiben dagegen einthreadig. Das Repository nutzt die sichere
Form der äußeren Parallelisierung über viele CDC-Chunks, doch ein SingleStream
liefert dafür erst dann genug Arbeit, wenn eine hinreichend große stabile
Containerfüllung entstanden ist.

Die aus Messung und Code abgeleitete Schwachstelle ist somit die
Parallelitätsrampe: kleine oder noch nicht volle Containerfüllungen führen zu
wenigen Hash-Jobs, während der einzelne SMB-Writer auf deren Abschluss wartet.
Das ist kein Beleg dafür, dass größere CDC-Chunks oder BLAKE3-Innenparallelität
allein schneller wären.

## Empfohlener Challenger

Ein eng begrenzter Scheduler-Challenger ist sinnvoller als eine Änderung der
Dedup-Policy:

1. Behalte FastCDC-v1, 512-KiB-Kompressionsregionen, Containerformat und die
   globale Worker-Obergrenze unverändert.
2. Für genau einen aktiven Ingest-Stream verleihe die freien Worker schon beim
   Hashen einer stabilen Batch und lasse sie auch für die direkte anschließende
   Codierung reserviert. Die vorhandenen `WorkerPermits` sind der natürliche
   Ort für diese Regel. Führe die Arbeit in einem einmalig angelegten,
   budgetierten Rayon-Pool mit `install` aus, statt das Fairness-Budget nur vor
   Arbeit auf dem Global-Pool zu prüfen.
3. Sobald ein zweiter Stream aktiv ist, teile das Budget wieder nach der
   bestehenden Regel gleichmäßig. Es darf keine zusätzliche, streamlokale
   Rayon- oder Zstd-Threadgruppe entstehen.
4. Füge eine kurze, zeitlich begrenzte Batch-Coalescing-Schwelle hinzu, nur
   wenn die stabile Byte-Menge sonst weniger Jobs als freie Worker ergibt. Sie
   muss durch die bestehenden Byte-Budgets begrenzt bleiben und darf den
   Commit-Cut nicht verzögern.

Das verbessert die Chance, dass ein langer einzelner Stream die vorhandene
CPU-Arbeit sofort ausnutzt. Bei mehreren Streams bleibt die Zahl gleichzeitig
laufender Worker begrenzt und die Gleichbehandlung wird wiederhergestellt.
Die Regel gehört in Laufzeit-Telemetrie, nicht in die Policy-ID, weil sie weder
CDC-Schnitte noch Kodierungsentscheidungen verändert.

Der Versuch braucht eine Gegenprobe mit 1, 2 und N gleichzeitigen SMB-Uploads.
Promovieren sollte er nur, wenn SingleStream-Durchsatz und
Abschlusslatenz steigen, während MultiStream-Gesamtdurchsatz, der langsamste
Stream, p99 der abgeschlossenen Uploads, RSS und Swap mindestens die bisherige
Grenze halten. Zusätzlich messen: aktive Stream-Lanes, freie/verliehene Worker,
Hash-Batch-Bytes und -Chunks, Wartezeit auf Worker-Permits sowie Zeit von
FUSE-Annahme bis Container-Publikation.

## Benchmark-Nachprüfung

Am 2026-08-22 wurde der grobe work-conserving Challenger als wegwerfbare
Laufzeitoption gebaut. Hash- und Encode-Arbeit forderten damit immer das volle
globale Budget an; `WorkerPermits` begrenzte die tatsächlich vergebenen Worker
weiterhin auf zehn. Baseline und Challenger liefen mit derselben Binärdatei in
drei alternierenden A/B-Paaren durch den SMB-SingleStream-Benchmark.

| Paar | Baseline | Challenger | Challenger/Baseline |
|---:|---:|---:|---:|
| 1 | 374,92 MiB/s | 391,35 MiB/s | 1,044x |
| 2 | 501,28 MiB/s | 490,18 MiB/s | 0,978x |
| 3 | 502,89 MiB/s | 505,05 MiB/s | 1,004x |

Das geometrische Mittel beträgt 1,008x. Die 0,8 Prozent liegen deutlich unter
der Laufstreuung. Der Challenger verbrauchte im Mittel 25,54 statt 25,01
Daemon-CPU-Sekunden. Er liefert damit keinen messbaren SingleStream-Gewinn und
wird nicht weiter auf MultiStream promoviert. Der wegwerfbare Produktcode wurde
nach dem Versuch entfernt; die sechs JSON-Berichte bleiben unter
`.artifacts/benchmarks/smb-permit-{a1,b1,b2,a2,a3,b3}.json` erhalten.

Die ursprüngliche Scheduler-Hypothese ist damit zu grob. Ein neuer Versuch
sollte zuerst Permit-Wartezeit und CPU-runnable Arbeit messen. Ohne belegte
Permit-Wartezeit ist eine komplexere Fairnessregel nicht gerechtfertigt.

### Permit- und Runnable-Phasen-Telemetrie

Der finale Folgelauf steht in
`.artifacts/benchmarks/smb-permit-telemetry-final.json` (drei sequenzielle Kopien der
Rocky-ISO im Flat-Root-Profil). Er ergab:

- Hash: 382 Phasen, maximale Phasenkonkurrenz 1, keine blockierte Phase,
  0,119 ms kumulierte Permit-Akquise, 2,26 us maximale Wartezeit, 1,009 s
  kumulierte Runnable-Walltime und keine Teilzuteilung;
- Encode: 71 Phasen, maximale Phasenkonkurrenz 2, keine blockierte Phase,
  6,26 us kumulierte Permit-Akquise, 188 ns maximale Wartezeit, 2,335 s
  kumulierte Runnable-Walltime und keine Teilzuteilung.

Angeforderte und zugeteilte Worker waren in beiden Phasen identisch. Die
Permit-Akquise entspricht rund 0,012 Prozent der Hash-Runnable-Walltime und
0,0003 Prozent der Encode-Runnable-Walltime. Permit-Starvation ist unter
dieser Last damit nicht der aktuelle SingleStream-Flaschenhals. Der nächste
Optimierungsversuch sollte Arbeit innerhalb der CPU-Phasen oder Lücken zwischen
Pipeline-Phasen untersuchen und das globale Permit-Budget beibehalten.

### CDC-Hash-Look-ahead

Ein wegwerfbarer Challenger ließ den seriellen CDC-Scanner den nächsten Batch
schneiden, während der globale Rayon-Pool den aktuellen Batch hasht. Scanner
und Hash-Worker teilten weiterhin eine gezählte Permit-Lease; bei mehreren
aktiven Jobs fiel der Code auf den bestehenden Pfad zurück.

Mit 4-MiB-Batches ergaben drei alternierende A/B-Paare geometrisch 1,009x
Durchsatz, aber 1,038x Daemon-CPU und 1,557x Hash-Runnable-Walltime. Die Zahl
der Hash-Phasen stieg von rund 377 auf rund 1.102. Der zusätzliche Rayon- und
Batch-Overhead verbrauchte den möglichen Overlap-Gewinn.

Mit 16-MiB-Batches ergaben zwei Gegenpaare geometrisch 1,000x
Gesamtdurchsatz. Der erste, physisch neue Upload fiel auf 0,943x zurück; die
beiden Reuse-Uploads schwankten stark zwischen den Paaren. Daemon-CPU stieg auf
1,028x und Hash-Runnable-Walltime auf rund 1,154x. Damit war auch die größere
Variante nicht promotionsfähig. Eine MultiStream-Gegenprobe wurde nicht
gestartet, weil bereits das SingleStream-Kriterium verfehlt wurde.

Der Challenger wurde vollständig entfernt. Der Befund ist enger als die
ursprüngliche Hypothese: Look-ahead innerhalb desselben 32-MiB-Containers
zerlegt einen effizienten Hash-Batch und verschiebt nur Arbeit. Ein neuer
Versuch müsste vollständige Batches über Containergrenzen hinweg überlappen,
ohne die per-Inode-Reihenfolge oder das globale CPU- und Speicherbudget zu
öffnen.

### Full-Batch-Look-ahead über Containergrenzen

Der nächste Lauf behielt vollständige 32-MiB-Batches bei. Nach einem
unveränderten ersten Batch konnte eine Lane einen bereits geschnittenen Batch
vorhalten und dessen Hashing mit dem CDC-Schnitt des folgenden vollen Batches
per `rayon::join` überlappen. Der Versuch ist nur mit
`FASTDUP_EXPERIMENT_FULL_BATCH_CDC_HASH_PIPELINE=1` aktiv.

Die erste Fassung wartete bereits vor der ersten Publikation auf zwei Batches.
Damit verletzte sie 13 von 16 Write-through-Vertragstests. Eine zweite Fassung
ließ den ersten Batch unverändert passieren, verschob aber bei blockierter
Container-Publikation den Admission-Backpressure-Punkt um einen Batch; zwei
Tests schlugen weiter fehl. Die endgültige Versuchsfassung schaltet den
Look-ahead deshalb sofort ab, sobald die Publication Queue Bytes enthält. Dann
gilt wieder der bisherige 32-MiB-Pfad. Mit und ohne Laufzeitoption bestanden
alle 69 Appliance-Tests einschließlich Commit-Cut-, Fault-, Recovery- und
neun-Lane-Fällen.

Zwei gegenläufig angeordnete A/B-Paare mit dieser Backpressure-Sicherung
ergaben:

| Kennzahl | Challenger/Baseline, geometrisch |
|---|---:|
| Gesamtdurchsatz | 1,021x |
| erster, physisch neuer Upload | 1,006x |
| zweiter Reuse-Upload | 1,025x |
| dritter Reuse-Upload | 1,049x |
| Daemon-CPU | 1,009x |
| Peak-RSS | 1,074x |

Das Signal ist klein, aber in beiden Paaren positiv: 1,023x und 1,019x beim
Gesamtdurchsatz. Der Unique-Pfad ist mit 1,006x noch nicht von Laufstreuung zu
trennen; der Hauptgewinn liegt bei den Reuse-Uploads. Die Zahl der Hash-Phasen
sank von 375/384 auf 260/266, weil vollständige Batches erhalten blieben.
Die anschließende 2-Stream-Gegenprobe verfehlte jedoch das
Nichtverschlechterungskriterium. Zwei gegenläufige Paare ergaben geometrisch
0,983x für Gesamtdurchsatz und langsamsten Stream sowie 1,048x Peak-RSS. Das
zweite Paar war mit 1,002x neutral, das erste fiel auf 0,964x; ein belastbarer
MultiStream-Erhalt ist damit nicht belegt. Eine 4-Stream-Messung war nach dem
bereits negativen 2-Stream-Gate nicht mehr gerechtfertigt.

Der Full-Batch-Challenger wurde deshalb trotz des kleinen SingleStream-Signals
vollständig aus dem Produktcode entfernt. Die vier maßgeblichen
SingleStream-Berichte liegen unter
`.artifacts/benchmarks/smb-full-batch-guarded-{b3,a3,a4,b4}.json`, die
2-Stream-Berichte unter
`.artifacts/benchmarks/smb-2stream-full-batch-{a1,b1,b2,a2}.json`.

### Echte asynchrone CDC-Hash-Stufe

Ein weiterer wegwerfbarer Challenger löste das Hashing eines vollständigen
stabilen Batches als asynchrone Arbeit im bestehenden globalen Rayon-Pool aus.
Die Lane konnte danach CDC-Schnitte für den folgenden Batch bilden. Ein
deterministischer Integrationstest mit 200 ms künstlicher Hash-Verzögerung
belegte den beabsichtigten Overlap: Während der Hash-Batch noch lief, schnitt
dieselbe Lane bereits Chunks des Folgebatches. Commit-Cut und Close warteten
auf eine laufende Arbeit; ein prozessweites Byte- und Worker-Budget begrenzte
die zusätzliche Parallelität.

Die unbeschränkte SingleStream-Variante zeigte in zwei gegenläufigen A/B-Paaren
zunächst ein positives Signal: geometrisch 1,097x Gesamtdurchsatz, 1,107x beim
ersten physisch neuen Upload sowie 0,944x Daemon-CPU. Die Berichte liegen unter
`.artifacts/benchmarks/smb-async-hash-{a1,b1,b2,a2}.json`.

Das Signal hielt das MultiStream-Gate nicht. Die ersten zwei 2-Stream-Paare
ergaben geometrisch nur 0,887x für Gesamtdurchsatz und langsamsten Stream sowie
1,16x Daemon-CPU. In einem dritten Challenger-Lauf endete der zweite Upload mit
`NT_STATUS_IO_TIMEOUT`; auch der Daemon-Shutdown lief in das 120-Sekunden-Limit
und musste abgebrochen werden. Die Berichte stehen unter
`.artifacts/benchmarks/smb-2stream-async-hash-{a1,b1,b2,a2,a3,b3}.json`.

Zwei engere Gates konnten den Ansatz nicht retten. Mit Aktivierung erst nach
einem synchronen Batch und nur einer aktiven Inode-Queue war der Median des
ersten Uploads in drei A/B-Paaren 1,4 Prozent schlechter, obwohl die beiden
Reuse-Kopien zulegten. Eine letzte, nur nach vollständiger Reuse aktivierte
Fassung fiel in zwei Gegenpaaren klar zurück:

| Kennzahl | Challenger/Baseline, geometrisch |
|---|---:|
| Gesamtdurchsatz | 0,821x |
| erster, physisch neuer Upload | 0,807x |
| zweiter Reuse-Upload | 0,858x |
| dritter Reuse-Upload | 0,816x |
| Daemon-CPU | 1,312x |
| Peak-RSS | 1,095x |

Die engeren Berichte liegen unter
`.artifacts/benchmarks/smb-async-hash-gated-{a1,b1,b2,a2,a3,b3}.json` und
`.artifacts/benchmarks/smb-async-reuse-{a1,b1,b2,a2}.json`. Der Mechanismus
erzeugt also nachweislich Overlap, aber Callback-, Fence- und Lane-Scheduling
machen den End-to-End-Pfad instabil und teuer. Der MultiStream-Timeout ist
zudem ein hartes Ausschlusskriterium. Die komplette Async-Stufe, ihre
Laufzeitschalter, Versuchstelemetrie und der Verzögerungstest wurden wieder aus
dem Produktcode entfernt. Die allgemeine Permit- und CPU-Phasen-Telemetrie
bleibt erhalten.

## Was die Primärquellen ausschließen

`update_rayon` für jeden aktuellen Chunk ist kein guter erster Umbau. Die
BLAKE3-Dokumentation warnt gerade bei Eingaben unter ungefähr 128 KiB vor
Zusatzkosten. Das Bündeln mehrerer unabhängiger Chunk-IDs zu einem einzigen
Hash wäre außerdem fachlich falsch, weil dann nicht mehr jede Chunk-ID die
Identität genau ihres logischen Chunks wäre.

Zstd-interne Threads sind ebenfalls kein freier Gewinn. Die aktuelle
Implementierung verwendet worker-lokale Kontexte und schaltet Zstd-interne
Worker aus. Das folgt der Zstd-Empfehlung eines getrennten Kontexts pro
Parallel-Worker. Zstd-Mehrthreading beginnt bei mindestens 512 KiB Jobgröße,
verbraucht zusätzlichen Speicher und braucht bei mehreren gleichzeitigen
Kontexten einen ausdrücklich geteilten Zstd-Threadpool, um Ressourcen zu
begrenzen. Für die vorhandenen 512-KiB-Regionen würde das eine zweite,
konkurrierende Parallelitätsebene schaffen. Erst ein Vergleich mit einem
globalen Budget könnte das rechtfertigen.

FUSE bietet keine Gegenbegründung für die Scheduler-Regel, aber eine
Messpflicht. Der Linux-Kernel blockiert weitere Hintergrundanfragen bei
`max_background`; oberhalb von `congestion_threshold` drosselt er auch
asynchrone Readahead- und Writeback-Arbeit. Vor einer Änderung von FUSE- oder
Samba-Parametern müssen diese Werte und die Anzahl wartender FUSE-Anfragen
aufgezeichnet werden. Die bisherigen Daten zeigen keinen I/O-Engpass, also
gibt es noch keinen Anlass, diese Grenzen hochzusetzen.

## Quellen

- [BLAKE3 `Hasher`: `update` ist einthreadig, `update_rayon` und die 128-KiB-Heuristik](https://docs.rs/blake3/latest/blake3/struct.Hasher.html)
- [Rayon `ThreadPoolBuilder`: feste Obergrenze für Threadzahl und Standardwahl](https://docs.rs/rayon/latest/rayon/struct.ThreadPoolBuilder.html)
- [Rayon `ThreadPool`: lokaler Pool und `install`](https://docs.rs/rayon/latest/rayon/struct.ThreadPool.html)
- [Zstd API Manual: Kontext-Reuse, ein Kontext pro Parallel-Worker](https://facebook.github.io/zstd/zstd_manual.html#Streaming_compression)
- [Zstd API Manual: `nbWorkers`, Mindest-Jobgröße, Speicher- und Pool-Sharing](https://facebook.github.io/zstd/zstd_manual.html#Advanced_compression_API)
- [Linux-Kernel-Dokumentation zu FUSE `max_background` und `congestion_threshold`](https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse.html)

## Lokale Belege

- [Write-through-Chunking und Hash-Batches](../../crates/fastdup-appliance/src/checkpoint.rs)
- [Chunk-ID als BLAKE3-256](../../crates/fastdup-format/src/container.rs)
- [Worker-lokaler Zstd-Kontext, interne Zstd-Worker deaktiviert](../../crates/fastdup-store/src/reduction_codec.rs)
