# Direct I/O unter Linux: öffentliche Guidance und Benchmarks

Datum: 2026-09-01

## Kurzurteil

`O_DIRECT` ist keine allgemein schnellere Schreibmethode, sondern eine
Cache-Policy: Die Anwendung entscheidet, dass der Linux Page Cache für diesen
I/O-Strom keinen ausreichenden Nutzen hat. Direct I/O ist deshalb vor allem
dann sinnvoll, wenn die Anwendung Cache, Readahead, Batching und I/O-Scheduling
selbst kontrolliert oder große, kalte Datenströme den Page Cache nur verdrängen
würden.

Für fastdup bestätigt die öffentliche Evidenz die bereits gemessene selektive
Policy:

- große DATA-Publications ab dem gemessenen Crossover sind ein guter Kandidat;
- große kurzlebige External-Sort-/Merge-Spools sind der beste zusätzliche
  Metadata-Kandidat;
- kleine WAL-, Selector-, High-Water- und Manifest-Updates sollten buffered
  bleiben;
- immutable Metadata-Runs sind nur dann Kandidaten, wenn sie nicht unmittelbar
  auditiert, erneut gelesen oder per `mmap` konsumiert werden.

Es gibt keinen portablen universellen Größenschwellwert. Der Schwellwert muss
für den konkreten Publisher, das Produktions-XFS, Kernel, NVMe und die
Durability-Sequenz bestimmt werden.

## Wann Direct I/O sinnvoll ist

Die Quellen stimmen bei diesen Bedingungen weitgehend überein:

1. **Working Set deutlich größer als RAM.** Der Linux-NFSD-Guide nennt als
   besonders günstigen Fall große sequenzielle I/Os auf Dateien von zwei- bis
   dreifacher Server-RAM-Größe. Direct I/O vermeidet dann Page Allocation,
   Writeback und Reclaim; `kswapd` und `kcompactd` verbrauchen weniger CPU.
2. **Write-once/read-once oder lange Zeit kalte Daten.** Der Page Cache würde
   wertvolle heiße Seiten verdrängen, obwohl diese Daten keinen Cache-Hit
   erzeugen. Beispiele sind Backup-/Scrub-Ströme, Compaction und Scratch-Dateien.
3. **Große, ausgerichtete, gebündelte I/Os.** Die Anwendung kann Alignment,
   eigene Puffer, mehrere Requests in flight und Readahead garantieren.
4. **Die Anwendung besitzt bereits einen semantisch besseren Cache.** InnoDB,
   RocksDB, Ceph BlueStore und ScyllaDB sind typische Beispiele. Sie können
   Double Caching vermeiden und ihre knappe RAM-Kapazität selbst zuteilen.
5. **Temporäre Sort-Spools.** MySQL bietet hierfür eigens
   `innodb_disable_sort_file_cache`; Merge-Sort-Tempfiles werden dann äquivalent
   zu `O_DIRECT` geöffnet.
6. **Stabile Tail Latency unter Memory Pressure ist wichtiger als ein warmer
   Cache.** Der Vorteil kann in weniger Reclaim-Jitter und besserer
   Vorhersagbarkeit liegen, selbst wenn der mittlere Durchsatz kaum steigt.

Quellen: [Linux NFSD I/O modes](https://kernel.org/doc/html/next/filesystems/nfs/nfsd-io-modes.html),
[RocksDB Direct I/O](https://github.com/facebook/rocksdb/wiki/Direct-IO),
[MySQL 8.4 InnoDB parameters](https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html),
[Ceph hardware recommendations](https://docs.ceph.com/en/latest/start/hardware-recommendations/).

## Wann Direct I/O nicht sinnvoll ist

1. **Kleine, häufige oder unaligned Updates.** Alignment/Padding, zusätzliche
   Syscalls und Read-Modify-Write können den eigentlichen I/O dominieren.
2. **Unmittelbares Wiederlesen.** Ein direct publiziertes Ergebnis ist nicht im
   Page Cache; der erste Read wird zu echter Device-I/O. RocksDB weist bei
   direct geschriebenen Compaction-Ergebnissen ausdrücklich darauf hin.
3. **Hot Working Set oder hohe Cache-Hitrate.** Buffered I/O profitiert von
   Cache-Hits, Coalescing und Kernel-Readahead. Direct I/O muss diese Funktionen
   auf Anwendungsebene ersetzen.
4. **`mmap` auf derselben Datei.** Linux empfiehlt, `mmap` und Direct I/O sowie
   buffered und Direct I/O auf derselben Datei nicht zu mischen; die
   Kohärenzarbeit kann den Durchsatz reduzieren.
5. **WAL/Manifest/kleine Control-Dateien.** RocksDB wendet Direct I/O bewusst nur
   auf SST-Dateien, nicht auf WAL oder MANIFEST an. `O_DIRECT` ersetzt außerdem
   keine Persistenzbarriere.
6. **Netzwerk-Dateisysteme ohne End-to-End-Kontrolle.** Bei NFS wird eventuell
   nur der Client-Page-Cache umgangen; der Server darf weiter cachen. Kleine
   synchrone I/Os können dort besonders schlecht laufen.
7. **Keine eigene Readahead-/Cache-Strategie.** RocksDB verlangt für Direct
   Compaction-I/O große interne Puffer und Readahead. Ohne Ersatzfunktion ist
   der Page Cache häufig besser.

Quellen: [Linux `open(2)`](https://man7.org/linux/man-pages/man2/open.2.html),
[RocksDB I/O guide](https://github.com/facebook/rocksdb/wiki/IO),
[RocksDB Direct I/O](https://github.com/facebook/rocksdb/wiki/Direct-IO),
[FUSE I/O modes](https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse-io.html).

## Was öffentliche Benchmarks tatsächlich zeigen

Die Prozentwerte sind keine allgemeine Prognose. Sie zeigen, wie stark
Working-Set-Größe, Zugriffsmuster, eigener Cache, Readahead und Queue Depth das
Vorzeichen verändern können.

| Quelle und Workload | Direct-I/O-Effekt | Interpretation |
| --- | ---: | --- |
| RocksDB 6.10, XFS/NVMe, 8-KiB Bulkload | -0,3 % ops/s | Publication-Write praktisch neutral |
| RocksDB 6.10, Random overwrite | -0,1 % ops/s, p99 +11,3 % | kein Write-Gewinn, schlechtere Tail Latency |
| RocksDB 6.10, Read while writing | +10,5 % ops/s, p99 -5,8 % | eigener Cache plus gemischter I/O-Strom profitiert |
| RocksDB 6.10, Random read | +42,4 % ops/s, p99 -24,0 % | großer Datensatz und eigener Block Cache; nicht auf allgemeine File-I/O übertragbar |
| PostgreSQL-Hackers, 182 GiB auf 64 GiB RAM, Seqscan | etwa 6 % kürzer | großer kalter Scan profitiert leicht |
| derselbe PostgreSQL-Test, Indexscan | etwa 31 % langsamer | buffered gewann durch ungefähr 30 % Page-Cache-Hitrate |
| experimenteller PostgreSQL-io_uring-Scan, 21,6 GiB | etwa 47 % kürzer / 1,88x Durchsatz | großer gebündelter Streaming-Read; kein kleiner Metadata-I/O |
| MySQL-Entwicklermessung, 5,3 GiB Scan | etwa 2-3 % schneller | kleiner, vom Autor als nicht wesentlich bewerteter Unterschied |
| fastdup Publication, lokales XFS, 128 KiB | -16,3 % Durchsatz, p99 +7,9 % | unterhalb des Crossover klar schädlich |
| fastdup Publication, lokales XFS, 4 MiB | +6,7 % Durchsatz, p99 -20,3 %, System-CPU -16,7 % | erster klarer Gewinn |
| fastdup Publication, lokales XFS, 8 MiB | +2,7 % Durchsatz, p99 -1,2 %, System-CPU -21,1 % | kleiner Durchsatz-, deutlicher CPU-Gewinn |

Der offizielle RocksDB-Test lief auf einer AWS `m5d.2xlarge` mit 8 CPUs,
32 GiB RAM, 300-GB-NVMe, XFS, 900 Millionen Keys, 32 Threads und 6 GiB Block
Cache. DIO wurde mit derselben 8-KiB-Blockgröße wie die Vergleichszeile
ausgeführt. Die Teiltests liefen nacheinander auf derselben Datenbank; sie sind
daher wertvolle öffentliche Evidenz, aber kein isoliertes Microbenchmark.

Quellen: [RocksDB performance benchmarks](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks/ff898ce0d13c4d428f58a5962964b358af6e9a56),
[PostgreSQL-Hackers buffered/direct test](https://www.postgresql.org/message-id/a3ac3e07-0150-4319-a69b-aa367ddf67a5%40vondra.me),
[PostgreSQL AIO v2.0 benchmark](https://www.postgresql.org/message-id/cidihin6txgswozfgrcs5jkzsqmrbkebhauyjjwr6uhtzqti7w%40vqzav76usvmq),
[MySQL bug 112964 developer measurements](https://bugs.mysql.com/bug.php?id=112964),
[fastdup Publication A/B](../benchmarks/direct-io-publication-2026-09-01.md).

Eine weitere relevante Zahl ist kein Direct-vs-buffered-Vergleich, sondern
zeigt die notwendige Gegenleistung: Im Ceph-BlueStore-Test verbesserten 8 GiB
statt 4 GiB Anwendungscache Random-Read-IOPS um 14,43 % und p99 um 61,76 %;
beim 70/30-Mix stiegen IOPS um 30,52 %. Wer den Kernel-Cache umgeht, muss den
eigenen Cache korrekt dimensionieren. Quelle:
[Ceph BlueStore cache investigation](https://ceph.io/en/news/blog/2019/part-4-rhcs-3-2-bluestore-advanced-performance-investigation/).

## Welche Effekte zu erwarten sind

### RAM

- weniger unproduktiver Page Cache für große kalte Streams;
- weniger doppelte Kopien, wenn die Anwendung dieselben Daten selbst cached;
- berechenbarere RAM-Aufteilung;
- dafür eigener Speicher für aligned Buffers, Readahead und gegebenenfalls
  semantischen Cache.

### CPU

- unter Memory Pressure oft weniger System-CPU durch weniger Page Allocation,
  Dirty Writeback, Reclaim, `kswapd` und `kcompactd`;
- weniger Kopierarbeit kann bei großen I/Os helfen;
- bei kleinen I/Os können Submission-, Completion-, Padding- und
  Checksumming-Kosten dominieren;
- asynchrones Scheduling oder Coroutines können zusätzliche User-CPU kosten.

### Durchsatz und Latenz

- bei großen kalten Streams: häufig neutral bis moderat schneller, unter starkem
  Reclaim auch deutlich schneller;
- bei hot reads: potenziell massiv langsamer, weil Cache-Hits verloren gehen;
- bei kleinen Writes: häufig langsamer und schlechtere p99;
- Direct I/O kann Tail Latency stabilisieren, wenn buffered Writeback oder
  Reclaim bisher Bursts erzeugt hat.

## Implementierungsregeln unter Linux/XFS

1. Alignment nicht hart codieren. Seit Linux 6.1 liefern
   `statx(STATX_DIOALIGN)` und `stx_dio_mem_align`/
   `stx_dio_offset_align` die Anforderungen für XFS-Regular-Files.
2. Buffer-Adresse, Offset und Länge müssen den gemeldeten Anforderungen
   entsprechen. Misalignment kann `EINVAL` liefern oder still buffered werden.
3. Der Buffer muss bis zum asynchronen Completion Event leben und darf nicht
   verändert werden. Short/partial I/O und separate CQE-Fehler müssen behandelt
   werden.
4. Keine überlappenden buffered/direct/mmap-Zugriffe auf dieselbe Datei. Der
   iomap-Pfad muss sonst dirty Page Cache flushen und vor/nach Direct Writes
   invalidieren.
5. `io_uring` und Direct I/O sind orthogonal: io_uring unterstützt buffered und
   direct. Direct I/O braucht dennoch Batching/Queue Depth, damit schnelle NVMe
   ausgelastet werden.
6. Readahead und Write Coalescing müssen bei Direct I/O explizit in der
   Anwendung erfolgen. RocksDB verwendet unter anderem 2 MiB Compaction-
   Readahead und 1 MiB Direct-Write-Puffer.
7. `O_DIRECT` bedeutet nicht durable. `fsync`, `fdatasync`, Directory-`fsync`,
   Rename-Ordering und die fastdup-Publication-Invarianten bleiben bestehen.
8. Asynchrone Direct-I/O-Puffer aus privatem/Heap-Speicher dürfen laut
   `open(2)` nicht während `fork()` in flight sein; Requests vorher abschließen
   oder geeigneten Shared-/`MADV_DONTFORK`-Speicher verwenden.
9. Capability und Policy getrennt halten: Capability per `statx`; Policy per
   Dateiart, erwarteter Wiederverwendung und gemessenem Crossover.

Quellen: [Linux `statx(2)`](https://man7.org/linux/man-pages/man2/statx.2.html),
[Linux `open(2)`](https://man7.org/linux/man-pages/man2/open.2.html),
[Linux iomap operations](https://docs.kernel.org/filesystems/iomap/operations.html),
[fio documentation](https://fio.readthedocs.io/en/latest/fio_doc.html).

## Benchmark-Regeln für fastdup

Ein fairer A/B-Test muss nicht nur `direct=0/1` ändern, sondern die echten
Publication- und Read-after-publication-Sequenzen ausführen:

- identische Payloads, Dateigrößen und Sync-/Rename-Sequenzen;
- Größenmatrix unter und über dem erwarteten Crossover;
- warm, cold und Memory-Pressure als getrennte Szenarien;
- mindestens drei bis fünf alternierende Durchläufe je Modus;
- Durchsatz sowie p50/p99/max, User-/System-CPU und Peak RSS;
- Page-Cache-/VM-Metriken wie `pgscan`, `pgsteal`, Major Faults,
  `kswapd`/`kcompactd`-CPU;
- Device Utilization, erreichte Queue Depth und Write Amplification;
- unmittelbaren Audit/Read/`mmap` nach Publication separat messen;
- Crash-/Durability-Orakel unverändert ausführen.

Die fio-Dokumentation warnt ausdrücklich, dass eine nominelle `iodepth` bei
synchronen Engines wirkungslos ist und die tatsächlich erreichte Tiefe geprüft
werden muss. fio kann Direct I/O und Cache-Invalidierung kontrollieren, ersetzt
aber keinen End-to-End-fastdup-Benchmark.

## Konkrete Empfehlung für fastdup

### Beibehalten

- adaptive Direct-Publication für große DATA Container;
- 4 MiB als derzeit empirisch belegter Default-Crossover auf der lokalen
  XFS-Testumgebung;
- Samples nur dort direct schreiben, wo sie denselben großen, kalten
  Publication-Strom repräsentieren;
- Telemetrie für gewählten Modus, Fallback, Alignment und Größenklasse.

### Nächster Metadata-Versuch

External-Sort-/Merge-Spools sind der klarste Kandidat. Sie sind groß,
sequenziell, kurzlebig und sollen den Cache nicht langfristig belegen. Der Test
sollte buffered, `posix_fadvise(DONTNEED)` nach buffered I/O und Direct I/O
gegeneinander stellen.

### Vorläufig buffered lassen

- Commit-/Activation-WAL;
- Selector-, Slot- und High-Water-Records;
- Namespace- und Manifest-Control-Dateien;
- Exact-/Similarity-Runs, solange Publication direkt von Full Audit und
  `mmap`-Nutzung gefolgt wird.

Für immutable Runs wäre Direct I/O erst nach einer Änderung sinnvoll, die den
unmittelbaren zweiten Read vermeidet, etwa indem der Writer den finalen Inhalt
bereits während der Publication verifiziert und der Aktivierungspfad keinen
vollständigen Device-Reread verlangt. Auch dann muss `mmap` auf derselben Datei
erst nach Abschluss und Schließen des Direct-Publishers erfolgen und separat
gegen buffered Publication gemessen werden.

## Fazit

Öffentliche Implementierungs-Guides und Benchmarks stützen keine pauschale
Umstellung. Sie stützen einen selektiven, gemessenen Einsatz an tiefen
Publication-/Scratch-Seams. Der bereits gemessene fastdup-Crossover bei 4 MiB
passt zu den öffentlichen Mustern: kleine Writes verlieren, große kalte Streams
reduzieren vor allem System-CPU und Cache-Druck. Der nächste sinnvolle Versuch
ist daher Direct I/O für External-Sort-Spools, nicht für die kleinen
Durability- und Control-Metadaten.
