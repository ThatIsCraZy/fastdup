# SPDK-Insights für fastdup-Hotpaths

Status: Forschungsnotiz, keine Architekturentscheidung  
Stand: 2026-09-01  
SPDK-Revision: [`0578808fc00fda1f97953b814674ab26528f8148`](https://github.com/spdk/spdk/commit/0578808fc00fda1f97953b814674ab26528f8148)

## Fragestellung und Abgrenzung

Diese Notiz prüft, welche CPU-, SIMD-, Cache- und Polling-Muster des aktuellen
offiziellen SPDK-Quellstands auf fastdups DATA-Hotpaths übertragbar sind. Als
externe Quellen dienen ausschließlich die offizielle SPDK-Dokumentation und
der auf die obige Revision gepinnte SPDK-Source. Die fastdup-Codebasis und ihre
Benchmarks wurden nur zur Relevanzzuordnung gelesen.

SPDK ist eine gute Implementierungsreferenz, aber kein passender kompletter
I/O-Unterbau für fastdup: SPDK optimiert vor allem userspace-gesteuerte
NVMe-/Accelerator-Queues. fastdup braucht weiterhin XFS, FUSE, `io_uring`, die
HDD-orientierte DATA-Policy sowie seine bestehenden Publication-, Recovery-
und Scrub-Invarianten. Übertragbar sind daher primär die kleinen Hot-Path-
Muster, nicht der DPDK/SPDK-Stack als Ganzes.

## Kurzurteil

Der größte Teil der sinnvollen SPDK-Muster ist bereits vorhanden:

- SIMD bleibt hinter einer kleinen, sicheren Naht und einer skalaren
  Referenzimplementierung;
- BLAKE3, CRC32C, Zstd, LZ4, `copy_from_slice` und `fill` delegieren an bereits
  optimierte Bibliotheken beziehungsweise libc-/Compiler-Primitiven;
- Codec-Zustand ist worker-lokal, CPU-Arbeit gebündelt und die Publication hat
  genau einen CQE-getriebenen Ring-Owner;
- Exact-/Similarity-Daten sind dicht, cache-line-bewusst und generation-lokal;
- HDD-I/O bleibt geordnet, während unabhängige CPU-Arbeit parallel laufen darf.

SPDK legt aber zwei konkrete Lücken im Completion-Hotpath offen:

1. **Umsetzbar, P1:** `RingWorker::reap_completions` in
   `crates/fastdup-io-uring/src/lib.rs` sammelt bei jeder Runde in eine neue
   `Vec<(u64, i32)>`. Ein auf `ring_entries` vorallokierter und je Runde nur
   geleerter Scratch-Vektor beseitigt diese vermeidbare Heap-Allokation ohne
   Format-, I/O- oder Unsafe-Änderung.
2. **Umsetzbar als A/B, P2:** `submitted: HashMap<u64, Operation>` kann durch
   eine vorallokierte Slot-Tabelle mit Generation im `user_data` ersetzt
   werden. SPDK adressiert den Completion-Tracker direkt über die validierte
   Command-ID und kann den Tracker des nächsten gültigen CQE vorladen. Für
   fastdups kleine Ringtiefe kann Hashing bereits billig genug sein; deshalb
   erst `cycles/CQE`, Allocations, LLC-Misses und p99 messen.

Ein dritter Kandidat stammt aus fastdups eigenem Profil, wird von SPDK aber
nicht positiv belegt: die wiederholte FILL-Erkennung über 16--256-KiB-Chunks in
`crates/fastdup-appliance/src/checkpoint.rs`. Sie verdient einen
scalar/AVX2/optional-AVX-512-Differentialbenchmark; SPDKs eigene
`spdk_mem_all_zero`-Funktion ist nur eine byteweise Schleife und liefert daher
keinen Grund, ungeprüft einen esoterischen SIMD-Pfad zu übernehmen.

## Zuordnung zu allen relevanten Hotpaths

| fastdup-Stufe und konkrete Naht | SPDK-Insight | Urteil |
| --- | --- | --- |
| FUSE-Empfang, `MutationPayload`, Dirty Extents | SPDK setzt Zero-Copy und Ownership statt nachträglicher schneller Kopien an die erste Stelle. | **Bereits vorhanden.** Owned FUSE-Bytes und Slices nicht wieder in einen DSA-/SIMD-Copy-Pfad materialisieren. |
| SeqCDC, `crates/fastdup-store/src/seqcdc.rs` | Kleine ISA-spezifische Kernel hinter einer stabilen API; kein SPDK-CDC-Gegenstück. | **Bereits vorhanden.** AVX2/BMI2 plus skalares Differentialorakel beibehalten. Kein AVX-512-Baselinewechsel. |
| FILL-Erkennung, `ChunkFragments::is_fill`, `classify_stable_chunk_shard`, `rechunk_range_into_writer` in `crates/fastdup-appliance/src/checkpoint.rs` | SPDK bietet für All-zero nur eine skalare Early-exit-Schleife; DSA kann Memory füllen oder zwei Puffer vergleichen, aber keinen Puffer ohne Vergleichsbild auf Gleichwertigkeit zu einem Byte prüfen. | **Umsetzbar nur als A/B.** Ein AVX2-Repeated-byte-Scan ist plausibel, aber noch unbelegt. Skalares Orakel und Corpus-Mix aus FILL, frühem/spätem Mismatch und Zufallsdaten verlangen. |
| Chunk ID, Container-/Index-Hashes | SPDKs Accel-Opcode-Katalog enthält CRC, aber keinen kryptographischen Content Hash. | **Bereits vorhanden / nicht passend.** BLAKE3 nicht durch CRC32C ersetzen und keine lokalen Intrinsics um BLAKE3 bauen. |
| CRC32C in `fastdup-format` und Recovery Checkpoints | SPDK priorisiert ISA-L, dann x86-SSE4.2, dann Tabelle; fragmentierte IOVs werden inkrementell verarbeitet. DSA bietet CRC und Copy+CRC als eigene Operationen. | **Bereits vorhanden** für SSE4.2-Runtime-Dispatch und inkrementelle CRCs. **P2-Hardwareexperiment** für große RAW-/Record-Assembly nur auf qualifizierten DSA-Hosts; Writer, Reader und Scrub müssen exakt dasselbe CRC-Wireformat behalten. |
| Record-Assembly, `encode_prehashed_{raw,zstd}_record_into` in `crates/fastdup-format/src/container.rs` | SPDKs Software-Backend ruft für Copy/Compare/Fill `memcpy`/`memcmp`/`memset` auf; selbst Software-`copy_crc32c` macht Copy und CRC in zwei Durchläufen. Nur das DSA-Backend hat eine kombinierte Hardwareoperation. | **Beibehalten.** Safe Slice-Primitiven nicht durch handgeschriebene Pointer-/SIMD-Loops ersetzen. DSA nur gegen echte Record-Größen und inklusive Submission-/Completion-Kosten testen. |
| Zstd/LZ4 Encode/Decode, Reduction Worker | SPDK legt ISA-L-/LZ4-Kontexte in per-channel State ab und setzt sie pro Auftrag zurück. | **Bereits vorhanden.** fastdups worker-lokale Zstd-Kontexte und bounded Scratch folgen demselben Muster. |
| IAA-Kompression | Das aktuelle SPDK-IAA-Modul akzeptiert nur Deflate und aktuell nur einen IOV je Seite. | **Nicht passend.** Deflate ist weder RAW/Zstd-v1 noch ZSTD_PREFIX und darf das durable Format nicht implizit ändern. |
| Exact Index mmap/lookup, Similarity lookup | SPDK legt Hot-Felder vorne und Cold-Felder außerhalb der normalen I/O-Cacheline ab; Pointer-Folgeziel des nächsten bereits validierten CQE wird vorab geladen. | **Weitgehend vorhanden.** Dense/mapped Pages, Page Bounds, blocked Bloom und geordnete Batch-Lookups zuerst nutzen. Manuelles Prefetch für Exact-/Similarity-Pages nur bei nachgewiesenen LLC-Stalls; Binärsuche und page-lokale Sortierung erschweren einen stabilen Vorlaufabstand. |
| Verified Read Plan und Manifest-Assembly | SPDKs Softwarepfad nutzt libc Copy/Fill; die größere Optimierung ist Batching, Ownership und lokaler Zustand. | **Bereits vorhanden.** Ein finaler Reply-Copy bleibt notwendig; kein DSA für kleine/fragmentierte Manifest-Slices. |
| `RingWorker::reap_completions`, `crates/fastdup-io-uring/src/lib.rs` | SPDK hält Tracker/Tasks pro Channel vorallokiert, verarbeitet CQEs in bounded Batches und nutzt eine direkte Tracker-Tabelle. | **Umsetzbar P1/P2.** Reusable CQE-Scratch zuerst; danach Slot-Tabelle getrennt benchmarken. |
| `RingWorker::submit_ready` und Root-Cohort | SPDK bündelt Queue-Arbeit und klingelt Doorbells erst nach einem Batch; Queue/Channel hat genau einen Owner. | **Bereits vorhanden.** fastdup füllt die SQ bis zur Ringgrenze, wartet CQE-getrieben und besitzt Ring/FDs auf einem Thread. |
| Idle-Verhalten | SPDK dokumentiert Busy Polling als niedrigste Latenz bei 100 % CPU und bietet Interrupt-/FD-Wakeup als Alternative. | **Bereits passend gelöst.** `submit_and_wait(1)` plus `eventfd` behalten. Dauer-Busy-Polling passt nicht zum HDD-Appliance- und CPU-Budget. |
| Scrub, Rebuild, GC | SPDKs Shared-nothing-Idee hält Queue- und Codec-Zustand lokal und übergibt kleine Nachrichten. | **Bereits vorhanden.** Geordnete HDD-Reads plus bounded parallele Verifikation sind wichtiger als mehr Poller oder ungeordnete Chunk-I/Os. |
| ISA-Dispatch und Release-Build | SPDK weist Operationen einmal beim Start einem Software-/Hardwaremodul zu, baut standardmäßig aber mit `-march=native`; im eigenen Build schaltet es AVX-512 teilweise explizit aus. | **Teilweise übernehmen.** Funktionsauswahl darf kalt und zentral sein; `target-cpu=native` oder AVX2/AVX-512 als shipped Baseline widerspricht fastdups x86-64-Baseline und ADR 0078. |

## Primärquellen und Folgerungen

### 1. Software zuerst, Hardware pro Operation und mit Fallback

SPDK abstrahiert Copy, Fill, Compare, CRC32C, Copy+CRC, Compression und weitere
Operationen hinter einem Acceleration Framework. Ohne Hardwaremodul bleibt für
jede Operation ein Softwarebackend; die Zuordnung erfolgt pro Opcode und kann
explizit überschrieben werden. Hardware ist damit Capability und Policy, nicht
Teil der Datenbedeutung. Quellen:
[Acceleration Framework, Design](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/accel_fw.md#L17-L32),
[Module-to-Opcode assignment](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/accel_fw.md#L174-L206),
[`accel_submit_task`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel.c#L345-L358).

Für fastdup folgt daraus keine neue generische Accelerator-Abstraktion. Die
bereits kleinen Nähte für SeqCDC und Similarity reichen. Wenn DSA später
qualifiziert wird, sollte nur ein konkreter großer Operationstyp hinter einer
internen Capability-Naht landen. Ein fehlender Channel, Queue-Sättigung oder
nicht vorhandene Hardware muss auf den byte-identischen Softwarepfad
zurückfallen. SPDK warnt ausdrücklich vor begrenzter DSA-Queue-Tiefe und
Channelzahl sowie zusätzlichen Kernel-/IOMMU-/Capability-Anforderungen.
Quelle:
[DSA limits and setup](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/accel_fw.md#L75-L140).

### 2. CRC32C: optimierte Bibliothek ja, eigener Intrinsics-Pfad nein

SPDK ruft mit ISA-L `crc32_iscsi` auf, verwendet sonst bei entsprechendem
Build SSE4.2-CRC über ausgerichtete 64-Bit-Wörter und fällt auf eine Tabelle
zurück. IOVs werden ohne Zusammenkopieren nacheinander in denselben CRC-Zustand
eingespeist. Quelle:
[`lib/util/crc32c.c`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/util/crc32c.c#L10-L132).

fastdup `crc32c = 0.6` besitzt bereits SSE4.2-Runtime-Dispatch; die Durable-
Encoder in `crates/fastdup-format/src/container.rs` und die Checkpoint-Pfade
müssen deshalb nicht mit lokalen Intrinsics dupliziert werden. Relevanter ist
die Zahl der vollständigen Speicherläufe. SPDKs Software-`COPY_CRC32C` kopiert
zuerst und berechnet danach den CRC über die Quelle; nur DSA reicht die
kombinierte Operation an Hardware weiter. Quellen:
[Software Copy+CRC](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel_sw.c#L720-L747),
[DSA Copy/CRC/Copy+CRC](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/module/accel/dsa/accel_dsa.c#L293-L319).

Damit ist ein DSA-Test nur dann sinnvoll, wenn er tatsächlich einen bestehenden
großen Copy plus CRC-Lauf ersetzt. Ein bloßer `copy_crc32c`-Wrapper um das
Softwarebackend spart nichts. Die Matrix muss 16, 64 und 256 KiB sowie reale
Record-/Containergrößen, Queue Depth, fragmented/contiguous Input,
cycles/byte, Wall-/CPU-Zeit und p99 enthalten. SPDKs eigenes `accel_perf`
variiert genau Transfergröße, Queue Depth, IOV-Anzahl und Backend und kann das
Resultat optional gegen den Softwarepfad verifizieren. Quellen:
[`accel_perf` options](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/examples/accel/perf/accel_perf.c#L178-L199),
[`accel_perf` verification](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/examples/accel/perf/accel_perf.c#L868-L909).

### 3. Copy, Compare, Fill und Scan

SPDK schreibt im optimierten Softwarebackend keine eigenen SIMD-Schleifen für
Copy, Compare oder Fill. Segmentierte IOVs werden in contiguous Teilstücke
zerlegt und an `memcpy`, `memcmp` beziehungsweise `memset` gegeben. Quelle:
[`lib/accel/accel_sw.c`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel_sw.c#L121-L196).

Das bestätigt fastdups bisherige Entscheidung für `copy_from_slice`,
`extend_from_slice` und `fill`. Manuelle AVX-Copy-Loops würden libc/
Compiler-Dispatch duplizieren, Alignment-/Tail-Code hinzufügen und bräuchten
nach den Repository-Regeln einen messbaren A/B-Vorteil, bevor `unsafe`
zulässig wäre.

Anders ist die FILL-Erkennung: Sie ist kein `memcmp`, weil kein zweiter
vollständiger Vergleichspuffer existiert. SPDKs öffentliche
`spdk_mem_all_zero`-Implementierung prüft ebenfalls byteweise mit Early Exit.
Quelle:
[`spdk_mem_all_zero`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/util/string.c#L381-L391).
Das ist keine Evidenz gegen SIMD, aber auch keine positive SPDK-Evidenz dafür.
Ein fastdup-A/B muss deshalb den vollständigen `hash_and_fill`-Abschnitt
messen, nicht nur einen künstlichen All-zero-Puffer. AVX-512 bleibt optional:
SPDK deaktiviert es in Teilen des eigenen x86-Builds, weil Compiler es sonst
sogar für unerwartet einfache Operationen erzeugen kann. Quelle:
[SPDK x86 compile flags](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/mk/spdk.common.mk#L78-L109).

### 4. Compression: lokaler Kontext ist übertragbar, IAA-Deflate nicht

SPDK hält ISA-L-Deflate/Inflate und LZ4 Stream-State pro I/O-Channel und
initialisiert ihn einmal; pro Auftrag wird nur die Session zurückgesetzt.
Quellen:
[per-channel codec state](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel_sw.c#L51-L68),
[channel initialization](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel_sw.c#L796-L848).
fastdups permanente Rayon-Worker, worker-lokale Zstd-Kontexte und wiederverwendete
Scratch-Puffer entsprechen diesem Muster bereits.

IAA ist keine Abkürzung für den aktuellen fastdup-Codec. Das aktuelle Modul
unterstützt nur Compress/Decompress, lehnt mehr als einen IOV ab und meldet als
Algorithmus ausschließlich Deflate. Quelle:
[`module/accel/iaa/accel_iaa.c`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/module/accel/iaa/accel_iaa.c#L122-L153),
[IAA algorithm support](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/module/accel/iaa/accel_iaa.c#L246-L280).
Ein Wechsel zu Deflate wäre eine neue Encoding Policy und ein neues Durable-
Format, keine transparente SIMD-Optimierung. Er gehört nicht in diesen
Hot-Path-Slice.

### 5. Completion-Pfad: vorallokieren, direkt adressieren, gezielt prefetchten

SPDKs Accel-Channel reserviert Task- und Sequence-Pools cache-line-ausgerichtet
beim Erzeugen des Channels. Der Hotpath nimmt und gibt Einträge aus lokalen
Freelists zurück. Quelle:
[`accel_create_channel`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/accel/accel.c#L3049-L3119).

Im NVMe-CQE-Loop ist die Command-ID der Index in ein vorallokiertes Tracker-
Array. Erst wenn der nächste CQE als gültig erkannt wurde, wird dessen Tracker
prefetched; danach verarbeitet die Schleife höchstens ein begrenztes Batch und
klingelt die Completion Doorbell einmal. Quelle:
[`nvme_pcie_qpair_process_completions`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/nvme/nvme_pcie_common.c#L918-L998).

fastdup hat `ready` und `submitted` bereits mit Ringkapazität vorallokiert,
erzeugt aber in `reap_completions` pro Runde eine neue temporäre `Vec`. Der
erste Umbau ist deshalb bewusst klein:

1. `completion_scratch: Vec<(u64, i32)>` in `RingWorker` ergänzen;
2. einmal mit `ring_entries` reservieren;
3. vor jedem Reap `clear`, unter dem CQ-Borrow per `extend` befüllen, Borrow
   beenden, dann die Einträge verarbeiten;
4. einen Benchmarkzähler für Scratch-Capacity-Growth oder Allocations ergänzen.

Die Slot-Tabelle ist ein eigener zweiter Versuch. `user_data` muss Slot plus
Generation codieren, damit ein verspätetes oder doppelt beobachtetes CQE nie
eine neue Operation desselben Slots abschließen kann. Erst wenn dieser Versuch
gegen die heutige `HashMap` gewinnt, ist ein Prefetch des nächsten validierten
Slots sinnvoll. Eine nackte Pointer-Prefetch-Intrinsic vor diesem Strukturumbau
hat keinen stabilen Zieladress- oder Abstandsvorteil.

SPDKs Layoutregel ist ebenfalls bereits weitgehend umgesetzt: häufig berührte
Queue-Felder stehen zusammen, Cold-Felder explizit außerhalb des normalen
I/O-Teils. Quelle:
[`nvme_pcie_qpair` layout](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/nvme/nvme_pcie_internal.h#L125-L203).
Zusätzlich sollen nur tatsächlich cross-core veränderte Objekte isoliert
werden; SPDK richtet beispielsweise ein Threadobjekt aus genau diesem Grund an
einer eigenen Cacheline aus. Quelle:
[`spdk_thread_create`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/lib/thread/thread.c#L527-L540).

### 6. Polling, Batching und Shared-nothing

SPDK erklärt den Kern seiner Skalierung als per-thread/per-channel State,
Single Ownership und kleine Nachrichten statt gemeinsam beschriebener Locks
und Atomics. Quellen:
[Message Passing and caching](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/concurrency.md#L1-L57),
[I/O channels](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/concurrency.md#L89-L105),
[single-owner hardware queues](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/userspace.md#L72-L97).

Das stützt fastdups einen `io_uring`-Owner, worker-lokale Codec-Zustände,
shard-lokale Cache-Telemetrie und ownership-basierten Buffer-Transfer. Es
stützt nicht, per-Inode-POSIX-Locks zu entfernen: Diese Locks repräsentieren
semantische Mutation Order, nicht zufällige I/O-Queue-Kontention.

SPDK dokumentiert zugleich die Kosten des Polling-Modells offen: Tight Polling
liefert niedrigste Latenz, verbraucht aber 100 % eines Cores; Interrupt Mode
blockiert auf FD-Ereignissen. Quelle:
[SPDK interrupt-mode overview](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/doc/interrupt_mode.md#L1-L9).
fastdups `eventfd` plus `io_uring::submit_and_wait(1)` ist für eine Appliance
mit HDD DATA Tier und gemeinsamem CPU-Budget die passendere Hälfte dieses
Trade-offs. Ein Busy-Poll-Core wäre erst bei einem NVMe-only-Profil und einem
End-to-End-p99-Gewinn vertretbar.

### 7. ISA-Portabilität und Dispatch

SPDKs Default-Konfiguration setzt `CONFIG_ARCH=native`, und das Buildsystem
gibt diese Wahl als `-march` an den Compiler weiter. Quellen:
[`CONFIG`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/CONFIG#L8-L15),
[`mk/spdk.common.mk`](https://github.com/spdk/spdk/blob/0578808fc00fda1f97953b814674ab26528f8148/mk/spdk.common.mk#L55-L76).
Das ist für eine auf genau einem qualifizierten Host gebaute SPDK-Appliance
vertretbar, aber nicht unverändert auf fastdups Release-Artefakt übertragbar.

fastdup unterstützt zwar nur x86-64, doch ADR 0078 hält den Baseline-ISA-Satz
bewusst bei x86-64. AVX2, BMI2, AVX-512 und spätere Erweiterungen dürfen nur
nach Runtime-Detection laufen; durable Entscheidungen behalten das skalare
Orakel. Deshalb:

- kein globales `-C target-cpu=native` für ausgelieferte Binaries;
- kein `+avx2`, `+bmi2` oder `+avx512*` als unbeabsichtigte Mindestanforderung;
- optionale qualifizierte Native-Builds nur als getrenntes Deploymentprofil;
- ISA-Kernel klein halten und Goldens/Differentialtests vor Microbenchmarks
  ausführen;
- eine einmalige kalte Kernel-Auswahl nur übernehmen, wenn sie gegenüber der
  heutigen Runtime-Detection messbar gewinnt.

## Priorisierte nächste Schritte

### P1 — ohne neue ISA und ohne `unsafe`

1. Reusable CQE-Scratch in `fastdup-io-uring::RingWorker` prototypisieren.
2. Den vorhandenen Publisher-Benchmark um Allocations/CQE, cycles/CQE,
   completed CQEs je Reap sowie p50/p99/max erweitern.
3. Nur bei messbarem Gewinn integrieren; Publication-Fault- und CQE-
   Ownership-Tests unverändert ausführen.

### P1 — SIMD-Differentialbenchmark

1. Den wiederholten Byte-Scan hinter einer sicheren internen Funktion bündeln.
2. Skalar, AVX2 und optional AVX-512 für 16/64/256 KiB vergleichen.
3. Fälle: kompletter FILL, Mismatch bei Byte 1/32/4 KiB/Ende, Zufallsdaten,
   reale Rocky- und strukturierte Chunks.
4. Nicht nur ns/byte, sondern den vollständigen `hash_and_fill`-Abschnitt,
   Branch Misses, Instructions und SingleStream-Ergebnis messen.
5. `unsafe` nur bei messbarem Vorteil und mit skalarem Byte-identischem Orakel
   übernehmen.

### P2 — qualifikationsabhängige Experimente

1. Preallocated generation-tagged Completion-Slots gegen `HashMap` A/B testen;
   erst danach optional nächsten gültigen Slot prefetchten.
2. Auf einem tatsächlich vorhandenen DSA-Host CRC32C und Copy+CRC32C gegen den
   aktuellen Softwarepfad testen. Softwarefallback, Queue-Sättigung, fehlende
   Channels, NUMA und Capability-Fehler gehören in dieselbe Matrix.
3. Ein Experiment gilt nur bei unverändertem Wireformat, null Process Swap,
   unveränderten Fault-/Recovery-/Scrub-Orakeln und einem End-to-End-Gewinn.

## Explizite Nicht-Maßnahmen

- SPDK/DPDK nicht als Ersatz für XFS/FUSE/`io_uring` integrieren.
- BLAKE3 nicht durch CRC32C, xxHash oder einen Accelerator-CRC ersetzen.
- Keine manuellen SIMD-Copy/Compare/Zero-Loops um sichere Slice-/libc-
  Primitiven bauen.
- IAA-Deflate nicht als transparenten Ersatz für Zstd-v1 oder ZSTD_PREFIX
  behandeln.
- Keine Busy-Poll-Reaktoren für den heutigen HDD-DATA-Pfad reservieren.
- Keine ungemessenen Prefetch-Intrinsics in Exact-/Similarity-Binärsuchen
  einfügen.
- Weder AVX2 noch AVX-512 zur globalen Build-Baseline machen.
- DSA/IOAT nicht als Pflicht-Hardware definieren; begrenzte Channels und
  Plattformkonfiguration sind Teil der Betriebsrealität, nicht ein seltener
  Fehlerfall.

## Abnahmegrößen

Jeder Kandidat muss mindestens gegen den heutigen Release-Pfad messen:

- cycles, instructions, Branch Misses, L1D- und LLC-Misses pro logischem MiB;
- Allocations und allokierte Bytes pro CQE, Chunk und Record;
- Byte-Durchsatz sowie p50/p99/max der betroffenen Operation;
- erreichte Queue Depth, CQEs pro Reap und Submission-/Completion-CPU;
- SingleStream-SMB-Durchsatz und completed-write p99/max;
- Process Swap, MemoryBudget-Reserve und physische DATA-I/O-Ordnung;
- byte-identische Chunk IDs, CRCs, Containerbilder und Similarity-Fingerprints;
- unveränderte Writer-, Recovery-, Demand-Read- und Offline-Scrub-Matrizen.

Ein isolierter Microbenchmark darf einen End-to-End-Rückschritt nicht
überstimmen. Insbesondere müssen DSA-/Prefetch-/AVX-512-Ergebnisse auf dem
qualifizierten Produktions-CPU-/NUMA-/Kernel-Profil reproduzierbar sein.
