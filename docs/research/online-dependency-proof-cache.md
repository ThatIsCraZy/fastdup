# Online-Dependency-Proof-Cache: Algorithmen und Datenlayout

Stand: 2026-08-21. Diese Notiz ist Entscheidungsgrundlage, kein ADR und keine
Freigabe eines Produktionsformats. Der untersuchte Zustand ist die lokale
Implementierung in `checkpoint.rs` mit 65.536 Einträgen, einer globalen
`Mutex`, einer `BTreeMap` für Proofs und einem `BTreeSet` für die
Ersetzungsreihenfolge.

## Ergebnis

fastdup sollte nicht einen größeren monolithischen Cache bauen, sondern zwei
semantisch verschiedene Klassen trennen:

1. **Generation Proof Set:** Proofs, die eine aktive oder bereits eingefrorene
   Commit-Generation benötigt, sind gepinnt und werden nicht durch eine
   Cache-Policy verdrängt. Die 65.536 Einträge sind hierfür die richtige
   V1-Obergrenze: zwei 512-MiB-Generationen geteilt durch FastCDC-v1s
   16-KiB-Minimum. Der Speicher gehört zum Ingest-/Commit-Budget, nicht zum
   opportunistischen Cache-Budget.
2. **Historical Proof Cache:** Frühere verifizierte Locations sind reine
   Beschleunigung. Dieser Cache darf unter Speicherdruck vollständig
   verschwinden, nach Neustart leer sein und seine Kapazität aus verfügbarem
   RAM ableiten.

Für den Historical Cache ist **S3-FIFO auf geshardeten Ringpuffern** gemäß
[ADR 0051](../adr/0051-use-s3-fifo-for-the-historical-proof-cache.md) gewählt.
**SIEVE** bleibt der mitzumessende einfachere Challenger. CLOCK-Pro, ARC und
W-TinyLFU sind für den ersten Umbau zu komplex;
striktes LRU, SLRU und 2Q verursachen unnötige Schreibzugriffe und
Synchronisation auf Hits.

Der Index sollte von der Eviction-Policy getrennt werden. Ein guter
Rust-Ausgangspunkt ist pro Shard ein `hashbrown::HashTable<u32>` mit bereits
vorhandenem 64-Bit-Hash. Die Tabelle enthält nur stabile Slot-Indizes; ein
vorreserviertes Slot-Array besitzt jeden vollständigen `ExactIndexEntry`
genau einmal. Die offizielle
[`HashTable`-Dokumentation](https://docs.rs/hashbrown/latest/hashbrown/struct.HashTable.html)
beschreibt genau diesen Anwendungsfall: Indizes in einen `Vec`, deren Hash und
Gleichheit aus dem referenzierten Wert kommen. `hashbrown` ist ein
SwissTable-Port unter MIT oder Apache-2.0 und
damit passend zur Apache-2.0-Lizenz von fastdup
([Projekt und Lizenz](https://github.com/rust-lang/hashbrown)).

## Lokaler Ausgangspunkt

Die aktuellen Typgrößen auf x86-64 sind:

| Bestandteil | Bytes |
|---|---:|
| `ChunkId` | 32 |
| `ExactIndexLocation` | 96 |
| `ExactIndexEntry` | 136 |
| Map-Key `(ChunkId, u32)` | 36 |
| Map-Value `(ExactIndexEntry, u64)` | 144 |
| Recency-Key `(u64, ChunkId, u32)` | 48 |

Damit werden pro Proof bereits 228 Byte logischer Inhalt in zwei Bäumen
abgelegt. Drei reproduzierte RSS-Messungen mit 65.536 synthetischen Einträgen
ergaben jeweils 27.152 KiB zusätzlich, also 424 Byte pro Eintrag. Die Messhelfer
liegen nur unter `.artifacts/tmp/`.

Der aktuelle Code hat trotz der ADR-Beschreibung keine in sich geschlossene
LRU-Semantik: `remember` erzeugt beziehungsweise erneuert einen Touch-Wert,
aber `verified_entry`, `verified_entries` und `unproven` verändern die
Reihenfolge nicht. Mehrere Ingest-Aufrufer führen später noch `remember` aus,
der Commit-Proof-Pfad dagegen nicht. Die Verdrängung folgt damit dem letzten
expliziten `remember`, nicht zuverlässig jedem Cache-Hit. Diese vom Aufrufer
abhängige Policy sollte nicht durch das bloße Vergrößern beider B-Trees
zementiert werden.

## Admission und Eviction sind verschiedene Entscheidungen

Eine Eviction-Policy wählt einen residenten Verlierer. Eine Admission-Policy
entscheidet, ob der Kandidat diesen Platz überhaupt erhalten soll. TinyLFU
formuliert diese Trennung explizit und schätzt dafür die jüngere Zugriffshäufigkeit
von Kandidat und Opfer
([TinyLFU-Paper](https://arxiv.org/abs/1512.00727)). Für fastdup gibt es aber
bereits ein billigeres, semantisch stärkeres Signal: die Herkunft des Proofs.

| Ereignis | Generation Proof Set | Historical Cache |
|---|---|---|
| Neu publizierter Chunk | bis zum Commit pinnen | nach erfolgreichem Commit in die S3-FIFO-Probation aufnehmen |
| Exact-Kandidat nach Cache-Miss vollständig verifiziert | für die aktuelle Generation pinnen | direkt als nachgewiesen wiederverwendet in `Main` aufnehmen |
| Historical-Cache-Hit | für die aktuelle Generation pinnen | saturierenden Reuse-Zähler erhöhen |
| Commit abgeschlossen/fehlgeschlagen | kompletten Generationssatz freigeben | keine Korrektheitswirkung |
| Speicherdruck oder Swap | nicht verdrängen; Admission der Pipeline begrenzen | Cache vollständig leeren und neue Admission sperren |

Das Herkunftssignal verhindert, dass ein langer Stream ausschließlich neuer
Chunks denselben Rang wie ein bereits erneut verwendeter Chunk erhält. Ein
TinyLFU-Sketch kann später zusätzlich entscheiden, ist aber keine Voraussetzung
für diese Trennung.

## Vergleich der Replacement-Policies

| Policy | Scan-/Loop-Verhalten | Hit-Pfad und Synchronisation | Metadaten | Eignung für fastdup |
|---|---|---|---|---|
| LRU | Ein einmaliger Scan verdrängt den Hotset; ein zyklischer Scan größer als der Cache kann vollständig thrashen. | Jeder Hit verschiebt ein Element in einer doppelt verketteten Liste. Das erzeugt mehrere zufällige Schreibzugriffe und gewöhnlich einen gemeinsamen Lock. | Zwei Links plus Index. | Ablehnen. |
| SLRU | Probation schützt wiederverwendete Einträge besser vor Scans; die feste Segmentaufteilung bleibt workloadabhängig. | Zwei LRU-Listen; Hits verschieben oder promovieren Einträge. | Zwei Links, Segmentzustand. | Besser als LRU, aber für viele Worker zu schreibintensiv. |
| 2Q | `A1in`/`A1out` erkennen zweite Zugriffe und sind scan-resistent. | FIFO für neue/ghost Einträge, aber LRU-Updates in `Am`; drei Verzeichnisse. | Residenter Key plus Ghost-Key und Listenlinks. | Semantisch passend, S3-FIFO ist einfacher und hit-path-ärmer. |
| ARC | Balanciert Recency und Frequency adaptiv; scan-resistent. | Vier LRU-Listen, Ghost-Hits verändern die Zielaufteilung; O(1), aber mutationsreich. | Residenten- und Ghost-Verzeichnisse bis ungefähr `2c`, Listenlinks. | Kein erster Kandidat: hoher Implementierungs- und Synchronisationsaufwand. |
| CLOCK | Ein Referenzbit, ein Ring und lazy second chance. | Hit setzt ein Bit; Opferwahl kann mehrere Slots scannen. | Ein Bit plus Ring. | Billig, aber LRU/CLOCK haben dieselben Scan- und Loop-Schwächen. |
| CLOCK-Pro | Nutzt Reuse Distance, Hot/Cold-Zustände, nicht-residente Testeinträge und drei Hände; wesentlich scan-/loop-fester. | Keine Listenbewegung auf Hit, aber koordinierte Handläufe bei Miss. | Bis `2m` Metadateneinträge, Hot/Cold/Test-Status und Hashindex. | Technisch stark, konzeptionell und im Tail-Verhalten unnötig komplex. |
| W-TinyLFU | Frequency-Admission schützt vor Scan-Pollution; ein Window fängt Bursts ab. | Jeder Zugriff aktualisiert einen ungefähren Frequenz-Sketch; Window-LRU und Main-SLRU benötigen Policy-Wartung. | Caffeines Design nennt 8 Byte Sketch pro Cache-Eintrag, zusätzlich Listenmetadaten. | Als spätere Admission-Option messen, nicht zuerst implementieren. |
| S3-FIFO | Kleine Probation entfernt One-Hit-Wonder; Ghost-Hits und Main-Reinsertion behalten wiederverwendete Einträge. Im Paper auch auf Block-Cache-Traces geprüft. | Hits sättigen nur einen 2-Bit-Zähler; nach der zweiten Wiederverwendung ist keine weitere Metadatenänderung nötig. FIFO-Ringe brauchen keine Hit-Reorder. | Slot-ID im Ring, 2 Bit Frequenz; Ghost nur Fingerprint und Insert-Epoche. | **Primärer Kandidat.** Gut begrenzbar, cache-lokal und scan-resistent. |
| SIEVE | Lazy Promotion plus schnelle Demotion mit einer Liste, einem Hand-Zeiger und einem Visited-Bit. | Hit setzt höchstens ein Bit. Die Hand kann bei einer Eviction mehrere besuchte Einträge überspringen. | Ein Bit, ein Hand-Zeiger und Listen-/Slot-Links. | **Challenger.** Sehr elegant, aber bisher vor allem auf Web-Cache-Traces belegt; Eviction-Scan braucht eine harte Arbeitsgrenze. |

### LRU, SLRU und 2Q

Das ursprüngliche 2Q-Paper speichert erste Zugriffe in `A1in`, behält ihre
Identifiers nach der Verdrängung in `A1out` und promoviert erst einen späteren
Ghost-Hit in das LRU-verwaltete `Am`. Es empfiehlt als Ausgangswerte 25 Prozent
für `A1in` und Ghost-Identifier entsprechend 50 Prozent des Buffers
([Johnson/Shasha, VLDB 1994](https://www.vldb.org/conf/1994/P439.PDF)). Das
erzielt konstante algorithmische Kosten und Scan-Resistenz, beseitigt aber
nicht die LRU-Listenmutation im Hotset.

SLRU teilt ebenfalls in Probation und Protected. Die ursprüngliche
Veröffentlichung ist
[`Caching Strategies to Improve Disk System Performance`](https://doi.org/10.1109/2.268884).
S3-FIFOs Auswertung weist auf die fehlende Ghost-Historie von SLRU als Nachteil
bei Scans hin
([SOSP-Paper, Abschnitt zum Policy-Vergleich](https://jasony.me/publication/sosp23-s3fifo.pdf)).

Ein exaktes LRU ließe sich wie Caffeine durch lossy, geshardete Read-Ringbuffer
und gebatchte Policy-Wartung skalierbarer machen
([offizielle Caffeine-Designnotiz](https://github.com/ben-manes/caffeine/wiki/Design)).
Für fastdup wäre dies trotzdem zusätzliche Mechanik, die S3-FIFO und SIEVE auf
Hits nicht benötigen.

### ARC und Patent-/Lizenzlage

ARC hält zwei residente LRU-Listen für Recency und Frequency sowie zwei
Ghost-Listen und passt deren Verhältnis über Ghost-Hits an. Das FAST-Paper
beschreibt O(1)-Operationen und Scan-Resistenz
([Megiddo/Modha, FAST 2003](https://www.usenix.org/conference/fast-03/arc-self-tuning-low-overhead-replacement-cache)).

ARC war Gegenstand mindestens der US-Patente
[`US6996676B2`](https://patents.google.com/patent/US6996676B2/en) und
[`US7167953B2`](https://patents.google.com/patent/US7167953B2/en). Die
verlinkten Registeransichten melden sie als 2024 beziehungsweise 2022
abgelaufen, weisen aber selbst darauf hin, dass ihr Legal Status keine
rechtliche Beurteilung ist. Das beseitigt nicht die Pflicht zur Prüfung anderer
Jurisdiktionen oder Patentfamilien, falls ARC gewählt würde.

OpenZFS enthält eine produktive ARC-Implementierung, deren Quelltext ausdrücklich
CDDL-1.0 ist
([OpenZFS `arc.c`](https://github.com/openzfs/zfs/blob/master/module/zfs/arc.c)).
Dieser Code sollte nicht in das Apache-2.0-Projekt kopiert werden. Für fastdup
gibt es auch technisch keinen Grund, vier LRU-Verzeichnisse nachzubauen.

### CLOCK und CLOCK-Pro

CLOCK setzt auf Hit ein Referenzbit und sucht bei Bedarf kreisförmig nach einem
unreferenzierten Opfer. CLOCK-Pro ergänzt nicht-residente Testeinträge,
Hot-/Cold-Zustände und drei Hände. Das Paper hält höchstens `2m`
Metadateneinträge, passt den Hot-/Cold-Anteil selbst an und zeigt Vorteile für
schwache Lokalität
([Jiang/Chen/Zhang, USENIX ATC 2005](https://www.usenix.org/conference/2005-usenix-annual-technical-conference/clock-pro-effective-improvement-clock-replacement)).

Das ist deutlich mehr als „CLOCK plus ein Bit“. Die Opferwahl hat amortisiert
günstige Kosten, aber ein einzelner Miss kann mehrere Handbewegungen auslösen.
Für einen Soft-State-Cache, dessen Kandidat ohne Korrektheitsverlust abgelehnt
werden darf, sind statische Probation/Main-Ringe leichter zu begrenzen und zu
auditieren.

### TinyLFU und W-TinyLFU

TinyLFU ist primär **Admission**, nicht Eviction. Es vergleicht geschätzte
jüngere Frequenzen von Kandidat und Opfer. W-TinyLFU setzt davor ein kleines
LRU-Window und nutzt SLRU für den Hauptcache. Das Paper berichtet für Caffeine
einen 4-Bit Count-Min-Sketch, eine History von zehn Cache-Größen und acht Byte
Sketch-Speicher pro Cache-Eintrag
([TinyLFU/W-TinyLFU](https://arxiv.org/abs/1512.00727)); die aktuelle
Caffeine-Designnotiz beschreibt zusätzlich gebufferte, gebatchte
Policy-Updates
([Caffeine Design](https://github.com/ben-manes/caffeine/wiki/Design)).

Das ist bei stark schiefen Popularitäten attraktiv. Für den Proof-Cache spricht
gegen einen sofortigen Einsatz:

- Jeder neue Proof hat bereits eine teure vollständige Verifikation bestanden.
- `newly-published` gegenüber `exact-reused` liefert ein exaktes Reuse-Signal,
  bevor ein probabilistischer Sketch nötig wird.
- Frequenz-Aging erzeugt zusätzlichen Speicherverkehr.
- Ein zu kleines Window kann einen unmittelbar folgenden zweiten sequentiellen
  Zugriff verpassen. Das TinyLFU-Paper zeigt selbst, dass die beste Window-Größe
  workloadabhängig sein kann.

Der Sketch bleibt als orthogonale, per Feature Flag simulierbare Admission-Stufe
sinnvoll, falls echte Traces zeigen, dass S3-FIFOs Probation den Main-Hotset
nicht ausreichend schützt.

### S3-FIFO

S3-FIFO verwendet drei statische FIFO-Queues: Small, Main und eine payloadlose
Ghost-Queue. Das Paper startet mit zehn Prozent Small und neunzig Prozent Main.
Ein Hit erhöht einen auf drei begrenzten 2-Bit-Zähler. Ein unbenutzter Small-
Eintrag geht beim Opferlauf in Ghost, ein hinreichend wiederverwendeter in Main;
ein Ghost-Hit wird direkt in Main aufgenommen. Main dekrementiert den Zähler
erst beim Opferlauf und reinseriert den Eintrag gegebenenfalls
([Algorithmus und Implementierung](https://jasony.me/publication/sosp23-s3fifo.pdf)).

Die Autoren evaluierten 6.594 Traces aus Block-, Key-Value- und Object-Caches.
Ihr CacheLib-Prototyp erreichte bei 16 Threads sechsfachen Durchsatz gegenüber
optimiertem LRU. Ringpuffer entfernen die beiden LRU-Zeiger und führen die
Eviction sequentiell durch Slot-IDs; Ghost kann als Bucket-Tabelle aus kurzem
Fingerprint plus virtueller Insert-Epoche dargestellt werden. Der veröffentlichte
Artifact-Code steht unter Apache-2.0
([S3-FIFO-Artifact](https://github.com/Thesys-lab/sosp23-s3fifo)).

Für fastdup ist besonders nützlich, dass die Eintragsherkunft die normale
S3-FIFO-Heuristik verstärkt: ein bereits Exact-reused und erneut physisch
verifizierter Chunk hat seine zweite Verwendung bewiesen und kann direkt nach
Main. Ein neu publizierter Chunk beginnt in Small.

### SIEVE

SIEVE verwendet eine Insert-Order-Liste, ein Visited-Bit pro Eintrag und eine
Hand. Hits setzen lediglich das Bit. Bei Eviction überspringt die Hand besuchte
Einträge, löscht deren Bit und entfernt den ersten unbesuchten Eintrag, ohne die
übersprungenen Einträge neu zu verlinken. Die Autoren nennen dies Lazy
Promotion und Quick Demotion
([SIEVE, NSDI 2024](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo)).
Das Paper berichtet doppelt so hohen Durchsatz wie ein optimiertes 16-Thread-LRU
und gute Miss-Raten auf 1.559 Web-Cache-Traces. Der Artifact-Code ist
Apache-2.0
([SIEVE-Artifact](https://github.com/cacheMon/NSDI24-SIEVE)).

SIEVE ist konzeptionell eleganter als CLOCK-Pro und S3-FIFO. Zwei Punkte müssen
fastdup-spezifisch gemessen werden:

1. Die publizierte Tracesammlung ist stärker Web-/KV-geprägt als S3-FIFOs
   Block-Cache-Sammlung.
2. Ein Opferlauf kann viele gesetzte Bits abräumen. fastdup müsste pro
   Admission eine harte Schrittzahl setzen und den Kandidaten bei Erreichen
   ablehnen oder die Restarbeit in einen begrenzten Maintenance-Job geben.

Darum ist SIEVE der richtige Vergleichskandidat, aber noch nicht die
Defaultentscheidung.

## Indexierung und Speicherlayout

### Empfohlener erster Aufbau

Pro Shard:

```text
cache-line-aligned shard state
├── Mutex
├── hashbrown::HashTable<u32>     # Hash -> Slot-ID
├── Box<[ProofSlot]>              # besitzt ExactIndexEntry genau einmal
├── Small: Ring<u32>
├── Main: Ring<u32>
├── Ghost: HashTable<GhostTag>    # kurzer Fingerprint + Insert-Epoche
└── cache-line-separated counters
```

`hashbrown::HashTable` nutzt quadratisches Probing und SIMD-Gruppen
([Rust-Dokumentation](https://docs.rs/hashbrown/latest/hashbrown/hash_table/index.html)).
SwissTable hält pro Bucket ein Control Byte; dessen sieben Hashbits erlauben,
mit SIMD eine Gruppe auf Kandidaten zu prüfen, bevor vollständige Keys geladen
werden
([Abseil SwissTable Design Notes](https://abseil.io/about/design/swisstables)).

Der Tabellenwert ist nur `u32`. Die Equality-Closure vergleicht den angefragten
`(ChunkId, logical_length)` mit dem `ExactIndexEntry` im stabilen Slot-Array.
Dadurch existiert der 36-Byte-Key nicht ein zweites Mal. Der 64-Bit-Tabellenhash
kann direkt aus unabhängigen Bytes der bereits kryptographischen BLAKE3-256
Chunk ID abgeleitet werden. Die vollständige Chunk ID und Länge bleiben immer
der entscheidende Vergleich; kurze Tags oder Fingerprints dürfen niemals einen
Proof autorisieren.

Ein grobes V1-Ziel pro residentem History-Proof ist:

| Anteil | Erwartete Bytes/Eintrag |
|---|---:|
| `ExactIndexEntry` | 136 |
| Slot-Zustand, Herkunft, 2-Bit-Frequenz, Padding | 8 bis 16 |
| Swiss-Index aus Control Byte und `u32` Slot-ID inklusive freier Buckets | 6 bis 8 |
| ein Ring-Slot | 4 |
| amortisierter Ghost-Fingerprint und Epoche | 8 bis 16 |
| **Zielbereich** | **162 bis 180** |

Das ist eine Hypothese, die über `size_of`, Allokator-Bytes und RSS geprüft
werden muss. Sie würde den heutigen Verbrauch von 424 Byte pro Eintrag mehr als
halbieren.

### Sharding statt Lock-Free als erster Schritt

Die Chunk ID ist gleichmäßig verteilt. 64 bis 256 cache-line-getrennte Shards
verteilen daher normale Zugriffe ohne workloadabhängige Schlüsselwahl. Batch-
APIs gruppieren Requests zuerst nach Shard und halten je Shard nur einen kurzen
Lock. Container-I/O und Verifikation finden immer außerhalb des Locks statt.

Ein Lock-free Read-Index ist nicht der sichere Ausgangspunkt. Eine fälschlich
zurückgegebene oder zerrissen kopierte Location wäre kein harmloser Cache-Fehler.
Sie müsste durch Slot-Generationen, Sequenzzähler und sichere
Speicherrückgewinnung gegen ABA abgesichert werden. Ein geshardeter Lock macht
die Paarung aus vollständigem Key, ACTIVE-Status und `ExactIndexEntry`
offensichtlich. Erst gemessene Shard-Contention rechtfertigt einen seqlock- oder
epoch-basierten Reader.

MemC3 zeigt als Primärquelle, dass optimistisches Cuckoo Hashing mit compact
CLOCK bei read-lastigen Caches über 90 Prozent Belegung, lock-freie Leser und
hohe Cache-Lokalität erreichen kann
([MemC3, NSDI 2013](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/fan)).
Für fastdup ist das ein späterer Index-Challenger: bucketized Cuckoo liefert
zwei begrenzte Bucket-Probes, benötigt bei Inserts aber Relocation, Versioning
und einen harten Kick-Abbruch. Da eine Cache-Admission bei Fehlschlag verworfen
werden darf, lässt sich der Pfad begrenzen. `hashbrown::HashTable<u32>` bietet
zunächst erheblich weniger eigener unsicherer Code.

### Cache Lines und NUMA

- Shard-Lock, Ring-Heads/Tails und häufig geschriebene Telemetrie liegen auf
  getrennten Cache Lines.
- Vollständige 136-Byte-Proofs liegen dicht im Slot-Array; Queue-Ringe enthalten
  nur `u32`, nicht Pointer.
- Lookups vergleichen zuerst Swiss-Control-Bytes und laden den großen Proof nur
  für kurze Hash-Kandidaten.
- Ein Hit aktualisiert höchstens den kleinen Slot-Zustand. S3-FIFO benötigt
  keine Listenbewegung.
- Der Shard-Count wird aus effektiven CPUs gewählt, bleibt aber für eine
  Cache-Epoche unveränderlich. Ein Profilwert muss Benchmarks reproduzierbar
  machen.
- Eine NUMA-lokale L0-Kopie ist erst nach Messung sinnvoll. Sie würde Proofs
  duplizieren oder eine zusätzliche Slot-Lebensdauer erfordern.

## Kapazität und Speicherdruck

Der Historical Cache darf keine feste Mindestgröße beanspruchen. Ein sinnvoller
erster Benchmarkbereich ist ein Hard Budget von `effective_ram / 256` und
`effective_ram / 128`, jeweils zusätzlich durch den gemeinsamen Reserve- und
`MemAvailable`-Mechanismus aus ADR 0046 begrenzt. 0,39 bis 0,78 Prozent RAM sind
groß genug, um Millionen kompakte Proofs zu untersuchen, ohne Payload-, Dirty-
DATA-, Index- und I/O-Budgets zu dominieren.

Die Geometrie wird in Cache-Epochen geändert:

- Unterhalb der Reserve oder bei beobachtetem Swap wird der komplette
  Historical Cache fallengelassen und seine Allokationen werden freigegeben.
- Ein kleineres Ziel führt nicht zu einem langen synchronen Eviction-Sturm.
- Wachstum verwendet `try_reserve` und wird nur zugelassen, wenn Ziel plus
  temporärer Umbau vollständig gegen aktuelle Headroom gebucht sind.
- Allocation Failure bedeutet Admission-Reject, nie Commit-Fehler.
- Das gepinnte Generation Proof Set bleibt separat gebucht. Reicht dessen
  garantierter Speicher nicht, muss die Ingest-Admission warten; einen für die
  laufende Generation erforderlichen Proof zu verdrängen wäre die falsche
  Reaktion.

Bei ungefähr 175 Byte pro History-Eintrag ergeben sich nur als Größenordnung:

| History Budget | Proofs | logische Abdeckung bei 64-KiB-Mittelwert |
|---:|---:|---:|
| 128 MiB | ca. 767.000 | ca. 47,9 GiB |
| 512 MiB | ca. 3,07 Mio. | ca. 192 GiB |
| 1 GiB | ca. 6,14 Mio. | ca. 384 GiB |

Kein Replacement-Algorithmus kann einen wiederholten zyklischen 100-TB-Stream
mit einem 1-GiB-Proof-Cache vollständig treffen. Wenn die Reuse Distance größer
als die Kapazität ist, thrashen LRU/FIFO-artige Policies grundsätzlich. Für
solche Traces helfen größere semantische Capabilities, etwa eine verifizierte
Container-Generation statt eines Proofs pro Chunk. File- oder sessionbezogene
Pinning-Policies sind ein weiterer späterer Forschungsweg. Eine Eviction-Heuristik
allein löst das Kapazitätsproblem nicht.

## Konkrete Empfehlung

1. Die V1-Zahl 65.536 in eine explizite Obergrenze für gepinnte Active/Frozen
   Generation Proofs umdeuten. Die Generation besitzt ihre Proofs bis zu
   Commit-Erfolg, Abbruch oder Rollback.
2. Der unabhängige `HistoricalProofCache` besitzt ein druckabhängiges
   Byte-Budget. Er startet leer und darf jederzeit geleert werden.
3. Der Cache ist in cache-line-getrennte Shards aufgeteilt. Jeder Shard
   verwendet `hashbrown::HashTable<u32>`, eine bedarfsgesteuert wachsende Slot-Arena und
   S3-FIFO-Ringe. Kein vollständiger Key wird im Index dupliziert.
4. Neu publizierte Einträge in Probation aufnehmen; erneut physisch verifizierte
   Exact-Reuse-Einträge direkt nach Main. Hits erhöhen nur einen saturierenden
   Zähler.
5. SIEVE hinter derselben Replay-Schnittstelle behalten. Die Entscheidung für
   S3-FIFO beruht auf identischen Trace-Budgets; eine Eviction darf höchstens
   eine feste Anzahl Slots untersuchen, danach wird Admission abgelehnt.
6. TinyLFU nur als separate, abschaltbare Admission-Policy ergänzen, wenn
   Replay-Traces eine relevante Main-Pollution zeigen. CLOCK-Pro und ARC nicht
   implementieren, bevor S3-FIFO/SIEVE nachweislich scheitern.
7. Lock-free Reads erst angehen, wenn Shard-Wait-Zeit im Single- und
   Multi-Stream-Profil sichtbar ist. Korrekte Proof-Paarung ist wichtiger als
   eine theoretisch lock-freie Lookup-Zahl.

## Mess- und Freigabegates

Vor Auswahl der Default-Policy zeichnet ein binäres, versioniertes
Benchmark-Trace nur Prozessereignisse auf; es ist kein On-Disk-Produktformat:

- vollständige Chunk ID für Replay, logische Länge und Shard;
- `published-new`, `exact-reused-verified`, `history-hit` oder `evicted`;
- Generation `active`/`frozen`/`committed`;
- vermiedene beziehungsweise ausgeführte Container-Range-Verifikation;
- monotone Ereignisnummer, keine Nutzdaten.

Der erste Replay vergleicht bei identischem Byte-Budget S3-FIFO und SIEVE.
Die vollständigen Ergebnisse stehen unter
[Online-Proof-Cache: S3-FIFO gegen SIEVE](../benchmarks/proof-cache-policy-replay.md).
FIFO, W-TinyLFU+S3-FIFO und optional ARC bleiben reine spätere
Simulator-Challenger. Weitere Pflichtworkloads:

- Rocky-ISO dreimal unverändert;
- 50 ISO-Varianten mit kleinen Änderungen;
- sequentielle VM-artige Streams mit Working Sets bei 0,5×, 1×, 2× und 10×
  Cache-Kapazität;
- viele parallele Dateien mit gemeinsamem Hotset;
- ein einmaliger Unique-Scan neben einem wiederholt verwendeten Hotset;
- Druckwechsel bis Cache-Ziel null.

Pflichtmetriken:

- Proof-Hits nach Herkunft und vermiedene physische Verify-Bytes/Range-Reads;
- Admission, Rejection, Small-to-Main, Ghost-Hits und Evictions;
- Bytes pro residentem und Ghost-Eintrag einschließlich Allokator/RSS;
- Lookup-CPU, Gruppen-Probes, volle Key-Vergleiche und Eviction-Schritte;
- Shard-Lock-Wait p50/p95/p99/max und Batch-Größe;
- Cache-Hitrate sowie End-to-End-Ingest MiB/s und completed-write p99/max;
- Purge-Latenz, freigegebener RSS, `MemAvailable`, Swap und `pswpout`.

Die Default-Policy muss gegenüber dem jetzigen Stand gleichzeitig erreichen:

- keine zweite physische Verifikation für einen gepinnten Generation-Proof;
- keine Korrektheitsänderung bei Historical-Cache-Ziel null;
- mindestens halbierter Speicher pro History-Proof;
- keine globale Lock-Contention im CPU-Profil;
- begrenzte Lookup-, Admission- und Eviction-Arbeit pro Aufruf;
- bessere oder gleiche vermiedene Verify-I/O bei Rocky/VM-Traces;
- kein Swap und keine Verletzung der gemeinsamen RAM-Reserve.

## Invarianten für die spätere Implementierung

- Nur eine vollständig verifizierte ACTIVE Location darf einen Proof erzeugen.
- Ein Lookup liefert nur dann einen Proof, wenn vollständige Chunk ID und
  logische Länge im besitzenden Slot übereinstimmen. Hash-Tag, Ghost-Fingerprint
  und Queue-Mitgliedschaft sind niemals Autorität.
- Jeder für Active/Frozen benötigte Proof bleibt bis zum Ende genau dieser
  Generation gepinnt. Historical Eviction kann ihn nicht entziehen.
- Jeder residente History-Slot besitzt genau einen Indexeintrag und genau eine
  Policy-Mitgliedschaft. Jeder Indexwert zeigt auf einen belegten Slot derselben
  Generation; Slot-Reuse verwendet eine Generation oder geschieht nur unter
  exklusivem Shard-Lock.
- Ein `remember` desselben Keys darf eine Location nur durch eine neu
  verifizierte ACTIVE Location ersetzen. Länge oder Chunk ID können sich nicht
  ändern.
- Queue-, Slot-, Ghost- und Indexanzahl überschreiten ihre vorab gebuchten
  Grenzen nie. Zähler und virtuelle Epochen verwenden checked arithmetic.
- Allocation Failure, voller Cuckoo-/Swiss-Index, Eviction-Step-Limit oder
  Speicherdruck ergeben Cache-Reject/Miss, keinen Daten- oder Commit-Fehler.
- Restart, Recovery, Scrub und Exact-Index-Rebuild beginnen ohne Historical
  Cache und verbrauchen ihn nicht als Integritätsbeweis.
- Demand Reads und spätere Scrubs behalten ihre vollständige Verifikation. Ein
  Online-Proof macht Bitrot nicht dauerhaft unsichtbar.

Writer/Admission, Lookup/Consumer und ein teurer Test-Audit müssen diese
Beziehungen paaren. Concurrency-Tests injizieren Yield-Punkte zwischen
Index-Find, Slot-Kopie, Frequenzupdate, Eviction, Slot-Reuse und Pressure-Purge.

## Primärquellen

- Song Jiang, Feng Chen, Xiaodong Zhang: [CLOCK-Pro, USENIX ATC 2005](https://www.usenix.org/conference/2005-usenix-annual-technical-conference/clock-pro-effective-improvement-clock-replacement)
- Theodore Johnson, Dennis Shasha: [2Q, VLDB 1994](https://www.vldb.org/conf/1994/P439.PDF)
- Nimrod Megiddo, Dharmendra Modha: [ARC, FAST 2003](https://www.usenix.org/conference/fast-03/arc-self-tuning-low-overhead-replacement-cache)
- Ramakrishna Karedla, J. Spencer Love, Bradley Wherry: [SLRU/Disk-Cache-Strategien, IEEE Computer 1994](https://doi.org/10.1109/2.268884)
- Gil Einziger, Roy Friedman, Ben Manes: [TinyLFU/W-TinyLFU](https://arxiv.org/abs/1512.00727)
- Juncheng Yang et al.: [S3-FIFO, SOSP 2023](https://jasony.me/publication/sosp23-s3fifo.pdf) und [Artifact](https://github.com/Thesys-lab/sosp23-s3fifo)
- Yazhuo Zhang et al.: [SIEVE, NSDI 2024](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo) und [Artifact](https://github.com/cacheMon/NSDI24-SIEVE)
- Bin Fan, David Andersen, Michael Kaminsky: [MemC3, NSDI 2013](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/fan)
- Google Abseil: [Swiss Tables Design Notes](https://abseil.io/about/design/swisstables)
- Rust `hashbrown`: [`HashTable` und Lizenz](https://github.com/rust-lang/hashbrown)
- US-Patentregisteransichten: [`US6996676B2`](https://patents.google.com/patent/US6996676B2/en), [`US7167953B2`](https://patents.google.com/patent/US7167953B2/en)
- OpenZFS: [produktive CDDL-1.0-ARC-Implementierung](https://github.com/openzfs/zfs/blob/master/module/zfs/arc.c)
