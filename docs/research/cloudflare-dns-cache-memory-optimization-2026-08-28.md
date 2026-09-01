# Cloudflares DNS-Cache-Optimierungen im Vergleich mit fastdup

Stand: 2026-08-28

## Ergebnis

Cloudflares wichtigste Lehre ist nicht eine bestimmte Replacement-Policy,
sondern ein Layout-Prinzip: unveränderliche Cachewerte exakt dimensionieren,
Pointer und redundante Felder entfernen, häufige Varianten kompakt halten und
zusammengehörige Daten zusammenhängend speichern. Der Effekt wurde gemeinsam
auf Allokationen, End-to-End-Latenz und Produktions-RSS geprüft
([Cloudflare, Messaufbau](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#benchmarking-memory-usage),
[Ergebnisse](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#the-results)).

fastdup folgt diesem Prinzip bereits weitgehend. Die Verified Read Cache nutzt
eine feste vierfach assoziative Geometrie ohne LRU-Zeiger, die Historical Proof
Cache indexbasierte S3-FIFO-Ringe, und die Exact Page Cache feste direkte Slots.
Alle drei sind rebuildable Beschleunigung unter einem gemeinsamen, fail-closed
Memory-Headroom-Budget. Deshalb ist aus dem Artikel **kein Wechsel der
Replacement-Policy** abzuleiten.

Sinnvoll sind drei begrenzte Folgeexperimente:

1. echte Allokations- und Kapazitätsmessung pro Cachetyp statt ausschließlich
   konservativer Konstanten;
2. Prüfung, ob Verified-Chunk-Payloads im realen Decoder nennenswert ungenutzte
   `Vec`-Kapazität behalten und ob diese ohne zusätzliche Payload-Kopie entfernt
   werden kann; und
3. ein kompakter Descriptor-Cache-Challenger, der die bereits im Descriptor
   enthaltene `ContainerId` nicht zusätzlich als 16-Byte-`HashMap`-Key hält.

Die Punkte 2 und 3 sind erst nach Messung Implementierungskandidaten. Für Punkt
2 gilt die Copy-Vermeidung der bestehenden Read-Seam als harte Schranke.

## Bezugsrahmen in fastdup

Die Cachedaten sind keine Storage Authority. Ein Verified Read Cache-Eintrag
wird erst nach vollständiger Verifikation sichtbar; Exact Pages liefern nur
unverifizierte Location-Kandidaten; Historical Proofs und Container-Deskriptoren
sind verwerfbare Beschleunigung. Das ist in ADR 0046 festgelegt
(`docs/adr/0046-bound-verified-read-cache-by-live-memory-headroom.md:5-20,48-67,84-107`).
Read Amplification und Hintergrund-I/O bleiben entsprechend ADR 0030 begrenzt
(`docs/adr/0030-bound-read-amplification-and-background-io.md:8-19`).
Die Historical Proof Cache muss S3-FIFO mit stabilen Slot-Indizes und begrenztem
Eviction-Scan beibehalten; ihre Byte-Charge ist ausdrücklich ein Modell, keine
Formatgröße (`docs/adr/0051-use-s3-fifo-for-the-historical-proof-cache.md:5-25`).

Aktuelle relevante Layouts:

- Verified Read Cache: `CacheKey` plus `Arc<Vec<u8>>`, vier Ways pro Set und
  Round-Robin-Victim (`crates/fastdup-store/src/read_cache.rs:186-220,238-288`);
  die reale Payload-Charge ist `Vec::capacity()`
  (`crates/fastdup-store/src/read_cache.rs:219-221,543-595`).
- Historical Proof Cache: `Option<ExactIndexEntry>` plus `u32`-Link,
  Frequenzbyte und Queue-Tag; Slot-Arena, Free-Liste, Swiss Table und getrennte
  Ghost-Strukturen liegen pro Shard
  (`crates/fastdup-appliance/src/historical_proof_cache.rs:145-199`).
- Descriptor Cache: `HashMap<[u8; 16], SealedContainerDescriptor>` in 256
  Shards (`crates/fastdup-store/src/container_descriptor_cache.rs:11-24,44-71`).
  Der Descriptor enthält über seinen Header selbst bereits die Container-ID
  (`crates/fastdup-format/src/container.rs:390-399,429-440`).
- Exact Page Cache: feste cache-line-ausgerichtete Direct-Mapped Slots und
  vollständige 4-KiB-Seiten (`crates/fastdup-store/src/exact_index_repository.rs:2654-2688,2747-2807`).

## Abgleich je Cloudflare-Technik

| Cloudflare-Technik | fastdup-Bewertung | Begründung und Folgerung |
| --- | --- | --- |
| `Vec<T>`/`String` nach Aufbau zu `Box<[T]>`/`Box<str>` machen | **Teilweise umgesetzt; gezielt messbar** | Shard-Verzeichnisse werden bereits als `Box<[...]>` eingefroren (`read_cache.rs:289-305,355-360`; `historical_proof_cache.rs:321-329`). Die große Verified Payload bleibt absichtlich `Arc<Vec<u8>>`, damit ein Decoder-`Vec` ohne Kopie übernommen und Hits durch einen billigen `Arc`-Clone geteilt werden (`read_cache.rs:198-220`). Ein blindes `Arc<[u8]>`- oder `Box<[u8]>`-Refactoring kann eine zusätzliche Vollkopie oder komplizierte Initialisierung erzwingen. Erst Capacity-Slack messen; nur eine copy-neutrale Alternative weiterverfolgen. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#the-cost-of-capacity) |
| Mehrere Listen zu einem Array plus kleinen Offsets zusammenführen; Flags packen | **Im Grundsatz umgesetzt; kein unmittelbarer Umbau** | Verified Payload und Exact Pages sind zusammenhängende Bytebereiche; Read Sets und S3-FIFO verwenden Arrays und kleine Slot-IDs statt pointerreicher Listen. Die mehreren Historical-Shard-Collections haben verschiedene Lebenszyklen und Semantiken, nicht bloß Sektionsgrenzen eines Wertes. `free` könnte theoretisch intrusiv über freie Slots laufen und Ghost-Index plus FIFO könnten eine Arena teilen, aber das erhöht Invarianten- und Eviction-Komplexität. Nur bei gemessenem Metadatenanteil untersuchen. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#fewer-lists-fewer-pointers) |
| Häufig mit dem Key identische Daten nicht erneut speichern, sondern beim Read ableiten | **Übertragbar auf Descriptor Cache** | `HashMap<[u8;16], SealedContainerDescriptor>` hält die Container-ID im Tabellen-Key und nochmals im Descriptor-Header. Ein `hashbrown::HashTable<SealedContainerDescriptor>` oder eine Slot-Arena mit Table-Index könnte die separate Keykopie vermeiden; bei Lookup wird weiterhin die volle 128-Bit-ID verglichen. Das ist rebuildable und ändert keine durable Struktur. Vorher tatsächliche Bucket-/Allocatorkosten messen, weil die theoretischen 16 Byte nicht 1:1 RSS sind. Für Verified Read und Historical Proof ist der Key dagegen nicht redundant: die Payload trägt keine Chunk-ID, und `ExactIndexEntry` braucht Chunk-ID plus Länge für Kollisionsprüfung und Rückgabe. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#dropping-the-owner) |
| Seltene große Enum-Varianten boxen, häufige kleine inline halten | **Nicht passend** | Die Cachewerte sind keine stark schief verteilten Sum Types wie DNS Record Data. `ExactLocationTransition` ist ein kleiner zustandsloser Enum (`crates/fastdup-format/src/exact_index.rs:53-80`); `ExactIndexLocation` ist ein einheitlicher fester Proof (`exact_index.rs:82-95`). Boxing würde Allokationen und Pointer-Chasing hinzufügen, ohne einen großen Max-Variant-Slot zu entfernen. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#enum-sizing) |
| Variable Records als einen längenpräfixierten Byteblob statt als Enum plus Einzelboxen speichern | **Bereits sinngemäß umgesetzt / für Proofs nicht passend** | Verified Chunks und Exact Pages werden als zusammenhängende Bytes gehalten. Der Historical Proof ist dagegen ein häufig feldweise gelesener, fester `Copy`-Wert; ihn auf dem Hit-Pfad erst aus Bytes zu dekodieren verschlechtert die Lokalität eher. Durable Exact-Index-Seiten werden ohnehin feldweise serialisiert; Rust-Layout darf gemäß Repository-Regel kein Dateiformat werden. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#storing-records-in-wire-format) |
| Wiederverwendbares Insert-Scratch, danach eine exakt dimensionierte Allokation | **Prinzip umgesetzt; Payload-Challenger nur copy-neutral** | fastdup besitzt bereits caller-owned Scratch und gebündelte Read-/Exact-Pläne; die Verified Payload übernimmt den Decoder-`Vec` gerade zur Vermeidung einer zusätzlichen Kopie. Cloudflares Scratch-plus-`memcpy` gewann bei vielen kleinen Record-Allokationen; fastdup hat typischerweise einen großen Decoder-Puffer. Ein extra `memcpy` ist daher nicht automatisch sinnvoll und kollidiert mit der bestehenden Copy-Reduktion. [Cloudflare-Technik](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/#storing-records-in-wire-format) |

## Konkrete Kandidaten

### P1: belastbare per-cache Allokationsmessung

Cloudflare maß Zahl und Größe der Allokationen pro Entry mit einem instrumentierten
Allocator und ergänzte dies um Produktions-RSS, weil Trafficmix, Occupancy und
Allocatorzustand das Ergebnis verändern. fastdup rechnet bei Historical Proofs
pauschal mit 224 Byte je residentem Entry
(`historical_proof_cache.rs:16,486-500`) und bei Deskriptoren mit 160 Byte
(`container_descriptor_cache.rs:11-15,329-338`). Diese Konstanten sind sichere
Budgets, aber kein Beleg für reale Fragmentierung, Bucket-Overhead oder
zurückgehaltene Capacity.

Experiment:

1. In einem Benchmarkprozess je Cachetyp isoliert 0, 25, 50, 75 und 100 Prozent
   des Zielbestands füllen; feste Seeds und produktionsnahe Größenverteilung
   verwenden.
2. Je Stufe Entries, logische Payloadbytes, Summe `Vec::capacity`, Zahl/Größe
   der Allokationen, allocator allocated/resident und Prozess-PSS/RSS erfassen.
3. Nach Warm-up und nach mindestens zwei kompletten Eviction-Umläufen messen;
   Peak, Plateau und nach `clear` verbleibendes Resident Set getrennt berichten.
4. Parallel Insert/s, Hit-Latenz p50/p99, Lock-Wartezeit und Cache-Hit-Rate
   erfassen. Eine Layoutänderung darf die ADR-0046-Reserve und den maximalen
   Eviction-Aufwand nicht verschlechtern.

Akzeptanz: Ein Kandidat braucht mindestens 10 Prozent weniger steady-state
Bytes pro residentem Eintrag oder mindestens 5 Prozent bessere Hit-Latenz bei
gleicher Byte-Grenze; p99 darf höchstens 2 Prozent schlechter werden. Die
hard-limit/accounting Tests müssen weiterhin konservativ oberhalb der realen
Allokationen liegen.

### P1: Descriptor ohne duplizierten Map-Key

Einen benchmark-only Challenger mit embedded-key Hash Table gegen die bestehende
`HashMap<[u8;16], Descriptor>`-Variante spielen. Beide müssen bei identischen
128-Bit-Keys denselben Hit/Miss-Verlauf und dieselbe Evictionfolge liefern.

Zusätzlich zu den allgemeinen Metriken messen:

- `size_of` von Bucket/Value und effektive Bytes je belegtem Bucket bei 50/75/90
  Prozent Load Factor;
- zufällige und container-lokale Lookup-Latenz;
- Rehash-Spitzen und Speicher nach Pressure-Purge; und
- vollständigen ID-Vergleich bei künstlichen Hashkollisionen.

Risiken sind schlechtere Library-Ergonomie, fehlerhafte Equality-Seams und
höhere Rehash-Kosten. Die Optimierung bleibt ausschließlich in-memory; Writer,
Recovery und Offline Scrub dürfen nicht berührt werden.

### P2: exakt dimensionierte geteilte Verified Payload

Zuerst ausschließlich instrumentieren: Histogramm von
`capacity - len`, Verhältnis `capacity/len`, Chunkgröße und Codecpfad bei Miss
und Admission. Falls p95-Slack unter 1 Prozent liegt, ist der Kandidat erledigt.

Nur falls der gewichtete Slack mindestens 5 Prozent der residenten Payloadbytes
ausmacht, zwei Implementierungen benchmarken:

1. aktuelles zero-copy `Arc<Vec<u8>>`; und
2. eine exakt dimensionierte Shared-Byte-Repräsentation, die entweder direkt
   als endgültige Allokation dekodiert oder nachweislich durch den Allocator ohne
   Payload-Kopie geschrumpft werden kann.

Ein `Vec -> Box<[u8]> -> Arc<[u8]>`-Pfad mit zusätzlichem vollständigem `memcpy`
ist kein akzeptabler Default. Messen: Admission bytes copied, Allokationen,
decode+admit ns/Byte, Hit ns, Restore MiB/s, RSS und Fragmentierung. Akzeptanz:
mindestens 5 Prozent weniger gesamter Cache-RSS ohne zusätzliche Vollkopie und
ohne messbaren Restore-Durchsatzverlust. Die vollständige Längen- und Chunk-ID-
Verifikation vor Admission (`read_cache.rs:517-532`) bleibt unverändert.

## Nicht empfohlene Änderungen

- S3-FIFO nicht wegen dieses Artikels ersetzen; Cloudflare untersucht hier
  Entry-Layout, nicht Eviction-Qualität.
- Keine Cachewerte durch Rust-Memory-Layout dauerhaft serialisieren.
- Keine großen Proof-Enums oder Einzelboxen einführen.
- Keine getrennten Cache-Budgets zu einem größeren Gesamtcache addieren; der
  gemeinsame Reserve- und Swap-Fail-Closed-Pfad aus ADR 0046 bleibt maßgeblich.
- Keine Speicherersparnis nur aus `size_of` ableiten. Alignment, Hash-Buckets,
  Allocator-Size-Classes, Fragmentierung und steady-state RSS gemeinsam messen.

## Quellen

- Cloudflare: [How we saved 100 terabytes of memory by optimizing 1.1.1.1's DNS cache](https://blog.cloudflare.com/dns-cache-memory-optimization-1111/),
  insbesondere die oben direkt verlinkten Abschnitte. Der Artikel ist die
  Primärquelle für Big Pineapples Layout und Messwerte.
- Rust Standard Library: [`Vec`](https://doc.rust-lang.org/std/vec/struct.Vec.html),
  [`Box`](https://doc.rust-lang.org/std/boxed/struct.Box.html) und
  [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) für die jeweiligen
  Ownership- und Layoutgarantien. Konkrete Allokationsfreiheit einer Konversion
  wird hier bewusst nicht unterstellt, sondern als Messfrage behandelt.
- fastdup-Primärquellen: die akzeptierten ADRs 0030, 0046 und 0051 sowie die in
  dieser Notiz mit Datei und Zeile genannten Implementierungen.
