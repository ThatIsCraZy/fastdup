# fastdup

> **Ein experimentelles POSIX-Dateisystem, das Exact Dedup als offen
> nachvollziehbare Storage-Semantik statt ausschließlich hinter einer
> Backup-Appliance implementiert.**

## Was fastdup besonders macht

fastdup nimmt normale Linux-Dateioperationen auf einem lebenden, veränderbaren
Namensraum an und übersetzt sie in unveränderliche, inhaltsidentifizierte
Container, hierarchische Manifeste und atomar ausgewählte Commit-Generationen.
Identische Logical Chunks werden nur einmal gespeichert. Trotzdem bleibt eine
Datei eine bytegenaue POSIX-Datei mit Random Writes, Sparse Extents, Hardlinks,
xattrs und offenen, bereits gelöschten Inodes.

Der entscheidende Unterschied ist die Trennung von Wahrheit und
Beschleunigung: Manifeste und geprüfte Container tragen die Inhalte; Exact
Index, Bloom-Filter und Read Caches dürfen vollständig verloren gehen und neu
aufgebaut werden. Writer, Recovery und Offline-Scrub prüfen dieselben
dauerhaften Invarianten. Crash-Grenzen, Format-Epochen, GC-Beweise und
Generation-Reservierungen sind als versionierte Formate, ADRs und
Fault-Injection-Tests im Apache-2.0-lizenzierten Quellcode nachvollziehbar.

### Abgrenzung zu Data Domain und StoreOnce

fastdup ist kein kleinerer Nachbau von Dell PowerProtect Data Domain oder HPE
StoreOnce. Die Produkte lösen überlappende Probleme, setzen ihre primäre
Abstraktion aber an einer anderen Stelle:

| Aspekt | fastdup | Dell PowerProtect Data Domain / HPE StoreOnce |
| --- | --- | --- |
| Primäre Aufgabe | Experimenteller, direkt mutierbarer Single-Node-POSIX-Speicher mit transparenter Exact Dedup | Ausgereifte Protection-Storage-Plattformen für Backup, Restore, Retention und Cyber Resilience |
| Frontdoor | Linux FUSE und optional Samba; die Anwendung sieht Dateien und Verzeichnisse | Backup-optimierte Integrationen wie DD Boost beziehungsweise StoreOnce Catalyst, zusätzlich je nach Produkt NFS/SMB und VTL |
| Technischer Schwerpunkt | Bytegenaue POSIX-Semantik, explizite Crash-Generationen, öffentlich spezifizierte Formate und neu aufbaubare Beschleunigungsindizes | Backup-Fenster, Restore-SLAs, Backup-Software-Ökosysteme, Replikation, Cloud-Tiering und zentraler Betrieb |
| Schutzumfang heute | Prozess-/Power-Loss auf funktionierendem Stable Storage; kein eigener Schutz gegen Geräteverlust, keine WORM-/Vault-Garantie | Herstellerfunktionen für Immutability, Verschlüsselung, Replikation und isolierte beziehungsweise Cloud-basierte Recovery-Kopien |
| Reifegrad | Forschungsprototyp mit reproduzierbaren Tests und Benchmarks, ohne Support- oder Kapazitätszusage | Kommerzielle Appliances und virtuelle Angebote mit dokumentierten Modell-, Support- und Integrationsgrenzen |

Dell beschreibt Data Domain als Purpose-Built Backup Appliance mit breitem
Backup-Software-Ökosystem, DD Boost, Replikation, Cloud Tier und
Security-/Immutability-Funktionen; DD OS unterstützt außerdem NFS, CIFS und VTL.
HPE positioniert StoreOnce ebenfalls als Purpose-Built Data-Protection-Plattform
mit Catalyst, NAS-/VTL-Zielen, Catalyst Copy, Cloud Bank und integrierten
Cyber-Resilience-Funktionen. Maßgeblich sind die aktuellen
[Dell-Produktinformationen](https://www.dell.com/en-us/shop/powerprotect-data-domain/sf/powerprotect-data-domain),
die [Dell-Protokolldokumentation](https://www.dell.com/support/manuals/en-ca/dd-os-7.10/dd_p_ddos_7.10.1.70_ag/data-access-by-protocol?guid=guid-ff3483b7-d324-4cc5-8814-877818407dfd&lang=en-us)
und die [HPE-Produktinformationen](https://www.hpe.com/us/en/storage/storeonce.html).
Die belegten Einzelmerkmale und bewusst vermiedenen Vergleichsbehauptungen
stehen in der
[Recherchegrundlage](docs/research/commercial-backup-appliance-comparison.md).

Das ist kein Überlegenheitsversprechen: Wer heute zertifizierte
Backup-Integrationen, Hersteller-Support, Replikation, Immutability oder
Cyber-Recovery benötigt, braucht ein entsprechend ausgereiftes Produkt.
fastdup ist interessant, wenn ein offener POSIX-Speicherkern untersucht,
erweitert und gegen explizite Crash- und Integritätsinvarianten geprüft werden
soll. Herstellerangaben zu Datenreduktion oder Durchsatz sind nicht direkt mit
den fastdup-Benchmarks vergleichbar.

Das Repository ist ein Prototyp und noch kein Backup-Produkt. Der dauerhafte
Pfad umfasst Exact Dedup, RAW/Zstd-Encoding, sparse Dateien, Checkpoints,
Recovery, Scrub, einen neu aufbaubaren Exact Index sowie adaptives DATA- und
Metadata-GC. Der erweiterte dauerhafte Pfad besitzt außerdem einen neu
aufbaubaren, an den Exact Index gebundenen Similarity Index und Depth-1-
`ZSTD_PREFIX`-Records. Diese Bausteine sind vollständig lesbar, recoverbar,
scrubbar und GC-sicher. Der Daemon kann sie nach einem gepaarten
Exact-/Similarity-Rebuild explizit mit `FASTDUP_ADVANCED_REDUCTION=prefix-v1`
aktivieren. Alle neuen Repositories tragen vom ersten Commit an dieselbe
aktuelle Writer-Policy; `off` ist deren unabhängiger RAW/Zstd-Fallback und
keine zweite Policy. Andere Policy-IDs werden ohne Migration abgewiesen. Ein
realer ABBA-SMB-Lauf ist dokumentiert, rechtfertigt wegen kleiner
Kapazitätswirkung und streuender GC-Kosten aber noch keine Aktivierung als
Default. Sparse-XOR-Delta, Dictionary und Reorder bleiben Forschungs- oder
Referenzpfade.

fastdup wird ausschließlich für 64-Bit-x86-Systeme entwickelt, getestet und
qualifiziert. ARM, andere CPU-Architekturen und entsprechende Cross-Builds sind
nicht unterstützt. AVX2/BMI2 und weitere x86-Erweiterungen werden nur nach
Runtime-Erkennung benutzt; die x86-64-Baseline bleibt lauffähig.

Die verbindlichen Begriffe stehen in [CONTEXT.md](CONTEXT.md). Entscheidungen
über Haltbarkeit und Formate stehen in den [ADRs](docs/adr/).

## Aktueller Funktionsumfang

Der FUSE-Pfad unterstützt unter anderem:

- Dateien anlegen, öffnen, lesen, schreiben, verkürzen, umbenennen und löschen
- zufällige Writes, Append, `O_TRUNC`, `O_EXCL`, `RENAME_NOREPLACE`, `flush`
  und `fsync`
- sparse Dateien mit DATA-, FILL- und HOLE-Extents
- Metadatenklone über `copy_file_range`, ohne bereits unveränderliche DATA
  erneut zu lesen oder zu chunken
- absturzsichere Checkpoints und Recovery auf den jüngsten vollständigen
  Commit
- selbstständige DATA-Tier-Recovery-Checkpoints für vollständigen Verlust der
  NVMe-Metadaten; aktueller und vorheriger Stand bleiben unabhängig prüfbar
- Unterverzeichnisse, Hardlinks, Symlinks, xattrs/ACLs, Besitz, Rechte,
  Zeitstempel und flüchtige POSIX-Record-Locks
- Namespace-Zustände oberhalb der 16-MiB-Objektgrenze: eine Commit-Root bindet
  geordnete, content-definierte 256-KiB/512-KiB/1-MiB-Shards; Recovery, GC,
  Scrub und DATA-Tier-Recovery prüfen denselben vollständigen Graphen
- `statfs`, dünne Allokation, Hole Punch, Zero Range und DATA/HOLE-Seeks
- Kernel-Read-Cache und Readahead für Read-only-Handles mit expliziter
  Bereichsinvalidierung; Read-only-`mmap` ist kohärent, Shared-writable-`mmap`
  wird in v1 abgewiesen
- Offline-Scrub, adaptives Online-/Offline-GC und Neuaufbau des Exact Index
- optionaler gepaarter Neuaufbau von Exact und Similarity sowie begrenzte
  Depth-1-Zstd-PREFIX-Auswahl gegen unabhängig dekodierbare Bases
- lock-freie Advanced-Reduction- und druckbegrenzte Similarity-Cache-
  Telemetrie für Queries, Trials, Base-I/O, Annahmen, Fallbacks und Einsparung

Der Adapter bildet noch nicht den gesamten POSIX-Umfang ab. Insbesondere BSD
`flock` sowie die breite Client-, POSIX-, Samba- und
Crash-Konformitätsmatrix sind noch offen. Der verbleibende Umfang ist in der
[POSIX-Testplanung](docs/testing/posix-conformance.md) erfasst.

## Aktueller Projektstand

Die zuletzt abgeschlossenen Abschnitte haben Kernel- und Userspace-Caches,
Memory-/Swap-Containment, Online-/Metadata-GC, den Haltbarkeitspfad bei
ausbleibendem I/O-Fortschritt und zwei repositoryweite
Upgrade-/Allocator-Grenzen geschlossen:

- Online GC beweist Kandidaten gegen die aktuelle und vorherige Commit-
  Generation, alle aktiven Metadata Root Pins und die aktive Exact-Index-
  Generation. Vor `RETIRING` werden diese Bindungen unter der gemeinsamen
  Publikationsbarriere erneut geprüft.
- Metadata GC markiert alle dauerhaften und prozesslokal gepinnten Graphen.
  Persistente Snapshot-/Addition-Kataloge beschleunigen unveränderte oder rein
  additive Läufe, erhalten aber nur nach einem exakten Mark Löschbefugnis.
- Ein kernelgestützter Appliance Lease schließt einen zweiten Writer und
  gleichzeitige Offline-Wartung aus. Ungültige Start-Policies scheitern, bevor
  das Repository geöffnet oder verändert wird.
- Ein expliziter Durability Supervisor schließt Mutation Admission nach fünf
  Sekunden ohne Checkpoint-Fortschritt. Deterministische Tests halten sowohl
  Metadata- als auch DATA-Sync an, bewegen die monotone Testzeit ohne reale
  Wartezeit und belegen: angenommene Writes bleiben lesbar, spätere Writes
  gelangen nicht mehr in den Namespace.
- Ein leerer, dauerhafter Appliance Recovery Latch wird vor dem Öffnen des
  Repositorys schreibbar gesetzt. Crash oder fehlgeschlagener Abschluss lassen
  ihn stehen; nur vollständige Recovery plus sauberer Shutdown oder ein
  erfolgreicher Offline-Scrub entfernen ihn. Fehler beim Setzen/Löschen sowie
  fehlerhafte Dateien und Symlinks werden fail-closed geprüft.
- Commit Record v2 trägt Repository Format Epoch 1. Writer, Recovery und Scrub
  akzeptieren ausschließlich Epoch 1; Commit-v1/Epoch-0 ist nicht migrierbarer
  Vorproduktionszustand und wird vor jeder Repository-Mutation abgewiesen.
- Zwei verkettete 4-KiB-DATA-Slots reservieren Container-Generationen in
  1.024er-Bereichen dauerhaft. Sie dürfen nur in einem leeren DATA-Repository
  initialisiert werden; ein nichtleeres Repository ohne High-Water wird ohne
  Envelope-Migration abgewiesen. Ein gesunder Start benötigt keinen
  Container-Verzeichnis-Scan; Crashs dürfen Nummern überspringen, aber niemals
  wiederverwenden.
- Read-only-FUSE-Handles nutzen jetzt den Kernel-Page-Cache mit `KEEP_CACHE`;
  schreibfähige Handles bleiben `DIRECT_IO`, Writeback bleibt aus. Erfolgreiche
  Writes, Truncates, Clone-/Fallocate-Mutationen invalidieren vor ihrer Antwort
  exakt den betroffenen Inode-Bereich, sobald der Inode einmal einen
  cachefähigen Read-only-Handle geliefert hat. Reine Write-only-Inodes
  überspringen den redundanten Kernel-Notify. Der reale Mount-Test deckt
  mehrere Handles, Seitengrenzen, Hole/Zero, Truncate sowie kohärentes
  Read-only-`mmap` und die Ablehnung von Shared-writable-`mmap` ab.
- Ein prozessweiter `MemoryBudgetGovernor` liefert allen rebuildbaren Caches
  höchstens alle 250 ms denselben fail-closed Prozess-/Host-/cgroup-Snapshot. Bereits
  belegter Host- oder Shared-cgroup-Swap schaltet fastdup nicht ab; nur der
  eigene Prozess-Swap schließt Cache Admission. `MemorySwapMax=0` in einer
  dedizierten cgroup plus `FASTDUP_REQUIRE_CGROUP_NO_SWAP=1` bilden die harte
  Kernel-Grenze.
- Das aktuelle Policy Set gilt vom ersten Repository-Commit an für `off` und
  `prefix-v1`; es gibt weder eine Legacy-Policy noch einen Policy-
  Migrationspfad. `rebuild-pool-indexes` veröffentlicht Exact und Similarity
  aus einem verifizierten Scan als kohärentes Paar; doppelte Chunk-IDs werden
  im begrenzten externen Sort vor der kanonischen Similarity-Publikation
  verdichtet.
- Langlebige, dichte Dedup-Bloom-Tabellen ab 2 MiB liegen in eigenen anonymen
  `MADV_HUGEPAGE`-Mappings. Kleine Tabellen bleiben auf dem Heap. Die
  Bloom-Probe-Hot-Loop erhält dadurch weder Sampling noch Locks noch eine
  Heap-/Mmap-Fallunterscheidung.
- Der Supervisor, die Latch-I/O und deren Synchronisation liegen ausschließlich
  im Daemon-/Maintenance-Kontrollpfad. Auch Epoch-Prüfung und High-Water-I/O
  liegen an Repository-Open, Commit, Container-Publikation und Scrub. Die
  POSIX-Namespace-, Ingest-Admission- und Reduktions-Hot-Loops erhielten keine
  zusätzliche Dateisystem-I/O und keinen neuen globalen Lock. Der Write-Pfad
  trägt neben dem Inode-lokalen Cache-Atomic nur begrenzte atomare
  Kapazitäts-Claims; `statvfs`, Commit-Epoch-Rotation und deren Mutex bleiben
  im Kontrollpfad. Kernel-Cache-Notify bleibt am FUSE-Rand und läuft nur nach
  einer erfolgreichen Inhaltsmutation eines cache-exponierten Inodes.
- Der vollständige serielle Workspace-Test, Clippy, der Release-Build und die
  reale siebenstufige SIGKILL/FUSE-Remount-Matrix sind für diesen Stand grün.
  Dauerhaft blockierte oder fehlerhaft bestätigende Hardware bleibt außerhalb
  des unterstützten Ausfallmodells.
- Das einzige Container-Format bleibt Version 2; es gibt weder einen
  Format-3-Writer noch Migration oder Legacy-Reader. Index-freie Prefix-Reads
  listen den DATA-Namensraum einmal, durchsuchen nur die vorhandenen kompakten,
  checksummierten Recovery Indexes, lesen genau den ausgewählten Record und
  cachen wiederholt verwendete Bases nur für den laufenden Container-Read.
  Physische Base-Adressen bleiben ausschließlich rebuildbare
  Location-Beschleunigung.
- Commit, geschardeter Namespace-Graph, Manifest Leaf/Inner, Exact Run Set,
  Metadata-Mark und Similarity-Publikation besitzen jeweils nur noch ihren
  aktuellen Writer- und Readerpfad. Similarity nutzt auch für Singleton-Snapshots immer
  Partition plus Family-Manifest; Vorproduktionsformate werden nicht migriert.
- Restore-Read-Pläne sortieren ausgewählte Exact-Locations nach
  Container/Record-Offset und lesen direkt benachbarte Records desselben
  Containers in einem höchstens 1-MiB großen DATA-Read. Jeder Record wird aus
  seinem eigenen Slice weiterhin vollständig und unabhängig verifiziert; die
  logische Ausgabeordnung und der skalare Einzel-Extent-Pfad bleiben erhalten.
- Ein separater Worker schreibt alle 90 Sekunden und beim sauberen Shutdown
  einen selbstständigen Recovery Checkpoint auf das DATA Tier. Zwei verkettete
  Selector-Slots halten den aktuellen und vorherigen vollständigen Graphen.
  Nach vollständigem Metadata-Tier-Verlust werden Checkpoint und DATA vor der
  ersten Metadata-Mutation geprüft, der originale Commit zuletzt installiert
  und Exact beziehungsweise Exact/Similarity vor dem Mount neu aufgebaut.
  Root-Pins binden parallel laufendes GC; Graphscan, Verifikation und HDD-I/O
  halten weder den Commit-Lock noch die Metadata-GC-Publikationsbarriere.
- Metadata- und DATA-Pool tragen checksummierte persistente Identitäten. Beide
  teilen eine Appliance-ID, besitzen verschiedene Pool-IDs und feste Rollen;
  vertauschte Pfade, fremde Pools, doppelte IDs, Symlinks und befüllte
  Vorproduktions-Pools ohne Identität scheitern vor Recovery beziehungsweise
  Offline-Scrub. Die Initialisierung ist gegen jeden Publikationsabbruch
  fault-injection-getestet und berührt keine Ingest-Hot-Loop.
- Produktiver Writable-Start verlangt zwei physisch getrennte XFS-
  Dateisysteme; verschiedene Verzeichnisse oder Pool-IDs allein genügen nicht.
  Nur `FASTDUP_POOL_ISOLATION=lab-allow-shared` erlaubt bewusst einen
  nicht-produktiven Ein-Disk-Aufbau. Ein lock-freier
  `CommitCapacityGovernor` schützt dauerhaft 64 MiB Metadata-Commit-Reserve
  und reserviert vor jeder sichtbaren Mutation den pessimistischen Metadata-/
  DATA-Footprint. `ENOSPC` kommt vor der Mutation; Reads und Cleanup bleiben
  möglich. Claims bleiben bei fehlgeschlagenem Commit gebunden und werden erst
  nach durablem Commit plus nachfolgender physischer Kapazitätsmessung
  freigegeben.

## Empfohlener nächster Entwicklungsabschnitt

Die Kapazitätsentscheidungen aus ADR 0081 bis 0083 sind umgesetzt. Als Nächstes
sollte die policy-gesteuerte Small-File-Platzierung aus ADR 0084 ihre eigene
XFS-Projektquota erhalten, ohne die geschützte Metadata-Reserve ausleihen zu
können. Parallel bleibt der HDD-Lesepfad auf echter rotierender Hardware zu
qualifizieren. Der korrigierte A/B nutzt den produktiven
`IoUringStorageIo`-Adapter: Bei 64-KiB-Chunks sinken Ring-Submissions von 128
auf 16 und der Planned-Pfad ist im Median 25,7 Prozent schneller. Beide Pfade
erzeugen wegen Kernel-Readahead trotzdem dieselben zehn sequenziellen Block-
Reads. Bei 256-KiB-Chunks sinken Submissions nur von 128 auf 64, Block-Reads von
34 auf 33, und Planned ist 12,0 Prozent langsamer. Obwohl der Gast `ROTA=1`
meldet, wurde keine HDD-Latenz emuliert. Coalescing bleibt für das HDD-Ziel
aktiv; ein Schwellwert, spekulatives Readahead oder Parallel-I/O benötigt zuerst
fragmentierte Messungen auf echter HDD. Gleichzeitig bleibt der opt-in
Advanced-Reduction-Pfad gegen breitere Backup-Corpora zu qualifizieren.

Der Abschnitt ist abgeschlossen, wenn:

1. ein alternierender Cold-Restore-A/B auf physischer HDD oder dem geplanten
   redundanten HDD-Array sequenzielle, fragmentierte und Container-übergreifende
   Dateien abdeckt;
2. Small-File-Workloads Suchwege, IOPS, Platzverbrauch und Write Amplification
   messen, bevor ein dauerhaftes Platzierungsformat festgelegt wird;
3. gefüllte Cache- und Small-File-Quoten die Metadata-Reserve nicht verbrauchen
   und jede zugelassene Mutation ihren bounded Commit abschließen kann;
4. mehrere versionierte Backup-Familien Exact-, Similarity- und Fallback-
   Entscheidungen reproduzierbar auslösen und ABBA-Läufe Kapazität,
   SMB-Durchsatz, completed-write-p99, Restore und Swap gemeinsam ausweisen;
5. alle Restores bytegenau sowie Recovery, Scrub und GC fail-closed bleiben;
   und
6. weder Restore-Optimierung noch Metrik, Cache-Governance oder Policy-Auswahl
   neue Locks, Syscalls oder Speicher-Samples in die Ingest- und Candidate-
   Hot-Loops einführen.

Danach folgen die randomisierte Process-Kill-Kampagne, Blockgeräte-Power-Cut-/
Torn-Write-Tests und die offenen POSIX-/Samba-Matrizen.

## Ingest-Pipeline

Ein Write durchläuft den Live-Namensraum und anschließend die dauerhafte
Reduktionspipeline:

1. SeqCDC-v1 bestimmt inhaltsabhängige Chunkgrenzen.
2. BLAKE3-256 bildet die Chunk-ID und prüft rekonstruierte Bytes.
3. Der Exact Index sucht eine bereits geprüfte Location. Er ist nur eine
   Beschleunigung und keine Wahrheitsquelle.
4. Neue benachbarte Chunks werden zu höchstens 512 KiB großen Compression
   Regions zusammengefasst.
5. Jede Region wird RAW oder als unabhängiger Zstd-Level-3-Record gespeichert.
   Zstd wird nur gewählt, wenn die vollständige Speicherung mindestens 4 KiB
   und 3 Prozent spart.
6. Ein Checkpoint veröffentlicht Container, Manifeste, Exact-Index-Runs und
   den neuen Namespace-Commit in der vorgeschriebenen Reihenfolge.

Gleichförmige allokierte Bereiche werden als FILL gespeichert. Nicht
allokierte Nullbereiche bleiben HOLE und benötigen keinen DATA-Record.

```text
POSIX/FUSE write
      |
      v
Live-Namensraum -> SeqCDC -> BLAKE3 -> Exact Lookup
                                           | Treffer
                                           +----------> vorhandene Location
                                           |
                                           | neu
                                           v
                                      RAW oder Zstd
                                           |
                                           v
                                Container -> Manifest -> Commit-WAL
```

Die Stufen teilen sich ein begrenztes CPU- und Speicherbudget. Dadurch kann
ein einzelner Stream freie Kerne nutzen, ohne bei mehreren Streams unbegrenzt
zusätzliche Arbeit oder Speicher zu erzeugen.

## SeqCDC-v1 und SIMD

SeqCDC-v1 ist das Standardprofil für Write-through-Ingest und
Checkpoint-Rechunking. Die Parameter sind:

| Parameter | Wert |
| --- | ---: |
| Modus | Increasing |
| Sequenzlänge | 6 Bytes |
| Skip-Trigger | 50 Gegenflanken |
| Skip-Länge | 1.024 Bytes |
| Minimale Chunkgröße | 16 KiB |
| Maximale Chunkgröße | 256 KiB |

Auf CPUs mit AVX2 und BMI2 verwendet der Scanner automatisch einen
vektorisierten Kernel. Auf anderen CPUs läuft die skalare Implementierung.
Beide Pfade liefern exakt dieselben Grenzen; Differentialtests prüfen diese
Eigenschaft. Punktuelles `unsafe` ist auf den SIMD-Kernel begrenzt, der sichere
Aufrufer prüft die CPU-Features und Slice-Grenzen.

`FASTDUP_SEQCDC_FORCE_SCALAR=1` schaltet ausschließlich für Diagnose und
Vergleichsmessungen auf den skalaren Pfad. Für den normalen Betrieb ist keine
Umgebungsvariable nötig.

Auf der gemessenen Rocky-ISO erreichte der isolierte AVX2/BMI2-Scanner 8.009
MiB/s und damit das 2,90-Fache des skalaren SeqCDC-Scanners. Im gepaarten
SingleStream-SMB-Test stieg der Median des Gesamtdurchsatzes gegenüber
skalarem SeqCDC um 13,8 Prozent; bei zwei gleichzeitigen Streams waren es
4,4 Prozent. Das sind Ergebnisse eines bestimmten Hosts und keine
Kapazitätszusage. Aufbau, Rohdaten, Streuung und Einschränkungen stehen im
[SeqCDC-Benchmark](docs/benchmarks/seqcdc-prototype-2026-08-22.md).

Der Wechsel auf SeqCDC-v1 änderte die Policy- und Exact-Index-Profilidentitäten.
Ältere FastCDC-Prototyp-Repositories sind absichtlich inkompatibel. Die
Entscheidung ist in [ADR 0054](docs/adr/0054-use-seqcdc-v1-as-the-default-chunking-profile.md)
festgehalten.

## Haltbarkeit und Integrität

Akzeptierte Änderungen sind sofort im Live-Namensraum sichtbar. Ein Checkpoint
friert einen konsistenten Präfix ein und veröffentlicht ihn erst, nachdem DATA,
Metadaten und Commit Record vollständig geschrieben und geprüft wurden. Nach
einem Absturz lädt fastdup die jüngste vollständige Generation. Eine
unterbrochene Datei kann deshalb mit ihrem bereits committeten Präfix
zurückkehren.

Unabhängig davon versucht der Daemon alle 90 Sekunden und beim geordneten
Shutdown, den vollständigen Graphen eines Commit als unveränderlichen Recovery
Checkpoint auf das DATA Tier zu schreiben. Er dient ausschließlich dem Verlust
des kompletten Metadata Tiers. Discovery läuft über zwei feste, verkettete
Head-Slots; Recovery akzeptiert nur einen vollständig geprüften Checkpoint samt
aller erreichbaren DATA-Abhängigkeiten.

Der Standard-Daemon plant etwa alle fünf Sekunden einen Checkpoint. Bei 512
MiB einzigartiger, noch nicht committeter DATA stoppt er kurz die Aufnahme
neuer Mutationen, um die Dirty-DATA-Menge zu begrenzen. `fsync` erzeugt keine
private Transaktion und verschärft die Haltbarkeitsgarantie nicht.

Die dauerhaften Grenzen werden mehrfach geprüft:

- BLAKE3-256 bindet Logical Chunks an ihre Inhalte.
- CRC32C, Header, Footer, physische Länge und Container-Hash prüfen Container.
- Unveränderliche Manifeste beschreiben jede sichtbare Dateiversion.
- Ein verkettetes, gechecksummtes Commit-WAL wählt die Namespace-Generation.
- Der Exact Index darf fehlen oder neu aufgebaut werden, ohne zur
  Inhaltsautorität zu werden.
- Aktivierte Exact-Runs werden nach vollständigem Audit read-only unter einer
  Immutable-File-Lease gemappt. Kompakte Seitengrenzen halten die binäre Suche
  aus I/O und decoded Page Cache heraus; Adapter-Fallback, Publication und
  Offline-Scrub bleiben unabhängige bounded `read_exact_at`-Pfade.
- Scrub prüft erreichbare Generationen, Container und aktive Locations.

Container werden zunächst aufgebaut, vollständig erneut gelesen und geprüft,
mit `fsync` stabilisiert, atomisch ohne Überschreiben veröffentlicht und durch
einen Directory-Sync abgeschlossen. Das Modell setzt einen ehrlichen
Stable-Storage-Stack voraus. RAID, Gerätespiegelung, Snapshots und Schutz gegen
Geräteverlust gehören nicht zu fastdup.

Details stehen im
[Checkpoint-Testplan](docs/testing/durable-posix-checkpoint.md), in der
[Container-Spezifikation](docs/specs/container-v1.md), der
[Metadatenspezifikation](docs/specs/metadata-generation-v1.md) und der
[Exact-Index-Spezifikation](docs/specs/exact-index-run-v1.md). Das DATA-Tier-
Notfallformat beschreibt die
[Recovery-Checkpoint-Spezifikation](docs/specs/recovery-checkpoint-v1.md).

## Bauen und testen

Vorausgesetzt werden Linux, eine aktuelle Rust-Toolchain und für einen echten
Mount ein nutzbares `/dev/fuse`. Alle erzeugten Dateien bleiben unter
`.artifacts`:

```bash
cd /source/fastdup

export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

# Cargo creates CARGO_TARGET_DIR itself, including its CACHEDIR.TAG safety
# marker. Do not pre-create that directory; otherwise `cargo clean` may refuse
# to remove the regenerable cache.
mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$TMPDIR"

cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p fastdup-appliance
```

## Lokalen Mount starten

Der Mountpunkt muss als Verzeichnis bestehen; fehlende Metadata- und
Containerwurzeln legt der Daemon nach erfolgreicher Start-Policy-Prüfung an.
Metadata und DATA dürfen nicht dasselbe Verzeichnis sein. Für einen lokalen
Funktionstest reichen getrennte Unterverzeichnisse; repräsentative Messungen
sollten getrennte Metadata- und DATA-Geräte nutzen.

```bash
mkdir -p \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers

/source/fastdup/.artifacts/target/release/fastdup-durable-fuse \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers
```

Der Daemon läuft im Vordergrund. `Ctrl-C` stoppt die Mutationsannahme, führt
den abschließenden Checkpoint aus und hängt den Mount aus.

Advanced Reduction ist opt-in. Alle neuen Repositories verwenden bereits vom
ersten Commit an das aktuelle Policy Set. Zuerst wird bei beendetem Daemon ein
kohärentes Indexpaar aufgebaut, danach wird die optionale Prefix-Auswahl
aktiviert:

```bash
BIN=/source/fastdup/.artifacts/target/release
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN/fastdup-maintenance" --offline rebuild-pool-indexes "$META" "$DATA"
FASTDUP_ADVANCED_REDUCTION=prefix-v1 \
  "$BIN/fastdup-durable-fuse" \
  /source/fastdup/.artifacts/mount "$META" "$DATA"
```

Fehlt das Paar oder passt seine Exact-Bindung nicht, bleibt der Write-Pfad
verfügbar und fällt auf unabhängiges RAW/Zstd zurück. Ein Repository-Head darf
nur mit genau dem aktuellen Policy Set geöffnet werden;
Prototype-Repositories mit einer anderen Policy-ID werden nicht migriert.

Für den produktiven No-Swap-Betrieb muss der Daemon in einer eigenen
cgroup-v2 mit `MemorySwapMax=0` laufen. Mit
`FASTDUP_REQUIRE_CGROUP_NO_SWAP=1` prüft er diese Kernel-Grenze noch vor dem
Öffnen von Metadata und DATA. Der gemeinsame `MemoryBudgetGovernor` passt die
rebuildbaren Cache-Budgets an den kleineren Host-/cgroup-Headroom an; bereits
belegter Host- oder Shared-cgroup-Swap schaltet die fastdup-Caches nicht ab.
Nur Swap des Daemon-Prozesses schließt ihre Admission.
Langlebige, dichte Dedup-Bloom-Tabellen ab 2 MiB erhalten eine eigene
`MADV_HUGEPAGE`-Arena, ohne die Bloom-Probe-Hot-Loop um Sampling oder Locks zu
erweitern. Details und Abnahmekriterien stehen in
[Memory and swap containment](docs/operations/memory-and-swap.md).

## Offline-Wartung

Vor Wartungsarbeiten muss der fastdup-Daemon beendet und der Mount ausgehängt
sein. `--offline` ist deshalb verpflichtend.

```bash
BIN=/source/fastdup/.artifacts/target/release/fastdup-maintenance
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN" --offline scrub "$META" "$DATA"
"$BIN" --offline metadata-gc "$META" "$DATA"
"$BIN" --offline rebuild-exact "$META" "$DATA"
"$BIN" --offline rebuild-pool-indexes "$META" "$DATA"
"$BIN" --offline scrub-gc "$META" "$DATA"
```

`scrub-gc` koppelt Garbage Collection an einen erfolgreichen Scrub und den
beobachteten Füllstand des DATA-Geräts. Ablauf und Ausgaben beschreibt die
[Wartungsanleitung](docs/operations/scrub-and-exact-index-rebuild.md).

Bei laufendem Daemon kann ein sofortiger, vollständig vom Daemon koordinierter
Online-GC-Durchlauf angefordert werden:

```bash
"$BIN" --online gc-now "$META"
```

Der Appliance Lease verhindert, dass derselbe Repository-Stand gleichzeitig
über den Offline-Pfad geöffnet wird.

## SMB und Samba

Eine Samba-Freigabe kann auf dem FUSE-Mount liegen. Das experimentelle Modul
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) ergänzt Duplicate Extents
und Integrity FSCTLs für Fast-Clone-Clients. Es übersetzt einen gültigen
`FSCTL_DUPLICATE_EXTENTS_TO_FILE`-Aufruf in genau einen Linux-
`copy_file_range`-Aufruf und fällt bei Fehlern nicht unbemerkt auf eine
gepufferte Kopie zurück.

Das Modul ist noch nicht für Veeam Fast Clone freigegeben. Es fehlen reale
Veeam-Traces, breitere Protokolltests und weitere Lock-, Alignment- und
Fehlerfälle. Der portable Contract-Test läuft mit:

```bash
sh samba/vfs_fastdup/tests/run.sh
```

## Workspace

| Komponente | Aufgabe |
| --- | --- |
| `fastdup-format` | Versionierte Container-, Manifest-, Commit- und Exact-Index-Bytes |
| `fastdup-store` | SeqCDC, Reduktion, Container, Exact Index, Scrub und GC |
| `fastdup-io-uring` | Erforderlicher Linux-`io_uring`-Pfad für begrenzte parallele Container-I/O |
| `fastdup-posix` | POSIX-Modell, Live-Dirty-Overlay und Low-Level-FUSE-Adapter |
| `fastdup-appliance` | Ingest, Checkpoints, Recovery und ausführbare Programme |
| `fastdup-copy-metrics` | Günstige Hot-Path- und Kopiertelemetrie |
| `fastdup-exact-bench` | Reproduzierbares A/B für aktivierte Exact-Lookups über mmap und bounded Reads |
| `fastdup-testkit` | Deterministische Fehler, Crash-Modell und Corpus-Werkzeuge |
| `samba/vfs_fastdup` | Experimentelles Samba-VFS-Modul für Fast Clone |

## Grenzen

Vor einem produktiven Einsatz fehlen insbesondere:

- vollständige POSIX-Abdeckung und breitere Client-Kompatibilität
- Schutz vor Geräteverlust
- Langzeit-, Zufalls-Kill- und echte Stromausfalltests auf Blockgeräten
- breitere versionierte Backup-Corpora und ein belastbares GC-/Restore-Gate,
  bevor Similarity und Zstd-PREFIX zum Default werden
- dauerhafte Writer-, Recovery-, Scrub- und GC-Invarianten für
  Dictionary-Encodings; Sparse-XOR-Delta und Reorder bleiben experimentell
- Veeam-Protokollevidenz für das Samba-Modul

Messwerte sind workload- und hostabhängig. Reproduzierbare Methoden und
Einschränkungen liegen unter [docs/benchmarks](docs/benchmarks/), Testpläne
unter [docs/testing](docs/testing/) und Betriebsnotizen unter
[docs/operations](docs/operations/).

Der aktuelle reale Online-GC-Interferenzlauf ist unter
[docs/benchmarks/online-gc-interference-2026-08-26.md](docs/benchmarks/online-gc-interference-2026-08-26.md)
dokumentiert. Der opt-in Prefix-ABBA-Lauf steht unter
[docs/benchmarks/persistent-prefix-smb-ab-2026-08-27.md](docs/benchmarks/persistent-prefix-smb-ab-2026-08-27.md).
Die Neubewertung des verworfenen Container-Formats 3 nach dem Governor-Fix ist
unter [docs/benchmarks/container-format-v3-gc-reevaluation-2026-08-27.md](docs/benchmarks/container-format-v3-gc-reevaluation-2026-08-27.md)
dokumentiert.
Der kalte A/B-Test für Restore-Coalescing steht unter
[docs/benchmarks/verified-restore-coalescing-2026-08-27.md](docs/benchmarks/verified-restore-coalescing-2026-08-27.md).
