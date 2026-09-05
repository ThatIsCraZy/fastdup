# Online-Similarity-Index ohne Remount und Pool-Rebuild

Stand: 2026-09-02

## Kurzantwort

Ja, fastdup kann Similarity im laufenden Betrieb ungefähr so fortschreiben wie
den Exact Index. Die passende Form ist aber **keine einzelne, frei mutierte
On-Disk-Hash-Tabelle**, sondern ein integrierter inkrementeller Index:

- ein begrenzter, nach `BucketKey` geshardeter Publikationspuffer;
- ein kleines, crash-replaybares Journal;
- immutable Similarity-L0-Runs und begrenzte Hintergrund-Compaction;
- eine atomar austauschbare `ReductionVersion`, die einen vollständigen
  Similarity-Run-View und den dazu passenden Exact Run Set gemeinsam hält;
- nur operationslange Generation-Leases statt eines mount-langen Pins.

Der für ADR 0089 gewählte Puffer ist nicht querybar. Neue Kandidaten werden
erst mit dem nächsten kleinen L0 sichtbar. Das vermeidet einen zweiten
Lookup-Pfad und erkauft diese Vereinfachung nur mit begrenzter Sichtlatenz.

Der entscheidende Unterschied zu einem naiven LSM ist der Wert:

```text
BucketKey -> vollständiger BucketState mit höchstens 64 Chunk IDs
```

Ein Run enthält also nicht einzelne zusätzliche Kandidaten, die bei jeder
Suche über alle Runs vereinigt werden müssten. Er enthält für jeden geänderten
Bucket dessen **vollständig materialisierten neuesten Zustand**. Eine Suche
findet den neuesten Wert für jeden der vier BucketKeys und untersucht weiterhin
höchstens `4 * 64 = 256` Repräsentanten. Damit bleibt die zentrale Schranke aus
[ADR 0018](../adr/0018-bound-and-version-similarity-search.md) erhalten.

Wenn „ohne Online-Overlay“ wörtlich bedeutet, dass es überhaupt keinen
mutablen, sofort abfragbaren Frontteil geben darf, muss ein neuer Fingerprint
bis zur nächsten L0-Publikation unsichtbar bleiben. Genau diese Variante wählt
ADR 0089. Journal, Batch Builder und Runs sind Lebenszyklusphasen **eines**
Similarity-Index; nur atomar aktivierte immutable Runs werden abgefragt.

## Was die Primärquellen tatsächlich zeigen

### LevelDB und RocksDB: kontinuierliche Updates ohne Reopen

LevelDB schreibt jede neue Mutation zunächst in ein Log und eine Memtable.
Nach ungefähr 4 MiB wird die Memtable im Hintergrund als immutable SSTable nach
L0 geschrieben; weitere Writes laufen bereits in einer neuen Memtable weiter.
Die MANIFEST-Datei beschreibt, welche SSTables den aktuellen Zustand bilden.
Recovery lädt das MANIFEST, spielt den Log-Tail ein und beginnt mit einem neuen
Log. Das ist der direkte First-Party-Beleg dafür, dass ein Online-Index weder
einen Remount noch einen vollständigen Rebuild pro Aktualisierung benötigt:
[LevelDB implementation notes](https://github.com/google/leveldb/blob/main/doc/impl.md).

RocksDB trennt die beiden Crash-Probleme explizit:

- Der WAL rekonstruiert nach einem Crash die noch nicht geflushten
  Memtable-Mutationen. Standardmäßig geht jede Mutation in Memtable und WAL:
  [RocksDB WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29).
- Das MANIFEST ist ein transaktionales Log der On-Disk-Versionen. Neue oder
  entfernte SSTs werden als Version Edits publiziert; unvollständige atomare
  Gruppen werden bei Recovery nicht teilweise angewendet:
  [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST).

RocksDBs `SuperVersion` bündelt die aktuelle SST-Version mit den lebenden
Memtables. Flush, Compaction oder Memtable-Wechsel erzeugen eine neue
SuperVersion; laufende Reads dürfen die alte weiterbenutzen, bis ihre Referenz
fällt. Danach kann sie eingesammelt werden:
[RocksDB terminology](https://github.com/facebook/rocksdb/wiki/Terminology) und
[live SST file tracking](https://github.com/facebook/rocksdb/wiki/How-we-keep-track-of-live-SST-files).
Das ist fast genau die benötigte Online-Ablösung des mount-langen
`PersistentReductionIndex`.

Die Kosten verschwinden dabei nicht. L0-Dateien überlappen und müssen begrenzt
werden. Ab L1 sind Schlüsselbereiche innerhalb eines Levels disjunkt, sodass
für einen Punkt-Lookup höchstens eine Datei pro Level infrage kommt. RocksDB
startet L0-Compaction ab einem festen Dateilimit und kann Writes bremsen oder
stoppen, wenn Compaction nicht nachkommt:
[leveled compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction)
und [write stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls).
Für fastdup sollte nur die **Similarity-Aufnahme** gedrosselt oder ausgelassen
werden, nicht der Nutzdaten-Write: Similarity ist nicht autoritative
Beschleunigung.

### Linux VDO/UDS: der nächstliegende Storage-Index

VDO behandelt den Dedup-Index ausdrücklich als ungenauen Hinweis und verifiziert
den vorgeschlagenen physischen Block vor dem Teilen. Veraltete Einträge sind
damit sicher; sie kosten nur Trefferqualität. Genau dieselbe Trennung hat
fastdup zwischen Similarity Candidate, Exact-Auflösung und verifiziertem Base
Chunk:
[Linux dm-vdo design](https://www.kernel.org/doc/html/latest/admin-guide/device-mapper/vdo-design.html#the-deduplication-index).

UDS fügt Records online in ein mutables `open chapter` ein. Wenn es voll ist,
wird es in ein read-optimiertes, danach unverändertes Chapter überführt und ein
neues open chapter begonnen. Alte Chapters rotieren aus einem festen
Dedup-Fenster. Ein `volume index` zeigt auf das neueste Chapter für einen Hash;
der Lookup benötigt dadurch höchstens eine Chapter-Index-Seite und eine
Record-Seite. Geschlossene Chapters werden nie in place geändert. Diese
Architektur belegt drei für fastdup wichtige Punkte:

1. Ein Hint-Index darf online und näherungsweise sein.
2. Ein begrenztes „recent window“ kann sinnvoller sein als vollständige
   Pool-Abdeckung.
3. Mutable Front + immutable Segmente ist auch bei produktiver Inline-Dedup die
   übliche Form, nicht ein periodischer Voll-Rebuild.

UDS ist aber kein vollständiges Crash-Protokoll-Vorbild für fastdup: Laut
Kernel-Dokumentation lebt der Volume Index vollständig im RAM und wird erst
beim Shutdown gespeichert. Seine Chapter-/Fenster-Geometrie und der begrenzte
Lookup sind übertragbar; die fastdup-Publikation sollte stattdessen die bereits
vorhandenen fsync-, Aktivierungs- und Fault-Injection-Regeln verwenden.

### OpenZFS: warum die direkte On-Disk-Hash-Tabelle nicht automatisch besser ist

Der klassische OpenZFS-DDT ist eine On-Disk-Hash-Tabelle auf Basis des
extensible-hashing ZAP. Er muss für jeden dedup-fähigen Write und Free
konsultiert werden:
[OpenZFS workload tuning](https://openzfs.github.io/openzfs-docs/Performance%20and%20Tuning/Workload%20Tuning.html#deduplication).
Die offizielle Dokumentation warnt entsprechend vor hohem RAM- und I/O-Bedarf:
[OpenZFS deduplication](https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Data%20Storage/Deduplication.html).

Fast Dedup ergänzt genau deshalb einen DDT-Log. Geänderte Einträge werden am
Ende eines TXG in einen speicherresidenten Baum übernommen und append-only auf
Disk geloggt. Lookups prüfen diesen Baum zuerst; im Hintergrund wird ein
begrenzter Anteil in den eigentlichen ZAP-DDT geflusht. Beim Pool-Import wird
das On-Disk-Log wieder in RAM geladen. Die Motivation nennt ausdrücklich den
teuren On-Disk-Table-Update für jeden Write:
[OpenZFS FDT-log change](https://github.com/openzfs/zfs/pull/15895).
Die aktuelle Implementierung hält zwei Log-Bäume (`active` und `flushing`),
wechselt sie nach Speicher-/Zeitgrenzen und lädt beide bei Import wieder:
[OpenZFS `ddt_log.c`](https://github.com/openzfs/zfs/blob/master/module/zfs/ddt_log.c).
Die offiziellen Parameter begrenzen unter anderem Log-RAM, TXGs bis zum Flush
und Einträge pro Transaktion:
[OpenZFS DDT-log parameters](https://openzfs.github.io/openzfs-docs/Performance%20and%20Tuning/Module%20Parameters.html#zfs-dedup-log-cap).

Damit ist OpenZFS gerade kein Argument für „einfach die Hash-Tabelle direkt
ändern“, sondern ein gemessener Gegenbeleg: Bei hoher Mutationsrate wurde der
direkte Pfad durch Journal, Batching und inkrementellen Flush ergänzt.

### Lucene: Online-Segmentpublikation und kurze Reader-Leases

Lucene erzeugt beim Schreiben neue Segmente; der Kern eines Segments ist
immutable. Jeder `DirectoryReader` hält eine konsistente Point-in-Time-Sicht und
kann mit `openIfChanged` ohne Prozessneustart auf eine neue Sicht wechseln:
[Lucene index package](https://lucene.apache.org/core/10_2_1/core/org/apache/lucene/index/package-summary.html).
`SearcherManager` tauscht die aktuelle Sicht periodisch aus und schließt eine
alte erst, wenn alle Threads sie freigegeben haben:
[Lucene SearcherManager](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/SearcherManager.html).
Durable Commits werden durch eine neue `segments_N`-Generation ausgewählt; die
höchste vollständige Generation ist aktiv:
[Lucene SegmentInfos](https://lucene.apache.org/core/10_1_0/core/org/apache/lucene/index/SegmentInfos.html).

Das ist das passende Vorbild für **Refresh ohne Remount**: atomar eine neue
Reader-Sicht publizieren, neue Operationen darauf lenken und alte Segmente erst
nach Ablauf ihrer Referenzen löschen. Lucene ist dagegen kein passender
Similarity-Datenaufbau; dafür bleibt fastdups Bucket-Semantik maßgeblich.

## Warum Exact und Similarity nicht dieselbe Tabellenzeile sind

Exact ist im Wesentlichen ein Point-Lookup:

```text
(ChunkId, logical_length) -> Location transitions
```

Similarity ist ein begrenztes Multi-Value-Problem:

```text
(profile, slot, logical_length, superfeature) ->
    die 64 kleinsten vollständigen ChunkIds
```

Ein naiver Similarity-LSM, in dem jeder Run bis zu 64 Vertreter pro Bucket
enthält, würde bei `R` Runs bis zu `4 * 64 * R` Vertreter untersuchen. Das
verletzt die bestehende 256er-Schranke und lässt Query-Kosten mit dem Run-Fanout
wachsen.

Darum muss die LSM-Zeile ein **Replacement Value** sein: der komplette neueste
64er-Bucket. Beim ersten Update eines Buckets im serialisierten Batch Builder
liest der Publisher dessen aktuellen effektiven Wert, fügt die neuen
unabhängigen Chunk IDs ein, sortiert deterministisch und behält die kleinsten
64. Weitere Updates desselben Batches verschmelzen im Puffer. Der spätere
L0-Run publiziert diesen vollständigen Wert. Lookup prüft immutable Runs von
neu nach alt und decodiert nur den ersten gefundenen Wert; ältere Werte
desselben BucketKeys werden nicht vereinigt.

Die Run-Probes müssen trotzdem begrenzt sein. Eine konkrete v1-Policy sollte
festlegen:

- höchstens einen aktiven und einen eingefrorenen Publikationsbatch;
- höchstens vier überlappende L0-Familien;
- eine feste maximale Levelzahl;
- ab L1 disjunkte BucketKey-Bereiche;
- Bloom/Fuse-Negative nur als Beschleunigung, nie als Autorität;
- bei ausgeschöpftem Similarity-Backlog: Similarity-Aufnahme auslassen und
  telemetrieren, niemals den FUSE-Write unbeschränkt blockieren.

Damit ist die Anzahl der Index-Probes konstant durch die Policy begrenzt. Die
Zahl der tatsächlich decodierten Repräsentanten bleibt unverändert bei 256.

## Empfohlener fastdup-Lebenszyklus

### 1. Aufnahme

Nur ein vollständig publizierter, unabhängig decodierbarer Chunk darf als Base
aufgenommen werden. Nach dessen Exact-L0-Publikation erzeugt der Writer seine
vier Bucket-Mutationen. Dependent-only Chunks werden wie heute ausgeschlossen.

Die Mutationen gehen in einen nach BucketKey geshardeten
`SimilarityPublicationBatch`. Er ist kein Candidate-Lookup-Pfad. Ein
`SimilarityIndexView::lookup_bucket()` kapselt ausschließlich den atomar
aktivierten immutable Run-Satz; der Reduction-Pfad kennt dessen Schichten
nicht.

### 2. Crash-Schutz des Tails

Die einfachste robuste Variante ist ein kleines, checksummed append-only
Similarity-Journal mit monotoner Sequenz und Exact-Aktivierungsbindung. Es
enthält die vorab berechnete Similarity Entry beziehungsweise die vier
Bucket-Mutationen, nicht Payloadbytes. Es macht den Tail nach einem Crash erneut
publizierbar, aber nicht vorzeitig querybar. Ein verloren gegangener, noch nicht
synchronisierter Tail verschlechtert nur Reduction und darf niemals
Nutzdaten-Durability blockieren.

Alternativ könnte Recovery neue Exact-L0-Einträge seit einem durable
Similarity-Watermark erneut lesen und fingerprinten. Das vermeidet ein eigenes
WAL, verlangt aber, dass die betreffenden Exact-Familien bis zur Verarbeitung
gepinnt bleiben, und macht Recovery-I/O vom Tailvolumen abhängig. Für v1 ist
das explizite kleine WAL einfacher und besser begrenzbar.

### 3. Flush und inkrementelle Compaction

Bei RAM-, Zeit- oder Eintragsgrenze wird der aktive Batch eingefroren und
sofort durch einen leeren ersetzt. Der Hintergrund-Writer erzeugt einen
page-checksummed L0-Run, liest ihn vollständig zurück, synchronisiert Datei und
Verzeichnis und publiziert erst danach eine neue Similarity-Version.

Compaction arbeitet nur auf ausgewählten, überlappenden Runs. Für denselben
BucketKey gewinnt der neueste vollständige BucketState; es ist kein Poolscan
und kein Rebuild. Erst nach dauerhafter Aktivierung der Ausgabe und Ablauf aller
alten Leases dürfen Eingabe-Runs entfernt werden.

### 4. Atomare Online-Aktivierung

Ein `ReductionVersion`-Objekt sollte enthalten:

```text
ReductionVersion {
    version_id,
    exact_run_set_id,
    similarity_version_id,
    similarity_wal_incorporated_sequence,
    policy/profile ids
}
```

Publikationsreihenfolge:

1. unabhängige DATA und Exact-L0-Zustände vollständig publizieren;
2. Similarity-L0/Compaction-Ausgaben schreiben, auditieren und synchronisieren;
3. Similarity-Version-Manifest schreiben und synchronisieren;
4. einen hash-verketteten Reduction-Aktivierungsrecord schreiben, rereaden,
   prüfen und synchronisieren;
5. erst danach den aktuellen `Arc<ReductionVersion>` atomar austauschen.

Ein Crash vor Schritt 4 lässt die alte vollständige ReductionVersion aktiv;
ein Crash danach wählt die vollständige neue. Ein zwischenzeitlich neuerer
Exact Index ohne passende Similarity-Aktivierung ist sicher und weiterhin für
Exact Dedup nutzbar. Das entspricht der bereits akzeptierten Reihenfolge aus
[ADR 0062](../adr/0062-build-exact-and-similarity-from-one-verified-scan.md),
nur für inkrementelle Runs statt einen Pool-Rebuild.

RocksDBs MANIFEST könnte dieses Problem allgemein lösen, aber fastdup braucht
dafür keine eingebettete Datenbank. Die vorhandene Exact-Aktivierungslogik mit
paired slots, vollständigem Dependency-Audit und old-or-complete-new Recovery
ist bereits der passendere lokale Baustein.

### 5. Online-Adoption und Leases

Jede neue Reduction-Operation lädt genau einmal den aktuellen
`Arc<ReductionVersion>`. Laufende Operationen behalten die alte Version. Neue
Operationen sehen sofort die neue. Der Similarity-Pin sollte nur bis zum Kopieren
der höchstens 16 ausgewählten Chunk IDs leben; die Exact-Generation bleibt nur
für die höchstens vier Base-Reads gepinnt. Nach dem Kopieren der Basebytes kann
auch sie fallen.

Damit blockiert ein offener Mount keine Aktualisierung und alte mmaps leben nur
so lange wie echte Queries. Telemetrie sollte Alter und Anzahl ausstehender
Leases melden. Ein aktiver Lease darf nicht gewaltsam ablaufen; bei ungewöhnlich
langen Leases wird nur Reclamation verschoben. Neue Similarity-Aufnahme kann
bei zu viel nicht reclaimbarem Altbestand degradiert werden.

## Deletes, GC und die 64-kleinsten-Regel

Insert-only ist exakt inkrementell: Aus dem bisherigen Top-64-Zustand und neuen
IDs lässt sich der neue Top-64-Zustand vollständig bestimmen.

Deletes sind grundsätzlich schwieriger. Wenn eine der 64 IDs verschwindet,
kennt ein nur auf 64 Vertreter begrenzter Index nicht automatisch die
65.-kleinste, zuvor verworfene ID. Drei Möglichkeiten bestehen:

1. Stale IDs als sichere Hints behalten; Exact-Auflösung verwirft sie. Das ist
   für v1 empfohlen.
2. Intern mehr als 64 IDs als Reserve halten, aber nur 64 abfragen. Das ändert
   Speicherformat und Representative Profile.
3. Betroffene Bucketbereiche lokal aus Container-/Exact-Evidenz neu aufbauen.
   Das ist ein regionaler Repair, kein regelmäßiger Pool-Rebuild.

VDO bestätigt die Sicherheitsseite dieses Ansatzes: veraltete Indexhinweise
werden toleriert und die bezeichneten Daten vor Nutzung verifiziert. Für
fastdup sollte eine hohe `stale_candidate_reject`-Rate einen lokalen
Bucket-Repair oder das Auslassen des Buckets auslösen.

## Bewertung der Alternativen

| Ansatz | Kein Remount | Kein Voll-Rebuild | Query begrenzbar | Crash-Sicht | Hauptproblem |
| --- | --- | --- | --- | --- | --- |
| Mutable In-place Hash-Tabelle | ja | im Normalfall | ja | WAL, Doppelseiten oder COW nötig | zufällige Metadatenwrites, Resize, Reader/Writer-Synchronisation |
| Naiver LSM mit Kandidaten-Deltas | ja | ja | Run-Probes ja, Repräsentanten nein | WAL + Manifest bewährt | verletzt `256 representatives examined` |
| Vollständiger BucketState in LSM | ja | ja | ja | WAL + atomare Version | begrenzte Compaction bleibt nötig |
| VDO-artige Chapters/Fenster | ja | ja | ja | fastdup-Protokoll zusätzlich nötig | bewusst unvollständiges Zeitfenster |
| Periodische Voll-Snapshots | erst nach Adoption | nein | ja | vorhandenes Protokoll | zu teuer und zu spät sichtbar |

Ein In-place-Hash-Ansatz ist nur dann attraktiver, wenn die Messung zeigt, dass
seine zufälligen Page-Writes, Journaling- und Resize-Kosten unter der
inkrementellen Run-Compaction liegen. Er beseitigt weder das Journal noch die
Notwendigkeit einer atomaren Sicht für parallele mmap-Reader. COW-Seiten machen
ihn faktisch wieder zu einer versionierten Struktur; direkte Seitenmutation
verträgt sich nicht mit den bestehenden immutable-file Leases aus
[ADR 0061](../adr/0061-map-immutable-similarity-runs-under-generation-leases.md).
OpenZFSs Wechsel zu einem DDT-Log ist der stärkste praktische Hinweis, dass
„direkt in die Hash-Tabelle“ bei Inline-Storage nicht der Default sein sollte.

## Klare Empfehlung für fastdup

ADR 0089 sollte auf **Online Similarity LSM mit vollständig materialisierten
Bucketwerten und operationslangen ReductionVersion-Leases** zugespitzt werden.
Das ist tatsächlich „wie unsere Dedup-Tabelle“ auf Lebenszyklus-Ebene:

- derselbe Publisher-/Run-/Activation-/Lease-Rahmen wie Exact;
- Similarity-spezifische Zeilenform `BucketKey -> BucketState64`;
- einen integrierten, nicht querybaren Batch Builder statt eines Overlay-Index;
- ein bounded WAL statt Recovery-Vollscan;
- inkrementelle Compaction statt regelmäßiger Rebuilds;
- atomarer `Arc`-Swap statt Remount;
- sichere Degradation auf Independent Encoding bei jedem Indexfehler oder
  Backlog.

Vor Implementierung sollten zwei kleine Prototypen gegeneinander gemessen
werden:

1. `BucketState64`-LSM mit nicht querybarem Batch Builder, vier L0-Familien und
   beispielsweise sechs festen Levels;
2. geshardete Doppelseiten-Hash-Tabelle mit WAL und COW-Resize.

Die Gates sollten Metadata-Tier-Write-Amplification, p99-Write-Latenz,
Similarity-Lookup-p99, Recoveryzeit des maximalen WAL-Tails, aktive Run-Probes,
Compaction-Backlog und physisch erzielte Similarity-Ersparnis messen. Nach dem
Linux-Kernel-Ergebnis von nur 0,0668 % zusätzlicher Allokationsersparnis darf
die Online-Pflege nicht mehr I/O kosten, als der Codec physisch einspart.

## Erforderliche Fault- und Invariant-Gates

- Crash vor/nach jeder Run-, Manifest- und Activation-Synchronisation: nur alte
  oder vollständige neue ReductionVersion wird gewählt.
- WAL-Torn-Tail: nur vollständige checksummed Records werden gespielt; ein
  unbrauchbarer Similarity-Tail deaktiviert Hinweise, nie DATA.
- Exact-vor-Similarity: kein Candidate wird publiziert, bevor der gebundene
  Exact Run Set ihn auflösen kann.
- L0-/Level-Grenzen: Query-Probes und decoded representatives bleiben unter den
  Policy-Limits, auch wenn Compaction absichtlich blockiert wird.
- Lease-Race: alte mmaps bleiben bis zur letzten Operation erhalten; erst dann
  dürfen Runs entfernt werden.
- Compaction-Race: Writer-Reread und Audit vor Aktivierung; alte oder komplette
  neue Run-Version, nie ein gemischter Satz.
- Stale Candidate: Exact miss, retiring oder dependent-only Location führt
  sicher zu einem anderen Candidate beziehungsweise Independent Encoding.
- Determinismus: Reihenfolge, Batchgrenzen und Workerzahl erzeugen denselben
  BucketState64 für dieselbe Insert-Menge.
- Backpressure: erschöpfter Similarity-Budgetpfad blockiert keine Nutzdaten-
  Durability; er zählt ausgelassene Einträge und degradiert sichtbar.

## Präzisierungen aus der Implementierung

Der implementierte Stand folgt dem Immutable-L0-/BucketState64-Vorschlag,
präzisiert jedoch drei Punkte gegenüber der ursprünglichen Recherche:

- Jede Exact-Aktivierung schließt neue Pins auf dem alten Snapshot. Deshalb
  behalten Queries den immutable Similarity-Stand und pinnen anschließend
  den aktuellen Exact-Stand. Jeder Basiskandidat bleibt ein geprüfter Hint;
  ein langfristig gebundenes Exact-Paar wäre hier ungeeignet.
- Ein zusätzlicher Hint-WAL entfällt. Zwei wartende Batches und ein Worker
  sind begrenzt; Sichtbarkeit beginnt erst nach dauerhaft aktiviertem L0.
  Überlast oder Crash dürfen Kandidaten verlieren, nicht Dateiinhalte.
- Die Kompaktierung ist zunächst tiered mit Fan-in vier, höchstens 24
  Familien und vollständigen neuesten Bucket-Werten. Sie liest keine DATA.
  Ein kurzer Schreib-Publikationsschutz verhindert GC-Retirement zwischen
  Basiswahl und Exact-Aktivierung des abhängigen Ziel-Chunks; laufendes GC
  führt zu Independent-Fallback statt Writer-Warten.

Zusätzlich lässt sich die Writer-Policy pro Share live überschreiben. Exact
und normale Kompression bleiben global verfügbar. Details und verbleibende
Realgeräte-Performancequalifikation stehen in
[ADR 0089](../adr/0089-refresh-reduction-snapshots-without-remounting.md) und
[der Formatbeschreibung](../formats/online-similarity-head-v1.md).
