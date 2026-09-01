# Belastbare Performance- und Reduction-Aussagen für die Website

Datum: 2026-09-02  
Status: Recherchegrundlage, kein unabhängiger Produktvergleich und keine SLA

## Kurzurteil

Die Website kann fastdup belastbar als Software darstellen, die handelsübliche
x86-64-Hardware in eine deduplizierende POSIX-/SMB-Appliance verwandelt. Die
stärkste aktuelle End-to-End-Zahl ist **1.022,1 MiB/s über drei serielle
SMB-Uploads** eines Rocky-Linux-ISO; der erste, physisch neue Upload erreichte
**601,0 MiB/s**, die beiden Exact-Dedup-Wiederholungen **1.570,1 und
1.576,2 MiB/s**. Die drei gleichzeitig live gehaltenen Kopien erreichten ein
Verhältnis von **3,104:1 zwischen logischen Daten und allokiertem
Repository-Speicher** (**67,78 % Einsparung**).
[SingleStream-Audit](../benchmarks/hot-buffer-reuse-2026-09-01.md#singlestream-smb-ab)

Der Messhost war eine **Hyper-V-VM mit zehn sichtbaren logischen CPUs** auf
einem **Intel Core i7-1370P** und getrennten virtuellen XFS-Geräten für
Metadaten und DATA. Intel klassifiziert den i7-1370P als Mobile-Prozessor mit
14 physischen Kernen und 20 Threads. Deshalb ist „10 vCPUs auf einem
Notebook-Prozessor“ korrekt; „10-Kern-CPU“ wäre es nicht.
[fastdup-Hostbeschreibung](../benchmarks/exact-lookup-mmap-2026-08-27.md),
[Intel-Prozessorspezifikation](https://www.intel.com/content/www/us/en/products/sku/232146/intel-core-i71370p-processor-24m-cache-up-to-5-20-ghz/specifications.html)

Die sichere Vergleichsaussage lautet: **Dell und HPE beschreiben den
öffentlich dokumentierten Reduktionspfad primär als Deduplizierung plus
Kompression; fastdup veröffentlicht eine transparente, mehrstufige Pipeline,
die darüber hinaus Sparse-/FILL-Repräsentation, metadata-only Clone-Reuse und
optional Similarity-geführte Depth-1-Zstd-Prefixes umfasst.** Das belegt einen
breiter *dokumentierten* Stack, nicht, dass die Hersteller intern keine
weiteren Verfahren besitzen oder dass fastdup universell besser reduziert.

## Geeignete Messwerte

### Aktueller SingleStream-SMB-Lauf

Der bestandene Lauf vom 1. September 2026 verwendete den aktuellen
Produktionspfad, ein 2.072.444.928-Byte großes Rocky Linux 10.2 Minimal ISO,
drei serielle Uploads in ein frisches Repository und zwölf Sekunden Settle.
Metadata lagen auf `/dev/sdb`, Container auf `/dev/sdc`; beide waren getrennte
XFS-Dateisysteme.

| Website-Metrik | Exakter Messwert |
| --- | ---: |
| erster Upload mit neuer Datenpublikation | 601,0 MiB/s |
| zweite Exact-Dedup-Kopie | 1.570,1 MiB/s |
| dritte Exact-Dedup-Kopie | 1.576,2 MiB/s |
| Gesamtrate, drei serielle Uploads | 1.022,1 MiB/s |
| logische Daten | 6.217.334.784 Bytes |
| allokiertes Repository inkl. Metadata | 2.003.144.704 Bytes |
| Datenreduktion | 3,1038x / 67,781 % |
| Peak-RSS / Prozess-Swap | 569,5 MB / 0 Bytes |

Primärbeleg: [Hot-Buffer-Reuse-Audit](../benchmarks/hot-buffer-reuse-2026-09-01.md#singlestream-smb-ab).
Die lokalen Schema-v3-Rohdaten liegen in
`.artifacts/benchmarks/smb-single-stream-buffer-reuse-final-repeat.json` und
sind absichtlich nicht Teil des Repositorys.

Sichere Kurzform für die Website:

> Gemessen in einer 10-vCPU-VM auf einem Intel Core i7-1370P: 601 MiB/s beim
> ersten ISO-Upload, bis zu 1.576 MiB/s bei nachfolgenden Exact-Dedup-Kopien
> und 3,104x Datenreduktion über drei gleichzeitig gespeicherte Kopien.

Notwendige Fußnote: ein einzelnes ISO, warme Wiederholungen, virtuelle
Testgeräte, keine zugesicherte Hardwaregrenze. Die 1.022,1 MiB/s sind
logischer Gesamtdurchsatz über **serielle**, nicht parallele Uploads. Der
ältere Guardrail-Lauf mit 1.447,7 MiB/s darf nicht als aktueller Wert
herausgepickt werden: sein Bericht ordnet die Abweichung ausdrücklich
unterschiedlichen Storage-Bedingungen zu.
[Exact-mmap-Guardrail](../benchmarks/exact-lookup-mmap-2026-08-27.md#smb-guardrail)

### Zehnminütiger FUSE-Stresstest

Ein separater 601-Sekunden-Lauf mit 50 deterministisch leicht veränderten
Versionen desselben ISO erreichte im Produktions-FUSE-Pfad **545,3 MB/s
aktive Write-p95**, **922,7 MB/s aktive Read-p95**, **98,311 % Exact-Hit-Anteil**
und **42,95x logische Daten zu allokierten DATA-plus-Metadata-Bytes**. Der
Daemon nutzte dabei im Mittel 1,616 CPUs; die zehn vCPUs wurden also nicht
durchgehend ausgelastet.
[600-Sekunden-Rerun](../benchmarks/io-intensive-fuse-600s.md#incremental-streaming-and-bounded-recovery-600-second-rerun)

Dieser Wert ist ein starker Workload-Beleg, aber **keine allgemeine
Kapazitätszusage**: Die Varianten unterscheiden sich nur durch wenige Bytes,
der finale Namespace ist nach dem Test leer, und der damalige Lauf behielt
unreachable Historie ohne GC-Credit. Für eine prominente Kachel daher
„42,95x im versionierten ISO-Stresstest“ statt bloß „bis zu 42,95x“ schreiben.

### Nur als Engineering-Evidence

Der isolierte SeqCDC-v1-Scanner erreichte auf demselben 10-vCPU-Host in
1-MiB-Slices **9.568 MiB/s mit AVX2/BMI2** gegenüber 6.223 MiB/s skalar.
Das ist ein In-Memory-Chunking-Kernel, kein SMB-, FUSE- oder Device-Durchsatz.
[SeqCDC-Entscheidung](../adr/0054-use-seqcdc-v1-as-the-default-chunking-profile.md)

Der experimentelle In-Memory-Reduction-Engine-Lauf erreichte auf zehn
ISO-Versionen **10,676x Payload-Reduction**. Container-Framing, Indizes,
Alignment, Manifeste und Dateisystemallokation sind dort ausgeschlossen; die
Engine ist keine Appliance-Durchsatzmessung. Diesen Wert höchstens in einer
mit „Research pipeline / payload only“ bezeichneten Engineering-Sektion
verwenden, nicht im Produkt-Hero.
[Reduction-Referenz](../benchmarks/data-reduction-reference-v1.md)

## Welche Reduction-Verfahren wirklich verfügbar sind

| Verfahren | Status | Belastbare Beschreibung |
| --- | --- | --- |
| Sparse Holes | produktiver Standard | Nicht allokierte Dateibereiche bleiben Manifest-Holes und benötigen keinen DATA-Payload. |
| Constant-byte FILL | produktiver Standard | Gleichförmige DATA-Runs werden als FILL-Rezept statt als Chunk-Payload gespeichert. Die v1-Schwelle beträgt 64 KiB. [ADR 0014](../adr/0014-allow-chunking-profiles-per-data-region.md) |
| SeqCDC-v1 | produktiver Standard | Inhaltsabhängige Chunkgrenzen mit 16 KiB Minimum, 64 KiB Ziel und 256 KiB Maximum stabilisieren Exact Dedup gegenüber Einfügungen und Änderungen. [ADR 0054](../adr/0054-use-seqcdc-v1-as-the-default-chunking-profile.md) |
| BLAKE3 Exact Dedup | produktiver Standard | Bereits vorhandene, verifizierte Chunks werden referenziert statt erneut gespeichert; der persistente Index bleibt rebuildbare Beschleunigung. [ADR 0015](../adr/0015-keep-exact-dedup-correct-without-index-authority.md) |
| Compression Grouping + adaptive RAW/Zstd | produktiver Standard | Benachbarte neue Chunks werden in maximal 512-KiB-Regionen gemeinsam bewertet. Zstd wird nur gewählt, wenn der vollständige Record mindestens 4 KiB und 3 % spart; sonst bleibt RAW. [Writer-Policy](../adr/0016-bound-compression-and-reordering.md) |
| Metadata-only range clone / recipe reuse | produktiv, workloadabhängig | `copy_file_range` und die Samba-Fast-Clone-Integration können bestehende immutable Chunk-Rezepte referenzieren, ohne DATA erneut einzulesen oder zu speichern. Die Veeam-Qualifikation ist noch offen. [ADR 0043](../adr/0043-expose-metadata-range-clones-for-veeam-fast-clone.md) |
| Similarity + Depth-1 ZSTD_PREFIX | produktiv, **opt-in** | Ein kohärenter Exact-/Similarity-Snapshot liefert höchstens vier Base-Trials; Prefix gewinnt nur mit mindestens 4 KiB und 5 % Vorteil gegenüber RAW/Zstd. Default des RPM ist `off`. [ADR 0063](../adr/0063-pin-a-coherent-reduction-snapshot-for-write-through.md), [`repository.env`](../../packaging/fastdup/repository.env) |
| Family-Zstd-Dictionaries | Research/Format-Gate | Experimentell gemessen, aber Container v1 hat keinen produktiven Dictionary-Codec. Nicht als aktuelle Appliance-Funktion bewerben. [ADR 0047](../adr/0047-train-and-activate-dictionaries-by-bounded-family.md) |
| Sparse-XOR Delta | Research-only | Im bytegenau geprüften In-Memory-Harness implementiert; kein dauerhafter produktiver Appliance-Writer. Nicht mit dem verfügbaren Prefix-Codec gleichsetzen. [Reduction-Referenz](../benchmarks/data-reduction-reference-v1.md) |
| Incompressibility Gate | implementiert, Produktionspfad `off` | LZ4/Zstd-1 sind nur Prädiktoren, keine gespeicherten zusätzlichen Codecs. Alle aktuellen Store-Einstiege übergeben `Off`; daher nicht als aktive Technik zählen. [ADR 0052](../adr/0052-reject-incompressible-regions-before-target-zstd.md) |
| Similarity-Reorder | verworfen | Produktions-Placement bleibt für HDD-Restore in logischer Reihenfolge. [ADR 0077](../adr/0077-prefer-restore-locality-over-similarity-reordering.md) |

Damit darf die öffentliche Liste heute fünf automatisch aktive Bausteine,
workloadabhängige Clone-Reuse und einen Opt-in-Mechanismus hervorheben.
Dictionary und Sparse-XOR gehören getrennt in eine Roadmap-/Research-Zeile.

## Was Dell und HPE selbst dokumentieren

- Dell beschreibt Data Domain mit Inline-Deduplizierung über variable,
  durchschnittlich 8-KiB große Segmente und anschließender lokaler
  Kompression. Aktuelle Dell-Angaben nennen je nach Modell bis zu 50x
  Deduplizierung beziehungsweise typischerweise 75:1 Datenreduktion inklusive
  typischerweise 30 % hardwareunterstützter Kompression; Dell weist auf
  Workload- und Konfigurationsabhängigkeit hin.
  [Dell SISL-Architektur](https://infohub.delltechnologies.com/en-us/l/dell-powerprotect-data-domain-sisl-scaling-architecture/no-data-available-191/),
  [Dell Portfolio](https://www.dell.com/en-us/shop/powerprotect-data-domain/sf/powerprotect-data-domain)
- HPE beschreibt StoreOnce mit Inline-Deduplizierung, feinen 4-KiB-Chunks und
  Kompression. Die in den QuickSpecs verwendeten 60:1 sind eine Annahme für
  effektive gegenüber nutzbarer Kapazität; Datentyp, Änderungsrate,
  Backup-Einstellungen, Zeitplan, Retention und konkurrierende Arbeit
  beeinflussen das Ergebnis.
  [HPE StoreOnce QuickSpecs](https://www.hpe.com/us/en/collaterals/collateral.c04328820.html),
  [HPE Metrikdefinitionen](https://support.hpe.com/hpesc/public/docDisplay?docId=sd00007401en_us&docLocale=en_US&page=capacity_efficiency_terms.html)

Die Herstellerunterlagen sind keine vollständige Offenlegung interner
Algorithmen. Daraus folgt **nicht**, dass Data Domain oder StoreOnce „nur zwei
Techniken“ verwenden. Außerdem sind deren Herstellerangaben nicht direkt mit
fastdups Corpus-Messungen vergleichbar: Zähler/Nenner, Datenmix, Retention,
Metadaten, Protokoll, Parallelität und Hardware unterscheiden sich.

## Empfohlene Website-Formulierung

Deutsch:

> **Aus Standard-x86-64-Hardware wird eine Dedup-Appliance.** fastdup bündelt
> Sparse- und FILL-Repräsentation, SeqCDC, BLAKE3 Exact Dedup, gruppierte
> adaptive Zstd-Kompression und metadata-only Clone-Reuse in einer offenen
> Pipeline; Similarity-geführte Depth-1-Zstd-Prefixes sind optional. Dell und
> HPE heben in ihren öffentlichen Produktunterlagen vor allem Deduplizierung
> plus Kompression hervor. fastdup macht jeden Schritt und seine Grenzen
> nachvollziehbar.

Englisch:

> **Turn standard x86-64 hardware into a dedup appliance.** fastdup combines
> sparse and FILL representation, SeqCDC, BLAKE3 exact deduplication, grouped
> adaptive Zstd compression, and metadata-only clone reuse in an open pipeline;
> similarity-guided depth-1 Zstd prefixes are optional. Dell and HPE primarily
> highlight deduplication plus compression in their public product material.
> fastdup makes every stage—and its limits—inspectable.

Nicht schreiben: „fastdup hat mehr Reduction-Techniken als Data Domain und
StoreOnce“, „fastdup reduziert besser als Legacy-Appliances“, „42,95x typische
Reduction“ oder „1,6 GiB/s auf einer 10-Kern-CPU“. Diese Varianten überschreiten
die vorhandene Evidenz.
