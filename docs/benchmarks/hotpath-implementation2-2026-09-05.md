# Zweite Hotpath-Optimierung: Umsetzung und SMB-Vergleich

Stand: 2026-09-05, Änderungen gegenüber
`1801e4a65296b54102a4929dcbc19458ff448dec` im lokalen Arbeitsbaum.
Grundlage ist der [zweite Audit](../research/hotpath-audit2-2026-09-05.md).

Advanced erreicht in drei SMB-Wiederholungen im Median **910,0 statt
762,9 MiB/s**, ein Gewinn von **19,3 %**. Die erste, überwiegend neue Kopie
steigt von 376,0 auf 493,9 MiB/s (**31,4 %**). Die Daemon-CPU sinkt von
19,53 auf 15,95 Sekunden (**18,3 %**). Normal bleibt innerhalb der
beobachteten Streuung unverändert. Gegenüber Normal bringt Advanced bei
diesem dreimal identisch geschriebenen ISO keinen zusätzlichen Netto-
Reduktionsgewinn. Exakte Wiederholungen werden bereits durch Exact Dedup
abgedeckt.

## Implementiert

| Bereich | Änderung und Begrenzung |
| --- | --- |
| Fingerprint | Rolling Hash und Minimum in 64-Shingle-Spans; Fingerprint-Profil v1 bleibt bytegleich. Vorhandener AVX2-Vote-Kern bleibt erhalten. |
| Advanced-Planung | Ein gepinnter Exact-/Similarity-Snapshot pro Batch; Fingerprints, Queries, unabhängige Vorbereitung und Codec-Trials in begrenzten CPU-Jobs. Base-I/O liegt zwischen CPU-Phasen und hält höchstens acht verifizierte Base-Owner je Welle. Kandidaten-/Trial-Limits und Auswahlregeln bleiben erhalten. |
| Parallelität | Dynamische Encode-Job-Vergabe und dynamische Hash-Pakete mit vier Chunks; Ausgabe bleibt nach Ordinal geordnet. Demand-Decodes dürfen bei mindestens zwei Records und 256 KiB Decode-Arbeit bis zu vier freie Ingest-Permits nutzen; sie warten dafür nicht und starten keine verschachtelte Rayon-Parallelität. |
| Similarity-Queries | Budgetierte Seiten-Endkeys aus dem vollständigen Run-Audit wählen die erste passende Bucket-Seite. Höchstens 64 KiB je Verzeichnis, 16 Runs und ein Viertel des Cache-Ziels; Budgetdruck entfernt die Beschleunigung mit funktionierendem Fallback. |
| Similarity-Kompaktion | Fortlaufender Bucket-Cursor verwendet vorhandene Referenzen direkt und führt Buckets über Seitengrenzen zusammen. Keine erneute Punktabfrage pro enumeriertem Bucket. |
| Similarity-Aktivierung | Neue Online-Bucket-Datei wird nach dauerhaftem Dateipublish einmal vollständig geprüft und unter derselben Mapping-Lease in die aktive Familie übernommen. Das Audit liegt vor dem Family-Manifest; Recovery und Offline-Scrub prüfen unabhängig weiter. |
| Materialisierung und Platzierung | Gemischte Fragment-Regionen werden vor der Planung einmal zusammengeführt und für Fingerprint, Trials und normales Encoding als Views weiterverwendet. Fragmentierung allein trennt keine Region mehr. Neue Chunks und gemischte Codec-Records folgen der logischen Reihenfolge, einschließlich Checkpoint-Tail. |
| Datei-I/O | Gemeinsamer Cache für höchstens 128 immutable Container-FDs und bekannte Dateilängen je kanonischem Root. Mutation, Rename und Unlink invalidieren über Adaptergrenzen; Cache-FDs blockieren keine GC-Lease. |
| Read-Antwort | `VerifiedChunkPayload` bleibt über `Bytes::from_owner` bis zum FUSE-Reply erhalten, wenn eine DATA-/DATA_SLICE-Extent die Antwort abdeckt. Gemischte Extents werden weiterhin zusammengefügt. Die bestehende Vec-API bleibt verfügbar. |
| io_uring-Puffer | Reservierte Vec-Kapazität wird erst nach vollständig erfolgreichen CQEs als initialisierte Bytes freigegeben. EOF und Fehler liefern keine Teilantwort. [A/B und Safety-Nachweis](read-buffer-spare-capacity-2026-09-05.md). |
| Messbarkeit | Separate aufsummierte Laufzeiten für Fingerprint, Lookup, Base-Read und Codec-Trial sowie eine Planungsspanne auf Publication-Ebene. |

Storage-/Pipeline-Entscheidungen sind in [ADR 0050](../adr/0050-overlap-reduction-stages-under-one-memory-and-cpu-budget.md)
und [ADR 0061](../adr/0061-map-immutable-similarity-runs-under-generation-leases.md)
ergänzt. Das dauerhafte Containerformat und Fingerprint-Profil wurden nicht
geändert. Die Prüfung kleinerer Records folgt unten; daraus ergibt sich kein
belegbarer universeller Ersatz für die bestehende Gruppierung.

## SMB-Methode

Unveränderter Runner des Skills `smb-single-stream-benchmark`:
`/root/.codex/skills/smb-single-stream-benchmark/scripts/run_benchmark.py`.
Jeder Lauf beginnt mit einem frischen Repository und schreibt dasselbe
Rocky-ISO dreimal nacheinander. Vor der Platzmessung sind drei Dateien live;
die Settle-Zeit beträgt zwölf Sekunden. Physischer Platz umfasst allozierte
DATA- **und** Metadatenblöcke. Die Reduktion ist
`1 - repository_allocated_bytes / logical_bytes`.

- Rocky 10.2 Minimal, 2.072.444.928 Bytes pro Kopie; zusammen 6.217.334.784 Bytes.
- ISO-SHA256: `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
- 10 logische CPUs; Kernel `6.12.0-211.50.1.el10_2.x86_64`;
  Samba/smbclient 4.23.5, SMB über Loopback auf Port 1445.
- Metadaten: `/dev/sdb1`, DATA: `/dev/sdc1`, beide XFS auf getrennten Disks.
  Workspace-lokale Bind-Mounts `.artifacts/bm` und `.artifacts/bd`.
- `lab-allow-shared`, deklarierte Small-File-Quota 1 GiB; keine harte XFS-Projektquota.
- Normal: Policy `off`; Advanced: `dependent-v1`. Der bestehende
  `advanced_reduction enabled=true`-Status bezeichnet die eingerichtete
  Fähigkeit und unterscheidet diese Policies nicht. Die Normal-Läufe haben
  null Similarity-Queries/-Trials; die gestartete Policy steht im Command-JSON.
- Alle 13 Läufe: `passed`, Cleanup ohne Fehler, Daemon-Peak-Swap null.
  Der Host hatte bereits etwa 7 MB Swap anderer Prozesse; die Angabe null
  bezieht sich auf den Benchmark-Daemon.

Der Runner prüft die abgeschlossenen SMB-Schreibvorgänge und Dateilängen.
Er führt keinen vollständigen Hash-Readback aller drei Dateien aus. Dies ist
ein SMB-Write-Benchmark, kein SMB-Read-Durchsatztest. Die Read-Änderungen sind
durch die Integritäts-/Owner-Tests und die gesonderten A/Bs qualifiziert.

## Vergleich über jeweils drei vollständige Läufe

Alle Werte sind jeweils Mediane der drei Laufwerte. Der Gesamtdurchsatz ist
Bytes durch die Summe der drei Upload-Zeiten, kein Mittel der Einzelraten.
Die Spaltenmediane müssen nicht aus demselben Lauf stammen.

| Stand / Modus | Gesamt MiB/s | Erste Kopie MiB/s | Repo allozierte Bytes | Reduktion | Daemon-CPU s | Completed-Write p99/max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vorher Normal | 1.041,3 | 615,8 | 2.003.595.264 | 67,774 % | 10,53 | 3.209,3 |
| Jetzt Normal | 1.035,5 | 609,6 | 2.001.117.184 | 67,814 % | 10,43 | 3.242,3 |
| Vorher Advanced | 762,9 | 376,0 | 2.105.159.680 | 66,140 % | 19,53 | 5.256,4 |
| Jetzt Advanced | 910,0 | 493,9 | 2.004.979.712 | 67,752 % | 15,95 | 4.001,7 |

Bei nur drei abgeschlossenen Datei-Writes pro Lauf ist nearest-rank p99
gleich dem Maximum. Das ist **Dateiabschlusslatenz**, keine Verteilung der
einzelnen SMB-WRITE-Requests und kein belastbarer Tail-Latency-Nachweis.

Advanced ist jetzt gegenüber Normal rund 12,1 % langsamer und verbraucht
52,9 % mehr Daemon-CPU. Die Median-Platzdifferenz beträgt nur 3.862.528 Bytes
zugunsten von Normal, also 0,062 Prozentpunkte Reduktion. Dieser kleine
Unterschied liegt innerhalb der beobachteten Laufstreuung. Einzelne
Prefix-Treffer sparen Payload, sind aber kein Netto-Repository-Vorteil
gegenüber Normal. Der letzte Advanced-Lauf hatte 24.947 Queries, 73 Base-Reads,
65 angenommene Prefixes, null angenommene Sparse-XORs und 9.773.625 gemeldete
gesparte Payload-Bytes. Das ist kein Vergleich gegen das gesamte Normal-Repo.

Gegenüber dem alten Advanced-Pfad sinkt der mediane physische Platz um
100.179.968 Bytes beziehungsweise 4,76 %. Die bessere gemeinsame Gruppierung
beseitigt damit den zuvor sichtbaren Nachteil weitgehend. Die Messung
quantifiziert das Gesamtpaket; sie isoliert keinen einzelnen Commit-Effekt.

### Alle Wiederholungen und zusätzliche Abschlussqualifikation

| Lauf | Gesamt MiB/s | Kopie 1 MiB/s | Repo Bytes | Reduktion % | CPU s | p99/max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline-normal | 924.403 | 502.233 | 2019450880 | 67.519026 | 10.73 | 3935.302 |
| current-normal | 803.060 | 403.672 | 2001117184 | 67.813907 | 10.83 | 4896.141 |
| baseline-advanced | 630.227 | 286.180 | 2075922432 | 66.610734 | 20.50 | 6906.279 |
| current-advanced | 794.559 | 398.158 | 1997545472 | 67.871354 | 15.98 | 4963.958 |
| current-normal-r2 | 1035.530 | 609.584 | 2000605184 | 67.822142 | 10.43 | 3242.271 |
| baseline-normal-r2 | 1045.536 | 615.849 | 2003574784 | 67.774378 | 10.53 | 3209.289 |
| baseline-normal-r3 | 1041.297 | 617.951 | 2003595264 | 67.774049 | 10.48 | 3198.375 |
| current-normal-r3 | 1073.194 | 635.331 | 2009985024 | 67.671276 | 10.42 | 3110.881 |
| baseline-advanced-r2 | 806.068 | 404.699 | 2111721472 | 66.034940 | 18.68 | 4883.724 |
| current-advanced-r2 | 920.694 | 495.778 | 2004979712 | 67.751781 | 15.62 | 3986.536 |
| current-normal-final | 1035.337 | 625.516 | 2000834560 | 67.818452 | 10.56 | 3159.689 |
| current-advanced-r3 | 910.046 | 493.894 | 2005196800 | 67.748290 | 15.95 | 4001.747 |
| baseline-advanced-r3 | 762.871 | 376.005 | 2105159680 | 66.140481 | 19.53 | 5256.414 |

Der erste Normal-Vergleich zeigte zunächst einen Rückgang. Deshalb wurden
zwei weitere Paare mit wechselnder Reihenfolge ausgeführt. Die ersten Kopien
lagen im zweiten Paar bei 609,6/615,8 MiB/s (neu/alt), im dritten bei
635,3/618,0 MiB/s. Der Medianquotient ist 0,9898. Eine stabile Regression ließ
sich damit nicht reproduzieren; es wurde keine spekulative Korrektur aus
diesem ersten Ausreißer abgeleitet. Alle Ergebnisse bleiben enthalten.
Die zusätzliche Normal-Abschlussqualifikation ist separat ausgewiesen und
wird nicht als vierte Stichprobe in die Drei-gegen-drei-Tabelle gemischt.

### Binärstände und Rohdaten

- Baseline: `4b3a07b20156251ffba996cee02f9e367326ff43fde009e4f338fc76d6642d90`.
- Erste Current-Läufe: `dc12c5be74206244a2927786778fb6fc64cda7b777bef81863c791ca982d9bcc`.
- Nach Checkpoint-Tail-Ordnungsprüfung: `e18b07f7913a8386d2a9038610fd362ec5d92f28c5cbf84b723a42324feeb5ca`;
  verwendet für `current-normal-final` und `current-advanced-r2/r3`.
  Der frühere Tail-Pfad erzeugte in diesen SMB-Läufen keine Container.
  Anschließend wurden nur Formatierung und Dokumentation verändert.
- Samba-Konfiguration SHA256:
  `3a6c4b7507fe3306170a4938b5d8f9d1c201af4f7c2f3c797fda4a30beebfeed`.
- Sämtliche Runner-Reports, Command-JSONs, Vergleichsskripte und
  `summary.json`: `.artifacts/benchmarks/smb-implementation2-20260905/`.
- Zusätzliche Tests und isolierte Qualifikation:
  `.artifacts/hotpath-implementation2-20260905/`.

Der dedizierte Samba-Prozess und die beiden Bind-Mounts wurden nach den
Läufen entfernt. Der reguläre Samba-Dienst blieb bestehen.

## Container-Geometrie und kleine Reads

Die im Audit vorgeschlagenen 64/128/256/512-KiB-Ziele wurden mit dem
bestehenden Format geprüft. Eingaben: jeweils 32 MiB Rocky-ISO ab Offset
256 MiB und Linux-6.12.1-Tar ab Offset null. Tatsächliches SeqCDC-v1-Chunking,
Gate `off`, unveränderte Chunkfolge, vollständige Container-Verifikation.
Pro Ziel elf abwechselnde Timing-Samples mit 128 Einzel-Chunk-Anfragen;
gemessen wird Decode plus Integritätsprüfung, ohne Datei-I/O und Read-Cache.

| Tar-Ziel KiB | Records | Containerbytes | 128 Reads, Median ms |
| --- | ---: | ---: | ---: |
| 64 | 466 | 7.528.448 | 7,594 |
| 128 | 291 | 7.319.552 | 11,953 |
| 256 | 157 | 7.114.752 | 23,341 |
| 512 | 71 | 6.889.472 | 48,460 |

64 KiB beschleunigen diesen kleinen Random-Read um 6,38×, kosten aber 9,27 %
mehr Containerplatz. Beim Rocky-Ausschnitt sind alle 416 Records RAW;
alle Ziele ergeben 33.710.080 Bytes und praktisch gleiche 3,17–3,26 ms.
Ein generelles Verkleinern würde deshalb Reduktion gegen einen stark
workloadabhängigen Read-Gewinn tauschen. Der Produktionsstandard bleibt
512 KiB. Die begrenzte parallele Decode-Optimierung ist umgesetzt; ein neues
Containerformat ist durch diese Messungen nicht gerechtfertigt.

Harness: `.artifacts/hotpath-implementation2-20260905/geometry/`;
Rohdaten: `geometry-run1.txt` im übergeordneten Verzeichnis.

## Prüfungen und Grenzen der Telemetrie

Sechs Crates einschließlich Testkit: **692 bestanden, 0 fehlgeschlagen,
10 ignoriert in 105 Testsuiten** (`test-wave3.txt`). Nach der letzten
Checkpoint-Tail-Korrektur zusätzlich alle Appliance-Tests:
**168 bestanden, 0 fehlgeschlagen, 1 ignoriert** (`test-checkpoint-order.txt`).
Clippy mit `--all-targets -- -D warnings` für die fünf Produktionscrates,
`cargo fmt --all --check`, Release-Build und `git diff --check` erfolgreich.

Die Tests umfassen Fingerprint-Orakel, Bucket-Seitengrenzen und
Budget-Fallback, Publication-/Recovery-Fehlerfälle, adapterübergreifende
FD-Invalidierung, gemeinsame Read-Owner samt Lebensdauer sowie erfolgreiche
Teil-CQEs, EOF und Fehler bei io_uring-Read-Puffern.

Die vier neuen Detailtimer summieren verstrichene Zeit je Operation über
Worker; sie sind keine reine CPU-Zeit und dürfen nicht zur parallelen
Gesamtlaufzeit addiert werden. `write_through_planning.runnable_wall_ns`
enthält den äußeren Planungsbereich einschließlich I/O und Admission-Warten.
Seine Worker-Zähler sind null, weil die inneren CPU-Phasen ihre Permits im
Store nehmen; null bedeutet hier nicht, dass keine Permits verwendet wurden.

Korrektur nach Codeprüfung in der dritten Umsetzung: Die ursprüngliche
Erklärung einer fehlenden Buchung in `collect_prehashed_decoded` war falsch.
Diese Funktion bucht bereits `CompressionRegionMaterialization`. Der
Materialisierungszähler enthält somit beide Orte; seine höhere Zahl darf
nicht mit einer vermeintlich fehlenden Format-Buchung erklärt werden.
Die dritte Umsetzung ergänzt einen separaten Teilzähler für die
Format-Konkatenation, ohne den bisherigen Gesamtzähler umzudefinieren.
Weitere Chancen stehen im anschließenden
[dritten Audit](../research/hotpath-audit3-2026-09-05.md).
