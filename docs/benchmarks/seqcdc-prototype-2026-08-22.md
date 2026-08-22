# SeqCDC-Prototyp auf dem Rocky-SMB-Workload

Stand: 2026-08-22. Dieser Versuch begann als Vergleich der veröffentlichten
skalaren SeqCDC-Grenzlogik mit FastCDC-v1. Nach der AVX2-Nachmessung wurde
SeqCDC-v1 zum dauerhaften Standardprofil. `FASTDUP_SEQCDC_FORCE_SCALAR=1`
schaltet nur für Vergleichsmessungen auf dieselben skalaren Grenzen zurück.

## Konfiguration

- Corpus: `Rocky-10.2-x86_64-minimal.iso`, 2.072.444.928 Bytes
- FastCDC: 16 KiB Minimum, 64 KiB Ziel, 256 KiB Maximum, Normalization 1
- SeqCDC: Increasing Mode, `SeqLength=6`, `SkipTrigger=50`,
  `SkipSize=1024`, 16 KiB Minimum, 256 KiB Maximum
- CPU: Intel Core i7-1370P VM, zehn logische CPUs, AVX2, kein AVX-512
- SingleStream: fünf alternierende FastCDC/SeqCDC-Paare, je drei serielle
  Uploads derselben ISO in ein frisches Repository
- MultiStream: zwei gleichzeitige Uploads in ein frisches Repository

Der SeqCDC-Parametersatz wurde nur auf gleiche mittlere Chunkgröße zur
Rocky-ISO kalibriert. Er ist keine allgemeine Produktionskonfiguration.

## Isolierter Grenzscanner

Auf einem Kern und mit sieben warmen Wiederholungen ergab der beste Lauf:

| Scanner | Chunks | Mittel | Durchsatz |
| --- | ---: | ---: | ---: |
| FastCDC-v1 | 25.721 | 80.574 Bytes | 2.351 MiB/s |
| SeqCDC skalar | 25.653 | 80.788 Bytes | 2.776 MiB/s |

SeqCDC war damit 18,1 Prozent schneller. Der Test liest die ISO einmal in den
Speicher und misst danach ausschließlich Grenzsuche. Er enthält weder FUSE
noch Hashing, Exact Lookup, Codierung oder Publication.

### AVX2/BMI2-Kernel

Ein zweiter Versuch verarbeitet 32 benachbarte Bytevergleiche pro Schritt.
AVX2 erzeugt Masken für steigende und fallende Flanken. BMI2 `PEXT` entfernt
Gleichheitsläufe aus der Zustandsfolge, `PDEP` übersetzt die gefundene Grenze
zurück auf die Byteposition. Runtime-Feature-Erkennung wählt den Kernel nur
auf CPUs mit AVX2 und BMI2, sonst läuft die skalare Referenz.

| Scanner | Chunks | Mittel | Durchsatz |
| --- | ---: | ---: | ---: |
| FastCDC-v1 | 25.721 | 80.574 Bytes | 2.410 MiB/s |
| SeqCDC skalar | 25.653 | 80.788 Bytes | 2.764 MiB/s |
| SeqCDC AVX2/BMI2 | 25.653 | 80.788 Bytes | 8.009 MiB/s |

AVX2/BMI2 ist damit 2,90-mal so schnell wie derselbe skalare SeqCDC-Scanner.
Chunkzahl und Checksumme sind identisch. Differentialtests prüfen zufällige
und gleichheitsreiche Eingaben sowie einen vollständigen 16-MiB-Stream.

Das Workspace-Lint bleibt `unsafe_code = "deny"`. Im Appliance-Crate hebt nur
das Scanner-Modul es lokal auf. Der einzige `unsafe`-Kernel trägt `target_feature =
"avx2,bmi2"`; der sichere Aufrufer prüft beide CPU-Features. Die zwei
unaligned Loads liegen nachweislich in dem Slice, weil die Schleife vor jedem
Schritt mindestens 32 verbleibende Bytes prüft.

## SMB SingleStream

Die Hostleistung streute stark. FastCDC lag je Lauf zwischen 458 und 668
MiB/s. Deshalb sind die paarweisen Medianwerte aussagekräftiger als das
arithmetische Mittel:

| Messwert | SeqCDC gegen FastCDC |
| --- | ---: |
| Drei-Upload-Gesamtdurchsatz | +2,99 % |
| Erster physischer Upload | -3,54 % |
| Mittel der beiden Exact-Wiederholungen | +9,70 % |
| Daemon-CPU-Zeit | -2,83 % |
| Completed-write-p99 | +3,67 % schlechter |
| Datenreduktion | -0,46 Prozentpunkte |

Das Ergebnis passt zur Pipeline: Beim ersten Upload dominieren physische
Arbeit und Publication, sodass der schnellere Scanner nichts gewinnt. Bei den
Wiederholungen fällt mehr Zeit auf CDC und Exact Reuse, dort wird das Signal
sichtbar. SeqCDC erhöhte zugleich die gemessene Chunk-Fragment-Coalescing-Menge
von rund 190 auf 246 MB. Dieser Mehrverbrauch erklärt einen Teil des schwachen
Erstupload-Ergebnisses und muss vor einem weiteren Versuch beseitigt werden.

## SMB MultiStream

Der erste 2-Stream-Vergleich sank von 829,7 auf 802,1 MiB/s und beim langsamsten
Stream von 414,9 auf 401,1 MiB/s, jeweils rund 3,3 Prozent. Im zweiten Paar lag
SeqCDC beim Uploaddurchsatz 1,5 Prozent über FastCDC. Danach beendete der
Daemon den SIGINT-Shutdown innerhalb von 120 Sekunden nicht und der Runner
musste ihn töten. Der Lauf zählt deshalb als fehlgeschlagen.

Damit bestand der skalare SeqCDC-Versuch das MultiStream-Gate nicht. Zwei
Paare reichen ohnehin nicht für eine Durchsatzaussage, der Shutdown-Fehler
beendet den Challenger aber bereits vorher.

## AVX2-End-to-End-Nachmessung

Die Hostleistung war gegenüber dem ersten Versuch deutlich niedriger. Deshalb
wurden Scalar, AVX2 und FastCDC innerhalb derselben aktuellen Messserie
verglichen. Jeder Tabellenwert ist der Median aus drei frischen Repositories.

### SingleStream

| Messwert | SeqCDC skalar | SeqCDC AVX2 | Änderung |
| --- | ---: | ---: | ---: |
| Drei-Upload-Gesamtdurchsatz | 334,4 MiB/s | 380,6 MiB/s | +13,8 % |
| Erster physischer Upload | 193,7 MiB/s | 217,8 MiB/s | +12,4 % |
| Zweite Exact-Wiederholung | 500,3 MiB/s | 597,0 MiB/s | +19,3 % |
| Dritte Exact-Wiederholung | 511,6 MiB/s | 618,6 MiB/s | +20,9 % |
| Completed-write-p99 | 10.204 ms | 9.076 ms | -11,1 % |
| Daemon-CPU-Zeit | 37,17 s | 32,02 s | -13,9 % |

Gegen FastCDC lag AVX2-SeqCDC in derselben Serie beim Gesamtdurchsatz im
Median 8,7 Prozent vorn. Die gemessene Datenreduktion lag 0,79 Prozentpunkte
darunter. Dieser Unterschied gehört zum Algorithmuswechsel, nicht zu SIMD:
Scalar und AVX2 liefern exakt dieselben SeqCDC-Grenzen.

### Zwei gleichzeitige Streams

| Variante | Aggregat | Langsamerer Stream | p99 | Daemon-CPU |
| --- | ---: | ---: | ---: | ---: |
| SeqCDC skalar | 329,4 MiB/s | 164,7 MiB/s | 11.999 ms | 35,82 s |
| SeqCDC AVX2 | 343,9 MiB/s | 172,0 MiB/s | 11.493 ms | 34,95 s |
| FastCDC-v1 | 339,8 MiB/s | 169,9 MiB/s | 11.630 ms | 34,91 s |

AVX2 gewinnt gegenüber Scalar 4,4 Prozent und gegenüber FastCDC 1,2 Prozent.
Keiner der neun neuen Zwei-Stream-Läufe hing beim Shutdown.

Die Hash-Permit-Wartezeit stieg im SingleStream-Median von 17 auf 234 ms und
im Zwei-Stream-Median von 110 auf 303 ms. Der schnellere Scanner produziert
Arbeit früher, danach wartet die Hash-Stufe häufiger auf den gemeinsamen
Worker-Bestand. Das nimmt den SIMD-Gewinn nicht zurück, zeigt aber den nächsten
Flaschenhals.

## Einschränkungen und Urteil

Der öffentliche DedupBench-Stand enthält die skalare SeqCDC-Implementierung,
aber nicht den im TPDS-Paper beschriebenen VSEQ-SIMD-Code. Der lokale Kernel
ist deshalb eine unabhängige Umsetzung der veröffentlichten SeqCDC-Regeln,
keine Portierung von VSEQ.

Der SIMD-Kernel hat den ursprünglichen Scalar-Challenger gedreht: physischer
Erstupload, Exact-Wiederholungen und zwei gleichzeitige Streams profitieren.
SeqCDC-v1 ist deshalb das Standardprofil. Policy Set und Exact-Index-Profil
tragen neue Identitäten; FastCDC-Prototypdaten werden nicht übernommen. Die
stark schwankenden Allokationswerte im Zwei-Stream-Test taugen weiterhin nicht
als belastbare Dedup-Aussage.

Ein abschließender Lauf ohne CDC-Umgebungsvariable bestand den dreifachen
SingleStream-SMB-Upload. Damit läuft der AVX2/BMI2-Dispatcher über den
Produktionsdefault und nicht mehr über einen Challenger-Schalter.

## Messdateien

- `.artifacts/benchmarks/seqcdc-rocky-microbenchmark.txt`
- `.artifacts/benchmarks/seqcdc-avx2-rocky-microbenchmark.txt`
- `.artifacts/benchmarks/seqcdc-default-rocky-microbenchmark.txt`
- `.artifacts/benchmarks/seqcdc-smb-pairs.json`
- `.artifacts/benchmarks/smb-single-stream-seqcdc-{baseline,challenger}-{a,b,c,d,e}.json`
- `.artifacts/benchmarks/smb-2stream-seqcdc-{baseline,challenger}-{a,b}.json`
- `.artifacts/benchmarks/seqcdc-current-scalar-{1,2,3}.json`
- `.artifacts/benchmarks/seqcdc-avx2-{fastcdc,seq}-pair{1,2,3}.json`
- `.artifacts/benchmarks/seqcdc-avx2-2stream-{scalar,avx2,fastcdc}-{1,2,3}.json`
- `.artifacts/benchmarks/seqcdc-default-single-stream.json`
- `.artifacts/benchmarks/seqcdc-default-shared-single-stream.json`

Die zweite 2-Stream-Challenger-Datei hat erwartungsgemäß `status=failed` und
enthält den Shutdown-Timeout.
