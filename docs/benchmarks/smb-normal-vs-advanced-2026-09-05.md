# SMB SingleStream: normale und Advanced Reduction, 2026-09-05

Im geforderten Vergleich mit drei identischen Rocky-ISO-Kopien war Advanced
Reduction insgesamt **31,74 % langsamer** und belegte **12.996.608 Bytes
zusätzlich** (+0,643 %). Normale Reduction erzielt für dieses Muster das bessere
Ergebnis. Das ist ein einzelnes Laufpaar, keine allgemeine Aussage über
Similarity bei veränderten Backup-Versionen.

## Messaufbau

Ausgeführt wurde der unveränderte Runner des Skills
`/root/.codex/skills/smb-single-stream-benchmark/scripts/run_benchmark.py`:
je Modus ein frisches Repository, drei serielle Uploads derselben ISO, danach
12 Sekunden Settle. Die Allokation wurde gemessen, während alle drei Dateien
live waren; erst danach löschte der Runner seine Dateien und Repositories.
Beide Läufe bestehen inklusive Dateigrößenprüfung und Null-Swap-Prüfung.
Der Skill führt keinen vollständigen Restore-Hash-Vergleich aus.

- Aktueller Release-Build einschließlich der sieben Hotpath-Optimierungen;
  gleicher Build für beide Modi, zuerst normal, dann Advanced.
- Normal: `FASTDUP_ADVANCED_REDUCTION=off`.
  Advanced: `FASTDUP_ADVANCED_REDUCTION=dependent-v1`.
- ISO: Rocky 10.2 x86_64 Minimal, 2.072.444.928 Bytes je Datei;
  zusammen 6.217.334.784 logische Bytes.
- SMB3 über eine separate lokale Samba-Instanz auf `127.0.0.1:1445`, Signing
  deaktiviert, Encryption aus, `strict sync=yes`, `sync always=no`.
  Der bestehende Samba-Dienst auf Port 445 blieb unverändert.
- Ein kleiner `smbclient`-Wrapper ergänzt ausschließlich `-p 1445`; der
  Originalrunner beaufsichtigt direkt den FUSE-Daemon.
- Metadata: `/dev/sdb1`, XFS, 20 GiB. DATA: `/dev/sdc1`, XFS, 200 GiB.
  Beide bestehenden Dateisysteme wurden für den Benchmark unter
  `.artifacts/bm` und `.artifacts/bd` eingebunden; ausschließlich frische
  UUID-Unterverzeichnisse wurden angelegt und wieder gelöscht.
- Beide Modi verwenden `FASTDUP_POOL_ISOLATION=lab-allow-shared` und
  `FASTDUP_SMALL_FILE_QUOTA_BYTES=1073741824`. Die Datenträger sind tatsächlich
  getrennt und werden vom Runner geprüft; harte Small-File-Quotas werden in
  dieser Lab-Konfiguration nicht erzwungen.
- Zehn vCPUs; Kernel `6.12.0-211.50.1.el10_2.x86_64`, Samba 4.23.5.

Der erste Vorbereitungsversuch brach vor einem Upload ab, weil die voreingestellte
64-GiB-Small-File-Quota die Metadatendisk überschreitet. Ein weiterer Start
scheiterte am zu langen Unix-Socket-Pfad; kurze workspace-lokale Bind-Mount-Pfade
beheben das. Beide fehlgeschlagenen Starts sind separat archiviert und gehen
nicht in die Messung ein. Es wurde dafür kein Produktionscode geändert.

## Durchsatz und Abschlusszeit

| Messwert | Normal | Advanced |
| --- | ---: | ---: |
| Erste ISO, MiB/s | 563,70 | 315,38 |
| Zweite identische ISO, MiB/s | 1.607,27 | 1.582,23 |
| Dritte identische ISO, MiB/s | 1.548,46 | 1.529,31 |
| Gesamtdurchsatz, MiB/s | **986,20** | **673,14** |
| Größter Dateiabschluss / Datei-p99, Sekunden | 3,506 | 6,267 |
| Daemon-CPU über das Messfenster, Sekunden | 11,32 | 20,19 |
| Peak RSS, MiB | 616,92 | 654,48 |
| Maximal beobachteter Prozess-Swap, Bytes | 0 | 0 |

Gesamtdurchsatz ist die Summe der logischen Upload-Bytes geteilt durch die
Summe der drei Upload-Zeiten, kein arithmetischer Mittelwert der MiB/s-Werte.
Datei-p99 ist bei drei Samples per nearest-rank identisch mit dem Maximum und
beschreibt keine einzelne SMB-WRITE-Anfrage. CPU und RSS werden einschließlich
der nachfolgenden Settle-Phase erfasst; der Gesamtdurchsatz enthält diese
12 Sekunden nicht. Aggregate Host-CPU-Auslastung lag bei 9,67 % beziehungsweise
12,32 %, Prozess-Swap in beiden Läufen bei null.

Der Geschwindigkeitsunterschied konzentriert sich auf die erste Kopie. Die
beiden bereits vorhandenen identischen Kopien profitieren in beiden Modi
von Exact Dedup und liegen bei ähnlichen Durchsätzen.

## Physische Reduction

| Messwert, alle drei Dateien live | Normal | Advanced |
| --- | ---: | ---: |
| Logische Bytes | 6.217.334.784 | 6.217.334.784 |
| Container, allokierte Bytes | 2.001.698.816 | 2.004.332.544 |
| Metadata, allokierte Bytes | 19.890.176 | 30.253.056 |
| Repository insgesamt, allokierte Bytes | **2.021.588.992** | **2.034.585.600** |
| Repository insgesamt, MiB | 1.927,94 | 1.940,33 |
| Reduction-Faktor | **3,075×** | **3,056×** |
| Speicherersparnis gegenüber logisch | **67,485 %** | **67,276 %** |

Advanced benötigt 10.362.880 zusätzliche Metadata-Bytes und 2.633.728 zusätzliche
Container-Bytes. Damit sinkt die gesamte Speicherersparnis um 0,209 Prozentpunkte.
Gemessen wird Dateisystemallokation einschließlich Indizes und sonstiger
Repository-Metadaten, keine reine Codec-Payloadsumme.

## Similarity war aktiv

Die Starttelemetrie bestätigt `Off` beziehungsweise `DependentV1`. Der generische
Zähler `advanced_reduction enabled=true` zeigt in dieser Version die verfügbare
Index-Anbindung an; die tatsächlich konfigurierte Auswahlpolitik wird separat
protokolliert. Im normalen Lauf gab es null Similarity Queries und null
Online-Similarity-Batches.

| Advanced-Zähler | Wert |
| --- | ---: |
| Similarity Queries | 25.332 |
| Queries ohne Candidate / entsprechender Fallback | 25.245 |
| Candidates | 123 |
| Base-Reads / Sparse-XOR-Trials / Prefix-Trials | jeweils 107 |
| Angenommene Prefixes | 82 |
| Angenommene Sparse-XOR-Encodings | 0 |
| Berechnete Payload-Ersparnis angenommener Trials, Bytes | 10.398.426 |
| Online-Similarity-Batches / Compactions | 65 / 21 |
| Reduction- / Online-Similarity-Fehler | 0 / 0 |

Die rechnerische Payload-Ersparnis ist der Vergleich gegen die jeweilige
unabhängige Trial-Alternative. Sie ist kein gemessener Nettogewinn gegenüber
dem vollständigen normalen Repository. Hier steht ihr ein größeres Repository
gegenüber; die zusätzlichen Metadaten sind separat messbar. Der Anteil von
Record-Gruppierung, Container-Packing und Allokationsvariation an der
Container-Differenz wurde in diesem Laufpaar nicht isoliert.

Die überwiegend erfolglosen Queries und die gestiegene Daemon-CPU passen zu
zusätzlicher Arbeit bei der ersten Kopie. Für eine Aussage zum Nutzen von
Similarity über mehrere Backup-Versionen wären verwandte, veränderte Eingaben
nötig. Der vorgeschriebene Drei-Kopien-ISO-Test misst vor allem Exact Dedup.

## Nachweise und Aufräumen

Alle Rohdaten liegen unter
`/source/fastdup/.artifacts/benchmarks/smb-reduction-20260905/`:
`normal.json`, `advanced.json`, `comparison.json`, `normal-command.json`,
`advanced-command.json`, beide Dry-Run-Logs, `smb.conf`, `run_comparison.py`,
`run_benchmark.snapshot.py`, `build.txt` und die getrennt benannten
fehlgeschlagenen Startreports. Authentifizierungsdaten wurden nicht kopiert
oder in Reports ausgegeben.

Die folgenden Eingaben stimmen zwischen beiden Reports exakt überein:

```text
ISO SHA-256
  aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8
FUSE-Binary SHA-256
  4b3a07b20156251ffba996cee02f9e367326ff43fde009e4f338fc76d6642d90
Samba-Konfiguration SHA-256
  3a6c4b7507fe3306170a4938b5d8f9d1c201af4f7c2f3c797fda4a30beebfeed
```

Der Runner meldet für beide Messläufe `status=passed` und keine Cleanup-Fehler.
Die UUID-Repositories sind entfernt, FUSE ist ausgehängt, die separate
Samba-Instanz ist beendet und beide temporären Bind-Mounts sind ausgehängt.
