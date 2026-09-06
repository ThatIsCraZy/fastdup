# SMB SingleStream — v0.6.1 — 2026-09-06

Gemessen mit dem unveränderten `smb-single-stream-benchmark`-Runner: drei
Wiederholungen je Reduction-Modus, jeweils drei serielle Uploads derselben ISO
auf ein frisches Repository. Reihenfolge: Normal, Advanced, Advanced, Normal,
Normal, Advanced. Alle sechs Läufe bestanden einschließlich Cleanup und
`--require-zero-swap`. Vor den Messungen wurden beide Modi per Dry-Run geprüft.

## Ergebnisse

Die Durchsätze sind Mediane der drei aggregierten Laufdurchsätze. Reduction
und belegter Speicher sind jeweils Mediane der drei Messungen nach 12 Sekunden
Settle-Zeit, während alle drei Dateien noch vorhanden sind.

| Messgröße | Normal | Advanced / Similarity |
| --- | ---: | ---: |
| Gesamtdurchsatz (MiB/s) | 1,489.4 | 1,313.6 |
| Speicherersparnis (%) | 67.82827 | 67.91339 |
| Reduktionsfaktor | 3.10832 | 3.11656 |
| Belegtes Repository (Bytes) | 2,000,224,256 | 1,994,932,224 |
| Maximal beobachtetes RSS (MiB) | 542.0 | 655.4 |
| Daemon-CPU-Zeit pro Lauf, Median (s) | 10.43 | 16.35 |
| Langsamster ISO-Upload über alle Läufe (ms) | 1912.51 | 2237.34 |
| Median des gemeldeten Datei-p99 (ms) | 1736.60 | 2214.18 |

| Kopie: medianer Durchsatz (MiB/s) | Normal | Advanced |
| --- | ---: | ---: |
| 1 | 1138.1 | 892.6 |
| 2 | 1735.8 | 1694.1 |
| 3 | 1775.7 | 1750.7 |

Das Datei-p99 bezeichnet die vollständige `smbclient put`-Laufzeit, keine
Latenz eines einzelnen SMB-WRITE-Requests. Bei drei Dateien je Lauf ist das
Nearest-Rank-p99 gleich dem Maximum dieses Laufs.

| Einzelner Lauf | MiB/s aggregiert | Speicherersparnis (%) |
| --- | ---: | ---: |
| normal r1 | 1304.64 | 67.82682 |
| normal r2 | 1489.35 | 67.82952 |
| normal r3 | 1500.65 | 67.82827 |
| advanced r1 | 1313.63 | 67.86042 |
| advanced r2 | 1361.66 | 67.92788 |
| advanced r3 | 1301.47 | 67.91339 |

## Einordnung

Advanced liegt beim Median des Gesamtdurchsatzes 11.80 % niedriger.
Die zusätzliche Ersparnis beträgt 0.08512 Prozentpunkte beziehungsweise
5.047 MiB bei 6,217,334,784 logischen Bytes pro Lauf.
Drei identische ISO-Kopien testen überwiegend Exact Dedup. Das Ergebnis
belegt keinen entsprechend kleinen Similarity-Nutzen bei veränderten oder
versionsähnlichen Backup-Daten.

Die Normal-Läufe streuen stärker; der erste Lauf war langsamer als die beiden
folgenden. Es wurden keine Caches zwischen den Läufen geleert. Drei Wiederholungen
liefern eine aktuelle Baseline, aber keine statistisch abgesicherte Aussage zu
kleinen Änderungen. Es wird kein Performance-Gewinn gegenüber älteren Binaries
behauptet.

## Umgebung und Nachweise

- Git-Commit: `86f0560be1cf92e7080a375387c25eb1e65d3ab4`; Quellstand vor dem Build sauber.
- Version: 0.6.1; vor der Messung mit `cargo build --locked --release` gebaut.
- SMB3 auf Loopback-Port 1445, Signing deaktiviert, Encryption aus.
- Separate XFS-Datenträger: Metadata `/dev/sdb1`, DATA `/dev/sdc1`.
- Rocky-ISO: 2.072.444.928 Bytes pro Kopie.
- `FASTDUP_ADVANCED_REDUCTION=off` beziehungsweise `dependent-v1`.
- Small-File-Quota 1 GiB, Pool-Isolation `lab-allow-shared`.
- Daemon-Swap in allen Läufen 0 Bytes. Historischer Host-Swap ist separate Telemetrie.
- Die Leistung ist eine lokale SMB-Messung, kein Durchsatznachweis über eine externe Netzwerkkarte.
- Gemessen wurde Write-Durchsatz; kein zusätzlicher vollständiger SMB-Readback.

Hashes:

- `fastdup-durable-fuse`: `15ba18ac6d08193ca89d9e9988fc4663d5be75b855922ef3e0a8e960f22e92e0`
- `run_benchmark.py`: `78f1327f2c8d968503843eb2fdb5f26007982d45e6d77f3945d7b5104f74f2a6`
- `smb.conf`: `3a6c4b7507fe3306170a4938b5d8f9d1c201af4f7c2f3c797fda4a30beebfeed`
- `Rocky-10.2-x86_64-minimal.iso`: `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`

Alle Artefakte unter `.artifacts/benchmarks/smb-v061-20260906/`:
`v061-{normal,advanced}-r{1,2,3}.json`, Runner-/Daemon-Logs, Kommando-Dateien,
`smb.conf`, `provenance.json`, `summary.json`, `run_acceptance.py` und
`summarize.py`. Der Skill-Runner wurde unverändert aufgerufen.
Der eigene Samba-Prozess und beide Bind-Mounts wurden nach den Läufen entfernt;
die Runner meldeten für jedes frische Repository fehlerfreien Cleanup.
