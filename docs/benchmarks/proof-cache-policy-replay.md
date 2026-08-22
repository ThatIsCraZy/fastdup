# Online-Proof-Cache: S3-FIFO gegen SIEVE

Stand: 2026-08-21. Dieser Replay vergleicht S3-FIFO und SIEVE mit denselben
realen fastdup-Zugriffen und demselben modellierten Byte-Budget. Die
anschließende Entscheidung für S3-FIFO steht in
[ADR 0051](../adr/0051-use-s3-fifo-for-the-historical-proof-cache.md).

## Versuchsaufbau

Die opt-in Trace-Naht zeichnet `Lookup`, `AdmitPublished` und
`AdmitExactReuse` direkt im Online-Dependency-Proof-Pfad auf. Ein Ereignis
enthält die vollständige BLAKE3 Chunk ID, die logische Länge und bei einer
Admission die Größe des physisch zu verifizierenden Container-Records, aber
keine Nutzdaten. Das versionierte Trace-v1-Format begrenzt Anzahl und Größe,
serialisiert alle Felder explizit und authentifiziert den gesamten Recordbereich
mit BLAKE3.

Beide Referenz-Policies erhalten 192 modellierte Byte je residentem Proof und
höchstens 256 Eviction-Schritte pro Admission. Eine über diesem Limit liegende
Admission wird abgewiesen; das ist ein Cache-Miss und kein Ingest-Fehler.
`AdmitExactReuse` geht bei S3-FIFO direkt in Main, während neue Publikationen in
Small beginnen. SIEVE verwendet für beide Herkünfte denselben Ring.

Zwei Traces wurden über die echte `DurableNamespace`-Ingest- und
Checkpoint-Pipeline erzeugt:

- Rocky Linux 10.2 Minimal ISO dreimal unverändert, 251.232 Ereignisse;
- 50 vollständige ISO-Streams, je Variante acht deterministische Änderungen
  von 32 Byte, 3.807.996 Ereignisse und rund 103,6 GiB logische Eingabe.

Die Varianten wurden beim Lesen erzeugt und nicht als 50 Dateien materialisiert.
Quelle war die auf SHA-256
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`
geprüfte `Rocky-10.2-x86_64-minimal.iso` mit 2.072.444.928 Byte.

## Ergebnisse

Unveränderte ISO, dreimal:

| Budget | Policy | Hit Rate | vermiedene Verify-I/O | nötige Verify-I/O | Rejections |
|---:|---|---:|---:|---:|---:|
| 1 MiB | S3-FIFO | 11,65 % | 3,37 GiB | 11,04 GiB | 23 |
| 1 MiB | SIEVE | 11,17 % | 3,27 GiB | 11,14 GiB | 39 |
| 2 MiB | S3-FIFO | 22,50 % | 5,48 GiB | 8,93 GiB | 42 |
| 2 MiB | SIEVE | 21,63 % | 5,02 GiB | 9,39 GiB | 79 |
| 4 MiB | S3-FIFO | 43,57 % | 9,35 GiB | 5,06 GiB | 76 |
| 4 MiB | SIEVE | 40,60 % | 8,44 GiB | 5,97 GiB | 167 |
| 8 MiB | beide | 72,02 % | 14,41 GiB | 0 GiB | 0 |

50 leicht veränderte ISO-Streams:

| Budget | Policy | Hit Rate | vermiedene Verify-I/O | nötige Verify-I/O | Rejections |
|---:|---|---:|---:|---:|---:|
| 1 MiB | S3-FIFO | 12,51 % | 55,18 GiB | 228,99 GiB | 916 |
| 1 MiB | SIEVE | 16,20 % | 72,43 GiB | 211,74 GiB | 665 |
| 2 MiB | S3-FIFO | 25,35 % | 91,77 GiB | 192,39 GiB | 1.831 |
| 2 MiB | SIEVE | 26,29 % | 89,85 GiB | 194,32 GiB | 1.865 |
| 4 MiB | S3-FIFO | 51,67 % | 163,75 GiB | 120,41 GiB | 3.648 |
| 4 MiB | SIEVE | 51,75 % | 165,02 GiB | 119,14 GiB | 3.606 |
| 8 MiB | beide | 98,03 % | 284,17 GiB | 0 GiB | 0 |

Die Trefferquote enthält auch kalte Lookups, für die noch keine physische
Location existiert. Diese 48.703 beziehungsweise 49.487 kalten Misses erzeugen
keine Verify-I/O. Der Replay-Katalog wird deshalb strikt in Ereignisreihenfolge
aufgebaut; eine erst später publizierte Location darf frühere Misses nicht
rückwirkend verteuern.

## Bewertung

S3-FIFO ist beim wiederholten unveränderten Stream unter Druck durchgängig
besser. SIEVE gewinnt bei 1 MiB im Varianten-Trace deutlich und liegt bei 2
bis 4 MiB nach Trefferzahl knapp vorn; bei 2 MiB bewahrt S3-FIFO trotz weniger
Treffern die wertvolleren Records und vermeidet 1,92 GiB mehr Verify-I/O. Ab
8 MiB passt das Working Set beider Traces vollständig, sodass beide Policies
identisch sind.

Damit gibt es keinen universellen Sieger. fastdup wählt trotzdem S3-FIFO, weil
es den wiederholten unveränderten Stream besser schützt und Proof-Herkunft über
Small und Main direkt abbildet. SIEVE bleibt der verpflichtende Replay-
Challenger. Vor der Freigabe fehlen weiterhin parallele Dateien, ein Unique-
Scan neben einem Hotset, VM-Backup-Traces mit größerem Working Set sowie
gemessene RSS- und Lock-Kosten der geshardeten Produktionsimplementierung.

## Reproduzierbarkeit und Grenzen

Die lokalen, nicht eingecheckten Artefakte liegen unter
`.artifacts/benchmarks/proof-cache-replay-20260821-*`. Die Trace-Hashes sind:

- Drei Kopien: `ec76af2a6080a37aa3d567282dadf198668f8adeffdc3408a79102c3868eb018`
- 50 Varianten: `9287503858b76ad38f57696c48aa97eb331758df318c175a5f736aacce96a5bc`

Die Referenzmodelle vergleichen Policy-Entscheidungen, nicht deren endgültige
CPU- oder RSS-Kosten. 192 Byte sind ein identischer konservativer Budgetwert,
keine Messung eines fertigen Slot-Layouts. Die anschließende
Produktionsimplementierung rechnet vorsichtiger mit 224 Byte je residentem
Proof. Verify-I/O ist die Summe der
betroffenen Container-Record-Längen, nicht die Zahl tatsächlicher XFS- oder
Geräte-Reads. Tracing läuft absichtlich außerhalb des Durchsatz-Hotpaths:
aktiv verwendet es eine Mutex-geschützte Ereignisliste, inaktiv nur einen
Atomic-Check. Die Zahlen sind daher keine Ingest-Durchsatzmessung.
