# Read-Puffer ohne vorgezogene Nullinitialisierung

Zwei alternierende A/B-Läufe auf dem warmen Rocky-ISO, 11 Samples je Variante,
2.048 Reads pro Sample und wiederverwendeter FD. Der sichere Referenzpfad
reserviert und nullt einen Vec vor `read_exact_at`; der Challenger lässt den
Kernel in reservierte Kapazität schreiben und setzt die Länge erst nach einem
vollständig erfolgreichen Read. Alle 16 adressierten Bereiche jeder Größe
wurden vor der Zeitmessung bytegenau verglichen.

| Größe | Lauf 1 sicher / Kapazität, ns | Faktor | Lauf 2 sicher / Kapazität, ns | Faktor |
|---|---:|---:|---:|---:|
| 4 KiB | 303,1 / 271,1 | 1,118× | 283,6 / 251,5 | 1,128× |
| 64 KiB | 2.501,6 / 1.733,6 | 1,443× | 2.437,5 / 1.835,8 | 1,328× |
| 1 MiB | 80.306,9 / 65.446,6 | 1,227× | 89.531,1 / 70.022,0 | 1,279× |

Das ist ein positional-read A/B für Allokation, Nullinitialisierung und
Kernel-Read, kein io_uring- oder SMB-Durchsatzversprechen. Es rechtfertigt die
eng begrenzte `ReadBuffer::finish`-Schnittstelle im io_uring-Adapter: Vec-Länge
bleibt null, bis die Summe erfolgreicher CQEs exakt die angeforderte Länge
abdeckt. Kurze Reads werden fortgesetzt; EOF und Fehler geben keine Teilbytes
zurück. Der bestehende Ring-Owner hält Puffer und FD bis zum letzten CQE.

Reproduzierbarer [Quelltext](sources/read_buffer_ab.rs); Eingabe:
`.artifacts/benchmark-source/Rocky-10.2-x86_64-minimal.iso`. Übersetzen mit
`TMPDIR=/source/fastdup/.artifacts/tmp rustc -O -C overflow-checks=on --edition=2024
docs/benchmarks/sources/read_buffer_ab.rs -o
/source/fastdup/.artifacts/hotpath-implementation2-20260905/read-buffer-ab`.
Rohdaten: `.artifacts/hotpath-implementation2-20260905/read-buffer-ab-{1,2}.txt`.
