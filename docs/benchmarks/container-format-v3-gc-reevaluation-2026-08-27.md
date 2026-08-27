# Container-Format V3: GC-Neubewertung nach dem Cache-Governor-Fix

Datum: 2026-08-27

## Fragestellung

Der verworfene Format-3-Prototyp spiegelte einen BLAKE3-Digest des Recovery
Indexes und einen festen 3-KiB-Filter für unabhängig dekodierbare Base Chunks
in Header und Footer. Frühere reale SMB-Läufe des Prototyps erreichten nur
99,7-164,5 MiB/s. Diese Läufe fanden in einem geteilten cgroup mit bereits
belegtem fremdem Swap statt; der damalige `MemoryBudgetGovernor` interpretierte
den cgroup-Swap fälschlich als Prozess-Swap und schloss alle rebuildbaren
Caches. Die damalige Writer-Performance ist deshalb kein gültiges Argument
gegen Format 3.

Diese Neubewertung beantwortet zwei getrennte Fragen:

1. Bleibt nach dem Prozess-Swap-Fix ein isolierter Writer-Nachteil des
   Digest-/Filter-Prototyps messbar?
2. Ist der verbleibende GC-Vorteil gegenüber dem aktuellen Format 2 groß genug,
   um einen inkompatiblen dauerhaften Formatwechsel zu rechtfertigen?

## Reale V2-ABBA-Messung

Der bestehende Rocky-ISO-Evolving-Family-Lauf wurde mit dem aktuellen
Format-2-Writer, dem pass-lokalen Recovery-Index-Resolver und dem korrigierten
Governor wiederholt. Jede Probe lief in einem eigenen transienten cgroup mit
`MemorySwapMax=0`; der fastdup-Prozess blieb bei 0 Byte Swap. Aufbau, zehn
Versionen, Restore, Löschung der Versionen 01 bis 04 und `gc-now` entsprechen
dem Lauf in `persistent-prefix-smb-ab-2026-08-27.md`.

| Policy | Probe | SMB MiB/s | completed-write p99 | Restore MiB/s | GC | Removed / Relocated |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| off | A1 | 753,0 | 6,37 s | 271,1 | 8,98 s | 0 / 0 |
| prefix-v1 | B1 | 624,1 | 6,72 s | 196,5 | 12,06 s | 18 / 7 |
| prefix-v1 | B2 | 660,0 | 7,24 s | 298,8 | 5,48 s | 0 / 0 |
| off | A2 | 1.273,1 | 3,67 s | 436,2 | 4,45 s | 0 / 0 |
| off, Mittel | 2 | 1.013,0 | 5,02 s | 353,7 | 6,71 s | - |
| prefix-v1, Mittel | 2 | 642,1 | 6,98 s | 247,6 | 8,77 s | - |

Das Prefix-/Off-Verhältnis fällt damit von zuvor `3,424×` auf `1,306×`.
Das Prefix-Mittel selbst sinkt von 31,33 s auf 8,77 s. B1 erledigt dabei mehr
physische Arbeit als die anderen Proben; es entfernt 18 Container und verlagert
7 Chunks. B2 ist ohne Relocation trotz 142 verifizierter Container schneller
als beide Off-Proben im alten Lauf. Die verbleibende Streuung ist daher kein
Beleg für einen allgemeinen Format-2-Prefix-GC-Engpass.

## Isolierte Format- und Resolver-Probes

Die vor dem Entfernen des Prototyps erhaltenen Release-Probes wurden fünfmal
abwechselnd ausgeführt. Der Median für das Erzeugen desselben Containerbilds
beträgt 6,36 ms für V2 und 6,10 ms für den V3-Prototyp. Diese Mikroprobe ersetzt
keinen vollständigen SMB-V3-Lauf, zeigt aber keinen isolierten
Digest-/Filter-Hot-Loop-Nachteil. Die alte reale V3-Serie bleibt wegen der
geschlossenen Caches ungültig.

Ein separater V2-I/O-Probe legt eine Base hinter 64 negativen Provider-
Containern mit je 64 unabhängigen Chunks ab. Ein vollständiger Lookup liest:

| Anteil | Bytes |
| --- | ---: |
| unvermeidbare Header/Footer-Envelopes | 532.480 |
| Recovery Indizes | 528.576 |
| ausgewählter Base Record | 65.728 |
| vollständige Repository-Dateien | 35.741.696 |

Der echte V3-Prototypfilter verwirft alle 64 negativen Provider ohne False
Positive und erkennt den Provider. Selbst ideal kann er in dieser Probe aber
nur die 528.576 Index-Bytes vermeiden: 1,47 % der Bytes eines vollständigen
Repository-Durchgangs. Warm im Speicher kostet die Verifikation aller 64
Indizes rund 0,445 ms pro Pass; die Filterprüfung ist im Vergleich
vernachlässigbar.

Im realen ABBA-Corpus umfasst ein kompletter Recovery-Index-Satz bei
93-142 Containern und rund 25.000 Chunks nur etwa 3,2 MiB. Selbst die bewusst
überhöhte Annahme eines vollständigen separaten Pool-Scans für jeden der sechs
Prefix Records ergibt weniger als 20 MiB vermeidbare Index-Lesevorgänge
gegenüber rund 2 GiB Containerdaten pro vollständiger Verifikation.

## Entscheidung

Container-Format 2 bleibt das einzige Format. Format 3 wird nicht länger wegen
eines vermeintlichen Writer-Hot-Loop-Nachteils verworfen; dieser Befund war vom
Cache-Governor-Fehler verfälscht. Es wird verworfen, weil Digest und
Independent-Base-Filter nach dem neuen V2-Resolver nur einen kleinen
rebuildbaren Beschleunigungsanteil adressieren und keinen inkompatiblen
dauerhaften Formatwechsel rechtfertigen.

Falls breitere Corpora erneut Base-Resolution als GC-Engpass zeigen, ist zuerst
ein poolweiter pass-lokaler Base-Resolver oder ein bereits unabhängig
auditierter Exact-Index-Pfad zu testen. Beide müssen bei fehlender oder
fehlerhafter Beschleunigung auf die verifizierten Format-2-Recovery-Indexes
zurückfallen und benötigen keine neuen dauerhaften Containerfelder.

## Lokale Rohdaten

- `.artifacts/benchmarks/advanced-reduction-v2-resolver-fixed-20260827/`
- `.artifacts/benchmarks/container-format-v2-v3-micro-ab-20260827.txt`
- `.artifacts/benchmarks/container-format-v2-resolver-io-probe-20260827.txt`
- `.artifacts/benchmarks/container-format-v3-filter-selectivity-20260827.txt`

