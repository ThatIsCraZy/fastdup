# Rechunk-Ausreißer: Diagnose und Erhalt stabiler Ingest-Bereiche

Stand: 2026-09-06. Ausgangspunkt ist der in
[Audit 8](hotpath-audit8-2026-09-06.md) dokumentierte einzelne SMB-Checkpoint
mit 30.285.275 Byte Rechunk-Arbeit (28,882 MiB).

Ein reproduzierbarer Leistungseinbruch bei einer unterbrochenen
Schreibreihenfolge ist behoben. Die genaue Ursache des ursprünglichen
SMB-Einzelfalls bleibt offen: Seine damalige Telemetrie enthält keine
Offset-/Sequence-Spur, und der große Rest trat in den zusätzlichen normalen
SMB-Wiederholungen nicht erneut auf. Die Übereinstimmung der Größenordnung
allein beweist keine gemeinsame Ursache.

## Reproduktion und Befund

Der vorhandene Test eines Containers über einem Frozen Commit Cut bestand
unverändert. Zwei zusätzliche, anschließend unter `.artifacts` abgelegte
Diagnoseprototypen prüfen wiederholte Schnitte sowie gleichzeitig laufende
Writes und Checkpoints. Auch sie blieben unter ihrer Ein-MiB-Grenze.

Ein deterministischer öffentlicher Write-/Checkpoint-Test trifft dagegen die
große Lücke: Er schreibt 40 MiB reproduzierbare Daten mit ein-MiB-Writes und
vertauscht nach den ersten 28 MiB zwei benachbarte Offsets. Die Beobachterfolge
ist damit `0..27, 29, 28, 30..39`. Der erste Sprung löscht bisher den ganzen
28-MiB-Tail; die Bytes bleiben in der autoritativen Dirty Extent Map erhalten.
Der Checkpoint findet deshalb für den großen vorangehenden Bereich keine
Prepared Extent Recipes.

Die gezielte Diagnose unterscheidet drei Erklärungen: verworfene Lane-Daten,
noch nicht abgeschlossene Publikation und abgelehnte Frozen-Rezepte. Im
deterministischen Fall zeigt sie direkt den ersten Mechanismus:

```text
reset: expected_offset=29360128 offset=30408704 tail_offset=0
       tail=29360128 pending=0
gap:   offset=0 length=31457280
adjacent reordered writes rechunked 31767683 bytes
```

Dabei lag der große Bereich noch ungeschnitten im Tail (`pending=0`). Der
Zähler belegt zusätzlichen Fallback im Checkpoint, nicht bereits doppelt
ausgeführtes CDC oder Hashing über sämtliche 28 MiB. Die Änderung verlagert
diese Arbeit in den vorhandenen Ingest-/Publikationspfad; die Zählerdifferenz
ist keine entsprechende Beschleunigung des gesamten Schreibens.

## Änderung und Schutzgrenzen

Vor einem durch ein Write-Fragment ausgelösten Lane-Reset werden vollständige
Chunks mit derselben SeqCDC-Implementierung extrahiert. Pending Chunks gehen
unter ursprünglicher Inode, Placement und Mutation Sequence an die bestehende
Publikationsqueue. Erst anschließend beginnt die Lane den neuen Bereich.
Verworfen wird höchstens der unvollständige CDC-Rest von zweimal 256 KiB.

Die Mutation-/Bereichsprüfungen bleiben erhalten. Ein später überschriebenes
Chunk darf Active nicht durch alte Bytes ersetzen; ein vorher eingefrorener
Cut darf weiterhin seine ursprünglichen Bytes übernehmen. Queue-Budget,
Worker-Permits, geordnete Publikation und Fehler-Fallback bleiben bestehen.
Truncate-/Barrier-Resets und Lane-Eviction ändern sich nicht. Ein Reset kann
jetzt einen zusätzlichen Teilcontainer erzeugen; die Änderung vermeidet
dafür den Verlust des vollständigen vorausgehenden Bereichs aus dem
Write-Through-Pfad. Es gibt kein neues Containerformat und keine neue
Unsafe-Stelle.

Die Pipeline-Regel ist in [ADR 0041](../adr/0041-overlap-one-frozen-commit-with-bounded-ingest-lanes.md)
festgehalten.

| Regression | Vorher | Mit Änderung |
| --- | ---: | ---: |
| Vertauschte Nachbar-Writes, Checkpoint nach 40 MiB | 31.767.683 B / 30,296 MiB | 1.291.398 B / 1,232 MiB |
| Derselbe Ablauf, zuvor bei 28 MiB eingefrorener Cut | nicht separat gemessen | 310.346 B / 0,296 MiB |

Die erste Zeile reduziert den Rechunk-Zähler um 95,93 %. Die Werte messen
Bytes, keine CPU-Zeit und keinen allgemeinen Durchsatzgewinn.

## Validierung

Die neue Regression wurde vor der Änderung rot ausgeführt. Mit Änderung
prüft sie sowohl einen normalen als auch einen zuvor eingefrorenen Cut und
liest die jeweiligen vollständigen Dateien nach Recovery bytegenau zurück.
Mit dem wiederhergestellten Exact Index läuft diese vollständige Prüfung in
etwa einer Sekunde statt wiederholt alle Container zu durchsuchen.

Zwei weitere Regressionen prüfen ein spätes Überschreiben im zuvor
eingefrorenen Bereich und einen injizierten Fehler bei der Publikation nach
einem Reset. Aktuelle Bytes, Frozen-Präfix, residenter Fehler-Fallback und
der später erfolgreich wiederhergestellte Commit stimmen bytegenau.

- Workspace: **777 bestanden, 0 fehlgeschlagen, 16 ignoriert**.
- `cargo clippy --workspace --all-targets -- -D warnings`: bestanden.
- Temporäre `[DEBUG-rechunk-cut]`-Ausgaben aus dem Quellcode entfernt.
- Frühere uncommittete Hotpath-Änderungen bleiben erhalten.

Reproduzierbarer Testaufruf:

```bash
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test -p fastdup-appliance --test write_through_ingest \
  adjacent_reordered_writes_preserve_stable_ingest_prefix -- --exact
```

## SMB-Diagnose

Sechs Wiederholungen mit dem unveränderten Binary des ursprünglichen
Ausreißers und acht erfolgreiche instrumentierte Läufe zeigten den großen
Rest nicht erneut. Ein neunter instrumentierter Lauf scheiterte beim
Beenden des FUSE-Daemons nach 120 Sekunden. Er bleibt als fehlgeschlagenes
Diagnoseartefakt erhalten und wird nicht als erfolgreicher Benchmark gewertet.
Der übrig gebliebene Test-Mount wurde entfernt. Der Shutdown-Fehler wurde
in dieser Diagnose nicht auf eine Ursache eingegrenzt.

Nachtrag: Die [anschließende Shutdown-Diagnose](shutdown-signal-loss-2026-09-06.md)
reproduziert den Hänger durch ein verlorenes SIGINT während eines
Supervisor-Zweigs und behebt ihn durch einen dauerhaft registrierten Empfänger.

Die instrumentierten Versuche dienen der Fehlersuche; zeitweise parallel
laufende Diagnosearbeit macht sie ungeeignet für einen Durchsatzvergleich.
Aus ihrem Ausbleiben lässt sich keine nachträgliche Erklärung des ursprünglichen
Ausreißers ableiten.

## SMB-Abnahme: Normal und Advanced

Das fertig gebaute, von Diagnoseausgaben bereinigte Binary wurde mit dem
unveränderten Runner des Skills `smb-single-stream-benchmark` geprüft.
Drei Durchgänge wechseln jeweils Normal (`off`) und Advanced (`dependent-v1`)
ab. Jeder der sechs Läufe lädt dieselbe Rocky-10.2-Minimal-ISO dreimal
sequenziell hoch und misst nach zwölf Sekunden, während alle drei Dateien
noch vorhanden sind. Es liefen keine Builds oder CPU-Testproben parallel.
Alle sechs Läufe bestanden einschließlich Cleanup; Prozess-Swap war immer null.

| Median aus je drei Läufen | Normal | Advanced / Similarity |
| --- | ---: | ---: |
| Aggregierter SMB-Schreibdurchsatz | 1.333,30 MiB/s | 1.207,10 MiB/s |
| Vollständiger Datei-Put, p99 / Maximum | 1.920,89 ms | 2.358,52 ms |
| Repository alloziert, einschließlich Metadaten | 1.907,85 MiB | 1.902,46 MiB |
| Datenreduktion | 67,8234 % | 67,9143 % |
| Reduktionsfaktor | 3,10785× | 3,11665× |
| Rechunk-Bytes über den gesamten Lauf | 2,680 MiB | 2,695 MiB |

Advanced ist hier im Median 9,46 % langsamer und spart gegenüber Normal
weitere 5,39 MiB Repository-Allokation beziehungsweise 0,0909 Prozentpunkte
der logischen Datenmenge. Die Aussage gilt für diese ISO mit zwei vollständigen
Duplikaten; sie ist keine allgemeine Bewertung anderer Similarity-Korpora.
Mit drei Put-Samples ist p99 gleich dem längsten vollständigen Datei-Put,
nicht einer Latenz einzelner SMB-Requests.

Der größte einzelne Checkpoint-Rest beträgt in diesen sechs Läufen 1,216 MiB.
Ein Checkpoint kann mehrere Dateien enthalten. Diese Abnahme bestätigt
erfolgreiche Normal-/Advanced-Läufe, ist aber kein gepaarter Vorher-/Nachher-
Durchsatznachweis der Reset-Änderung. Rohwerte je Lauf stehen in
`benchmarks/smb-rechunk-fixed-20260906/summary.json` unter `.artifacts`.

## Artefakte

Alles liegt unter `/source/fastdup/.artifacts/`:

- `diagnose-rechunk-cut-20260906/`: Vorher-Sicherung, rote/grüne Tests,
  Diagnoseprototypen, Workspace-/Clippy-Logs, Release-Quellen und Provenienz.
- `benchmarks/smb-rechunk-investigation-20260906/`: unveränderte Wiederholungen.
- `benchmarks/smb-rechunk-instrumented-20260906/`: instrumentierte Versuche
  einschließlich des fehlgeschlagenen Shutdowns.
- `benchmarks/smb-rechunk-fixed-20260906/`: Abnahme des fertigen Release-Binaries.

SHA-256 des fertigen Binaries:
`6b464d3d98fcc6f9b1e5e5a5bc0eb5a32935487a21783c2ea59b7dd50fee2ea2`.
