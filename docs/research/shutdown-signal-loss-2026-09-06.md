# Shutdown-Hänger durch verlorenes SIGINT

Stand: 2026-09-06. Der im
[Rechunk-Diagnoselauf](rechunk-lane-reset-2026-09-06.md) beobachtete
Shutdown-Hänger wurde am echten Daemon reproduziert und behoben.

## Ursache

Der Supervisor erzeugte `tokio::signal::ctrl_c()` in jeder Runde von
`tokio::select!` neu. Gewann ein anderer Zweig, wurde die wartende Future mit
ihrem Signalempfänger verworfen. Während der Supervisor anschließend diesen
Zweig bearbeitete, konnte SIGINT ohne aktiven Empfänger eintreffen. Der globale
Signalhandler blieb installiert: Der Prozess endete deshalb auch nicht durch
die standardmäßige SIGINT-Aktion. Der später neu erzeugte Empfänger sah das
bereits verarbeitete Signal nicht mehr.

Die Hauptschleife lief daraufhin weiter, einschließlich ihrer Telemetrie.
Genau dieses Verhalten zeigte der ursprüngliche fehlgeschlagene SMB-Lauf,
den dessen Runner nach 120 Sekunden beenden musste.

## Reproduktion und Gegenprobe

Eine direkte Probe startet den Release-Daemon auf zwei separaten Datenträgern
und sendet einmal SIGINT. Ohne gezielt getroffenes Zeitfenster beendet sich
der bisherige Daemon in etwa 32 ms.

Die Timing-Probe verlängert ausschließlich den dritten ausgewählten
Supervisor-Tick um 500 ms und meldet den Eintritt mit einem Diagnosemarker.
Der externe Prozess sendet SIGINT innerhalb dieses Fensters. Dabei läuft der
tatsächliche Supervisor; Dateisystem, Signalhandler und Shutdown-Pfad werden
nicht durch Mocks ersetzt. Bei einem Hänger sichert die Probe Thread-Stacks.

| Gleiche Timing-Probe | Ohne Fix | Mit Fix |
| --- | --- | --- |
| Drei Wiederholungen | 3/3 hängen nach 3 s | 3/3 sauber beendet |
| Shutdown-Zeit | kein Abschluss | 0,518 / 0,566 / 0,568 s |
| FUSE selbst ausgehängt | nein | jeweils ja |
| Recovery-Latch nach sauberem Ende gelöscht | nein | jeweils ja |

Die Zeiten mit Fix enthalten die künstliche Pause von 500 ms und sind keine
gewöhnlichen Shutdown-Latenzen. Eine weitere Probe schreibt vor SIGINT eine
noch nicht committete Datei. Der Daemon beendet sich sauber; nach Neustart
stimmen sämtliche 1.048.576 Byte mit dem ursprünglichen Inhalt überein.

## Änderung und Regression

`ShutdownSignal` besitzt einen einmal registrierten Unix-Signalempfänger.
Der Supervisor hält diesen Besitzer über seine komplette Ereignisschleife.
Einzelne `recv()`-Wartevorgänge dürfen weiterhin durch `select!` abgebrochen
werden; während anderer Arbeit eintreffende Interrupts bleiben im Empfänger
vermerkt. Die Registrierung erfolgt vor dem ersten Wartevorgang.

Ein ausgewählter Checkpoint darf weiterhin fertiglaufen. Anschließend führt
der Supervisor den vorhandenen geordneten Shutdown aus: Mutation Admission
schließen, Hintergrunddienste stoppen, Commit aufholen, FUSE aushängen und
erst dann den Recovery-Latch löschen. Weder ein erzwungenes Prozessende noch
ein Timeout ersetzt diesen Ablauf. Containerformat und Write-/Read-Hotpaths
ändern sich nicht.

Der vor dem Fix rot ausgeführte Regressionstest sendet ein echtes SIGINT in
einem isolierten Kindprozess. Ein zweiter Empfänger bestätigt ohne Sleep,
dass Tokio das Signal tatsächlich während der Bearbeitung eines anderen
Zweigs zugestellt hat. Der Test prüft zwei Fälle: einen zuvor abgebrochenen
Signal-Wartevorgang und ein Signal vor dem ersten Wartevorgang. Vorher läuft
die Erwartung nach 200 ms ab; mit dem Fix sind beide Benachrichtigungen
unmittelbar verfügbar.

```bash
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test -p fastdup-appliance --bin fastdup-durable-fuse -- \
  --exact tests::shutdown_signal_survives_busy_supervisor_branch
```

Die Lebensdauer des Signalempfängers ist auch in
[ADR 0070](../adr/0070-arm-a-recovery-latch-before-repository-access.md)
festgehalten. Die Diagnosepause und sämtliche `[DEBUG-shutdown]`-Ausgaben
sind aus dem produktiven Quellcode entfernt.

## Abschließende Validierung

- Workspace: **778 bestanden, 0 fehlgeschlagen, 16 ignoriert**.
- Alle acht Daemon-Tests auch am final formatierten Quellstand bestanden.
- `cargo clippy --workspace --all-targets -- -D warnings`: bestanden.
- SingleStream-SMB mit dem unveränderten Benchmark-Skill: ein Lauf mit Normal
  und ein Lauf mit Advanced / Similarity, jeweils drei vollständige ISO-Uploads
  und zwölf Sekunden Settle-Zeit. Beide Läufe enden erfolgreich einschließlich
  SIGINT-Shutdown und Cleanup, bei null Prozess-Swap.
- Keine parallelen Builds oder CPU-Testproben während dieser SMB-Abnahme.

Die vollständigen Daemon-Proben prüfen neben Exit-Code null auch das vom
Daemon ausgeführte Unmount und die entfernte Recovery-Latch-Datei. Der
Signalverlust wird damit durch Erhalt der Benachrichtigung behoben, ohne die
bisherige Durability-Abschlussfolge abzukürzen.

## Artefakte

- `/source/fastdup/.artifacts/diagnose-shutdown-20260906/`: Ausgangsquellen,
  Timing-Probe, rote/grüne Regression, Thread-Stacks, Recovery-Probe,
  Validierungslogs und finale Build-Provenienz.
- `/source/fastdup/.artifacts/benchmarks/smb-shutdown-fixed-20260906/`:
  SingleStream-SMB-Abnahme mit dem fertigen Binary.

SHA-256 des bereinigten Release-Binaries:
`d37effa0d34fa4b8e990a0640fa2c267b6dba0555ca8c7e733cf9ac45535cd59`.
