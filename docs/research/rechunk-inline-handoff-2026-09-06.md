# Rechunk-Rest durch eine Lücke bei der Inline-Rezeptübergabe

Stand: 2026-09-06.

Ein weiterer reproduzierbarer Grund für große Rechunk-Reste ist gefunden und
behoben: Ein später Ingest-Job konnte vollständige Chunks aus dem bereits
eingefrorenen Präfix entnehmen und dessen Exact-/FILL-Rezepte erst nach der
Freigabe seiner Lane an Namespace übergeben. Der Checkpoint konnte genau
dazwischen planen und musste den vollständigen Bereich erneut chunken.

Das beweist **nicht** die Ursache des einzelnen historischen SMB-Samples aus
[Audit 8](hotpath-audit8-2026-09-06.md). Dort wurden von 30.285.275 Rechunk-Bytes
30.165.986 Bytes als neue Chunks encodiert; die kontrollierte Exact-Reproduktion
trifft dagegen überwiegend bereits bekannte Chunk IDs. Die historischen
Repository-Wurzeln sind nicht mehr vorhanden, und der erhaltene Report enthält
keine Offset-/Sequence-Spur. Die ähnliche Größenordnung allein erlaubt keine
eindeutige Zuordnung. Auch die bereits dokumentierte
[Lane-Reset-Ursache](rechunk-lane-reset-2026-09-06.md) bleibt eine separate
Reproduktion, kein nachträglicher Beweis für dieses Sample.

## Kontrollierter Ablauf

1. Eine Basisdatei macht reproduzierbare Daten für Exact-Reuse verfügbar.
   Der FILL-Fall benötigt keine Basisdatei.
2. Eine zweite Datei erhält 28 MiB sequenzielle Ein-MiB-Writes. `Sync` wartet
   deren Ingest ab; `begin_commit` friert dieses Präfix ein.
3. Weitere acht MiB werden im Active Epoch geschrieben. Ihr Ingest-Job erreicht
   die Container-Schwelle und extrahiert auch den vorher eingefrorenen Tail.
4. Der Worker pausiert nach Freigabe der Lane und vor seiner Job-Retirement.
   Im alten Ablauf liegen die Inline-Rezepte noch im Rückgabewert von
   `stage_write_batch` und sind für Namespace unsichtbar.
5. `checkpoint_profiled` darf den Frozen Cut fertigstellen. Die Ingest-Fence
   umfasst den späteren Job nicht; die Publication-Fence kennt nur detached
   Container. Der Lane-Drain findet das entnommene Präfix nicht mehr.

Vor der Korrektur werden in beiden dauerhaften Regressionstests exakt
**29.360.128 Byte (28 MiB)** erneut gechunkt. Der ursprüngliche Diagnoseprototyp
zeigt nach alleiniger Änderung der Übergabereihenfolge **94.698 Byte** im
Exact-Fall. Der Zähler misst zusätzliche Fallback-Arbeit im Checkpoint, keine
allgemeine SMB-Durchsatzsteigerung. Die Pause erzwingt ein legales Interleaving;
sie misst nicht dessen Häufigkeit im Betrieb.

Die drei geprüften Erklärungen waren verspätete Übergabe, verworfene Lane-Daten
und abgelehnte Frozen-Rezepte. Der Ablauf benötigt keinen Reset, und allein
das Vorziehen der Übergabe beseitigt den großen Rest: Die Frozen-Rezepte sind
inhaltlich zulässig, kommen aber im alten Ablauf zu spät.

## Korrektur und Grenzen

`stage_write_batch` installiert Inline-Rezepte, solange es die Lane besitzt,
und liefert anschließend nur noch den Erfolgs-/Fehlerstatus. Damit gehören
Entnahme und Übergabe aus Sicht des Commit-Drains zusammen. Die spätere
Übergabe in `process_job` entfällt. Die übrigen Queues und ihre endlichen
Warteziele bleiben bestehen; der Checkpoint wartet nicht pauschal auf sämtliche
nach dem Cut angenommenen Writes.

Namespace prüft weiterhin Mutation Sequence und vollständige Bereichsabdeckung.
Die Übergabe verarbeitet ausschließlich Speicherzustand. Observer geben den
Inode-State-Lock vor Queue-Admission frei, und Externalization erwirbt keinen
Observer-order-Lock. Die verlängerte Lane-Sperre serialisiert diese Übergabe
mit dem Commit-Drain; sie umfasst keine zusätzliche Storage-Publikation.

Die Regressionen verwenden öffentliche Write-/Sync-/Checkpoint-/Read-/Recovery-
Operationen und einen pro Appliance gekapselten, ausschließlich unter
`cfg(test)` vorhandenen Pausenpunkt. Beide müssen den Cut während der Pause
abschließen, unter einem MiB Rechunk bleiben, live alle 36 MiB bytegenau lesen
und nach simuliertem Crash ausschließlich das eingefrorene 28-MiB-Präfix
zurücklesen. Der Pausenpunkt fügt Produktions-Builds weder Felder noch Aufrufe
hinzu. Es gibt keine Änderung des dauerhaften Formats und kein neues Unsafe.

## Reproduktion und Artefakte

```bash
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test -p fastdup-appliance --lib checkpoint::ingest_handoff_tests
```

Unter `.artifacts/diagnose-rechunk-origin-20260906/` liegen die unveränderte
Quellbasis, der historische Checkpoint-Code, Diagnoseprototyp und Gegenprobe
sowie `regression-red.log` und `regression-green.log`. Beide Regressionen
wurden vor der Korrektur rot und danach einschließlich Recovery grün ausgeführt.
Temporäre Dateisystem-Gates und `[DEBUG-rechunk-origin]`-Probes wurden aus dem
Produktions- und Testcode entfernt.

Validierung: **783 Workspace-Tests bestanden, 0 fehlgeschlagen, 16 ignoriert**;
`cargo clippy --workspace --all-targets -- -D warnings` bestanden. Der erste
Workspace-Build brach wegen erschöpftem Speicherplatz beim Linken ab. Nach
Bereinigung neu erzeugbarer Cargo-Artefakte bestand der vollständige Wiederlauf;
beide Logs bleiben erhalten. Es wurde kein neuer SMB-Durchsatzvergleich gemessen.

Für die eindeutige Zuordnung eines künftigen natürlichen Ausreißers benötigt
die Diagnose die betroffenen Inodes, Fallback-Offsets und -Längen sowie
Cut-/Chunk-Sequences und Reset-/Rezeptübergabe-Ereignisse desselben Laufs.
