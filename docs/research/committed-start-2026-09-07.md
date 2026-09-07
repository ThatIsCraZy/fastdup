# Committed startup — 2026-09-07

The preceding structural startup spent 775,728 ms validating the Namespace and
Container structures on the VM (26,960 published Containers). It still visited
every Container and could repeat the structural pass to resolve independent
Bases. The new normal path keeps committed Metadata validation but transfers
DATA availability/integrity verification to the paced initial scrub (ADR 0091).

A deterministic fixture publishes one referenced RAW Container and 32 unrelated
Containers. The original structural entry point performs 232 DATA storage
operations; the committed entry point performs zero. This asserts the actual
absence of a DATA inventory visit rather than a machine-specific elapsed-time
threshold. Writable startup separately checks that no directory scan occurs;
its paired Container-generation reservation WAL remains part of admission.

The scrub receives required identities/lengths from exactly the graph selected
at startup. It verifies each published Container and its dependent Bases, then
removes matched identities from the outstanding set using the same in-memory
Container image. Entirely missing Containers cannot disappear from the audit
merely because they are absent from the directory listing. RETIRING Locations
cannot satisfy the set. Successful full verification and an empty outstanding
set are both required before the existing GC scheduling gate opens.

Targeted cases cover missing Container files, damaged seals and payloads,
missing independent Bases, healthy dependent verification, RETIRING rejection,
corrupt newest Metadata without rollback or WAL mutation, and torn/invalid/
broken-chain WAL suffixes. Before/after-every-operation fault injection around
committed recovery and suffix truncation preserves the selected Commit and
byte-exact contents after another crash. The existing DATA/Metadata checkpoint
fault matrix also runs the new writable recovery and initial dependency scrub
against its previous-or-complete-generation oracle.

No new disk format or reusable verification certificate is introduced. The
selected Metadata graph is still eagerly validated; this is not a claim of
fully lazy Namespace/Manifest loading or constant-time Metadata recovery.

Artifacts and execution logs are under `.artifacts/committed-start/`.

Validation completed: 150 store/appliance library and binary tests, 60 appliance
integration tests, 56 end-to-end maintenance tests and 9 Recovery Checkpoint
fault tests passed. Production Clippy passed with warnings denied. The package
build includes TypeScript checking and the embedded WebUI build.
