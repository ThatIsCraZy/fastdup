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

## VM validation of 0.6.4-14

The operator explicitly approved restarting the running backup. After stopping
SMB and allowing the old repository process to exit, the verified RPM was
installed and the services restarted. No repository reinitialization occurred.

The new process (PID 61396) started at 02:38:51 CEST and reported its mount at
02:39:10. The host-side probe observed the mount after 19.051 seconds. The
`namespace_commit` phase itself took 1,783 ms and inode reservation 8 ms; the
remaining startup time includes active-index recovery. There were 42,346
published Containers by then, compared with 26,960 at the preceding structural
start. These are consecutive operational measurements, not controlled
cold-cache A/B results or a claim that the deployed restart simulated power loss.
Crash behavior is covered separately by the injected-fault tests above.

A local SMB 3.1.1 probe wrote 4 MiB, flushed, overwrote the header, appended a
tail, flushed again and read back all 4,194,320 bytes exactly in 0.276 seconds.
SHA-256: `7ab541d4f9cb148b75309956e36ad58eb551a2f92dad7775fe54e2c08c8a1621`.
The temporary probe file was deleted; traffic stayed on VM loopback, not VPN.
This is a functional check, not a sustained Veeam throughput benchmark.

The initial scrub was running without error, with 651 Containers verified and
1,062,723,840 bytes read at the first sample. All five cache pools were present.
Repository, agent, control and Samba services were active with no automatic
restarts; process swap was zero. The full initial scrub had not yet completed.
