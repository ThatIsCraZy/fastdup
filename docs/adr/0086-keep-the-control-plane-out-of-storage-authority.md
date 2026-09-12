---
status: accepted
---

# Keep the Control Plane out of storage authority

The appliance exposes one privileged local Control Plane, but Pool identities,
Commit records, Manifests, and verified Containers remain the only repository
authority. Control configuration is published transactionally and reconciled
with the live Repository Runtime; telemetry is independently rebuildable, so a
missing or corrupt UI database can disable management without changing content,
liveness, recovery, or Scrub decisions.

The network-facing web process is unprivileged. A separate root-owned agent
accepts only versioned typed commands over a credential-checked Unix socket and
performs topology validation again immediately before destructive provisioning.

Persisted lifecycle intent does not prove Runtime health. Mount presence and the
repository service's live process/result determine availability. A failed service
(including an assertion/SIGABRT or OOM termination) is reported even while systemd
waits to restart it. A stale mount entry cannot hide a stopped/failed process.
Probe failures are unknown evidence, not proof of an outage. Mount inspection
reads the kernel mount table and does not block on a FUSE getattr; service
inspection has a bounded wait.

Runtime metrics are a separate observation. A missing or slow management reply
clears stale rates/details but does not turn an active mount/process into Error.
Ordinary closed mutation admission is backpressure, not repository failure.
An explicitly reported integrity failure remains an alarm; a missed metrics
sample cannot clear it. A confirmed process crash also requires a new successful
Runtime observation before its alarm clears: an old FUSE entry may briefly
outlive the previous process during auto-restart. Actual mount/process failures remain visible in the top
bar even when the agent connection itself is live. Intentional unmount,
initialization and startup recovery retain distinct states. Startup becomes
Online only after mount, process and management readiness are established.

Both snapshot repository state and streamed telemetry expose the same observed
state. Physical disk observations remain independently available. Health
transitions are audited once; successful evidence clears the current issue
without turning the control database into storage authority. The serialized
legacy `write_blocked` issue remains readable for old telemetry, but new health
samples do not emit it as a repository failure.
