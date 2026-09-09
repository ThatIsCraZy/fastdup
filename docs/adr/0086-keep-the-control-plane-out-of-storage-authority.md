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

Persisted lifecycle intent does not prove Runtime health. Each live sample
reconciles it with a bounded management inspection: an unreachable Runtime or
closed mutation admission cannot be reported as healthy Online. Intentional
unmount, initialization and startup recovery retain their distinct states.
Both snapshot repository state and streamed telemetry expose the observed state;
loss of counters clears frontend rates and their baseline. Physical disk
observations remain independently available. Runtime failures and write pauses
remain visible in the top bar even when the agent connection itself is live.
Health transitions are audited once, and successful observations clear the
current issue without turning the control database into storage authority.
