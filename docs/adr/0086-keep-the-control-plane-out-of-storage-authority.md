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
