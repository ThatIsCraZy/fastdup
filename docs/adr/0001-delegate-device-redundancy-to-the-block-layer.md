---
status: accepted
---

# Delegate device redundancy to the block layer

The later production profile will protect against the loss of one NVMe or HDD
through a redundant block-device layout beneath XFS; fastdup will not initially
implement its own replication or erasure coding. The logical model still permits
multiple physical locations so relocation, repair, and a later evidence-driven
replication design do not leak placement into manifests. The MVP intentionally
covers process and power loss only on functioning storage and makes no device-
loss claim.

## Consequences

The MVP contains no RAID recognition or degraded-array behavior. Before the
single-device-loss production claim is enabled, deployment validation and health
policy must be implemented and tested against certified redundant layouts.
`Location[]` never by itself implies independent replicas.
