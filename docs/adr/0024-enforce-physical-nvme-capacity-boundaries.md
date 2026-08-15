---
status: accepted
---

# Enforce physical NVMe capacity boundaries

Commit-critical metadata, WAL, indexes, and rebuild state receive a physically
enforced reserve through separate XFS filesystems or hard project quotas. Small-
file data and hot cache have independent quotas and cannot borrow that reserve.
Before acknowledging a mutation, fastdup pessimistically reserves enough
metadata and container capacity to complete its bounded commit; failure returns
an expected capacity error while reads and cleanup remain available.

## Consequences

Policy-known small files begin on NVMe and spill new records to HDD above an
initial 8 MiB hysteresis threshold; unknown families begin on HDD unless an
allowed hint says otherwise. Existing immutable records need not move
synchronously. Cache Locations are removable extras and never sole durable
coverage, while Small-File Locations are ordinary durable Locations. Persisted
Pool IDs, appliance ownership, roles, redundancy, and mount options are validated
before write admission; mount paths are not identities.
