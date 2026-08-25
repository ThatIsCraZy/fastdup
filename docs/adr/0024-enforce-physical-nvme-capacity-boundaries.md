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

`statfs` reports post-reduction physical data-tier capacity rather than
multiplying space by a measured or expected reduction ratio. It removes a
ten-percent operating reserve from total and available blocks. Crossing the
same reserve on the metadata tier reduces client-visible available blocks to
zero even while the data tier still has space. These values are a current
observation, not a reservation made for `fallocate` or a later write.
The daemon samples both backing filesystems before mounting and refreshes the
cached observation every five seconds on a dedicated thread. FUSE `statfs`
replies never issue backing-filesystem I/O or enter the write worker pool;
Samba may query free space while writing.

An explicit reporting override may replace total and available bytes for
qualification, demonstrations, or a later administratively defined logical
quota. The override changes only `statfs`; physical mutation admission and
`ENOSPC` behavior remain authoritative. Production startup rejects incomplete,
zero, nonnumeric, or internally inconsistent override pairs.

## Consequences

Policy-known small files begin on NVMe and spill new records to HDD above an
initial 8 MiB hysteresis threshold; unknown families begin on HDD unless an
allowed hint says otherwise. Existing immutable records need not move
synchronously. Cache Locations are removable extras and never sole durable
coverage, while Small-File Locations are ordinary durable Locations. Persisted
Pool IDs, appliance ownership, roles, redundancy, and mount options are validated
before write admission; mount paths are not identities.
