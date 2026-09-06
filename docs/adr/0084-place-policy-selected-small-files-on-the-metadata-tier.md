---
status: accepted
---

# Place policy-selected Small Files on the Metadata Tier

Policy-known Small Files begin in their protected Metadata-Tier quota and spill
new records to the Data Tier above an initial 8 MiB hysteresis threshold;
unknown families begin on Data unless an allowed hint says otherwise. Existing
immutable records do not move synchronously. Small-File Locations are ordinary
durable coverage, while Cache Locations remain removable extras and may never
be the sole coverage of a live Logical Chunk.

## Implemented policy and capacity boundary

The default suffix policy selects `.xml` and `.json` case-insensitively.
`user.fastdup.placement=metadata` or `data` overrides the name match; a logical
size above 8 MiB always selects DATA for new publication. The current selector
re-evaluates size and policy; it is not a persisted one-way "has spilled" bit.
A later shrink or policy change may therefore select Small-File placement again.
The suffix list can be replaced atomically at runtime; a Frozen Commit Cut
retains its acquired policy. None of these choices synchronously relocates
already published immutable records or defeats Exact reuse.

`SmallFileTierIsolation` prepares `.fastdup-small-file-containers` on the
Metadata filesystem and installs an inheriting XFS project hard quota under
the required production isolation policy (ADR 0081). Small-File writes reserve
both physical Metadata headroom and their own quota bucket before mutation
(ADR 0082). `TieredStorageIo` exposes both Container directories through one
read, discovery, and verification seam; tier placement is not content identity.
Small-File coverage on Metadata alone does not provide device-loss protection.

Evidence: [placement and live-policy tests](../../crates/fastdup-posix/tests/small_file_placement.rs),
[tier-neutral publication/read test](../../crates/fastdup-store/tests/tiered_container_repository.rs),
[capacity-admission tests](../../crates/fastdup-appliance/tests/commit_capacity.rs),
and the [2026-09-01 XFS/FUSE quota exhaustion qualification](../testing/full-tier-enospc.md).
