---
status: accepted
---

# Isolate physical tier capacity with hard quotas

The v1 production profile places Metadata and DATA on distinct XFS
filesystems. Writable startup resolves both roots to their mounted device and
filesystem type and fails closed when the device identities match or either
filesystem is not XFS. Persistent Pool IDs and roles from ADR 0080 identify the
logical pools; this check independently proves their physical capacity fault
domains.

`FASTDUP_POOL_ISOLATION=lab-allow-shared` is an explicit non-production bypass
for single-disk development and CI. The default and the only production value
is `required`; malformed policy fails before either storage root is opened.

The Metadata filesystem also contains the Small-File Container directory from
ADR 0084. Startup assigns it an inheriting XFS project and installs a hard
quota through `SmallFileTierIsolation::prepare`; configuration that would
consume the protected Metadata floor is rejected. The lab policy reports quota
enforcement as bypassed. Any future disk-backed cache needs the same independent
capacity boundary.

The [XFS/FUSE exhaustion qualification](../testing/full-tier-enospc.md), last
exercised on 2026-09-01, fills DATA and the Small-File quota and checks rejected
write invisibility, reads, cleanup, scrub, and remount. Its loop-device evidence
does not establish power-loss or hardware-cache behavior.
