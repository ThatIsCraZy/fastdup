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

The current Metadata filesystem contains only commit-critical state. Before
ADR 0084 enables Small-File placement, that feature must add an independent XFS
project quota or filesystem so it cannot borrow the Metadata reserve. The same
rule applies to any future disk-backed cache. Production qualification fills
each noncritical quota and proves that one bounded Metadata commit and cleanup
remain possible.
