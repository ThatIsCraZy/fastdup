---
status: accepted
---

# Admit Logical Share quota at the Namespace seam

Each managed SMB Share may define one Logical Share quota as a small decimal
value with a GB, TB, or PB unit. The quota root is the stable directory derived
from the Share's immutable management identity, so renaming a Share does not
move data or change quota ownership.

Usage is the sum of POSIX-allocated logical bytes for regular inodes reachable
below that root. DATA, FILL, Exact-Dedup and metadata Clone ranges count at full
logical length; sparse holes do not. Hard links inside one quota tree count once.
Hard links and renames across quota boundaries return `EXDEV`, avoiding
ambiguous ownership and an O(subtree) mutation in the frontend path.

Every allocation-changing write, truncate, fallocate and clone computes its
exact before/after allocation delta while holding the inode mutation lock. A
per-Share atomic ledger reserves positive deltas before live mutation; exceeding
the limit returns `ENOSPC`. Failed mutations return their reservation, successful
shrinks release usage, and a deleted inode releases its remaining usage only
when its final open/lookup pin is gone. Concurrent writers therefore cannot
overshoot the limit; the operator's tolerated one-to-two-gigabyte error is not
consumed by the implementation.

Policy replacement fences mutation admission, reconstructs membership and
usage from the live Namespace, and rejects limits below current usage. The
root-only agent publishes the last accepted policy as a fsynced, atomically
renamed manifest. Repository recovery loads and validates that manifest before
the FUSE mount begins serving requests. Hot replacement updates the same
Namespace ledger through the typed root-only management socket; the web process
never owns or performs quota admission.

Share `statfs` total equals the logical quota. Free and available bytes are the
minimum of remaining logical quota and current Repository-wide physical
availability. Physical commit-capacity admission remains independent and may
return `ENOSPC` before the logical quota when reduced data cannot fit on the
actual Pools. A Logical Share quota is not a physical reservation or a promise
that its nominal logical size can be reached for every workload.
