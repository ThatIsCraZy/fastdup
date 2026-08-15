---
status: accepted
---

# Require an honest stable-storage stack

fastdup's crash contract requires storage that propagates file and directory
syncs to stable media. Volatile caches without working Flush/FUA or power-loss
protection and barrier-disabling mount modes are unsupported. Internal XFS pools
use a certified 4 KiB block and alignment profile with barriers enabled and
internal `noatime`; checksums, rather than assumed sector atomicity, detect torn
writes.

## Consequences

Direct-I/O buffers and format blocks are 4 KiB aligned. Larger-sector profiles
need an explicit future format/I/O decision. XFS reflink may assist offline
maintenance or temporary generations but never establishes deduplication truth.
Device redundancy validation and degraded-array behavior are intentionally not
part of the MVP implementation.
