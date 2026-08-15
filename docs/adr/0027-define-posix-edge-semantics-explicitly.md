---
status: accepted
---

# Define POSIX edge semantics explicitly

fastdup preserves byte-exact case-sensitive names without Unicode normalization,
uses monotonically increasing never-reused 64-bit Inode IDs, and retains an
unlinked inode while live handles exist. After a crash no handle survives, so a
durably unlinked open orphan need not reappear. Samba handles its Unicode and
case policy above this byte-oriented namespace.

## Consequences

Hole punching creates HOLE extents; zero-range creates allocated FILL(0) DATA.
Successful fallocate pessimistically reserves RAW physical capacity, including
KEEP_SIZE reservations, while collapse/insert range initially return
`EOPNOTSUPP`. `O_SYNC` and `O_DSYNC` share the deliberate ten-second durability
semantics of `fsync`. Version 1 uses relatime; mtime, ctime, and explicitly set
times remain ordinary atomic metadata mutations.
