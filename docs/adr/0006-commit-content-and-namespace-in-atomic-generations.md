---
status: accepted
---

# Commit content and namespace in atomic generations

fastdup periodically commits one complete generation containing a contiguous
prefix of accepted file-content and namespace mutations. Recovery selects the
newest wholly valid generation and never assembles a state from independently
newer pieces. This satisfies the ten-second bound without treating an arbitrarily
long open-to-close write session as one transaction.

## Consequences

Successful `create`, `rename`, `unlink`, links, size changes, allocation changes,
ACL changes, and xattr changes share the same ten-second commit bound as writes.
An atomic rename recovers wholly before or wholly after replacement, including
link counts and target identity. Applications needing a transaction across many
writes publish a separately written file with atomic rename. Accepted mutations
receive a monotonic per-inode sequence; commit generations contain contiguous
sequence prefixes, and a later accepted overlapping write wins. Concurrent
overlap is supported for correctness but is not a target hot path.
