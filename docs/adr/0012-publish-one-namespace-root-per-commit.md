---
status: accepted
---

# Publish one namespace root per commit

Directory entries map byte-exact names to stable Inode IDs, while immutable Inode
Versions reference Manifest Roots and POSIX metadata. One checksummed Commit
Record containing its generation, predecessor, and Namespace Root ID is the only
transactional visibility point. Data containers and immutable metadata objects
must already be durable before that record can be published.

## Consequences

Hardlinks share one Inode Version transition, renames update the namespace
atomically, and symlink targets remain byte-exact. Large ACLs, xattrs, and symlink
targets may use immutable content-identified Metadata Objects behind an explicit
versioned inline threshold. Objects made durable but never selected by a valid
Commit Record are harmless orphans eligible for later GC.
