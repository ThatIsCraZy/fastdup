---
status: accepted
---

# Fence writer downgrade in the authoritative Commit chain

Every Commit Record uses format v2 and carries Repository Format Epoch one in
the field at offset 22. The current writer reads and writes exactly epoch one.
Append, recovery, and offline Scrub validate every retained record before graph
fallback. Epoch zero, Commit format v1, and unknown epochs are unsupported
pre-production state and fail closed before repository mutation.

The Commit chain is the downgrade fence because it is already the authoritative
mutation boundary and older binaries reject the v2 record structurally. A
separate marker file was rejected because binaries predating the marker could
ignore it. Since the repository has not shipped, there is no epoch-zero import,
upgrade transaction, downgrade writer model, or migration utility to maintain.
The first Commit of every repository already carries the fence.

Policy Set and Repository Format Epoch remain distinct. A Policy Set identifies
chunking, encoding, placement, and maintenance choices for one generation; the
epoch fences repository-wide reader/writer compatibility. Object-local versions
still determine how individual immutable bytes decode. Under ADR 0074 the
writer accepts only the one current Policy Set from the first Commit. Current
object writers and readers likewise have no compatibility obligation to
superseded pre-production encodings.
