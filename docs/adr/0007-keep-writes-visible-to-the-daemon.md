---
status: accepted
---

# Keep writes visible to the daemon

fastdup v1 disables the FUSE writeback cache and does not support writable shared
memory mappings because either could acknowledge dirty data before the daemon can
start its ten-second durability deadline. Every acknowledged write must first
reach fastdup and receive a per-inode mutation sequence. Read-only mappings and
read caching remain allowed.

## Consequences

The initial path uses FUSE write-through plus a fastdup-owned dirty-extent overlay
for coherent live reads. Concurrent writes invalidate affected kernel read-cache
ranges; `direct_io` remains separately benchmarkable. Locks are transient and
checked before mutation admission. If a commit has not progressed after five
seconds, the appliance stops admitting writes and reports a critical storage
fault while prioritizing already admitted data. Arbitrarily stalled total I/O is
outside the supported failure envelope.
