---
status: accepted
---

# Deliver a POSIX exact-dedup MVP before advanced reduction

This is a completed historical milestone rather than the current product
boundary. Similarity/Prefix, automatic GC, and Samba qualification now exist;
ADR 0054 supersedes FastCDC. DATA-tier disaster recovery and production
Small-File placement remain open.

The MVP is a usable FUSE low-level POSIX filesystem with immutable hierarchical
manifests, bounded random updates, FastCDC, Exact Dedup, Bloom/hot acceleration,
and independently selectable RAW/Zstd encoding. Similarity, Delta, automatic GC,
full production Samba hardening, and single-device-loss protection follow only
after this correctness and performance baseline is measured. Stage 0/1 still
implements the faultable durable Container Store first.

## Consequences

Exactly one daemon holds the durable Appliance Lease and may advance generations;
offline maintenance also requires exclusivity. FUSE assigns per-inode mutation
orders while processing independent inodes concurrently, with writeback and
writable shared mmap disabled. Samba remains a tested consumer of the POSIX
mount, not a second protocol implementation. The initial Small-File Policy
matches `.xml`, `.json`, and explicit placement hints case-insensitively without
normalizing stored names.
