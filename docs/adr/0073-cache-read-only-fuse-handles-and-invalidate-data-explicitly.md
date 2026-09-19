---
status: superseded by ADR 0046
---

# Cache read-only FUSE handles and invalidate DATA explicitly

This record allowed `FOPEN_KEEP_CACHE` for read-only files and required explicit
range invalidation after mutation. ADR 0046 replaced that kernel-cache policy:
all regular FUSE handles use `FOPEN_DIRECT_IO`, repository file content is owned
by the Unified Read Cache, and shared file mappings remain unsupported. Zero
attribute/name TTL and the prohibition on FUSE writeback remain current.
