---
status: accepted
---

# Do not turn RoW history into implicit snapshots

The MVP exposes only the current Namespace Root and provides no user snapshot,
version-history, undelete, or per-file rollback API. The current and immediately
previous online generations remain live for atomic recovery. Older WAL records
may survive diagnostically but do not retain their object graphs.

## Consequences

The current and previous complete Data-Tier Recovery Checkpoints pin their full
graphs until a newer checkpoint is durable and verified. After all online,
checkpoint, handle, hardlink, and encoding-dependency pins end, unlink,
replacement, and truncate data are GC-eligible without a time-based recycle bin.
Rollback means automatic selection of a wholly valid generation; user-visible
history requires a future snapshot and quota design.
