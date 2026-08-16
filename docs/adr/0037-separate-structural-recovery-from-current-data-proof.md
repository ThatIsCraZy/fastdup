---
status: accepted
---

# Separate structural recovery from the current DATA proof

Normal crash recovery validates the Commit WAL, every reachable Namespace Root
binding, and every namespace/inode/reservation transition in forward order. It
then proves complete Manifest and DATA dependencies newest-first, considering
only the current and immediately previous WAL generations that are live for
atomic recovery. A healthy newest generation therefore requires one complete
DATA-graph proof rather than one proof per historical Commit Record.

## Consequences

The forward Recovery Transition Prefix proves ordering and allocator safety; it
does not prove that a generation's Manifest or DATA graph is complete. A broken
root or invalid transition truncates that prefix because later transitions can
no longer be established. An unsupported Policy Set anywhere in the valid WAL
record prefix refuses recovery rather than being hidden by rollback.

Within the transition prefix, recovery verifies the current generation first.
Explicitly classified missing or corrupt graph dependencies permit one atomic
fallback to the immediately previous generation. Transient I/O and unsupported
capabilities abort recovery. Failure of both live candidates returns no
recoverable generation; older WAL history is diagnostic and is never exposed as
an implicit snapshot.

Every selected generation still receives a fresh complete graph proof after a
restart, and demand reads still re-verify immutable Container data. Recovery
never combines roots, manifests, or DATA dependencies across generations.
Offline scrub and rebuild remain responsible for exhaustive historical physical
verification; rereading obsolete graphs during every healthy mount is not a
scrub substitute.
