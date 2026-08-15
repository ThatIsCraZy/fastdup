---
status: accepted
---

# fsync does not strengthen durability

Successful `fsync`, `fdatasync`, and SMB `FLUSH` do not provide stronger
durability than fastdup's system-wide ten-second durability window. This is a
deliberate POSIX durability deviation: the primary workload is long backup
ingest, retained data is replaceable, and forcing every client to coordinate
synchronous commits would add operational and performance cost without useful
risk reduction for this deployment.

## Consequences

Clients must not use successful synchronization calls as a zero-loss transaction
boundary. Every successful write, including a write to a still-open ingest, must
enter a recoverable commit within ten seconds. The appliance targets internal
commits every five seconds and must stop acknowledging writes if it cannot
preserve that bound on supported, functioning hardware. This exception applies
only to durability; byte-exact reads and namespace behavior retain their
specified semantics.
