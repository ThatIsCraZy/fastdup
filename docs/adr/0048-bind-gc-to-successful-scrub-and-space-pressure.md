---
status: accepted
---

# Bind GC to successful scrub evidence and explicit space pressure

DATA GC may consume only an opaque GC Scrub Plan produced by one complete,
successful End-to-End Scrub. The plan binds the exact current and immediately
previous online Commit Records, their reachable Chunk identities, the verified
Container inventory, and the candidate set. Any change to that online pair
invalidates the plan before destructive work. The first implementation removes
only Containers with no reachable Chunk; a partially live Container requires
the RETIRING, relocation, pin-drain, and Location-Set transition protocol from
ADR 0021 and is never treated as immediately reclaimable.

Scrub and GC execute as asynchronous maintenance phases. Below 90% Data Pool
occupancy Scrub runs on a dedicated Unix nice +10 thread. At or above 90% it
runs at normal priority. GC runs at normal priority when occupancy is at least
90% or the completed Scrub proves more than 20% of total Container file bytes
immediately reclaimable; otherwise GC remains background priority. Both
comparisons use integer byte accounting: 90% is inclusive and 20% is strict.
Occupancy is sampled at maintenance-cycle admission and reclaim pressure is
known only after Scrub.

Before deleting a candidate, GC builds and activates a fully verified RoW Exact
Index that excludes every candidate. It then rereads and verifies each exact
Container identity, unlinks only those canonical names, and synchronizes the
Container directory. A fault before index activation deletes nothing; a fault
after activation leaves either harmless additional garbage or a durable subset
of the planned deletions. The Exact Index remains acceleration rather than
liveness authority.

Until the durable Appliance Lease and online RETIRING/pin protocol exist, the
destructive maintenance cycle requires exclusive offline ownership. The
asynchronous interface does not imply permission to race a writable mount.

ADR 0049 extends this offline-only cycle with bounded replacement publication
for profitable sets of partially live Containers; it does not relax the online
RETIRING and pin requirements recorded here.
