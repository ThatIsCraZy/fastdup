---
status: accepted
---

# Bind storage pools to one Appliance and fixed roles

Every new Metadata and Data Pool persists one checksummed current-format
identity before ordinary repository access. Both records carry the same random
Appliance ID, different random Pool IDs, and immutable Metadata or Data roles;
mount paths and command-line order are never identity. Writable startup and
offline Scrub fail closed on a missing record in a populated Pool, corruption,
duplicate Pool IDs, an Appliance-ID mismatch, or a role mismatch.

First initialization publishes each record immutably with file and directory
durability. An interruption may leave neither record, one complete record, or
both complete records; startup may finish a missing side only when that Pool
contains bootstrap objects and binds it to the already durable Appliance ID.
Prototype Pools without identity records are unsupported current-only state,
not migration inputs.
