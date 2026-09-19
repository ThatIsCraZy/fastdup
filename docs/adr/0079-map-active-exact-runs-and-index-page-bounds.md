---
status: accepted
---

# Index active Exact Run page bounds under generation leases

ADR 0046 replaced file-backed Exact mappings with bounded Direct-I/O readers.
The current decision retains immutable generation leases and audited page-key
bounds: activation verifies Run identity, length, complete hash, page checksums,
ordering, and family partitioning before selection, then records the first and
last `(Chunk ID, logical length)` key of each page.

Lookup uses those bounds and optional membership hints to skip impossible pages,
then decodes and verifies only candidate pages through the Unified Read Cache.
Bounds, hints, and pages are acceleration only; a negative is not content or
liveness authority and a selected Location still follows normal eligibility and
verification rules.

The serialized Repository owner may reuse unchanged audited Runs only while its
installed Run Set matches the last successfully synchronized Activation Log
state. Ambiguous activation I/O revokes that state. Restart, public activation,
unknown/colliding Runs, recovery, and scrub audit stored bytes independently.
Leases prevent cooperating mutation or removal until the last reader retires.
