---
status: accepted
---

# Version writer policy without stranding old data

Each commit generation identifies one immutable Policy Set governing new
chunking, encoding, placement, and maintenance decisions. Durable records retain
their complete decode profile independently. Runtime feature flags stop writers
from creating an encoding but never disable a decoder still required by reachable
objects; removing one requires a verified offline migration.

## Consequences

Startup inventories required versions and refuses read-write service when the
binary cannot interpret them. A downgrade may mount read-only only if every
reachable object remains decodable. Write admission is controlled by explicit
health states (`STARTING`, `HEALTHY`, `READ_ONLY`, `CORRUPTION`, `REBUILDING`,
`STOPPING`), while scrub is an orthogonal activity. Correctness states are
durable; counters and histograms are asynchronous. Default telemetry excludes
payloads, paths, and unbounded-cardinality IDs, and deterministic AUDIT sampling
is reproducible from Chunk ID, generation, and a versioned seed.
