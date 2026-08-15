---
status: accepted
---

# Allow chunking profiles per data region

Large backup objects may contain heterogeneous VM and structured-data regions,
so manifests identify the versioned Chunking Profile used by each DATA region
rather than pinning one profile to an entire file. The initial FastCDC profile
uses 16 KiB minimum, 64 KiB target, 256 KiB maximum, fixed Gear table and fixed
masks; every CPU implementation must produce identical boundaries.

## Consequences

A versioned policy, not an implementation heuristic, selects profiles and must
make deterministic decisions over bounded input. Constant single-byte runs of at
least 64 KiB use FILL extents, force boundaries, and reset CDC. Additional CDC or
fixed-size profiles remain disabled until corpus measurements justify them.
