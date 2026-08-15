---
status: accepted
---

# Bound compression and reordering

Compression Regions contain adjacent complete DATA chunks and are capped at
512 KiB decoded bytes; holes, fills, profile transitions, and forced boundaries
end a region. Regions may be physically clustered only inside a 64 MiB logical
Placement Window, with `reorder=off` retaining input order as the measurement
baseline. This bounds restore locality loss while permitting compression-aware
packing.

Version 1 assigns an indivisible logical chunk to the Placement Window
containing its first byte. A CDC chunk may therefore straddle the numeric
64-MiB boundary by at most the versioned 256-KiB maximum Chunk size. A
Compression Region may contain only chunks assigned to one window; no record is
ever moved into another window merely because later bytes cross the boundary.
Changing this ownership rule or eliminating the bounded one-Chunk overhang is a
writer-policy change and requires a new profile.

## Consequences

RAW is preferred unless an encoding saves both 3% and 4 KiB after record headers,
index entries, and alignment are charged. These thresholds and Zstd effort are
versioned policy. Codec IDs and parameters are serialized explicitly; encoder
output need not be stable, but decode must reproduce BLAKE3-verified logical
chunks. Unknown codecs are rejected.
