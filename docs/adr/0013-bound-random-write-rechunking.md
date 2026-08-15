---
status: accepted
---

# Bound random-write rechunking

Random updates restart CDC from a committed boundary before the dirty range and
stop after several consecutive new boundaries match the old manifest. Work is
hard-capped initially at 8 MiB; if content has not resynchronized, fastdup writes
an explicitly marked Forced Chunk Boundary and reuses the unchanged suffix. This
keeps a small update bounded independently of total file size.

## Consequences

The cap is a versioned, benchmarkable profile value and telemetry reports forced
boundaries and estimated lost dedup opportunities. Holes force boundaries and
reset CDC without hashing virtual zeros. Empty DATA has no chunks; a nonempty
DATA region below the minimum chunk size becomes one chunk. Portable and SIMD
CDC implementations must produce byte-identical boundary decisions against a
pinned golden corpus.
