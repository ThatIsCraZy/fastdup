---
status: accepted
---

# Reject incompressible regions before target Zstd

The adaptive Container writer implements `incompressibility-gate-v1` before its
target Zstd level-3 encode for dependency-free Compression Regions of at least
128 KiB. It first runs LZ4 with a destination capped at the largest payload
that can still satisfy ADR 0016's complete-record 3%-and-4-KiB rule. If LZ4
does not fit, a bounded Zstd level-1 trial may rescue data whose useful
long-distance repetition LZ4 cannot represent. The writer runs Zstd level 3
only after one predictor fits. Smaller regions bypass the predictors.

Predictor output is never durable. RAW and Zstd-v1 remain the only resulting
independent encodings, and the final writer cost comparison, publication
reread, recovery, and scrub invariants do not change. Dictionary, prefix, and
delta candidates do not use this dependency-free gate.

Every encoding worker owns its mutable Zstd context and a reusable bounded
scratch buffer. The safe LZ4 block call does not allocate per region, and no
predictor uses a shared codec lock. An expected destination-too-small result
selects the next fallback; any other codec failure remains an error. A failed
bounded Zstd session is reset before the worker reuses its context.

The runtime reports gate eligibility, size bypasses, LZ4 and Zstd-1 outcomes,
target trials and outcomes, RAW selections after gate rejection, and scratch
high-water. These counters are acceleration evidence, not durable authority.
The public format-writer seam accepts explicit `Off`, `Lz4Only`, and `V1`
policies so benchmarks can compare them without changing code. Production
store entry points remain `Off` until the benchmark gates below pass;
implementing a predictor is not authority to promote a measured regression.
The gate version is recorded in benchmark output. Changing its ordering,
minimum size, predictors, or cost cap requires a new gate-policy version and
the evidence gates in
[`zstd-incompressibility-gate.md`](../research/zstd-incompressibility-gate.md).

## Consequences

When promoted, incompressible regions avoid the more expensive Zstd level-3
pass. The LZ4 and Zstd-1 trials add work for compressible regions, so promotion remains
conditional on the Rocky SingleStream, structured-data, entropy-trap, CPU,
latency, and physical-byte comparisons in the research note. A byte-entropy
classifier is not part of v1 because byte histograms cannot identify all
LZ77 repetition.
