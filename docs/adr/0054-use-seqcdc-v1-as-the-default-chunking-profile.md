---
status: accepted
---

# Use SeqCDC-v1 as the default chunking profile

fastdup uses SeqCDC Increasing Mode with sequence length 6, opposing-slope skip
trigger 50, skip length 1024 bytes, 16-KiB minimum, and 256-KiB maximum for both
write-through ingest and checkpoint rechunking. The AVX2/BMI2 scanner and scalar
fallback must return identical boundaries. Measurements on the Rocky ISO found
2.90 times the scalar scanner throughput, 13.8 percent higher SingleStream SMB
throughput, and no MultiStream regression.

Write-through Tails preserve request-owned slices. Their scanner carries the
SeqCDC state across slice boundaries and invokes the same AVX2/BMI2 comparison
kernel on every sufficiently long in-slice span; it does not concatenate the
slices. A six-round scan of the 2-GiB Rocky ISO through 1-MiB slices produced the
same 25,653 boundaries and averaged 9,568 MiB/s with SIMD versus 6,223 MiB/s
with the scalar oracle.

This changes the durable Policy Set and Exact-Index profile identities. Existing
FastCDC repositories are intentionally incompatible with this prototype build.
The decision supersedes the FastCDC-specific chunking statements in ADRs 0032,
0040, 0041, 0050, and 0053 without changing their queueing, memory, durability,
or verification rules.
