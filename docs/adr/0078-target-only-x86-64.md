---
status: accepted
---

# Target only x86-64

fastdup is developed, tested, benchmarked, and shipped only for 64-bit x86
processors. Other CPU architectures are unsupported: they receive no fallback
implementation, cross-compilation gate, performance work, or compatibility
promise. This deliberately trades portability for one measurable machine model
across cache-line layout, SIMD dispatch, io_uring, FUSE, XFS, and appliance
qualification.

The baseline remains x86-64 rather than AVX2. Hot modules may select AVX2,
BMI2, AVX-512, or later x86 extensions after runtime detection and must retain
an x86-64 scalar oracle whenever durable decisions depend on their output.
Changing the minimum instruction set requires a new decision and complete
golden/differential evidence.

