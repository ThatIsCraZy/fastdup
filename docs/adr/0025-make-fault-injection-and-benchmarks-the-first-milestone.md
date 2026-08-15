---
status: accepted
---

# Make fault injection and benchmarks the first milestone

Before feature breadth, fastdup builds deterministic format tests, a modelable
Storage-I/O boundary, and exhaustive interruption after writes, syncs, renames,
and directory syncs. Each interrupted state is reopened through production
recovery and checked against the same invariants. Real XFS process-kill tests
supplement this deterministic layer.

## Consequences

The primary large fixture is the versioned Rocky 10.2 minimal ISO pinned by URL,
2,072,444,928-byte length, and SHA-256
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
A fixed-seed generator creates versioned JSON/XML families up to 800 KiB. Corpus,
build, test, and profiling artifacts remain inside the project workspace and are
never committed as large binaries. Every report records corpus hashes, hardware,
NUMA, storage, XFS/kernel/build identity, feature matrix, byte accounting,
throughput/latency, CPU, I/O, and amplification metrics.

RAW, CDC, exact, compression, grouping, similarity, delta, and reorder remain
independently benchmarkable. No feature becomes default from compression ratio
alone; end-to-end physical bytes, ingest and restore distributions, CPU, and
maintenance amplification are all decision inputs.
