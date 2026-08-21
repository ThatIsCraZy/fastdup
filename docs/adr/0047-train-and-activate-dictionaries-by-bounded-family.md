---
status: accepted
---

# Train and activate dictionaries by bounded family

fastdup does not retain multiple uncommitted Containers or extend one Zstd
history across Container boundaries to improve compression. Containers remain
independently durable, Compression Regions remain independently decodable, and
a full Container remains eligible for immediate publication.

Dictionary training is instead a cold, asynchronous optimization over a
bounded sample reservoir from prior content in one Dictionary Family. A
trained Dictionary Object is immutable and must become durable before any new
encoding may name it. Training, catalog admission, or retraining must never
block ordinary RAW/Zstd ingest; memory pressure may discard samples or reject
new catalog entries without changing file bytes or durability.

The writer trials RAW, dependency-free Zstd, and the selected family dictionary
for each eligible Compression Region. Dictionary encoding wins only after its
complete record cost and an amortized share of the retained Dictionary Object
beat the alternatives. Unknown families use dependency-free encodings. A
family may activate a replacement only after held-out evidence demonstrates a
net physical-byte gain within CPU, RSS, restore-latency, and read-amplification
budgets; old objects remain reachable while selected encodings depend on them.

The initial evidence rejects a global or adjacent-Container dictionary. Ten
held-out structured targets amortized a 64-KiB family dictionary and reduced
total bytes by 7.10% for JSON and 4.38% for XML. A dictionary from the wrong
structured family saved less than 0.1% after its own cost. Two preceding
32-MiB Rocky ISO regions trained dictionaries for the next 32-MiB region, but
no dictionary encoding was selected and physical payload was unchanged while
training consumed additional CPU and RSS. The reproducible measurements are
recorded in
[`dictionary-catalog-v1.md`](../benchmarks/dictionary-catalog-v1.md).

## Consequences

Dictionary choice is workload evidence, not a file-extension correctness rule.
The catalog must be explicitly bounded by sample bytes, active object bytes,
families, queued training work, and effective memory headroom. Its counters
must expose sampled/trained bytes, rejected work, trials, accepted regions,
object cost, net saved bytes, CPU time, resident bytes, and family hit/miss
rates. Durable dictionary records remain gated on an assigned versioned codec,
writer/reader/recovery/scrub pairing, dependency reachability, and crash-fault
coverage; the current Container v1 continues to emit only RAW and
dependency-free Zstd.
