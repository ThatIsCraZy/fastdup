---
status: accepted
---

# Drop the length band from Similarity buckets

Bucket profile v2 (ADR 0103) still let Similarity contribute almost nothing
on real Veeam data. On one incremental run (17.14 GiB decoded, 129,096 new
Chunks) the appliance stored 27.0% of the logical bytes. Plain per-Chunk Zstd
level 3 alone stores 26.8%. An offline probe on the same run indexed every
earlier Chunk by its four smallest content-defined ~5-KiB sub-block digests and
tried one codec-5 Base per new Chunk, of any length, under the production
5%/4-KiB rule. It stored 15.8%: 40.9% less than plain compression.

The synthetic changed-block probe
(`crates/fastdup-store/examples/similarity_feature_probe.rs`) separates the
feature from the length constraint. It edits 15% of the 1-MiB blocks of a
646-MiB filesystem image in 4-KiB pages, shifts 0, 25 or 100% of those blocks
by an in-file insertion, writes them back to back like a raw incremental file,
and replays the production query shape. Physical bytes relative to plain
compression:

| Feature family | 0% shifted | 25% shifted | 100% shifted |
| --- | --- | --- | --- |
| v1 Superfeatures in v2 length bands (before) | −22.1% | −21.9% | −22.9% |
| v1 Superfeatures, any length | −59.4% | −60.1% | −61.9% |
| Sub-block min-hash, one per slot | −55.0% | −57.2% | −58.6% |
| Sub-block bottom-4 digests | −59.0% | −60.1% | −63.7% |

The length band, not the feature, loses the candidates: changed regions are
where SeqCDC boundaries and lengths move.

## Decision

Bucket profile v3 keeps the v1 Fingerprint and its four Superfeature slots but
gives every length one band; the key's length component is the band minimum,
1. New families are written as v3. Profile-v1 and v2 families stay readable and
queryable, each query unions the newest value of up to three bucket profiles,
and compaction never merges across profiles. A pool-index rebuild converts the
candidate universe to v3. Binaries without v3 reject v3 families.

Online and frozen queries rank candidates by the number of the four buckets
they share with the target first, then by Sketch distance, then by Chunk ID.
Each shared bucket estimates about a quarter of the content overlap; the Sketch
only breaks ties. On the synthetic probe this adds 0.7 to 1.7 points over
Sketch-only ranking.

A candidate Base may be at most four times longer than its target, so a
dependent read decodes at most four Base bytes per target byte. The bound costs
about one point on the synthetic probe; a bound of two costs about five.

Codec 5 carries every Base of another length. Sparse-XOR stays equal-length, so
a different-length candidate goes straight to its Prefix trial in both writers.
The cold-Base admission learner (ADR 0018) sizes the read by Base length but
classifies the target's independent compression by the target length. The
candidate, trial and acceptance bounds are unchanged.

## Consequences

Buckets now hold Chunks of every length, and each keeps its 64 smallest Chunk
IDs, so a popular Superfeature samples all lengths. Base fanout can grow
because one Base now serves targets of several lengths; depth stays one.
Restore locality loses the incidental length grouping of v2.

The synthetic figures are not a qualification. The gate is the next real
Veeam incremental: its stored ratio against plain compression and the online
candidate and acceptance counters. The probe's sub-block digests remain the
documented alternative if v1 Superfeatures underperform on real data.
