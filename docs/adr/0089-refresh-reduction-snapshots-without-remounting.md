---
status: proposed
---

# Maintain Similarity as an online immutable-run index

Maintain Similarity incrementally with immutable Run families and an atomic
Reduction Head. New independent Chunks become candidates after L0 activation;
ordinary publication never needs a full-pool rebuild or remount. Similarity may
lag or drop queued hints because it is optional acceleration.

## Durable and query invariants

- A Run stores a complete replacement `BucketState64` for each changed Bucket
  Key. Newest value wins; query-time union across Runs is forbidden.
- A query reads at most four states of 64 representatives, ranks at most 16
  candidates, and attempts at most four encodes.
- Only independent Chunks enter Similarity. A candidate becomes a Base only
  after current Exact resolution and full eligibility checks, preserving
  dependency depth one.
- Exact activates first. Failure afterwards leaves the previous Reduction Head
  active and Exact usable; unpublished Similarity objects are garbage.
- The two-slot Reduction Head selects one complete recoverable Run Set. Reader
  leases protect displaced Runs; it does not pin an Exact generation for the
  lifetime of the mount.
- A dependent-write guard protects the selected Base through durable target
  Exact activation and invalidates stale GC proofs.

The implementation bounds active families at 24, publication batches at 4,096
entries, the queue at two batches, and chronological compaction fan-in at four.
Capacity pressure drops hints instead of blocking Exact or DATA publication.
Offline rebuild remains the bootstrap and repair path.

## Writer policy

Repository and Share policies select `off` or `dependent_v1`; a Share override
inherits the repository default when absent. New managed Shares start off.
Disabling bypasses fingerprints, candidate reads, trial encoding, and hint
publication while retaining Exact Dedup, independent compression, and decoding
of existing dependent records. Cross-policy-subtree hardlinks and renames fail
with `EXDEV` so every inode has one policy owner.

## Status and acceptance gate

The implementation, durable format, recovery, scrub, compaction, and fault
coverage exist, but default-on use is not accepted. Real-device qualification
must show acceptable L0 latency, negative-probe amplification, compaction I/O,
and net DATA-read/write benefit.

Recorded evidence is mixed: the 50-version Linux A/B reached 23.84:1 reduction
versus 6.59:1 with the feature off, but reduced effective write-through by
42.25%. A later live workload read 13.1 MB of Base DATA to save 9.2 MB at a
0.12% candidate hit rate. Keep the default off until the acceptance gate passes.

See [the head format](../formats/online-similarity-head-v1.md),
[implementation evidence](../benchmarks/online-similarity-share-policy-2026-09-05.md),
and the [Linux A/B](../benchmarks/linux-6.12-online-similarity-2026-09-05.md).
