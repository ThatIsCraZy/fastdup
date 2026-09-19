---
status: accepted
---

# Merge commit-cut Drain Residue into the checkpoint writer

At a Frozen Commit Cut, complete staged Chunks go directly from their Ingest
Lanes to the single-writer `AdaptiveCommitWriter` as Drain Residue. They bypass
the detached-publication queue, preserving already-hashed bytes and Chunk
boundaries while avoiding a partial Container and a later reread.

## Invariants

- Only Chunks at or below the cut's per-inode mutation sequence are detached;
  post-cut staging remains in its Lane.
- The planner emits each staged Chunk inside the frozen planned range, using a
  DATA_SLICE when a suffix-anchored Chunk crosses that range boundary. It never
  rechunks the residue.
- Lane payload and Drain Residue share one admission charge. Transfer is
  net-zero; absorption, reset, cancellation, and Drop release the appropriate
  ownership exactly once.
- Residue not referenced by the frozen plan is discarded. Exact lookup may
  still reuse an already active Location for the same Chunk.

Writer, recovery, and fault tests cover truncate-plus-rewrite across a cut,
boundary slices, blocked gate release, cancellation, and byte-exact recovery.
ADR 0096 defines the ordinary cut announcement that releases advisory ingest
waiters; watchdog-driven admission closure remains fallback only.
