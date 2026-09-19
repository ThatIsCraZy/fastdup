---
status: accepted
---

# Fence writer downgrade in the authoritative Commit chain

The Repository Format Epoch is carried by every Commit Record, so the
authoritative chain is also the writer-compatibility fence. Writers, recovery,
and offline scrub accept only the current epoch and fail before mutation on any
other value; an older binary therefore cannot silently advance a newer pool.

The current epoch is **3**, introduced by ADR 0095 for record-range Namespace
shards. Epochs 0–2 are unsupported pre-production inputs. There is no migration:
an older pool must be re-ingested.

The Commit chain is the fence because every writer already has to validate it.
A separate marker could be ignored by binaries predating that marker. Repository
epoch, object-local format version, and Policy Set remain separate concepts:
the epoch gates repository-wide compatibility, object versions define decoding,
and the Policy Set governs new writer and maintenance choices.
