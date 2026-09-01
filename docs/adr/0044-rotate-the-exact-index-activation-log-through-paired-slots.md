---
status: accepted
---

# Rotate the Exact Index Activation Log through paired slots

The pre-stable Exact Index Activation Log uses two fixed, directory-durable
files, `exact-index.activation.wal` and
`exact-index.activation.1.wal`. A new writer bounds each ordinary slot at 64
exact 4-KiB Activation Records (256 KiB). When the selected slot reaches that
bound, the writer replaces only the inactive slot's contents with an exact copy
of the selected slot's last Activation Record followed by the new Activation
Record. The copied record is the **bridge record**. Its byte identity makes the
two slots one unambiguous overlapping chain without a mutable head pointer.

This decision extends the nonauthoritative-index rule from
[ADR 0015](0015-keep-exact-dedup-correct-without-index-authority.md), the RoW
rebuild rule from [ADR 0023](0023-rebuild-indexes-as-new-generations.md), and
the immutable sorted-Run design from
[ADR 0035](0035-build-the-exact-index-from-immutable-sorted-runs.md). It uses
the same proven paired-slot shape as the Namespace Commit Log without making
the two logs one transaction: Exact activation failure may degrade reduction
performance but never blocks or rolls back Namespace durability.

## Why paired slots

The previous single append-only file stopped after 16,384 records. At the
measured 287 Container/index activations per ten-minute ISO workload, this
would disable new Exact Index activations after approximately 9.5 hours. An
unbounded replacement file would merely postpone the same lifetime and startup
cost problem.

Two pre-created names fit the existing storage seam:

- the selected slot is never modified during rotation;
- every rotation write targets only the inactive slot;
- checksummed records make a torn trailing write distinguishable from a
  complete invalid record;
- the bridge is byte-identical to the other slot's final valid record; and
- the final synchronization of the inactive slot is the only rotation commit
  point.

No mutable head pointer, deletion, rename-over-existing, or assumed atomic
sector write is required. Slot choice, overlap, bounds, and recovery
remain behind the existing `ExactIndexRunRepository` interface.

## Slot invariants

Both names are created, truncated to zero, file-synchronized, and then made
directory-durable before an Activation Record can use them. Initialization is
idempotent and synchronizes the directory even when both names already exist.

Within one nonempty slot:

- every complete record passes its structural and CRC32C checks;
- generations increase by exactly one;
- `previous_record_hash` names the exact preceding record, except that the
  first bridge may name a predecessor no longer retained in that slot;
- Run Set generations increase strictly; and
- an ordinary new slot contains at most 64 records or 262,144 bytes.

A partial final record is a torn tail and is ignored only for recovery. A
complete invalid record, invalid internal hash link, or non-increasing Run Set
generation rejects the complete activation graph. Append requires a clean
tail; it never overwrites or repairs ambiguous live bytes.

Across two nonempty slots, the slot with the higher final generation is
selectable only when its first complete record is byte-identical to the lower
slot's final record. Equal final generations require byte-identical final
records. The longer byte-valid prefix supplies transition evidence; equal
length prefixes must be entirely byte-identical. A fork, missing overlap, or a
nonempty peer without one valid record disables the index rather than selecting
an arbitrarily convenient history.

Index disablement is safe because the Exact Index is rebuildable acceleration.
It must never make Namespace DATA unavailable or authorize reclamation.

## Append and rotation protocol

All steps run under the Exact Index Repository's activation lock and only after
the candidate Run Set and every immutable Run dependency have been completely
audited and made durable.

For an ordinary append below the 64-record bound:

1. load both bounded slots and select one continuous current prefix;
2. require a clean tail and verify the proposed generation, predecessor hash,
   and increasing Run Set generation;
3. append the new record and set the slot's exact length;
4. reread and validate the complete slot plus the exact intended bytes; and
5. synchronize that slot.

Step 5 is the sole activation commit point.

At the bound:

1. retain the selected slot unchanged;
2. truncate only the inactive slot in the process-visible state;
3. write the selected last record at offset zero as the bridge;
4. write the new record immediately after it and set the length to 8 KiB;
5. reread and validate the exact two-record chain and cross-slot overlap; and
6. synchronize the inactive slot.

Step 6 is both rotation and activation commit. A crash before it selects the
old durable slot. An effective synchronization may select the complete new
slot even if the call returned an ambiguous error. No mixed Run Set is valid.

Retrying an already-selected Run Set audits its immutable dependencies and
synchronizes the selected slot without appending another record.

## Recovery and scrub

Recovery reads and validates both slot files, selects their unique overlapping
head, then pairs that record with its exact content-addressed Run Set and every
referenced immutable Run before exposing an active reader. A corrupt or missing
activation graph disables index acceleration but does not roll back the
Namespace Commit WAL.

Both activation slots are bounded to 64 records from creation. A former 64-MiB
single-slot chain is unsupported pre-production state and is not a migration
input. The authoritative Commit-chain fence from ADR 0071 is present from the
first repository Commit.

Offline `audit_activation_log` repeats both local chain and cross-slot overlap
checks and fully audits the selected Run Set dependency graph. Discarded
activation history is not a snapshot and does not pin old Runs or DATA.

## Paired verification

- Writer: validates the selected snapshot and rereads the exact target bytes
  before the final slot sync.
- Reader/recovery: validates both slots, bridge identity, the selected record,
  Run Set identity, and every Run dependency.
- Offline scrub: uses the explicit activation-log audit seam and fails on a
  corrupt inactive peer as well as a corrupt selected peer.
- Fault injection: fails before and after every rotation operation and accepts
  only the previous or complete new Run Set. Only an effective final slot sync
  may expose the new activation.
- Lifetime gate: performs repeated rotations while requiring both slots to
  remain at or below 256 KiB.

## Consequences

Activation-log I/O and memory are lifetime-bounded. Rotation occurs every 63
successor activations because one of each
64 records is the bridge. The record byte format is unchanged. This decision
does not solve large Exact-Run compaction or index-object garbage collection.
Repository-wide format-epoch fencing is supplied separately by ADR 0071.
