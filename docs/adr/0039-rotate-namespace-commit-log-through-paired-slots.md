---
status: accepted
---

# Rotate the Namespace Commit Log through paired slots

The pre-stable Namespace Commit Log uses two fixed, directory-durable files,
`commit.wal` and `commit.1.wal`. A new writer bounds each ordinary slot at 64
exact 4-KiB Commit Records (256 KiB). When the selected slot reaches that
bound, the writer replaces only the inactive slot's contents with an exact copy
of the selected slot's last Commit Record followed by the new Commit Record.
The copied record is the **bridge record**. Its byte identity makes the two
slots one unambiguous overlapping chain without a mutable head pointer.

This decision extends [ADR 0019](0019-commit-only-after-data-and-metadata-are-durable.md),
retains the Policy refusal rule from
[ADR 0029](0029-version-writer-policy-without-stranding-old-data.md), preserves
the allocator guarantee from
[ADR 0034](0034-reserve-inode-ids-before-visibility.md), and keeps only current
and immediately previous as live recovery candidates as required by
[ADR 0037](0037-separate-structural-recovery-from-current-data-proof.md).

## Why paired slots

Rotation must not turn one ever-growing WAL or head file into another. The
storage seam has exact offset writes, truncation, file synchronization, and
directory synchronization, but deliberately has no assumed atomic sector write
or atomic replacement of an existing name. Two pre-created names are enough:

- the selected slot is never modified during rotation;
- every change is made to the inactive slot;
- checksummed Commit Records make a torn replacement a valid prefix or an
  invalid tail;
- the bridge is byte-identical to the other slot's last valid record; and
- the final synchronization of the inactive slot is the only rotation Commit
  point.

No Anchor, generation counter, deletion, rename-over-existing, or directory
mutation is needed at the Commit point. This keeps the module deep: callers
only load the current prefix and append one Commit; slot choice, overlap, bounds,
rotation, and recovery remain inside the Generation Log implementation.

## Slot invariants

Both names are created, truncated to zero, file-synchronized, and then made
directory-durable before any Commit can use them. Retrying initialization must
synchronize the directory even when both live names already exist.

Within one nonempty slot:

- every byte before a reported torn tail is an exact complete Commit Record;
- generations increase by exactly one;
- `previous_record_hash` names the exact preceding record in that slot, except
  that the first bridge may name a predecessor no longer stored there;
- namespace mutation cutoff, Inode reservation end, and Inode allocation
  cursor never decrease; and
- an ordinary new slot is at most 64 records or 262,144 bytes.

Across two nonempty slots, the one with the higher final generation is selectable
only when its first record is byte-identical to the lower slot's final record.
Equal final generations must also have byte-identical final records; the longer
valid prefix supplies transition evidence, and equal-length prefixes must be
entirely byte-identical. Any fork or non-overlap is corruption and writable
startup fails closed. One valid slot plus an absent or empty peer is accepted
during initialization. A nonempty peer with no valid first record fails closed
rather than letting an arbitrarily old slot masquerade as current. Loss of a
whole slot remains outside the MVP's device-redundancy guarantee.

The bridge carries the latest durable namespace cutoff, reservation high-water,
and allocation cursor into every successor. Recovery therefore never needs
discarded lifetime history to prevent Inode-ID reuse. The bridge plus the first
new record also retain the current and immediately previous Namespace Roots.

## Append and rotation protocol

All steps run under the Generation Repository's exclusive Commit lock.

For an ordinary append below the 64-record bound:

1. load both bounded slots, select exactly one continuous current prefix, and
   require its tail to be clean;
2. verify the proposed Namespace transition against the final selected record;
3. append the new record to the selected slot and set its exact length;
4. re-read the bounded slot, verify every record and the exact intended bytes;
5. synchronize that slot.

Step 5 is the sole Namespace Commit point.

At the bound:

1. retain the selected slot unchanged;
2. truncate the inactive slot in the live view;
3. write the exact selected last record at offset zero as the bridge;
4. write the new record immediately after it and set length to 8 KiB;
5. re-read and verify the exact two-record chain and overlap;
6. synchronize the inactive slot.

Step 6 is both the rotation and Namespace Commit point. A failure before it
leaves the old durable slot selected after crash. A failure after its effect may
recover either the old generation or the complete new generation depending on
whether the synchronization became durable; no mixed Namespace graph is valid.
The old slot becomes inactive only after the successor is durable and is not
changed again until a later rotation.

## Recovery and scrub

Normal recovery reads at most both 256-KiB slots, validates them independently,
then applies the overlap rule. It validates Namespace Root transitions forward
from the first stored record and proves DATA only for current and, on a
classified current-graph failure, immediately previous. A segment-relative
torn or invalid tail is reported without treating its bytes as Commit Records.

Both slots are bounded to 64 records from creation. Commit format v1, epoch
zero, and a former 64-MiB single-slot chain are unsupported pre-production
state and are not migration inputs. ADR 0071 requires v2 epoch-one records from
the first Commit.

No slot is a user-visible historical snapshot. Discarded records cease to pin
Metadata Objects or DATA. Offline scrub must apply the same per-slot and
cross-slot invariants and report a fork rather than choosing one.

## Paired verification

The writer re-reads the exact target slot before its final sync. Recovery
independently validates record bytes, the internal chain, monotone fields, and
cross-slot bridge identity. Fault injection covers failure before and after
every operation at a rotation boundary. A manual lifetime gate commits and
recovers more than 16,384 generations. Scrub repeats both local and cross-slot
checks.

## Consequences

Commit-log I/O and memory are bounded independently of appliance lifetime.
Rotation happens every 63 new generations per
slot because one of the 64 records is the bridge. The scheme intentionally
retains only enough Commit history for transition proof and the two online
recovery candidates. Audit history belongs in separate, non-authoritative
telemetry rather than the Namespace Commit Log.
