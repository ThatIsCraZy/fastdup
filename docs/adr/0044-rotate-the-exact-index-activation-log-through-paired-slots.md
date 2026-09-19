---
status: accepted
---

# Rotate the Exact Index Activation Log through paired slots

The Exact Index Activation Log uses two directory-durable files:
`exact-index.activation.wal` and `exact-index.activation.1.wal`. Each slot holds
at most 64 exact 4-KiB Activation Records. Rotation replaces only the inactive
slot with the selected slot's final record followed by the successor. The copied
byte-identical record is the bridge between both histories.

This keeps activation bounded without a mutable head pointer, rename-over,
deletion, or assumed atomic sector write. Exact activation remains independent
of Namespace durability: an invalid activation graph disables rebuildable Exact
acceleration but never makes Namespace DATA unavailable.

## Durable invariants

Both slot names are created, file-synchronized, and made directory-durable
before use. Initialization is idempotent and synchronizes the directory even
when both names already exist.

Inside a nonempty slot:

- every complete record passes structural and CRC32C validation;
- record generations advance by one and link by `previous_record_hash`;
- Run Set generations increase strictly; and
- the slot contains at most 64 records or 256 KiB.

The first bridge may name an unretained predecessor. Recovery may ignore only a
partial final record; a complete invalid record, broken link, invalid generation
or dirty append tail rejects the graph.

Across nonempty slots, the higher final generation is selectable only when its
first complete record equals the lower slot's final record. Equal final
generations require identical final records. The longer valid prefix provides
transition evidence; equal-length prefixes must be identical. A fork, missing
overlap, or nonempty peer without a valid record disables the index.

## Publication

Activation runs under the repository activation lock after the candidate Run
Set and all immutable Run dependencies are durable and validated. The online
owner may use validated encoder output and successful publication as evidence;
standalone activation performs independent storage audits.

Below the slot limit, the writer validates the retained synchronized snapshot,
appends one successor, validates the resulting chain and synchronizes that slot.
At the limit it leaves the selected slot untouched, truncates the inactive slot,
writes the bridge and successor, sets the length to 8 KiB, validates the new
chain and synchronizes the inactive slot. The final slot sync is the only commit
point in either path.

The writer consumes and returns one bounded slot buffer. Ordinary append does
not rehash or decode the known prefix; rotation moves the final encoded record
within the same buffer. Standalone activation still rereads the target slot.
Only a successful final sync advances the live cursor. Any error discards the
cursor, so retry independently reconstructs storage and dependencies, including
after an effective sync reported failure. Retrying an already selected Run Set
audits its dependencies and synchronizes the selected slot without appending.

The online predecessor lookup may reuse a matching installed lookup directory.
A selector mismatch uses the independent path. Recovery and offline audit never
trust the writer cursor.

## Recovery and verification

Recovery validates both slots and their unique overlap, then pairs the selected
record with its content-addressed Run Set and every referenced Run. Offline
`audit_activation_log` repeats the full graph audit and rejects corruption in
either peer. Discarded activation history pins neither Runs nor DATA.

Fault injection covers every append and rotation boundary. Recovery may expose
only the previous activation or the complete successor, and only an effective
final sync may expose the successor. Lifetime tests rotate repeatedly while
both slots remain within 256 KiB and compare the retained writer buffer with
independent recovery.

The former pre-production 64-MiB single-slot chain is not a migration input.
Repository-wide format fencing is defined by ADR 0071.
