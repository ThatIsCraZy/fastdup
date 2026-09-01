---
status: accepted
---

# Reserve Inode IDs before visibility

The namespace durably advances an Inode ID reservation end before allocating
IDs from that range. Persisting only the newest committed `next_inode` would
reuse IDs that were acknowledged and then lost inside the durability window;
per-create durable logging would instead put synchronous metadata I/O on the
create path. Range reservation preserves never-reuse identity while amortizing
that durability cost.

The production v1 daemon reserves `2^32` IDs per writable start. This makes
refill I/O and coordination absent from the create hot path for any practical
process lifetime while retaining the same crash rule: recovery starts at the
durable range end and skips its unused suffix. The `u64` identity space leaves
more than four billion complete restart ranges before exhaustion.

## Consequences

Recovery begins allocation at the durable reservation end and deliberately
skips unused or crash-lost IDs. Reservation exhaustion closes create admission
until a larger range is durable; it never wraps, reuses, or guesses from the
currently visible namespace. Reservation size remains an implementation policy,
not an on-disk identity rule.
