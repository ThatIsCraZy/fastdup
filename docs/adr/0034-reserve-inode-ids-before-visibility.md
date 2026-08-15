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

## Consequences

Recovery begins allocation at the durable reservation end and deliberately
skips unused or crash-lost IDs. Reservation exhaustion closes create admission
until a larger range is durable; it never wraps, reuses, or guesses from the
currently visible namespace. Reservation size remains an implementation policy,
not an on-disk identity rule.
