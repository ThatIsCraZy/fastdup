---
status: accepted
---

# Admit the commit-cut backlog through the pending-region gate

`wait_for_commit_cut` drains a fixed pre-cut Ingest backlog whose staging needs
the pending-region gate. Lane draining and checkpoint absorption run only after
that wait, so the cut must not block the backlog on capacity that the cut itself
will release.

The wait opens a counted commit-cut hatch and closes it with an RAII guard on
every exit. Reservations that would block may proceed while this hatch is open.
It is tracked separately from the supervisor's one-generation watchdog hatch so
neither scope can close or prolong the other.

The window is bounded by the captured `(inode, sequence)` set, Lane limits, and
worker reservation limits; post-cut work is excluded. Consecutive-cut tests
prove that the hatch closes after each generation. ADR 0098 explains the ledger
leak that caused the observed saturation; no headroom-shortage claim remains in
this record.
