---
status: accepted
---

# Arm an Appliance Recovery Latch before repository access

Every writable daemon creates and durably synchronizes one empty Appliance
Recovery Latch in the Metadata root after acquiring the Appliance Lease and
before opening ordinary repository state. The latch remains present for the
complete writable lifetime. A clean shutdown removes and directory-syncs it
only after mutation admission is closed, Online GC has stopped, every admitted
mutation has caught up into a durable Commit, and the FUSE mount is detached.
Process loss, a failed checkpoint, a failed final catch-up, or an interrupted
unmount therefore leaves the latch armed for the next owner.

The latch is existence-based rather than a mutable health record. Its canonical
file is empty; unexpected bytes or a non-regular filesystem object fail closed.
This avoids a torn enum, timestamp, PID, or in-place state transition becoming
authority. Creation and removal use the same create, file-sync, directory-sync,
unlink ordering as other durable names. A failure while arming occurs before
ordinary repository access. A failure while clearing may conservatively retain
the latch after a shutdown that was actually clean.

A daemon that observes an armed latch performs the normal complete recovery,
DATA-dependency verification, Inode reservation Commit, and recovered Online-GC
finalization before mounting or admitting mutations. Offline maintenance may
inspect an armed repository, but only a successful complete Scrub (including
the scrub phase of `scrub-gc`/full-speed `gc-now`) may clear the latch; commands
that would mutate acceleration or Metadata state without that proof fail
closed. The latch is not a promise that arbitrarily stalled storage meets the
ten-second durability window and does not expand ADR 0007's supported failure
envelope.

Checkpoint timing is evaluated by a control-path Durability Supervisor from
explicit monotonic elapsed durations. Production obtains those durations from
`Instant`; deterministic tests supply literal fake-clock values. No clock
adapter, latch I/O, filesystem access, or additional synchronization enters the
POSIX mutation or Ingest-Lane hot loops.
