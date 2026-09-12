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

The supervisor registers its SIGINT receiver once before entering its event
loop and retains that registration while handling other events. Canceling an
individual receive wait must not discard an interrupt delivered during a
checkpoint or a scheduler branch. Pending interrupts are handled after the
selected work returns; they do not cancel an in-progress durable publication.
Orderly catch-up, FUSE unmount, and latch clearing retain the ordering above.
A process-isolated regression delivers a real SIGINT after canceling the
receive wait and before its first poll, and requires both notifications to
remain available to the supervisor.

On orderly shutdown, the runtime closes mutation admission before waiting for
management clients or background workers. It immediately signals Online GC,
Scrub, and the periodic Recovery Checkpoint scheduler. GC observes a shared
cooperative stop request inside long graph/proof scans, rather than waiting for
its asynchronous scheduler to regain control after a complete quantum. A stop
is a distinct maintenance outcome, not an integrity failure. The frontend's
repository view is not cancelled.

The owner awaits actual worker termination; dropping or aborting an asynchronous
join handle is not evidence that its blocking storage worker stopped. All
workers are drained even if an earlier shutdown phase fails. Already admitted
mutations still undergo final catch-up. The last Recovery Checkpoint is published
after that catch-up and after any in-flight periodic copy has completed. Only
successful background termination, catch-up, final recovery publication, and
unmount permit clearing the latch. Errors retain the latch and are reported
after cleanup. Phase durations distinguish GC drain, final commit, recovery
copy, and unmount; shutdown is not promised to outrun a blocked kernel I/O or a
required durability sync.
