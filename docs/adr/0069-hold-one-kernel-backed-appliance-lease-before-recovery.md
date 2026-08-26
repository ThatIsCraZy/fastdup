---
status: accepted
---

# Hold one kernel-backed Appliance Lease before recovery

Every writable daemon and offline maintenance process acquires one exclusive,
nonblocking Appliance Lease in the Metadata root before recovery or repository
mutation and retains it for its complete lifetime. A persistent mode-0600 file
records diagnostic owner kind and PID, but the open-file-description `flock` is
the only ownership authority: process exit, including `SIGKILL`, releases it
without trusting PID reuse, timestamps, socket reachability, or stale-file
removal. Online control clients do not acquire a second Lease because they
delegate work to the owning daemon without opening a repository.

This deliberately chooses a kernel-released local-filesystem lease over a
durable PID record or the existing Online-GC socket. Records cannot safely
distinguish a dead process from a reused PID, and socket ownership covers only
Online GC rather than every generation-mutating path. An inability to acquire
or synchronize the Lease fails closed before ordinary repository access.
