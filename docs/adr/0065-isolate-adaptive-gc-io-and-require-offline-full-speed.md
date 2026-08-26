---
status: accepted
---

# Isolate adaptive GC I/O and require offline full speed

Adaptive maintenance executes each Scrub and GC phase on a short-lived worker
in Linux's work-conserving idle I/O class. Space pressure may promote CPU
scheduling from Unix nice +10 to normal priority, but it does not promote
adaptive maintenance into the frontend I/O class. This is deliberately
asymmetric: frontend work can prevent admission of new maintenance I/O, while
maintenance never asks the write hot loop to measure, lock, signal, sleep, or
reserve capacity on its behalf.

The explicit `gc-now` execution mode skips CPU and I/O demotion. The appliance
CLI admits it only after the operator supplies `--offline` and acquires the
exclusive Appliance Lease from ADR 0069. Full speed changes resource scheduling
only. It does not weaken the
generation proof, replacement-before-deletion, Exact activation, identity
reread, or directory-sync invariants.

Linux I/O priority is applied to a fresh phase worker because an unprivileged
thread cannot be assumed able to promote itself after entering the idle class.
The unsafe `ioprio_set` syscall is confined to one small platform adapter with
an integer-only safe interface. It does not touch file buffers or durable
formats.

Online candidate discovery, proof, relocation, RETIRING activation, pin drain,
and unlink remain bounded resumable quanta. Their DATA operations use a
maintenance-specific adapter or request class and never share frontend
admission capacity. Scheduled windows may raise maintenance concurrency, but
only explicit exclusive full-speed mode may remove the frontend I/O priority
separation.

The writable appliance implements three adaptive paces. Defaults admit a small
background quantum every fifteen minutes under continuing frontend submissions,
a larger idle quantum once per minute after thirty quiet seconds, and an urgent
large quantum every thirty seconds at the inclusive 90% high watermark. Space
pressure stays latched until occupancy reaches the inclusive 85% low watermark,
preventing admission oscillation. Operators may configure all intervals, both
watermarks, one wrapping daily UTC window, and the maximum replacement-encoder
worker count before startup. Invalid configuration fails before storage opens.
Scheduled work uses Idle mode but retains idle-class I/O. Background relocation
uses one encoder worker; Idle/Urgent work uses at most the configured count and
available CPUs. Candidate proof, transition activation, pin drain, and unlink
remain serialized. The scheduler samples the frontend
io_uring submission counter that already exists; write and read paths gain no
GC counter, lock, notification, or branch.

Online DATA maintenance uses a synchronous `FsStorageIo` view on the
idle-prioritized maintenance worker. That view shares the process-local
Container descriptor cache and RETIRING selection state with the frontend
repository, but it does not share the frontend io_uring ring or inflight-byte
budget. Exact-generation pins remain shared through the same Exact repository.

The local online `gc-now` control request starts one urgent quantum immediately
with normal CPU priority but retains idle I/O priority. Only the existing
explicitly offline `gc-now` command means unrestricted CPU and ordinary I/O
priority. The control path is a daemon-owned mode-0600 Unix socket inside the
Metadata root; a CLI request never opens DATA or Metadata repositories and
therefore cannot become a competing storage owner. Bind and connect resolve
that filesystem socket through a short directory-file-descriptor path, so the
bounded `sockaddr_un.sun_path` does not impose a shorter-than-POSIX limit on the
Metadata root while filesystem ownership and mode remain the authorization
boundary.
