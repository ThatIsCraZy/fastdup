# FastDup Control Plane

The appliance runs three local systemd services:

- `fastdup-control` is the unprivileged HTTPS, session, API, UI, audit, and
  telemetry-history process.
- `fastdup-agent` owns the credential-checked Unix socket and is the only
  process allowed to provision block devices or control systemd and Samba.
- `fastdup-repository` owns the live FUSE mount. systemd enforces
  `MemorySwapMax=0`, sends `SIGINT` for a checkpointed unmount, and is
  configured never to escalate to `SIGKILL`.

The process and resource seams are independent:

- `fastdup-control` and `fastdup-agent` are children of
  `fastdup-management.slice`. The slice has one aggregate `MemoryMax=1G`,
  starts reclaim at `MemoryHigh=768M`, cannot swap, and may consume at most
  one CPU (`CPUQuota=100%`). Its CPU and I/O weights are 100.
- `fastdup-repository` and offline repository maintenance run in
  `fastdup-storage.slice`, outside that memory and CPU envelope. The storage
  slice has CPU and I/O weights of 1000 and no CPU quota. Offline scrub is a
  fixed systemd template job rather than a child of the privileged agent.
- The management slice is a local cgroup OOM containment domain. Exhausting
  its hard limit can restart one or both management processes, but the local
  OOM cannot select or kill the Repository Runtime.
  Conversely, the Repository Runtime has no `Requires=`, `BindsTo=`, or
  `PartOf=` relationship to either management process. A WebUI crash therefore
  leaves the mount and acknowledged I/O untouched.

`CPUQuota=100%` means one logical CPU worth of aggregate runtime, not 100% of
the appliance. CPU weight 100 versus 1000 makes storage win contention while
still granting management scheduler progress; when the host is idle, the UI
may burst up to its full one-CPU ceiling. This portable policy does not pin a
specific CPU. Appliances that reserve a physical core must add topology-aware
`AllowedCPUs=` drop-ins at installation time and must never assume that CPU 0
is an isolated core.

Install the units (including both `.slice` units and the maintenance template),
sysusers, tmpfiles, and Samba files from `packaging/`, run
`systemd-sysusers` and `systemd-tmpfiles --create`, and include
`/etc/samba/fastdup-shares.conf` from `smb.conf`. The first HTTPS start creates
a self-signed certificate under `/var/lib/fastdup/control/tls`.

The first login is `admin` / `fastdup01.`. No management mutation is accepted
until that password is replaced with at least twelve characters.

The repository runtime exposes version 1 of its typed local management
protocol at
`/var/lib/fastdup/repository/metadata/.fastdup-management.sock`. It returns
lock-free POSIX frontend counters and fixed latency percentiles and applies
Online-GC enablement, pressure watermarks, and per-Share Logical Share quotas
rules live. Each Share uses `/srv/fastdup/repository/.fastdup-shares/<share-id>`
as its stable root; changing the visible Share name therefore never changes its
data path. The UI accepts an optional integer from 1 through 999 and a decimal
GB/TB/PB unit. This changes `statfs` reporting at the Share root only: it is not
a reservation or hard write quota. stderr remains diagnostic output and is
never parsed for telemetry.

The React application is built with Node.js but Node is not present in the
runtime. Build it first with `npm run build` in `web/fastdup-ui`; the Rust
Control Plane embeds the resulting `dist` tree into its binary.

After installation, verify the live hierarchy and limits with:

```sh
systemctl show fastdup-management.slice \
  -p CPUQuotaPerSecUSec -p CPUWeight -p MemoryHigh -p MemoryMax \
  -p MemorySwapMax -p IOWeight
systemctl show fastdup-control fastdup-agent fastdup-repository \
  -p Slice -p ControlGroup -p OOMScoreAdjust
systemd-cgls
```

Acceptance requires the sum of `memory.current` below
`fastdup-management.slice` to remain below 1 GiB, `memory.swap.current` to stay
zero, and `cpu.stat` to show throttling above one CPU worth of sustained work.
Killing or cgroup-OOMing `fastdup-control` and then `fastdup-agent` must not
change the repository unit's active state, mount identity, or current Commit
generation.
