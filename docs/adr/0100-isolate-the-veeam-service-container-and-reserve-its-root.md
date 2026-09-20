---
status: accepted
---

# Isolate the Veeam Service Container and reserve its root

The Veeam Service Container is a managed Linux frontend to one exclusive
`veeam` directory in the existing Repository. Its persistent operating-system
and vendor-service state lives outside the Repository. The SMB name `veeam`
(case-insensitive, including its hidden-share alias) is reserved even while the
container is stopped. Existing conflicting shares or unmanaged directories are
never silently adopted or deleted.

Use Rocky's packaged systemd-nspawn instead of adding an LXC distribution to the
appliance. A private user namespace maps container root to host UID/GID 524288,
with a 65536-ID range. Only the designated Repository directory is bound into
the guest. The fixed range is reserved for this frontend and cannot be changed
without an explicit ownership migration. An IPVLAN endpoint shares the host
NIC's MAC while using a distinct IPv4 address; duplicate-address detection runs
on the actual parent interface before provisioning. The host and its IPVLAN
child do not communicate directly through that parent interface.

Quota and Advanced Reduction join the same complete Namespace policy manifest
as SMB roots, including after recovery, reconciliation and SMB changes. A
stopped container retains its directory, policy and installed service state.
Configuration jobs serialize with policy reconciliation. The desired
configuration is persisted before external provisioning and a failed job is
retryable without replacing the rootfs. The transport account is guest UID/GID 1000 (host 525288); it owns the directory.
Only the optional bootstrap public key is retained in the Control Plane.
A temporary installation password may pass through a typed command and stdin
to the guest, but never through stored settings, command arguments or audit
output. Disabling SSH also locks this password and removes bootstrap sudo.

The container follows the Repository mount lifecycle. The host checks the FUSE
mount before starting it; the guest has an inaccessible underlying repository
directory and verifies the bind before accepting SSH installation. This version
exposes a Linux repository, not an XFS or Hardened Repository claim. No filesystem
magic is forged and no unsupported reflink is converted into a successful copy.
ADRs 0003, 0027 and 0043 remain unchanged. Native Linux reflink translation,
retention authorization and synchronous protection commits are separate gates
before the full feature request may be called implemented.

ADR 0101 supersedes the initial no-magic-view restriction with a narrowly scoped
Veeam process adapter; the isolation and Namespace policy decisions remain.
