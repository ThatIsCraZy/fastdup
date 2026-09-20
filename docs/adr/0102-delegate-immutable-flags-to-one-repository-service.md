---
status: accepted
---

# Delegate immutable flags to one repository service

Veeam's hardened-repository protocol separates data transfer from retention
enforcement. The unprivileged transport service writes backup data, while its
root-owned immutability service sets and clears Linux immutable inode flags
after recording retention state. A user-namespaced service container presents
that root process to FastDup as a nonzero mapped UID.

FastDup binds an Immutability Authority to the reserved Veeam Namespace root.
The binding names that root inode and the container's mapped root UID. Only
that UID inside that exact subtree, or appliance UID 0 for recovery, may change
file flags and Veeam's exact `user.immutable.until` retention attribute. The
authority gains no access to any other extended attribute. The transport UID
and every other mapped or host identity remain
ordinary file owners. New descendants inherit the binding, open unlinked
inodes retain it until their final reference disappears, and hard links or
renames may not cross the authority boundary.

The authority may also publish an inode owned by the transport identity or by
the authority itself as container `root:root`. The rule records all three
identities: the only permitted caller, the original writer and the only
permitted final owner. This matches Veeam's `fchown(fd, 0, 0)` after it applies
retention. The authority cannot choose a different recipient or perform the
handoff outside the bound subtree.

The appliance derives the service IDs from the effective identity of the
running vendor immutability and transport processes. It derives the final
container `root:root` owner from the active UID/GID maps. It does not assume
that Veeam's numeric service accounts or the user-namespace range remain stable
across installation or upgrade. Missing, unmapped or host-root identities omit
the handoff rule; periodic policy reconciliation installs it once all three
identities are unambiguous.

Veeam may change the lock owner and group in separate system calls after making
the inode immutable. The rule therefore admits the two intermediate states in
which either the destination UID or destination GID is already installed while
the other field still equals the resolved writer or immutability-service
identity. No other immutable metadata mutation is admitted.

A successful protection change is stronger than an ordinary `fsync` under ADR
0003. Before replying, the durable appliance checkpoints at least the Namespace
mutation sequence containing the flag change. An I/O or checkpoint failure is
returned to the caller, which must treat the outcome as ambiguous and inspect
the flag before retrying. This narrow exception prevents Veeam from recording
an immutable backup whose protection could still disappear in FastDup's normal
durability window.

Veeam remains the retention-clock authority. Its persistent
`/etc/veeam/immureposvc` state detects clock movement and decides when a flag may
be cleared. That directory is a second, private FastDup Namespace root bound
only at its service path in the container, because a root process in a user
namespace cannot set `FS_IMMUTABLE_FL` on the host-owned container filesystem.
It uses the same scoped authority but is not visible below `/repository`.
FastDup durably enforces the flag but does not infer expiry from an xattr. A
repository is advertised as hardened only while the scoped authority is enabled
and the Veeam immutability service is healthy.
