# Veeam Service Container

This frontend is experimental. Native Fast Clone was verified with Veeam
13.1.1.18 through the packaged adapter. Hardened Immutability uses a scoped
service authority and a synchronous durable commit for every protection-state
change; live Veeam qualification evidence is recorded separately.
The broader requirements in `Featurerequest.md` remain qualification gates.

Build the image on the development workstation with
`packaging/veeam/build-image.sh`, then build the RPM with
`packaging/build-rpm.sh`. All build products are under `.artifacts/`; the RPM
ships the image and provisioning helper. Building requires root and Rocky 10
package repositories with package signature checking enabled. The guest has
systemd, SSH, Bash, Perl, RPM/DNF and XFS tooling, but contains no Veeam binaries,
SSH host keys or reusable bootstrap passwords. Veeam installs its own services.

Open **Veeam** in the WebUI after mounting the Repository. Enter the physical
parent interface, a dedicated IPv4 address/prefix, gateway and DNS. Generate a temporary installation password; an OpenSSH
public key is optional for administration. Select Hardened Immutability,
Advanced Reduction and optionally a logical quota, then
provision. The agent rejects stale revisions and conflicting names. The
provisioner checks for duplicate IPv4 ownership with ARP; IP address management
must still reserve the address against future DHCP/static allocation.

Add that address in Veeam as a new Linux server with single-use credentials,
user `veeam`, the temporary password and sudo elevation. Use `/repository` as its backup
repository path. Disable SSH in the FastDup WebUI when installation succeeds.
The packaged adapter provides the Veeam process with a scoped XFS capability
view and native range cloning. For a hardened repository, configure Veeam with
at least seven immutable days. Veeam services, certificates and
`/etc/veeam/immureposvc` clock state persist inside
`/var/lib/machines/fastdup-veeam`; reapplying settings never replaces this tree.

`veeam` is permanently reserved as a managed SMB share name, including `VEEAM`
and `veeam$`. Its host path is `/srv/fastdup/repository/veeam`; only that directory
is visible as `/repository` inside the container. Container root maps to host
UID/GID 524288. When Hardened Immutability is enabled, only that identity may
set or clear immutable flags below this reserved root. It is not host root and
has no authority in SMB shares or other Namespace paths. FastDup resolves the
running immutability and transport services' effective UID/GID values through
their actual user-namespace mapping, so vendor account allocation may change between installs. An absent or
ambiguous service identity disables ownership handoff. The transport cannot
change flags. The authority can change only an inode owned by the resolved
transport or authority identity, and only to container `root:root`, also
resolved through the active namespace maps. FastDup
confirms a flag transition only after the containing Namespace sequence is durable. No whole-Repository mount,
block device or management socket is
passed into the guest. Keep this UID range exclusive to the managed container.

Veeam's `/etc/veeam/immureposvc` clock state is backed by the separate private
root `/srv/fastdup/repository/.fastdup-veeam-immurepo`. It is bound only at the
service-state path and is not visible through `/repository` or SMB. This lets
the user-namespaced service set and clear a real durable immutable flag on
`timeLog`; existing state is copied on first provisioning and the original
container copy is retained behind the bind mount.

Quota reports allocated logical file bytes through the existing Namespace
ledger; it is independent of physical Pool headroom. Advanced Reduction governs
new writer work beneath the container root. Both remain active when the
container stops or SMB settings change. A service restart does not remove data.
Changing network or bootstrap settings stops and restarts the container and
therefore interrupts its active transfers.

IPVLAN has a separate network namespace and shares the parent interface's MAC,
which avoids requiring extra source MACs on a VM vNIC. Host-to-child traffic on
the parent is isolated by Linux; reach the container from another LAN node.
Veeam needs SSH during installation, Installer TCP 6160, Transport TCP 6162 and
its selected data-transfer range (default 2500–3300). The container does not
reuse the host's SMB firewall profile. Network access restrictions must be
qualified with the chosen Veeam topology.

For a failed provision, inspect `fastdup-veeam-provision.service` and
`fastdup-veeam.service` journals on the appliance, correct the cause and apply
the settings again. An interrupted image extraction leaves
`/var/lib/machines/fastdup-veeam.staging` for inspection instead of overwriting
it. A Stop action disables automatic container start and retains all data and
configuration. A subsequent Start reapplies the recorded configuration.

## Certificate bootstrap result (Veeam 13.1.1.18)

The actual server generated a Linux Deployment Kit via the REST API. Its
Deployer and certificates installed successfully inside the container, but
registering this host with `credentialsStorageType=Certificate` was rejected:
Veeam requires credential authentication or a Veeam Infrastructure Appliance.
This matches the explicit limitation in [Using Veeam Deployment Kit](https://helpcenter.veeam.com/docs/vbr/userguide/deployment_kit.html).
Do not expose a working certificate-bootstrap option for this generic Linux
container or impersonate an Infrastructure Appliance. After one-time SSH
installation, Veeam's persistent services handle communication without SSH.
The temporary password is sent to `chpasswd` over stdin in the guest and is
never part of the persisted settings or audit record. Disabling installation
access clears the bootstrap key, locks the password, removes sudo permission
and prevents sshd from starting.

## Fast Clone adapter

The package binds `libfastdup-reflink.so` and a scoped `xfs_info` query read-only
into the container. Only Veeam Transport/Environment and their children preload
the adapter. `/repository` must resolve to the real FastDup FUSE mount; unrelated
filesystems retain their identity. This is an experimental compatibility layer,
not an actual XFS filesystem or a vendor support claim. See ADR 0101.

Set the Veeam repository to eight concurrent tasks, aligned blocks, Fast Clone
on XFS volumes and decompression before storing. The adapter translates each
supported FICLONE/FICLONERANGE into one metadata-only native range clone. It does
not copy data on errors. Requests above Linux MAX_RW_COUNT are rejected before
mutation. The service journal records `fastdup-reflink native_clones`, byte
counts and failures; verify them alongside Veeam's operation result.
