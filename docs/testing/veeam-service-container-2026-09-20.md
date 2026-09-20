# Veeam Linux service container — 20 September 2026

This is an experimental Linux repository integration with verified Veeam native
Fast Clone and scoped Hardened Repository immutability. The backing filesystem
is FastDup FUSE with a process-scoped XFS compatibility view, not real XFS or a
vendor support claim. Development, image generation and RPM builds ran locally.
The test appliance received the resulting RPM; the existing Veeam server,
repositories, backup data and original job were retained.

## Implementation and environment

Rocky Linux 10 with systemd-nspawn hosts the original Veeam Deployment and
Transport services, version 13.1.1.18. An IPVLAN endpoint provides a dedicated
IPv4 address; four ARP duplicate-address probes passed before provisioning.
Container root maps to host UID 524288 and the transport account to 525288.
Only `/srv/fastdup/repository/veeam` is bound into the guest as `/repository`.
The guest operating system and Veeam state persist separately.

The WebUI provisions and starts/stops this frontend and configures networking,
temporary installation access, Advanced Reduction and an optional logical quota.
SMB rejects the reserved name, including case and hidden-share aliases.
The Repository lifecycle now coordinates both SMB and the service container.
See [ADR 0100](../adr/0100-isolate-the-veeam-service-container-and-reserve-its-root.md)
and the [operator instructions](../operations/veeam-service-container.md).

## Verified behavior

| Check | Result |
| --- | --- |
| Original Veeam Deployment/Transport installation | Passed, 13.1.1.18 |
| Single-use SSH password bootstrap with sudo | Passed |
| Certificate-only registration of this generic Linux host | Rejected by Veeam |
| SSH shutdown after installation | Port 22 closed; password locked; key and sudo grant cleared |
| Installer and Transport after SSH shutdown and container restart | Active on TCP 6160 and 6162 |
| Advanced Reduction setting | `dependent_v1` and `off` both applied to the Veeam root |
| Logical quota | 1 GB and 1 TB settings applied without replacing SMB rules |
| `statvfs` with 1 TB quota | Reported 1,000,000,000,000 bytes |
| Allocation exceeding quota | Rejected with `ENOSPC` |
| Native `copy_file_range`, 4 MiB committed input | Completed; destination bytes match |
| Unadapted `FICLONE` on the bound FUSE mount | Rejected with `EOPNOTSUPP` (95) |
| Veeam repository type | `LinuxHardened`, Fast Clone enabled, seven immutable days |
| Repository immutable flags and retention xattrs | Set by Veeam and enforced |
| Lock owner transition and cleanup | `root:root`; prior lock flag cleared and file removed by Veeam |
| Persistent clock state | `timeLog` on private FastDup root, immutable after service restart |
| Local control tests | 61 passed, 1 ignored |
| Local frontend lifecycle tests | 6 passed |
| Local UI tests, including closing SSH in the same view as password generation | 69 passed |
| Control-library Clippy | Passed |

Workspace-wide Clippy remains blocked by pre-existing findings outside this
change; logs are retained with the local evidence. Browser automation was not
available in the execution environment, so UI validation used component tests,
the production build and the live typed control API.

## Certificate-only first connection

The production server generated a Linux Deployment Kit. Its original Deployer
and certificates installed inside the container. A registration request using
`credentialsStorageType=Certificate` was nevertheless rejected: Veeam requires
credential authentication or a Veeam Infrastructure Appliance for this host.
This agrees with the restriction in the official
[Deployment Kit documentation](https://helpcenter.veeam.com/docs/vbr/userguide/deployment_kit.html).
Installing the kit alone does not make a generic Linux container an
Infrastructure Appliance. The implemented path therefore uses temporary
credentials once, followed by closing SSH. No permanent bootstrap password is
stored in FastDup settings or audit records.

## Backup qualification

A new repository and a new clone of the explicitly authorized source job were
created. Only the clone was redirected. Its automatic schedule and automatic
retries are disabled. The new repository initially used two concurrent tasks, then eight as requested,
with per-VM backup files, block alignment and `decompressBeforeStoring=true`.
The backup configuration uses a 1 TB quota and Advanced Reduction off.
The original job configuration was compared with its pre-test JSON; all
pre-existing server, repository and job identifiers remained present.

The initial Active Full completed with Success for all eight VMs in 606.872
seconds. It transferred 304,383,916,728 bytes, or 501.562 MB/s over the complete
job (decimal units, including setup and finalization). After this two-task
baseline, the new repository was changed to eight concurrent tasks at the
operator's request. Existing repositories were not changed.

After the write-pipeline race fix described below, a second Active Full ran with
all eight tasks concurrently. It completed all eight VMs successfully in
391.461 seconds and transferred
304,190,978,032 bytes: **777.065 MB/s over the complete job**, including setup
and finalization. Veeam reported 111.6–125 MB/s per task. Its final load summary
was `Source 98% > Proxy 20% > Network 28% > Target 51%`, with Source as the
primary bottleneck. Compared with the two-task Active Full, wall time fell by
35.5% and complete-job throughput increased by 54.9%. Repository and container
remained active and the repository journal recorded no warning or panic.

The first two VM tasks transferred about 72.33 GB in 242 seconds (299 MB/s
aggregate). The next pair transferred about 77.88 GB in 104 seconds (749 MB/s
aggregate), with Veeam reporting Source as the bottleneck on both tasks.
These are task windows, not the whole-job average. The preceding SMB result
of 653 MB/s used eight concurrent tasks. Concurrency, cache state and the
pre-existing deduplication corpus differ, so this is not a controlled A/B
performance comparison. No performance tuning was applied during this run.

## Evidence and remaining gates

Private environment identifiers, credentials and generated vendor certificates
are excluded from this document. Local evidence is under
`.artifacts/veeam-container/`, including package provenance, filesystem probe,
policy tests, Veeam inventory snapshots and session/task results. Deployment
receipts are under `.artifacts/test-vm-update/`.

ADR 0101 documents the scoped XFS compatibility view and translation. ADR 0102
documents retention authorization and synchronous durable protection commits.
The separate real-XFS baseline phase, restore boot test, vendor-component
upgrade qualification and vendor support status remain outside this result.

## Dirty-target clone regression found during adapter qualification

A new range-clone probe overwrote 1 MiB of a newly written 4 MiB target. The
existing POSIX clone assertion incorrectly required resident dirty payload to
remain equal: legitimate retirement reduced it from 4 MiB to 3 MiB and aborted
the runtime. The completed backup was not running during this probe. Automatic
recovery completed and both frontends restarted.

A 16-byte local reproduction failed first at that assertion, then at the stale
dirty-payload counter after the assertion was corrected. The fix permits only
a decrease and retires the exact difference from the existing linked-inode
ledger. The new test checks untouched neighbors and no source/target DATA reads.
A durable test verifies that replacing dirty payload with a clone checkpoints
and recovers exact bytes without any DATA-container I/O. All seven selected
clone/recovery/fault-injection tests pass. No durable format changed.

## Packaged native reflink adapter

Release 0.8.1-12 packages a Veeam-process adapter as described in ADR 0101.
The native FUSE mount remains FUSE; only the scoped Veeam process view reports
XFS capabilities. FICLONE/FICLONERANGE become one native copy_file_range without
buffered copying. Direct tests passed range content, untouched neighbors,
bidirectional Copy-on-Write, cross-filesystem rejection, invalid-pointer EFAULT,
read-only EBADF and oversized-request rejection without destination mutation.
The geometry query reports the actual 4096-byte blocks and logical quota.

After enabling the adapter and eight repository tasks, the clone's subsequent
incremental completed successfully for all eight VMs. The first discovery
attempt failed before backup because the vendor xfs_info wrapper could not
query a FUSE directory and supplied block alignment zero; the scoped geometry
query fixes this. Veeam does not synthesize another Full on the same day as the
initial Full ([KB1868](https://www.veeam.com/kb1868)). A successful incremental
therefore establishes compatibility but is not evidence of Veeam cloning.

## Actual Veeam Fast Clone result

Veeam 13.1.1.18 exported a new incremental restore point into a standalone full
on the same new repository. Automatic export deletion was explicitly set
to Never. The source chain and all pre-existing production backups were retained.
The vendor Export-VBRRestorePoint session returned Success, starting at
03:59:43.244 and completing at 03:59:54.986 local time (11.742 seconds).

The vendor agent inherited the packaged adapter and recorded **7,924 native
clone calls covering 33,215,066,112 bytes, with zero clone errors**. The resulting
VBK was 33,220,407,296 bytes and represented a 120 GiB virtual disk plus VM
metadata. The difference consists of newly written backup metadata. Agent logs
record VirtualSyntheticEnabled=true and successful session completion. These
are actual Veeam-issued operations, not cp or a standalone ioctl probe.

Evidence: `.artifacts/veeam-clone/export-result.txt`, `export-agent.log`,
`export-windows.log`, `export-clone-counters.log`, and
`packaged-adapter-state.log`. The measured 33.2 GB in 11.7 seconds is logical
clone throughput including setup, not physical disk write throughput. This
qualifies the tested export/clone path; scheduled synthetic-full and broader
recovery/vendor-upgrade scenarios remain separate tests.

Veeam Backup Validator 13.1.0.411 subsequently read the exported full and returned
**exit code 0**; its repository agent session also completed successfully. The
parent backup ID initially returned "Cannot find last point". Using the exact
export's child backup ID, as documented in [KB4485](https://www.veeam.com/kb4485),
resolved that lookup issue. No repair or source modification was requested.
Evidence: `validator-success.txt`, `validator.xml`, and `validator-agent.log`
under `.artifacts/veeam-clone/`. This is backup-file integrity validation, not a
booted VM restore test.

## Normal job and write-pipeline race regression

A later ordinary job start used `performActiveFull=false` and eight concurrent
tasks. At 12:43 local time, six overlapping streams exposed a race between FUSE
write registration and final lane retirement. The registrar selected a lane,
then released the registry lock before incrementing its outstanding count. The
last predecessor could remove that lane during the gap, causing the registered
write to fail the lookup assertion. The repository process aborted and systemd
recovered it; no backup or repository was deleted.

A deterministic barrier-based regression test first reproduced the detached
lane, then passed after registration and the outstanding increment became one
registry operation. The `fastdup-posix` unit suite passed 42 tests with its
placement-policy microbenchmark ignored as declared, and all integration tests
passed. Clippy also passes with warnings denied. Release 0.8.1-14 containing this fix
was deployed successfully. A six-stream follow-up transferred 71,123,668,044
bytes without another runtime fault. Two other tasks were rejected before data
transfer because metadata left by the aborted run was not synchronized with the
Veeam database. The Veeam-prescribed rescan was run only for the new test
repository and completed successfully; it did not delete backup data.

The next ordinary job completed all eight VMs successfully in 93.149 seconds
and transferred 17,721,335,404
bytes (190.25 MB/s over the complete job, including setup and finalization).
Veeam reported per-task processing rates from 50 MB/s to 325 MB/s and the source
as the aggregate job bottleneck. Network traffic verification found no corrupt
blocks. The repository and service container remained active and the repository
journal recorded no warning or panic.

All eight tasks were Increment operations. Veeam ended the successful session
without starting a Synthetic Full, so this run produced no new clone calls. This
is consistent with the documented same-day Full suppression in KB1868 because
the clone already had a Full from the earlier qualification. The existing
vendor export remains the direct Fast Clone proof. Session, task and log evidence
for the ordinary runs and the repository rescan is under
`.artifacts/veeam-container/normal-run-*`.

The final 0.8.1-14 package includes the write-pipeline race fix. Its packaged
runtime SHA256 is
`bdcca1e4f8db82ee7453df192738fa6f1735aa9555279e47a740b1d5e98bfd58`
and adapter SHA256 remains
`866f6ee14a21819a36f9c7356434e7ccf57af9c3620478eeb62366808aa267de`;
the adapter still matches the binary used for export and validation exactly.

Final deployment receipt: `.artifacts/test-vm-update/20260920T105310Z-4f1110fbcf/job.json`.
The update completed successfully; the Repository reported online without a
runtime issue. Veeam services restarted with the identical adapter, SSH remained
closed, the exported VBK retained its exact size, and the live WebUI served the
updated Fast Clone wording.

## Hardened immutability qualification

The repository was converted to Veeam type `LinuxHardened` with seven immutable
days. Fast Clone, eight concurrent tasks, aligned blocks, per-VM files and
`decompressBeforeStoring=true` remained enabled. The appliance resolves the
running Immutability and Transport processes and their active user-namespace
maps on every policy reconciliation. In this installation that produced caller
`524288:524811`, writer `525288:524699` and final container `root:root`
`524288:524288`. The policy obtains those host IDs from live processes and maps;
the appliance configuration owns the private namespace range.

A syscall trace from the diagnostic run established that Veeam issues
`fchown(fd, 0, 0)` after applying retention. Release 0.8.1-24 separated the
writer, authority and final-owner identities. The following ordinary run then
completed with **Success** in
156.139 seconds. The Veeam log recorded successful immutable checkpoints,
created `.veeam.15.lock` as container `root:root`, cleared the immutable flag on
the preceding lock and removed it. The final lock and newest VIB files reported
the immutable flag; VIBs carried `user.immutable.until` dates seven days ahead,
and non-mutating `O_WRONLY` probes were rejected with `EPERM`.

The service clock file initially exposed a user-namespace limitation on the
container root filesystem: `FS_IMMUTABLE_FL` requires authority in the backing
filesystem's user namespace. Release 0.8.1-26 binds the separate private FastDup
root `/srv/fastdup/repository/.fastdup-veeam-immurepo` only at
`/etc/veeam/immureposvc`. Existing state was copied while its original container
copy was retained. On restart Veeam logged successful removal and reapplication
of the `timeLog` flag, and an independent ioctl check returned
`owner=0:523 immutable=true`. The service and its persistent clock state can
therefore perform both sides of the retention lifecycle.

The final RPM is `fastdup-0.8.1-26.el10.x86_64`, SHA-256
`73a108e6707175ddf36e4b983227c67cbae5e00ed078fcf913d8709e27b01051`.
The private deployment receipt remains under `.artifacts/test-vm-update/`. The helper
verified the installed EVR, all appliance services, the FUSE mount, running
binary hash and agent state `repositoryState=online` with no runtime issue.
The final Veeam preflight again compared the original qualification-job JSON
byte-for-byte and found it unchanged. No existing repository or backup was
deleted.
