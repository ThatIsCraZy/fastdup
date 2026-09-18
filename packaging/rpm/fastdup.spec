%global debug_package %{nil}

Name:           fastdup
Version:        0.7.4
Release: 63%{?dist}
Summary:        Deduplicating POSIX storage appliance with an embedded WebUI
License:        Apache-2.0 AND GPL-3.0-or-later
URL:            https://github.com/ThatIsCraZy/fastdup
Source0:        %{name}-%{version}-%{_arch}.tar.gz
ExclusiveArch:  x86_64

Requires:       fuse3
Requires:       samba = 4.23.5
Requires:       samba-common-tools
Requires:       systemd
Requires:       systemd-udev
Requires:       util-linux
Requires:       xfsprogs
Requires:       openssl
Requires:       policycoreutils
Requires:       shadow-utils
Requires:       findutils

%description
fastdup is an experimental single-node POSIX storage appliance. This package
contains its FUSE repository runtime, offline maintenance tool, HTTPS control
plane with embedded WebUI, privileged provisioning agent, systemd resource
policy, and required Linux runtime configuration.

%prep
%setup -q

%build
# Release binaries and the WebUI are built by packaging/build-rpm.sh before
# rpmbuild is invoked. Keeping that build in the repository script guarantees
# that all Cargo/npm output remains below .artifacts as required by the project.

%install
install -d \
    %{buildroot}%{_libexecdir}/fastdup \
    %{buildroot}%{_libdir}/samba/vfs \
    %{buildroot}%{_bindir} \
    %{buildroot}%{_unitdir}/smb.service.d \
    %{buildroot}%{_sysusersdir} \
    %{buildroot}%{_tmpfilesdir} \
    %{buildroot}%{_sysctldir} \
    %{buildroot}%{_sysconfdir}/fastdup \
    %{buildroot}%{_sysconfdir}/samba

install -m 0755 bin/fastdup-durable-fuse %{buildroot}%{_libexecdir}/fastdup/
install -m 0755 bin/fastdup-control %{buildroot}%{_libexecdir}/fastdup/
install -m 0755 bin/fastdup-agent %{buildroot}%{_libexecdir}/fastdup/
install -m 0755 bin/fastdup-maintenance %{buildroot}%{_bindir}/
install -m 0755 samba-vfs/fastdup.so %{buildroot}%{_libdir}/samba/vfs/
install -m 0644 systemd/fastdup-agent.service systemd/fastdup-control.service systemd/fastdup-maintenance@.service systemd/fastdup-management.slice systemd/fastdup-repository.service systemd/fastdup-storage.slice %{buildroot}%{_unitdir}/
install -m 0644 systemd/smb.service.d/20-fastdup.conf %{buildroot}%{_unitdir}/smb.service.d/
install -m 0644 sysusers.d/fastdup-control.conf %{buildroot}%{_sysusersdir}/
install -m 0644 tmpfiles.d/fastdup.conf %{buildroot}%{_tmpfilesdir}/
install -m 0644 sysctl.d/90-fastdup-io-uring.conf %{buildroot}%{_sysctldir}/
install -m 0640 fastdup/repository.env %{buildroot}%{_sysconfdir}/fastdup/
install -m 0644 samba/fastdup.conf %{buildroot}%{_sysconfdir}/samba/
install -m 0644 samba/fastdup-shares.conf %{buildroot}%{_sysconfdir}/samba/

%post
systemd-sysusers %{_sysusersdir}/fastdup-control.conf >/dev/null 2>&1 || :
systemd-tmpfiles --create %{_tmpfilesdir}/fastdup.conf >/dev/null 2>&1 || :
%{_prefix}/lib/systemd/systemd-sysctl %{_sysctldir}/90-fastdup-io-uring.conf >/dev/null 2>&1 || :
if [ -f %{_sysconfdir}/samba/smb.conf ] \
    && ! grep -Fq '%{_sysconfdir}/samba/fastdup-shares.conf' %{_sysconfdir}/samba/smb.conf; then
    sed -i '/^[[:space:]]*\[global\][[:space:]]*$/a\# BEGIN fastdup managed include\n\tinclude = %{_sysconfdir}/samba/fastdup-shares.conf\n# END fastdup managed include' \
        %{_sysconfdir}/samba/smb.conf
fi
# Enable the narrow SELinux permission needed for Samba exports of FUSE.
if selinuxenabled; then
    setsebool -P samba_share_fusefs on || exit 1
fi
systemctl daemon-reload >/dev/null 2>&1 || :

%preun
if [ "$1" -eq 0 ]; then
    systemctl --no-reload disable --now fastdup-control.service fastdup-agent.service >/dev/null 2>&1 || :
    if [ -f %{_sysconfdir}/samba/smb.conf ]; then
        sed -i '/^# BEGIN fastdup managed include$/,/^# END fastdup managed include$/d' \
            %{_sysconfdir}/samba/smb.conf
    fi
fi

%postun
systemctl daemon-reload >/dev/null 2>&1 || :

%files
%doc README.md
%{_libexecdir}/fastdup/fastdup-durable-fuse
%{_libexecdir}/fastdup/fastdup-control
%{_libexecdir}/fastdup/fastdup-agent
%{_bindir}/fastdup-maintenance
%{_libdir}/samba/vfs/fastdup.so
%{_unitdir}/fastdup-agent.service
%{_unitdir}/fastdup-control.service
%{_unitdir}/fastdup-maintenance@.service
%{_unitdir}/fastdup-repository.service
%{_unitdir}/fastdup-management.slice
%{_unitdir}/fastdup-storage.slice
%{_unitdir}/smb.service.d/20-fastdup.conf
%{_sysusersdir}/fastdup-control.conf
%{_tmpfilesdir}/fastdup.conf
%{_sysctldir}/90-fastdup-io-uring.conf
%config(noreplace) %attr(0640,root,fastdup-control) %{_sysconfdir}/fastdup/repository.env
%config(noreplace) %{_sysconfdir}/samba/fastdup.conf
%config(noreplace) %{_sysconfdir}/samba/fastdup-shares.conf

%changelog
* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-63
- Share verified Container images across readers, reuse freshly sealed envelope
  views, and keep maintenance scans from consuming resident Container images.

* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-62
- Reapply the transient checkpoint staging escape hatch before every catch-up
  attempt and preserve metadata GC reachability.

* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-61
- Rebuild the repository runtime, checkpoint staging watchdog, and regression
  tests from the current working tree.

* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-60
- Keep the checkpoint staging escape hatch policy centralized and cover the
  frozen-cut staging deadlock, including its transient-pause restrictions,
  with executable regression tests.

* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-59
- Break the checkpoint staging deadlock in which a full pending-region gate
  blocked frozen commit-cut Ingest while drain absorption waited on that same
  gate; transient admission closure now opens a bounded one-generation escape
  hatch and clears it after successful commit or an empty checkpoint.

* Fri Sep 18 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-58
- Decode exact Metadata-GC namespace graphs through a lightweight inode-transition
  and Manifest-root view, reducing a 100k-file exact mark from 1469 ms to 132 ms.
- Reuse one consecutive Namespace graph across exact metadata marks.

* Thu Sep 17 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-57
- Add an online-GC run-now action to the Control Plane UI and agent command
  surface, with machine-readable runtime gate responses and an online repository
  precondition.

* Thu Sep 17 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-56
- Wake mutation-admission waiters when the final commit-cut or administrative
  fence releases, removing a rare FUSE direct-write stall.
- Compact Metadata Mark Catalogs at the 32-run chain limit from retained catalog
  and Commit authority, covering publication, old-run retirement, and the final
  directory sync with fault-injection regressions.
- Merge frozen-cut drain residue into the checkpoint writer and refresh read and
  cache telemetry boundaries.

* Thu Sep 17 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-53
- Close mutation admission atomically and notify the write-through observer so a
  checkpoint timeout releases already admitted writers instead of deadlocking
  against Ingest backpressure. Management and Online-GC state reads no longer
  queue behind the draining admission fence.

* Thu Sep 17 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-52
- Rebuild from the current tree with checkpoint cut, generation, metadata cache,
  and durable read-path updates.

* Thu Sep 17 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-51
- Rebuild from the current tree with the underfilled Container fill-compaction
  ranking, bounded ContainerImage cache, and stale-candidate recovery changes.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-50
- Treat a missing Container in the advisory GC candidate queue as an absent hint
  instead of a proof failure, so fill-compaction shortlists cannot block Online-GC
  behind stale catalog rows.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-49
- Rank aged, active, sub-4 MiB Containers as fill-compaction candidates and retain
  their Demand-published images in the unified cache. Independent maintenance proof
  reads remain independent of the image pool.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-48
- Cancel Online-GC contention only when durable progress is stalled or
  mutation admission is closed, so healthy background GC is not aborted by every
  short checkpoint cycle.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-47
- Give each shared-publication member its own retirement fence. A shared group
  now blocks only through its group barrier; members no longer wait for the
  highest ordinal among unrelated group participants.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-46
- Cancel Online-GC contention while checkpoint work owns durable progress and
  back off canceled quanta, while reporting repeated publication-retirement
  waits with bounded queue-state diagnostics.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-43
- Charge pinned ExactMembership against shared cache headroom without counting
  it toward the evictable unified-cache target, preventing the recovery lease
  assertion introduced by Build 41.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-42
- Populate pinned ExactMembership during Independent and Scan recovery by
  exempting only that class from ordinary admission bypass; all other cache
  intents remain unchanged.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-41
- Keep active-Run ExactMembership filters pinned in the unified read cache:
  pressure, swap and class ceilings cannot evict them, while Exact pages and
  page bounds remain reclaimable. Construct missing filters during publication
  and audit without waiting for an evictable-budget window.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-40
- Let bounded Exact-cache warming overlap Online GC without waiting for GC
  quiescence, and base its frontend-idle gate on POSIX read/write activity.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-39
- Deploy the latest metadata-read cache, Exact-index read, and Unified Cache
  changes together with the validated repository/SMB lifecycle.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-38
- Replace the unsupported repository stop hook with a supported SMB dependency
  drop-in. The Runtime now bounds every systemctl request externally, rechecks
  activation after a timed-out request, repeats forced termination while needed,
  and uses lazy unmount only after confirming a remaining FUSE mount.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-37
- Coordinate the SMB frontend with the repository mount: stop and, if necessary,
  force-close SMB before orderly unmount, retry mount release after clients
  exit, and restore an enabled SMB unit only after a new Runtime has mounted.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-36
- Add a bounded, pressure-aware Exact-Index warm worker behind the single
  verified-cache owner, enforce a protected 70% Exact acceleration ceiling, and
  expose adaptive Exact-cache status and configuration through appliance telemetry.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-35
- Serialize tracked cache ownership release with admission accounting so an
  ephemeral namespace drop cannot expose charged entries without a clock owner;
  keep production cache pressure graceful instead of aborting if that invariant
  is ever violated again.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-34
- Set the controller deadband below the demand-growth step so a cache can expand
  from its cold target while alternating live-memory candidates still remain
  suppressed by the consecutive-period streak.

* Wed Sep 16 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-33
- Damp the shared cache-budget target and its candidate headroom so alternating
  live-memory samples stay inside a hysteretic deadband instead of producing
  one-sample GiB target swings; sustained trends and safety pressure still move
  admission immediately.
- Round Verified DATA, Exact membership, and Historical Proof class targets to
  a stable granularity, and suppress repeated cache-pressure refresh atomics on
  the read hot path.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-32
- Release short-lived Exact writer and bounds cache namespaces by the keys they
  actually admitted, instead of scanning every shard clock at namespace drop.
- Defer stale FIFO-key compaction until enough removals can pay for the scan,
  removing repeated exact publication and recovery cache-ownership stalls.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-31
- Buffer scrub-progress frames in a bounded pending writer and flush them at the
  1 MiB limit or the existing batch-sync boundary, removing one storage write and
  writer readback from every scrub certificate.
- Preserve crash-safe scrub resume by validating only complete durable frames on
  load, making unflushed suffixes repeatable, and covering buffer visibility,
  partial-flush loss, and cooperative-stop behavior with structural tests.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-30
- Defer intermediate Direct-I/O length-head synchronization for unpublished
  immutable temporary objects, including Exact Runs, Run Sets, streamed partitions,
  and staged metadata. The final file synchronization before rename remains the
  publication barrier.
- Keep authoritative WAL and published-object length-head barriers synchronous,
  and cover the deferred temporary path with a Direct-I/O regression test.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-29
- Reuse the independently recovered Exact Run Set when deriving startup RETIRING
  entries, and let that immutable Run Set seed the bounded RETIRING projection.
- Rebind namespace startup to the prepared Exact generation without reloading its
  Activation Log when the recovery writer snapshot is still synchronized.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-28
- Reuse an already installed Exact generation for repeated in-process recovery,
  avoiding a redundant complete audit of unchanged immutable Runs.
- Report startup timings for Exact activation recovery and Online-GC finalization.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-27
- Make Online-GC manifest, Metadata-GC, liveness, Exact-retirement and generation-drain
  waits honor cooperative shutdown cancellation, allowing a long maintenance cycle to
  stop promptly without SIGKILL.
- Preserve durable RETIRING recovery points when a stop interrupts retirement.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-26
- Bound Verified DATA read-cache share to 20 percent of the Unified Read Cache and
  protect Exact cache pages from ordinary Verified-DATA displacement.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-24
- Track Recovery Checkpoint protection by root identity instead of transient
  refcount, so unchanged checkpoint pins no longer invalidate the clean Metadata
  mark and force an exact scan every maintenance quantum.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-23
- Reuse the process-local audit proof for an unchanged immutable GC candidate
  catalog, avoiding its complete row-hash reread every online GC quantum while
  preserving a fresh lease and a full audit after publication or restart.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-22
- Cache a converged Exact Run-reference window while the Activation Log and
  installed/retired generation set are unchanged; deferred unlinks still force
  the next idle sweep to retry from durable references.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-21
- Carry a bounded process-local Exact RETIRING projection across normal L0
  appends, so idle Online-GC quanta no longer merge every active Exact Run.
- Rebuild RETIRING authority independently after fresh recovery or audit, and
  force full durable scans whenever compaction invalidates the projection.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-20
- Drive Online-GC through a bounded candidate queue and collect each proven
  victim set once, publishing retired candidates without a pool-sized rebuild.
- Bound victim proof by candidate Chunk and dependent-target budgets; remove
  the process-wide reverse-dependency cache and its pool-sized full scans.

* Tue Sep 15 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-19
- Retire superseded GC candidate catalog generations every Online-GC quantum.
- Retire Exact Runs and Run Sets outside the paired Activation-Log window and
  live generation pins; an idle repository now converges to bounded on-disk
  Exact and hint-object counts.

* Sat Sep 12 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.4-1
- Bound missing-Exact-index fallback to required Records and dependencies.
- Separate mount health from telemetry gaps and ordinary write backpressure.
- Reclaim resident free allocator arenas on a bounded background cadence.
- Display disk read and write IOPS alongside throughput in the WebUI.

* Wed Sep 09 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.3-1
- Release compressed Verified Read caching, Metadata GC crash fixes and live Runtime health.
- Refresh bilingual README, product page, screenshots and release documentation.

* Wed Sep 09 2026 fastdup maintainers <noreply@fastdup.local> - 0.7.2-2
- Retire Metadata GC journals before exact collection and safe content republication.
- Reflect Runtime loss and closed write admission in live UI state and audit events.
- Recover disconnected FUSE mountpoints after a daemon crash.
- Compress cold Verified Read entries within the adaptive shared RAM budget.

* Wed Sep 09 2026 fastdup contributors - 0.7.2-1
- Cache verified Manifest nodes and dispatch independent Samba clones asynchronously
- Include exact byte-granular Veeam clone continuation fixes
- Correct physical reduction and reorganize cache/read-avoidance telemetry

* Wed Sep 09 2026 fastdup contributors - 0.7.1-3
- Accept consecutive byte-granular Veeam clones without cluster alignment limits

* Wed Sep 09 2026 fastdup contributors - 0.7.1-2
- Clone exact partial-cluster lengths from Veeam without DATA I/O or rounding

* Wed Sep 09 2026 fastdup contributors - 0.7.1-1
- Accept Veeam 4 KiB-aligned clones and reconcile legacy native Integrity flags
- Learn admission for speculative cold Base reads and expose decision counters

* Mon Sep 07 2026 fastdup maintainers - 0.7.0-1
- Repository usage overview and five-minute cache counter windows.
- Durable resume of incomplete background scrub rounds.

* Mon Sep 07 2026 fastdup contributors <fastdup@localhost> - 0.6.4-14
- Mount from the committed Metadata graph without scanning Container storage.
- Detect missing committed Chunks during paced initial scrub before allowing GC.
- Resume after a damaged WAL suffix while preserving its validated prefix.

* Mon Sep 07 2026 fastdup contributors <fastdup@localhost> - 0.6.4-13
- Mount after structural verification and scrub payloads in the background.
- Bound scrub I/O, gate GC, latch integrity failures and show scrub telemetry.
- Copy committed recovery metadata without repeated full DATA verification.

* Mon Sep 07 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-12
- Share cache RAM by measured benefit with a 92-percent operating ceiling.
- Expose cache budgets, tier priorities and reservations in the WebUI.
- Partition large logical Manifest layouts and keep mixed shrink updates path-local.
- Bind GC catalog bootstrap counts to one concurrent-publication name snapshot.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-8
- Serve management requests independently of checkpoint waits.
- Read reduction telemetry without locking ingest lanes.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-7
- Preserve shared ingest lane ownership across truncate and checkpoint overlap.
- Keep detached publication failure handling independent of producer lane locks.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-4
- Treat a full Zstd trial output at a Chunk boundary as RAW fallback.
- Prevent checkpoint failure and blocked SMB writes on incompressible tails.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-3
- Keep checkpoint and recovered readers on the current Exact index after activation.
- Avoid repeated full Container scans while retaining bounded GC operation pins.
- Allow repository recovery to finish before startup share activation times out.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-2
- Fix Veeam ReFs.SetFileIntegrity: persist and report enabled SMB integrity policy.
- Validate handle permissions and clone integrity-policy compatibility.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.4-1
- Fix guest and authenticated SMB filesystem access and SELinux FUSE policy.
- Create separate SMB accounts through the authenticated WebUI and root agent.
- Refresh inventory after login and adapt oversized Small-File quotas with warnings.

* Sun Sep 06 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.1-1
- Optimize ingest, reads, rechunking, and bounded worker/cache coordination.
- Retain SIGINT notifications during supervisor work for reliable shutdown.
- Fix post-commit and fallocate Metadata-I/O failure handling.

* Sat Sep 05 2026 fastdup maintainers <noreply@fastdup.local> - 0.6.0-1
- Add persistent online similarity, per-share reduction policy, and Sparse-XOR.
- Optimize storage hot paths and bound telemetry history aggregation memory.
- Refresh bilingual documentation and the GitHub Pages product page.

* Tue Sep 01 2026 fastdup maintainers <noreply@fastdup.local> - 0.5.0-1
- Package the FUSE runtime, WebUI control plane, systemd policy, and io_uring setup.
