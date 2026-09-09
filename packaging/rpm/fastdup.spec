%global debug_package %{nil}

Name:           fastdup
Version:        0.7.2
Release:        1%{?dist}
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
    %{buildroot}%{_unitdir} \
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
install -m 0644 systemd/* %{buildroot}%{_unitdir}/
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
%{_sysusersdir}/fastdup-control.conf
%{_tmpfilesdir}/fastdup.conf
%{_sysctldir}/90-fastdup-io-uring.conf
%config(noreplace) %attr(0640,root,fastdup-control) %{_sysconfdir}/fastdup/repository.env
%config(noreplace) %{_sysconfdir}/samba/fastdup.conf
%config(noreplace) %{_sysconfdir}/samba/fastdup-shares.conf

%changelog
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
