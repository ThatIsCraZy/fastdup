%global debug_package %{nil}

Name:           fastdup
Version:        0.5.0
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
* Tue Sep 01 2026 fastdup maintainers <noreply@fastdup.local> - 0.5.0-1
- Package the FUSE runtime, WebUI control plane, systemd policy, and io_uring setup.
