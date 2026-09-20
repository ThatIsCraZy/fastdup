#!/usr/bin/env bash
# Build on the development machine, never on the appliance.
set -euo pipefail
workspace=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
root="$workspace/.artifacts/veeam-image/rootfs"
export TMPDIR="$workspace/.artifacts/tmp"
mkdir -p "$root" "$TMPDIR" "$workspace/.artifacts/veeam-image/logs"
dnf -y --installroot="$root" --releasever=10 \
  --setopt=cachedir="$workspace/.artifacts/veeam-image/cache" \
  --setopt=logdir="$workspace/.artifacts/veeam-image/logs" \
  --setopt=install_weak_deps=False install \
  bash systemd openssh-server openssh-clients sudo perl tar gzip xfsprogs \
  util-linux iproute procps-ng hostname which dnf nftables
install -d "$root/repository" "$root/root/.ssh" "$root/etc/ssh/sshd_config.d"
chmod 000 "$root/repository"
chmod 700 "$root/root/.ssh"
printf 'fastdup-veeam\n' > "$root/etc/hostname"
printf 'PermitRootLogin prohibit-password\nPasswordAuthentication no\nKbdInteractiveAuthentication no\n' > "$root/etc/ssh/sshd_config.d/00-fastdup.conf"
# The image carries no machine identity or private SSH host keys.
rm -f "$root/etc/machine-id" "$root/var/lib/dbus/machine-id" "$root"/etc/ssh/ssh_host_*
touch "$root/etc/machine-id"
# Unlock key-only root access without introducing a reusable password.
sed -i 's/^root:[^:]*:/root:*:/' "$root/etc/shadow"
systemctl --root="$root" enable sshd.service
systemctl --root="$root" mask systemd-udevd.service systemd-udevd-control.socket systemd-udevd-kernel.socket
cp "$workspace/packaging/veeam/check-repository.sh" "$root/usr/local/sbin/fastdup-check-repository"
chmod 755 "$root/usr/local/sbin/fastdup-check-repository"
cat > "$root/etc/systemd/system/fastdup-repository-guard.service" <<'UNIT'
[Unit]
Description=Verify the bound FastDup repository
Before=sshd.service
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/fastdup-check-repository
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
UNIT
mkdir -p "$root/etc/systemd/system/sshd.service.d"
printf '[Unit]\nRequires=fastdup-repository-guard.service fastdup-network.service\nAfter=fastdup-repository-guard.service fastdup-network.service\n' > "$root/etc/systemd/system/sshd.service.d/fastdup.conf"
systemctl --root="$root" enable fastdup-repository-guard.service
printf '1\n' > "$root/etc/fastdup-veeam-image-version"
tar --numeric-owner --xattrs --acls -C "$root" -czf "$workspace/.artifacts/veeam-image/rootfs.tar.gz" .
sha256sum "$workspace/.artifacts/veeam-image/rootfs.tar.gz" > "$workspace/.artifacts/veeam-image/rootfs.sha256"
