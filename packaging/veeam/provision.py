#!/usr/bin/python3
"""Root-only, repeatable provisioning. Never rebuild an installed service state."""
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

CONFIG = Path('/etc/fastdup/veeam.json')
ROOT = Path('/var/lib/machines/fastdup-veeam')
IMAGE = '/usr/share/fastdup/veeam-rootfs.tar.gz'
UID = 524288
IMMUTABILITY_STATE = Path('/srv/fastdup/repository/.fastdup-veeam-immurepo')

def run(*args):
    return subprocess.run(args, check=True, timeout=300, capture_output=True, text=True).stdout

def write(path, content, mode=0o644, shifted=False, guest_uid=0):
    temporary = path.with_name(path.name + '.fastdup-new')
    # Refuse links placed by the guest; never follow its files into host paths.
    if any(p.is_symlink() for p in [path, temporary, *path.parents] if p != Path('/')):
        raise RuntimeError('Symlink in managed container configuration path')
    path.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open('w') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(mode)
    if shifted:
        os.chown(temporary, UID + guest_uid, UID + guest_uid)
    temporary.replace(path)

def main():
    settings = json.loads(CONFIG.read_text())
    interface = settings['interface']
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,12}', interface):
        raise RuntimeError('Invalid parent interface')
    address = ipaddress.IPv4Interface(f"{settings['address']}/{settings['prefix']}")
    gateway = ipaddress.IPv4Address(settings['gateway'])
    dns = ipaddress.IPv4Address(settings['dns'])
    if gateway not in address.network or gateway == address.ip:
        raise RuntimeError('Invalid gateway')
    if run('findmnt', '-n', '-o', 'FSTYPE', '--mountpoint', '/srv/fastdup/repository').strip() != 'fuse':
        raise RuntimeError('FastDup mount is absent')
    repository = Path('/srv/fastdup/repository/veeam')
    if repository.is_symlink() or not repository.is_dir():
        raise RuntimeError('Veeam root is not a real directory')
    if IMMUTABILITY_STATE.is_symlink() or not IMMUTABILITY_STATE.is_dir():
        raise RuntimeError('Veeam immutability state root is not a real directory')
    for device in json.loads(run('ip', '-j', 'address', 'show')):
        if any(info.get('local') == str(address.ip) for info in device.get('addr_info', [])):
            raise RuntimeError('Container address is already assigned to the appliance')
    # DAD checks the actual L2 segment; no ping-only inference.
    run('arping', '-D', '-I', interface, '-c', '4', '-w', '5', str(address.ip))
    if not ROOT.exists():
        stage = ROOT.with_name('fastdup-veeam.staging')
        if stage.exists():
            raise RuntimeError('Incomplete image extraction; inspect the staging directory before retry')
        stage.mkdir(parents=True, mode=0o700)
        run('tar', '--numeric-owner', '--xattrs', '--acls', '-xzf', IMAGE, '-C', str(stage))
        stage.rename(ROOT)
    if not (ROOT / 'etc/fastdup-veeam-image-version').is_file():
        raise RuntimeError('Existing rootfs is not a managed FastDup image')
    # Run account creation inside the guest user namespace, with no host bind.
    run('systemd-nspawn', '--quiet', '--register=no', '--settings=no',
        '--private-users=524288:65536', '--private-users-ownership=chown',
        '--directory=' + str(ROOT), '/bin/bash', '-c',
        "getent passwd veeam >/dev/null || { useradd --uid 1000 --create-home --shell /bin/bash veeam; usermod --password '*' veeam; }; install -d -m 700 -o veeam -g veeam /home/veeam/.ssh" + ("; usermod --password '*' veeam" if not settings['sshEnabled'] else ""))
    shifted = ROOT.stat().st_uid == UID
    if ROOT.stat().st_uid not in (0, UID):
        raise RuntimeError('Unexpected rootfs ownership')
    legacy_immutability_state = ROOT / 'etc/veeam/immureposvc'
    if legacy_immutability_state.exists() and not any(IMMUTABILITY_STATE.iterdir()):
        for source in legacy_immutability_state.iterdir():
            if source.is_symlink() or not source.is_file():
                raise RuntimeError('Unexpected Veeam immutability state entry')
            target = IMMUTABILITY_STATE / source.name
            shutil.copy2(source, target, follow_symlinks=False)
            os.chown(target, source.stat().st_uid, source.stat().st_gid)
    write(ROOT / 'etc/resolv.conf', f'nameserver {dns}\n', shifted=shifted)
    write(ROOT / 'home/veeam/.ssh/authorized_keys', (settings['sshPublicKey'].strip() + '\n') if settings['sshEnabled'] else '', 0o600, shifted, 1000)
    write(ROOT / 'root/.ssh/authorized_keys', '', 0o600, shifted)
    write(ROOT / 'etc/ssh/sshd_config.d/01-fastdup-bootstrap.conf', 'Match User veeam\n    PasswordAuthentication yes\nMatch all\n' if settings['sshEnabled'] else '', shifted=shifted)
    write(ROOT / 'etc/sudoers.d/fastdup-veeam', 'veeam ALL=(ALL) NOPASSWD: ALL\n' if settings['sshEnabled'] else '', 0o440, shifted)
    network_unit = f'''[Unit]
Description=FastDup Veeam private IPv4 endpoint
Before=network.target sshd.service
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/sbin/ip link set host0 up
ExecStart=/usr/sbin/ip address replace {address} dev host0
ExecStart=/usr/sbin/ip route replace default via {gateway} dev host0
[Install]
WantedBy=multi-user.target
'''
    write(ROOT / 'etc/systemd/system/fastdup-network.service', network_unit, shifted=shifted)
    wants = ROOT / 'etc/systemd/system/multi-user.target.wants/fastdup-network.service'
    if wants.parent.is_symlink():
        raise RuntimeError('Symlink in managed systemd wants directory')
    if not wants.is_symlink() and not wants.exists():
        wants.symlink_to('../fastdup-network.service')
    # Closing SSH also prevents connections using credentials added by the guest.
    ssh_override = '[Unit]\nConditionPathExists=/etc/fastdup-ssh-enabled\n'
    write(ROOT / 'etc/systemd/system/sshd.service.d/access.conf', ssh_override, shifted=shifted)
    marker = ROOT / 'etc/fastdup-ssh-enabled'
    if settings['sshEnabled']:
        write(marker, '', shifted=shifted)
    else:
        marker.unlink(missing_ok=True)
    # Preserve vendor unit files and keep the compatibility layer scoped to
    # Veeam and its children. No global /etc/ld.so.preload modification.
    for service in ('veeamtransport', 'veeamenvironment', 'veeamimmurepo'):
        write(ROOT / f'etc/systemd/system/{service}.service.d/80-fastdup-reflink.conf',
              '[Service]\nEnvironment=LD_PRELOAD=/usr/local/lib/libfastdup-reflink.so\n',
              shifted=shifted)
    # Release 16 used the executable name instead of Veeam's unit name.
    obsolete = ROOT / 'etc/systemd/system/veeamimmureposvc.service.d/80-fastdup-reflink.conf'
    obsolete.unlink(missing_ok=True)
    nspawn = f'''[Exec]
Boot=yes
PrivateUsers={UID}:65536
NoNewPrivileges=no
[Files]
PrivateUsersOwnership=chown
Bind=/srv/fastdup/repository/veeam:/repository
Bind=/srv/fastdup/repository/.fastdup-veeam-immurepo:/etc/veeam/immureposvc
BindReadOnly=/usr/libexec/fastdup/veeam/libfastdup-reflink.so:/usr/local/lib/libfastdup-reflink.so
BindReadOnly=/usr/libexec/fastdup/veeam/xfs_info:/usr/local/sbin/xfs_info
[Network]
Private=yes
VirtualEthernet=no
IPVLAN={interface}:host0
'''
    write(Path('/etc/systemd/nspawn/fastdup-veeam.nspawn'), nspawn, 0o600)
    print('Veeam system container prepared; existing service state retained')

if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(f'Provisioning command failed: {error.cmd[0]}: {error.stderr.strip()}')
