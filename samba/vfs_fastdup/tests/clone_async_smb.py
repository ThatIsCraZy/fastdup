#!/usr/bin/env python3
"""Exercise concurrent Clone IOCTLs, ordering and CLOSE on one SMB connection.

Uses the same SMB_TEST_* variables as clone_ranges_smb.py, plus optional PORT
(default 445), ROUNDS (default 16), and MIN_CLOSE_SECONDS (default 0). The latter
is for a syscall-delay injection in an isolated test smbd, never production.
Existing SOURCE is read-only; every destination has a unique generated name.
"""
import os
import struct
import socket
import time
import uuid
from impacket.smbconnection import SMBConnection
from impacket.smb3structs import SMB2_DIALECT_311, SMB2_IOCTL, SMB2Ioctl, SMB2_CLOSE, SMB2Close, SMB2_CANCEL, SMB2Cancel
from impacket.nt_errors import STATUS_SUCCESS, STATUS_CANCELLED


def run():
    host = os.environ['SMB_TEST_SERVER']
    share = os.environ['SMB_TEST_SHARE']
    c = SMBConnection(host, host, sess_port=int(os.getenv('SMB_TEST_PORT', '445')),
                      preferredDialect=SMB2_DIALECT_311)
    nt_hash = os.getenv('SMB_TEST_NT_HASH', '')
    auth = {'nthash': bytes.fromhex(nt_hash)} if nt_hash else {}
    c.login(os.environ['SMB_TEST_USER'], os.getenv('SMB_TEST_PASSWORD', ''), **auth)
    tree = c.connectTree(share)
    smb = c.getSMBServer()
    names, opened = [], []
    size = 2 * 1024 * 1024
    half = size // 2

    def ioctl(handle, code, data=b'', output=0):
        return smb.ioctl(tree, handle, ctlCode=code, flags=1, inputBlob=data,
                         maxInputResponse=0, maxOutputResponse=output)

    def clone(target, source, offset=0, target_offset=0, length=half):
        data = bytes(source) + struct.pack('<QQQ', offset, target_offset, length)
        packet = smb.SMB_PACKET()
        packet['Command'], packet['TreeID'] = SMB2_IOCTL, tree
        body = SMB2Ioctl()
        body['FileID'], body['CtlCode'] = target, 0x98344
        body['MaxInputResponse'], body['MaxOutputResponse'] = 0, 0
        body['InputCount'], body['OutputOffset'], body['Flags'] = len(data), 0, 1
        body['Buffer'] = data
        packet['Data'] = body
        return smb.sendSMB(packet)

    def finish(ids):
        for request in ids:
            assert smb.recvSMB(request).isValidAnswer(STATUS_SUCCESS)

    def close(handle):
        # Impacket's high-level close deletes pathname bookkeeping even when
        # another handle to the same file remains open. Use the actual wire
        # CLOSE and retire that bookkeeping only after the last alias.
        packet = smb.SMB_PACKET()
        packet['Command'], packet['TreeID'] = SMB2_CLOSE, tree
        body = SMB2Close(); body['Flags'], body['FileID'] = 0, handle
        packet['Data'] = body
        assert smb.recvSMB(smb.sendSMB(packet)).isValidAnswer(STATUS_SUCCESS)
        name = smb._Session['OpenTable'].pop(handle)['FileName']
        if not any(entry['FileName'] == name for entry in smb._Session['OpenTable'].values()):
            smb.GlobalFileTable.pop(name, None)

    def read_all(handle):
        return b''.join(c.readFile(tree, handle, offset, 65536) for offset in range(0, size, 65536))

    try:
        source = c.createFile(tree, os.environ['SMB_TEST_SOURCE'], creationDisposition=1,
                              desiredAccess=0x120089)
        opened.append(source)
        assert c.queryInfo(tree, source)['EndOfFile'] >= size
        expected_source = read_all(source)
        algorithm = struct.unpack('<HHIII', ioctl(source, 0x9027C, output=16))[0]
        targets = []
        for _ in range(8):
            name = '.fastdup-async-clone-' + uuid.uuid4().hex
            target = c.createFile(tree, name, creationDisposition=2, desiredAccess=0x12019f)
            names.append(name); opened.append(target); targets.append(target)
            smb.setInfo(tree, target, inputBlob=struct.pack('<Q', size), fileInfoClass=20)
            ioctl(target, 0x9C280, struct.pack('<HHI', algorithm, 0, 0))
        # All eight requests are outstanding on this one connection, not eight
        # independent smbd processes. Compare each complete destination later.
        rounds = int(os.getenv('SMB_TEST_ROUNDS', '16'))
        start = time.monotonic()
        for i in range(rounds):
            offset = (i % 2) * half
            finish([clone(target, source, offset=offset) for target in targets])
        duration = time.monotonic() - start
        expected = expected_source[((rounds-1) % 2)*half:((rounds-1) % 2+1)*half] + bytes(half)
        for target in targets:
            assert read_all(target) == expected, 'parallel clone data/untouched bytes differ'
        print(f'parallel requests={rounds*8} logical_MiB_s={rounds*8/duration:.2f} seconds={duration:.6f}', flush=True)

        # Same inode through distinct handles: last overlapping clone wins in
        # arrival order. A synchronous Integrity SET can complete in between.
        alias = c.createFile(tree, names[0], creationDisposition=1, desiredAccess=0x12019f)
        opened.append(alias)
        pending = [clone(targets[0], source, offset=0), clone(alias, source, offset=half)]
        ioctl(alias, 0x9C280, struct.pack('<HHI', algorithm, 0, 0))
        finish(pending)
        assert read_all(targets[0]) == expected_source[half:] + bytes(half)
        close(alias); opened.remove(alias)

        # Submit then CLOSE without receiving the clone reply. CLOSE must be an
        # apply fence; reopening must expose all bytes and unchanged neighbors.
        pending = clone(targets[0], source, length=size)
        start = time.monotonic()
        close(targets[0]); opened.remove(targets[0])
        close_seconds = time.monotonic() - start
        finish([pending])
        target = c.createFile(tree, names[0], creationDisposition=1, desiredAccess=0x12019f)
        opened.append(target)
        assert read_all(target) == expected_source
        assert close_seconds >= float(os.getenv('SMB_TEST_MIN_CLOSE_SECONDS', '0'))
        print(f'PASS: parallel data, same-inode ordering, Integrity and CLOSE fence ({close_seconds:.6f}s)', flush=True)

        # A CANCEL races with an already accepted syscall. Either terminal
        # status is allowed, but CLOSE must still fence any applied mutation.
        pending = clone(targets[2], source, length=size)
        packet = smb.SMB_PACKET()
        packet['Command'], packet['TreeID'], packet['MessageID'] = SMB2_CANCEL, tree, pending
        packet['Data'] = SMB2Cancel()
        smb.sendSMB(packet)
        answer = smb.recvSMB(pending)
        assert answer['Status'] in (STATUS_SUCCESS, STATUS_CANCELLED)
        close(targets[2]); opened.remove(targets[2])
        target = c.createFile(tree, names[2], creationDisposition=1, desiredAccess=0x12019f)
        opened.append(target)
        content = read_all(target)
        assert content == expected_source if answer['Status'] == STATUS_SUCCESS else content in (expected, expected_source)
        print('PASS: CANCEL retains atomic result and CLOSE fence', flush=True)

        # Closing a SOURCE while its clone is in flight must also retain the
        # underlying handle until completion.
        pending = clone(targets[1], source, length=size)
        close(source); opened.remove(source)
        finish([pending])
        assert read_all(targets[1]) == expected_source
        print('PASS: source CLOSE retains in-flight clone', flush=True)

        # Abrupt disconnect on a second session while a clone is in flight.
        # No acknowledgement survived, so the destination may contain either
        # complete predecessor or complete successor, never partial contents.
        other = SMBConnection(host, host, sess_port=int(os.getenv('SMB_TEST_PORT', '445')),
                              preferredDialect=SMB2_DIALECT_311)
        other.login(os.environ['SMB_TEST_USER'], os.getenv('SMB_TEST_PASSWORD', ''), **auth)
        other_tree = other.connectTree(share)
        other_smb = other.getSMBServer()
        name = '.fastdup-async-disconnect-' + uuid.uuid4().hex
        other_source = other.createFile(other_tree, os.environ['SMB_TEST_SOURCE'], creationDisposition=1, desiredAccess=0x120089)
        other_target = other.createFile(other_tree, name, creationDisposition=2, desiredAccess=0x12019f)
        names.append(name)
        other_smb.setInfo(other_tree, other_target, inputBlob=struct.pack('<Q', size), fileInfoClass=20)
        other_smb.ioctl(other_tree, other_target, ctlCode=0x9C280, flags=1,
                       inputBlob=struct.pack('<HHI', algorithm, 0, 0), maxInputResponse=0, maxOutputResponse=0)
        other_smb.ioctl(other_tree, other_target, ctlCode=0x98344, flags=1,
                       inputBlob=bytes(other_source) + struct.pack('<QQQ', 0, 0, size),
                       maxInputResponse=0, maxOutputResponse=0, waitAnswer=0)
        time.sleep(0.05)
        sock = other_smb.get_socket()
        sock.shutdown(socket.SHUT_RDWR); sock.close()
        time.sleep(0.5)
        target = c.createFile(tree, name, creationDisposition=1, desiredAccess=0x12019f)
        opened.append(target)
        assert read_all(target) in (bytes(size), expected_source)
        print('PASS: disconnect preserves complete old/new contents and subsequent access', flush=True)

    finally:
        for handle in reversed(opened):
            close(handle)
        for name in names:
            c.deleteFile(share, name)
        c.logoff()


if __name__ == '__main__':
    run()
