#!/usr/bin/env python3
"""Replay consecutive byte-exact clones over SMB 3.1.1.

SMB_TEST_SERVER/SHARE/USER and PASSWORD (or NT_HASH) configure authentication.
SMB_TEST_SOURCE names an existing immutable source, at least 2 MiB, opened
read-only. Only a unique test destination is created and removed.
"""
import os
import struct
import time
import uuid

from impacket.smbconnection import SMBConnection
from impacket.smb3structs import SMB2_DIALECT_311


def run():
    server = os.environ["SMB_TEST_SERVER"]
    share = os.environ["SMB_TEST_SHARE"]
    connection = SMBConnection(server, server, sess_port=int(os.getenv("SMB_TEST_PORT", "445")), preferredDialect=SMB2_DIALECT_311)
    nt_hash = os.environ.get("SMB_TEST_NT_HASH", "")
    connection.login(os.environ["SMB_TEST_USER"], os.environ.get("SMB_TEST_PASSWORD", ""),
                     nthash=bytes.fromhex(nt_hash) if nt_hash else b"")
    tree = connection.connectTree(share)
    name = ".fastdup-clone-sequence-" + uuid.uuid4().hex
    source = target = None
    created = False
    size = 2 * 1024 * 1024
    ranges = [
        (1617920, 1609728, 7168),
        (1625088, 1616896, 5120),
        (1630208, 1622016, 1),
        (1630209, 1622017, 8193),
        (1638402, 1630210, 65536),
        (1700003, 1800007, 5120),
        (size - 5120, size - 5120, 5120),
    ]

    def ioctl(handle, code, data=b"", output=0):
        return connection.getSMBServer().ioctl(
            tree, handle, ctlCode=code, flags=1, inputBlob=data,
            maxInputResponse=0, maxOutputResponse=output)

    def read_all(handle):
        return b"".join(connection.readFile(tree, handle, offset, 65536)
                        for offset in range(0, size, 65536))

    try:
        source = connection.createFile(tree, os.environ["SMB_TEST_SOURCE"],
                                       creationDisposition=1, desiredAccess=0x120089)
        assert connection.queryInfo(tree, source)["EndOfFile"] >= size, "source shorter than 2 MiB"
        algorithm = struct.unpack("<HHIII", ioctl(source, 0x9027C, output=16))[0]
        target = connection.createFile(tree, name, creationDisposition=2)
        created = True
        connection.getSMBServer().setInfo(tree, target, inputBlob=struct.pack("<Q", size),
                                          fileInfoClass=20)
        ioctl(target, 0x9C280, struct.pack("<HHI", algorithm, 0, 0))
        expected = bytearray(size)
        for source_offset, target_offset, length in ranges:
            started = time.monotonic()
            ioctl(target, 0x98344, bytes(source) + struct.pack("<QQQ", source_offset,
                                                             target_offset, length))
            elapsed = time.monotonic() - started
            data = connection.readFile(tree, source, source_offset, length)
            assert len(data) == length
            expected[target_offset:target_offset + length] = data
            print(f"clone source={source_offset} target={target_offset} bytes={length} "
                  f"seconds={elapsed:.6f}", flush=True)
        assert connection.queryInfo(tree, target)["EndOfFile"] == size
        assert read_all(target) == expected, "consecutive clone or neighboring bytes differ"
        connection.getSMBServer().flush(tree, target)
        connection.closeFile(tree, target)
        target = None
        target = connection.createFile(tree, name, creationDisposition=1, desiredAccess=0x120089)
        assert connection.queryInfo(tree, target)["EndOfFile"] == size
        assert read_all(target) == expected, "flush/reopen changed target bytes or size"
        print("PASS: complete 2 MiB target byte-exact before and after flush/reopen", flush=True)
    finally:
        if target is not None:
            connection.closeFile(tree, target)
        if source is not None:
            connection.closeFile(tree, source)
        if created:
            connection.deleteFile(share, name)
        connection.logoff()


if __name__ == "__main__":
    run()
