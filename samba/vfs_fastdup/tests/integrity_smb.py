#!/usr/bin/env python3
"""Real SMB 3.1.1 Integrity regression; use a disposable writable test share.

Requires impacket. SMB_TEST_SERVER/SHARE/USER/PASSWORD come from the environment;
no credentials are printed. Only this run's uniquely named fixtures are removed.
"""
import os
import struct
import uuid

from impacket.smbconnection import SMBConnection
from impacket.smb3structs import SMB2_DIALECT_311


def run():
    server = os.environ["SMB_TEST_SERVER"]
    share = os.environ["SMB_TEST_SHARE"]
    connection = SMBConnection(server, server, preferredDialect=SMB2_DIALECT_311)
    connection.login(os.environ["SMB_TEST_USER"], os.environ["SMB_TEST_PASSWORD"])
    tree = connection.connectTree(share)
    path = "fastdup-integrity-" + uuid.uuid4().hex
    directory = path + "-dir"
    handle = None
    created = directory_created = False

    def ioctl(code, data=b"", output=0):
        return connection.getSMBServer().ioctl(
            tree, handle, ctlCode=code, flags=1, inputBlob=data,
            maxInputResponse=0, maxOutputResponse=output)

    def set_integrity(algorithm, flags=0):
        ioctl(0x9C280, struct.pack("<HHI", algorithm, 0, flags))

    def get_integrity(expected):
        result = struct.unpack("<HHIII", ioctl(0x9027C, output=16))
        algorithm, reserved, flags, chunk, cluster = result
        assert algorithm == expected and reserved == flags == 0, result
        assert chunk == (0 if expected == 0 else cluster), result
        return cluster

    def rejected(operation, status):
        try:
            operation()
        except Exception as error:
            assert error.get_error_code() == status, str(error)
        else:
            raise AssertionError(f"operation should fail with {status:#x}")

    try:
        handle = connection.createFile(tree, path, creationDisposition=2)
        created = True
        cluster = get_integrity(0)
        enabled = 1 if cluster == 4096 else 2
        for requested in (1, 2):
            set_integrity(requested)
            get_integrity(enabled)
            set_integrity(0xFFFF)
            get_integrity(enabled)
        for algorithm, flags in ((3, 0), (2, 1), (0, 1), (0xFFFF, 2)):
            rejected(lambda: set_integrity(algorithm, flags), 0xC000000D)
            get_integrity(enabled)
        rejected(lambda: ioctl(0x9C280, b"\0" * 7), 0xC000000D)
        payload = b"verified SMB integrity payload" * 1024
        connection.writeFile(tree, handle, payload)
        assert connection.readFile(tree, handle, bytesToRead=len(payload)) == payload
        connection.getSMBServer().flush(tree, handle)
        connection.closeFile(tree, handle)
        handle = None
        connection.rename(share, path, path + "-renamed")
        path += "-renamed"
        handle = connection.createFile(tree, path, creationDisposition=1,
                                       desiredAccess=0x80)
        get_integrity(enabled)
        rejected(lambda: set_integrity(0), 0xC0000022)
        get_integrity(enabled)
        connection.closeFile(tree, handle)
        handle = connection.createFile(tree, path, creationDisposition=1)
        set_integrity(0)
        get_integrity(0)
        connection.closeFile(tree, handle)
        handle = None
        connection.createDirectory(share, directory)
        directory_created = True
        handle = connection.createFile(tree, directory, creationDisposition=1,
                                       creationOption=1)
        set_integrity(2)
        get_integrity(enabled)
        connection.closeFile(tree, handle)
        handle = connection.createFile(tree, directory, creationDisposition=1,
                                       creationOption=1, desiredAccess=0x100)
        set_integrity(0xFFFF)
        get_integrity(enabled)
        print("PASS: Integrity SET/GET, UNCHANGED, invalid requests, file IO, "
              "rename/reopen, read-only handle rejection and directory handles")
    finally:
        if handle is not None:
            connection.closeFile(tree, handle)
        if created:
            connection.deleteFile(share, path)
        if directory_created:
            connection.deleteDirectory(share, directory)
        connection.logoff()


if __name__ == "__main__":
    run()
