# vfs_fastdup

`vfs_fastdup` is the experimental Samba-side bridge for fastdup metadata range
clones. It is GPL-3.0-or-later, as required for an in-process Samba VFS module;
the Rust workspace remains under its repository license.

The module:

- advertises `FILE_SUPPORTS_BLOCK_REFCOUNTING` only when explicitly enabled;
- maps `FSCTL_DUPLICATE_EXTENTS_TO_FILE` to exactly one `copy_file_range` call
  on fastdup FUSE descriptors;
- implements a fixed, restart-stable Integrity Information state;
- rejects unsupported, misaligned, oversized, overlapping, out-of-bounds, or
  short clones without falling back to a buffered data copy; and
- fences CLOSE behind all previously accepted operations on that Samba handle.

CLOSE is an application-order fence, not an implicit durable checkpoint. A
successful clone is immediately visible through the live POSIX view and enters
fastdup's ordinary checkpoint window.

## Share configuration

The adapter is disabled by default. A development share enables it explicitly:

```ini
[backup]
    path = /path/to/fastdup-fuse-mount
    vfs objects = fastdup
    fastdup:enabled = yes
    fastdup:clone alignment = 65536
    fastdup:maximum clone bytes = 1073741824
```

The alignment must be a power of two of at least 4 KiB. The maximum must be a
nonzero alignment multiple and no larger than `0x7ffff000`, keeping every clone
inside one Linux `copy_file_range` syscall.

## Tests and Samba build

The portable contract test needs only a C11 compiler and stores its executable
under the repository artifact directory:

```bash
sh samba/vfs_fastdup/tests/run.sh
```

For an ABI compile, place a Samba source checkout below `.artifacts` and run:

```bash
sh samba/vfs_fastdup/build-against-samba.sh \
  /source/fastdup/.artifacts/samba-vfs-fastdup/samba-4.23.5
```

The helper copies the module into the disposable Samba source tree, configures
only workspace-local output, and builds `libvfs_module_fastdup.so`. Samba 4.23.5
is the currently validated source tag. A real SMB 3.1.1 Veeam trace is still a
release gate; this module must not be presented as production Veeam support.
