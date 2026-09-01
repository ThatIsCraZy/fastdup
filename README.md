# fastdup

<p align="center">
  <strong>Crash-safe, deduplicating POSIX storage for Linux — built in Rust.</strong>
</p>

<p align="center">
  English · <a href="README.de.md">Deutsch</a>
</p>

<p align="center">
  <a href="https://github.com/ThatIsCraZy/fastdup/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/ThatIsCraZy/fastdup"></a>
  <a href="LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <img alt="Linux x86-64" src="https://img.shields.io/badge/platform-Linux%20x86--64-lightgrey">
  <img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-orange">
</p>

fastdup is an experimental single-node **deduplicating filesystem and storage
appliance**. It exposes a mutable Linux POSIX namespace through FUSE and
optionally SMB/Samba, while storing identical content-defined chunks only once.
The Rust storage engine combines SeqCDC, BLAKE3, Zstd, immutable containers,
rebuildable indexes, crash recovery, integrity scrubbing, and adaptive garbage
collection.

> [!WARNING]
> fastdup is a research prototype, not a production backup product. It has no
> device-loss protection, WORM guarantee, replication, or vendor support. Use
> it for evaluation and development, not as the only copy of important data.

## Why fastdup?

Most deduplication systems hide their storage semantics behind a proprietary
backup appliance. fastdup makes the interesting parts inspectable:

- **Transparent exact deduplication:** identical logical chunks share physical
  storage without changing byte-exact file behavior.
- **Real POSIX semantics:** random writes, sparse files, hardlinks, symlinks,
  xattrs/ACLs, ownership, timestamps, record locks, and open unlinked inodes.
- **Crash-safe generations:** immutable manifests and a checksummed commit WAL
  select the newest fully durable namespace after a crash.
- **Truth is separate from acceleration:** manifests and verified containers
  are authoritative; exact/similarity indexes, Bloom filters, and read caches
  may be discarded and rebuilt.
- **Auditable durability:** writers, recovery, and offline scrub enforce the
  same versioned invariants, backed by ADRs, fault injection, and benchmarks.
- **Bounded resource use:** `io_uring`, bounded ingest lanes, cache governance,
  and adaptive DATA/metadata GC are designed for predictable single-node use.

## How it works

```text
POSIX / FUSE / SMB write
          │
          ▼
   live namespace ──► SeqCDC-v1 ──► BLAKE3-256 ──► exact lookup
                                                        │
                              existing verified chunk ◄─┤
                                                        │ new
                                                        ▼
                                                   RAW or Zstd
                                                        │
                                                        ▼
                                  container ──► manifest ──► commit WAL
```

SeqCDC-v1 creates content-defined chunks from 16 KiB to 256 KiB. BLAKE3-256
identifies their contents. New neighboring chunks become compression regions
of at most 512 KiB and are stored independently as RAW or Zstd level 3.
Uniform allocated ranges become FILL extents; unallocated zero ranges remain
HOLE extents and consume no DATA record.

An optional advanced-reduction path adds a rebuildable similarity index and
depth-1 `ZSTD_PREFIX` records. It is explicitly opt-in and always falls back to
independently decodable RAW/Zstd storage when its acceleration state is absent
or stale.

## Current capabilities

- Create, open, read, write, truncate, rename, and unlink files
- Random writes, append, `O_TRUNC`, `O_EXCL`, `RENAME_NOREPLACE`, `flush`, and
  `fsync`
- Sparse DATA/FILL/HOLE extents, hole punching, zero ranges, and DATA/HOLE seeks
- Metadata-only clones through `copy_file_range`
- Subdirectories, hardlinks, symlinks, xattrs/ACLs, permissions, and ownership
- Crash recovery to the latest complete commit generation
- Independent DATA-tier recovery checkpoints for total metadata-tier loss
- Rebuildable exact and similarity indexes
- Offline scrub plus adaptive online/offline DATA and metadata garbage collection
- Read-only kernel page cache and readahead with explicit range invalidation
- Logical per-share quotas and policy-selected small-file placement
- HTTPS control plane with an embedded WebUI
- Experimental Samba VFS module for SMB Fast Clone

The durable path supports exact deduplication, RAW/Zstd encoding, sparse files,
checkpoints, recovery, scrub, and GC. Advanced reduction remains opt-in while
broader backup corpora are evaluated. fastdup targets **Linux on x86-64 only**;
AVX2/BMI2 paths are selected at runtime and retain scalar equivalents.

## Install release 0.5

The native RPM targets **Rocky Linux 10 on x86-64** and contains the FUSE
runtime, maintenance CLI, privileged appliance agent, HTTPS control plane,
embedded WebUI, systemd policy, and Samba VFS module.

```bash
curl -LO \
  https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm
sudo dnf install ./fastdup-0.5.0-1.el10.x86_64.rpm
sudo systemctl enable --now fastdup-agent.service fastdup-control.service
```

Open `https://<appliance-host>:8080/`. The first certificate is self-signed.
The initial credentials are `admin` / `fastdup01.` and the UI immediately
requires a new password of at least twelve characters.

The repository service does not start until two empty, physically separate
devices are selected for the metadata and DATA tiers in the WebUI.

> [!CAUTION]
> Provisioning erases the partition table and all contents of both selected
> devices. Verify the device identities before confirming.

The release page also publishes a source RPM and SHA-256 checksums:
[fastdup 0.5 release](https://github.com/ThatIsCraZy/fastdup/releases/tag/v0.5).

## Build and test

Requirements are Linux, a current Rust toolchain, and `/dev/fuse` for real
mount tests. Repository policy requires every generated artifact to stay under
`.artifacts/`:

```bash
cd /source/fastdup

export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH=/source/fastdup/.artifacts/cargo/bin:$PATH

mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$TMPDIR"

cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p fastdup-appliance
```

Cargo creates `CARGO_TARGET_DIR` itself; do not pre-create that directory.

To build the Rocky Linux RPM reproducibly, install Node.js/npm, `rpm-build`,
`patchelf`, and the Samba development packages, then run:

```bash
./packaging/build-rpm.sh
```

## Run a local lab mount

The production runtime requires separate physical XFS filesystems. For a local
single-disk lab only, create separate directories and enable the explicit lab
override:

```bash
mkdir -p \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers

FASTDUP_POOL_ISOLATION=lab-allow-shared \
  /source/fastdup/.artifacts/target/release/fastdup-durable-fuse \
  /source/fastdup/.artifacts/mount \
  /source/fastdup/.artifacts/repository/metadata \
  /source/fastdup/.artifacts/repository/containers
```

The daemon stays in the foreground. `Ctrl-C` stops mutation admission, creates
a final checkpoint, and unmounts cleanly.

### Enable advanced reduction

With the daemon stopped, build one coherent exact/similarity index pair before
enabling prefix selection:

```bash
BIN=/source/fastdup/.artifacts/target/release
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN/fastdup-maintenance" --offline rebuild-pool-indexes "$META" "$DATA"
FASTDUP_POOL_ISOLATION=lab-allow-shared \
FASTDUP_ADVANCED_REDUCTION=prefix-v1 \
  "$BIN/fastdup-durable-fuse" \
  /source/fastdup/.artifacts/mount "$META" "$DATA"
```

If the paired snapshot is missing or stale, writes remain available and fall
back to independent RAW/Zstd encoding.

## Maintenance

Stop the daemon and unmount before using the mandatory `--offline` mode:

```bash
BIN=/source/fastdup/.artifacts/target/release/fastdup-maintenance
META=/source/fastdup/.artifacts/repository/metadata
DATA=/source/fastdup/.artifacts/repository/containers

"$BIN" --offline scrub "$META" "$DATA"
"$BIN" --offline metadata-gc "$META" "$DATA"
"$BIN" --offline rebuild-exact "$META" "$DATA"
"$BIN" --offline rebuild-pool-indexes "$META" "$DATA"
"$BIN" --offline scrub-gc "$META" "$DATA"
```

See the [maintenance guide](docs/operations/scrub-and-exact-index-rebuild.md)
for recovery, scrub, index rebuild, and GC semantics.

## Repository map

| Path | Purpose |
| --- | --- |
| `crates/fastdup-format` | Versioned container, manifest, commit, and index formats |
| `crates/fastdup-store` | SeqCDC, reduction, containers, indexes, scrub, and GC |
| `crates/fastdup-io-uring` | Bounded asynchronous Linux container I/O |
| `crates/fastdup-posix` | POSIX model, live dirty overlay, and FUSE adapter |
| `crates/fastdup-appliance` | Ingest, checkpoints, recovery, and executables |
| `crates/fastdup-control` | HTTPS API, WebUI, provisioning, and telemetry |
| `crates/fastdup-testkit` | Deterministic faults, crash model, and corpus tools |
| `samba/vfs_fastdup` | Experimental Samba VFS module for Fast Clone |

## Design and evidence

- [Domain language](CONTEXT.md)
- [Architecture Decision Records](docs/adr/)
- [Durable format specifications](docs/specs/)
- [Test plans](docs/testing/)
- [Operations guides](docs/operations/)
- [Reproducible benchmarks](docs/benchmarks/)
- [Commercial appliance comparison methodology](docs/research/commercial-backup-appliance-comparison.md)
- [Samba VFS status and limits](samba/vfs_fastdup/README.md)

One example result: on the documented Rocky ISO workload, the isolated
AVX2/BMI2 SeqCDC scanner reached 8,009 MiB/s (2.90× the scalar scanner), while
the paired single-stream SMB benchmark improved end-to-end median throughput by
13.8%. These are host- and workload-specific measurements, not performance
promises; see the [full benchmark](docs/benchmarks/seqcdc-prototype-2026-08-22.md).

## Limitations

Before production use, fastdup still needs:

- Complete POSIX coverage and a broader client/Samba compatibility matrix
- Device-loss protection, replication, immutability, and encryption policy
- Long-running, randomized kill, and physical power-cut testing
- Broader versioned backup corpora and a production gate for advanced reduction
- Real Veeam protocol evidence for the Samba module
- Capacity and support commitments

Commercial systems such as Dell PowerProtect Data Domain and HPE StoreOnce
solve overlapping backup-storage problems but provide mature integrations,
replication, retention, cyber-resilience, and support that fastdup does not.
fastdup is intended for studying and extending an open POSIX deduplication
engine, not as a superiority claim or drop-in replacement.

## License

The Rust workspace and project documentation are licensed under
[Apache License 2.0](LICENSE). The in-process Samba module under
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) is licensed separately under
GPL-3.0-or-later, as required for a Samba VFS module.
