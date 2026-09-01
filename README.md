# fastdup

<p align="center">
  <strong>Turn suitable x86-64 hardware into a high-performance dedup appliance—managed from your browser.</strong>
</p>

<p align="center">
  English · <a href="README.de.md">Deutsch</a>
</p>

<p align="center">
  <a href="https://github.com/ThatIsCraZy/fastdup/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/ThatIsCraZy/fastdup"></a>
  <a href="LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <img alt="Rocky Linux 10 x86-64" src="https://img.shields.io/badge/platform-Rocky%20Linux%2010%20x86--64-lightgrey">
</p>

<p align="center">
  <strong><a href="https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm">Download the RPM</a></strong>
  · <a href="https://thatiscrazy.github.io/fastdup/">Product page</a>
  · <a href="https://github.com/ThatIsCraZy/fastdup/releases/tag/v0.5">Release notes</a>
</p>

fastdup is an experimental, software-defined single-node storage appliance for
Linux. Install one RPM on suitable x86-64 hardware, attach separate metadata
and DATA tiers, and expose normal files and directories through FUSE and
SMB/Samba. A multi-stage reduction pipeline and optimized Rust data path target
high throughput, while the embedded HTTPS WebUI keeps administration simple.

> [!WARNING]
> fastdup is a research prototype, not a production backup product. Do not use
> it as the only copy of important data. Current limitations are listed below.

## Measured with 10 vCPUs on a notebook-class processor

The benchmark VM ran on an Intel Core i7-1370P and exposed only ten logical
CPUs, AVX2/BMI2, and no AVX-512:

| Measurement | Result | Scope |
| --- | ---: | --- |
| Three serial SingleStream SMB uploads | **1,022.1 MiB/s** | current production path |
| First physical / fastest exact upload | **601.0 / 1,576.2 MiB/s** | byte-verified SMB run |
| Three-copy reduction | **67.78% saved / 3.104×** | including metadata; exact dedup alone is capped at 66.67% |
| SeqCDC AVX2/BMI2 scanner | **9,568 MiB/s** | isolated 1 MiB-slice Rocky-ISO scan |
| 50 live, minimally changed ISO versions | **49.07× DATA / 46.87× with metadata** | 50/50 first-cycle files BLAKE3-verified |

These are host- and workload-specific measurements, not an SLA. See the
[current SMB evidence](docs/benchmarks/hot-buffer-reuse-2026-09-01.md),
[601-second FUSE run](docs/benchmarks/io-intensive-fuse-600s.md), and
[interactive product page](https://thatiscrazy.github.io/fastdup/#performance).

The three-copy result is deliberately not a maximum-capacity benchmark: three
identical copies can demonstrate at most 3:1 from exact dedup alone. fastdup
already exceeds the corresponding 66.67% saving including repository metadata,
because its other reduction stages contribute too. Ratios such as 50:1 require
enough redundant versions and a suitable data mix. The current 50-live-version
workload reaches 49.07× on DATA and 46.87× including all allocated metadata;
the exact result is documented in the
[current reduction rerun](docs/benchmarks/iso50-live-reduction-2026-09-02.md).

fastdup goes beyond a classic exact-dedup-plus-compression pipeline. The durable
default combines SeqCDC content-defined chunking, BLAKE3-verified exact dedup,
sparse HOLE and constant-byte FILL extents, grouped adaptive RAW/Zstd with a
versioned saving threshold. Workloads using `copy_file_range` also get
metadata-only Fast Clone. A rebuildable similarity index with depth-1
`ZSTD_PREFIX` is opt-in. Content-identified dictionaries and sparse-XOR delta
remain clearly labelled research paths; similarity reorder was
evaluated and rejected in favor of restore locality. Proprietary appliance
internals are not fully disclosed, so this project does not claim an
unverifiable numeric technique advantage. The sourcing is recorded in the
[website claims research](docs/research/webpage-performance-reduction-claims-2026-09-02.md).

## Install on Rocky Linux 10

You need:

- Rocky Linux 10 on x86-64
- root access for the RPM installation
- two empty, physically separate block devices: one for metadata and one for DATA
- TCP 8080 reachable from your management network

Download and install the current binary package:

```bash
curl -LO https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm
curl -LO https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS

sudo dnf install ./fastdup-0.5.0-1.el10.x86_64.rpm
sudo systemctl enable --now fastdup-agent.service fastdup-control.service
```

Then open:

```text
https://<appliance-host>:8080/
```

The first certificate is self-signed. Sign in with `admin` / `fastdup01.` and
replace the initial password immediately; the WebUI enforces a minimum of twelve
characters before it accepts any management change.

The package deliberately starts only the management services. It does **not**
format disks or mount a repository automatically.

> [!CAUTION]
> Repository provisioning erases the partition tables and all data on both
> selected devices. Confirm model, serial number, WWN, capacity, and HBA path in
> the WebUI before you continue.

## Simple administration through the WebUI

The WebUI is included in the RPM and served by the local control plane. From a
single browser interface, an administrator can:

- select and provision the metadata and DATA devices with destructive-action guards
- mount, unmount, recover, and offline-scrub the repository
- create and edit SMB shares, access rules, encryption requirements, and quotas
- monitor throughput, capacity, deduplication, CPU/RAM use, disk I/O, and checkpoints
- configure online GC, pressure thresholds, auto-mount, maintenance windows,
  small-file placement, and optional advanced reduction
- inspect jobs and alarms, export the audit history, rotate TLS, and change passwords

![fastdup WebUI overview with sample data](docs/assets/webui-overview.png)

<p align="center"><em>WebUI preview with sample data: repository health, throughput, reduction, capacity, and disk I/O.</em></p>

| Safe device selection and provisioning | SMB shares and logical quotas |
| --- | --- |
| ![Drive provisioning in the fastdup WebUI](docs/assets/webui-drives.png) | ![SMB share management in the fastdup WebUI](docs/assets/webui-shares.png) |

<p align="center"><em>These screenshots are generated from the real React WebUI using its bundled preview dataset.</em></p>

## First-time setup

1. **Install the RPM** and start `fastdup-agent` plus `fastdup-control` as shown above.
2. **Open the WebUI**, accept or locally trust the initial certificate, sign in,
   and set a new administrator password.
3. **Choose “Laufwerke”** and select one eligible metadata device and one eligible
   DATA device. The WebUI excludes root, boot, swap, mounted, holder, and
   physically overlapping targets.
4. **Review the destructive confirmation** and initialize the repository. fastdup
   creates and mounts the required XFS pools with their storage roles.
5. **Mount the repository** from the “Repository” page.
6. **Create an SMB share** under “SMB-Freigaben”; choose users/groups, read-only
   state, encryption, access-based enumeration, and an optional logical quota.
7. **Monitor the system** on “Übersicht”, “Telemetrie”, and “Ereignisse”.

For firewalling, expose TCP 8080 only to the intended management network. SMB
client access follows the host's Samba/firewall policy.

## Everyday administration

| Task | WebUI location | Notes |
| --- | --- | --- |
| Check health and capacity | **Übersicht** | Live frontend throughput, reduction, reserve, and disk I/O |
| Mount or cleanly unmount | **Repository** | Unmount stops new mutations and checkpoints first |
| Run an integrity check | **Repository → Offline-Scrub** | Requires an offline repository |
| Manage physical targets | **Laufwerke** | Uses stable device identities; no free-form device paths |
| Create or restrict SMB shares | **SMB-Freigaben** | Users/groups, read-only, encryption, ABE, and logical quota |
| Inspect historical metrics | **Telemetrie** | Throughput, latency/resource and reduction views |
| Review work and alarms | **Ereignisse** | Job progress, failures, alerts, and CSV audit export |
| Change runtime policy | **Einstellungen** | GC, pressure, placement, maintenance, TLS, and password |

The repository process is isolated from the management plane by systemd slices.
Restarting the WebUI or its agent does not stop the mounted repository. The
repository runtime itself runs with swap disabled and performs a checkpointed
shutdown through `SIGINT`.

## What fastdup stores

Applications see a mutable POSIX filesystem; fastdup stores immutable,
content-identified containers and manifests underneath it:

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

The durable formats remain authoritative. Exact/similarity indexes, Bloom
filters, and read caches are only acceleration and can be rebuilt. Recovery,
offline scrub, and writers verify the same versioned invariants.

Current filesystem support includes random writes, sparse files, hardlinks,
symlinks, xattrs/ACLs, record locks, metadata-only `copy_file_range` clones,
crash recovery, DATA-tier recovery checkpoints, adaptive garbage collection,
per-share logical quotas, and policy-selected small-file placement.

## Command-line maintenance

Most routine work belongs in the WebUI. For recovery or scripted offline
maintenance, stop and unmount the repository before using `--offline`:

```bash
fastdup-maintenance --offline scrub METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline metadata-gc METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline rebuild-exact METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline rebuild-pool-indexes METADATA_ROOT DATA_ROOT
fastdup-maintenance --offline scrub-gc METADATA_ROOT DATA_ROOT
```

See the [maintenance guide](docs/operations/scrub-and-exact-index-rebuild.md)
for the exact recovery, scrub, index rebuild, and GC semantics.

To remove the software while intentionally retaining repository data:

```bash
sudo dnf remove fastdup
```

## Current limits

- Linux x86-64 only; the published RPM targets Rocky Linux 10
- requires honest stable storage and two physically separate XFS-backed tiers
- no built-in device redundancy, replication, WORM, encryption-at-rest policy,
  cloud tier, or device-loss protection
- incomplete POSIX and broad Samba/client conformance coverage
- Samba Fast Clone support remains experimental and is not yet Veeam-qualified
- advanced similarity reduction remains opt-in pending broader workload evidence
- no production support, performance SLA, or capacity commitment

Commercial backup appliances such as Dell PowerProtect Data Domain and HPE
StoreOnce provide mature integrations, retention, cyber-resilience, and support
that fastdup does not. fastdup is an open implementation for evaluation and
storage research, not a drop-in replacement.

## For developers

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

- [Domain language](CONTEXT.md)
- [Architecture Decision Records](docs/adr/)
- [Durable format specifications](docs/specs/)
- [Test plans](docs/testing/)
- [Operations guides](docs/operations/)
- [Reproducible benchmarks](docs/benchmarks/)
- [Control-plane architecture](docs/operations/control-plane.md)
- [Samba VFS status and limits](samba/vfs_fastdup/README.md)

## License

The Rust workspace and project documentation are licensed under
[Apache License 2.0](LICENSE). The in-process Samba module under
[`samba/vfs_fastdup`](samba/vfs_fastdup/README.md) is licensed separately under
GPL-3.0-or-later, as required for a Samba VFS module.
