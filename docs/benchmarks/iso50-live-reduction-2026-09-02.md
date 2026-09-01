# 50 live ISO versions: current reduction rerun

Date: 2026-09-02  
Revision: `adde99c09fadf690995feb02e4a19b4b8de9a105`  
Result: **PASS — 50/50 first-cycle variants restored byte-exactly**

## Question

The website previously quoted `42.95x` from the historical 601-second FUSE
churn run. That number divided all accepted ingest history by DATA plus roughly
860 MB of metadata from 237 intermediate checkpoints. It did not answer the
simpler product question: how much physical repository space does the current
code need while exactly 50 minimally changed versions are live?

## Corpus and method

The current production FUSE path ingested the pinned 2,072,444,928-byte Rocky
Linux 10.2 minimal ISO. Its SHA-256 was
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
The existing deterministic plan created 50 variants with eight globally unique,
nonzero one-byte XOR edits per variant: 400 distinct changed offsets in total.

The benchmark used:

- the current `fastdup-durable-fuse` binary and `fuse_iso_churn` harness;
- one writer and one reader so the capacity governor, not parallel worst-case
  reservation, determined the scratch size;
- the default production reduction path with Advanced Reduction off;
- online GC off, so the result receives no GC credit;
- separate 8-GiB Metadata and 16-GiB DATA `tmpfs` mounts under `.artifacts`;
- a 600-second lifecycle with a snapshot after the settle checkpoint while all
  first-cycle files were still live.

The RAM-backed tiers make this **not a device-throughput benchmark**. They do
not change the serialized Container or Metadata byte counts. The test ran on
the same 10-vCPU VM on an Intel Core i7-1370P used by the other current
benchmarks.

## Exact 50-version live snapshot

The snapshot was captured after all 50 writes and the settle checkpoint, before
any first-cycle delete. Every file had the full source length.

| Counter | Result |
| --- | ---: |
| live files | 50 |
| logical bytes | 103,622,246,400 |
| allocated DATA bytes | 2,111,537,152 |
| allocated Metadata bytes | 99,266,560 |
| allocated DATA + Metadata | 2,210,803,712 |
| logical / DATA | **49.0743x** |
| logical / (DATA + Metadata) | **46.8708x** |
| DATA saving | **97.9623%** |
| whole-repository saving | **97.8665%** |
| checkpoints at snapshot | 168 |

The first cycle then read and BLAKE3-verified all 50 variants before deleting
them. At the end of the fixed 601.09-second window, the harness had written a
second 50-file cycle, verified 20 of those additional files, deleted all 100
names, and exited successfully. The complete run therefore reported 100 files
written, 70 byte-exact restores, 100 deletes, and one fully completed cycle.

## Interpretation

The result is close to, but correctly below, a nominal `50:1`:

- `50:1` would require 50 byte-identical logical copies, one physical source
  copy, and no framing or metadata overhead;
- this corpus deliberately introduces 400 distinct byte changes, which create
  additional logical Chunks;
- DATA includes immutable Container framing and alignment;
- the whole-repository ratio also includes every allocated Metadata byte.

For this workload, the defensible current wording is therefore **49.07x on the
DATA tier and 46.87x including Metadata across 50 live, minimally changed ISO
versions**. It is a corpus result, not a universal reduction guarantee.

## Evidence

Workspace-local raw evidence is retained outside Git:

- `.artifacts/iso50-current-20260902-r5/live-50-snapshot.txt`;
- `.artifacts/iso50-current-20260902-r5/workload.log`;
- `.artifacts/iso50-current-20260902-r5/daemon.log`;
- `.artifacts/iso50-current-20260902-r5/final-snapshot.txt`.

The benchmark build used the required workspace-local
`CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`.
