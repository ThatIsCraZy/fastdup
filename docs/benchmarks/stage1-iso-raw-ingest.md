# Stage-1 Rocky ISO RAW ingest and restore

Date: 2026-08-15. This is a correctness and sustained-I/O run of the currently
implemented RAW Container v1 store. It is not a deduplication, delta, CDC, FUSE,
or POSIX result.

## Corpus and method

The pinned Rocky Linux 10.2 minimal ISO from [the corpus
specification](corpus.md) was reflink-copied into ten fixtures. Each fixture has
the original length of `2072444928` bytes and exactly eight deterministic,
single-byte XOR edits at distinct pseudorandom offsets. The plan seed is
`0x4d595df4d0f33173`. `cmp -l` independently established that each fixture
differs from the base only at its eight planned offsets, and every fixture was
SHA-256 hashed. The 80 edits touch 80 distinct fixed 64-KiB positions, so no two
mutations are hidden inside one harness chunk.

The ingest harness uses fixed 64-KiB chunks and groups at most 512 chunks per
container. Fixed chunking is a harness choice because CDC is not implemented.
Every publish includes format encoding, a production-path full reread, all CRC
and BLAKE3 checks, file sync, no-replace rename, and directory sync. A successful
publish taking longer than ten seconds fails the run.

After all writes, the harness drops and reopens the store. Its in-memory recipe
then reads every container through the production verifier and compares every
restored chunk byte-for-byte with a fresh sequential read of its source ISO.
Finally, the compact startup audit independently enumerates and fully verifies
all containers without retaining their payloads.

Restore and startup audit follow the ingest on a 128-GiB host without dropping
the kernel page cache. Their rates are hot-path verifier/reconstruction numbers,
not cold data-tier restore forecasts.

## Result

Result: PASS.

| observation | result |
| --- | ---: |
| variants | 10 |
| logical bytes | 20,724,449,280 |
| fixed chunks | 316,230 |
| source bytes changed / fixed chunks touched | 80 / 80 |
| immutable containers | 620 |
| published file bytes | 20,833,239,040 |
| allocated bytes | 20,833,239,040 |
| format overhead | 0.5249% |
| exact-reuse candidate chunks | 284,545 |
| exact-reuse candidate bytes | 18,647,941,120 (89.9804%) |
| actual Exact-Dedup hits | 0 |
| actual Delta encodings | 0 |
| publish p50 / p99 / maximum | 116 / 168 / 994 ms |
| publishes over ten seconds | 0 / 620 |
| ingest elapsed / logical rate | 89.174 s / 232.40 MB/s |
| byte-exact hot restore elapsed / logical rate | 27.831 s / 744.65 MB/s |
| maximum ingest-process RSS | 217,032 KiB |
| independent full startup audit | 27.43 s, 195,476 KiB maximum RSS |

The 89.98% figure counts repeated BLAKE3 identities in the workload and is only
a measure of Exact-Dedup opportunity. The RAW store physically writes every
copy. Likewise, the eight-byte edits make useful future Delta fixtures, but no
Delta implementation exists to exercise or credit yet.

The ten-second observation is a lower-layer container-publish proxy. It does not
establish the appliance guarantee for POSIX writes because the namespace,
commit scheduler, manifests, and FUSE interface do not yet exist. That claim is
gated by the [POSIX conformance plan](../testing/posix-conformance.md).

## Reproduction

The commands refuse to overwrite an existing variant or store directory:

```bash
cargo run --release -p fastdup-testkit --example prepare_iso_variants
cargo run --release -p fastdup-testkit --example ingest_iso_variants
cargo run --release -p fastdup-testkit --example audit_container_store
```

The measured store is
`/source/fastdup/.artifacts/tier-data/iso-raw-ingest-v1`. The variant manifest,
including every offset and before/after byte, is
`/source/fastdup/.artifacts/tier-data/corpus/rocky-minimal-variants-v1/manifest-v1.tsv`.
Generated stores and corpora remain workspace-local artifacts rather than
source files.
