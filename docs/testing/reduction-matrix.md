# Data-reduction matrix harness

`reduction_matrix` runs one explicit reduction policy over one or more input
files, ingests the files sequentially into the same in-memory `ReductionEngine`,
then restores every object and compares it byte-for-byte with a fresh read of
its source path. Inputs must therefore remain unchanged for the complete run.

All build, temporary, report, and corpus paths must stay below
`/source/fastdup/.artifacts`. The harness downloads nothing.

## Run one policy

```bash
mkdir -p /source/fastdup/.artifacts/target /source/fastdup/.artifacts/tmp
RUSTUP_HOME=/source/fastdup/.artifacts/rustup \
CARGO_HOME=/source/fastdup/.artifacts/cargo \
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
PATH=/source/fastdup/.artifacts/cargo/bin:$PATH \
cargo run --release -p fastdup-store --example reduction_matrix -- \
  --preset exact --workers 8 --inflight-mib 64 \
  /source/fastdup/.artifacts/tier-data/corpus/structured-v1/inventory-v1.json \
  /source/fastdup/.artifacts/tier-data/corpus/structured-v1/inventory-v2.json
```

To benchmark an immutable, content-identified Zstd dictionary, pass every
training sample separately. Training completes before the timed ingest and
restore phases; the fixed CSV suffix records the resulting BLAKE3-256 ID and
exact dictionary length. The default maximum is 64 KiB. Training files are
deterministically presented to Zstd as ordered 16-KiB samples; this makes a
small number of large XML/JSON files a valid training corpus without treating
sample boundaries as an implementation accident.

```bash
RUSTUP_HOME=/source/fastdup/.artifacts/rustup \
CARGO_HOME=/source/fastdup/.artifacts/cargo \
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
PATH=/source/fastdup/.artifacts/cargo/bin:$PATH \
cargo run --release -p fastdup-store --example reduction_matrix -- \
  --preset grouping --workers 8 --inflight-mib 64 \
  --dictionary-kib 64 \
  --dictionary-sample /source/fastdup/.artifacts/tier-data/corpus/structured-v1/inventory-v1.json \
  --dictionary-sample /source/fastdup/.artifacts/tier-data/corpus/structured-v1/inventory-v2.json \
  /source/fastdup/.artifacts/tier-data/corpus/structured-v1/inventory-v3.json
```

Dictionary samples are not ingested unless they are also named as positional
inputs. A dictionary requires `COMPRESSION`; invalid combinations fail rather
than silently changing the selected feature policy. The reported payload cost
does not include the dictionary object, so comparisons must add
`dictionary_bytes` once per retained dictionary generation.

Without an explicit feature selection, the policy is `raw`. Repeatable direct
flags (`--raw`, `--cdc`, `--exact`, `--compression`, `--grouping`,
`--similarity`, `--delta`, and `--reorder`) build an exact custom feature set;
`--all` selects every flag. `ReductionPolicy::v1` remains authoritative for
dependency validation, so invalid custom combinations fail instead of silently
enabling prerequisites.

The named presets add only the prerequisites useful for that experiment:

| preset | enabled features |
| --- | --- |
| `raw` | RAW |
| `cdc` | RAW, CDC |
| `exact` | RAW, CDC, Exact |
| `compression` | RAW, CDC, Compression |
| `grouping` | RAW, CDC, Compression, Grouping |
| `similarity` | RAW, CDC, Exact, Similarity |
| `delta` | RAW, CDC, Exact, Similarity, Delta |
| `reorder` | RAW, CDC, Compression, Grouping, Reorder |
| `all` | every feature |

The report exposes Similarity candidates, Delta trials, accepted Delta chunks,
their logical/payload bytes, maximum dependency depth, reordered regions, and
placement windows. A zero in any emitted field remains a real observation from
the engine.

## CSV schema

A successful command writes exactly one CSV record to standard output and no
header. Diagnostics and failures use the process error path. Columns are fixed;
the final two fields are `-`, `0` when no dictionary is configured:

```text
policy,policy_id,files,logical_bytes,physical_payload_bytes,exact_hit_bytes,logical_chunks,exact_hits,raw_chunks,zstd_regions,zstd_dictionary_regions,delta_chunks,fill_extents,fill_bytes,similarity_candidates,delta_trials,delta_logical_bytes,delta_payload_bytes,maximum_delta_depth,reordered_regions,placement_windows,workers_configured,workers_max_used,inflight_mib,ingest_seconds,restore_seconds,elapsed_seconds,ingest_bytes_per_second,restore_bytes_per_second,dictionary_id,dictionary_bytes
```

Counts and byte totals are checked aggregates of `ReductionReport` values.
`physical_payload_bytes` counts encoded payload only. It does not estimate
future container record headers, recovery indexes, alignment, footers, filesystem
allocation, or metadata-tier cost and must not be presented as total on-disk
usage. `delta_chunks` counts accepted Delta encodings; candidate and trial counts
include work that was considered and rejected. `zstd_dictionary_regions` is the
subset of Zstd regions encoded with the immutable configured dictionary. FILL
extents and bytes report constant DATA ranges represented without chunks; they
remain distinct from sparse holes.
`workers_max_used` is the maximum worker count reported for any one input, not a
sum. Ingest time includes reading each source and reducing it. Restore time
includes fresh source reads, reconstruction, integrity verification, and the
byte comparison. The harness does not drop the kernel page cache, so source
re-reads and restore rates are OS-cache-dependent and are normally hot after the
preceding ingest. The total elapsed value covers both phases and harness
bookkeeping. Throughput is emitted as an integer number of logical bytes per
second, rounded down from overflow-safe `u128` nanosecond arithmetic.

## Current memory limit

The engine's current ingest interface accepts `&[u8]`, while restore returns a
`Vec<u8>`. The harness consequently reads each complete source into memory for
ingest and again for comparison during restore; it is not a streaming harness.
`--inflight-mib` is a nominal scheduling bound used to cap concurrent decoded
work units. It is not a hard encoder or process-memory bound: source buffers,
completed worker outputs, codec contexts and self-check decodes, the in-memory
archive, Exact Index, and restored object are outside it. Size the run
accordingly. This limitation must be removed at the engine API before
interpreting a very large-file run as appliance-scale memory behavior.
