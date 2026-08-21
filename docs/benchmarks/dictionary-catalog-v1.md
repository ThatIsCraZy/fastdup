# Bounded Dictionary Catalog experiment

Date: 2026-08-21

This experiment answers whether fastdup should retain two or more complete
Containers in RAM and compress them together. It uses the public
`reduction_matrix` seam with training and target inputs kept strictly separate.
Every run restored its targets byte-for-byte. Training occurs before timed
ingest, and all outputs live below
`.artifacts/benchmarks/dictionary-catalog-20260821/`.

The results are payload measurements from the experimental Reduction Engine,
not durable Container-size or FUSE-throughput claims. `total` below is encoded
payload plus one retained Dictionary Object. Repeating one target ten times is
intentional: Exact is disabled by the `grouping` preset so the experiment
isolates dictionary amortization rather than duplicate reuse.

## Structured families

JSON v1/v2 train and JSON v3 is held out; XML uses the equivalent split. Each
held-out target is encoded ten times. Plain grouped Zstd uses 1,126,570 bytes
for JSON and 1,163,010 bytes for XML.

| family | requested dictionary | actual dictionary | payload | total | net saving vs plain |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSON | 16 KiB | 16,384 | 1,069,400 | 1,085,784 | 40,786 (3.62%) |
| JSON | 32 KiB | 32,768 | 1,039,410 | 1,072,178 | 54,392 (4.83%) |
| JSON | 64 KiB | 65,536 | 981,040 | 1,046,576 | 79,994 (7.10%) |
| JSON | 128 KiB | 131,072 | 951,510 | 1,082,582 | 43,988 (3.90%) |
| XML | 16 KiB | 16,384 | 1,112,590 | 1,128,974 | 34,036 (2.93%) |
| XML | 32 KiB | 32,768 | 1,088,720 | 1,121,488 | 41,522 (3.57%) |
| XML | 64 KiB | 65,536 | 1,046,590 | 1,112,126 | 50,884 (4.38%) |
| XML | 128 KiB | 131,072 | 1,016,940 | 1,148,012 | 14,998 (1.29%) |

The 64-KiB object is the best tested net choice for either isolated family; a
larger dictionary improves payload but loses after its own retained bytes.
This is a measured corpus result, not a universal default.

A dictionary trained on the other structured family nearly breaks even only
after ten targets: JSON-on-XML saves 1,124 bytes (0.097%) and XML-on-JSON saves
504 bytes (0.045%) after the 65,536-byte object. This supports explicit family
selection and a minimum net-gain gate.

A single combined JSON+XML dictionary did better on this generated corpus:

| requested dictionary | payload for 20 targets | total | net saving vs 2,289,580 plain bytes |
| ---: | ---: | ---: | ---: |
| 32 KiB | 2,162,040 | 2,194,808 | 94,772 (4.14%) |
| 64 KiB | 2,080,500 | 2,146,036 | 143,544 (6.27%) |
| 128 KiB | 1,888,720 | 2,019,792 | 269,788 (11.78%) |

This does not justify merging arbitrary formats. The fixtures share one
generated inventory vocabulary and therefore constitute evidence for one
broader structured-inventory family only. Production activation needs held-out
real workload samples.

For the ten-target 64-KiB runs, dictionary ingest increased from 0.0306 to
0.0514 seconds for JSON and from 0.0358 to 0.0518 seconds for XML. Maximum RSS
increased from approximately 15.5 MiB to 21.2 MiB for JSON and 24.6 MiB for
XML. The corpus is too small for stable throughput conclusions, but it proves
that byte savings are not free.

## Sequential Rocky ISO regions

The pinned Rocky Linux 10.2 minimal ISO was split without transformation into
three consecutive 32-MiB regions. Bytes `[0, 64 MiB)` are training-only; bytes
`[64, 96 MiB)` are the held-out target. This directly models learning from two
prior Containers without retaining them in the active ingest generation.

Plain grouped Zstd emitted 33,484,832 payload bytes. Every dictionary run
emitted exactly the same payload, selected zero dictionary regions, and
therefore lost the complete Dictionary Object cost:

| requested maximum | actual object | payload | net result |
| ---: | ---: | ---: | ---: |
| 16 KiB | 16,384 | 33,484,832 | 16,384 bytes worse |
| 32 KiB | 32,768 | 33,484,832 | 32,768 bytes worse |
| 64 KiB | 11,964 | 33,484,832 | 11,964 bytes worse |
| 128 KiB | 23,565 | 33,484,832 | 23,565 bytes worse |

The trainer may return less than its requested maximum, hence the actual
object lengths above. Plain encoding consumed 0.15 user CPU-seconds and about
111.6 MiB maximum RSS. Dictionary runs consumed 1.79--1.98 user CPU-seconds
and about 141.6--142.3 MiB maximum RSS, dominated by cold-path training. Host
`pswpin`, `pswpout`, and used Swap did not change.

## Decision

Do not combine Container compression histories and do not train a global or
adjacent-Container dictionary. Keep immediate full-Container publication.
Evaluate bounded, asynchronous dictionaries only for demonstrated Dictionary
Families, beginning with repetitive structured data. Training uses prior
samples, never blocks ingest, and produces an immutable object that must be
durable before later encodings reference it.

Production integration remains gated because Container v1 has no assigned
dictionary codec and currently rejects every dependency-bearing record. The
next safe slice is the durable Dictionary Object plus its versioned record
codec and writer/reader/recovery/scrub/fault pairs, not extra live Container
retention.
