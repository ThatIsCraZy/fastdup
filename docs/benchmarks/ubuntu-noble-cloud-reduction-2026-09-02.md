# Ubuntu Noble cloud-image reduction benchmark

Date: 2026-09-02  
Revision: `916248dbc6f38919ca78d43facdbe3157e972e79` plus the uncommitted
publication-ordering fix described below.

## Result

The conservative whole-repository data-reduction factor is **5.84:1** with
advanced Similarity disabled. This divides 26,288,727,552 logical bytes by all
4,502,515,712 allocated DATA and Metadata bytes after the final offline scrub.
The DATA-tier-only factor is 6.02:1.

With `dependent-v1` Similarity enabled, the corresponding factors are 5.80:1
for the whole repository and 5.98:1 for the DATA tier. Similarity found only
three useful Prefix encodings in 30,543 queries. It reported 130,359 bytes of
payload savings, but the final repository was 27,652,096 bytes larger than the
paired baseline. The workload therefore provides no measurable net Similarity
benefit; its tiny codec saving is below run-to-run Container/checkpoint
variation.

## Corpus

All six distinct dated Ubuntu 24.04 LTS (Noble) builds listed by the official
source on 2026-09-02 were fetched from
`https://cloud-images.ubuntu.com/noble/DATE/` and verified against each date's
official `SHA256SUMS` file. The `current` link pointed to 2026-08-26 and was not
counted as a seventh image.

| Date | QCOW2 bytes | SHA-256 |
| --- | ---: | --- |
| 2026-06-15 | 620,852,736 | `5fa5b05e5ec239858c4531485d6023b0896448c2df7c63b34f8dae6ea6051a44` |
| 2026-07-05 | 621,673,984 | `ffe6203da54deeb6db5d2a98a83f9ec8e55f149d3f7ba622e1abe5fa966ee3d6` |
| 2026-07-25 | 624,105,472 | `d1940f7d69d343355e183dff1e08a59852d32e7309baa7a4bad8365b11b005ac` |
| 2026-08-01 | 624,239,616 | `0533b0655c32e68b31d792ecd6ccfca95abdbc536c4446874fe0513bd4140ffe` |
| 2026-08-14 | 624,447,488 | `6e40c07ae715f744f84af0bec76415cc1987dd115b4b8de437818561f01a3733` |
| 2026-08-26 | 624,829,952 | `d0fe84bb5f80853425fa6be28e2c106f30104c3cfe8611933f2e65c9b63f0e30` |

The six QCOW2 files contain 3,740,149,248 logical bytes. Each was also converted
with QEMU 10.1 to a fully allocated 3,758,096,384-byte RAW image. Only one RAW
image was present in staging at a time, so the six persistent QCOW2 files plus
one rotating RAW file stayed around 7.0 GiB and below the authorized 10-GB
staging limit. The combined repository workload contains 26,288,727,552
logical bytes (24.49 GiB).

## Method

- Metadata: `/dev/sdb1`, 20-GB XFS, mounted at
  `/var/lib/fastdup/repository/metadata`.
- DATA: `/dev/sdc1`, 200-GB XFS, mounted at
  `/var/lib/fastdup/repository/data`.
- Corpus: `/source/fastdup/.artifacts/ubuntu-cloud-benchmark-source`, a bind
  mount backed by the separate `/home` logical volume.
- Results and runner:
  `/source/fastdup/.artifacts/ubuntu-cloud-benchmark-results`.
- Online GC was disabled. Physical usage is allocated bytes from `du -B1` on
  both repository roots, not apparent file length.
- Each A/B run started from empty repository roots. The oldest QCOW2 was
  written independently, the paired Exact/Similarity indexes were rebuilt,
  and the other five versions were written with the selected policy. The same
  sequence was repeated for RAW while retaining the QCOW2 files.
- Every destination was read back through FUSE and SHA-256 checked immediately.
  A final offline scrub independently decoded and verified the complete
  repository before the final measurement.
- The existing XFS mounts do not provide the required project-quota interface,
  so the laboratory-only shared-isolation bypass was used. The tiers are still
  physically separate devices. The daemon also warned that its cgroup allowed
  swap. These limitations make this a capacity/reduction benchmark, not a
  production quota or throughput qualification.

The paired measurements use the same rebuilt Release binary. `baseline-r5`
uses `FASTDUP_ADVANCED_REDUCTION=off`; `similarity-r3` uses
`FASTDUP_ADVANCED_REDUCTION=dependent-v1` only for the five target versions in
each format.

## Allocated-byte results

| Policy and stage | Logical bytes | DATA bytes | Metadata bytes | Whole repository | Factor |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off, QCOW2 complete | 3,740,149,248 | 2,898,231,296 | 27,639,808 | 2,925,871,104 | 1.278:1 |
| Similarity, QCOW2 complete | 3,740,149,248 | 2,900,213,760 | 27,631,616 | 2,927,845,376 | 1.277:1 |
| Off, combined before scrub | 26,288,727,552 | 4,356,165,632 | 135,528,448 | 4,491,694,080 | 5.853:1 |
| Similarity, combined before scrub | 26,288,727,552 | 4,383,109,120 | 136,232,960 | 4,519,342,080 | 5.817:1 |
| **Off, combined after scrub** | **26,288,727,552** | **4,366,987,264** | **135,528,448** | **4,502,515,712** | **5.839:1** |
| **Similarity, combined after scrub** | **26,288,727,552** | **4,393,934,848** | **136,232,960** | **4,530,167,808** | **5.803:1** |

The final baseline stores 82.873% fewer allocated bytes than the logical
input. The post-scrub measurement is deliberately conservative: the scrub
adds about 10.82 MB of recovery/checkpoint state that remains part of the
repository.

The checkpoint telemetry identifies 11,188,043,776 logical bytes (10.42 GiB,
42.56% of the corpus) as FILL extents, predominantly zero ranges in the RAW
images. After excluding those FILL bytes, the remaining 15,100,683,776 logical
bytes still map to only 4,502,515,712 whole-repository bytes, an effective
3.35:1 result from Exact Dedup, compression, and their Metadata/recovery
overhead together. This run does not include separate Exact-off and
compression-off probes, so it cannot honestly split that combined 3.35:1 into
independent Dedup and compression factors.

## Similarity telemetry

| Metric | QCOW2 targets | RAW targets | Total |
| --- | ---: | ---: | ---: |
| Queries | 18,142 | 12,401 | 30,543 |
| Candidates / Base reads | 1 | 2 | 3 |
| Base bytes read | 145,343 | 239,072 | 384,415 |
| Sparse-XOR trials / accepted | 1 / 0 | 2 / 0 | 3 / 0 |
| Prefix trials / accepted | 1 / 1 | 2 / 2 | 3 / 3 |
| Saved payload bytes | 66,764 | 63,595 | 130,359 |
| Errors | 0 | 0 | 0 |

Only 0.0098% of queries found a candidate. The 127.31-KiB reported payload
saving is 0.0029% of the baseline repository. The final A/B difference is
instead +26.38 MiB (+0.614%) for Similarity, so it must be treated as noise and
overhead rather than a negative codec claim. For this corpus, the practical
conclusion is to leave Similarity off; SeqCDC/Exact, compression, and FILL
already capture the useful reduction.

## Integrity and discovered defect

Both final probes restored 12 of 12 files with matching SHA-256. Their RAW hash
manifests are identical. Offline scrub results:

- Baseline: 12 manifests, 351 Containers, 86,327 Chunks, `scrub_ok=true`.
- Similarity: 12 manifests, 354 Containers, 86,949 Chunks, `scrub_ok=true`.

The first Similarity attempt exposed a correctness assertion before the second
target image: advanced reduction grouped ordinary, independent, and dependent
records by codec, but publication Locations were zipped positionally against
Chunk-ID-sorted claims. `PublicationClaims::finish` now sorts verified
Locations by `(Chunk ID, logical length)` before pairing them. The new
`publication_claims_match_grouped_encoder_locations_by_chunk_key` regression
test failed on the original path and passes after the fix. All 32
`fastdup-appliance` library unit tests pass, and the complete Similarity corpus
is the successful end-to-end regression.

Raw evidence is retained outside source control in:

- `baseline-r5`: paired final baseline;
- `similarity-r3`: paired final Similarity probe;
- `similarity-r1`: original assertion failure; and
- `run_probe.sh`: exact orchestration, rotation, hashing, and measurement logic.
