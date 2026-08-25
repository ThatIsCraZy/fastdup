# Exact writer-image publication proof

Measured on 2026-08-24 with the `smb-single-stream-benchmark` workflow. Each
run uploaded the same 2,072,444,928-byte Rocky 10.2 minimal ISO three times in
sequence to a fresh repository. The ISO SHA256 was
`aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`.
Metadata used XFS on `/dev/sda`, Containers used XFS on `/dev/sdb`, the Samba
configuration SHA256 was
`1dfb704cc44e44e6b0d4debb67c2208a106347f786a33cb4425007537d600be2`,
and every run observed zero daemon Swap.

The baseline binary SHA256 was
`2b1315bdb3a5a759383843735f6b495710e50a11591641f4ebc104012c3656c8`.
The challenger SHA256 was
`54a3dd37c1be5027bad288b67455d8df8b5f409452905709c79fb22cdedbe80c`.
The runs alternated baseline and challenger. Reports are retained as
`.artifacts/benchmarks/smb-whole-hash-{baseline,challenger}-{1,2,3}.json`.

| variant | run | aggregate MiB/s | completed-write p99 ms | daemon CPU s | host busy % | reduction ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 591.0 | 6,395.8 | 17.83 | 10.23 | 2.884 |
| baseline | 2 | 475.0 | 6,560.4 | 21.91 | 11.39 | 2.846 |
| baseline | 3 | 462.7 | 6,315.3 | 23.77 | 12.29 | 2.907 |
| challenger | 1 | 650.5 | 5,332.8 | 16.42 | 9.89 | 2.926 |
| challenger | 2 | 635.1 | 5,148.6 | 17.62 | 10.72 | 2.920 |
| challenger | 3 | 428.9 | 7,365.6 | 27.27 | 13.22 | 2.845 |
| baseline median | | 475.0 | 6,395.8 | 21.91 | 11.39 | 2.884 |
| challenger median | | 635.1 | 5,332.8 | 17.62 | 10.72 | 2.920 |

At the median, exact writer-image proof increased completed-write throughput by
33.7%, reduced completed-write p99 by 16.6%, and reduced daemon CPU by 19.6%.
The third challenger run was a visible contention outlier: host busy time,
Chunk-hash runnable time, and encode runnable time all rose together. It remains
in the result set and is why the comparison uses medians rather than best runs.

Repository allocation and whole-reread bytes varied by roughly 1--3% across
runs because SMB/FUSE request timing changes Container packing. The median
challenger read 1.24% fewer Container bytes than the median baseline. Therefore
the reported gain is the observed SingleStream end-to-end effect, not a pure
BLAKE3 microbenchmark. The code change itself removes only the second
whole-image BLAKE3 pass: exact byte comparison and all Record CRC, decoded
Chunk-ID, Recovery-Index, padding, and envelope checks remain in publication;
recovery and scrub still recompute the complete Container hash.
