# Benchmark corpus

## Rocky Linux image

- File: `Rocky-10.2-x86_64-minimal.iso`
- Official URL:
  `https://download.rockylinux.org/pub/rocky/10.2/isos/x86_64/Rocky-10.2-x86_64-minimal.iso`
- Exact length: `2072444928` bytes
- SHA-256: `aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8`
- Official checksum:
  `https://download.rockylinux.org/pub/rocky/10.2/isos/x86_64/Rocky-10.2-x86_64-minimal.iso.CHECKSUM`

The fetcher must verify both length and hash before atomically publishing the
file into the workspace-local corpus directory. It must reuse an already valid
file and reject every mismatch.

On 2026-08-15 the current benchmark host fetched and reverified the fixture at
`/source/fastdup/.artifacts/tier-data/corpus/Rocky-10.2-x86_64-minimal.iso`.
The XFS file has exact logical and allocated length after trimming speculative
EOF allocation.

### Minimal-change ISO family

`fastdup-testkit` derives ten deterministic fixtures from the pinned ISO using
XFS reflinks followed by exactly eight single-byte XOR edits per fixture. The
version-1 plan uses seed `0x4d595df4d0f33173`, requires unique in-bounds offsets,
and verifies the complete difference set independently with `cmp -l`.

| variant | SHA-256 |
| --- | --- |
| 00 | `da295e8d5b0f6819867369e59138bd3ce7fddd9d041862712268cafeadd756c6` |
| 01 | `e67a2c27fc5495194de667b14023752833b03f4db1e4f4d647fcb7542cc6c51b` |
| 02 | `5ec2ad6c7d8694cb2787da337c17263a5f534f8b7cbdf85f5447e5548718d794` |
| 03 | `7fce1ebb3b0be1736cd4bdc11b9ca20c83b5da05140a207e8aca96529c5338ec` |
| 04 | `37cfecf789ef35fa642720f68f628f6d76de38c86d62e3bbd799424da43fcf41` |
| 05 | `367544503bebcad958b71a00df89b6cc0ac81c3346673721ac0abe1e5bcf129f` |
| 06 | `8b63d715816e31329ac2bddc50638dc6a216142fe30eb2dc2eb6ead5517c7603` |
| 07 | `10fba84a0f14c577079274e0a9d3dfa8595e579339442823172e53d02c8707ad` |
| 08 | `aa7b93d45853456a7338eff27c3b2e63dcb74cfcc5350f73117b70ec6fd68a81` |
| 09 | `2d6c67866658cc75c31f292b00b69ef15f4e332d97bb76e616ebf20a6e070c0b` |

The current artifacts and full mutation manifest are under
`/source/fastdup/.artifacts/tier-data/corpus/rocky-minimal-variants-v1`.
Reproduce into a new directory with:

```bash
cargo run --release -p fastdup-testkit --example prepare_iso_variants -- \
  /source/fastdup/.artifacts/tier-data/corpus/Rocky-10.2-x86_64-minimal.iso \
  /source/fastdup/.artifacts/tier-data/corpus/rocky-minimal-variants-v2
```

## Structured-data families

`fastdup-testkit` implements a time- and randomness-independent generator for
three related JSON and three equivalent XML inventories. Version 2 applies
bounded value changes; version 3 adds a second edit pattern, removes periodic
records, and appends records. The deterministic integration test generates two
independent directories and requires byte equality plus the 800-KiB cap.

Golden manifest:

| file | bytes | SHA-256 |
| --- | ---: | --- |
| `inventory-v1.json` | 591329 | `5a67af9da60cde06ed61f7ac19bf13c18301f4dd8c538acfad1caae1c29bc6f1` |
| `inventory-v2.json` | 591329 | `a608c74a8d242cd6c5c8548707d2355d75a61895adae1f669557cc36c3144e17` |
| `inventory-v3.json` | 594574 | `811fee8faff94bce70466eea7b472ce5fc80e5d9af4ba225d40c265ec348f25f` |
| `inventory-v1.xml` | 661802 | `4a702f6387b7a0765617c99778a0829ae2e93a22d8f87829f987a9741ce86ce0` |
| `inventory-v2.xml` | 661802 | `8f44b5487614601567e81139c8179a7627d546242e926de73e0bb2f632026000` |
| `inventory-v3.xml` | 665421 | `d70d7fcb5471a3b3a014e4e3223407d29812e4acca160e41ef0b7f3007c07d75` |

The current artifacts are under
`/source/fastdup/.artifacts/tier-data/corpus/structured-v1`. All three JSON files
pass `jq` parsing and all three XML files pass `xmllint --noout`.

Reproduce into a new directory with:

```bash
cargo run --release -p fastdup-testkit --example generate_structured_corpus -- \
  /source/fastdup/.artifacts/corpus/structured-v1
```
