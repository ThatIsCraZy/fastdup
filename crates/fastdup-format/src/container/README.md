# Container implementation map

`../container.rs` retains the public exports, identities, verified Location
coordinates, format constants, errors and scalar wire helpers. The private
modules below implement the existing format; splitting them does not introduce
a new on-disk version or change the public `fastdup_format` interface.

| Module | Responsibility |
| --- | --- |
| `adaptive.rs` | Prehashed inputs, parallel record planning, ordering and adaptive writer entry points |
| `compression.rs` | Reusable compressor, incompressibility-gate policy and worker metrics |
| `writer.rs` | Container layout, initialized record assembly, sealing and writer-carried publication evidence |
| `aligned.rs` | Page-aligned buffer ownership and append operations |
| `records.rs` | Independent RAW/Zstd encoding, record checksums and decoded Chunk verification |
| `dependent.rs` | Depth-one Zstd-prefix/Sparse-XOR encoding, codec-specific assembly, Base/target verification |
| `envelope.rs` | Header/Footer and Recovery Index serialization, layout validation and structural commitment |
| `summary.rs` | Immutable geometry accounting derived from records; no liveness claims |
| `recovery.rs` | Paired-envelope discovery, Recovery Index candidates and bounded selected-record reads |
| `structure.rs` | Payload-free structural verification |
| `image.rs` | Complete-image decoding, full publication verification and verified record transplantation |
| `payload.rs` | Verified payload provenance, shared backing and read views |
| `cache_payload.rs`, `payload_backing.rs` | Children of `payload`: compressed cache representation and owned/pooled backing |

Imports name their actual private module instead of forwarding implementation
helpers through the facade. `pub(super)` is restricted to the private Container
subsystem: it does not let another crate or another `fastdup-format` subsystem
construct verified payloads/publications or bypass validation. Codec-specific
payload fields remain private to their codec. Buffer ownership and pooled-cache
lifetimes remain with `payload`.

## Paths that are intentionally retained

- Structural checks, bounded candidate reads and complete-image verification
  are different proof strengths. A paired envelope or a Recovery Index candidate
  cannot substitute for record CRC and decoded Chunk identity checks. Recovery,
  scrub and maintenance still need these paths (ADRs 0060, 0075, 0088, 0090).
- Writer-carried evidence avoids redoing work on bytes just encoded in memory.
  Independent publication verification checks bytes read back from storage.
  Keeping both is intentional; neither is obsolete duplication.
- `VerifiedContainerImage::prepare_encoded_record` is used by maintenance to
  transplant a verified independent record. It is not an unchecked byte-copy
  shortcut.
- The RAW/Zstd/prefix/Sparse-XOR convenience encoders also build codec and
  corruption fixtures. The scalar Sparse-XOR encoder is an independent oracle
  for the optimized implementation, not dead production cleanup material.
- `writer::encode_container_from_adaptive_plans_zeroed` is compiled only in
  tests. Its deliberately separate implementation checks byte-for-byte equality
  of the append writer and provides the assembly benchmark baseline.

The usage audit found no clearly dead production path to remove. Tests for
private payload ownership/provenance live under `payload`, the prefix output
boundary test under `dependent`, and adaptive assembly/order tests under
`adaptive`; tests do not require widening proof internals to the crate.

Validation: `cargo test -p fastdup-format` exercises codec corruption, structural
versus content proofs, Recovery Index candidates, owned reads, publication
verification and record transplantation. Run it with the workspace-local Cargo
target and temporary directories required by the repository instructions.
