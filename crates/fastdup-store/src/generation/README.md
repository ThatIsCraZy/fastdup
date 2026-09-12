# Generation repository module map

`../generation.rs` is the stable facade. It owns the repository's shared locks,
cache handles and root-pin registries, keeps the opaque successor capability
state, and re-exports the existing result/error types. Private modules implement
methods on the same `GenerationRepository`; they do not create new repository
instances, adapters, locks or allocation layers.

| Module | Responsibility |
| --- | --- |
| `commit.rs` | Predecessor fence, Namespace publication, WAL append and durability ordering |
| `recovery.rs` | WAL-prefix compatibility, graph selection, fallback and mount recovery |
| `checkpoint_copy.rs` | Pinned Metadata graph export to DATA-tier Recovery Checkpoints and installation |
| `manifests.rs` | Complete/append Manifest publication, reads and explicit structural scrub |
| `manifest_edits.rs` | Path-local replacement, truncation, splice and successor-proof composition |
| `graph.rs` | Namespace transition rules, graph traversal and required Chunk identities |
| `metadata.rs` | Metadata publication, cache-aware object reads and canonical object names |
| `liveness.rs` | Retained-generation reachability, deltas, scrub and fenced DATA-GC operations |
| `metadata_gc.rs` | Metadata catalogs, exact marking, candidate checks, deletion and invalidation |
| `pins.rs` | Root-pin acquisition/release and conservative dirty-state transitions |
| `verification.rs` | Required-Chunk verification through Containers and the pinned Exact Index |
| `results.rs` | Opaque result/proof types and their existing read-only accessors |
| `error.rs` | Error conversion and the recovery-fallback classification |
| `tests.rs` | Existing transition and warm-Metadata-cache regression tests |

## Boundaries to preserve

- DATA and immutable Metadata synchronization precede Commit-WAL durability.
  Publication and recovery are separate operations, even when the caller already
  owns a valid in-process successor proof (ADRs 0019, 0036 and 0039).
- A cached Metadata object does not select a generation or prove liveness.
  Independent recovery, scrub and deletion-proof scopes remain at their existing
  entry points; `metadata.rs` preserves the shared cache's bypass behavior
  (ADR 0046). Do not replace explicit structural scrub with an ordinary read.
- A normal mount's selected Metadata graph and outstanding DATA verification
  remain distinct (ADRs 0090 and 0091).
- Metadata collection still holds its publication barrier and Commit lock across
  the exact mark/delete operation. Root-pin changes invalidate the conservative
  GC state; a catalog delta never grants deletion authority (ADRs 0066–0068).
- Private cross-module helpers and result fields are visible only inside
  `generation`. Public/crate-visible exports retain their old paths; no proof
  constructor or private proof field becomes public.

This is a source-organization refactor. Existing function bodies, field order,
serialization, cache policy, lock scope and I/O order are retained. All 181
function implementations (including relocated tests) match the pre-refactor
source apart from visibility and rustfmt formatting. No new unsafe code or
runtime dispatch is introduced.

Validation uses the existing Store unit tests, Appliance namespace crash/clone,
Manifest-growth and structural-recovery suites, and Testkit generation and
Recovery-Checkpoint fault matrices. Production Clippy also checks the durable
FUSE consumer. Build and audit artifacts live under `.artifacts/generation-refactor`.

Validation for this extraction: 206 tests passed and production Clippy passed.
One additional Testkit case,
`missing_data_location_prevents_commit_and_corrupt_newest_data_falls_back`,
still returns generation 3 where the test expects 2. Running that exact test
against the saved original, unsplit `generation.rs` reproduced the same failure.
It is an existing verification issue, not changed or hidden by this refactor;
its baseline reproduction is in the workspace-local audit artifacts.
