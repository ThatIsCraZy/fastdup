# Exact ingest references without DATA reads — 2026-09-13

The online Exact path used to verify a selected encoding Record before reusing
its Location. This included its stored payload and, when necessary, decoding
and hashing. A cold reference therefore caused DATA I/O even during an
Exact-only write workload. Increasing cache residency could only avoid repeated
checks; it could not eliminate first checks of old Records.

ADR 0015 already permits trusted-client Exact reuse from the incoming Chunk ID
and logical length. The appliance now selects an eligible ACTIVE reference
from the activated Exact Run Set and builds its Manifest recipe without reading
the old Container. This creates no physical Location evidence or cached payload.
Recent writer publications and historical preferences still respect current
Location transitions. Online successor checking uses the same rule when the
bounded per-Chunk evidence set cannot admit a dependency.

## Reproduction and result

`cold_exact_duplicate_ingest_and_commit_do_not_read_data` writes and commits
2 MiB of deterministic pseudorandom data, destroys the owner, opens with normal
committed-Metadata recovery, and writes the identical bytes to another file.
The DATA operation window covers duplicate ingest and its successful Commit.

| Observation | Before | After |
| --- | ---: | ---: |
| DATA `Read` / `ReadExactAt` operations | 27 | 0 |
| New Containers for the duplicate | 0 | 0 |

The same test damages stored Records after the second Commit. The first demand
read fails, confirming that reference-only reuse cannot grant unchecked bytes
to a reader. This is a local operation-count regression, not a measurement of
the live Veeam job's complete device throughput.

## Protection and coverage

- Cold Exact selection and the recent-publication overlay read no DATA and
  admit no physical Location proofs or payloads.
- Cache pressure does not introduce payload revalidation for Exact reuse.
- All 65,536 proof slots can be occupied: the new reference still retains GC
  admission and its Commit-only fallback resolves through the index without DATA.
- At most one shared DATA-reference admission belongs to each Active/Frozen
  epoch. An in-flight lookup retains admission during transfer; cancellation
  and failed-Commit retry preserve ownership. An idle checkpoint releases late
  evidence from a completed cut so GC cannot remain blocked indefinitely.
- GC's existing atomic admission check prevents retirement while a writer
  introduces references. During retirement, new transactions conservatively
  publish independent encodings. Commit fallback selects the current index
  inside the Commit/retirement lock, avoiding an obsolete planning snapshot.
- RETIRING candidates remain ineligible. A historical preference cannot
  override a newer transition. Independent graph checks still detect corruption.
- The cold duplicate fault fixture injects failures before and after each of
  34 Metadata operations: 68 cases preserve the previous or complete Namespace
  across crash recovery. A prepublication failure also exercises same-owner
  retry without DATA I/O. This does not claim that every ambiguous staging
  error can be retried without reopening the owner.

The selected qualification comprises 327 passing tests and 18 existing ignored
manual/benchmark tests: Store and Appliance libraries, durable Namespace and
Namespace faults, Exact Location lookup, Manifest readers, Record singleflight,
end-to-end maintenance and physical Location-cache tests. Store/Appliance
libraries and executable targets pass Clippy with warnings denied.

An existing fault-suite instability was also reproduced on the untouched
`e1ad61c` baseline in an isolated checkout: the unaligned-clone test failed at
`relative=38, after=true`, expecting the completed image at a fixed I/O ordinal.
Initial current-tree runs showed analogous clone/growth expectations; four
consecutive complete fault-suite repetitions subsequently passed. The Exact
regression is stable. The underlying cause of those intermittent ordinal-based
failures was not established by this change; their baseline reproduction is
retained rather than weakening their recovery assertions.

## Commands and artifacts

All commands use `CARGO_TARGET_DIR=/source/fastdup/.artifacts/target` and
`TMPDIR=/source/fastdup/.artifacts/tmp`.

```sh
cargo test -p fastdup-store -p fastdup-appliance --lib
cargo test -p fastdup-appliance --test durable_namespace --test durable_namespace_faults
cargo test -p fastdup-store --test exact_index_location --test manifest_reader --test record_read_singleflight
cargo test -p fastdup-testkit --test end_to_end_maintenance --test location_proof_cache
cargo clippy -p fastdup-store -p fastdup-appliance --lib --bins -- -D warnings
```

Logs, red/green evidence and the baseline comparison are under
`.artifacts/tmp/exact-reference-reuse/`. No durable format, sync ordering, payload
verification contract or cache budget was changed. No new cache was introduced.
The running backup server was not restarted or updated during this work.
