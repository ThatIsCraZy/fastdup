# Small-File Runtime Policy Hot-Path Check — 2026-09-01

## Scope

The configurable Small-File suffix policy replaces the fixed `.json` / `.xml`
comparisons. Policy validation, canonicalization and matcher compilation run
only during a management update. The immutable compiled matcher is installed
under the namespace catalog write lock and every write uses it while holding
the catalog read lock that was already required to inspect hardlink names.

The write path adds no lock, syscall, allocation, reference-count operation or
configuration parsing. A commit clones one policy `Arc` per commit cut, not per
write, so an in-flight cut remains deterministic across a live update.

## Release microbenchmark

Command:

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test -p fastdup-posix --release \
  dynamic_matcher_hot_path_benchmark -- --ignored --nocapture
```

Twenty million mixed matching and non-matching names were evaluated against
the former fixed two-suffix predicate and the compiled reverse suffix trie.
The retained regression harness verifies identical results. On the development
host the fixed predicate measured 3.21 ns/name and the dynamic matcher measured
5.14 ns/name: 1.93 ns/name absolute matcher cost. This occurs inside the
pre-existing catalog traversal and is below timer-visible end-to-end write
latency; importantly, the cost has no dependency on the number of configured
suffixes (up to the validated limit of 64).

## Result

The runtime configurability introduces no new write-path synchronization or
allocation and no measurable end-to-end I/O regression. Retain and rerun the
ignored release harness whenever the compiled matcher changes.
