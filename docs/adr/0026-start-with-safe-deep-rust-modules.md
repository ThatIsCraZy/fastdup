---
status: accepted
---

# Start with safe deep Rust modules

The initial Rust workspace separates a pure `fastdup-format` module from the
I/O-owning `fastdup-store` and deterministic `fastdup-testkit`, adding a daemon
only after the container core is sound. These are deep boundaries rather than a
microcrate per type. Format, store, and testkit initially forbid unsafe code;
future SIMD, FUSE, or io_uring unsafe code must live in isolated platform modules
behind scalar/reference interfaces and differential tests.

## Consequences

Durable structures use explicit serialization rather than Serde/Bincode or Rust
layout. A small locked dependency set supplies established BLAKE3 and CRC32C
implementations. Stage 1 uses synchronous/vectored file I/O behind the faultable
storage boundary; io_uring must later pass the identical recovery suite. Pipeline
features are runtime policy flags, while Cargo features only gate platform code
or dependencies. The pinned toolchain and every build/test/temp artifact remain
reproducible under the workspace-local execution rule.
